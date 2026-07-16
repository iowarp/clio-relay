"""Scheduler provider boundary for cluster job status and cancellation."""

from __future__ import annotations

import re
import subprocess
import tempfile
import threading
import time
from collections.abc import Callable, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import PurePosixPath
from typing import IO, Protocol, runtime_checkable

from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.models import (
    SchedulerConnectorPlacement,
    SchedulerConnectorStepIdentity,
    SchedulerConnectorStepStatus,
    SchedulerPhase,
    SchedulerStatus,
)

SQUEUE_FIELDS = "%i|%T|%R|%P|%q|%u|%D|%C|%m|%V|%S|%M|%l"
SACCT_FIELDS = "JobIDRaw,State,Partition,QOS,Submit,Start,Elapsed,NNodes,NCPUS,ReqMem"
SCHEDULER_PENDING_CHECK_ID = "scheduler.pending"
SCHEDULER_ALLOCATED_CHECK_ID = "scheduler.allocated"
SCHEDULER_RUNNING_CHECK_ID = "scheduler.running"
SCHEDULER_COMPLETED_CHECK_ID = "scheduler.completed"
SCHEDULER_RUNTIME_METADATA_CHECK_ID = "scheduler.structured-metadata"
SCHEDULER_COMMAND_TIMEOUT_SECONDS = 15.0
CONNECTOR_STEP_REGISTRATION_TIMEOUT_SECONDS = 15.0
CONNECTOR_STEP_REGISTRATION_POLL_SECONDS = 0.2
CONNECTOR_LAUNCHER_DIAGNOSTIC_BYTES = 16 * 1024
CONNECTOR_STEP_CLEANUP_TIMEOUT_SECONDS = 15.0
CONNECTOR_STEP_FAILED_RECONCILIATION_OBSERVATIONS = 3
SCHEDULER_RECONCILIATION_MAX_AGE = timedelta(days=7)
SCHEDULER_RECONCILIATION_TIME_TOLERANCE = timedelta(seconds=5)
_CONNECTOR_LAUNCHER_REAPER_LOCK = threading.Lock()
_CONNECTOR_LAUNCHER_REAPER_WAKE = threading.Event()
_CONNECTOR_LAUNCHERS: set[subprocess.Popen[bytes]] = set()
_connector_launcher_reaper_thread: threading.Thread | None = None


class SchedulerProvider(Protocol):
    """Provider interface for scheduler status, cancellation, and target identity."""

    name: str

    def poll(self, scheduler_job_id: str) -> SchedulerStatus:
        """Poll scheduler status for a scheduler job id."""
        ...

    def cancel(self, scheduler_job_id: str) -> subprocess.CompletedProcess[str]:
        """Request scheduler cancellation for a scheduler job id."""
        ...

    def scheduler_cluster_name(self) -> str | None:
        """Return the scheduler-native cluster name when one exists."""
        ...


@runtime_checkable
class SchedulerValidationProvider(SchedulerProvider, Protocol):
    """Optional provider operations used by deterministic live acceptance."""

    name: str

    def submit_held_validation_job(self, *, job_name: str, run_seconds: int) -> str:
        """Submit bounded held work and return its scheduler job id."""
        ...

    def release_validation_job(self, scheduler_job_id: str) -> subprocess.CompletedProcess[str]:
        """Release a held validation job without changing any other job."""
        ...


@runtime_checkable
class SchedulerAllocationConnectorProvider(SchedulerProvider, Protocol):
    """Optional provider boundary for a connector step inside an owned allocation."""

    def connector_placement(self, scheduler_job_id: str) -> SchedulerConnectorPlacement:
        """Resolve the single exact allocation host where the connector must execute."""
        ...

    def launch_connector_step(
        self,
        scheduler_job_id: str,
        *,
        placement_host: str,
        step_marker: str,
        command: Sequence[str],
        output_path: str,
    ) -> SchedulerConnectorStepIdentity:
        """Launch an asynchronous provider-owned connector step."""
        ...

    def poll_connector_step(
        self,
        scheduler_job_id: str,
        *,
        scheduler_step_id: str,
        placement_host: str,
    ) -> SchedulerConnectorStepStatus:
        """Observe one exact connector step and its pinned placement."""
        ...

    def cancel_connector_step(
        self,
        scheduler_job_id: str,
        *,
        scheduler_step_id: str,
    ) -> subprocess.CompletedProcess[str]:
        """Cancel only one exact connector step, never its parent allocation."""
        ...

    def find_connector_step(
        self,
        scheduler_job_id: str,
        *,
        step_marker: str,
        placement_host: str,
    ) -> SchedulerConnectorStepIdentity | None:
        """Reconcile a crash-interrupted launch by its unique provider marker."""
        ...


@runtime_checkable
class SchedulerReconciliationProvider(SchedulerProvider, Protocol):
    """Optional exact-marker lookup for interrupted scheduler submissions."""

    def find_job_ids_by_marker(
        self,
        marker: str,
        *,
        submitted_after: datetime,
        scheduler_user: str,
    ) -> list[str]:
        """Return scheduler ids whose provider-native job name exactly matches marker."""
        ...


class ExternalSchedulerProvider:
    """Provider for runtimes whose scheduler lifecycle is owned externally."""

    name = "external"

    def poll(self, scheduler_job_id: str) -> SchedulerStatus:
        """Report that no relay-owned scheduler observation is configured."""
        _validate_scheduler_job_id(scheduler_job_id)
        return SchedulerStatus(
            scheduler=self.name,
            scheduler_job_id=scheduler_job_id,
            phase=SchedulerPhase.UNKNOWN,
            queue_position_note="scheduler observation is owned by the deployment driver",
        )

    def cancel(self, scheduler_job_id: str) -> subprocess.CompletedProcess[str]:
        """Reject scheduler cancellation when no relay-owned provider is configured."""
        _validate_scheduler_job_id(scheduler_job_id)
        return subprocess.CompletedProcess(
            ["external-scheduler", scheduler_job_id],
            2,
            "",
            "scheduler cancellation is owned by the deployment driver",
        )

    def scheduler_cluster_name(self) -> str | None:
        """Return no scheduler-native identity for externally owned runtimes."""
        return None

    def submit_held_validation_job(self, *, job_name: str, run_seconds: int) -> str:
        """Reject live submission when scheduling is externally managed."""
        del job_name, run_seconds
        raise ConfigurationError("external scheduler providers cannot submit held validation jobs")

    def release_validation_job(self, scheduler_job_id: str) -> subprocess.CompletedProcess[str]:
        """Reject release when scheduling is externally managed."""
        _validate_scheduler_job_id(scheduler_job_id)
        return subprocess.CompletedProcess(
            ["external-scheduler", "release", scheduler_job_id],
            2,
            "",
            "scheduler release is owned by the deployment driver",
        )


class SlurmSchedulerProvider:
    """SLURM provider backed by squeue, controller/accounting history, and scancel."""

    name = "slurm"

    def connector_placement(self, scheduler_job_id: str) -> SchedulerConnectorPlacement:
        """Resolve and prove one BatchHost for a single-node SLURM allocation."""
        _validate_slurm_allocation_job_id(scheduler_job_id)
        record = self._scontrol_one(scheduler_job_id)
        if record is None:
            raise RelayError(f"SLURM job was not found for connector placement: {scheduler_job_id}")
        raw_node_count = record.get("NumNodes")
        try:
            node_count = int(raw_node_count or "")
        except ValueError as exc:
            raise RelayError("SLURM connector placement has an invalid NumNodes value") from exc
        if node_count != 1:
            raise RelayError(
                "SLURM connector placement requires an unambiguous single-node allocation"
            )
        batch_host = record.get("BatchHost")
        node_list = record.get("NodeList")
        if (
            batch_host is None
            or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,1023}", batch_host) is None
            or node_list is None
            or not node_list
            or node_list in {"(null)", "None", "N/A"}
        ):
            raise RelayError("SLURM connector placement omitted a valid BatchHost or NodeList")
        hosts_result = _run_scheduler_command(["scontrol", "show", "hostnames", node_list])
        if hosts_result.returncode != 0:
            raise _scheduler_command_error("scontrol show hostnames", hosts_result)
        hosts = [line.strip() for line in hosts_result.stdout.splitlines() if line.strip()]
        if hosts != [batch_host]:
            raise RelayError("SLURM BatchHost did not exactly match the single allocation host")
        return SchedulerConnectorPlacement(
            scheduler=self.name,
            scheduler_job_id=scheduler_job_id,
            placement_host=batch_host,
            allocation_node_count=1,
            source="slurm-scontrol-batch-host",
            verified=True,
        )

    def launch_connector_step(
        self,
        scheduler_job_id: str,
        *,
        placement_host: str,
        step_marker: str,
        command: Sequence[str],
        output_path: str,
    ) -> SchedulerConnectorStepIdentity:
        """Launch one detached connector and resolve its exact active SLURM step."""
        _validate_slurm_allocation_job_id(scheduler_job_id)
        _validate_connector_placement_host(placement_host)
        _validate_connector_step_marker(step_marker)
        connector_command = _validate_connector_command(command)
        connector_output = _validate_connector_output_path(output_path)
        launch_command = [
            "srun",
            f"--jobid={scheduler_job_id}",
            "--overlap",
            "--exact",
            "--nodes=1",
            "--ntasks=1",
            f"--nodelist={placement_host}",
            f"--job-name={step_marker}",
            "--input=none",
            f"--output={connector_output}",
            f"--error={connector_output}",
            "--open-mode=append",
            "--",
            *connector_command,
        ]
        with tempfile.TemporaryFile(prefix="clio-relay-srun-", mode="w+b") as private_output:
            try:
                launcher = subprocess.Popen(  # noqa: S603 - validated argv, no shell
                    launch_command,
                    stdin=subprocess.DEVNULL,
                    stdout=private_output,
                    stderr=subprocess.STDOUT,
                    start_new_session=True,
                    close_fds=True,
                )
            except OSError as exc:
                raise RelayError(
                    f"could not start detached SLURM connector launcher: {exc}"
                ) from exc
            try:
                deadline = time.monotonic() + CONNECTOR_STEP_REGISTRATION_TIMEOUT_SECONDS
                while True:
                    identity = self.find_connector_step(
                        scheduler_job_id,
                        step_marker=step_marker,
                        placement_host=placement_host,
                    )
                    if identity is not None:
                        _register_connector_launcher_for_reaping(launcher)
                        return identity.model_copy(update={"source": "slurm-srun-detached-marker"})
                    returncode = launcher.poll()
                    if returncode is not None:
                        diagnostic = _read_connector_launcher_diagnostic(private_output)
                        suffix = f": {diagnostic}" if diagnostic else ""
                        raise RelayError(
                            "detached SLURM connector launcher exited before its exact step "
                            f"was registered (returncode={returncode}){suffix}"
                        )
                    if time.monotonic() >= deadline:
                        raise RelayError(
                            "detached SLURM connector step was not registered within the "
                            "bounded provider timeout"
                        )
                    time.sleep(CONNECTOR_STEP_REGISTRATION_POLL_SECONDS)
            except BaseException as launch_error:
                _terminate_connector_launcher(launcher)
                try:
                    self._cleanup_failed_connector_registration(
                        scheduler_job_id,
                        step_marker=step_marker,
                        placement_host=placement_host,
                    )
                except (ConfigurationError, RelayError) as cleanup_error:
                    raise RelayError(
                        "failed SLURM connector registration could not prove exact-step cleanup: "
                        f"{cleanup_error}"
                    ) from launch_error
                raise

    def _cleanup_failed_connector_registration(
        self,
        scheduler_job_id: str,
        *,
        step_marker: str,
        placement_host: str,
    ) -> None:
        """Reconcile and cancel only a late step after launcher registration failed."""
        deadline = time.monotonic() + CONNECTOR_STEP_CLEANUP_TIMEOUT_SECONDS
        absent_observations = 0
        while True:
            identity = self.find_connector_step(
                scheduler_job_id,
                step_marker=step_marker,
                placement_host=placement_host,
            )
            if identity is None:
                absent_observations += 1
                if absent_observations >= CONNECTOR_STEP_FAILED_RECONCILIATION_OBSERVATIONS:
                    return
            else:
                canceled = self.cancel_connector_step(
                    scheduler_job_id,
                    scheduler_step_id=identity.scheduler_step_id,
                )
                cancel_error = (
                    _scheduler_command_error("scancel connector step", canceled)
                    if canceled.returncode != 0
                    else None
                )
                while True:
                    status = self.poll_connector_step(
                        scheduler_job_id,
                        scheduler_step_id=identity.scheduler_step_id,
                        placement_host=placement_host,
                    )
                    if status.state == "absent":
                        return
                    if time.monotonic() >= deadline:
                        detail = f": {cancel_error}" if cancel_error is not None else ""
                        raise RelayError(
                            "late SLURM connector step remained active after exact-step "
                            f"cancellation{detail}"
                        )
                    time.sleep(CONNECTOR_STEP_REGISTRATION_POLL_SECONDS)
            if time.monotonic() >= deadline:
                raise RelayError(
                    "failed SLURM connector registration did not reach a stable absent state"
                )
            time.sleep(CONNECTOR_STEP_REGISTRATION_POLL_SECONDS)

    def poll_connector_step(
        self,
        scheduler_job_id: str,
        *,
        scheduler_step_id: str,
        placement_host: str,
    ) -> SchedulerConnectorStepStatus:
        """Observe exact active-step presence through ``squeue --steps``."""
        _validate_slurm_allocation_job_id(scheduler_job_id)
        _validate_connector_step_id(scheduler_job_id, scheduler_step_id)
        _validate_connector_placement_host(placement_host)
        result = _run_scheduler_command(
            [
                "squeue",
                "--noheader",
                f"--steps={scheduler_step_id}",
                "--format=%i|%N",
            ]
        )
        if result.returncode != 0:
            raise _scheduler_command_error("squeue --steps", result)
        rows = [
            row
            for line in result.stdout.splitlines()
            if line.strip() and (row := _split_row(line, 2)) is not None
        ]
        if any(row[0] != scheduler_step_id for row in rows) or len(rows) > 1:
            raise RelayError("SLURM returned ambiguous connector step identity")
        if not rows:
            return SchedulerConnectorStepStatus(
                scheduler=self.name,
                scheduler_job_id=scheduler_job_id,
                scheduler_step_id=scheduler_step_id,
                placement_host=placement_host,
                record_found=False,
                state="absent",
                observed_host=None,
                verified=True,
            )
        observed_host = rows[0][1]
        if observed_host != placement_host:
            raise RelayError(
                "SLURM connector step did not run on its provider-verified placement host"
            )
        return SchedulerConnectorStepStatus(
            scheduler=self.name,
            scheduler_job_id=scheduler_job_id,
            scheduler_step_id=scheduler_step_id,
            placement_host=placement_host,
            record_found=True,
            state="active",
            observed_host=observed_host,
            verified=True,
        )

    def cancel_connector_step(
        self,
        scheduler_job_id: str,
        *,
        scheduler_step_id: str,
    ) -> subprocess.CompletedProcess[str]:
        """Cancel only ``job.step`` so the parent allocation remains untouched."""
        _validate_slurm_allocation_job_id(scheduler_job_id)
        _validate_connector_step_id(scheduler_job_id, scheduler_step_id)
        return _run_scheduler_command(["scancel", scheduler_step_id])

    def find_connector_step(
        self,
        scheduler_job_id: str,
        *,
        step_marker: str,
        placement_host: str,
    ) -> SchedulerConnectorStepIdentity | None:
        """Find at most one active connector step after an interrupted launch."""
        _validate_slurm_allocation_job_id(scheduler_job_id)
        _validate_connector_step_marker(step_marker)
        _validate_connector_placement_host(placement_host)
        result = _run_scheduler_command(
            [
                "squeue",
                "--noheader",
                "--steps",
                f"--jobs={scheduler_job_id}",
                f"--name={step_marker}",
                "--format=%i|%j|%N",
            ]
        )
        if result.returncode != 0:
            raise _scheduler_command_error("squeue --steps marker lookup", result)
        matches: list[str] = []
        for line in result.stdout.splitlines():
            row = _split_row(line, 3)
            if row is None or row[1] != step_marker:
                continue
            _validate_connector_step_id(scheduler_job_id, row[0])
            if row[2] != placement_host:
                raise RelayError(
                    "SLURM connector marker resolved outside its verified placement host"
                )
            if row[0] not in matches:
                matches.append(row[0])
        if len(matches) > 1:
            raise RelayError("multiple active SLURM steps matched one connector marker")
        if not matches:
            return None
        return SchedulerConnectorStepIdentity(
            scheduler=self.name,
            scheduler_job_id=scheduler_job_id,
            scheduler_step_id=matches[0],
            step_marker=step_marker,
            placement_host=placement_host,
            source="slurm-squeue-step-marker",
            verified=True,
        )

    def poll(self, scheduler_job_id: str) -> SchedulerStatus:
        """Poll SLURM, including clusters where accounting storage is disabled."""
        _validate_scheduler_job_id(scheduler_job_id)
        current = self._squeue_one(scheduler_job_id)
        if current is not None:
            status = _status_from_squeue_row(current).model_copy(
                update={"record_found": True, "active_record_found": True}
            )
            if status.phase == SchedulerPhase.PENDING:
                return _with_queue_position(status, self._squeue_pending_jobs())
            return status
        history_errors: list[str] = []
        try:
            historical = self._sacct_one(scheduler_job_id)
        except RelayError as exc:
            historical = None
            history_errors.append(str(exc))
        if historical is not None:
            return _status_from_sacct_row(scheduler_job_id, historical).model_copy(
                update={"record_found": True, "active_record_found": False}
            )
        try:
            controller_record = self._scontrol_one(scheduler_job_id)
        except RelayError as exc:
            controller_record = None
            history_errors.append(str(exc))
        if controller_record is not None:
            return _status_from_scontrol_record(
                scheduler_job_id,
                controller_record,
            ).model_copy(update={"record_found": True, "active_record_found": False})
        diagnostic = "; ".join(history_errors)
        note = "scheduler job was not found by squeue, sacct, or scontrol"
        if diagnostic:
            note = f"{note}; {diagnostic}"
        return SchedulerStatus(
            scheduler=self.name,
            scheduler_job_id=scheduler_job_id,
            phase=SchedulerPhase.UNKNOWN,
            record_found=False if not history_errors else None,
            active_record_found=False,
            queue_position_note=note,
        )

    def cancel(self, scheduler_job_id: str) -> subprocess.CompletedProcess[str]:
        """Cancel a SLURM job with scancel."""
        _validate_scheduler_job_id(scheduler_job_id)
        return _run_scheduler_command(
            ["scancel", scheduler_job_id],
        )

    def scheduler_cluster_name(self) -> str:
        """Read the configured SLURM ClusterName through the provider boundary."""
        result = _run_scheduler_command(["scontrol", "show", "config"])
        if result.returncode != 0:
            raise _scheduler_command_error("scontrol", result)
        for line in result.stdout.splitlines():
            key, separator, value = line.partition("=")
            if separator and key.strip() == "ClusterName":
                cluster_name = value.strip().split()[0]
                if cluster_name:
                    return cluster_name
        raise RelayError("scheduler provider output omitted SLURM ClusterName")

    def submit_held_validation_job(self, *, job_name: str, run_seconds: int) -> str:
        """Submit one bounded held SLURM job for deterministic lifecycle validation."""
        _validate_validation_job_name(job_name)
        if run_seconds < 1 or run_seconds > 300:
            raise ConfigurationError("validation run_seconds must be between 1 and 300")
        result = _run_scheduler_command(
            [
                "sbatch",
                "--parsable",
                "--hold",
                "--job-name",
                job_name,
                "--time",
                "00:05:00",
                "--wrap",
                f"sleep {run_seconds}",
            ]
        )
        if result.returncode != 0:
            raise _scheduler_command_error("sbatch", result)
        scheduler_job_id = result.stdout.strip().splitlines()[-1].split(";", 1)[0].strip()
        _validate_scheduler_job_id(scheduler_job_id)
        return scheduler_job_id

    def release_validation_job(self, scheduler_job_id: str) -> subprocess.CompletedProcess[str]:
        """Release one exact held SLURM validation job."""
        _validate_scheduler_job_id(scheduler_job_id)
        return _run_scheduler_command(["scontrol", "release", scheduler_job_id])

    def find_job_ids_by_marker(
        self,
        marker: str,
        *,
        submitted_after: datetime,
        scheduler_user: str,
    ) -> list[str]:
        """Find current or recent SLURM jobs by exact name, user, and time window."""
        _validate_reconciliation_marker(marker)
        _validate_scheduler_user(scheduler_user)
        submitted_after = _validate_reconciliation_time(submitted_after)
        earliest_submit = submitted_after - SCHEDULER_RECONCILIATION_TIME_TOLERANCE
        latest_submit = datetime.now(UTC) + SCHEDULER_RECONCILIATION_TIME_TOLERANCE
        result = _run_scheduler_command(
            [
                "squeue",
                "-h",
                "--name",
                marker,
                "--user",
                scheduler_user,
                "-o",
                "%A|%j|%u|%V",
            ],
        )
        if result.returncode != 0:
            raise _scheduler_command_error("squeue", result)
        matches: list[str] = []
        for line in result.stdout.splitlines():
            row = _split_row(line, 4)
            if row is None or row[1] != marker or row[2] != scheduler_user:
                continue
            submit_time = _parse_slurm_reconciliation_time(row[3])
            if submit_time is None or submit_time < earliest_submit or submit_time > latest_submit:
                continue
            _validate_scheduler_job_id(row[0])
            if row[0] not in matches:
                matches.append(row[0])
            if len(matches) > 1:
                break
        local_start = earliest_submit.astimezone().strftime("%Y-%m-%dT%H:%M:%S")
        history = _run_scheduler_command(
            [
                "sacct",
                "-n",
                "-P",
                "-X",
                "--name",
                marker,
                "--user",
                scheduler_user,
                "--starttime",
                local_start,
                "-o",
                "JobIDRaw,JobName,User,Submit",
            ],
        )
        if history.returncode != 0:
            error = _scheduler_command_error("sacct", history)
            raise RelayError(
                "SLURM accounting history is required to prove scheduler marker uniqueness: "
                f"{error}"
            ) from error
        for line in history.stdout.splitlines():
            row = _split_row(line, 4)
            if row is None or row[1] != marker or row[2] != scheduler_user:
                continue
            submit_time = _parse_slurm_reconciliation_time(row[3])
            if submit_time is None or submit_time < earliest_submit or submit_time > latest_submit:
                continue
            if not row[0].isdecimal():
                continue
            _validate_scheduler_job_id(row[0])
            if row[0] not in matches:
                matches.append(row[0])
            if len(matches) > 1:
                break
        return matches

    def _squeue_one(self, scheduler_job_id: str) -> list[str] | None:
        result = _run_scheduler_command(
            ["squeue", "-h", "-j", scheduler_job_id, "-o", SQUEUE_FIELDS],
        )
        if result.returncode != 0:
            if _slurm_job_absent_from_active_queue(result):
                return None
            raise _scheduler_command_error("squeue", result)
        for line in result.stdout.splitlines():
            row = _split_row(line, 13)
            if row and row[0] == scheduler_job_id:
                return row
        return None

    def _squeue_pending_jobs(self) -> list[list[str]]:
        result = _run_scheduler_command(
            ["squeue", "-h", "-t", "PD", "-o", SQUEUE_FIELDS],
        )
        if result.returncode != 0:
            raise _scheduler_command_error("squeue", result)
        return [row for line in result.stdout.splitlines() if (row := _split_row(line, 13))]

    def _sacct_one(self, scheduler_job_id: str) -> list[str] | None:
        result = _run_scheduler_command(
            [
                "sacct",
                "-n",
                "-P",
                "-j",
                scheduler_job_id,
                "-o",
                SACCT_FIELDS,
            ],
        )
        if result.returncode != 0:
            raise _scheduler_command_error("sacct", result)
        for line in result.stdout.splitlines():
            row = _split_row(line, 10)
            if row and row[0] == scheduler_job_id:
                return row
        return None

    def _scontrol_one(self, scheduler_job_id: str) -> dict[str, str] | None:
        result = _run_scheduler_command(
            ["scontrol", "show", "job", scheduler_job_id, "-o"],
        )
        if result.returncode != 0:
            raise _scheduler_command_error("scontrol", result)
        for line in result.stdout.splitlines():
            record = _parse_scontrol_record(line)
            if record.get("JobId") == scheduler_job_id:
                return record
        return None


SchedulerProviderFactory = Callable[[], SchedulerProvider]
_PROVIDER_FACTORIES: dict[str, SchedulerProviderFactory] = {
    "external": ExternalSchedulerProvider,
    "slurm": SlurmSchedulerProvider,
}


def register_scheduler_provider(
    name: str,
    factory: SchedulerProviderFactory,
    *,
    replace: bool = False,
) -> None:
    """Register an additional scheduler provider factory."""
    normalized = _normalize_provider_name(name)
    if normalized in _PROVIDER_FACTORIES and not replace:
        raise ConfigurationError(f"scheduler provider is already registered: {normalized}")
    _PROVIDER_FACTORIES[normalized] = factory


def provider_for_scheduler(name: str | None) -> SchedulerProvider:
    """Return an explicitly selected scheduler provider."""
    if name is None or name.strip() == "":
        raise ConfigurationError(
            "scheduler provider must be explicit; configure external or a scheduler provider"
        )
    normalized = _normalize_provider_name(name)
    if normalized in {"external", "none", "unmanaged"}:
        normalized = "external"
    factory = _PROVIDER_FACTORIES.get(normalized)
    if factory is None:
        raise ConfigurationError(f"unsupported scheduler provider: {name}")
    provider = factory()
    if _normalize_provider_name(provider.name) != normalized:
        raise ConfigurationError(
            f"scheduler provider factory {normalized} returned provider {provider.name}"
        )
    return provider


def validation_provider_for_scheduler(name: str | None) -> SchedulerValidationProvider:
    """Return a provider that implements deterministic lifecycle validation operations."""
    provider = provider_for_scheduler(name)
    if not isinstance(provider, SchedulerValidationProvider):
        raise ConfigurationError(
            f"scheduler provider does not support lifecycle validation: {provider.name}"
        )
    return provider


def allocation_connector_provider_for_scheduler(
    name: str | None,
) -> SchedulerAllocationConnectorProvider:
    """Return a provider that can prove and enter one exact allocation placement."""
    provider = provider_for_scheduler(name)
    if not isinstance(provider, SchedulerAllocationConnectorProvider):
        raise ConfigurationError(
            f"scheduler provider does not support allocation connectors: {provider.name}"
        )
    return provider


def reconciliation_provider_for_scheduler(
    name: str | None,
) -> SchedulerReconciliationProvider:
    """Return a provider that can prove one interrupted submission by exact marker."""
    provider = provider_for_scheduler(name)
    if not isinstance(provider, SchedulerReconciliationProvider):
        raise ConfigurationError(
            f"scheduler provider does not support exact submission reconciliation: {provider.name}"
        )
    return provider


def _status_from_squeue_row(row: Sequence[str]) -> SchedulerStatus:
    raw_state = row[1]
    return SchedulerStatus(
        scheduler=SlurmSchedulerProvider.name,
        scheduler_job_id=row[0],
        phase=_phase_from_slurm_state(raw_state),
        raw_state=raw_state,
        reason=_empty_to_none(row[2]),
        partition=_empty_to_none(row[3]),
        qos=_empty_to_none(row[4]),
        user=_empty_to_none(row[5]),
        nodes=_optional_int(row[6]),
        cpus=_optional_int(row[7]),
        memory=_empty_to_none(row[8]),
        submit_time=_empty_to_none(row[9]),
        start_time=_empty_to_none(row[10]),
        elapsed=_empty_to_none(row[11]),
        time_limit=_empty_to_none(row[12]),
    )


def _status_from_sacct_row(scheduler_job_id: str, row: Sequence[str]) -> SchedulerStatus:
    raw_state = row[1].split()[0] if row[1] else None
    return SchedulerStatus(
        scheduler=SlurmSchedulerProvider.name,
        scheduler_job_id=scheduler_job_id,
        phase=_phase_from_slurm_state(raw_state),
        raw_state=raw_state,
        partition=_empty_to_none(row[2]),
        qos=_empty_to_none(row[3]),
        submit_time=_empty_to_none(row[4]),
        start_time=_empty_to_none(row[5]),
        elapsed=_empty_to_none(row[6]),
        nodes=_optional_int(row[7]),
        cpus=_optional_int(row[8]),
        memory=_empty_to_none(row[9]),
        queue_position_note="historical scheduler status from sacct",
    )


def _status_from_scontrol_record(
    scheduler_job_id: str,
    record: dict[str, str],
) -> SchedulerStatus:
    raw_state = record.get("JobState")
    user_id = _empty_to_none(record.get("UserId"))
    exit_code = _empty_to_none(record.get("ExitCode"))
    note = "historical scheduler status from scontrol"
    if exit_code is not None:
        note = f"{note}; ExitCode={exit_code}"
    return SchedulerStatus(
        scheduler=SlurmSchedulerProvider.name,
        scheduler_job_id=scheduler_job_id,
        phase=_phase_from_slurm_state(raw_state),
        raw_state=raw_state,
        reason=_empty_to_none(record.get("Reason")),
        partition=_empty_to_none(record.get("Partition")),
        qos=_empty_to_none(record.get("QOS")),
        user=user_id.split("(", 1)[0] if user_id is not None else None,
        nodes=_optional_int(record.get("NumNodes", "")),
        cpus=_optional_int(record.get("NumCPUs", "")),
        memory=_empty_to_none(record.get("MinMemoryNode")),
        submit_time=_empty_to_none(record.get("SubmitTime")),
        eligible_time=_empty_to_none(record.get("EligibleTime")),
        start_time=_empty_to_none(record.get("StartTime")),
        elapsed=_empty_to_none(record.get("RunTime")),
        time_limit=_empty_to_none(record.get("TimeLimit")),
        queue_position_note=note,
    )


def _with_queue_position(
    status: SchedulerStatus,
    pending_jobs: Sequence[Sequence[str]],
) -> SchedulerStatus:
    comparable = [
        row
        for row in pending_jobs
        if row[0] != status.scheduler_job_id
        and _empty_to_none(row[3]) == status.partition
        and _empty_to_none(row[4]) == status.qos
        and _sort_time(row[9]) <= _sort_time(status.submit_time)
    ]
    jobs_ahead = len(comparable)
    return status.model_copy(
        update={
            "jobs_ahead": jobs_ahead,
            "queue_position": jobs_ahead + 1,
            "queue_position_scope": "same partition and qos, earlier or equal submit time",
            "queue_position_note": (
                "approximate; SLURM scheduling is priority and backfill based, not FIFO"
            ),
        }
    )


def _phase_from_slurm_state(raw_state: str | None) -> SchedulerPhase:
    if raw_state is None:
        return SchedulerPhase.UNKNOWN
    normalized = raw_state.strip().upper().split()[0].rstrip("+")
    if normalized in {"PENDING", "PD", "REQUEUED", "RQ", "REQUEUE_HOLD", "RH"}:
        return SchedulerPhase.PENDING
    if normalized in {"CONFIGURING", "CF", "COMPLETING", "CG", "RESIZING", "RS"}:
        return SchedulerPhase.ALLOCATED
    if normalized in {"RUNNING", "R", "SUSPENDED", "S"}:
        return SchedulerPhase.RUNNING
    if normalized in {"COMPLETED", "CD"}:
        return SchedulerPhase.COMPLETED
    if normalized in {"CANCELLED", "CANCELED", "CA"}:
        return SchedulerPhase.CANCELED
    if normalized in {
        "BOOT_FAIL",
        "BF",
        "DEADLINE",
        "DL",
        "FAILED",
        "F",
        "NODE_FAIL",
        "NF",
        "OUT_OF_MEMORY",
        "OOM",
        "PREEMPTED",
        "PR",
        "REVOKED",
        "RV",
        "TIMEOUT",
        "TO",
    }:
        return SchedulerPhase.FAILED
    return SchedulerPhase.UNKNOWN


def _split_row(line: str, expected_fields: int) -> list[str] | None:
    row = [item.strip() for item in line.rstrip("\n").split("|")]
    if len(row) != expected_fields:
        return None
    return row


def _parse_scontrol_record(line: str) -> dict[str, str]:
    normalized = line.strip()
    matches = list(re.finditer(r"(?<!\S)([A-Za-z][A-Za-z0-9_:]*)=", normalized))
    record: dict[str, str] = {}
    for index, match in enumerate(matches):
        end = matches[index + 1].start() if index + 1 < len(matches) else len(normalized)
        record[match.group(1)] = normalized[match.end() : end].strip()
    return record


def _empty_to_none(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    if stripped in {"", "N/A", "Unknown", "None"}:
        return None
    return stripped


def _optional_int(value: str) -> int | None:
    if value.strip() == "":
        return None
    try:
        return int(value)
    except ValueError:
        return None


def _sort_time(value: str | None) -> str:
    return value or ""


_SCHEDULER_JOB_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:+-]*$")
_VALIDATION_JOB_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")
_SLURM_ALLOCATION_JOB_ID = re.compile(r"^[0-9]+(?:_[0-9]+)?(?:\+[0-9]+)?$")
_CONNECTOR_STEP_MARKER = re.compile(r"^clio-relay-connector-[a-f0-9]{32}$")


def _validate_scheduler_job_id(value: str) -> None:
    if not _SCHEDULER_JOB_ID.fullmatch(value):
        raise ConfigurationError(f"invalid scheduler job id: {value!r}")


def _validate_slurm_allocation_job_id(value: str) -> None:
    if _SLURM_ALLOCATION_JOB_ID.fullmatch(value) is None:
        raise ConfigurationError(f"invalid SLURM allocation job id: {value!r}")


def _validate_connector_step_id(scheduler_job_id: str, scheduler_step_id: str) -> None:
    prefix = f"{scheduler_job_id}."
    if not scheduler_step_id.startswith(prefix) or not scheduler_step_id[len(prefix) :].isdecimal():
        raise ConfigurationError(
            "SLURM connector step id must be an exact numeric step of its allocation"
        )


def _validate_connector_placement_host(value: str) -> None:
    if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,1023}", value) is None:
        raise ConfigurationError("SLURM connector placement host is invalid")


def _validate_connector_step_marker(value: str) -> None:
    if _CONNECTOR_STEP_MARKER.fullmatch(value) is None:
        raise ConfigurationError("SLURM connector step marker is invalid")


def _validate_connector_output_path(value: str) -> str:
    if "\x00" in value or "\n" in value or "\r" in value:
        raise ConfigurationError("connector output path contains forbidden characters")
    parsed = PurePosixPath(value)
    if not parsed.is_absolute() or ".." in parsed.parts or str(parsed) != value:
        raise ConfigurationError("connector output path must be normalized and absolute")
    if len(value.encode("utf-8")) > 4_096:
        raise ConfigurationError("connector output path exceeds the provider limit")
    return value


def _validate_connector_command(command: Sequence[str]) -> list[str]:
    rendered = list(command)
    if not rendered or len(rendered) > 128:
        raise ConfigurationError("connector command must contain between 1 and 128 arguments")
    encoded_size = 0
    for argument in rendered:
        if argument == "":
            raise ConfigurationError("connector command arguments must be non-empty strings")
        if "\x00" in argument or "\n" in argument or "\r" in argument:
            raise ConfigurationError("connector command contains forbidden characters")
        encoded_size += len(argument.encode("utf-8"))
    if encoded_size > 32 * 1024:
        raise ConfigurationError("connector command exceeds the provider limit")
    return rendered


def _read_connector_launcher_diagnostic(stream: IO[bytes]) -> str:
    """Return bounded private launcher diagnostics after the launcher has exited."""
    try:
        stream.seek(0)
        payload = stream.read(CONNECTOR_LAUNCHER_DIAGNOSTIC_BYTES + 1)
    except OSError:
        return ""
    bounded = payload[:CONNECTOR_LAUNCHER_DIAGNOSTIC_BYTES]
    return bounded.decode("utf-8", errors="replace").strip()


def _terminate_connector_launcher(process: subprocess.Popen[bytes]) -> None:
    """Boundedly terminate a detached launcher whose step identity was not proven."""
    if process.poll() is not None:
        return
    try:
        process.terminate()
        process.wait(timeout=2.0)
    except ProcessLookupError:
        return
    except subprocess.TimeoutExpired:
        process.kill()
        try:
            process.wait(timeout=2.0)
        except (ProcessLookupError, subprocess.TimeoutExpired):
            return


def _register_connector_launcher_for_reaping(process: subprocess.Popen[bytes]) -> None:
    """Retain and reap one successful detached launcher without persisting its PID."""
    global _connector_launcher_reaper_thread
    with _CONNECTOR_LAUNCHER_REAPER_LOCK:
        _CONNECTOR_LAUNCHERS.add(process)
        if _connector_launcher_reaper_thread is None:
            _connector_launcher_reaper_thread = threading.Thread(
                target=_reap_connector_launchers,
                name="clio-relay-srun-reaper",
                daemon=True,
            )
            _connector_launcher_reaper_thread.start()
    _CONNECTOR_LAUNCHER_REAPER_WAKE.set()


def _reap_connector_launchers() -> None:
    """Bound one daemon reaper to all provider-detached launcher processes."""
    while True:
        _CONNECTOR_LAUNCHER_REAPER_WAKE.wait(timeout=0.5)
        _CONNECTOR_LAUNCHER_REAPER_WAKE.clear()
        with _CONNECTOR_LAUNCHER_REAPER_LOCK:
            launchers = tuple(_CONNECTOR_LAUNCHERS)
        for launcher in launchers:
            if launcher.poll() is None:
                continue
            with _CONNECTOR_LAUNCHER_REAPER_LOCK:
                _CONNECTOR_LAUNCHERS.discard(launcher)


def _validate_validation_job_name(value: str) -> None:
    if not _VALIDATION_JOB_NAME.fullmatch(value):
        raise ConfigurationError(f"invalid scheduler validation job name: {value!r}")


def _validate_reconciliation_marker(value: str) -> None:
    if not value.startswith("clio-relay-") or not _VALIDATION_JOB_NAME.fullmatch(value):
        raise ConfigurationError(f"invalid scheduler reconciliation marker: {value!r}")


def _validate_scheduler_user(value: str) -> None:
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}", value):
        raise ConfigurationError(f"invalid scheduler reconciliation user: {value!r}")


def _validate_reconciliation_time(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ConfigurationError("scheduler reconciliation time must include a timezone")
    normalized = value.astimezone(UTC)
    now = datetime.now(UTC)
    if normalized > now + timedelta(minutes=5):
        raise ConfigurationError("scheduler reconciliation time is in the future")
    if normalized < now - SCHEDULER_RECONCILIATION_MAX_AGE:
        raise ConfigurationError("scheduler reconciliation intent exceeded its history window")
    return normalized


def _parse_slurm_reconciliation_time(value: str) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        local_timezone = datetime.now().astimezone().tzinfo
        parsed = parsed.replace(tzinfo=local_timezone)
    return parsed.astimezone(UTC)


def _normalize_provider_name(value: str) -> str:
    normalized = value.strip().lower().replace("_", "-")
    if not re.fullmatch(r"[a-z][a-z0-9-]*", normalized):
        raise ConfigurationError(f"invalid scheduler provider name: {value!r}")
    return normalized


def _run_scheduler_command(command: list[str]) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            command,
            text=True,
            capture_output=True,
            check=False,
            timeout=SCHEDULER_COMMAND_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired as exc:
        raise RelayError(
            f"scheduler provider command timed out after "
            f"{SCHEDULER_COMMAND_TIMEOUT_SECONDS:g}s: {command[0]}"
        ) from exc
    except OSError as exc:
        raise RelayError(f"scheduler provider command failed: {command[0]}: {exc}") from exc


def _slurm_job_absent_from_active_queue(
    result: subprocess.CompletedProcess[str],
) -> bool:
    """Recognize SLURM's nonzero response for a job no longer visible to ``squeue``."""
    return (
        result.returncode != 0
        and not result.stdout.strip()
        and "invalid job id specified" in result.stderr.casefold()
    )


def _scheduler_command_error(
    executable: str,
    result: subprocess.CompletedProcess[str],
) -> RelayError:
    detail = result.stderr.strip() or result.stdout.strip() or f"exit code {result.returncode}"
    return RelayError(f"scheduler provider command failed: {executable}: {detail}")

"""Scheduler observation for the worker (RL18), backed by ``psij-python``.

The worker polls and cancels scheduler jobs with the status and cancel
commands of psij's batch executors, so Slurm, PBS, LSF and Cobalt are covered
by the same code. Two things psij does not offer stay here, and only for
Slurm: the ``ClusterName`` read and the exact-name lookup that reconciles a
submission interrupted before its scheduler job id was recorded.
"""

from __future__ import annotations

import re
import subprocess
import threading
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Protocol, runtime_checkable

from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.models import SchedulerPhase, SchedulerStatus

if TYPE_CHECKING:
    from psij.executors.batch.batch_scheduler_executor import BatchSchedulerExecutor

PSIJ_SCHEDULERS = frozenset({"slurm", "pbs", "lsf", "cobalt"})
SCHEDULER_COMMAND_TIMEOUT_SECONDS = 15.0
SCHEDULER_RECONCILIATION_MAX_AGE = timedelta(days=7)
SCHEDULER_RECONCILIATION_TIME_TOLERANCE = timedelta(seconds=5)

_SCHEDULER_JOB_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:+\[\]-]*$")
_MARKER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")
_PSIJ_PHASES = {
    "NEW": SchedulerPhase.SUBMITTED,
    "QUEUED": SchedulerPhase.PENDING,
    "ACTIVE": SchedulerPhase.RUNNING,
    "COMPLETED": SchedulerPhase.COMPLETED,
    "FAILED": SchedulerPhase.FAILED,
    "CANCELED": SchedulerPhase.CANCELED,
}


class SchedulerProvider(Protocol):
    """Scheduler status, cancellation and target identity."""

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
class SchedulerReconciliationProvider(SchedulerProvider, Protocol):
    """Exact-marker lookup for interrupted scheduler submissions."""

    def find_job_ids_by_marker(
        self,
        marker: str,
        *,
        submitted_after: datetime,
        scheduler_user: str,
    ) -> list[str]:
        """Return scheduler ids whose job name exactly matches marker."""
        ...


def _validate_scheduler_job_id(value: str) -> None:
    if not _SCHEDULER_JOB_ID.fullmatch(value):
        raise ConfigurationError(f"invalid scheduler job id: {value!r}")


def _run(command: list[str]) -> subprocess.CompletedProcess[str]:
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
            f"scheduler command timed out after {SCHEDULER_COMMAND_TIMEOUT_SECONDS:g}s: "
            f"{command[0]}"
        ) from exc
    except OSError as exc:
        raise RelayError(f"scheduler command failed: {command[0]}: {exc}") from exc


def _command_error(result: subprocess.CompletedProcess[str]) -> RelayError:
    executable = str(result.args[0]) if isinstance(result.args, list) else str(result.args)
    detail = result.stderr.strip() or result.stdout.strip() or f"exit code {result.returncode}"
    return RelayError(f"scheduler command failed: {executable}: {detail}")


class ExternalSchedulerProvider:
    """Runtimes whose scheduler lifecycle is owned by something other than relay."""

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
        """Refuse cancellation when no relay-owned scheduler is configured."""
        _validate_scheduler_job_id(scheduler_job_id)
        return subprocess.CompletedProcess(
            ["external-scheduler", scheduler_job_id],
            2,
            "",
            "scheduler cancellation is owned by the deployment driver",
        )

    def scheduler_cluster_name(self) -> str | None:
        """Return no scheduler-native identity."""
        return None


_executors: dict[str, BatchSchedulerExecutor] = {}
_executors_lock = threading.Lock()


def psij_executor(scheduler: str) -> BatchSchedulerExecutor:
    """Return the shared psij batch executor for one scheduler name."""
    from psij import JobExecutor
    from psij.executors.batch.batch_scheduler_executor import (
        BatchSchedulerExecutor,
        BatchSchedulerExecutorConfig,
    )

    with _executors_lock:
        found = _executors.get(scheduler)
        if found is None:
            work_dir = Path.home() / ".psij" / "work" / "clio-relay-worker"
            config = BatchSchedulerExecutorConfig(work_directory=work_dir)
            created = JobExecutor.get_instance(scheduler, config=config)
            if not isinstance(created, BatchSchedulerExecutor):
                raise ConfigurationError(f"psij executor {scheduler!r} is not a batch executor")
            _executors[scheduler] = found = created
        return found


class PsijSchedulerProvider:
    """Poll and cancel through one psij batch executor."""

    def __init__(self, name: str) -> None:
        if name not in PSIJ_SCHEDULERS:
            raise ConfigurationError(f"unsupported scheduler provider: {name}")
        self.name = name

    def poll(self, scheduler_job_id: str) -> SchedulerStatus:
        """Poll one job with psij's status command."""
        _validate_scheduler_job_id(scheduler_job_id)
        executor = psij_executor(self.name)
        command = executor.get_status_command([scheduler_job_id])
        result = _run(list(command))
        try:
            found = executor.parse_status_output(result.returncode, result.stdout)
        except Exception as exc:
            if result.returncode != 0:
                raise _command_error(result) from exc
            raise RelayError(f"cannot read {command[0]} output: {exc}") from exc
        observed = found.get(scheduler_job_id)
        if observed is None:
            return SchedulerStatus(
                scheduler=self.name,
                scheduler_job_id=scheduler_job_id,
                phase=SchedulerPhase.UNKNOWN,
                active_record_found=False,
                queue_position_note=f"{command[0]} no longer lists the job",
            )
        return SchedulerStatus(
            scheduler=self.name,
            scheduler_job_id=scheduler_job_id,
            phase=_PSIJ_PHASES.get(observed.state.name, SchedulerPhase.UNKNOWN),
            record_found=True,
            active_record_found=not observed.state.final,
            raw_state=observed.state.name,
            reason=observed.message,
        )

    def cancel(self, scheduler_job_id: str) -> subprocess.CompletedProcess[str]:
        """Cancel one job with psij's cancel command."""
        _validate_scheduler_job_id(scheduler_job_id)
        return _run(list(psij_executor(self.name).get_cancel_command(scheduler_job_id)))

    def scheduler_cluster_name(self) -> str | None:
        """Return no scheduler-native cluster name; only Slurm has one relay reads."""
        return None


class SlurmSchedulerProvider(PsijSchedulerProvider):
    """Slurm through psij, plus ``ClusterName`` and exact-name reconciliation."""

    def __init__(self) -> None:
        super().__init__("slurm")

    def scheduler_cluster_name(self) -> str:
        """Read the configured Slurm ``ClusterName``."""
        result = _run(["scontrol", "show", "config"])
        if result.returncode != 0:
            raise _command_error(result)
        for line in result.stdout.splitlines():
            key, separator, value = line.partition("=")
            if separator and key.strip() == "ClusterName" and value.strip():
                return value.strip().split()[0]
        raise RelayError("scontrol show config omitted ClusterName")

    def find_job_ids_by_marker(
        self,
        marker: str,
        *,
        submitted_after: datetime,
        scheduler_user: str,
    ) -> list[str]:
        """Find current or recent Slurm jobs by exact name, user and time window."""
        if not marker.startswith("clio-relay-") or not _MARKER.fullmatch(marker):
            raise ConfigurationError(f"invalid scheduler reconciliation marker: {marker!r}")
        if not _MARKER.fullmatch(scheduler_user):
            raise ConfigurationError(f"invalid scheduler reconciliation user: {scheduler_user!r}")
        if submitted_after.tzinfo is None:
            raise ConfigurationError("scheduler reconciliation time must include a timezone")
        now = datetime.now(UTC)
        submitted_after = submitted_after.astimezone(UTC)
        if submitted_after < now - SCHEDULER_RECONCILIATION_MAX_AGE:
            raise ConfigurationError("scheduler reconciliation intent exceeded its history window")
        earliest = submitted_after - SCHEDULER_RECONCILIATION_TIME_TOLERANCE
        latest = now + SCHEDULER_RECONCILIATION_TIME_TOLERANCE
        local_start = earliest.astimezone().strftime("%Y-%m-%dT%H:%M:%S")
        queries = (
            ["squeue", "-h", "--name", marker, "--user", scheduler_user, "-o", "%A|%j|%u|%V"],
            [
                "sacct", "-n", "-P", "-X", "--name", marker, "--user", scheduler_user,
                "--starttime", local_start, "-o", "JobIDRaw,JobName,User,Submit",
            ],
        )  # fmt: skip
        matches: list[str] = []
        for command in queries:
            result = _run(command)
            if result.returncode != 0:
                raise _command_error(result)
            for line in result.stdout.splitlines():
                row = [part.strip() for part in line.split("|")]
                if len(row) != 4 or row[1] != marker or row[2] != scheduler_user:
                    continue
                submitted = _parse_time(row[3])
                if submitted is None or not earliest <= submitted <= latest:
                    continue
                if not row[0].isdecimal() or row[0] in matches:
                    continue
                matches.append(row[0])
        return matches


def _parse_time(value: str) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=datetime.now().astimezone().tzinfo)
    return parsed.astimezone(UTC)


def provider_for_scheduler(name: str | None) -> SchedulerProvider:
    """Return the provider for an explicitly named scheduler."""
    if name is None or not name.strip():
        raise ConfigurationError(
            "scheduler provider must be explicit; configure external or a psij scheduler"
        )
    normalized = name.strip().lower().replace("_", "-")
    if normalized in {"external", "none", "unmanaged"}:
        return ExternalSchedulerProvider()
    if normalized == "slurm":
        return SlurmSchedulerProvider()
    if normalized in PSIJ_SCHEDULERS:
        return PsijSchedulerProvider(normalized)
    raise ConfigurationError(f"unsupported scheduler provider: {name}")


def reconciliation_provider_for_scheduler(name: str | None) -> SchedulerReconciliationProvider:
    """Return a provider that can find one interrupted submission by exact marker."""
    provider = provider_for_scheduler(name)
    if not isinstance(provider, SchedulerReconciliationProvider):
        raise ConfigurationError(
            f"scheduler provider does not support exact submission reconciliation: {provider.name}"
        )
    return provider

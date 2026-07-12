"""Scheduler provider boundary for cluster job status and cancellation."""

from __future__ import annotations

import re
import subprocess
from collections.abc import Callable, Sequence
from datetime import UTC, datetime, timedelta
from typing import Protocol, runtime_checkable

from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.models import SchedulerPhase, SchedulerStatus

SQUEUE_FIELDS = "%i|%T|%R|%P|%q|%u|%D|%C|%m|%V|%S|%M|%l"
SACCT_FIELDS = "JobIDRaw,State,Partition,QOS,Submit,Start,Elapsed,NNodes,NCPUS,ReqMem"
SCHEDULER_PENDING_CHECK_ID = "scheduler.pending"
SCHEDULER_ALLOCATED_CHECK_ID = "scheduler.allocated"
SCHEDULER_RUNNING_CHECK_ID = "scheduler.running"
SCHEDULER_COMPLETED_CHECK_ID = "scheduler.completed"
SCHEDULER_RUNTIME_METADATA_CHECK_ID = "scheduler.structured-metadata"
SCHEDULER_COMMAND_TIMEOUT_SECONDS = 15.0
SCHEDULER_RECONCILIATION_MAX_AGE = timedelta(days=7)
SCHEDULER_RECONCILIATION_TIME_TOLERANCE = timedelta(seconds=5)


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

    def poll(self, scheduler_job_id: str) -> SchedulerStatus:
        """Poll SLURM, including clusters where accounting storage is disabled."""
        _validate_scheduler_job_id(scheduler_job_id)
        current = self._squeue_one(scheduler_job_id)
        if current is not None:
            status = _status_from_squeue_row(current).model_copy(update={"record_found": True})
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
                update={"record_found": True}
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
            ).model_copy(update={"record_found": True})
        diagnostic = "; ".join(history_errors)
        note = "scheduler job was not found by squeue, sacct, or scontrol"
        if diagnostic:
            note = f"{note}; {diagnostic}"
        return SchedulerStatus(
            scheduler=self.name,
            scheduler_job_id=scheduler_job_id,
            phase=SchedulerPhase.UNKNOWN,
            record_found=False if not history_errors else None,
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


def _validate_scheduler_job_id(value: str) -> None:
    if not _SCHEDULER_JOB_ID.fullmatch(value):
        raise ConfigurationError(f"invalid scheduler job id: {value!r}")


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


def _scheduler_command_error(
    executable: str,
    result: subprocess.CompletedProcess[str],
) -> RelayError:
    detail = result.stderr.strip() or result.stdout.strip() or f"exit code {result.returncode}"
    return RelayError(f"scheduler provider command failed: {executable}: {detail}")

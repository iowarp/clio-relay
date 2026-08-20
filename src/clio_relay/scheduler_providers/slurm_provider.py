"""SLURM scheduler provider: status polling, cancellation, and lifecycle validation.

Connector-step machinery (placement/launch/poll/cancel/reconcile) is mixed
in from ``slurm_connector.py`` via ``_SlurmConnectorMixin`` -- see that
module's docstring for why it is a separate owner file.
"""

from __future__ import annotations

import subprocess
from datetime import UTC, datetime

from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.models import SchedulerPhase, SchedulerStatus

from .command import (
    _run_scheduler_command,
    _scheduler_command_error,
    _slurm_job_absent_from_active_queue,
)
from .constants import SACCT_FIELDS, SCHEDULER_RECONCILIATION_TIME_TOLERANCE, SQUEUE_FIELDS
from .slurm_connector import _SlurmConnectorMixin
from .slurm_status import (
    _parse_scontrol_record,
    _split_row,
    _status_from_sacct_row,
    _status_from_scontrol_record,
    _status_from_squeue_row,
    _with_queue_position,
)
from .validation import (
    _parse_slurm_reconciliation_time,
    _validate_reconciliation_marker,
    _validate_reconciliation_time,
    _validate_scheduler_job_id,
    _validate_scheduler_user,
    _validate_validation_job_name,
)


class SlurmSchedulerProvider(_SlurmConnectorMixin):
    """SLURM provider backed by squeue, controller/accounting history, and scancel."""

    name = "slurm"

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

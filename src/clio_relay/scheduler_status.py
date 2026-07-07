"""Scheduler status polling helpers."""

from __future__ import annotations

import subprocess
from collections.abc import Sequence

from clio_relay.core_queue import ClioCoreQueue
from clio_relay.models import JobState, RelayJob, SchedulerPhase, SchedulerStatus

SQUEUE_FIELDS = "%i|%T|%R|%P|%q|%u|%D|%C|%m|%V|%S|%M|%l"
SACCT_FIELDS = "JobIDRaw,State,Partition,QOS,Submit,Start,Elapsed,NNodes,NCPUS,ReqMem"


def relay_queue_status(queue: ClioCoreQueue, job: RelayJob) -> dict[str, object]:
    """Return relay-level queue position for a job."""
    if job.state != JobState.QUEUED:
        return {"state": job.state.value, "jobs_ahead": None, "position": None}
    jobs_ahead = 0
    for candidate in queue.list_jobs():
        if candidate.job_id == job.job_id:
            break
        if candidate.cluster == job.cluster and candidate.state == JobState.QUEUED:
            jobs_ahead += 1
    return {"state": job.state.value, "jobs_ahead": jobs_ahead, "position": jobs_ahead + 1}


def poll_slurm_status(scheduler_job_id: str) -> SchedulerStatus:
    """Poll SLURM for a job status, using sacct after squeue no longer sees the job."""
    current = _squeue_one(scheduler_job_id)
    if current is not None:
        status = _status_from_squeue_row(current)
        if status.phase == SchedulerPhase.PENDING:
            return _with_queue_position(status, _squeue_pending_jobs())
        return status
    historical = _sacct_one(scheduler_job_id)
    if historical is not None:
        return _status_from_sacct_row(scheduler_job_id, historical)
    return SchedulerStatus(
        scheduler_job_id=scheduler_job_id,
        phase=SchedulerPhase.UNKNOWN,
        queue_position_note="scheduler job was not found by squeue or sacct",
    )


def _squeue_one(scheduler_job_id: str) -> list[str] | None:
    result = subprocess.run(
        ["squeue", "-h", "-j", scheduler_job_id, "-o", SQUEUE_FIELDS],
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        return None
    for line in result.stdout.splitlines():
        row = _split_row(line, 13)
        if row and row[0] == scheduler_job_id:
            return row
    return None


def _squeue_pending_jobs() -> list[list[str]]:
    result = subprocess.run(
        ["squeue", "-h", "-t", "PD", "-o", SQUEUE_FIELDS],
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        return []
    return [row for line in result.stdout.splitlines() if (row := _split_row(line, 13))]


def _sacct_one(scheduler_job_id: str) -> list[str] | None:
    result = subprocess.run(
        [
            "sacct",
            "-n",
            "-P",
            "-j",
            scheduler_job_id,
            "-o",
            SACCT_FIELDS,
        ],
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        return None
    for line in result.stdout.splitlines():
        row = _split_row(line, 10)
        if row and row[0] == scheduler_job_id:
            return row
    return None


def _status_from_squeue_row(row: Sequence[str]) -> SchedulerStatus:
    raw_state = row[1]
    return SchedulerStatus(
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
    normalized = raw_state.strip().upper()
    if normalized in {"PENDING", "PD"}:
        return SchedulerPhase.PENDING
    if normalized in {"CONFIGURING", "CF", "COMPLETING", "CG", "RESIZING", "RS"}:
        return SchedulerPhase.ALLOCATED
    if normalized in {"RUNNING", "R"}:
        return SchedulerPhase.RUNNING
    if normalized in {"COMPLETED", "CD"}:
        return SchedulerPhase.COMPLETED
    if normalized in {"CANCELLED", "CA"}:
        return SchedulerPhase.CANCELED
    if normalized in {"FAILED", "F", "TIMEOUT", "TO", "NODE_FAIL", "NF", "OUT_OF_MEMORY", "OOM"}:
        return SchedulerPhase.FAILED
    return SchedulerPhase.UNKNOWN


def _split_row(line: str, expected_fields: int) -> list[str] | None:
    row = [item.strip() for item in line.rstrip("\n").split("|")]
    if len(row) != expected_fields:
        return None
    return row


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

"""Turn raw squeue/sacct/scontrol text into typed SLURM status objects.

This module has no dependency on ``slurm_provider.py`` on purpose (that
module depends on this one, for row-to-status conversion during ``poll()``);
a reverse edge would be a real import cycle. The scheduler name these
functions stamp is therefore the literal ``"slurm"`` string rather than a
``SlurmSchedulerProvider.name`` class-attribute reference -- the two are
always equal (no subclass overrides ``name``), so this is a structural
consequence of breaking the cycle, not a behavior change.
"""

from __future__ import annotations

import re
from collections.abc import Sequence

from clio_relay.models import SchedulerPhase, SchedulerStatus

_SLURM_PROVIDER_NAME = "slurm"


def _status_from_squeue_row(row: Sequence[str]) -> SchedulerStatus:
    raw_state = row[1]
    return SchedulerStatus(
        scheduler=_SLURM_PROVIDER_NAME,
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
        scheduler=_SLURM_PROVIDER_NAME,
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
        scheduler=_SLURM_PROVIDER_NAME,
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

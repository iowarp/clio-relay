"""Raw cluster submission order, job listing, and lease-record selection.

``list_queue_jobs`` is the public page-through-the-queue entry point.
``_raw_queue_evidence``/``_raw_submission_payload`` compute a job's position
in its cluster's raw FIFO submission order (the truth before any admission
policy is applied) and are reused by both job listing and the admission
simulation in ``queue_admission_simulation``. ``_leases_by_job`` picks the
single most-relevant lease record per job when a scan returns more than one.
"""

from __future__ import annotations

from datetime import datetime

from clio_relay.core_queue import ClioCoreQueue
from clio_relay.errors import ConfigurationError
from clio_relay.models import JobKind, JobState, Lease, RelayJob, utc_now
from clio_relay.queue_diagnosis_constants import (
    DEFAULT_RESULT_LIMIT,
    DEFAULT_SCAN_LIMIT,
    MAX_SCAN_LIMIT,
    _validate_bounds,  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)


def list_queue_jobs(
    queue: ClioCoreQueue,
    *,
    cluster: str | None = None,
    state: JobState | None = None,
    kind: JobKind | None = None,
    include_terminal: bool = False,
    cursor: int = 1,
    limit: int = DEFAULT_RESULT_LIMIT,
    scan_limit: int = DEFAULT_SCAN_LIMIT,
) -> dict[str, object]:
    """List one stable job source window with bounded queue-position evidence."""
    _validate_bounds(limit=limit, scan_limit=scan_limit)
    selected, next_cursor, total = queue.list_jobs_page(
        cursor=cursor,
        limit=limit,
        cluster=cluster,
        state=state,
        kind=kind,
        include_terminal=include_terminal or state is not None,
    )
    active_jobs: list[RelayJob] = []
    if any(job.state is JobState.QUEUED for job in selected):
        active_jobs, active_truncated = queue.scan_active_jobs(limit=scan_limit)
        if active_truncated:
            raise ConfigurationError(
                "queue-position discovery exceeded scan_limit; increase scan_limit up to "
                f"{MAX_SCAN_LIMIT} or reduce active queue retention"
            )
    return {
        "jobs": [_job_summary(job, active_jobs) for job in selected],
        "count": len(selected),
        "cluster": cluster,
        "state": None if state is None else state.value,
        "kind": None if kind is None else kind.value,
        "include_terminal": include_terminal,
        "source_cursor": cursor,
        "source_limit": limit,
        "source_next_cursor": next_cursor,
        "source_total": total,
        "source_total_semantics": "global_submission_sequence_high_water",
        "filters_apply_within_source_window": True,
        "result_truncated": next_cursor is not None,
        "scan_limit": scan_limit,
        "scan_truncated": False,
        "active_job_capacity": queue.active_job_capacity(),
    }


def _raw_queue_evidence(
    job: RelayJob,
    jobs: list[RelayJob],
    *,
    scan_truncated: bool,
) -> dict[str, object]:
    if job.state != JobState.QUEUED:
        return {
            "state": job.state.value,
            "jobs_ahead": None,
            "position": None,
            "raw_preceding_job_ids": [],
            "raw_preceding_job_ids_truncated": False,
            "scan_truncated": scan_truncated,
            "position_exact": True,
        }
    ordered_cluster_jobs = [
        candidate
        for candidate in jobs
        if candidate.cluster == job.cluster and candidate.state == JobState.QUEUED
    ]
    target_index = next(
        (
            index
            for index, candidate in enumerate(ordered_cluster_jobs)
            if candidate.job_id == job.job_id
        ),
        None,
    )
    if target_index is None:
        return {
            "state": job.state.value,
            "jobs_ahead": None,
            "position": None,
            "raw_preceding_job_ids": [],
            "raw_preceding_job_ids_truncated": False,
            "scan_truncated": scan_truncated,
            "position_exact": False,
        }
    preceding = [candidate.job_id for candidate in ordered_cluster_jobs[:target_index]]
    return {
        "state": job.state.value,
        "jobs_ahead": len(preceding),
        "position": len(preceding) + 1,
        "raw_preceding_job_ids": preceding[:20],
        "raw_preceding_job_ids_truncated": len(preceding) > 20,
        "scan_truncated": scan_truncated,
        "position_exact": not scan_truncated,
    }


def _raw_submission_payload(  # pyright: ignore[reportUnusedFunction]
    raw: dict[str, object],
) -> dict[str, object]:
    return {
        "jobs_ahead": raw["jobs_ahead"],
        "position": raw["position"],
        "preceding_job_ids": raw["raw_preceding_job_ids"],
        "preceding_job_ids_truncated": raw["raw_preceding_job_ids_truncated"],
        "scan_truncated": raw["scan_truncated"],
        "position_exact": raw["position_exact"],
        "semantics": "raw_cluster_submission_order",
    }


def _job_summary(job: RelayJob, jobs: list[RelayJob]) -> dict[str, object]:
    queue_evidence = _raw_queue_evidence(job, jobs, scan_truncated=False)
    return {
        "job": job.model_dump(mode="json"),
        "relay_queue": {
            "state": queue_evidence["state"],
            "jobs_ahead": queue_evidence["jobs_ahead"],
            "position": queue_evidence["position"],
        },
    }


def _leases_by_job(  # pyright: ignore[reportUnusedFunction]
    leases: list[Lease],
) -> dict[str, Lease]:
    result: dict[str, Lease] = {}
    now = utc_now()
    for lease in leases:
        existing = result.get(lease.job_id)
        if existing is None:
            result[lease.job_id] = lease
            continue
        if _lease_sort_key(lease, now) > _lease_sort_key(existing, now):
            result[lease.job_id] = lease
    return result


def _lease_sort_key(lease: Lease, now: datetime) -> tuple[int, datetime]:
    return (0 if lease.is_expired(now) else 1, lease.expires_at)

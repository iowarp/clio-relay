"""Stale-job discovery, bounded recovery, and explicit job cancellation.

``discover_stale_jobs`` runs the same per-job diagnosis engine as
``diagnose_job`` (``queue_diagnosis._diagnose_job``) across an operator-scoped
set of active jobs and returns only the ones it classifies stale, refusing to
answer at all if any part of the underlying scan was truncated (an
incomplete classification is worse than none). ``cleanup_stale_jobs`` plans
-- and, outside dry-run, executes -- the bounded recovery actions that follow
from that discovery, always through relay-only scheduler cancellation.
``cancel_queue_job`` is the single relay job cancellation primitive both
``cleanup_stale_jobs`` and every other caller (CLI, HTTP, MCP) use.
"""

from __future__ import annotations

from datetime import datetime
from typing import cast

from clio_relay.core_queue import ClioCoreQueue
from clio_relay.errors import ConfigurationError
from clio_relay.models import JobKind, JobState, Lease, RelayJob, utc_now
from clio_relay.queue_admission_snapshot import (
    _admission_snapshot,  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
from clio_relay.queue_diagnosis import (
    _diagnose_job,  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
from clio_relay.queue_diagnosis_constants import (
    ACTIVE_STATES,
    DEFAULT_RESULT_LIMIT,
    DEFAULT_STALE_AFTER_SECONDS,
    DEFAULT_STALE_SCAN_LIMIT,
    DEFAULT_WORKER_FRESH_SECONDS,
    QueueCancelPolicy,
    _require_job_cluster,  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    _validate_bounds,  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    _validate_stale_after,  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)


def discover_stale_jobs(
    queue: ClioCoreQueue,
    *,
    cluster: str,
    older_than_seconds: int,
    job_id: str | None = None,
    kind: JobKind | None = None,
    limit: int = DEFAULT_RESULT_LIMIT,
    scan_limit: int = DEFAULT_STALE_SCAN_LIMIT,
) -> dict[str, object]:
    """Discover stale active jobs using an explicit operator age threshold."""
    queue.reconcile_pending_transitions()
    _validate_stale_after(older_than_seconds)
    _validate_bounds(limit=limit, scan_limit=scan_limit)
    scanned_jobs, scan_truncated = queue.scan_active_jobs(limit=scan_limit)
    if job_id is not None:
        exact = queue.get_job(job_id)
        _require_job_cluster(exact, cluster)
        if kind is not None and exact.kind != kind:
            raise ConfigurationError(
                f"job {job_id} has kind {exact.kind.value}, not requested kind {kind.value}"
            )
        jobs = [exact] if exact.state in ACTIVE_STATES else []
    else:
        jobs = [
            job
            for job in scanned_jobs
            if job.cluster == cluster
            and job.state in ACTIVE_STATES
            and (kind is None or job.kind == kind)
        ]
    now = utc_now()
    endpoints, endpoints_truncated = queue.scan_fresh_endpoints(
        limit=scan_limit,
        cluster=cluster,
        fresh_seconds=DEFAULT_WORKER_FRESH_SECONDS,
        now=now,
    )
    admission_snapshot = _admission_snapshot(
        queue,
        cluster=cluster,
        endpoints=endpoints,
        endpoint_scan_truncated=endpoints_truncated,
        scan_limit=scan_limit,
        now=now,
    )
    stale: list[dict[str, object]] = []
    lease_scan_truncated_job_ids: list[str] = []
    lease_records_by_job: dict[str, list[Lease]] = {}
    for job in jobs:
        job_leases, job_leases_truncated = queue.scan_job_leases(
            job.job_id,
            limit=scan_limit,
        )
        if job_leases_truncated:
            lease_scan_truncated_job_ids.append(job.job_id)
            continue
        lease_records_by_job[job.job_id] = job_leases
    classification_complete = (
        not scan_truncated and not endpoints_truncated and not lease_scan_truncated_job_ids
    )
    if not classification_complete:
        return {
            "cluster": cluster,
            "job_id": job_id,
            "kind": None if kind is None else kind.value,
            "older_than_seconds": older_than_seconds,
            "jobs": [],
            "count": 0,
            "matched_count": 0,
            "limit": limit,
            "result_truncated": False,
            "scan_limit": scan_limit,
            "scan_truncated": True,
            "active_scan_truncated": scan_truncated,
            "endpoint_scan_truncated": endpoints_truncated,
            "lease_scan_truncated": bool(lease_scan_truncated_job_ids),
            "lease_scan_truncated_job_ids": lease_scan_truncated_job_ids,
            "classification_complete": False,
            "unclassified_job_ids": [job.job_id for job in jobs],
            "active_job_capacity": queue.active_job_capacity(),
            "generated_at": now.isoformat(),
        }
    for job in jobs:
        diagnosis = _diagnose_job(
            queue,
            job,
            jobs=scanned_jobs,
            scan_truncated=scan_truncated,
            leases=lease_records_by_job[job.job_id],
            endpoints=endpoints,
            admission_snapshot=admission_snapshot,
            related_records_truncated=admission_snapshot.lease_scan_truncated,
            stale_after_seconds=older_than_seconds,
            now=now,
        )
        if diagnosis["stale"] is not True:
            continue
        stale.append(diagnosis)
    matched_count = len(stale)
    return {
        "cluster": cluster,
        "job_id": job_id,
        "kind": None if kind is None else kind.value,
        "older_than_seconds": older_than_seconds,
        "jobs": stale[:limit],
        "count": min(matched_count, limit),
        "matched_count": matched_count,
        "limit": limit,
        "result_truncated": matched_count > limit,
        "scan_limit": scan_limit,
        "scan_truncated": False,
        "active_scan_truncated": False,
        "endpoint_scan_truncated": False,
        "lease_scan_truncated": False,
        "lease_scan_truncated_job_ids": [],
        "classification_complete": True,
        "unclassified_job_ids": [],
        "active_job_capacity": queue.active_job_capacity(),
        "generated_at": now.isoformat(),
    }


def cleanup_stale_jobs(
    queue: ClioCoreQueue,
    *,
    cluster: str,
    older_than_seconds: int = DEFAULT_STALE_AFTER_SECONDS,
    job_id: str | None = None,
    kind: JobKind | None = None,
    max_attempts: int = 3,
    dry_run: bool = True,
    cancel_queued: bool = False,
    limit: int = DEFAULT_RESULT_LIMIT,
    scan_limit: int = DEFAULT_STALE_SCAN_LIMIT,
) -> dict[str, object]:
    """Preview or execute bounded stale recovery without scheduler cancellation."""
    if max_attempts < 1:
        raise ValueError("max_attempts must be at least 1")
    discovery = discover_stale_jobs(
        queue,
        cluster=cluster,
        older_than_seconds=older_than_seconds,
        job_id=job_id,
        kind=kind,
        limit=limit,
        scan_limit=scan_limit,
    )
    stale_jobs = cast(list[dict[str, object]], discovery["jobs"])
    planned: list[dict[str, object]] = []
    for diagnosis in stale_jobs:
        job = cast(dict[str, object], diagnosis["job"])
        state = job.get("state")
        reason = diagnosis.get("reason")
        action = "none"
        if state == JobState.QUEUED.value and cancel_queued:
            action = "cancel_queued_relay_job"
        elif reason == "stale_lease":
            action = "recover_expired_lease"
        elif state in {JobState.LEASED.value, JobState.RUNNING.value} and reason in {
            "stale_ownership",
            "stale_lease_scheduler_active",
            "scheduler_terminal_relay_nonterminal",
            "no_recent_progress",
        }:
            action = "cancel_stale_relay_job"
        planned.append(
            {
                "job_id": job.get("job_id"),
                "state": state,
                "expected_updated_at": job.get("updated_at"),
                "reason": reason,
                "action": action,
                "scheduler_policy": "relay-only",
            }
        )
    if dry_run:
        return {
            "cluster": cluster,
            "job_id": job_id,
            "dry_run": True,
            "older_than_seconds": older_than_seconds,
            "cancel_queued": cancel_queued,
            "planned": planned,
            "recoverable": stale_jobs,
            "recovered": [],
            "recovered_count": 0,
            "canceled": [],
            "canceled_count": 0,
            "conflicts": [],
            "conflict_count": 0,
            "scheduler_cancel_requested": False,
            "scan_truncated": discovery["scan_truncated"],
            "classification_complete": discovery["classification_complete"],
            "mutation_blocked_by_incomplete_scan": (
                discovery["classification_complete"] is not True
            ),
            "active_scan_truncated": discovery["active_scan_truncated"],
            "endpoint_scan_truncated": discovery["endpoint_scan_truncated"],
            "lease_scan_truncated": discovery["lease_scan_truncated"],
            "lease_scan_truncated_job_ids": discovery["lease_scan_truncated_job_ids"],
            "unclassified_job_ids": discovery["unclassified_job_ids"],
            "result_truncated": discovery["result_truncated"],
        }
    recovered: list[RelayJob] = []
    canceled: list[dict[str, object]] = []
    conflicts: list[dict[str, object]] = []
    for action in planned:
        candidate_job_id = action.get("job_id")
        if not isinstance(candidate_job_id, str):
            continue
        if action["action"] in {
            "cancel_queued_relay_job",
            "cancel_stale_relay_job",
        }:
            expected_state_value = action.get("state")
            expected_updated_at_value = action.get("expected_updated_at")
            expected_state = (
                JobState(expected_state_value) if isinstance(expected_state_value, str) else None
            )
            expected_updated_at = (
                datetime.fromisoformat(expected_updated_at_value)
                if isinstance(expected_updated_at_value, str)
                else None
            )
            result = cancel_queue_job(
                queue,
                candidate_job_id,
                cluster=cluster,
                scheduler_policy="relay-only",
                expected_state=expected_state,
                expected_updated_at=expected_updated_at,
            )
            if result.get("cancellation_requested") is True:
                resulting_job = cast(dict[str, object], result["job"])
                if resulting_job.get("state") == JobState.CANCELED.value:
                    canceled.append(resulting_job)
                else:
                    conflicts.append(
                        {
                            "job_id": candidate_job_id,
                            "status": "cleanup_pending",
                            "expected_state": expected_state_value,
                            "expected_updated_at": expected_updated_at_value,
                            "observed_job": resulting_job,
                        }
                    )
            else:
                conflicts.append(
                    {
                        "job_id": candidate_job_id,
                        "expected_state": expected_state_value,
                        "expected_updated_at": expected_updated_at_value,
                        "observed_job": result["job"],
                    }
                )
        elif action["action"] == "recover_expired_lease":
            updated = queue.recover_stale_job(
                candidate_job_id,
                cluster=cluster,
                max_attempts=max_attempts,
            )
            if updated is not None:
                recovered.append(updated)
    return {
        "cluster": cluster,
        "job_id": job_id,
        "dry_run": False,
        "older_than_seconds": older_than_seconds,
        "cancel_queued": cancel_queued,
        "planned": planned,
        "recoverable": stale_jobs,
        "recovered": [job.model_dump(mode="json") for job in recovered],
        "recovered_count": len(recovered),
        "canceled": canceled,
        "canceled_count": len(canceled),
        "conflicts": conflicts,
        "conflict_count": len(conflicts),
        "scheduler_cancel_requested": False,
        "scan_truncated": discovery["scan_truncated"],
        "classification_complete": discovery["classification_complete"],
        "mutation_blocked_by_incomplete_scan": (discovery["classification_complete"] is not True),
        "active_scan_truncated": discovery["active_scan_truncated"],
        "endpoint_scan_truncated": discovery["endpoint_scan_truncated"],
        "lease_scan_truncated": discovery["lease_scan_truncated"],
        "lease_scan_truncated_job_ids": discovery["lease_scan_truncated_job_ids"],
        "unclassified_job_ids": discovery["unclassified_job_ids"],
        "result_truncated": discovery["result_truncated"],
    }


def cancel_queue_job(
    queue: ClioCoreQueue,
    job_id: str,
    *,
    cluster: str | None = None,
    scheduler_policy: QueueCancelPolicy = "relay-only",
    expected_state: JobState | None = None,
    expected_updated_at: datetime | None = None,
) -> dict[str, object]:
    """Cancel a relay job with explicit scheduler cancellation policy."""
    if scheduler_policy not in {"relay-only", "request-scheduler"}:
        raise ValueError("scheduler_policy must be relay-only or request-scheduler")
    existing = queue.get_job(job_id)
    _require_job_cluster(existing, cluster)
    cancel_scheduler = scheduler_policy == "request-scheduler"
    job, requested = queue.cancel_job_if_active(
        job_id,
        cancel_scheduler=cancel_scheduler,
        expected_state=expected_state,
        expected_updated_at=expected_updated_at,
    )
    return {
        "job": job.model_dump(mode="json"),
        "scheduler_policy": scheduler_policy,
        "scheduler_cancel_requested": cancel_scheduler,
        "cancellation_requested": requested,
        "cancellation_acknowledged": job.state is JobState.CANCELED,
        "state_transitioned": requested and job.state is JobState.CANCELED,
    }

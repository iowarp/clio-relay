"""Queue management operations shared by CLI, HTTP, and MCP surfaces."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Literal, cast

from clio_relay.core_queue import ClioCoreQueue
from clio_relay.errors import ConfigurationError, NotFoundError
from clio_relay.models import (
    TERMINAL_STATES,
    EndpointRegistration,
    EndpointRole,
    JobKind,
    JobState,
    Lease,
    RelayJob,
    utc_now,
)
from clio_relay.relay_ops import scheduler_status_for_job
from clio_relay.worker_concurrency import kind_concurrency_metadata

QueueCancelPolicy = Literal["relay-only", "request-scheduler"]


ACTIVE_STATES = {JobState.QUEUED, JobState.LEASED, JobState.RUNNING}
DEFAULT_STALE_AFTER_SECONDS = 2 * 60 * 60
DEFAULT_RESULT_LIMIT = 100
DEFAULT_SCAN_LIMIT = 1_000
MAX_RESULT_LIMIT = 500
MAX_SCAN_LIMIT = 10_000
DEFAULT_WORKER_FRESH_SECONDS = 60


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


def diagnose_job(
    queue: ClioCoreQueue,
    job_id: str,
    *,
    cluster: str | None = None,
    stale_after_seconds: int = DEFAULT_STALE_AFTER_SECONDS,
    scan_limit: int = DEFAULT_SCAN_LIMIT,
) -> dict[str, object]:
    """Explain why one exact relay job is or is not progressing."""
    queue.reconcile_pending_transitions()
    _validate_stale_after(stale_after_seconds)
    _validate_bounds(limit=1, scan_limit=scan_limit)
    job = queue.get_job(job_id)
    _require_job_cluster(job, cluster)
    jobs, scan_truncated = queue.scan_active_jobs(limit=MAX_SCAN_LIMIT)
    leases, leases_truncated = queue.scan_job_leases(job.job_id, limit=20)
    endpoints, endpoints_truncated = queue.scan_fresh_endpoints(
        limit=scan_limit,
        cluster=job.cluster,
        fresh_seconds=DEFAULT_WORKER_FRESH_SECONDS,
    )
    return _diagnose_job(
        queue,
        job,
        jobs=jobs,
        scan_truncated=scan_truncated,
        leases=leases,
        endpoints=endpoints,
        related_records_truncated=leases_truncated or endpoints_truncated,
        stale_after_seconds=stale_after_seconds,
        now=utc_now(),
    )


def diagnose_queue(
    queue: ClioCoreQueue,
    *,
    cluster: str | None = None,
    stale_after_seconds: int = DEFAULT_STALE_AFTER_SECONDS,
    limit: int = DEFAULT_RESULT_LIMIT,
    scan_limit: int = DEFAULT_SCAN_LIMIT,
) -> dict[str, object]:
    """Return a bounded compatibility summary of suspicious relay state."""
    queue.reconcile_pending_transitions()
    _validate_stale_after(stale_after_seconds)
    _validate_bounds(limit=limit, scan_limit=scan_limit)
    jobs, scan_truncated = queue.scan_active_jobs(limit=MAX_SCAN_LIMIT)
    if cluster is not None:
        jobs = [job for job in jobs if job.cluster == cluster]
    leases_by_job: dict[str, Lease] = {}
    lease_scan_truncated_job_ids: list[str] = []
    for job in jobs:
        if job.state == JobState.QUEUED:
            continue
        job_leases, job_leases_truncated = queue.scan_job_leases(
            job.job_id,
            limit=scan_limit,
        )
        if job_leases_truncated:
            lease_scan_truncated_job_ids.append(job.job_id)
            continue
        lease = _leases_by_job(job_leases).get(job.job_id)
        if lease is not None:
            leases_by_job[job.job_id] = lease
    issues: list[dict[str, object]] = []
    for job in jobs:
        if job.state == JobState.QUEUED:
            continue
        if job.state == JobState.LEASED:
            if job.job_id in lease_scan_truncated_job_ids:
                continue
            lease = leases_by_job.get(job.job_id)
            if lease is None:
                issues.append(
                    _issue(
                        job,
                        code="leased_without_lease",
                        severity="error",
                        message="Job is leased but has no durable lease record.",
                    )
                )
            elif lease.is_expired():
                issues.append(
                    _issue(
                        job,
                        code="expired_lease",
                        severity="warning",
                        message="Job lease is expired and can be recovered.",
                        lease=lease,
                    )
                )
            if len(issues) >= limit:
                break
            continue
        if job.state == JobState.RUNNING:
            if job.job_id in lease_scan_truncated_job_ids:
                continue
            lease = leases_by_job.get(job.job_id)
            if lease is None:
                issues.append(
                    _issue(
                        job,
                        code="running_without_lease",
                        severity="warning",
                        message=(
                            "Job is running without a durable lease; the worker may have exited "
                            "after launching work."
                        ),
                    )
                )
            elif lease.is_expired():
                issues.append(
                    _issue(
                        job,
                        code="running_expired_lease",
                        severity="warning",
                        message="Running job has an expired lease.",
                        lease=lease,
                    )
                )
        if len(issues) >= limit:
            break
    return {
        "cluster": cluster,
        "checked_jobs": len(jobs),
        "issues": issues,
        "issue_count": len(issues),
        "limit": limit,
        "result_truncated": len(issues) >= limit,
        "scan_limit": scan_limit,
        "scan_truncated": scan_truncated,
        "lease_scan_truncated": bool(lease_scan_truncated_job_ids),
        "lease_scan_truncated_job_ids": lease_scan_truncated_job_ids,
        "stale_after_seconds": stale_after_seconds,
        "active_job_capacity": queue.active_job_capacity(),
        "generated_at": utc_now().isoformat(),
    }


def discover_stale_jobs(
    queue: ClioCoreQueue,
    *,
    cluster: str,
    older_than_seconds: int,
    job_id: str | None = None,
    kind: JobKind | None = None,
    limit: int = DEFAULT_RESULT_LIMIT,
    scan_limit: int = DEFAULT_SCAN_LIMIT,
) -> dict[str, object]:
    """Discover stale active jobs using an explicit operator age threshold."""
    queue.reconcile_pending_transitions()
    _validate_stale_after(older_than_seconds)
    _validate_bounds(limit=limit, scan_limit=scan_limit)
    scanned_jobs, scan_truncated = queue.scan_active_jobs(limit=MAX_SCAN_LIMIT)
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
    endpoints, endpoints_truncated = queue.scan_fresh_endpoints(
        limit=scan_limit,
        cluster=cluster,
        fresh_seconds=DEFAULT_WORKER_FRESH_SECONDS,
    )
    now = utc_now()
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
            related_records_truncated=False,
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
    scan_limit: int = DEFAULT_SCAN_LIMIT,
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


def _diagnose_job(
    queue: ClioCoreQueue,
    job: RelayJob,
    *,
    jobs: list[RelayJob],
    scan_truncated: bool,
    leases: list[Lease],
    endpoints: list[EndpointRegistration],
    related_records_truncated: bool,
    stale_after_seconds: int,
    now: datetime,
) -> dict[str, object]:
    job_leases = [lease for lease in leases if lease.job_id == job.job_id]
    lease = _leases_by_job(job_leases).get(job.job_id)
    owner_id = lease.endpoint_id if lease is not None else job.leased_by
    owner = queue.get_endpoint(owner_id) if owner_id is not None else None
    healthy_workers = [
        endpoint
        for endpoint in endpoints
        if endpoint.role == EndpointRole.WORKER
        and now - endpoint.last_seen_at <= timedelta(seconds=DEFAULT_WORKER_FRESH_SECONDS)
    ]
    last_event, events_truncated = queue.latest_job_event(job.job_id)
    last_progress, progress_count, progress_truncated = queue.latest_job_progress(job.job_id)
    activity_times = [job.updated_at]
    if last_event is not None:
        activity_times.append(last_event.created_at)
    if last_progress is not None:
        activity_times.append(last_progress.created_at)
    last_activity_at = max(activity_times)
    age_seconds = max(0.0, (now - job.created_at).total_seconds())
    inactivity_seconds = max(0.0, (now - last_activity_at).total_seconds())
    queue_evidence = _queue_evidence(job, jobs, scan_truncated=scan_truncated)
    scheduler = scheduler_status_for_job(queue, job.job_id, limit=20)
    scheduler_phases = _scheduler_phases(scheduler)
    lease_expired = lease is not None and lease.is_expired(now)
    owner_heartbeat_age = (
        max(0.0, (now - owner.last_seen_at).total_seconds()) if owner is not None else None
    )
    owner_stale = owner is None or (
        owner_heartbeat_age is not None and owner_heartbeat_age >= stale_after_seconds
    )
    jobs_ahead = queue_evidence.get("jobs_ahead")

    if job.state in TERMINAL_STATES:
        reason = "terminal"
    elif job.state == JobState.QUEUED:
        if isinstance(jobs_ahead, int) and jobs_ahead > 0:
            reason = "blocked_by_jobs_ahead"
        elif not healthy_workers:
            reason = "waiting_for_worker_capacity"
        elif age_seconds >= stale_after_seconds:
            reason = "queued_beyond_threshold"
        else:
            reason = "waiting_for_worker_capacity"
    elif lease is None:
        reason = "stale_ownership"
    elif lease_expired and scheduler_phases & {"completed", "failed", "canceled"}:
        reason = "scheduler_terminal_relay_nonterminal"
    elif lease_expired and scheduler:
        reason = "stale_lease_scheduler_active"
    elif lease_expired:
        reason = "stale_lease"
    elif owner_stale:
        reason = "stale_ownership"
    elif scheduler_phases & {"pending", "submitted", "allocated"}:
        reason = "scheduler_pending"
    elif scheduler_phases & {"completed", "failed", "canceled"}:
        reason = "scheduler_terminal_relay_nonterminal"
    elif inactivity_seconds >= stale_after_seconds:
        reason = "no_recent_progress"
    else:
        reason = "runtime_in_progress"

    stale_reasons = {
        "queued_beyond_threshold",
        "stale_ownership",
        "stale_lease",
        "stale_lease_scheduler_active",
        "scheduler_terminal_relay_nonterminal",
        "no_recent_progress",
    }
    stale = reason in stale_reasons or (
        job.state == JobState.QUEUED and age_seconds >= stale_after_seconds
    )
    current_tasks, tasks_truncated = queue.scan_job_tasks(job.job_id, limit=20)
    artifact_count, artifacts_truncated = queue.job_artifact_count(job.job_id)
    return {
        "job": job.model_dump(mode="json"),
        "terminal": job.state in TERMINAL_STATES,
        "reason": reason,
        "stale": stale,
        "stale_after_seconds": stale_after_seconds,
        "age_seconds": age_seconds,
        "update_age_seconds": max(0.0, (now - job.updated_at).total_seconds()),
        "last_activity_at": last_activity_at.astimezone(UTC).isoformat(),
        "inactivity_seconds": inactivity_seconds,
        "queue": queue_evidence,
        "active_job_capacity": queue.active_job_capacity(),
        "lease": {
            "present": lease is not None,
            "expired": lease_expired,
            "record": lease.model_dump(mode="json") if lease is not None else None,
            "lease_age_seconds": (
                max(0.0, (now - lease.acquired_at).total_seconds()) if lease is not None else None
            ),
        },
        "worker": {
            "owner_endpoint_id": owner_id,
            "owner_registered": owner is not None,
            "owner": owner.model_dump(mode="json") if owner is not None else None,
            "owner_heartbeat_age_seconds": (owner_heartbeat_age),
            "owner_healthy": owner in healthy_workers if owner is not None else False,
            "owner_stale": owner_stale,
            "healthy_worker_count": len(healthy_workers),
            "fresh_seconds": DEFAULT_WORKER_FRESH_SECONDS,
        },
        "scheduler": scheduler,
        "current_tasks": [
            task.model_dump(mode="json") for task in current_tasks if task.state in ACTIVE_STATES
        ],
        "last_event": last_event.model_dump(mode="json") if last_event is not None else None,
        "last_progress": (
            last_progress.model_dump(mode="json") if last_progress is not None else None
        ),
        "progress_record_count": progress_count,
        "artifact_count": artifact_count,
        "record_reads": {
            "bounded": True,
            "related_records_truncated": related_records_truncated,
            "events_truncated": events_truncated,
            "tasks_truncated": tasks_truncated,
            "progress_truncated": progress_truncated,
            "artifacts_truncated": artifacts_truncated,
        },
        "last_error": job.last_error,
        "generated_at": now.isoformat(),
    }


def _queue_evidence(
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
            "blocking_job_ids": [],
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
            "blocking_job_ids": [],
            "blocking_job_ids_truncated": False,
            "scan_truncated": scan_truncated,
            "position_exact": False,
        }
    blocking = [candidate.job_id for candidate in ordered_cluster_jobs[:target_index]]
    return {
        "state": job.state.value,
        "jobs_ahead": len(blocking),
        "position": len(blocking) + 1,
        "blocking_job_ids": blocking[:20],
        "blocking_job_ids_truncated": len(blocking) > 20,
        "scan_truncated": scan_truncated,
        "position_exact": not scan_truncated,
    }


def _scheduler_phases(statuses: list[dict[str, object]]) -> set[str]:
    phases: set[str] = set()
    for item in statuses:
        status = item.get("status")
        if not isinstance(status, dict):
            continue
        phase = cast(dict[str, object], status).get("phase")
        if isinstance(phase, str):
            phases.add(phase.lower())
    return phases


def _require_job_cluster(job: RelayJob, cluster: str | None) -> None:
    if cluster is not None and job.cluster != cluster:
        raise ConfigurationError(
            f"job {job.job_id} belongs to cluster {job.cluster}, not requested cluster {cluster}"
        )


def _validate_stale_after(value: int) -> None:
    if value < 1:
        raise ValueError("stale age threshold must be at least 1 second")


def _validate_bounds(*, limit: int, scan_limit: int) -> None:
    if limit < 1 or limit > MAX_RESULT_LIMIT:
        raise ValueError(f"limit must be between 1 and {MAX_RESULT_LIMIT}")
    if scan_limit < 1 or scan_limit > MAX_SCAN_LIMIT:
        raise ValueError(f"scan_limit must be between 1 and {MAX_SCAN_LIMIT}")
    if scan_limit < limit:
        raise ValueError("scan_limit must be greater than or equal to limit")


def worker_status(
    queue: ClioCoreQueue,
    *,
    cluster: str | None = None,
    fresh_seconds: int = 60,
    scan_limit: int = DEFAULT_SCAN_LIMIT,
) -> dict[str, object]:
    """Return registered worker capacity and current leases."""
    queue.reconcile_pending_transitions()
    if fresh_seconds < 1:
        raise ValueError("fresh_seconds must be at least 1")
    _validate_bounds(limit=1, scan_limit=scan_limit)
    now = utc_now()
    fresh_endpoints, fresh_endpoints_truncated = queue.scan_fresh_endpoints(
        limit=scan_limit,
        cluster=cluster,
        fresh_seconds=fresh_seconds,
    )
    history_endpoints, history_endpoints_truncated = queue.scan_endpoints(
        limit=scan_limit,
        cluster=cluster,
    )
    by_endpoint_id = {
        endpoint.endpoint_id: endpoint
        for endpoint in [*history_endpoints, *fresh_endpoints]
        if endpoint.role.value == "worker"
    }
    all_endpoints = list(by_endpoint_id.values())
    endpoints = [
        endpoint
        for endpoint in fresh_endpoints
        if endpoint.role.value == "worker"
        and now - endpoint.last_seen_at <= timedelta(seconds=fresh_seconds)
    ]
    endpoints_truncated = fresh_endpoints_truncated or history_endpoints_truncated
    slot_endpoints = [endpoint for endpoint in endpoints if "worker_slot" in endpoint.metadata]
    if slot_endpoints:
        capacity_endpoints = slot_endpoints
        configured_concurrency = len(slot_endpoints)
    else:
        capacity_endpoints = [
            endpoint
            for endpoint in endpoints
            if endpoint.metadata.get("worker_supervisor") is not True
        ]
        configured_concurrency = sum(
            _endpoint_concurrency(endpoint.metadata) for endpoint in capacity_endpoints
        )
    supervisor_endpoints = [
        endpoint for endpoint in endpoints if endpoint.metadata.get("worker_supervisor") is True
    ]
    kind_policy_endpoints = supervisor_endpoints or capacity_endpoints
    kind_configurations, kind_configurations_valid = _kind_concurrency_configurations(
        kind_policy_endpoints
    )
    kind_concurrency_consistent = kind_configurations_valid and len(kind_configurations) <= 1
    configured_kind_concurrency: dict[str, int] | None
    if kind_concurrency_consistent:
        configured_kind_concurrency = kind_configurations[0] if kind_configurations else {}
    else:
        configured_kind_concurrency = None
    scanned_leases, leases_truncated = queue.scan_leases(limit=scan_limit)
    leases: list[Lease] = []
    jobs_by_id: dict[str, RelayJob] = {}
    for lease in scanned_leases:
        try:
            job = queue.get_job(lease.job_id)
        except NotFoundError:
            continue
        if cluster is not None and job.cluster != cluster:
            continue
        leases.append(lease)
        jobs_by_id[job.job_id] = job
    active_leases_by_kind = {kind.value: 0 for kind in JobKind}
    counted_jobs: set[str] = set()
    for lease in leases:
        if lease.is_expired() or lease.job_id in counted_jobs:
            continue
        job = jobs_by_id.get(lease.job_id)
        if job is None or (cluster is not None and job.cluster != cluster):
            continue
        counted_jobs.add(job.job_id)
        active_leases_by_kind[job.kind.value] += 1
    return {
        "cluster": cluster,
        "workers": [endpoint.model_dump(mode="json") for endpoint in endpoints],
        "worker_count": len(capacity_endpoints),
        "configured_concurrency": configured_concurrency,
        "configured_kind_concurrency": configured_kind_concurrency,
        "kind_concurrency_consistent": kind_concurrency_consistent,
        "kind_concurrency_configurations": kind_configurations,
        "active_leases_by_kind": active_leases_by_kind,
        "active_job_capacity": queue.active_job_capacity(),
        "fresh_seconds": fresh_seconds,
        "registered_worker_count": len(all_endpoints),
        "stale_worker_count": len(all_endpoints) - len(endpoints),
        "leases": [lease.model_dump(mode="json") for lease in leases],
        "active_leases": [
            lease.model_dump(mode="json") for lease in leases if not lease.is_expired()
        ],
        "expired_leases": [lease.model_dump(mode="json") for lease in leases if lease.is_expired()],
        "scan_limit": scan_limit,
        "scan_truncated": endpoints_truncated or leases_truncated,
        "endpoint_scan_truncated": endpoints_truncated,
        "lease_scan_truncated": leases_truncated,
        "generated_at": utc_now().isoformat(),
    }


def _job_summary(job: RelayJob, jobs: list[RelayJob]) -> dict[str, object]:
    queue_evidence = _queue_evidence(job, jobs, scan_truncated=False)
    return {
        "job": job.model_dump(mode="json"),
        "relay_queue": {
            "state": queue_evidence["state"],
            "jobs_ahead": queue_evidence["jobs_ahead"],
            "position": queue_evidence["position"],
        },
    }


def _leases_by_job(leases: list[Lease]) -> dict[str, Lease]:
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


def _issue(
    job: RelayJob,
    *,
    code: str,
    severity: str,
    message: str,
    lease: Lease | None = None,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "code": code,
        "severity": severity,
        "message": message,
        "job": job.model_dump(mode="json"),
    }
    if lease is not None:
        payload["lease"] = lease.model_dump(mode="json")
    return payload


def _endpoint_concurrency(metadata: dict[str, object]) -> int:
    value = metadata.get("concurrency")
    if isinstance(value, int) and value > 0:
        return value
    return 1


def _kind_concurrency_configurations(
    endpoints: list[EndpointRegistration],
) -> tuple[list[dict[str, int]], bool]:
    configurations: list[dict[str, int]] = []
    seen: set[tuple[tuple[str, int], ...]] = set()
    valid = True
    for endpoint in endpoints:
        raw = endpoint.metadata.get("kind_concurrency", {})
        if not isinstance(raw, dict):
            valid = False
            continue
        try:
            configuration = kind_concurrency_metadata(cast(dict[str, int], raw))
        except ConfigurationError:
            valid = False
            continue
        key = tuple(configuration.items())
        if key in seen:
            continue
        seen.add(key)
        configurations.append(configuration)
    return configurations, valid

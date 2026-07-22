"""Queue management operations shared by CLI, HTTP, and MCP surfaces."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Literal, cast

from clio_relay.core_queue import MAX_LIVE_LEASE_RECORDS, ClioCoreQueue
from clio_relay.errors import ConfigurationError, NotFoundError, QueueConflictError
from clio_relay.models import (
    TERMINAL_STATES,
    EndpointRegistration,
    EndpointRole,
    JobKind,
    JobState,
    Lease,
    McpAdmissionClass,
    McpCallSpec,
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
DEFAULT_STALE_SCAN_LIMIT = MAX_SCAN_LIMIT
DEFAULT_WORKER_FRESH_SECONDS = 60


@dataclass(frozen=True)
class _AdmissionSnapshot:
    """Bounded worker-policy and lease state used by queue diagnosis."""

    analysis_complete: bool
    incomplete_reasons: tuple[str, ...]
    configured_kind_concurrency: dict[JobKind, int] | None
    kind_concurrency_consistent: bool
    healthy_worker_count: int
    configured_worker_slots: int
    free_worker_slots: int | None
    active_lease_count: int
    active_leases_by_kind: dict[JobKind, int]
    global_lease_count: int | None
    lease_index_validated: bool
    lease_index_validation_error: str | None
    lease_index_validation_error_truncated: bool
    endpoint_scan_truncated: bool
    lease_scan_truncated: bool
    unresolved_lease_job_ids: tuple[str, ...]
    expired_cluster_lease_job_ids: tuple[str, ...]


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
    now = utc_now()
    jobs, scan_truncated = queue.scan_active_jobs(limit=scan_limit)
    leases, leases_truncated = queue.scan_job_leases(job.job_id, limit=20)
    endpoints, endpoints_truncated = queue.scan_fresh_endpoints(
        limit=scan_limit,
        cluster=job.cluster,
        fresh_seconds=DEFAULT_WORKER_FRESH_SECONDS,
        now=now,
    )
    admission_snapshot = _admission_snapshot(
        queue,
        cluster=job.cluster,
        endpoints=endpoints,
        endpoint_scan_truncated=endpoints_truncated,
        scan_limit=scan_limit,
        now=now,
    )
    return _diagnose_job(
        queue,
        job,
        jobs=jobs,
        scan_truncated=scan_truncated,
        leases=leases,
        endpoints=endpoints,
        admission_snapshot=admission_snapshot,
        related_records_truncated=(
            leases_truncated or endpoints_truncated or admission_snapshot.lease_scan_truncated
        ),
        stale_after_seconds=stale_after_seconds,
        now=now,
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
    jobs, scan_truncated = queue.scan_active_jobs(limit=scan_limit)
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


def _diagnose_job(
    queue: ClioCoreQueue,
    job: RelayJob,
    *,
    jobs: list[RelayJob],
    scan_truncated: bool,
    leases: list[Lease],
    endpoints: list[EndpointRegistration],
    admission_snapshot: _AdmissionSnapshot,
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
    queue_evidence = _queue_evidence(
        queue,
        job,
        jobs,
        scan_truncated=scan_truncated,
        admission_snapshot=admission_snapshot,
    )
    scheduler = scheduler_status_for_job(queue, job.job_id, limit=20)
    scheduler_phases = _scheduler_phases(scheduler)
    lease_expired = lease is not None and lease.is_expired(now)
    owner_heartbeat_age = (
        max(0.0, (now - owner.last_seen_at).total_seconds()) if owner is not None else None
    )
    owner_stale = owner is None or (
        owner_heartbeat_age is not None and owner_heartbeat_age >= stale_after_seconds
    )
    admission = cast(dict[str, object], queue_evidence["admission"])
    admission_complete = admission.get("analysis_complete") is True
    target_admissible = admission.get("target_admissible_now")
    target_ineligibility = admission.get("target_ineligibility")
    effective_blockers = admission.get("effective_blocking_job_ids")

    if job.state in TERMINAL_STATES:
        reason = "terminal"
    elif job.kind is JobKind.INPUT_INGEST and job.state is JobState.QUEUED:
        reason = "input_ingest_in_progress"
    elif job.state == JobState.QUEUED:
        if not admission_complete:
            reason = "admission_analysis_incomplete"
        elif target_admissible is True:
            reason = "eligible_for_admission"
        elif isinstance(effective_blockers, list) and effective_blockers:
            reason = "blocked_by_admissible_jobs_ahead"
        elif target_ineligibility == "kind_capacity_saturated":
            reason = "waiting_for_kind_capacity"
        elif target_ineligibility == "pending_execution_cleanup":
            reason = "waiting_for_execution_cleanup"
        elif target_ineligibility == "global_lease_capacity_exhausted":
            reason = "waiting_for_global_lease_capacity"
        elif not healthy_workers or target_ineligibility == "no_worker_capacity":
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


def _admission_snapshot(
    queue: ClioCoreQueue,
    *,
    cluster: str,
    endpoints: list[EndpointRegistration],
    endpoint_scan_truncated: bool,
    scan_limit: int,
    now: datetime,
) -> _AdmissionSnapshot:
    workers = [endpoint for endpoint in endpoints if endpoint.role == EndpointRole.WORKER]
    slot_endpoints = [endpoint for endpoint in workers if "worker_slot" in endpoint.metadata]
    if slot_endpoints:
        capacity_endpoints = slot_endpoints
    else:
        capacity_endpoints = [
            endpoint
            for endpoint in workers
            if endpoint.metadata.get("worker_supervisor") is not True
        ]
    supervisor_endpoints = [
        endpoint for endpoint in workers if endpoint.metadata.get("worker_supervisor") is True
    ]
    kind_policy_endpoints = (
        [*supervisor_endpoints, *capacity_endpoints] if supervisor_endpoints else capacity_endpoints
    )
    kind_configurations, kind_configurations_valid = _kind_concurrency_configurations(
        kind_policy_endpoints
    )
    kind_concurrency_consistent = kind_configurations_valid and len(kind_configurations) <= 1
    configured_kind_concurrency = (
        {
            JobKind(kind): limit
            for kind, limit in (kind_configurations[0] if kind_configurations else {}).items()
        }
        if kind_concurrency_consistent
        else None
    )
    lease_index_validation_error: str | None = None
    lease_index_validation_error_truncated = False
    try:
        indexed_counts_by_kind, indexed_global_lease_count = (
            queue.lease_admission_capacity_snapshot(cluster=cluster)
        )
    except QueueConflictError as exc:
        indexed_counts_by_kind = None
        indexed_global_lease_count = None
        raw_error = str(exc)
        lease_index_validation_error = raw_error[:1_000]
        lease_index_validation_error_truncated = len(raw_error) > 1_000
    scanned_leases, lease_scan_truncated = queue.scan_leases(limit=scan_limit)
    unresolved_lease_job_ids: list[str] = []
    expired_cluster_lease_job_ids: list[str] = []
    active_leases_by_kind = {kind: 0 for kind in JobKind}
    active_lease_endpoint_counts: dict[str, int] = {}
    active_lease_job_ids: set[str] = set()
    duplicate_active_lease_job_ids: list[str] = []
    active_lease_count = 0
    global_admission_lease_count = 0
    global_lease_count_exact = not lease_scan_truncated
    recoverable_expired_cluster_by_kind = {kind: 0 for kind in JobKind}
    for lease in scanned_leases:
        if lease.is_expired(now):
            try:
                expired_job = queue.get_job(lease.job_id)
            except NotFoundError:
                unresolved_lease_job_ids.append(lease.job_id)
                global_lease_count_exact = False
                continue
            if expired_job.cluster != cluster:
                global_admission_lease_count += 1
            elif expired_job.state in ACTIVE_STATES:
                expired_cluster_lease_job_ids.append(lease.job_id)
                global_lease_count_exact = False
            else:
                recoverable_expired_cluster_by_kind[expired_job.kind] += 1
            continue
        global_admission_lease_count += 1
        active_lease_endpoint_counts[lease.endpoint_id] = (
            active_lease_endpoint_counts.get(lease.endpoint_id, 0) + 1
        )
        if lease.job_id in active_lease_job_ids:
            duplicate_active_lease_job_ids.append(lease.job_id)
        active_lease_job_ids.add(lease.job_id)
        try:
            leased_job = queue.get_job(lease.job_id)
        except NotFoundError:
            unresolved_lease_job_ids.append(lease.job_id)
            continue
        if leased_job.cluster != cluster:
            continue
        active_lease_count += 1
        active_leases_by_kind[leased_job.kind] += 1

    configured_worker_slots = (
        len(slot_endpoints)
        if slot_endpoints
        else sum(_endpoint_concurrency(endpoint.metadata) for endpoint in capacity_endpoints)
    )
    capacity_ownership_invalid = False
    free_worker_slots = 0
    for endpoint in capacity_endpoints:
        declared_slots = 1 if slot_endpoints else _endpoint_concurrency(endpoint.metadata)
        owned = active_lease_endpoint_counts.get(endpoint.endpoint_id, 0)
        if owned > declared_slots:
            capacity_ownership_invalid = True
            continue
        free_worker_slots += declared_slots - owned

    incomplete_reasons: list[str] = []
    if endpoint_scan_truncated:
        incomplete_reasons.append("worker_endpoint_scan_truncated")
    if lease_scan_truncated:
        incomplete_reasons.append("lease_scan_truncated")
    if not kind_configurations_valid:
        incomplete_reasons.append("invalid_worker_kind_policy")
    elif not kind_concurrency_consistent:
        incomplete_reasons.append("inconsistent_worker_kind_policy")
    if unresolved_lease_job_ids:
        incomplete_reasons.append("unresolved_lease_job")
    if expired_cluster_lease_job_ids:
        incomplete_reasons.append("lease_recovery_required")
    if duplicate_active_lease_job_ids:
        incomplete_reasons.append("duplicate_active_job_lease")
    if capacity_ownership_invalid:
        incomplete_reasons.append("worker_capacity_ownership_invalid")
    if lease_index_validation_error is not None:
        incomplete_reasons.append("lease_index_validation_failed")
    lease_index_validated = False
    if (
        global_lease_count_exact
        and indexed_counts_by_kind is not None
        and indexed_global_lease_count is not None
    ):
        expected_global_count = indexed_global_lease_count - sum(
            recoverable_expired_cluster_by_kind.values()
        )
        expected_counts_by_kind = {
            kind: indexed_counts_by_kind.get(kind, 0) - recoverable_expired_cluster_by_kind[kind]
            for kind in JobKind
        }
        lease_index_validated = (
            expected_global_count == global_admission_lease_count
            and expected_counts_by_kind == active_leases_by_kind
        )
        if not lease_index_validated:
            incomplete_reasons.append("lease_index_snapshot_mismatch")
    return _AdmissionSnapshot(
        analysis_complete=not incomplete_reasons,
        incomplete_reasons=tuple(incomplete_reasons),
        configured_kind_concurrency=configured_kind_concurrency,
        kind_concurrency_consistent=kind_concurrency_consistent,
        healthy_worker_count=len(capacity_endpoints),
        configured_worker_slots=configured_worker_slots,
        free_worker_slots=free_worker_slots if not capacity_ownership_invalid else None,
        active_lease_count=active_lease_count,
        active_leases_by_kind=active_leases_by_kind,
        global_lease_count=(global_admission_lease_count if global_lease_count_exact else None),
        lease_index_validated=lease_index_validated,
        lease_index_validation_error=lease_index_validation_error,
        lease_index_validation_error_truncated=lease_index_validation_error_truncated,
        endpoint_scan_truncated=endpoint_scan_truncated,
        lease_scan_truncated=lease_scan_truncated,
        unresolved_lease_job_ids=tuple(sorted(set(unresolved_lease_job_ids))),
        expired_cluster_lease_job_ids=tuple(sorted(set(expired_cluster_lease_job_ids))),
    )


def _queue_evidence(
    queue: ClioCoreQueue,
    job: RelayJob,
    jobs: list[RelayJob],
    *,
    scan_truncated: bool,
    admission_snapshot: _AdmissionSnapshot,
) -> dict[str, object]:
    raw = _raw_queue_evidence(job, jobs, scan_truncated=scan_truncated)
    if job.kind is JobKind.INPUT_INGEST and job.state is JobState.QUEUED:
        admission: dict[str, object] = {
            "analysis_complete": True,
            "applicable": False,
            "target_admissible_now": False,
            "target_ineligibility": "internal_input_ingest",
            "effective_blocking_job_ids": [],
            "effective_blocking_job_ids_truncated": False,
        }
        return {
            **raw,
            "raw_submission_order": _raw_submission_payload(raw),
            "blocking_job_ids": [],
            "blocking_job_ids_truncated": False,
            "admission": admission,
        }
    if job.state != JobState.QUEUED:
        admission: dict[str, object] = {
            "analysis_complete": True,
            "applicable": False,
            "target_admissible_now": None,
            "target_ineligibility": "job_not_queued",
            "effective_blocking_job_ids": [],
            "effective_blocking_job_ids_truncated": False,
        }
        return {
            **raw,
            "raw_submission_order": _raw_submission_payload(raw),
            "blocking_job_ids": [],
            "blocking_job_ids_truncated": False,
            "admission": admission,
        }
    admission = _queued_admission_evidence(
        queue,
        job,
        jobs,
        scan_truncated=scan_truncated,
        snapshot=admission_snapshot,
    )
    effective_blockers = cast(list[str], admission["effective_blocking_job_ids"])
    return {
        **raw,
        "raw_submission_order": _raw_submission_payload(raw),
        "blocking_job_ids": effective_blockers,
        "blocking_job_ids_truncated": admission["effective_blocking_job_ids_truncated"],
        "admission": admission,
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


def _raw_submission_payload(raw: dict[str, object]) -> dict[str, object]:
    return {
        "jobs_ahead": raw["jobs_ahead"],
        "position": raw["position"],
        "preceding_job_ids": raw["raw_preceding_job_ids"],
        "preceding_job_ids_truncated": raw["raw_preceding_job_ids_truncated"],
        "scan_truncated": raw["scan_truncated"],
        "position_exact": raw["position_exact"],
        "semantics": "raw_cluster_submission_order",
    }


def _queued_admission_evidence(
    queue: ClioCoreQueue,
    job: RelayJob,
    jobs: list[RelayJob],
    *,
    scan_truncated: bool,
    snapshot: _AdmissionSnapshot,
) -> dict[str, object]:
    ordered = [
        candidate
        for candidate in jobs
        if candidate.cluster == job.cluster and candidate.state == JobState.QUEUED
    ]
    target_index = next(
        (index for index, candidate in enumerate(ordered) if candidate.job_id == job.job_id),
        None,
    )
    incomplete_reasons = list(snapshot.incomplete_reasons)
    if scan_truncated:
        incomplete_reasons.append("active_job_scan_truncated")
    if target_index is None:
        incomplete_reasons.append("target_outside_active_job_snapshot")
    analysis_complete = snapshot.analysis_complete and not incomplete_reasons
    common: dict[str, object] = {
        "applicable": True,
        "analysis_complete": analysis_complete,
        "incomplete_reasons": incomplete_reasons,
        "semantics": "effective_next-job-admission-under-fresh-worker-policy",
        "policy_source": "fresh_worker_endpoint_registrations",
        "kind_concurrency_consistent": snapshot.kind_concurrency_consistent,
        "configured_kind_concurrency": (
            None
            if snapshot.configured_kind_concurrency is None
            else kind_concurrency_metadata(snapshot.configured_kind_concurrency)
        ),
        "healthy_worker_count": snapshot.healthy_worker_count,
        "configured_worker_slots": snapshot.configured_worker_slots,
        "free_worker_slots": snapshot.free_worker_slots,
        "active_lease_count": snapshot.active_lease_count,
        "active_leases_by_kind": {
            kind.value: snapshot.active_leases_by_kind[kind] for kind in JobKind
        },
        "global_lease_count": snapshot.global_lease_count,
        "global_lease_count_semantics": (
            "durable_lease_records_after_requested_cluster_expiry_recovery"
        ),
        "lease_index_validated": snapshot.lease_index_validated,
        "lease_index_validation_error": snapshot.lease_index_validation_error,
        "lease_index_validation_error_truncated": (snapshot.lease_index_validation_error_truncated),
        "global_lease_limit": MAX_LIVE_LEASE_RECORDS,
        "global_lease_capacity_remaining": (
            None
            if snapshot.global_lease_count is None
            else max(0, MAX_LIVE_LEASE_RECORDS - snapshot.global_lease_count)
        ),
        "active_job_scan_truncated": scan_truncated,
        "endpoint_scan_truncated": snapshot.endpoint_scan_truncated,
        "lease_scan_truncated": snapshot.lease_scan_truncated,
        "unresolved_lease_job_ids": list(snapshot.unresolved_lease_job_ids),
        "expired_cluster_lease_job_ids": list(snapshot.expired_cluster_lease_job_ids),
        "target_admissible_now": None,
        "target_ineligibility": "analysis_incomplete" if not analysis_complete else None,
        "effective_blocking_job_ids": [],
        "effective_blocking_job_ids_truncated": False,
        "simulated_predecessor_admissions": [],
        "simulated_predecessor_admissions_truncated": False,
        "skipped_predecessors": [],
        "skipped_predecessors_truncated": False,
        "remaining_global_lease_capacity_at_target": None,
        "simulated_global_lease_count_at_target": None,
    }
    if not analysis_complete or target_index is None:
        return common

    policy = snapshot.configured_kind_concurrency
    free_slots = snapshot.free_worker_slots
    if policy is None or free_slots is None:
        common["analysis_complete"] = False
        common["incomplete_reasons"] = [*incomplete_reasons, "capacity_policy_unavailable"]
        common["target_ineligibility"] = "analysis_incomplete"
        return common
    if snapshot.global_lease_count is None:
        common["analysis_complete"] = False
        common["incomplete_reasons"] = [
            *incomplete_reasons,
            "global_lease_capacity_unavailable",
        ]
        common["target_ineligibility"] = "analysis_incomplete"
        return common
    remaining_global_slots = max(
        0,
        MAX_LIVE_LEASE_RECORDS - snapshot.global_lease_count,
    )
    if remaining_global_slots < 1:
        common["target_admissible_now"] = False
        common["target_ineligibility"] = "global_lease_capacity_exhausted"
        common["remaining_global_lease_capacity_at_target"] = 0
        common["simulated_global_lease_count_at_target"] = snapshot.global_lease_count
        return common

    simulated_counts = dict(snapshot.active_leases_by_kind)
    admitted_predecessors: list[RelayJob] = []
    skipped_predecessors: list[dict[str, str]] = []
    target_ineligibility: str | None = None
    target_admissible = False
    for candidate in ordered[: target_index + 1]:
        is_target = candidate.job_id == job.job_id
        if candidate.kind is JobKind.INPUT_INGEST:
            if is_target:
                target_ineligibility = "internal_input_ingest"
                break
            skipped_predecessors.append(
                {"job_id": candidate.job_id, "reason": "internal_input_ingest"}
            )
            continue
        cleanup_pending = queue.job_has_pending_execution_cleanup(
            candidate.job_id,
            cluster=candidate.cluster,
        )
        kind_limit = policy.get(candidate.kind)
        kind_saturated = (
            kind_limit is not None and simulated_counts.get(candidate.kind, 0) >= kind_limit
        )
        if cleanup_pending:
            if is_target:
                target_ineligibility = "pending_execution_cleanup"
                break
            skipped_predecessors.append(
                {"job_id": candidate.job_id, "reason": "pending_execution_cleanup"}
            )
            continue
        if kind_saturated:
            if is_target:
                target_ineligibility = "kind_capacity_saturated"
                break
            skipped_predecessors.append(
                {"job_id": candidate.job_id, "reason": "kind_capacity_saturated"}
            )
            continue
        if remaining_global_slots < 1:
            if is_target:
                target_ineligibility = (
                    "admissible_predecessors_consumed_global_lease_capacity"
                    if admitted_predecessors
                    else "global_lease_capacity_exhausted"
                )
                break
            skipped_predecessors.append(
                {"job_id": candidate.job_id, "reason": "global_lease_capacity_exhausted"}
            )
            continue
        if free_slots < 1:
            if is_target:
                target_ineligibility = (
                    "admissible_predecessors_consumed_capacity"
                    if admitted_predecessors
                    else "no_worker_capacity"
                )
                break
            skipped_predecessors.append(
                {"job_id": candidate.job_id, "reason": "no_worker_slot_available"}
            )
            continue
        if is_target:
            target_admissible = True
            break
        admitted_predecessors.append(candidate)
        free_slots -= 1
        remaining_global_slots -= 1
        simulated_counts[candidate.kind] = simulated_counts.get(candidate.kind, 0) + 1

    effective_blockers: list[str] = []
    if target_ineligibility in {
        "admissible_predecessors_consumed_capacity",
        "admissible_predecessors_consumed_global_lease_capacity",
    }:
        effective_blockers = [candidate.job_id for candidate in admitted_predecessors]
    elif target_ineligibility == "kind_capacity_saturated":
        initial_kind_count = snapshot.active_leases_by_kind.get(job.kind, 0)
        kind_limit = policy.get(job.kind)
        if kind_limit is not None and initial_kind_count < kind_limit:
            effective_blockers = [
                candidate.job_id
                for candidate in admitted_predecessors
                if candidate.kind == job.kind
            ]

    admitted_ids = [candidate.job_id for candidate in admitted_predecessors]
    common.update(
        {
            "target_admissible_now": target_admissible,
            "target_ineligibility": target_ineligibility,
            "effective_blocking_job_ids": effective_blockers[:20],
            "effective_blocking_job_ids_truncated": len(effective_blockers) > 20,
            "simulated_predecessor_admissions": admitted_ids[:20],
            "simulated_predecessor_admissions_truncated": len(admitted_ids) > 20,
            "skipped_predecessors": skipped_predecessors[:20],
            "skipped_predecessors_truncated": len(skipped_predecessors) > 20,
            "remaining_worker_slots_at_target": free_slots,
            "remaining_global_lease_capacity_at_target": remaining_global_slots,
            "simulated_global_lease_count_at_target": (
                snapshot.global_lease_count + len(admitted_predecessors)
            ),
            "simulated_active_leases_by_kind_at_target": {
                kind.value: simulated_counts[kind] for kind in JobKind
            },
        }
    )
    return common


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
    (
        slot_endpoints,
        supervisor_endpoints,
        worker_generation_id,
        worker_generation_complete,
        fresh_worker_generation_count,
    ) = _select_active_worker_generation(queue, endpoints)
    supervised_generation_selected = worker_generation_id is not None
    if supervised_generation_selected:
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
    kind_policy_endpoints = supervisor_endpoints or capacity_endpoints
    kind_configurations, kind_configurations_valid = _kind_concurrency_configurations(
        kind_policy_endpoints
    )
    kind_concurrency_consistent = (
        kind_configurations_valid
        and len(kind_configurations) <= 1
        and worker_generation_complete is not False
    )
    configured_kind_concurrency: dict[str, int] | None
    if kind_concurrency_consistent:
        configured_kind_concurrency = kind_configurations[0] if kind_configurations else {}
    else:
        configured_kind_concurrency = None
    lane_policy_endpoints = supervisor_endpoints or capacity_endpoints
    lane_configurations = [
        _endpoint_lane_configuration(endpoint.metadata) for endpoint in lane_policy_endpoints
    ]
    distinct_lane_configurations = sorted(
        {item for item in lane_configurations if item is not None}
    )
    configured_workload_concurrency: int | None = None
    configured_control_query_concurrency: int | None = None
    if slot_endpoints:
        slot_lane_configurations = [
            _endpoint_lane_configuration(endpoint.metadata) for endpoint in slot_endpoints
        ]
        workload_slots = sum(item == (1, 0) for item in slot_lane_configurations)
        control_slots = sum(item == (0, 1) for item in slot_lane_configurations)
        lane_concurrency_consistent = (
            all(item is not None for item in slot_lane_configurations)
            and workload_slots + control_slots == len(slot_endpoints)
            and worker_generation_complete is not False
        )
        if supervisor_endpoints:
            supervisor_lane_configurations = [
                _endpoint_lane_configuration(endpoint.metadata) for endpoint in supervisor_endpoints
            ]
            lane_concurrency_consistent = (
                lane_concurrency_consistent
                and all(item is not None for item in supervisor_lane_configurations)
                and set(cast(tuple[int, int], item) for item in supervisor_lane_configurations)
                == {(workload_slots, control_slots)}
            )
        if lane_concurrency_consistent:
            configured_workload_concurrency = workload_slots
            configured_control_query_concurrency = control_slots
        distinct_lane_configurations = [(workload_slots, control_slots)]
    else:
        lane_concurrency_consistent = (
            all(item is not None for item in lane_configurations)
            and len(distinct_lane_configurations) <= 1
            and worker_generation_complete is not False
        )
        if lane_concurrency_consistent and distinct_lane_configurations:
            configured_workload_concurrency, configured_control_query_concurrency = (
                distinct_lane_configurations[0]
            )
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
    active_leases_by_mcp_admission_class = {
        admission_class.value: 0 for admission_class in McpAdmissionClass
    }
    counted_jobs: set[str] = set()
    for lease in leases:
        if lease.is_expired() or lease.job_id in counted_jobs:
            continue
        job = jobs_by_id.get(lease.job_id)
        if job is None or (cluster is not None and job.cluster != cluster):
            continue
        counted_jobs.add(job.job_id)
        active_leases_by_kind[job.kind.value] += 1
        active_leases_by_mcp_admission_class[
            (
                job.spec.admission_class.value
                if job.kind is JobKind.MCP_CALL and isinstance(job.spec, McpCallSpec)
                else McpAdmissionClass.WORKLOAD.value
            )
        ] += 1
    return {
        "cluster": cluster,
        "workers": [endpoint.model_dump(mode="json") for endpoint in endpoints],
        "worker_count": len(capacity_endpoints),
        "configured_concurrency": configured_concurrency,
        "configured_kind_concurrency": configured_kind_concurrency,
        "kind_concurrency_consistent": kind_concurrency_consistent,
        "kind_concurrency_configurations": kind_configurations,
        "configured_workload_concurrency": configured_workload_concurrency,
        "configured_control_query_concurrency": configured_control_query_concurrency,
        "control_query_concurrency_consistent": lane_concurrency_consistent,
        "worker_generation_id": worker_generation_id,
        "worker_generation_complete": worker_generation_complete,
        "fresh_worker_generation_count": fresh_worker_generation_count,
        "control_query_concurrency_configurations": [
            {
                "workload": workload,
                "control_query": control,
            }
            for workload, control in distinct_lane_configurations
        ],
        "active_leases_by_kind": active_leases_by_kind,
        "active_leases_by_mcp_admission_class": active_leases_by_mcp_admission_class,
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
    queue_evidence = _raw_queue_evidence(job, jobs, scan_truncated=False)
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


def _select_active_worker_generation(
    queue: ClioCoreQueue,
    endpoints: list[EndpointRegistration],
) -> tuple[
    list[EndpointRegistration],
    list[EndpointRegistration],
    str | None,
    bool | None,
    int,
]:
    """Select the newest supervised process generation and its fresh slots.

    Endpoint records intentionally survive process exit. During a systemd
    restart, the previous and replacement generations can therefore both be
    inside the freshness window. Capacity must come from exactly one complete
    parent generation rather than summing those records together.
    """
    fresh_supervisors = {
        endpoint.endpoint_id: endpoint
        for endpoint in endpoints
        if endpoint.metadata.get("worker_supervisor") is True
    }
    slots_by_parent: dict[str, list[EndpointRegistration]] = {}
    unbound_slots: list[EndpointRegistration] = []
    for endpoint in endpoints:
        if "worker_slot" not in endpoint.metadata:
            continue
        parent_endpoint_id = endpoint.metadata.get("parent_endpoint_id")
        if not isinstance(parent_endpoint_id, str) or not parent_endpoint_id:
            unbound_slots.append(endpoint)
            continue
        slots_by_parent.setdefault(parent_endpoint_id, []).append(endpoint)
    candidate_ids = set(fresh_supervisors) | set(slots_by_parent)
    if not candidate_ids:
        if unbound_slots:
            return unbound_slots, [], None, False, 0
        return [], [], None, None, 0

    candidates: list[
        tuple[datetime, str, EndpointRegistration | None, list[EndpointRegistration]]
    ] = []
    for parent_endpoint_id in candidate_ids:
        try:
            parent = fresh_supervisors.get(parent_endpoint_id) or queue.get_endpoint(
                parent_endpoint_id
            )
        except NotFoundError:
            parent = None
        slots = slots_by_parent.get(parent_endpoint_id, [])
        observed_at = (
            parent.registered_at
            if parent is not None
            else max(slot.registered_at for slot in slots)
        )
        candidates.append((observed_at, parent_endpoint_id, parent, slots))
    _observed_at, selected_id, selected_parent, selected_slots = max(
        candidates,
        key=lambda item: (item[0], item[1]),
    )
    selected_slots = sorted(
        [*selected_slots, *unbound_slots],
        key=lambda endpoint: (
            _worker_slot_index(endpoint.metadata),
            endpoint.endpoint_id,
        ),
    )
    complete = _worker_generation_is_complete(selected_parent, selected_slots)
    return (
        selected_slots,
        [] if selected_parent is None else [selected_parent],
        selected_id,
        complete,
        len(candidate_ids),
    )


def _worker_generation_is_complete(
    parent: EndpointRegistration | None,
    slots: list[EndpointRegistration],
) -> bool:
    """Require every declared slot from one exact supervisor generation."""
    if parent is None or parent.metadata.get("worker_supervisor") is not True:
        return False
    expected_concurrency = _endpoint_concurrency(parent.metadata)
    if expected_concurrency < 2 or len(slots) != expected_concurrency:
        return False
    indices = [_worker_slot_index(endpoint.metadata) for endpoint in slots]
    if indices != list(range(expected_concurrency)):
        return False
    return all(
        endpoint.metadata.get("parent_endpoint_id") == parent.endpoint_id
        and _endpoint_concurrency(endpoint.metadata) == 1
        and endpoint.hostname == parent.hostname
        and endpoint.pid == parent.pid
        and endpoint.registered_at >= parent.registered_at
        for endpoint in slots
    )


def _worker_slot_index(metadata: dict[str, object]) -> int:
    """Return a sortable slot index while keeping malformed metadata invalid."""
    value = metadata.get("worker_slot")
    return value if type(value) is int and value >= 0 else 2**63 - 1


def _endpoint_lane_configuration(metadata: dict[str, object]) -> tuple[int, int] | None:
    """Return one explicit workload/control slot declaration, or fail closed."""
    workload = metadata.get("workload_concurrency")
    control = metadata.get("control_query_concurrency")
    if (
        type(workload) is not int
        or type(control) is not int
        or workload < 0
        or control < 0
        or workload + control != _endpoint_concurrency(metadata)
    ):
        return None
    admission_class = metadata.get("mcp_admission_class")
    if admission_class is not None:
        if admission_class not in {
            McpAdmissionClass.WORKLOAD.value,
            McpAdmissionClass.CONTROL_QUERY.value,
        }:
            return None
        expected = (1, 0) if admission_class == McpAdmissionClass.WORKLOAD.value else (0, 1)
        if (workload, control) != expected:
            return None
    return workload, control


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

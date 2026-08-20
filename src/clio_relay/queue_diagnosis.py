"""Per-job and per-cluster progress diagnosis.

``diagnose_job`` explains why one exact relay job is or is not progressing
(queued-vs-admissible, lease/ownership/scheduler-phase reasoning, staleness),
built on the admission snapshot (``queue_admission_snapshot``) and the
admission simulation (``queue_admission_simulation``). ``diagnose_queue`` is
the cheaper, purely lease-shaped compatibility summary that flags jobs with
missing or expired leases without running the full per-job admission
analysis. ``_diagnose_job`` is the shared per-job engine both ``diagnose_job``
and ``queue_stale_recovery``'s stale discovery call directly.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import cast

from clio_relay.core_queue import ClioCoreQueue
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
from clio_relay.queue_admission_simulation import (
    _queue_evidence,  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
from clio_relay.queue_admission_snapshot import (
    _admission_snapshot,  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    _AdmissionSnapshot,  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
from clio_relay.queue_diagnosis_constants import (
    ACTIVE_STATES,
    DEFAULT_RESULT_LIMIT,
    DEFAULT_SCAN_LIMIT,
    DEFAULT_STALE_AFTER_SECONDS,
    DEFAULT_WORKER_FRESH_SECONDS,
    _require_job_cluster,  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    _validate_bounds,  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    _validate_stale_after,  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
from clio_relay.queue_listing import (
    _leases_by_job,  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
from clio_relay.relay_ops import scheduler_status_for_job


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

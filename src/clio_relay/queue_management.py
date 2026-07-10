"""Queue management operations shared by CLI, HTTP, and MCP surfaces."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Literal, cast

from clio_relay.core_queue import ClioCoreQueue
from clio_relay.models import JobState, Lease, RelayJob, utc_now
from clio_relay.relay_ops import cancel_job
from clio_relay.scheduler_status import relay_queue_status

QueueCancelPolicy = Literal["relay-only", "request-scheduler"]


ACTIVE_STATES = {JobState.QUEUED, JobState.LEASED, JobState.RUNNING}


def list_queue_jobs(
    queue: ClioCoreQueue,
    *,
    cluster: str | None = None,
    state: JobState | None = None,
    include_terminal: bool = False,
) -> dict[str, object]:
    """List relay jobs with queue position information."""
    jobs = queue.list_jobs()
    if cluster is not None:
        jobs = [job for job in jobs if job.cluster == cluster]
    if state is not None:
        jobs = [job for job in jobs if job.state == state]
    elif not include_terminal:
        jobs = [job for job in jobs if job.state in ACTIVE_STATES]
    return {
        "jobs": [_job_summary(queue, job) for job in jobs],
        "count": len(jobs),
        "cluster": cluster,
        "state": None if state is None else state.value,
        "include_terminal": include_terminal,
    }


def diagnose_queue(
    queue: ClioCoreQueue,
    *,
    cluster: str | None = None,
) -> dict[str, object]:
    """Return queue diagnostics for stuck or suspicious relay state."""
    jobs = queue.list_jobs()
    if cluster is not None:
        jobs = [job for job in jobs if job.cluster == cluster]
    leases = queue.list_leases(cluster=cluster)
    leases_by_job = _leases_by_job(leases)
    issues: list[dict[str, object]] = []
    for job in jobs:
        if job.state == JobState.QUEUED:
            continue
        if job.state == JobState.LEASED:
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
            continue
        if job.state == JobState.RUNNING:
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
    return {
        "cluster": cluster,
        "checked_jobs": len(jobs),
        "issues": issues,
        "issue_count": len(issues),
        "generated_at": utc_now().isoformat(),
    }


def cleanup_stale_jobs(
    queue: ClioCoreQueue,
    *,
    cluster: str,
    max_attempts: int = 3,
    dry_run: bool = False,
) -> dict[str, object]:
    """Recover stale leased/running jobs, or preview what would be recovered."""
    before = diagnose_queue(queue, cluster=cluster)
    issues = cast(list[dict[str, object]], before["issues"])
    recoverable = [
        issue for issue in issues if issue.get("code") in {"expired_lease", "running_expired_lease"}
    ]
    if dry_run:
        return {
            "cluster": cluster,
            "dry_run": True,
            "recoverable": recoverable,
            "recovered": [],
            "recovered_count": 0,
        }
    recovered = queue.recover_stale_jobs(cluster=cluster, max_attempts=max_attempts)
    return {
        "cluster": cluster,
        "dry_run": False,
        "recoverable": recoverable,
        "recovered": [job.model_dump(mode="json") for job in recovered],
        "recovered_count": len(recovered),
    }


def cancel_queue_job(
    queue: ClioCoreQueue,
    job_id: str,
    *,
    scheduler_policy: QueueCancelPolicy = "relay-only",
) -> dict[str, object]:
    """Cancel a relay job with explicit scheduler cancellation policy."""
    if scheduler_policy not in {"relay-only", "request-scheduler"}:
        raise ValueError("scheduler_policy must be relay-only or request-scheduler")
    cancel_scheduler = scheduler_policy == "request-scheduler"
    job = cancel_job(queue, job_id, cancel_scheduler=cancel_scheduler)
    return {
        "job": job.model_dump(mode="json"),
        "scheduler_policy": scheduler_policy,
        "scheduler_cancel_requested": cancel_scheduler,
    }


def worker_status(
    queue: ClioCoreQueue,
    *,
    cluster: str | None = None,
    fresh_seconds: int = 60,
) -> dict[str, object]:
    """Return registered worker capacity and current leases."""
    if fresh_seconds < 1:
        raise ValueError("fresh_seconds must be at least 1")
    now = utc_now()
    all_endpoints = [
        endpoint
        for endpoint in queue.list_endpoints(cluster=cluster)
        if endpoint.role.value == "worker"
    ]
    endpoints = [
        endpoint
        for endpoint in all_endpoints
        if now - endpoint.last_seen_at <= timedelta(seconds=fresh_seconds)
    ]
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
    leases = queue.list_leases(cluster=cluster)
    return {
        "cluster": cluster,
        "workers": [endpoint.model_dump(mode="json") for endpoint in endpoints],
        "worker_count": len(capacity_endpoints),
        "configured_concurrency": configured_concurrency,
        "fresh_seconds": fresh_seconds,
        "registered_worker_count": len(all_endpoints),
        "stale_worker_count": len(all_endpoints) - len(endpoints),
        "leases": [lease.model_dump(mode="json") for lease in leases],
        "active_leases": [
            lease.model_dump(mode="json") for lease in leases if not lease.is_expired()
        ],
        "expired_leases": [lease.model_dump(mode="json") for lease in leases if lease.is_expired()],
        "generated_at": utc_now().isoformat(),
    }


def _job_summary(queue: ClioCoreQueue, job: RelayJob) -> dict[str, object]:
    return {
        "job": job.model_dump(mode="json"),
        "relay_queue": relay_queue_status(queue, job),
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

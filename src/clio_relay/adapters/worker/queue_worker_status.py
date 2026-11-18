"""Registered worker capacity and current lease reporting.

``worker_status`` is the operator-facing view of what the cluster's worker
fleet looks like right now: which endpoints are fresh, the selected
supervised process generation (via ``queue_worker_capacity``), the
configured kind concurrency policy and whether every worker agrees on it,
and the live and expired lease records themselves.
"""

from __future__ import annotations

from datetime import timedelta

from clio_relay.adapters.worker.core_queue import ClioCoreQueue
from clio_relay.errors import NotFoundError
from clio_relay.models import (
    JobKind,
    Lease,
    RelayJob,
    utc_now,
)
from clio_relay.adapters.worker.queue_diagnosis_constants import (
    DEFAULT_SCAN_LIMIT,
    _validate_bounds,  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
from clio_relay.adapters.worker.queue_worker_capacity import (
    _endpoint_concurrency,  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    _kind_concurrency_configurations,  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    _select_active_worker_generation,  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)


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
        "worker_generation_id": worker_generation_id,
        "worker_generation_complete": worker_generation_complete,
        "fresh_worker_generation_count": fresh_worker_generation_count,
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

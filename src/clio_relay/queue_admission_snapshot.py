"""Bounded worker-policy and lease-state capacity snapshot.

``_admission_snapshot`` scans fresh worker endpoints and durable leases once
and reduces them into a single, cross-checked ``_AdmissionSnapshot``: the
configured per-kind concurrency policy (and whether every worker agrees on
it), free worker slots, active lease counts by kind, and the global lease
count validated against the CTE's own lease index. Diagnosis and stale
recovery both build their per-job admission reasoning on top of this one
snapshot instead of re-deriving it.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from clio_relay.core_queue import ClioCoreQueue
from clio_relay.errors import NotFoundError, QueueConflictError
from clio_relay.models import EndpointRegistration, EndpointRole, JobKind
from clio_relay.queue_diagnosis_constants import ACTIVE_STATES
from clio_relay.queue_worker_capacity import (
    _endpoint_concurrency,  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    _kind_concurrency_configurations,  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)


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


def _admission_snapshot(  # pyright: ignore[reportUnusedFunction]
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

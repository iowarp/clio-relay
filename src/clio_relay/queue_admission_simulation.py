"""Next-job admission simulation under the fresh worker capacity policy.

Given a queued job's raw submission-order position (from ``queue_listing``)
and a bounded ``_AdmissionSnapshot`` (from ``queue_admission_snapshot``),
``_queued_admission_evidence`` walks the cluster's queued jobs in order,
simulating which of the job's predecessors would themselves be admitted
under the current worker-slot and global-lease-capacity policy, to answer
whether the target job is admissible *right now* -- and if not, which
already-admissible jobs are effectively blocking it. ``_queue_evidence`` is
the thin dispatcher that decides whether that simulation even applies (a
non-queued job, or an internal input-ingest job, gets a fixed short-circuit
answer instead).
"""

from __future__ import annotations

from typing import cast

from clio_relay.core_queue import MAX_LIVE_LEASE_RECORDS, ClioCoreQueue
from clio_relay.models import JobKind, JobState, RelayJob
from clio_relay.queue_admission_snapshot import (
    _AdmissionSnapshot,  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
from clio_relay.queue_listing import (
    _raw_queue_evidence,  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    _raw_submission_payload,  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
from clio_relay.worker_concurrency import kind_concurrency_metadata


def _queue_evidence(  # pyright: ignore[reportUnusedFunction]
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

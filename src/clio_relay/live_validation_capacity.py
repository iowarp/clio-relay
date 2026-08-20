"""Live worker fleet capacity/containment checks for queue validation.

Owns the "read the worker fleet's own reported configuration and hold it to
account" concern: extracting/asserting the configured JARVIS kind
concurrency and total worker capacity, requiring every live worker to
report kernel-enforced descendant containment, snapshotting per-slot
heartbeats, requiring an otherwise-empty relay queue before the controlled
fixture starts, and the durable lease-capacity audit check (canonical
leases, operational indexes, and admission counts must all agree). Moved
verbatim out of ``queue_validation.py`` (iowarp/clio-relay#231-style split);
no behavior changed.
"""

from __future__ import annotations

from datetime import datetime
from typing import cast

from clio_relay.core_queue import ClioCoreQueue
from clio_relay.errors import RelayError
from clio_relay.live_validation_support import _evidence, _list, _mapping, _require
from clio_relay.models import TERMINAL_STATES, JobKind
from clio_relay.queue_management import MAX_SCAN_LIMIT
from clio_relay.validation_report import ValidationRecorder


def _controlled_capacity(capacity: dict[str, object], kind: JobKind) -> tuple[int, int]:
    worker_count = capacity.get("worker_count")
    configured_total = capacity.get("configured_concurrency")
    configured_by_kind = _mapping(
        capacity.get("configured_kind_concurrency"),
        "configured kind concurrency",
    )
    kind_limit = configured_by_kind.get(kind.value)
    _require(isinstance(worker_count, int) and worker_count > 0, "no fresh worker slots")
    _require(
        isinstance(configured_total, int) and configured_total > 0,
        "worker total concurrency is not bounded",
    )
    _require(
        capacity.get("kind_concurrency_consistent") is True,
        "fresh workers disagree on kind concurrency",
    )
    _require(
        isinstance(kind_limit, int) and kind_limit > 0,
        f"no explicit concurrency limit for {kind.value}",
    )
    return cast(int, kind_limit), cast(int, configured_total)


def _controlled_process_containment(capacity: dict[str, object]) -> dict[str, object]:
    workers = _list(capacity.get("workers"), "worker registrations")
    _require(bool(workers), "worker status exposed no live containment identities")
    modes: set[str] = set()
    endpoint_ids: list[str] = []
    for raw_worker in workers:
        worker = _mapping(raw_worker, "worker registration")
        endpoint_id = worker.get("endpoint_id")
        metadata = _mapping(worker.get("metadata"), "worker metadata")
        containment = _mapping(
            metadata.get("process_containment"),
            "worker process containment",
        )
        mode = containment.get("mode")
        _require(isinstance(endpoint_id, str), "worker containment omitted endpoint identity")
        _require(isinstance(mode, str) and bool(mode), "worker containment omitted provider mode")
        _require(
            containment.get("enforceable") is True,
            f"worker {endpoint_id} lacks kernel-enforced process containment: "
            f"{containment.get('reason')}",
        )
        endpoint_ids.append(cast(str, endpoint_id))
        modes.add(cast(str, mode))
    _require(len(modes) == 1, f"live workers disagree on containment mode: {sorted(modes)}")
    return {
        "enforceable": True,
        "mode": next(iter(modes)),
        "worker_endpoint_ids": sorted(endpoint_ids),
        "worker_count": len(endpoint_ids),
    }


def _worker_heartbeat_snapshot(capacity: dict[str, object]) -> dict[str, datetime]:
    workers = _list(capacity.get("workers"), "worker registrations")
    snapshot: dict[str, datetime] = {}
    for raw_worker in workers:
        worker = _mapping(raw_worker, "worker registration")
        metadata = _mapping(worker.get("metadata"), "worker metadata")
        endpoint_id = worker.get("endpoint_id")
        observed_at = worker.get("last_seen_at")
        if not isinstance(endpoint_id, str) or not isinstance(metadata.get("worker_slot"), int):
            continue
        if not isinstance(observed_at, str):
            raise RelayError(f"worker slot {endpoint_id} omitted last_seen_at")
        snapshot[endpoint_id] = datetime.fromisoformat(observed_at)
    return snapshot


def _require_quiet_validation_queue(queue: ClioCoreQueue, *, cluster: str) -> None:
    indexed_active, truncated = queue.scan_active_jobs(limit=MAX_SCAN_LIMIT)
    _require(not truncated, "active queue exceeds the validation scan bound")
    active = [
        job for job in indexed_active if job.cluster == cluster and job.state not in TERMINAL_STATES
    ]
    _require(
        not active,
        "controlled live validation requires an otherwise empty relay queue; active jobs="
        + ",".join(job.job_id for job in active[:20]),
    )


def _validate_lease_capacity_audit(
    recorder: ValidationRecorder,
    queue: ClioCoreQueue,
    *,
    check_id: str,
    cluster: str,
) -> None:
    with recorder.check(
        check_id,
        "prove canonical leases, exact operational indexes, and admission counts agree",
    ) as evidence:
        audit = queue.audit_lease_capacity()
        _require(audit.get("valid") is True, "lease capacity audit did not pass")
        aggregate = _mapping(audit.get("aggregate"), "lease capacity aggregate")
        canonical = _mapping(audit.get("canonical"), "canonical lease capacity")
        operational = _mapping(
            audit.get("operational_indexes"),
            "lease operational indexes",
        )
        evidence.append(
            _evidence(
                "lease_capacity_audit",
                f"relay-queue://{cluster}/lease-capacity",
                {
                    "schema_version": audit.get("schema_version"),
                    "valid": True,
                    "scan_truncated": audit.get("scan_truncated"),
                    "result_truncated": audit.get("result_truncated"),
                    "canonical": canonical,
                    "operational_indexes": operational,
                    "aggregate": aggregate,
                },
            )
        )

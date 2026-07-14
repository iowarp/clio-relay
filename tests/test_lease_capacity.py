from __future__ import annotations

import json
import multiprocessing
import os
from pathlib import Path
from queue import Empty
from time import perf_counter
from typing import Any, cast

import pytest

from clio_relay.core_queue import (
    LEASE_CAPACITY_AGGREGATE_SCHEMA,
    LEASE_CAPACITY_AUDIT_SCHEMA,
    MAX_LEASE_CAPACITY_RECORD_BYTES,
    MAX_LIVE_LEASE_RECORDS,
    ClioCoreQueue,
)
from clio_relay.errors import QueueConflictError
from clio_relay.models import JarvisRunSpec, JobKind, JobState, Lease, RelayJob


def _job(key: str, *, cluster: str = "configured-target") -> RelayJob:
    return RelayJob(
        cluster=cluster,
        kind=JobKind.JARVIS,
        spec=JarvisRunSpec(command=["true"]),
        idempotency_key=key,
    )


def _mapping(value: object) -> dict[str, object]:
    assert isinstance(value, dict)
    return cast(dict[str, object], value)


def _mismatches(report: dict[str, object]) -> list[dict[str, object]]:
    value = report.get("mismatches")
    assert isinstance(value, list)
    return [_mapping(item) for item in cast(list[object], value)]


def _acquire_in_process(
    root: str,
    job_id: str,
    endpoint_id: str,
    start_event: Any,
    results: Any,
    kind_limit: int | None,
) -> None:
    queue = ClioCoreQueue(Path(root))
    start_event.wait(timeout=20)
    try:
        lease = queue.acquire_job(
            job_id,
            endpoint_id,
            cluster="configured-target",
            kind_concurrency=(None if kind_limit is None else {JobKind.JARVIS: kind_limit}),
        )
        results.put((job_id, None if lease is None else lease.lease_id, None))
    except Exception as exc:  # pragma: no cover - surfaced in the parent assertion
        results.put((job_id, None, f"{type(exc).__name__}: {exc}"))


def _run_parallel_acquisitions(
    queue: ClioCoreQueue,
    jobs: list[RelayJob],
    *,
    kind_limit: int | None,
) -> list[tuple[str, str | None, str | None]]:
    context = multiprocessing.get_context("spawn")
    start_event = context.Event()
    results = context.Queue()
    processes = [
        context.Process(
            target=_acquire_in_process,
            args=(
                str(queue.root),
                job.job_id,
                f"worker-{index}",
                start_event,
                results,
                kind_limit,
            ),
        )
        for index, job in enumerate(jobs)
    ]
    for process in processes:
        process.start()
    start_event.set()
    for process in processes:
        process.join(timeout=30)
        assert process.exitcode == 0
    observed: list[tuple[str, str | None, str | None]] = []
    for _ in processes:
        try:
            observed.append(results.get(timeout=5))
        except Empty as exc:  # pragma: no cover - guarded by child exit assertions
            raise AssertionError("acquisition child produced no result") from exc
    return observed


def test_capacity_pair_generations_track_acquire_renew_and_release(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    job = queue.submit_job(_job("capacity-generations"))
    initial = queue._read_lease_capacity_aggregate_unlocked()  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001

    lease = queue.acquire_job(job.job_id, "worker", cluster=job.cluster)
    assert lease is not None
    acquired = queue._read_lease_capacity_aggregate_unlocked()  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    assert acquired.aggregate.epoch_id == initial.aggregate.epoch_id
    assert acquired.aggregate.generation == initial.aggregate.generation + 1
    assert queue.lease_admission_capacity_snapshot(cluster=job.cluster) == (
        {JobKind.JARVIS: 1},
        1,
    )

    assert queue.renew_lease(lease.lease_id) is not None
    renewed = queue._read_lease_capacity_aggregate_unlocked()  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    assert renewed.aggregate.generation == acquired.aggregate.generation + 1
    assert renewed.aggregate.global_live_leases == 1

    queue.release_lease(lease.lease_id)
    released = queue._read_lease_capacity_aggregate_unlocked()  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    assert released.aggregate.generation == renewed.aggregate.generation + 1
    assert released.aggregate.global_live_leases == 0
    assert queue.audit_lease_capacity()["valid"] is True


@pytest.mark.parametrize(
    "corruption",
    [
        "missing",
        "extra",
        "hardlink",
        "checksum",
        "duplicate-key",
        "generation-mismatch",
        "oversized",
    ],
)
def test_capacity_pair_corruption_fails_closed(
    tmp_path: Path,
    corruption: str,
) -> None:
    root = tmp_path / "core"
    queue = ClioCoreQueue(root)
    job = queue.submit_job(_job(f"capacity-corruption-{corruption}"))
    initial_checkpoint = (root / "lease_capacity" / "checkpoint.json").read_bytes()
    lease = queue.acquire_job(job.job_id, "worker", cluster=job.cluster)
    assert lease is not None
    aggregate_path = root / "lease_capacity" / "aggregate.json"
    checkpoint_path = root / "lease_capacity" / "checkpoint.json"

    if corruption == "missing":
        aggregate_path.unlink()
    elif corruption == "extra":
        (root / "lease_capacity" / "unexpected.json").write_text("{}", encoding="utf-8")
    elif corruption == "hardlink":
        source = tmp_path / "checkpoint-hardlink.json"
        source.write_bytes(checkpoint_path.read_bytes())
        checkpoint_path.unlink()
        os.link(source, checkpoint_path)
        assert os.lstat(checkpoint_path).st_nlink == 2
    elif corruption == "checksum":
        aggregate = json.loads(aggregate_path.read_text(encoding="utf-8"))
        aggregate["global_live_leases"] = 0
        aggregate_path.write_text(json.dumps(aggregate), encoding="utf-8")
    elif corruption == "duplicate-key":
        aggregate_text = aggregate_path.read_text(encoding="utf-8")
        aggregate_path.write_text(
            f'{aggregate_text[:-1]},"generation":999}}',
            encoding="utf-8",
        )
    elif corruption == "generation-mismatch":
        checkpoint_path.write_bytes(initial_checkpoint)
    elif corruption == "oversized":
        aggregate_path.write_bytes(b"x" * (MAX_LEASE_CAPACITY_RECORD_BYTES + 1))
    else:  # pragma: no cover - exhaustive parameter guard
        raise AssertionError(corruption)

    restarted = ClioCoreQueue(root)
    with pytest.raises(QueueConflictError):
        restarted.lease_admission_capacity_snapshot(cluster=job.cluster)
    report = restarted.audit_lease_capacity()
    assert report["schema_version"] == LEASE_CAPACITY_AUDIT_SCHEMA
    assert report["valid"] is False


def test_coherent_stale_aggregate_is_audited_and_explicitly_repaired(tmp_path: Path) -> None:
    root = tmp_path / "core"
    queue = ClioCoreQueue(root)
    job = queue.submit_job(_job("stale-capacity"))
    assert queue.acquire_job(job.job_id, "worker", cluster=job.cluster) is not None
    original = queue._read_lease_capacity_aggregate_unlocked()  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    stale_transition = queue._prepare_lease_capacity_transition_unlocked(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        scope_deltas={(job.cluster, job.kind): -1}
    )
    queue._apply_lease_capacity_transition_unlocked(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        stale_transition,
        target="after",
        label="coherent stale aggregate fixture",
    )

    report = queue.audit_lease_capacity()
    assert report["valid"] is False
    assert any(
        mismatch["type"] in {"aggregate_scope_mismatch", "aggregate_global_mismatch"}
        for mismatch in _mismatches(report)
    )
    repaired = queue.repair_lease_operational_indexes()
    assert repaired["record_count"] == 1
    current = queue._read_lease_capacity_aggregate_unlocked()  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    assert current.aggregate.epoch_id != original.aggregate.epoch_id
    assert queue.audit_lease_capacity()["valid"] is True


def test_repair_recovers_a_checksum_tamper_and_restores_migration_gate(tmp_path: Path) -> None:
    root = tmp_path / "core"
    queue = ClioCoreQueue(root)
    job = queue.submit_job(_job("repair-checksum"))
    assert queue.acquire_job(job.job_id, "worker", cluster=job.cluster) is not None
    aggregate_path = root / "lease_capacity" / "aggregate.json"
    aggregate = json.loads(aggregate_path.read_text(encoding="utf-8"))
    aggregate["document_sha256"] = "0" * 64
    aggregate_path.write_text(json.dumps(aggregate), encoding="utf-8")

    restarted = ClioCoreQueue(root)
    assert restarted.index_migration_status()["complete"] is False
    repaired = restarted.repair_lease_operational_indexes()
    assert repaired["record_count"] == 1
    assert restarted.index_migration_status()["complete"] is True
    assert restarted.audit_lease_capacity()["valid"] is True


def test_existing_queue_capacity_migration_replays_a_torn_pair(tmp_path: Path) -> None:
    root = tmp_path / "core"
    (root / "jobs").mkdir(parents=True)
    (root / "leases").mkdir(parents=True)
    job = _job("legacy-capacity-migration").model_copy(
        update={"state": JobState.LEASED, "leased_by": "legacy-worker"}
    )
    lease = Lease.new(job.job_id, "legacy-worker", 300)
    (root / "jobs" / f"{job.job_id}.json").write_text(
        job.model_dump_json(),
        encoding="utf-8",
    )
    (root / "leases" / f"{lease.lease_id}.json").write_text(
        lease.model_dump_json(),
        encoding="utf-8",
    )
    queue = ClioCoreQueue(root)
    state = queue.index_migration_status()
    assert _mapping(state["lease_capacity_aggregate"])["complete"] is False

    def crash_after_aggregate(_aggregate: object) -> None:
        raise RuntimeError("simulated torn capacity migration")

    queue._after_lease_capacity_aggregate_write = crash_after_aggregate  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    with pytest.raises(RuntimeError, match="torn capacity migration"):
        while state["complete"] is not True:
            state = queue.migrate_indexes_batch(batch_size=10)

    restarted = ClioCoreQueue(root)
    restarted.initialize()
    state = restarted.index_migration_status()
    while state["complete"] is not True:
        state = restarted.migrate_indexes_batch(batch_size=10)
    assert _mapping(state["lease_capacity_aggregate"])["complete"] is True
    assert restarted.audit_lease_capacity()["valid"] is True


def test_capacity_aggregate_supports_worst_case_sparse_scope_document(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    queue.initialize()
    transition = queue._prepare_lease_capacity_transition_unlocked(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        scope_deltas={
            (f"cluster-{index}", JobKind.JARVIS): 1 for index in range(MAX_LIVE_LEASE_RECORDS)
        }
    )
    queue._apply_lease_capacity_transition_unlocked(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        transition,
        target="after",
        label="worst-case sparse aggregate fixture",
    )
    aggregate_path = queue.root / "lease_capacity" / "aggregate.json"
    aggregate = json.loads(aggregate_path.read_text(encoding="utf-8"))
    assert aggregate["schema_version"] == LEASE_CAPACITY_AGGREGATE_SCHEMA
    assert aggregate["global_live_leases"] == MAX_LIVE_LEASE_RECORDS
    assert len(aggregate["cluster_kind_counts"]) == MAX_LIVE_LEASE_RECORDS
    assert aggregate_path.stat().st_size < MAX_LEASE_CAPACITY_RECORD_BYTES


def test_full_audit_matches_real_canonical_and_index_records(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    for index in range(50):
        job = queue.submit_job(_job(f"audit-equality-{index}", cluster=f"cluster-{index % 5}"))
        assert (
            queue.acquire_job(
                job.job_id,
                f"worker-{index}",
                cluster=job.cluster,
            )
            is not None
        )

    started = perf_counter()
    report = queue.audit_lease_capacity()
    elapsed = perf_counter() - started
    assert report["valid"] is True
    assert _mapping(report["canonical"])["global_live_leases"] == 50
    assert _mapping(report["operational_indexes"])["manifests"] == 50
    assert elapsed < 30.0


def test_multi_process_kind_capacity_cannot_be_exceeded(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    jobs = [queue.submit_job(_job(f"multiprocess-kind-{index}")) for index in range(4)]
    observed = _run_parallel_acquisitions(queue, jobs, kind_limit=2)

    assert all(error is None for _job_id, _lease_id, error in observed)
    assert sum(lease_id is not None for _job_id, lease_id, _error in observed) == 2
    counts, global_total = queue.lease_admission_capacity_snapshot(cluster="configured-target")
    assert counts == {JobKind.JARVIS: 2}
    assert global_total == 2
    assert queue.audit_lease_capacity()["valid"] is True


def test_multi_process_global_9999_plus_two_admits_exactly_one(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    jobs = [queue.submit_job(_job(f"multiprocess-global-{index}")) for index in range(2)]
    transition = queue._prepare_lease_capacity_transition_unlocked(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        scope_deltas={("preexisting-cluster", JobKind.JARVIS): 9_999}
    )
    queue._apply_lease_capacity_transition_unlocked(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        transition,
        target="after",
        label="9999 global capacity fixture",
    )

    observed = _run_parallel_acquisitions(queue, jobs, kind_limit=None)

    assert all(error is None for _job_id, _lease_id, error in observed)
    assert sum(lease_id is not None for _job_id, lease_id, _error in observed) == 1
    _counts, global_total = queue.lease_admission_capacity_snapshot(cluster="configured-target")
    assert global_total == MAX_LIVE_LEASE_RECORDS

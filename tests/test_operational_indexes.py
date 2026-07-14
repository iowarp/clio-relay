from __future__ import annotations

import json
import os
import subprocess
import sys
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from time import perf_counter
from typing import cast

import pytest

from clio_relay.core_queue import (
    DEFAULT_CORE_LOCK_TIMEOUT_SECONDS,
    LEASE_OPERATIONAL_INDEX_SCHEMA,
    MAX_ACTIVE_JOB_RECORDS,
    MAX_LIVE_LEASE_RECORDS,
    ClioCoreQueue,
)
from clio_relay.errors import QueueConflictError
from clio_relay.models import (
    EndpointRegistration,
    EndpointRole,
    GatewaySession,
    GatewaySessionState,
    JarvisRunSpec,
    JobKind,
    JobState,
    Lease,
    RelayJob,
    RelayTask,
    utc_now,
)
from clio_relay.queue_management import diagnose_job, discover_stale_jobs


def _job(key: str, *, metadata: dict[str, object] | None = None) -> RelayJob:
    return RelayJob(
        cluster="ares",
        kind=JobKind.JARVIS,
        spec=JarvisRunSpec(command=["true"]),
        idempotency_key=key,
        metadata=metadata or {},
    )


def _lease_operational_files(queue: ClioCoreQueue) -> list[Path]:
    return [
        path
        for family in (
            "lease_indexes",
            "lease_identity_refs",
            "leases_by_endpoint",
            "leases_by_cluster_kind",
            "leases_by_expiry",
        )
        for path in (queue.root / family).rglob("*")
        if path.is_file()
    ]


def test_sparse_active_capacity_rejects_before_reading_payloads(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    queue.initialize()
    active = queue.root / "jobs_active"
    for index in range(MAX_ACTIVE_JOB_RECORDS):
        (active / f"sparse-{index:05d}.json").touch()

    with pytest.raises(QueueConflictError, match="active_job_capacity_reached"):
        queue.submit_job(_job("capacity-overflow"))

    assert queue.active_job_capacity() == {
        "limit": MAX_ACTIVE_JOB_RECORDS,
        "used": MAX_ACTIVE_JOB_RECORDS,
        "remaining": 0,
        "over_capacity": False,
    }


def test_preexisting_over_capacity_queue_can_still_drain(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    job = queue.submit_job(_job("legacy-over-capacity"))
    endpoint = queue.register_endpoint(
        EndpointRegistration(
            role=EndpointRole.WORKER,
            cluster="ares",
            hostname="worker",
            pid=123,
        )
    )
    original_scan = queue._scan_many  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001

    def truncated_queued_scan(
        directory: Path,
        model: type[RelayJob],
        *,
        limit: int,
    ) -> tuple[list[RelayJob], bool]:
        if directory == queue.root / "jobs_queued":
            return [job], True
        return original_scan(directory, model, limit=limit)

    monkeypatch.setattr(queue, "_scan_many", truncated_queued_scan)

    lease = queue.acquire_next_job(endpoint.endpoint_id, cluster="ares")

    assert lease is not None
    assert lease.job_id == job.job_id


def test_live_lease_index_cold_count_and_warm_admission_stay_bounded(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    queued = queue.submit_job(_job("capacity-admission-probe"))
    job = _job("capacity-template").model_copy(update={"state": JobState.RUNNING})
    for index in range(MAX_LIVE_LEASE_RECORDS):
        lease = Lease.new(f"job-{index}", f"capacity-worker-{index}", 300)
        indexed_job = job.model_copy(update={"job_id": lease.job_id})
        identity = queue._lease_index_identity(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            lease,
            job=indexed_job,
        )
        for path in (
            queue._lease_identity_ref_path(identity),  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            queue._lease_endpoint_ref_path(identity),  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            queue._lease_endpoint_guard_path(identity),  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            queue._lease_cluster_kind_ref_path(identity),  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            queue._lease_expiry_ref_path(identity),  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        ):
            path.parent.mkdir(parents=True, exist_ok=True)
            path.touch()

    started = perf_counter()
    counts = queue._active_lease_counts_by_kind(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        cluster="ares"
    )
    elapsed = perf_counter() - started

    assert counts == {JobKind.JARVIS: MAX_LIVE_LEASE_RECORDS}
    assert elapsed < DEFAULT_CORE_LOCK_TIMEOUT_SECONDS, (
        "10k cold indexed lease count exceeded the queue lock timeout: "
        f"{elapsed:.3f}s >= {DEFAULT_CORE_LOCK_TIMEOUT_SECONDS:.3f}s"
    )
    started = perf_counter()
    assert queue.acquire_job(queued.job_id, "10k-capacity-probe", cluster="ares") is None
    acquire_elapsed = perf_counter() - started
    assert acquire_elapsed < 2.0, (
        f"10k warm indexed lease acquisition exceeded lock budget: {acquire_elapsed:.3f}s"
    )


def test_live_lease_capacity_fails_closed_above_active_bound(
    tmp_path: Path,
) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    queue.initialize()
    job = _job("over-capacity-template").model_copy(update={"state": JobState.RUNNING})
    for index in range(MAX_LIVE_LEASE_RECORDS + 1):
        lease = Lease.new(f"job-{index}", f"worker-{index}", 300)
        identity = queue._lease_index_identity(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            lease,
            job=job.model_copy(update={"job_id": lease.job_id}),
        )
        path = queue._lease_expiry_ref_path(identity)  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        path.touch()

    with pytest.raises(QueueConflictError, match="10000 records"):
        queue._active_lease_counts_by_kind(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            cluster="ares"
        )


def test_legacy_lease_operational_indexes_migrate_and_repair(tmp_path: Path) -> None:
    root = tmp_path / "core"
    (root / "jobs").mkdir(parents=True)
    (root / "leases").mkdir(parents=True)
    job = _job("legacy-lease-operational").model_copy(
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
    while state["complete"] is not True:
        state = queue.migrate_indexes_batch(batch_size=10)

    identity = queue._lease_index_identity(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        lease,
        job=job,
    )
    endpoint_ref = queue._lease_endpoint_ref_path(identity)  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    assert endpoint_ref.is_file()
    assert queue._lease_endpoint_guard_path(identity).is_file()  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    assert queue._lease_cluster_kind_ref_path(identity).is_file()  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    assert queue._lease_expiry_ref_path(identity).is_file()  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    assert queue._lease_identity_ref_path(identity).is_file()  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    assert queue._active_lease_for_endpoint("legacy-worker") == lease  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001

    endpoint_ref.unlink()
    stale_ref = endpoint_ref.parent / "legacy-unbound.ref"
    stale_ref.touch()
    repaired = queue.repair_lease_operational_indexes()
    assert repaired["record_count"] == 1
    assert endpoint_ref.is_file()
    assert not stale_ref.exists()

    manifest_path = queue._lease_index_path(lease.lease_id)  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["schema_version"] = "clio-relay.lease-operational-index.v1"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    migration_path = root / "migrations" / "index-v1.json"
    migration = json.loads(migration_path.read_text(encoding="utf-8"))
    migration["operational_families"]["leases"]["schema_version"] = (
        "clio-relay.lease-operational-index.v1"
    )
    migration["lease_operational_repair"] = {
        "complete": True,
        "schema_version": "clio-relay.lease-operational-index.v1",
    }
    migration["complete"] = True
    migration_path.write_text(json.dumps(migration), encoding="utf-8")

    upgraded = ClioCoreQueue(root)
    upgraded_state = upgraded.index_migration_status()
    while upgraded_state["complete"] is not True:
        upgraded_state = upgraded.migrate_indexes_batch(batch_size=10)
    upgraded_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert upgraded_manifest["schema_version"] == LEASE_OPERATIONAL_INDEX_SCHEMA
    assert upgraded._active_lease_for_endpoint("legacy-worker") == lease  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001


def test_lease_operational_indexes_fail_closed_on_corrupt_or_missing_ref(
    tmp_path: Path,
) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    job = queue.submit_job(_job("lease-index-corruption"))
    lease = queue.acquire_job(job.job_id, "worker", cluster="ares")
    assert lease is not None
    leased_job = queue.get_job(job.job_id)
    identity = queue._lease_index_identity(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        lease,
        job=leased_job,
    )
    kind_ref = queue._lease_cluster_kind_ref_path(identity)  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    kind_ref.write_text("tampered", encoding="utf-8")
    with pytest.raises(QueueConflictError, match="unsafe lease reference"):
        queue._active_lease_counts_by_kind(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            cluster="ares"
        )

    kind_ref.write_text("", encoding="utf-8")
    kind_ref.unlink()
    with pytest.raises(QueueConflictError, match="indexes disagree"):
        queue._active_lease_counts_by_kind(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            cluster="ares"
        )


def test_lease_indexes_reject_missing_endpoint_and_consistent_kind_relabel(
    tmp_path: Path,
) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    first = queue.submit_job(_job("lease-binding-first"))
    lease = queue.acquire_job(first.job_id, "same-worker", cluster="ares")
    assert lease is not None
    leased_job = queue.get_job(first.job_id)
    identity = queue._lease_index_identity(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        lease,
        job=leased_job,
    )

    endpoint_ref = queue._lease_endpoint_ref_path(identity)  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    endpoint_guard = queue._lease_endpoint_guard_path(identity)  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    endpoint_ref.unlink()
    with pytest.raises(QueueConflictError, match="references and guards disagree"):
        queue.acquire_next_job("same-worker", cluster="ares")

    queue.repair_lease_operational_indexes()
    endpoint_ref.unlink()
    endpoint_guard.unlink()
    with pytest.raises(QueueConflictError, match="endpoint and expiry indexes disagree"):
        queue.acquire_next_job("same-worker", cluster="ares")

    queue.repair_lease_operational_indexes()
    original_kind_ref = queue._lease_cluster_kind_ref_path(identity)  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    original_expiry_ref = queue._lease_expiry_ref_path(identity)  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    relabeled = replace(identity, job_kind=JobKind.MCP_CALL)
    original_kind_ref.unlink()
    original_expiry_ref.unlink()
    for path in (
        queue._lease_cluster_kind_ref_path(relabeled),  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        queue._lease_expiry_ref_path(relabeled),  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    ):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.touch()
    with pytest.raises(QueueConflictError, match="identity and expiry indexes disagree"):
        queue._active_lease_counts_by_kind(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            cluster="ares"
        )


def test_lease_reference_hardlinks_fail_closed(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    saved = queue.submit_job(_job("lease-hardlink-corruption"))
    lease = queue.acquire_job(saved.job_id, "hardlink-worker", cluster="ares")
    assert lease is not None
    identity = queue._lease_index_identity(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        lease,
        job=queue.get_job(saved.job_id),
    )
    identity_ref = queue._lease_identity_ref_path(identity)  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    source = tmp_path / "hardlink-source.ref"
    source.touch()
    identity_ref.unlink()
    os.link(source, identity_ref)
    assert os.lstat(identity_ref).st_nlink == 2

    with pytest.raises(QueueConflictError, match="unsafe reference"):
        queue._active_lease_counts_by_kind(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            cluster="ares"
        )


def test_lease_index_writes_reject_unsafe_operational_parent(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    queue.initialize()
    parent = queue.root / "lease_indexes"
    parent.rmdir()
    parent.write_text("not-an-owned-directory", encoding="utf-8")
    lease = Lease.new("job-safe-parent", "worker-safe-parent", 300)
    identity = queue._lease_index_identity(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        lease,
        job=_job("safe-parent").model_copy(
            update={"job_id": lease.job_id, "state": JobState.RUNNING}
        ),
    )

    with pytest.raises(QueueConflictError, match="ancestry is unsafe"):
        queue._write_lease_index_identity_unlocked(identity)  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001


def test_lease_index_repair_intent_rebuilds_after_clear_crash(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "core"
    queue = ClioCoreQueue(root)
    saved = queue.submit_job(_job("repair-clear-crash"))
    lease = queue.acquire_job(saved.job_id, "repair-worker", cluster="ares")
    assert lease is not None

    def crash_during_rebuild(*_args: object, **_kwargs: object) -> object:
        raise RuntimeError("simulated repair rebuild crash")

    monkeypatch.setattr(
        queue,
        "_sync_lease_operational_indexes_unlocked",
        crash_during_rebuild,
    )
    with pytest.raises(RuntimeError, match="repair rebuild crash"):
        queue.repair_lease_operational_indexes()

    restarted = ClioCoreQueue(root)
    restarted.initialize()

    assert restarted._active_lease_for_endpoint("repair-worker") == lease  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    assert list((root / "transition_intents").glob("*.json")) == []


def test_exact_target_lease_wal_preserves_near_limit_job_domain(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    submitted = queue.submit_job(
        _job(
            "large-exact-target-wal",
            metadata={"bounded_payload": "x" * 600_000},
        )
    )
    lease = queue.acquire_job(
        submitted.job_id,
        "large-job-worker",
        cluster="ares",
        ttl_seconds=-1,
    )
    assert lease is not None

    recovered = queue.recover_stale_job(submitted.job_id, cluster="ares")

    assert recovered is not None
    assert recovered.state is JobState.QUEUED
    assert queue.scan_job_leases(submitted.job_id, limit=10) == ([], False)


def test_idempotent_replay_repairs_crash_gap_after_canonical_job_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "core"
    queue = ClioCoreQueue(root)
    job = _job(
        "crash-repair",
        metadata={
            "owner": "clio-relay",
            "owner_session_id": "desktop",
            "owner_session_generation_id": "generation-1",
        },
    )

    def crash_after_canonical(record: RelayJob) -> None:
        (root / "jobs").mkdir(parents=True, exist_ok=True)
        (root / "jobs" / f"{record.job_id}.json").write_text(
            record.model_dump_json(),
            encoding="utf-8",
        )
        raise RuntimeError("simulated hard crash")

    with monkeypatch.context() as crash:
        crash.setattr(queue, "_write_job_unlocked", crash_after_canonical)
        with pytest.raises(RuntimeError, match="simulated hard crash"):
            queue.submit_job(job)

    replayed = ClioCoreQueue(root).submit_job(job)
    membership, next_cursor, total, source_count = ClioCoreQueue(root).list_owner_session_jobs_page(
        "desktop",
        session_generation_id="generation-1",
        include_terminal=True,
    )

    assert replayed.job_id == job.job_id
    assert (root / "jobs_active" / f"{job.job_id}.json").is_file()
    assert (root / "job_indexes" / f"{job.job_id}.json").is_file()
    assert [item.job_id for item in membership] == [job.job_id]
    assert next_cursor is None
    assert total == source_count == 1


@pytest.mark.parametrize(
    "mode",
    [
        "terminal",
        "lease",
        "lease_after_index",
        "task",
        "stale_before_job_exact",
        "stale_after_job_exact",
        "stale_after_canonical_exact",
        "stale_after_index_exact",
        "stale_before_job_bulk",
        "stale_after_job_bulk",
        "stale_after_canonical_bulk",
        "stale_after_index_bulk",
        "release_after_canonical",
        "release_after_index",
        "gateway_close",
    ],
)
def test_hard_exit_queue_transitions_converge_on_restart(tmp_path: Path, mode: str) -> None:
    marker = tmp_path / f"{mode}.json"
    crashed = subprocess.run(
        [
            sys.executable,
            "-m",
            "tests.queue_transition_crash_fixture",
            str(tmp_path),
            str(marker),
            mode,
        ],
        cwd=Path(__file__).parents[1],
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert crashed.returncode == 83, crashed.stderr
    identity = json.loads(marker.read_text(encoding="utf-8"))
    job_id = identity["job_id"]
    assert isinstance(job_id, str)

    queue = ClioCoreQueue(tmp_path / "core")
    queue.initialize()
    job = queue.get_job(job_id)
    assert list((queue.root / "transition_intents").glob("*.json")) == []

    if mode == "terminal":
        assert job.state is JobState.SUCCEEDED
        assert not (queue.root / "jobs_active" / f"{job_id}.json").exists()
        assert not (queue.root / "jobs_queued" / f"{job_id}.json").exists()
        assert queue.active_job_capacity()["used"] == 0
    elif mode.startswith("lease"):
        assert job.state is JobState.QUEUED
        assert job.leased_by is None
        leases, truncated = queue.scan_job_leases(job_id, limit=10)
        assert leases == []
        assert truncated is False
        assert (queue.root / "jobs_active" / f"{job_id}.json").is_file()
        assert (queue.root / "jobs_queued" / f"{job_id}.json").is_file()
        assert _lease_operational_files(queue) == []
    elif mode == "task":
        task_id = identity["task_id"]
        assert isinstance(task_id, str)
        task = queue.get_task(task_id)
        assert isinstance(task, RelayTask)
        indexed_tasks, truncated = queue.scan_job_tasks(job_id, limit=10)
        assert truncated is False
        assert [item.task_id for item in indexed_tasks] == [task_id]
        manifests = list((queue.root / "scheduler_refs_by_job" / job_id).glob("*.json"))
        assert len(manifests) == 1
        manifest = json.loads(manifests[0].read_text(encoding="utf-8"))
        assert manifest["scheduler_ids"] == ["scheduler-hard-crash"]
        reverse_ref = queue._scheduler_reverse_ref_path(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            "scheduler-hard-crash",
            job_id,
            f"task:{task_id}",
        )
        assert reverse_ref.is_file()
    elif mode.startswith("stale_"):
        assert job.state is JobState.QUEUED
        exact_leases, exact_truncated = queue.scan_job_leases(job_id, limit=10)
        global_leases, global_truncated = queue.scan_leases(limit=10)
        assert exact_leases == []
        assert global_leases == []
        assert exact_truncated is global_truncated is False
        assert _lease_operational_files(queue) == []
        recovery_events = [
            json.loads(path.read_text(encoding="utf-8"))
            for path in (queue.root / "events" / job_id).glob("*.json")
            if json.loads(path.read_text(encoding="utf-8"))["event_type"] == "job.requeued"
        ]
        assert len(recovery_events) == 1
        assert len(recovery_events[0]["payload"]["expired_lease_ids"]) == 2
    elif mode.startswith("release_"):
        exact_leases, exact_truncated = queue.scan_job_leases(job_id, limit=10)
        global_leases, global_truncated = queue.scan_leases(limit=10)
        assert exact_leases == []
        assert global_leases == []
        assert exact_truncated is global_truncated is False
        assert _lease_operational_files(queue) == []
    else:
        session_id = identity["session_id"]
        assert isinstance(session_id, str)
        session = queue.get_gateway_session(session_id)
        assert session.state is GatewaySessionState.CLOSED
        assert list((queue.root / "active_gateway_refs_by_job" / job_id).glob("*.json")) == []
        assert "active_gateway_record" not in queue.plan_terminal_job_gc(job_id).protections


def test_core_rejects_new_owned_work_after_generation_quiescence(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    generation = queue.prepare_owner_session_start(
        "desktop",
        recorded_generation_id=None,
        candidate_generation_id="generation-1",
    )
    metadata: dict[str, object] = {
        "owner": "clio-relay",
        "owner_session_id": "desktop",
        "owner_session_generation_id": generation,
    }
    queue.set_owner_session_closing("desktop", session_generation_id=generation)

    with pytest.raises(QueueConflictError, match="closing and rejects new work"):
        queue.submit_job(_job("late-owned-job", metadata=metadata))
    with pytest.raises(QueueConflictError, match="closing and rejects new work"):
        queue.create_gateway_session(
            GatewaySession(cluster="ares", name="late-gateway", metadata=metadata)
        )


def test_terminal_membership_survives_active_index_removal(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    job = queue.submit_job(
        _job(
            "terminal-membership",
            metadata={
                "owner": "clio-relay",
                "owner_session_id": "desktop",
                "owner_session_generation_id": "generation-1",
            },
        )
    )
    queue.update_job_state(job.job_id, JobState.SUCCEEDED)

    active, _, _, active_source_count = queue.list_owner_session_jobs_page(
        "desktop",
        session_generation_id="generation-1",
    )
    terminal, _, _, terminal_source_count = queue.list_owner_session_jobs_page(
        "desktop",
        session_generation_id="generation-1",
        include_terminal=True,
    )

    assert active == []
    assert active_source_count == 1
    assert [item.job_id for item in terminal] == [job.job_id]
    assert terminal_source_count == 1


def test_diagnosis_finds_fresh_worker_without_scanning_endpoint_history(
    tmp_path: Path,
) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    job = queue.submit_job(_job("fresh-worker-diagnosis"))
    queue.register_endpoint(
        EndpointRegistration(
            role=EndpointRole.WORKER,
            cluster="ares",
            hostname="current-worker",
            pid=123,
        )
    )
    for index in range(1_001):
        (queue.root / "endpoints" / f"historical-{index:04d}.json").touch()

    diagnosis = diagnose_job(queue, job.job_id, cluster="ares")

    worker = diagnosis["worker"]
    assert isinstance(worker, dict)
    assert worker["healthy_worker_count"] == 1


def test_stale_discovery_traverses_full_admitted_active_capacity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    submitted = queue.submit_job(_job("late-active-id"))
    old = utc_now() - timedelta(hours=3)
    aged = submitted.model_copy(update={"created_at": old, "updated_at": old})

    def full_active_population(*, limit: int) -> tuple[list[RelayJob], bool]:
        assert limit == MAX_ACTIVE_JOB_RECORDS
        return [aged], False

    monkeypatch.setattr(queue, "scan_active_jobs", full_active_population)

    result = discover_stale_jobs(
        queue,
        cluster="ares",
        older_than_seconds=60,
    )

    assert result["count"] == 1
    jobs = result["jobs"]
    assert isinstance(jobs, list)
    first = cast(list[object], jobs)[0]
    assert isinstance(first, dict)
    job_payload = cast(dict[str, object], first)["job"]
    assert isinstance(job_payload, dict)
    assert job_payload["job_id"] == submitted.job_id

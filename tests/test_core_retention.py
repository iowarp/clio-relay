from __future__ import annotations

import json
import os
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, cast

import pytest

from clio_relay.core_queue import (
    RECORD_FAMILY_MAX_BYTES,
    ClioCoreQueue,
    _purge_tree_batch,  # pyright: ignore[reportPrivateUsage]
)
from clio_relay.errors import NotFoundError, QueueConflictError
from clio_relay.models import (
    ArtifactRef,
    GatewaySession,
    JarvisRunSpec,
    JobGcPhase,
    JobKind,
    JobState,
    MonitorRule,
    ProgressRecord,
    RelayJob,
    RelayTask,
    StorageReservationEstimate,
)


def _intent(key: str, *, metadata: dict[str, object] | None = None) -> RelayJob:
    return RelayJob(
        cluster="test-cluster",
        kind=JobKind.JARVIS,
        spec=JarvisRunSpec(command=["true"]),
        idempotency_key=key,
        metadata=metadata or {},
    )


def _terminal_job(queue: ClioCoreQueue, key: str) -> tuple[RelayJob, RelayJob]:
    intent = _intent(key)
    submitted = queue.submit_job(intent)
    terminal = queue.update_job_state(submitted.job_id, JobState.SUCCEEDED)
    return intent, terminal


def _finish_gc(queue: ClioCoreQueue, job_id: str, *, batch_size: int = 7) -> None:
    for _ in range(1_000):
        result = queue.collect_terminal_job(
            job_id,
            execute=True,
            batch_size=batch_size,
            external_quarantine_id=f"test-quarantine:{job_id}",
        )
        if result.complete:
            return
    raise AssertionError("terminal job GC did not complete within its bounded batches")


def test_terminal_gc_is_dry_run_by_default_and_requires_external_quarantine(
    tmp_path: Path,
) -> None:
    queue = ClioCoreQueue(tmp_path)
    _intent_record, job = _terminal_job(queue, "gc-dry-run")

    dry_run = queue.collect_terminal_job(job.job_id)
    refused = queue.collect_terminal_job(job.job_id, execute=True)

    assert dry_run.dry_run is True
    assert dry_run.plan.eligible is True
    assert queue.get_job(job.job_id).state is JobState.SUCCEEDED
    assert queue.get_job_tombstone(job.job_id) is None
    assert refused.dry_run is False
    assert refused.plan.eligible is False
    assert refused.plan.protections == ["external_spool_quarantine_unconfirmed"]


def test_terminal_gc_uses_original_submission_digest_after_operational_metadata_mutates(
    tmp_path: Path,
) -> None:
    queue = ClioCoreQueue(tmp_path / "mutable")
    intent, job = _terminal_job(queue, "gc-original-submission-digest")
    job = queue.update_job_metadata(
        job.job_id,
        {
            "runtime_metadata": {
                "scheduler_job_id": "123",
                "phase": "completed",
                "scheduler": "slurm",
            },
            "cancellation_request": {
                "cancel_scheduler": False,
                "cleanup_acknowledged": True,
            },
            "operational_attempt": 3,
        },
    )
    assert job.submission_digest is not None
    assert queue.plan_terminal_job_gc(job.job_id).eligible is True
    _finish_gc(queue, job.job_id)
    replay = queue.submit_job(intent)
    assert replay.job_id == job.job_id
    assert replay.state is JobState.SUCCEEDED

    for case, replacement in (("tampered", "0" * 64), ("missing", None)):
        forged_queue = ClioCoreQueue(tmp_path / case)
        _intent_record, forged_job = _terminal_job(forged_queue, f"gc-digest-{case}")
        record_path = next((forged_queue.root / "idempotency").glob("*.json"))
        record = json.loads(record_path.read_text(encoding="utf-8"))
        if replacement is None:
            del record["job_digest"]
        else:
            record["job_digest"] = replacement
        record_path.write_text(json.dumps(record), encoding="utf-8")
        assert (
            "idempotency_record_ambiguous"
            in forged_queue.plan_terminal_job_gc(forged_job.job_id).protections
        )


def test_explicit_storage_reservation_is_part_of_idempotency_identity(
    tmp_path: Path,
) -> None:
    queue = ClioCoreQueue(tmp_path)
    original = _intent("storage-reservation-digest").model_copy(
        update={
            "storage_reservation": StorageReservationEstimate(
                core_bytes=1_024,
                spool_bytes=2_048,
            )
        }
    )
    submitted = queue.submit_job(original)
    assert queue.submit_job(original).job_id == submitted.job_id

    changed = original.model_copy(
        update={
            "storage_reservation": StorageReservationEstimate(
                core_bytes=2_048,
                spool_bytes=2_048,
            )
        }
    )
    with pytest.raises(QueueConflictError, match="different job payload"):
        queue.submit_job(changed)


@pytest.mark.parametrize("fault_phase", list(JobGcPhase))
def test_terminal_gc_resumes_after_every_phase_and_never_reexecutes_idempotency(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fault_phase: JobGcPhase,
) -> None:
    queue = ClioCoreQueue(tmp_path / fault_phase.value)
    intent, job = _terminal_job(queue, f"gc-fault-{fault_phase.value}")
    task = queue.append_task(RelayTask(job_id=job.job_id, name="completed-task"))
    queue.update_task_state(task.task_id, JobState.SUCCEEDED)
    queue.append_artifact(
        ArtifactRef(job_id=job.job_id, uri=(tmp_path / "artifact").as_uri(), kind="result")
    )
    queue.append_progress(ProgressRecord(job_id=job.job_id, current=1, total=1))
    injected = False

    def fail_after_checkpoint(phase: JobGcPhase) -> None:
        nonlocal injected
        if phase is fault_phase and not injected:
            injected = True
            raise RuntimeError(f"fault after {phase.value}")

    monkeypatch.setattr(queue, "_after_gc_checkpoint", fail_after_checkpoint)
    with pytest.raises(RuntimeError, match=f"fault after {fault_phase.value}"):
        for _ in range(1_000):
            queue.collect_terminal_job(
                job.job_id,
                execute=True,
                batch_size=100,
                external_quarantine_id=f"fault-test:{job.job_id}",
            )
    assert injected is True
    replay = queue.submit_job(intent)
    assert replay.job_id == job.job_id
    assert replay.state is JobState.SUCCEEDED

    def accept_checkpoint(_phase: JobGcPhase) -> None:
        return

    monkeypatch.setattr(queue, "_after_gc_checkpoint", accept_checkpoint)
    _finish_gc(queue, job.job_id)

    tombstone = queue.get_job_tombstone(job.job_id)
    assert tombstone is not None
    assert tombstone.phase is JobGcPhase.COMPLETE
    assert tombstone.external_quarantine_id == f"fault-test:{job.job_id}"
    with pytest.raises(NotFoundError):
        queue.get_job(job.job_id)
    final_replay = queue.submit_job(intent)
    assert final_replay.job_id == job.job_id
    assert final_replay.state is JobState.SUCCEEDED


def test_terminal_gc_protects_active_lease_scheduler_gateway_and_owner_records(
    tmp_path: Path,
) -> None:
    queue = ClioCoreQueue(tmp_path)
    active = queue.submit_job(_intent("gc-active"))
    assert queue.plan_terminal_job_gc(active.job_id).protections == ["job_not_terminal"]

    _lease_intent, leased = _terminal_job(queue, "gc-lease")
    lease = queue.acquire_job(leased.job_id, "worker", cluster="test-cluster")
    assert lease is None
    # A forged residual lease index is ambiguous and must protect the job.
    lease_dir = tmp_path / "leases_by_job" / leased.job_id
    lease_dir.mkdir(parents=True, exist_ok=True)
    (lease_dir / "ambiguous.json").write_text("{}", encoding="utf-8")
    assert "lease_records_ambiguous" in queue.plan_terminal_job_gc(leased.job_id).protections

    _scheduler_intent, scheduler_job = _terminal_job(queue, "gc-scheduler")
    scheduler_task = queue.append_task(
        RelayTask(
            job_id=scheduler_job.job_id,
            name="scheduler-task",
            state=JobState.SUCCEEDED,
            metadata={"scheduler_job_ids": ["12345"]},
        )
    )
    assert scheduler_task.state is JobState.SUCCEEDED
    assert (
        "scheduler_state_active_or_ambiguous"
        in queue.plan_terminal_job_gc(scheduler_job.job_id).protections
    )

    _gateway_intent, gateway_job = _terminal_job(queue, "gc-gateway")
    queue.create_gateway_session(
        GatewaySession(
            cluster="test-cluster",
            name="active-gateway",
            metadata={"relay_job_id": gateway_job.job_id},
        )
    )
    assert "active_gateway_record" in queue.plan_terminal_job_gc(gateway_job.job_id).protections

    owner_job = queue.submit_job(
        _intent(
            "gc-owner",
            metadata={
                "owner_session_id": "session",
                "owner_session_generation_id": "unclosed-generation",
            },
        )
    )
    queue.update_job_state(owner_job.job_id, JobState.SUCCEEDED)
    assert (
        "owner_session_state_ambiguous" in queue.plan_terminal_job_gc(owner_job.job_id).protections
    )

    closed_owner = queue.submit_job(
        _intent(
            "gc-owner-closed",
            metadata={
                "owner_session_id": "closed-session",
                "owner_session_generation_id": "generation-1",
            },
        )
    )
    queue.update_job_state(closed_owner.job_id, JobState.SUCCEEDED)
    assert (
        queue.prepare_owner_session_start(
            "closed-session",
            recorded_generation_id=None,
            candidate_generation_id="generation-1",
        )
        == "generation-1"
    )
    queue.set_owner_session_closing(
        "closed-session",
        session_generation_id="generation-1",
    )
    queue.set_owner_session_closed(
        "closed-session",
        session_generation_id="generation-1",
    )
    assert queue.plan_terminal_job_gc(closed_owner.job_id).eligible is True
    queue.reopen_owner_session(
        "closed-session",
        previous_session_generation_id="generation-1",
        session_generation_id="generation-2",
    )
    assert queue.plan_terminal_job_gc(closed_owner.job_id).eligible is True
    assert (
        queue.get_owner_session_closed(
            "closed-session",
            session_generation_id="generation-1",
        )
        is not None
    )
    assert (
        queue.get_owner_session_closed(
            "closed-session",
            session_generation_id="generation-2",
        )
        is None
    )
    generation_two_job = queue.submit_job(
        _intent(
            "gc-owner-generation-2",
            metadata={
                "owner_session_id": "closed-session",
                "owner_session_generation_id": "generation-2",
            },
        )
    )
    queue.update_job_state(generation_two_job.job_id, JobState.SUCCEEDED)
    assert (
        "owner_session_state_ambiguous"
        in queue.plan_terminal_job_gc(generation_two_job.job_id).protections
    )

    queue.prepare_owner_session_start(
        "generation-mismatch-session",
        recorded_generation_id=None,
        candidate_generation_id="generation-1",
    )
    queue.set_owner_session_closing(
        "generation-mismatch-session",
        session_generation_id="generation-1",
    )
    with pytest.raises(QueueConflictError, match="generation changed"):
        queue.set_owner_session_closed(
            "generation-mismatch-session",
            session_generation_id="generation-2",
        )

    residual_owner = queue.submit_job(
        _intent(
            "gc-owner-residual",
            metadata={
                "owner_session_id": "residual-session",
                "owner_session_generation_id": "residual-generation",
            },
        )
    )
    queue.update_job_state(residual_owner.job_id, JobState.SUCCEEDED)
    queue.prepare_owner_session_start(
        "residual-session",
        recorded_generation_id=None,
        candidate_generation_id="residual-generation",
    )
    queue.set_owner_session_closing(
        "residual-session",
        session_generation_id="residual-generation",
    )
    queue.set_owner_session_closed(
        "residual-session",
        session_generation_id="residual-generation",
        residual_resource_ids=["connector:still-running"],
    )
    assert (
        "owner_session_residual_resources"
        in queue.plan_terminal_job_gc(residual_owner.job_id).protections
    )


def test_terminal_gc_uses_per_job_monitor_and_gateway_reference_indexes(
    tmp_path: Path,
) -> None:
    queue = ClioCoreQueue(tmp_path)
    _intent_record, monitor_job = _terminal_job(queue, "gc-monitor-index")
    rule = queue.append_monitor_rule(MonitorRule(job_id=monitor_job.job_id, pattern="done"))
    assert "enabled_monitor_rule" in queue.plan_terminal_job_gc(monitor_job.job_id).protections
    queue.update_monitor_rule(rule.model_copy(update={"enabled": False}))
    assert queue.plan_terminal_job_gc(monitor_job.job_id).eligible is True

    _intent_record, artifact_job = _terminal_job(queue, "gc-gateway-artifact-index")
    artifact = queue.append_artifact(
        ArtifactRef(job_id=artifact_job.job_id, uri=(tmp_path / "linked").as_uri(), kind="result")
    )
    artifact_gateway = queue.create_gateway_session(
        GatewaySession(
            cluster="test-cluster",
            name="artifact-gateway",
            artifacts=[artifact.artifact_id],
        )
    )
    assert "active_gateway_record" in queue.plan_terminal_job_gc(artifact_job.job_id).protections
    queue.close_gateway_session(artifact_gateway.session_id)
    assert queue.plan_terminal_job_gc(artifact_job.job_id).eligible is True

    _intent_record, scheduler_job = _terminal_job(queue, "gc-gateway-scheduler-index")
    scheduler_task = queue.append_task(
        RelayTask(
            job_id=scheduler_job.job_id,
            name="terminal-scheduler-task",
            state=JobState.SUCCEEDED,
            metadata={"scheduler_status": {"scheduler_job_id": "777", "phase": "completed"}},
        )
    )
    assert scheduler_task.state is JobState.SUCCEEDED
    scheduler_gateway = queue.create_gateway_session(
        GatewaySession(
            cluster="test-cluster",
            name="scheduler-gateway",
            scheduler_job_id="777",
        )
    )
    assert "active_gateway_record" in queue.plan_terminal_job_gc(scheduler_job.job_id).protections
    queue.close_gateway_session(scheduler_gateway.session_id)
    assert queue.plan_terminal_job_gc(scheduler_job.job_id).eligible is True


def test_terminal_task_pending_cleanup_blocks_gc_until_acknowledged(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path)
    _intent_record, job = _terminal_job(queue, "gc-pending-execution-cleanup")
    task = queue.append_task(
        RelayTask(
            job_id=job.job_id,
            name="terminal-with-sidecars",
            state=JobState.SUCCEEDED,
            metadata={"cluster": "test-cluster"},
        )
    )
    queue.register_execution_cleanup(
        task.task_id,
        {
            "cluster": "test-cluster",
            "execution_sidecars": {"progress": "progress.jsonl"},
        },
    )
    plan = queue.plan_terminal_job_gc(job.job_id)
    assert "pending_execution_cleanup" in plan.protections
    assert plan.eligible is False

    queue.acknowledge_execution_cleanup(
        job.job_id,
        task.task_id,
        metadata={"execution_sidecars_removed": True},
    )
    assert queue.plan_terminal_job_gc(job.job_id).eligible is True


def test_terminal_gc_scales_past_501_owned_and_unrelated_records(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path)
    _intent_record, job = _terminal_job(queue, "gc-large-owned")
    unrelated = queue.submit_job(_intent("gc-large-unrelated"))
    for index in range(502):
        task = RelayTask(job_id=job.job_id, name=f"terminal-{index}", state=JobState.SUCCEEDED)
        artifact = ArtifactRef(
            job_id=job.job_id,
            uri=(tmp_path / f"owned-{index}").as_uri(),
            kind="result",
        )
        rule = MonitorRule(job_id=job.job_id, pattern=f"done-{index}", enabled=False)
        for family, record_id, record in (
            ("tasks", task.task_id, task),
            ("tasks_by_job", task.task_id, task),
            ("artifacts", artifact.artifact_id, artifact),
            ("artifacts_by_job", artifact.artifact_id, artifact),
            ("monitor_rules", rule.rule_id, rule),
            ("monitor_rules_by_job", rule.rule_id, rule),
        ):
            directory = tmp_path / family
            if family.endswith("_by_job"):
                directory /= job.job_id
            directory.mkdir(parents=True, exist_ok=True)
            (directory / f"{record_id}.json").write_text(
                record.model_dump_json(),
                encoding="utf-8",
            )
        unrelated_rule = MonitorRule(
            job_id=unrelated.job_id,
            pattern=f"unrelated-{index}",
            enabled=False,
        )
        (tmp_path / "monitor_rules" / f"{unrelated_rule.rule_id}.json").write_text(
            unrelated_rule.model_dump_json(),
            encoding="utf-8",
        )
    assert queue.plan_terminal_job_gc(job.job_id).eligible is True
    _finish_gc(queue, job.job_id, batch_size=100)
    assert queue.get_job_tombstone(job.job_id) is not None


def test_legacy_retention_indexes_migrate_in_bounded_batches(tmp_path: Path) -> None:
    for family in (
        "jobs",
        "tasks",
        "artifacts",
        "monitor_rules",
        "gateway_sessions",
    ):
        (tmp_path / family).mkdir(parents=True, exist_ok=True)
    job = _intent("legacy-retention-index")
    task = RelayTask(job_id=job.job_id, name="done", state=JobState.SUCCEEDED)
    artifact = ArtifactRef(
        job_id=job.job_id,
        uri=(tmp_path / "legacy-artifact").as_uri(),
        kind="result",
    )
    rule = MonitorRule(job_id=job.job_id, pattern="done", enabled=False)
    gateway = GatewaySession(
        cluster="test-cluster",
        name="legacy-gateway",
        metadata={"relay_job_id": job.job_id},
    )
    for family, record_id, record in (
        ("jobs", job.job_id, job),
        ("tasks", task.task_id, task),
        ("artifacts", artifact.artifact_id, artifact),
        ("monitor_rules", rule.rule_id, rule),
        ("gateway_sessions", gateway.session_id, gateway),
    ):
        (tmp_path / family / f"{record_id}.json").write_text(
            record.model_dump_json(),
            encoding="utf-8",
        )

    queue = ClioCoreQueue(tmp_path)
    state = queue.index_migration_status()
    batches = 0
    while state["complete"] is not True:
        state = queue.migrate_indexes_batch(batch_size=1)
        batches += 1
        assert batches < 30

    assert (tmp_path / "monitor_rules_by_job" / job.job_id / f"{rule.rule_id}.json").is_file()
    assert not any((tmp_path / "active_tasks_by_job" / job.job_id).iterdir())
    assert not any((tmp_path / "active_monitor_rules_by_job" / job.job_id).iterdir())
    assert any((tmp_path / "active_gateway_refs_by_job" / job.job_id).iterdir())
    queue.close_gateway_session(gateway.session_id)
    assert not any((tmp_path / "active_gateway_refs_by_job" / job.job_id).iterdir())


def test_gc_purge_is_iterative_and_detects_directory_swap_races(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    deep_root = tmp_path / "deep-trash"
    deep_root.mkdir()
    leaf = deep_root
    depth = 40 if os.name == "nt" else 1_100
    for _ in range(depth):
        leaf /= "d"
        leaf.mkdir()
    (leaf / "record.json").write_text("{}", encoding="utf-8")
    complete = False
    for _ in range(depth + 2):
        _removed, complete = _purge_tree_batch(deep_root, limit=100)
        if complete:
            break
    assert complete is True

    race_root = tmp_path / "race-trash"
    race_root.mkdir()
    (race_root / "record.json").write_text("{}", encoding="utf-8")
    moved_root = tmp_path / "race-trash-original"
    original_scandir = os.scandir

    class _SwapAfterScan:
        def __init__(self, path: os.PathLike[str] | str) -> None:
            self._path = Path(path)
            self._context: Any = original_scandir(path)

        def __enter__(self) -> Iterator[os.DirEntry[str]]:
            return cast(Iterator[os.DirEntry[str]], self._context.__enter__())

        def __exit__(self, *args: object) -> None:
            self._context.__exit__(*args)
            if self._path == race_root and not moved_root.exists():
                race_root.replace(moved_root)
                race_root.mkdir()

    monkeypatch.setattr(os, "scandir", _SwapAfterScan)
    with pytest.raises(QueueConflictError, match="changed during traversal"):
        _purge_tree_batch(race_root, limit=1)
    assert (moved_root / "record.json").is_file()


def test_terminal_gc_refuses_symlink_or_reparse_roots_without_following_them(
    tmp_path: Path,
) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    _intent_record, job = _terminal_job(queue, "gc-symlink")
    event_dir = queue.root / "events" / job.job_id
    outside = tmp_path / "outside"
    outside.mkdir()
    marker = outside / "marker"
    marker.write_text("must remain", encoding="utf-8")
    original = queue.root / "events" / f"{job.job_id}-original"
    event_dir.replace(original)
    try:
        event_dir.symlink_to(outside, target_is_directory=True)
    except OSError:
        assert os.name == "nt"
        original.replace(event_dir)
        _finish_gc(queue, job.job_id)
    else:
        with pytest.raises(QueueConflictError, match="symlink or reparse-point"):
            queue.collect_terminal_job(
                job.job_id,
                execute=True,
                external_quarantine_id=f"symlink-test:{job.job_id}",
            )
        assert marker.read_text(encoding="utf-8") == "must remain"


def test_owner_session_generation_state_machine_is_atomic_and_crash_resumable(
    tmp_path: Path,
) -> None:
    queue = ClioCoreQueue(tmp_path)
    assert (
        queue.prepare_owner_session_start(
            "state-machine-session",
            recorded_generation_id=None,
            candidate_generation_id="generation-a",
        )
        == "generation-a"
    )
    assert (
        queue.prepare_owner_session_start(
            "state-machine-session",
            recorded_generation_id=None,
            candidate_generation_id="ignored-candidate",
        )
        == "generation-a"
    )
    with pytest.raises(QueueConflictError, match="does not match active"):
        queue.prepare_owner_session_start(
            "state-machine-session",
            recorded_generation_id="unrelated-generation",
            candidate_generation_id="generation-b",
        )
    with pytest.raises(QueueConflictError, match="active generation"):
        queue.set_owner_session_closing(
            "state-machine-session",
            session_generation_id="generation-b",
        )
    queue.set_owner_session_closing(
        "state-machine-session",
        session_generation_id="generation-a",
    )
    with pytest.raises(QueueConflictError, match="cannot be cleared"):
        queue.clear_owner_session_closing(
            "state-machine-session",
            session_generation_id="generation-a",
        )
    queue.set_owner_session_closed(
        "state-machine-session",
        session_generation_id="generation-a",
    )
    assert (
        queue.prepare_owner_session_start(
            "state-machine-session",
            recorded_generation_id="generation-a",
            candidate_generation_id="generation-b",
        )
        == "generation-b"
    )
    # Simulate metadata still recording A after the durable A -> B transition.
    assert (
        queue.prepare_owner_session_start(
            "state-machine-session",
            recorded_generation_id="generation-a",
            candidate_generation_id="discarded-generation-c",
        )
        == "generation-b"
    )
    queue.clear_owner_session_closing(
        "state-machine-session",
        session_generation_id="generation-b",
    )


def test_owner_session_initial_activation_race_selects_one_authoritative_generation(
    tmp_path: Path,
) -> None:
    def prepare(candidate: str) -> str:
        return ClioCoreQueue(tmp_path).prepare_owner_session_start(
            "activation-race",
            recorded_generation_id=None,
            candidate_generation_id=candidate,
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        selected = list(executor.map(prepare, ["generation-a", "generation-b"]))
    assert len(set(selected)) == 1
    assert selected[0] in {"generation-a", "generation-b"}


def test_legacy_unversioned_jobs_require_exact_immutable_closure_coverage(
    tmp_path: Path,
) -> None:
    queue = ClioCoreQueue(tmp_path)
    submitted = queue.submit_job(_intent("legacy-unversioned-owner"))
    legacy = submitted.model_copy(update={"metadata": {"owner_session_id": "legacy-session"}})
    for family in ("jobs", "jobs_active", "jobs_queued"):
        (tmp_path / family / f"{legacy.job_id}.json").write_text(
            legacy.model_dump_json(),
            encoding="utf-8",
        )
    queue.update_job_state(legacy.job_id, JobState.SUCCEEDED)
    assert "owner_session_state_ambiguous" in queue.plan_terminal_job_gc(legacy.job_id).protections

    queue.prepare_owner_session_start(
        "legacy-session",
        recorded_generation_id=None,
        candidate_generation_id="upgrade-generation",
    )
    queue.set_owner_session_closing(
        "legacy-session",
        session_generation_id="upgrade-generation",
    )
    queue.set_owner_session_closed(
        "legacy-session",
        session_generation_id="upgrade-generation",
        legacy_unversioned_job_ids=[legacy.job_id],
    )
    assert queue.plan_terminal_job_gc(legacy.job_id).eligible is True
    legacy_closure = queue.get_owner_session_closed("legacy-session")
    assert legacy_closure is not None
    assert legacy_closure.covered_legacy_job_ids == [legacy.job_id]
    assert legacy_closure.covered_by_session_generation_id == "upgrade-generation"

    legacy_closure_path = tmp_path / "owner_sessions" / "legacy-session.closed.json"
    original_closure = legacy_closure_path.read_text(encoding="utf-8")
    orphaned_closure = json.loads(original_closure)
    orphaned_closure["covered_by_session_generation_id"] = "missing-generation"
    legacy_closure_path.write_text(json.dumps(orphaned_closure), encoding="utf-8")
    assert (
        "owner_session_legacy_coverage_ambiguous"
        in queue.plan_terminal_job_gc(legacy.job_id).protections
    )

    malformed_closure = json.loads(original_closure)
    malformed_closure["covered_legacy_job_ids"] = [legacy.job_id, legacy.job_id]
    legacy_closure_path.write_text(json.dumps(malformed_closure), encoding="utf-8")
    assert "owner_session_state_ambiguous" in queue.plan_terminal_job_gc(legacy.job_id).protections
    legacy_closure_path.write_text(original_closure, encoding="utf-8")
    assert queue.plan_terminal_job_gc(legacy.job_id).eligible is True

    queue.reopen_owner_session(
        "legacy-session",
        previous_session_generation_id="upgrade-generation",
        session_generation_id="next-generation",
    )
    assert queue.plan_terminal_job_gc(legacy.job_id).eligible is True
    with pytest.raises(QueueConflictError, match="require owner_session_generation_id"):
        queue.submit_job(
            _intent("new-unversioned-owner", metadata={"owner_session_id": "legacy-session"})
        )


def test_core_record_caps_reject_oversized_writes_and_forged_reads(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path)
    oversized = "x" * (RECORD_FAMILY_MAX_BYTES["jobs"] + 1)
    with pytest.raises(QueueConflictError, match="jobs record exceeds"):
        queue.submit_job(_intent("oversized-write", metadata={"oversized": oversized}))

    intent, job = _terminal_job(queue, "oversized-forged-read")
    del intent
    job_path = tmp_path / "jobs" / f"{job.job_id}.json"
    with job_path.open("wb") as stream:
        stream.write(b"{" + b" " * RECORD_FAMILY_MAX_BYTES["jobs"] + b"}")
    with pytest.raises(QueueConflictError, match="jobs record exceeds"):
        queue.get_job(job.job_id)


def test_task_artifact_and_progress_pages_are_stable_and_bounded(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path)
    job = queue.submit_job(_intent("stable-pages"))
    for index in range(3):
        queue.append_task(RelayTask(job_id=job.job_id, name=f"task-{index}"))
        queue.append_artifact(
            ArtifactRef(
                job_id=job.job_id,
                uri=(tmp_path / f"artifact-{index}").as_uri(),
                kind="result",
            )
        )
    for index in range(501):
        queue.append_progress(
            ProgressRecord(job_id=job.job_id, current=index, total=501, label="iteration")
        )

    task_first, task_cursor, task_total = queue.list_tasks_page(job.job_id, limit=2)
    task_second, task_end, _ = queue.list_tasks_page(job.job_id, cursor=task_cursor or 1, limit=2)
    artifact_first, artifact_cursor, artifact_total = queue.list_artifacts_page(job.job_id, limit=2)
    artifact_second, artifact_end, _ = queue.list_artifacts_page(
        job.job_id, cursor=artifact_cursor or 1, limit=2
    )
    assert task_total == artifact_total == 3
    assert task_end is artifact_end is None
    assert [record.sequence for record in [*task_first, *task_second]] == [1, 2, 3]
    assert [record.sequence for record in [*artifact_first, *artifact_second]] == [1, 2, 3]

    cursor: int | None = 1
    progress_ids: list[str] = []
    sequences: list[int | None] = []
    while cursor is not None:
        page, cursor, total = queue.list_progress_page(
            job.job_id,
            cursor=cursor,
            limit=100,
        )
        assert total == 501
        progress_ids.extend(record.progress_id for record in page)
        sequences.extend(record.sequence for record in page)
    assert len(progress_ids) == len(set(progress_ids)) == 501
    assert sequences == list(range(1, 502))
    with pytest.raises(ValueError, match="between 1 and 500"):
        queue.list_progress_page(job.job_id, limit=501)

from __future__ import annotations

import hashlib
import shutil
import threading
import time
from pathlib import Path

import pytest
from filelock import Timeout

import clio_relay.core_queue as core_queue_module
from clio_relay.core_queue import DEFAULT_CORE_LOCK_TIMEOUT_SECONDS, ClioCoreQueue
from clio_relay.errors import QueueConflictError
from clio_relay.models import (
    Cursor,
    GatewaySession,
    GatewaySessionState,
    JarvisRunSpec,
    JobKind,
    JobState,
    MonitorRule,
    MonitorRuleAction,
    ProgressRecord,
    RelayEvent,
    RelayJob,
    RelayTask,
    RemoteAgentTaskSpec,
    TaskEventStatus,
    TaskTimelineEvent,
)
from clio_relay.relay_ops import evaluate_monitor_rules


def test_core_lock_admits_same_process_waiters_in_ticket_order(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path, lock_timeout_seconds=2)
    lock = queue._lock  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    acquired: list[int] = []
    errors: list[BaseException] = []
    threads: list[threading.Thread] = []

    def acquire_after_signal(index: int, started: threading.Event) -> None:
        started.set()
        try:
            with lock:
                assert lock.is_locked is True
                acquired.append(index)
        except Exception as exc:
            errors.append(exc)

    assert lock.is_locked is False
    with lock:
        assert lock.is_locked is True
        with lock:
            assert lock.is_locked is True
        for index in range(4):
            started = threading.Event()
            thread = threading.Thread(
                target=acquire_after_signal,
                args=(index, started),
                name=f"core-lock-waiter-{index}",
            )
            thread.start()
            threads.append(thread)
            assert started.wait(timeout=1)
            expected_next_ticket = index + 2
            deadline = time.monotonic() + 1
            while time.monotonic() < deadline:
                with lock._condition:  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
                    if lock._next_ticket >= expected_next_ticket:  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
                        break
                time.sleep(0.001)
            else:
                pytest.fail(f"waiter {index} did not enter core-lock admission")

    assert lock.is_locked is False
    for thread in threads:
        thread.join(timeout=2)
        assert not thread.is_alive()
    assert errors == []
    assert acquired == [0, 1, 2, 3]


def test_core_lock_default_is_bounded_for_production_contention(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path)
    lock = queue._lock  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001

    assert DEFAULT_CORE_LOCK_TIMEOUT_SECONDS == 30.0
    assert lock.timeout == DEFAULT_CORE_LOCK_TIMEOUT_SECONDS


def test_core_lock_local_wait_is_bounded_and_abandoned_ticket_is_skipped(
    tmp_path: Path,
) -> None:
    queue = ClioCoreQueue(tmp_path, lock_timeout_seconds=0.1)
    lock = queue._lock  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    outcomes: list[BaseException] = []
    elapsed: list[float] = []
    started = threading.Event()

    def bounded_waiter() -> None:
        started.set()
        began = time.monotonic()
        try:
            with lock:
                pytest.fail("waiter acquired a lock that remained locally owned")
        except Exception as exc:
            outcomes.append(exc)
        finally:
            elapsed.append(time.monotonic() - began)

    with lock:
        thread = threading.Thread(target=bounded_waiter, name="bounded-core-lock-waiter")
        thread.start()
        assert started.wait(timeout=1)
        thread.join(timeout=1)
        assert not thread.is_alive()

    assert len(outcomes) == 1
    assert isinstance(outcomes[0], Timeout)
    assert len(elapsed) == 1
    assert 0.05 <= elapsed[0] < 1
    with lock:
        pass


def test_durable_record_read_retries_wrapped_windows_sharing_denial(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue = ClioCoreQueue(tmp_path)
    job = queue.submit_job(
        RelayJob(
            cluster="configured-target",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["true"]),
            idempotency_key="transient-read-sharing",
        )
    )
    task = queue.append_task(RelayTask(job_id=job.job_id, name="sharing-race"))
    task_path = queue.root / "tasks" / f"{task.task_id}.json"
    original = core_queue_module._read_bounded_record_bytes  # pyright: ignore[reportPrivateUsage]
    attempts = 0

    def transient_read(path: Path) -> bytes:
        nonlocal attempts
        if path == task_path and attempts < 2:
            attempts += 1
            try:
                raise PermissionError(13, "Permission denied", str(path))
            except PermissionError as exc:
                raise QueueConflictError(f"cannot read durable record {path}: {exc}") from exc
        return original(path)

    monkeypatch.setattr(core_queue_module, "_read_bounded_record_bytes", transient_read)

    assert queue.get_task(task.task_id) == task
    assert attempts == 2


def test_submit_is_idempotent_and_events_are_ordered(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path)
    first = RelayJob(
        cluster="ares",
        kind=JobKind.JARVIS,
        spec=JarvisRunSpec(command=["echo", "hello"]),
        idempotency_key="same-submit",
    )
    second = RelayJob(
        cluster="ares",
        kind=JobKind.JARVIS,
        spec=JarvisRunSpec(command=["echo", "hello"]),
        idempotency_key="same-submit",
    )

    saved_first = queue.submit_job(first)
    saved_second = queue.submit_job(second)
    queue.append_event(saved_first.job_id, "custom", "custom event")

    events, cursor = queue.drain_events(Cursor(job_id=saved_first.job_id))

    assert saved_second.job_id == saved_first.job_id
    assert [event.seq for event in events] == [1, 2]
    assert [event.event_type for event in events] == ["job.queued", "custom"]
    assert cursor.next_seq == 3


def test_event_pages_are_bounded_contiguous_and_do_not_advance_cursor(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path)
    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["echo", "hello"]),
            idempotency_key="bounded-event-pages",
        )
    )
    queue.append_event(job.job_id, "second", "second")
    queue.append_event(job.job_id, "third", "third")

    first, next_seq = queue.read_event_page(job.job_id, next_seq=1, limit=2)
    second, completed_seq = queue.read_event_page(job.job_id, next_seq=next_seq, limit=2)

    assert [event.seq for event in first] == [1, 2]
    assert [event.seq for event in second] == [3]
    assert completed_seq == 4
    assert not (queue.root / "cursors" / f"{job.job_id}.json").exists()


def test_task_timeline_events_are_durable_and_resumable(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path)
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.REMOTE_AGENT,
            spec=RemoteAgentTaskSpec(prompt_path="/tmp/prompt.md"),
            idempotency_key="task-events",
        )
    )
    task = queue.append_task(RelayTask(job_id=job.job_id, name="remote-agent.discovery"))

    first = queue.append_task_event(
        TaskTimelineEvent(
            task_id=task.task_id,
            event_type="dataset_found",
            label="dataset",
            status=TaskEventStatus.SUCCEEDED,
            summary="Found staged dataset",
            path_refs=["/mnt/common/datasets/example_001"],
        )
    )
    second = queue.append_task_event(
        TaskTimelineEvent(
            task_id=task.task_id,
            event_type="script_found",
            label="script",
            summary="Found service launch descriptor",
            path_refs=["scripts/red_sea.py"],
        )
    )

    events, next_cursor = ClioCoreQueue(tmp_path).drain_task_events(
        task.task_id,
        cursor=2,
        limit=10,
    )
    job_events, _ = queue.drain_events(Cursor(job_id=job.job_id), limit=20)

    assert first.seq == 1
    assert second.seq == 2
    assert [event.seq for event in events] == [2]
    assert events[0].event_type == "script_found"
    assert next_cursor == 3
    assert "task.timeline.dataset_found" in [event.event_type for event in job_events]


def test_task_timeline_cursor_and_limit_must_be_positive(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path)
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.REMOTE_AGENT,
            spec=RemoteAgentTaskSpec(prompt_path="/tmp/prompt.md"),
            idempotency_key="task-events-invalid-cursor",
        )
    )
    task = queue.append_task(RelayTask(job_id=job.job_id, name="remote-agent.discovery"))

    with pytest.raises(ValueError, match="cursor"):
        queue.drain_task_events(task.task_id, cursor=0)
    with pytest.raises(ValueError, match="limit"):
        queue.drain_task_events(task.task_id, limit=0)


def test_gateway_sessions_are_durable_and_updateable(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path)
    session = queue.create_gateway_session(
        GatewaySession(
            cluster="test-cluster",
            name="live-service-example",
            requested_resources={"nodes": 1, "exclusive": True},
            gateway={"strategy": "ssh_forward", "remote_port": 11111},
        )
    )

    updated = queue.update_gateway_session(
        session.session_id,
        state=GatewaySessionState.READY,
        scheduler_job_id="12345",
        node="ares-comp-01",
        gateway={"strategy": "ssh_forward", "remote_port": 11111, "local_port": 5900},
        metadata={"dataset": "example_001"},
    )
    listed = ClioCoreQueue(tmp_path).list_gateway_sessions(cluster="test-cluster")
    closed = queue.close_gateway_session(session.session_id)

    assert listed[0].session_id == session.session_id
    assert updated.state == GatewaySessionState.READY
    assert updated.scheduler_job_id == "12345"
    assert updated.gateway["local_port"] == 5900
    assert updated.metadata["dataset"] == "example_001"
    assert closed.state == GatewaySessionState.CLOSED


def test_closed_gateway_session_cannot_be_reopened_or_updated(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path)
    session = queue.create_gateway_session(
        GatewaySession(cluster="test-cluster", name="live-service-example")
    )
    queue.close_gateway_session(session.session_id)

    with pytest.raises(QueueConflictError, match="cannot reopen"):
        queue.update_gateway_session(session.session_id, state=GatewaySessionState.READY)
    with pytest.raises(QueueConflictError, match="cannot update"):
        queue.update_gateway_session(session.session_id, node="compute-01")

    closed = queue.close_gateway_session(session.session_id)
    assert closed.state == GatewaySessionState.CLOSED


def test_submit_rejects_reused_idempotency_key_with_different_payload(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path)
    queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["echo", "hello"]),
            idempotency_key="same-submit",
        )
    )

    with pytest.raises(QueueConflictError, match="different job payload"):
        queue.submit_job(
            RelayJob(
                cluster="ares",
                kind=JobKind.JARVIS,
                spec=JarvisRunSpec(command=["echo", "different"]),
                idempotency_key="same-submit",
            )
        )


def test_submit_distinguishes_idempotency_keys_with_same_sanitized_form(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path)
    first = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["echo", "hello"]),
            idempotency_key="a/b",
        )
    )
    second = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["echo", "hello"]),
            idempotency_key="a_b",
        )
    )

    assert first.job_id != second.job_id
    assert len(list((tmp_path / "idempotency").glob("*.json"))) == 2


def test_submit_recovers_reserved_idempotency_record(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path)
    queue.initialize()
    key_path = _idempotency_path(tmp_path, "reserved")
    key_path.write_text(
        '{"state":"reserved","job_id":"job_reserved","idempotency_key":"reserved"}',
        encoding="utf-8",
    )

    saved = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["echo", "hello"]),
            idempotency_key="reserved",
        )
    )

    assert saved.job_id == "job_reserved"
    assert queue.get_job("job_reserved").job_id == "job_reserved"


def test_submit_rejects_reserved_idempotency_digest_mismatch(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path)
    queue.initialize()
    key_path = _idempotency_path(tmp_path, "reserved")
    key_path.write_text(
        '{"state":"reserved","job_id":"job_reserved","idempotency_key":"reserved",'
        '"job_digest":"not-this-payload"}',
        encoding="utf-8",
    )

    with pytest.raises(QueueConflictError, match="different job payload"):
        queue.submit_job(
            RelayJob(
                cluster="ares",
                kind=JobKind.JARVIS,
                spec=JarvisRunSpec(command=["echo", "hello"]),
                idempotency_key="reserved",
            )
        )


def test_submit_repairs_existing_job_missing_initial_event(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path)
    job = RelayJob(
        cluster="ares",
        kind=JobKind.JARVIS,
        spec=JarvisRunSpec(command=["echo", "hello"]),
        idempotency_key="repair-event",
    )
    saved = queue.submit_job(job)
    shutil.rmtree(tmp_path / "events" / saved.job_id)

    repeated = queue.submit_job(job)
    events, _ = queue.drain_events(Cursor(job_id=saved.job_id), limit=10)

    assert repeated.job_id == saved.job_id
    assert [event.event_type for event in events] == ["job.queued"]


def test_legacy_queue_requires_and_completes_crash_safe_bounded_index_migration(
    tmp_path: Path,
) -> None:
    for family in ("jobs", "tasks", "leases", "artifacts", "progress", "events"):
        (tmp_path / family).mkdir(parents=True, exist_ok=True)
    job = RelayJob(
        cluster="ares",
        kind=JobKind.JARVIS,
        spec=JarvisRunSpec(command=["echo", "legacy"]),
        idempotency_key="legacy-index-migration",
    )
    task = RelayTask(job_id=job.job_id, name="legacy-task")
    progress = ProgressRecord(job_id=job.job_id, label="legacy", current=1, total=2)
    (tmp_path / "jobs" / f"{job.job_id}.json").write_text(
        job.model_dump_json(indent=2), encoding="utf-8"
    )
    (tmp_path / "tasks" / f"{task.task_id}.json").write_text(
        task.model_dump_json(indent=2), encoding="utf-8"
    )
    (tmp_path / "progress" / f"{progress.progress_id}.json").write_text(
        progress.model_dump_json(indent=2), encoding="utf-8"
    )
    event_dir = tmp_path / "events" / job.job_id
    event_dir.mkdir(parents=True)
    for seq, event_type in ((1, "job.queued"), (2, "legacy.observed")):
        event = RelayEvent(
            job_id=job.job_id,
            seq=seq,
            event_type=event_type,
            message=event_type,
        )
        (event_dir / f"{seq:020d}.json").write_text(
            event.model_dump_json(indent=2), encoding="utf-8"
        )

    queue = ClioCoreQueue(tmp_path)
    assert queue.index_migration_status()["complete"] is False
    with pytest.raises(QueueConflictError, match="migrate-indexes"):
        queue.acquire_next_job("worker-before-migration", cluster="ares")

    batches = 0
    state = queue.index_migration_status()
    while state["complete"] is not True:
        state = queue.migrate_indexes_batch(batch_size=1)
        batches += 1
        assert batches < 20

    assert (tmp_path / "jobs_queued" / f"{job.job_id}.json").is_file()
    assert (tmp_path / "tasks_by_job" / job.job_id / f"{task.task_id}.json").is_file()
    latest, truncated = queue.latest_job_event(job.job_id)
    assert truncated is False
    assert latest is not None and latest.seq == 2
    latest_progress, progress_count, progress_truncated = queue.latest_job_progress(job.job_id)
    assert progress_truncated is False
    assert progress_count == 1
    assert latest_progress is not None and latest_progress.progress_id == progress.progress_id
    migrated_tasks, next_task_cursor, task_count = queue.list_tasks_page(job.job_id)
    migrated_progress, next_progress_cursor, migrated_progress_count = queue.list_progress_page(
        job.job_id
    )
    assert task_count == migrated_progress_count == 1
    assert next_task_cursor is next_progress_cursor is None
    assert migrated_tasks[0].task_id == task.task_id
    assert migrated_tasks[0].sequence == 1
    assert migrated_progress[0].progress_id == progress.progress_id
    assert migrated_progress[0].sequence == 1
    lease = queue.acquire_next_job("worker-after-migration", cluster="ares")
    assert lease is not None and lease.job_id == job.job_id


def test_leasing_reads_active_index_not_terminal_history(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue = ClioCoreQueue(tmp_path)
    queued = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["echo", "queued"]),
            idempotency_key="active-index-target",
        )
    )
    for index in range(2_000):
        terminal = RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            state=JobState.SUCCEEDED,
            spec=JarvisRunSpec(command=["true"]),
            idempotency_key=f"terminal-history-{index}",
        )
        (tmp_path / "jobs" / f"{terminal.job_id}.json").write_text(
            terminal.model_dump_json(), encoding="utf-8"
        )

    def fail_global_history_read() -> list[RelayJob]:
        raise AssertionError("lease admission read terminal history")

    monkeypatch.setattr(queue, "list_jobs", fail_global_history_read)
    lease = queue.acquire_next_job("indexed-worker", cluster="ares")

    assert lease is not None and lease.job_id == queued.job_id


def test_task_event_head_advances_without_linear_history_probe(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path)
    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["true"]),
            idempotency_key="task-event-head",
        )
    )
    task = queue.append_task(RelayTask(job_id=job.job_id, name="head-test"))
    head_path = tmp_path / "task_event_heads" / f"{task.task_id}.json"
    head_path.write_text(f'{{"task_id":"{task.task_id}","latest_seq":100000}}', encoding="utf-8")

    saved = queue.append_task_event(
        TaskTimelineEvent(
            task_id=task.task_id,
            event_type="checkpoint",
            label="checkpoint",
            summary="checkpoint",
        )
    )

    assert saved.seq == 100001


def test_submit_promotes_reserved_idempotency_record_with_existing_job(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path)
    job = RelayJob(
        cluster="ares",
        kind=JobKind.JARVIS,
        spec=JarvisRunSpec(command=["echo", "hello"]),
        idempotency_key="reserved-existing",
    )
    saved = queue.submit_job(job)
    key_path = _idempotency_path(tmp_path, "reserved-existing")
    record = key_path.read_text(encoding="utf-8")
    key_path.write_text(
        record.replace('"state": "committed"', '"state": "reserved"'),
        encoding="utf-8",
    )

    repeated = queue.submit_job(job)
    repaired = key_path.read_text(encoding="utf-8")

    assert repeated.job_id == saved.job_id
    assert '"state": "committed"' in repaired


def _idempotency_path(root: Path, key: str) -> Path:
    return root / "idempotency" / f"key_{hashlib.sha256(key.encode('utf-8')).hexdigest()}.json"


def test_lease_survives_restart_without_duplicate_execution(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path)
    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["echo", "hello"]),
            idempotency_key="restart",
        )
    )

    lease = queue.acquire_next_job("endpoint-1", cluster="ares", ttl_seconds=60)
    duplicate = ClioCoreQueue(tmp_path).acquire_next_job(
        "endpoint-2",
        cluster="homelab",
        ttl_seconds=60,
    )

    assert lease is not None
    assert duplicate is None
    assert queue.get_job(job.job_id).state == JobState.LEASED


def test_expired_lease_requeues_job_for_retry(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path)
    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["echo", "hello"]),
            idempotency_key="retry-expired",
        )
    )

    lease = queue.acquire_next_job("endpoint-1", cluster="ares", ttl_seconds=-1)
    recovered = queue.recover_stale_jobs(cluster="ares", max_attempts=3)
    next_lease = queue.acquire_next_job("endpoint-2", cluster="ares", ttl_seconds=60)
    events, _ = queue.drain_events(Cursor(job_id=job.job_id), limit=20)

    assert lease is not None
    assert [item.job_id for item in recovered] == [job.job_id]
    assert recovered[0].state == JobState.QUEUED
    assert recovered[0].leased_by is None
    assert next_lease is not None
    assert next_lease.job_id == job.job_id
    assert queue.get_job(job.job_id).attempts == 2
    assert "job.requeued" in [event.event_type for event in events]


def test_expired_lease_fails_job_after_retry_limit(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path)
    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["echo", "hello"]),
            idempotency_key="retry-exhausted",
        )
    )

    lease = queue.acquire_next_job("endpoint-1", cluster="ares", ttl_seconds=-1)
    recovered = queue.recover_stale_jobs(cluster="ares", max_attempts=1)
    next_lease = queue.acquire_next_job("endpoint-2", cluster="ares", ttl_seconds=60)
    failed = queue.get_job(job.job_id)
    events, _ = queue.drain_events(Cursor(job_id=job.job_id), limit=20)

    assert lease is not None
    assert [item.job_id for item in recovered] == [job.job_id]
    assert recovered[0].state == JobState.FAILED
    assert next_lease is None
    assert failed.state == JobState.FAILED
    assert failed.leased_by is None
    assert failed.last_error == "expired lease exceeded retry limit"
    assert "job.failed" in [event.event_type for event in events]


def test_renewed_lease_prevents_stale_recovery(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path)
    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["echo", "hello"]),
            idempotency_key="renewed-lease",
        )
    )

    lease = queue.acquire_next_job("endpoint-1", cluster="ares", ttl_seconds=-1)
    assert lease is not None
    renewed = queue.renew_lease(lease.lease_id, ttl_seconds=60)
    recovered = queue.recover_stale_jobs(cluster="ares", max_attempts=3)

    assert renewed is not None
    assert recovered == []
    assert queue.get_job(job.job_id).state == JobState.LEASED
    assert queue.get_job(job.job_id).leased_by == "endpoint-1"


def test_cursor_replay_after_restart(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path)
    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["echo", "hello"]),
            idempotency_key="cursor",
        )
    )
    queue.append_event(job.job_id, "one", "one")
    events, cursor = ClioCoreQueue(tmp_path).drain_events(Cursor(job_id=job.job_id, next_seq=2))

    assert [event.event_type for event in events] == ["one"]
    assert cursor.next_seq == 3


def test_task_records_have_state_events(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path)
    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["echo", "hello"]),
            idempotency_key="task",
        )
    )
    task = queue.append_task(RelayTask(job_id=job.job_id, name="jarvis.execution"))

    updated = queue.update_task_state(
        task.task_id,
        JobState.RUNNING,
        metadata={"pid": 123},
    )
    listed = ClioCoreQueue(tmp_path).list_tasks(job.job_id)
    events, _ = queue.drain_events(Cursor(job_id=job.job_id), limit=20)

    assert updated.state == JobState.RUNNING
    assert updated.metadata["pid"] == 123
    assert [item.task_id for item in listed] == [task.task_id]
    assert [event.event_type for event in events][-2:] == ["task.queued", "task.running"]


def test_progress_records_are_durable_and_emit_events(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path)
    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["echo", "hello"]),
            idempotency_key="progress",
        )
    )

    progress = queue.append_progress(
        ProgressRecord(
            job_id=job.job_id,
            label="steps",
            current=10,
            total=20,
            unit="step",
            message="half way",
        )
    )
    listed = ClioCoreQueue(tmp_path).list_progress(job.job_id)
    events, _ = queue.drain_events(Cursor(job_id=job.job_id), limit=20)

    assert [item.progress_id for item in listed] == [progress.progress_id]
    assert listed[0].current == 10
    assert listed[0].total == 20
    assert [event.event_type for event in events][-1] == "progress.updated"
    assert events[-1].payload["progress_id"] == progress.progress_id


def test_monitor_rule_triggers_once_from_event_text(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path)
    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["echo", "hello"]),
            idempotency_key="monitor",
        )
    )
    rule = queue.append_monitor_rule(
        MonitorRule(
            job_id=job.job_id,
            pattern="step 100",
            event_types=["stdout.delta"],
        )
    )
    queue.append_event(
        job.job_id,
        "stdout.delta",
        "progress",
        payload={"text": "reached step 100\n"},
    )

    first = evaluate_monitor_rules(queue)
    second = evaluate_monitor_rules(queue)
    events, _ = queue.drain_events(Cursor(job_id=job.job_id, next_seq=1), limit=20)

    assert first == [
        {"rule_id": rule.rule_id, "action": "emit_event", "matched_seq": 3},
    ]
    assert second == []
    assert [event.event_type for event in events].count("monitor.triggered") == 1


def test_monitor_rule_records_progress_from_regex_groups(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path)
    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["echo", "hello"]),
            idempotency_key="monitor-progress",
        )
    )
    rule = queue.append_monitor_rule(
        MonitorRule(
            job_id=job.job_id,
            pattern=r"PROGRESS current=(?P<current>\d+) total=(?P<total>\d+) (?P<message>.+)",
            action=MonitorRuleAction.RECORD_PROGRESS,
            event_types=["stdout.delta"],
            action_payload={
                "label": "iteration",
                "current_group": "current",
                "total_group": "total",
                "message_group": "message",
                "unit": "step",
            },
        )
    )
    progress_text = (
        "PROGRESS current=4 total=10 running\nPROGRESS current=5 total=10 still-running\n"
    )
    queue.append_event(
        job.job_id,
        "stdout.delta",
        progress_text.strip(),
        payload={"text": progress_text},
    )

    result = evaluate_monitor_rules(queue)
    second = evaluate_monitor_rules(queue)
    progress = queue.list_progress(job.job_id)
    updated_rule = queue.list_monitor_rules(job.job_id)[0]
    events, _ = queue.drain_events(Cursor(job_id=job.job_id, next_seq=1), limit=20)

    assert result[0]["rule_id"] == rule.rule_id
    assert result[0]["action"] == "record_progress"
    assert second == []
    assert len(progress) == 2
    assert progress[0].label == "iteration"
    assert progress[0].current == 4
    assert progress[0].total == 10
    assert progress[0].message == "running"
    assert progress[0].unit == "step"
    assert progress[0].source_event_seq == 3
    assert progress[1].current == 5
    assert progress[1].message == "still-running"
    assert updated_rule.enabled is True
    assert updated_rule.triggered_at is None
    assert updated_rule.next_seq == 8
    assert [event.event_type for event in events][-4:] == [
        "progress.updated",
        "monitor.triggered",
        "progress.updated",
        "monitor.triggered",
    ]


def test_monitor_rule_submits_remote_agent_with_event_context(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path)
    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["echo", "hello"]),
            idempotency_key="monitor-submit-agent-source",
        )
    )
    rule = queue.append_monitor_rule(
        MonitorRule(
            job_id=job.job_id,
            pattern=r"ETA current=(?P<current>\d+) total=(?P<total>\d+)",
            action=MonitorRuleAction.SUBMIT_AGENT,
            event_types=["progress.updated"],
            action_payload={
                "cluster": "ares",
                "prompt_path": "/tmp/monitor-prompt.md",
                "mcp_config_path": "/tmp/monitor-mcp.json",
                "model": "codex-test",
                "workdir": "/tmp/work",
                "timeout_seconds": 30,
            },
        )
    )
    queue.append_event(
        job.job_id,
        "progress.updated",
        "ETA current=4 total=10",
        payload={"current": 4, "total": 10},
    )

    result = evaluate_monitor_rules(queue)
    second = evaluate_monitor_rules(queue)
    submitted_job_id = result[0]["submitted_job_id"]
    agent_job = queue.get_job(str(submitted_job_id))
    events, _ = queue.drain_events(Cursor(job_id=job.job_id, next_seq=1), limit=20)

    assert second == []
    assert result == [
        {
            "rule_id": rule.rule_id,
            "action": "submit_agent",
            "matched_seq": 3,
            "submitted_job_id": submitted_job_id,
        }
    ]
    assert agent_job.kind == JobKind.REMOTE_AGENT
    assert agent_job.cluster == "ares"
    assert agent_job.idempotency_key == f"monitor:{rule.rule_id}:3"
    assert isinstance(agent_job.spec, RemoteAgentTaskSpec)
    assert agent_job.spec.prompt_path == "/tmp/monitor-prompt.md"
    assert agent_job.spec.mcp_config_path == "/tmp/monitor-mcp.json"
    assert agent_job.spec.model == "codex-test"
    assert agent_job.spec.workdir == "/tmp/work"
    assert agent_job.spec.timeout_seconds == 30
    assert agent_job.spec.context["monitor_rule_id"] == rule.rule_id
    assert agent_job.spec.context["source_job_id"] == job.job_id
    assert agent_job.spec.context["source_event_seq"] == 3
    assert agent_job.spec.context["source_event_type"] == "progress.updated"
    assert agent_job.spec.context["match_groups"] == {"current": "4", "total": "10"}
    assert events[-1].event_type == "monitor.triggered"
    assert events[-1].payload["submitted_job_id"] == submitted_job_id

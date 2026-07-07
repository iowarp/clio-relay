from __future__ import annotations

from pathlib import Path

from clio_relay.core_queue import ClioCoreQueue
from clio_relay.models import (
    Cursor,
    JarvisRunSpec,
    JobKind,
    JobState,
    MonitorRule,
    MonitorRuleAction,
    ProgressRecord,
    RelayJob,
    RelayTask,
)
from clio_relay.relay_ops import evaluate_monitor_rules


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


def test_submit_recovers_reserved_idempotency_record(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path)
    queue.initialize()
    key_path = tmp_path / "idempotency" / "reserved.json"
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

from __future__ import annotations

from pathlib import Path

from clio_relay.core_queue import ClioCoreQueue
from clio_relay.models import Cursor, JarvisRunSpec, JobKind, JobState, MonitorRule, RelayJob
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

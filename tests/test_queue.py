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

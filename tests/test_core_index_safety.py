from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pytest

import clio_relay.core_queue as core_queue_module
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.errors import QueueConflictError
from clio_relay.models import (
    EndpointRegistration,
    EndpointRole,
    GatewaySession,
    JarvisRunSpec,
    JobKind,
    RelayJob,
    RelayTask,
    TaskTimelineEvent,
    utc_now,
)


def _job(key: str) -> RelayJob:
    return RelayJob(
        cluster="ares",
        kind=JobKind.JARVIS,
        spec=JarvisRunSpec(command=["true"]),
        idempotency_key=key,
    )


def test_stale_recovery_uses_exact_scheduler_indexes_without_global_task_scan(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue = ClioCoreQueue(tmp_path)
    job = queue.submit_job(_job("indexed-scheduler-recovery"))
    lease = queue.acquire_next_job("worker", cluster="ares", ttl_seconds=-1)
    assert lease is not None
    queue.append_task(
        RelayTask(
            job_id=job.job_id,
            name="scheduler-owned",
            metadata={"scheduler_job_ids": ["12345"]},
        )
    )

    def forbid_global_read(*_args: object, **_kwargs: object) -> list[object]:
        raise AssertionError("global task scan attempted")

    monkeypatch.setattr(queue, "_read_many", forbid_global_read)
    assert queue.recover_stale_job(job.job_id, cluster="ares") is None
    assert queue.recover_stale_jobs(cluster="ares") == []


def test_stale_recovery_fails_closed_on_ambiguous_scheduler_index(
    tmp_path: Path,
) -> None:
    queue = ClioCoreQueue(tmp_path)
    job = queue.submit_job(_job("ambiguous-scheduler-index"))
    assert queue.acquire_next_job("worker", cluster="ares", ttl_seconds=-1) is not None
    scheduler_index = tmp_path / "scheduler_refs_by_job" / job.job_id
    (scheduler_index / "unsafe.tmp").write_text("ambiguous", encoding="utf-8")

    with pytest.raises(QueueConflictError, match="contains an unsafe record"):
        queue.recover_stale_job(job.job_id, cluster="ares")


def test_gateway_reverse_indexes_refuse_cardinality_overflow(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(core_queue_module, "MAX_GATEWAY_INDEX_RECORDS", 2)
    queue = ClioCoreQueue(tmp_path)
    job = queue.submit_job(_job("gateway-index-bound"))
    for index in range(3):
        queue.create_gateway_session(
            GatewaySession(
                cluster="ares",
                name=f"gateway-{index}",
                scheduler_job_id="shared-scheduler-id",
            )
        )

    with pytest.raises(QueueConflictError, match="exceeded its safety bound"):
        queue.append_task(
            RelayTask(
                job_id=job.job_id,
                name="bounded-scheduler-source",
                metadata={"scheduler_job_ids": ["shared-scheduler-id"]},
            )
        )


def test_task_event_page_recovers_contiguous_records_without_head(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path)
    job = queue.submit_job(_job("task-event-head-recovery"))
    task = queue.append_task(RelayTask(job_id=job.job_id, name="task-events"))
    event = queue.append_task_event(
        TaskTimelineEvent(
            task_id=task.task_id,
            event_type="checkpoint",
            label="checkpoint",
            summary="checkpoint persisted",
        )
    )
    (tmp_path / "task_event_heads" / f"{task.task_id}.json").unlink()

    events, next_cursor = queue.drain_task_events(task.task_id)
    assert events == [event]
    assert next_cursor == 2


def test_fresh_endpoint_snapshot_keeps_a_newer_canonical_heartbeat(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path)
    endpoint = queue.register_endpoint(
        EndpointRegistration(
            role=EndpointRole.WORKER,
            cluster="ares",
            hostname="worker",
            pid=123,
        )
    )
    observed_at = utc_now()
    newer = endpoint.model_copy(update={"last_seen_at": observed_at + timedelta(seconds=1)})
    queue._write(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        queue.root / "endpoints" / f"{endpoint.endpoint_id}.json",
        newer,
    )

    endpoints, truncated = queue.scan_fresh_endpoints(
        limit=10,
        cluster="ares",
        fresh_seconds=60,
        now=observed_at,
    )

    assert truncated is False
    assert [item.endpoint_id for item in endpoints] == [endpoint.endpoint_id]

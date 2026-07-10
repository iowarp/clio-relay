from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from typing import cast

from clio_relay.core_queue import ClioCoreQueue
from clio_relay.models import (
    Cursor,
    EndpointRegistration,
    EndpointRole,
    JarvisRunSpec,
    JobKind,
    JobState,
    RelayJob,
    utc_now,
)
from clio_relay.queue_management import (
    cancel_queue_job,
    cleanup_stale_jobs,
    diagnose_queue,
    list_queue_jobs,
    worker_status,
)


def test_queue_list_filters_active_jobs_and_reports_position(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path)
    first = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["echo", "first"]),
            idempotency_key="queue-first",
        )
    )
    second = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["echo", "second"]),
            idempotency_key="queue-second",
        )
    )
    queue.update_job_state(first.job_id, JobState.SUCCEEDED)

    active = list_queue_jobs(queue, cluster="ares")
    all_jobs = list_queue_jobs(queue, cluster="ares", include_terminal=True)
    active_jobs = cast(list[dict[str, object]], active["jobs"])
    active_job = cast(dict[str, object], active_jobs[0]["job"])

    assert active["count"] == 1
    assert active_job["job_id"] == second.job_id
    assert active_jobs[0]["relay_queue"] == {
        "state": "queued",
        "jobs_ahead": 0,
        "position": 1,
    }
    assert all_jobs["count"] == 2


def test_queue_diagnose_and_cleanup_stale_expired_lease(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path)
    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["sleep", "60"]),
            idempotency_key="expired-lease",
        )
    )
    lease = queue.acquire_next_job("endpoint-1", cluster="ares", ttl_seconds=-1)

    diagnostics = diagnose_queue(queue, cluster="ares")
    dry_run = cleanup_stale_jobs(queue, cluster="ares", dry_run=True)
    recovered = cleanup_stale_jobs(queue, cluster="ares", dry_run=False)
    diagnostic_issues = cast(list[dict[str, object]], diagnostics["issues"])
    recoverable = cast(list[dict[str, object]], dry_run["recoverable"])
    recoverable_job = cast(dict[str, object], recoverable[0]["job"])

    assert lease is not None
    assert diagnostics["issue_count"] == 1
    assert diagnostic_issues[0]["code"] == "expired_lease"
    assert dry_run["recovered_count"] == 0
    assert recoverable_job["job_id"] == job.job_id
    assert recovered["recovered_count"] == 1
    assert queue.get_job(job.job_id).state.value == "queued"


def test_queue_cancel_requires_explicit_scheduler_policy(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path)
    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["sleep", "60"]),
            idempotency_key="cancel-policy",
        )
    )

    result = cancel_queue_job(queue, job.job_id, scheduler_policy="relay-only")
    events, _ = queue.drain_events(job_id_cursor(job.job_id), limit=20)
    result_job = cast(dict[str, object], result["job"])

    assert result["scheduler_cancel_requested"] is False
    assert result["scheduler_policy"] == "relay-only"
    assert result_job["state"] == "canceled"
    assert events[-2].payload["cancel_scheduler"] is False


def test_queue_cancel_can_request_scheduler_policy(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path)
    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["sleep", "60"]),
            idempotency_key="cancel-scheduler-policy",
        )
    )

    result = cancel_queue_job(queue, job.job_id, scheduler_policy="request-scheduler")
    events, _ = queue.drain_events(job_id_cursor(job.job_id), limit=20)

    assert result["scheduler_cancel_requested"] is True
    assert result["scheduler_policy"] == "request-scheduler"
    assert events[-2].payload["cancel_scheduler"] is True


def test_worker_status_reports_capacity_and_leases(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path)
    queue.register_endpoint(
        EndpointRegistration(
            role=EndpointRole.WORKER,
            cluster="ares",
            hostname="node",
            pid=123,
            metadata={"concurrency": 4},
        )
    )
    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["echo", "hello"]),
            idempotency_key="worker-status",
        )
    )
    queue.acquire_next_job("endpoint-1", cluster="ares", ttl_seconds=60)

    status = worker_status(queue, cluster="ares")
    active_leases = cast(list[dict[str, object]], status["active_leases"])

    assert status["worker_count"] == 1
    assert status["configured_concurrency"] == 4
    assert len(active_leases) == 1
    assert active_leases[0]["job_id"] == job.job_id


def test_worker_status_ignores_stale_workers(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    endpoint = queue.register_endpoint(
        EndpointRegistration(
            role=EndpointRole.WORKER,
            cluster="ares",
            hostname="old-node",
            pid=123,
            metadata={"concurrency": 8},
        )
    )
    stale = endpoint.model_copy(update={"last_seen_at": utc_now() - timedelta(seconds=120)})
    endpoint_path = queue.root / "endpoints" / f"{endpoint.endpoint_id}.json"
    endpoint_path.write_text(stale.model_dump_json(indent=2), encoding="utf-8")

    status = worker_status(queue, cluster="ares", fresh_seconds=60)

    assert status["worker_count"] == 0
    assert status["configured_concurrency"] == 0
    assert status["registered_worker_count"] == 1
    assert status["stale_worker_count"] == 1
    assert status["workers"] == []


def test_worker_status_counts_active_slots_not_supervisor(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    parent = queue.register_endpoint(
        EndpointRegistration(
            role=EndpointRole.WORKER,
            cluster="ares",
            hostname="node",
            pid=123,
            metadata={"concurrency": 4, "worker_supervisor": True},
        )
    )
    for index in range(4):
        queue.register_endpoint(
            EndpointRegistration(
                role=EndpointRole.WORKER,
                cluster="ares",
                hostname="node",
                pid=123,
                metadata={
                    "worker_slot": index,
                    "parent_endpoint_id": parent.endpoint_id,
                    "concurrency": 1,
                },
            )
        )

    status = worker_status(queue, cluster="ares")

    assert status["worker_count"] == 4
    assert status["configured_concurrency"] == 4
    assert status["registered_worker_count"] == 5
    assert status["stale_worker_count"] == 0


def job_id_cursor(job_id: str) -> Cursor:
    return Cursor(job_id=job_id)

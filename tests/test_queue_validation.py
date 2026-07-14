from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pytest

from clio_relay.core_queue import ClioCoreQueue
from clio_relay.errors import QueueConflictError
from clio_relay.models import (
    Cursor,
    EndpointRegistration,
    EndpointRole,
    JobKind,
    JobState,
    RelayJob,
    RemoteAgentTaskSpec,
    utc_now,
)
from clio_relay.queue_management import cancel_queue_job, cleanup_stale_jobs
from clio_relay.queue_validation import run_queue_management_validation
from clio_relay.relay_ops import cancel_job
from clio_relay.validation_report import ValidationStatus, load_release_gate_policy
from tests.queue_validation_fixtures import LiveWorkerFleet


def test_queue_validation_observes_real_workers_processes_and_scheduler(
    tmp_path: Path,
) -> None:
    with LiveWorkerFleet(tmp_path) as fleet:
        report = run_queue_management_validation(
            fleet.queue,
            job_id=None,
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            older_than_seconds=1,
            scan_limit=100,
            scheduler_provider=fleet.scheduler,
            scheduler_timeout_seconds=30,
            scheduler_poll_seconds=0.02,
        )

    assert report.status is ValidationStatus.PASSED
    assert {check.check_id for check in report.checks} == {
        "queue.worker-containment-enforced",
        "queue.kind-concurrency-parallel",
        "queue.kind-concurrency-worker-enforced",
        "queue.list-bounded",
        "queue.diagnose-specific-reason",
        "queue.stale-dry-run",
        "queue.stale-cleanup-executed",
        "queue.cancel-running-worker-process",
        "queue.scheduler-preserved-default",
    }
    assert all(check.evidence for check in report.checks)
    stale_target = next(
        resource for resource in report.resources if resource.role == "queue-management-target"
    )
    running_target = next(
        resource
        for resource in report.resources
        if resource.role == "queue-management-running-target"
    )
    worker = next(resource for resource in report.resources if resource.kind == "relay_worker")
    scheduler = next(resource for resource in report.resources if resource.kind == "scheduler_job")
    assert stale_target.state == "canceled"
    assert stale_target.metadata["initial_state"] == "queued"
    assert running_target.state == "canceled"
    assert running_target.metadata["initial_state"] == "running"
    assert running_target.metadata["worker_cancellation_acknowledged"] is True
    assert running_target.metadata["lease_released"] is True
    assert running_target.metadata["outer_process_exited"] is True
    assert running_target.metadata["child_process_exited"] is True
    assert running_target.metadata["residual_process_count"] == 0
    assert worker.metadata["kind_concurrency"] == {"jarvis": 2}
    assert worker.metadata["process_containment"]["enforceable"] is True
    controlled = worker.metadata["controlled_probe"]
    assert controlled["overflow_lease_acquired"] is False
    assert len(controlled["running_processes"]) == 2
    assert controlled["live_worker_observation"]["overflow_state"] == "queued"
    assert scheduler.state == "completed"
    assert scheduler.metadata["scheduler_cancel_requested"] is False
    assert report.cleanup.cancel_scheduler_jobs is False
    assert report.cleanup.remaining_resources == []
    assert fleet.scheduler.released is True
    assert fleet.scheduler.canceled is False
    assert all(job.state is JobState.CANCELED for job in fleet.queue.list_jobs())
    assert fleet.queue.list_leases(cluster="test-cluster") == []
    events, _ = fleet.queue.drain_events(Cursor(job_id=running_target.resource_id), limit=100)
    event_types = {event.event_type for event in events}
    assert "execution.started" in event_types
    assert "execution.canceled" in event_types
    assert "scheduler.cancel_requested" not in event_types


def test_queue_validation_fails_without_explicit_jarvis_kind_limit(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    queue.register_endpoint(
        EndpointRegistration(
            role=EndpointRole.WORKER,
            cluster="test-cluster",
            hostname="worker",
            pid=123,
            metadata={"concurrency": 3},
        )
    )

    report = run_queue_management_validation(
        queue,
        job_id=None,
        cluster="test-cluster",
        older_than_seconds=60,
        scan_limit=100,
    )

    assert report.status is ValidationStatus.FAILED
    assert report.error is not None and "no explicit concurrency limit" in report.error
    assert queue.list_jobs() == []
    assert report.cleanup.remaining_resources == []


def test_submit_and_acquire_exact_job_enforces_kind_limit_atomically(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    first = _template_for_cluster(queue=None, suffix="first")
    second = _template_for_cluster(queue=None, suffix="second")

    first, first_lease = queue.submit_and_acquire_job(
        first,
        "endpoint-one",
        kind_concurrency={JobKind.REMOTE_AGENT: 1},
    )
    second, second_lease = queue.submit_and_acquire_job(
        second,
        "endpoint-two",
        kind_concurrency={JobKind.REMOTE_AGENT: 1},
    )

    assert first_lease is not None
    assert first.state is JobState.LEASED
    assert second_lease is None
    assert second.state is JobState.QUEUED


def test_exact_stale_cleanup_never_cancels_neighboring_job(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    first = queue.submit_job(_template_for_cluster(queue=None, suffix="first"))
    second = queue.submit_job(_template_for_cluster(queue=None, suffix="second"))
    stale_at = utc_now() - timedelta(minutes=10)
    for job in (first, second):
        path = queue.root / "jobs" / f"{job.job_id}.json"
        path.write_text(
            job.model_copy(update={"created_at": stale_at, "updated_at": stale_at}).model_dump_json(
                indent=2
            ),
            encoding="utf-8",
        )

    result = cleanup_stale_jobs(
        queue,
        cluster="cluster-a",
        job_id=first.job_id,
        older_than_seconds=60,
        cancel_queued=True,
        dry_run=False,
        limit=1,
        scan_limit=100,
    )

    assert result["canceled_count"] == 1
    assert result["conflict_count"] == 0
    assert queue.get_job(first.job_id).state is JobState.CANCELED
    assert queue.get_job(second.job_id).state is JobState.QUEUED


def test_stale_snapshot_cannot_cancel_job_leased_after_discovery(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    queued = queue.submit_job(_template_for_cluster(queue=None, suffix="lease-race"))
    snapshot = queue.get_job(queued.job_id)
    lease = queue.acquire_job(queued.job_id, "real-worker", cluster="cluster-a")
    assert lease is not None

    result = cancel_queue_job(
        queue,
        queued.job_id,
        cluster="cluster-a",
        scheduler_policy="relay-only",
        expected_state=snapshot.state,
        expected_updated_at=snapshot.updated_at,
    )

    assert result["state_transitioned"] is False
    assert queue.get_job(queued.job_id).state is JobState.LEASED


def test_completion_wins_cancel_race_and_terminal_state_cannot_be_overwritten(
    tmp_path: Path,
) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    job = queue.submit_job(_template_for_cluster(queue=None, suffix="completion-race"))
    queue.update_job_state(job.job_id, JobState.SUCCEEDED)

    canceled = cancel_job(queue, job.job_id)

    assert canceled.state is JobState.SUCCEEDED
    events, _ = queue.drain_events(Cursor(job_id=job.job_id), limit=20)
    assert not any(event.event_type == "job.cancel_requested" for event in events)
    with pytest.raises(QueueConflictError, match="cannot change terminal job"):
        queue.update_job_state(job.job_id, JobState.CANCELED)


def test_release_policy_requires_live_queue_and_worker_identity_evidence() -> None:
    policy = load_release_gate_policy(Path("docs/release-gate-1.0.yaml"))
    requirement = next(
        item for item in policy.requirements if item.requirement_id == "ares-queue-management"
    )

    assert {
        "queue.worker-containment-enforced",
        "queue.kind-concurrency-parallel",
        "queue.kind-concurrency-worker-enforced",
        "queue.diagnose-specific-reason",
        "queue.stale-cleanup-executed",
        "queue.cancel-running-worker-process",
        "queue.scheduler-preserved-default",
        "worker.artifact-version",
        "worker.artifact-sha256",
        "worker.source-identity",
        "worker.scheduler-provider",
        "worker.target-identity",
        "worker.component-clio-kit-released",
    }.issubset(requirement.required_checks)
    assert {"relay_job", "relay_worker", "scheduler_job", "cluster_target"}.issubset(
        requirement.required_resource_kinds
    )


def _template_for_cluster(queue: ClioCoreQueue | None, suffix: str) -> RelayJob:
    del queue
    return RelayJob(
        cluster="cluster-a",
        kind=JobKind.REMOTE_AGENT,
        spec=RemoteAgentTaskSpec(prompt_path=f"/tmp/{suffix}"),
        idempotency_key=f"queue-validation-{suffix}",
    )

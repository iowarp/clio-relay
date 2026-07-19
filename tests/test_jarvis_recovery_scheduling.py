"""Focused scheduling tests for durable execution-recovery maintenance."""

from __future__ import annotations

from pathlib import Path
from typing import Any, NoReturn, cast

import pytest
from pytest import MonkeyPatch

import clio_relay.endpoint as endpoint_module
from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.endpoint import EndpointWorker
from clio_relay.models import (
    EndpointRole,
    JarvisRunSpec,
    JobKind,
    JobState,
    Lease,
    McpAdmissionClass,
    RelayJob,
    RelayTask,
)


def _submit_job(queue: ClioCoreQueue, *, key: str) -> RelayJob:
    """Submit one bounded JARVIS job used only to exercise queue ordering."""
    return queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["controlled"]),
            idempotency_key=key,
        )
    )


def _register_cleanup_marker(queue: ClioCoreQueue, job: RelayJob) -> RelayTask:
    """Register the minimal durable marker that transfers work to cleanup."""
    task = queue.append_task(
        RelayTask(
            job_id=job.job_id,
            name="jarvis.execution",
            metadata={"cluster": job.cluster},
        )
    )
    return queue.register_execution_cleanup(
        task.task_id,
        {
            "execution_sidecars": {
                "schema_version": "clio-relay.execution-sidecars.v1",
                "progress": ".progress-recovery-order.jsonl",
                "runtime": ".runtime-recovery-order.jsonl",
            },
            "execution_cleanup": {
                "schema_version": "clio-relay.execution-cleanup.v1",
                "launch_protocol": "broker-release-after-ownership-v1",
            },
        },
    )


def test_unrelated_job_runs_before_due_recovery_scan(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """A due maintenance marker must not delay an immediately leasable job."""
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    recovery_job = _submit_job(queue, key="due-recovery-maintenance")
    recovery_lease = queue.acquire_next_job(
        "crashed-endpoint",
        cluster="ares",
        ttl_seconds=60,
    )
    assert recovery_lease is not None
    queue.update_job_state(recovery_job.job_id, JobState.RUNNING)
    _register_cleanup_marker(queue, recovery_job)
    queue.release_lease(recovery_lease.lease_id)
    unrelated = _submit_job(queue, key="unrelated-science-job")
    worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="ares",
        queue=queue,
    )
    observations: list[str] = []

    def run_job(job: RelayJob, lease: Lease) -> None:
        assert lease.job_id == unrelated.job_id
        observations.append(f"job:{job.job_id}")
        queue.update_job_state(job.job_id, JobState.RUNNING)
        queue.update_job_state(job.job_id, JobState.SUCCEEDED)

    def reconcile_cleanup() -> None:
        observations.append("cleanup")

    monkeypatch.setattr(worker, "_run_job", run_job)
    monkeypatch.setattr(worker, "_reconcile_pending_execution_cleanup", reconcile_cleanup)
    try:
        result = worker.run_once()
        assert result is not None
        assert result.job_id == unrelated.job_id
        assert result.state is JobState.SUCCEEDED
        assert observations == [f"job:{unrelated.job_id}"]

        assert worker.run_once() is None
        assert observations == [f"job:{unrelated.job_id}", "cleanup"]
    finally:
        worker.close()


def test_expired_lease_with_pending_cleanup_remains_running(tmp_path: Path) -> None:
    """Stale recovery cannot requeue work already owned by cleanup recovery."""
    queue = ClioCoreQueue(tmp_path / "core")
    job = _submit_job(queue, key="expired-owned-recovery")
    lease = queue.acquire_next_job(
        "crashed-endpoint",
        cluster="ares",
        ttl_seconds=-1,
    )
    assert lease is not None
    queue.update_job_state(job.job_id, JobState.RUNNING)
    _register_cleanup_marker(queue, job)

    recovered = queue.recover_stale_jobs(cluster="ares", max_attempts=1)
    current = queue.get_job(job.job_id)
    leases, truncated = queue.scan_job_leases(job.job_id, limit=10)

    assert recovered == []
    assert current.state is JobState.RUNNING
    assert current.leased_by == "crashed-endpoint"
    assert [item.lease_id for item in leases] == [lease.lease_id]
    assert truncated is False


def test_pre_migration_expired_lease_without_cleanup_is_requeued(
    tmp_path: Path,
) -> None:
    """A missing legacy migration receipt cannot stall an unrelated stale job."""
    queue = ClioCoreQueue(tmp_path / "core")
    job = _submit_job(queue, key="legacy-stale-without-cleanup")
    lease = queue.acquire_next_job(
        "crashed-endpoint",
        cluster="ares",
        ttl_seconds=-1,
    )
    assert lease is not None
    queue.update_job_state(job.job_id, JobState.RUNNING)
    shard = queue._execution_cleanup_shard(job.job_id)  # pyright: ignore[reportPrivateUsage]
    receipt = queue._execution_cleanup_migration_receipt_path(  # pyright: ignore[reportPrivateUsage]
        job.cluster,
        shard,
    )
    receipt.unlink()

    recovered = queue.recover_stale_jobs(cluster="ares")

    assert [item.job_id for item in recovered] == [job.job_id]
    assert queue.get_job(job.job_id).state is JobState.QUEUED
    assert receipt.is_file()


def test_only_worker_slot_zero_owns_cleanup_reconciliation(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """Concurrent worker slots must have exactly one cleanup reconciler."""
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    supervisor = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="ares",
        concurrency=2,
        queue=queue,
    )
    observed_ownership: list[bool] = []

    class StopSlot(Exception):
        """End one otherwise-infinite worker-slot loop after construction."""

    class FakeSlotWorker:
        """Capture constructor ownership without starting a real slot loop."""

        def __init__(
            self,
            *,
            queue: ClioCoreQueue,
            reconcile_execution_cleanup: bool,
            **_kwargs: object,
        ) -> None:
            self.queue = queue
            self.endpoint: object | None = None
            observed_ownership.append(reconcile_execution_cleanup)

        def run_once(self, **_kwargs: object) -> NoReturn:
            raise StopSlot

    monkeypatch.setattr(endpoint_module, "EndpointWorker", FakeSlotWorker)
    try:
        with pytest.raises(StopSlot):
            cast(Any, supervisor)._serve_worker_slot(0, 0, McpAdmissionClass.WORKLOAD)
        with pytest.raises(StopSlot):
            cast(Any, supervisor)._serve_worker_slot(1, 0, McpAdmissionClass.WORKLOAD)
    finally:
        supervisor.close()

    assert observed_ownership == [True, False]


def test_cleanup_gets_bounded_fairness_under_continuous_queue_load(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """Slot zero services maintenance after a bounded foreground job burst."""
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    jobs = [
        _submit_job(queue, key=f"continuous-foreground-{index}")
        for index in range(endpoint_module.EXECUTION_CLEANUP_MAX_FOREGROUND_JOBS + 1)
    ]
    worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="ares",
        queue=queue,
    )
    observations: list[str] = []

    def run_job(job: RelayJob, _lease: Lease) -> None:
        observations.append(f"job:{job.job_id}")
        queue.update_job_state(job.job_id, JobState.RUNNING)
        queue.update_job_state(job.job_id, JobState.SUCCEEDED)

    monkeypatch.setattr(worker, "_run_job", run_job)
    monkeypatch.setattr(
        worker,
        "_reconcile_pending_execution_cleanup",
        lambda: observations.append("cleanup"),
    )
    try:
        for expected in jobs:
            result = worker.run_once()
            assert result is not None
            assert result.job_id == expected.job_id
    finally:
        worker.close()

    boundary = endpoint_module.EXECUTION_CLEANUP_MAX_FOREGROUND_JOBS
    assert observations[:boundary] == [f"job:{job.job_id}" for job in jobs[:boundary]]
    assert observations[boundary:] == ["cleanup", f"job:{jobs[-1].job_id}"]

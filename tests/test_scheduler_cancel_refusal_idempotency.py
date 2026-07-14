"""Regression coverage for durable scheduler-cancellation refusal reconciliation."""

from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Any, cast

from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.endpoint import EndpointWorker
from clio_relay.models import (
    Cursor,
    EndpointRole,
    JarvisRunSpec,
    JobKind,
    JobState,
    RelayEvent,
    RelayJob,
    RelayTask,
    SchedulerCancelDispositionState,
)
from clio_relay.relay_ops import cancel_job
from clio_relay.scheduler_providers import ExternalSchedulerProvider


class _RecordingExternalScheduler(ExternalSchedulerProvider):
    """Record any cancellation that incorrectly crosses the ownership boundary."""

    def __init__(self) -> None:
        self.canceled: list[str] = []

    def cancel(self, scheduler_job_id: str) -> subprocess.CompletedProcess[str]:
        """Record and delegate an unexpected external cancellation request."""
        self.canceled.append(scheduler_job_id)
        return super().cancel(scheduler_job_id)


def _canceled_job_with_unowned_scheduler_identity(
    queue: ClioCoreQueue,
    *,
    idempotency_key: str,
    scheduler_job_id: str,
) -> RelayJob:
    """Create a canceled job carrying only legacy, unverified scheduler metadata."""
    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["sleep", "60"]),
            idempotency_key=idempotency_key,
        )
    )
    queue.append_task(
        RelayTask(
            job_id=job.job_id,
            name="jarvis.execution",
            state=JobState.RUNNING,
            metadata={"scheduler": "external", "scheduler_job_ids": [scheduler_job_id]},
        )
    )
    canceled = cancel_job(queue, job.job_id, cancel_scheduler=True)
    assert canceled.state is JobState.CANCELED
    return canceled


def _canceled_job_with_mixed_scheduler_identities(
    queue: ClioCoreQueue,
    *,
    owned_scheduler_job_id: str,
    unowned_scheduler_job_id: str,
) -> RelayJob:
    """Create a canceled job whose owned cancellation remains retryable."""
    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["sleep", "60"]),
            idempotency_key="mixed-scheduler-identities",
        )
    )
    task = RelayTask(
        job_id=job.job_id,
        name="jarvis.execution",
        state=JobState.RUNNING,
    )
    queue.append_task(
        task.model_copy(
            update={
                "metadata": {
                    "scheduler": "external",
                    "scheduler_job_ids": [
                        owned_scheduler_job_id,
                        unowned_scheduler_job_id,
                    ],
                    "scheduler_job_ownership": [
                        {
                            "scheduler_job_id": owned_scheduler_job_id,
                            "scheduler_provider": "external",
                            "relay_job_id": job.job_id,
                            "task_id": task.task_id,
                            "execution_id": f"execution-{owned_scheduler_job_id}",
                            "runtime_metadata_source": "jarvis_sidecar",
                            "ownership_verified": True,
                            "proof": "authenticated_runtime_sidecar",
                        }
                    ],
                }
            }
        )
    )
    canceled = cancel_job(queue, job.job_id, cancel_scheduler=True)
    assert canceled.state is JobState.CANCELED
    return canceled


def _worker(
    settings: RelaySettings,
    queue: ClioCoreQueue,
    scheduler: _RecordingExternalScheduler,
) -> EndpointWorker:
    """Create and register the worker used for public-loop reconciliation tests."""
    worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="ares",
        queue=queue,
        scheduler_provider=scheduler,
    )
    worker.register()
    return worker


def _scheduler_refusal_events(queue: ClioCoreQueue, job_id: str) -> list[RelayEvent]:
    """Return the bounded refusal-event set for one compact regression fixture."""
    events, _ = queue.drain_events(Cursor(job_id=job_id), limit=100)
    return [event for event in events if event.event_type == "scheduler.cancel_refused"]


def test_repeated_unowned_cancel_reconciliation_is_terminal_and_idempotent(
    tmp_path: Path,
) -> None:
    """Repeated worker polls emit one refusal and retain one terminal disposition."""
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    job = _canceled_job_with_unowned_scheduler_identity(
        queue,
        idempotency_key="fresh-unowned-cancel",
        scheduler_job_id="legacy-unowned-24680",
    )
    scheduler = _RecordingExternalScheduler()
    worker = _worker(settings, queue, scheduler)

    for _ in range(3):
        assert worker.run_once() is None

    refused = _scheduler_refusal_events(queue, job.job_id)
    disposition = queue.get_scheduler_cancel_disposition(job.job_id, cluster=job.cluster)
    assert len(refused) == 1
    assert refused[0].payload == {
        "scheduler_job_id": "legacy-unowned-24680",
        "metadata_source": "unverified_durable_metadata",
        "ownership_verified": False,
    }
    assert disposition is not None
    assert disposition.complete is True
    assert disposition.identity_resolution == "resolved"
    assert [item.state for item in disposition.dispositions] == [
        SchedulerCancelDispositionState.REFUSED
    ]
    assert queue.get_scheduler_cancel_pending(job.job_id, cluster=job.cluster) is None
    assert scheduler.canceled == []


def test_restart_finalizes_existing_refusal_without_emitting_it_again(tmp_path: Path) -> None:
    """A crash after recording refusal cannot restart an unbounded event loop."""
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    scheduler_job_id = "legacy-unowned-13579"
    job = _canceled_job_with_unowned_scheduler_identity(
        queue,
        idempotency_key="interrupted-unowned-cancel",
        scheduler_job_id=scheduler_job_id,
    )
    pending = queue.register_scheduler_cancel_identity(
        job.job_id,
        cluster=job.cluster,
        scheduler_job_id=scheduler_job_id,
        provider="external",
        ownership_verified=False,
    )
    assert pending.identity_resolution == "pending"
    assert [item.state for item in pending.dispositions] == [
        SchedulerCancelDispositionState.REFUSED
    ]
    queue.append_event(
        job.job_id,
        "scheduler.cancel_refused",
        "Refused scheduler cancellation because no owned scheduler identity was available",
        payload={
            "scheduler_job_id": scheduler_job_id,
            "metadata_source": "unverified_durable_metadata",
            "ownership_verified": False,
        },
    )
    scheduler = _RecordingExternalScheduler()
    worker = _worker(settings, queue, scheduler)

    for _ in range(3):
        assert worker.run_once() is None

    disposition = queue.get_scheduler_cancel_disposition(job.job_id, cluster=job.cluster)
    assert len(_scheduler_refusal_events(queue, job.job_id)) == 1
    assert disposition is not None
    assert disposition.complete is True
    assert disposition.identity_resolution == "resolved"
    assert [item.scheduler_job_id for item in disposition.dispositions] == [scheduler_job_id]
    assert [item.state for item in disposition.dispositions] == [
        SchedulerCancelDispositionState.REFUSED
    ]
    assert queue.get_scheduler_cancel_pending(job.job_id, cluster=job.cluster) is None
    assert scheduler.canceled == []


def test_stale_mixed_identity_reconciliations_emit_one_refusal_across_restart(
    tmp_path: Path,
) -> None:
    """Only the atomic creator emits while an owned retry keeps the record pending."""
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    owned_scheduler_job_id = "owned-retry-24680"
    unowned_scheduler_job_id = "unowned-refused-13579"
    job = _canceled_job_with_mixed_scheduler_identities(
        queue,
        owned_scheduler_job_id=owned_scheduler_job_id,
        unowned_scheduler_job_id=unowned_scheduler_job_id,
    )
    stale = queue.get_scheduler_cancel_pending(job.job_id, cluster=job.cluster)
    assert stale is not None
    scheduler = _RecordingExternalScheduler()
    worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="ares",
        queue=queue,
        scheduler_provider=scheduler,
    )
    worker.scheduler_cancel_retry_base_seconds = 3_600
    reconcile = cast(Any, worker)._reconcile_canceled_scheduler_job

    reconcile(stale)
    reconcile(stale)
    restarted = _worker(settings, queue, scheduler)
    restarted.scheduler_cancel_retry_base_seconds = 3_600
    assert restarted.run_once() is None

    refused = _scheduler_refusal_events(queue, job.job_id)
    pending = queue.get_scheduler_cancel_pending(job.job_id, cluster=job.cluster)
    assert len(refused) == 1
    assert refused[0].payload["scheduler_job_id"] == unowned_scheduler_job_id
    assert queue.get_scheduler_cancel_disposition(job.job_id, cluster=job.cluster) is None
    assert pending is not None
    assert pending.identity_resolution == "resolved"
    assert {item.scheduler_job_id: item.state for item in pending.dispositions} == {
        owned_scheduler_job_id: SchedulerCancelDispositionState.RETRY_WAIT,
        unowned_scheduler_job_id: SchedulerCancelDispositionState.REFUSED,
    }
    assert scheduler.canceled == [owned_scheduler_job_id]


def test_explicit_refusal_identity_wins_over_job_runtime_metadata(tmp_path: Path) -> None:
    """A task-level refusal event cannot be relabeled with a job-level scheduler id."""
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["true"]),
            idempotency_key="explicit-refusal-identity",
            metadata={
                "runtime_metadata": {
                    "scheduler_job_id": "job-level-111",
                    "source": "jarvis_sidecar",
                }
            },
        )
    )
    worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="ares",
        queue=queue,
        scheduler_provider=_RecordingExternalScheduler(),
    )

    cast(Any, worker)._record_scheduler_cancel_refused(
        job,
        scheduler_job_id="task-level-222",
        metadata_source="unverified_durable_metadata",
    )

    refused = _scheduler_refusal_events(queue, job.job_id)
    assert len(refused) == 1
    assert refused[0].payload == {
        "scheduler_job_id": "task-level-222",
        "metadata_source": "unverified_durable_metadata",
        "ownership_verified": False,
    }


def test_stale_superseded_operator_request_completion_is_idempotent(tmp_path: Path) -> None:
    """Two slots may terminalize one pre-job-write cancellation record safely."""
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["true"]),
            idempotency_key="stale-superseded-operator-request",
        )
    )
    queue.ensure_scheduler_cancel_pending(job.job_id, reason="operator_request")
    stale = queue.get_scheduler_cancel_pending(job.job_id, cluster=job.cluster)
    assert stale is not None
    worker = _worker(settings, queue, _RecordingExternalScheduler())
    reconcile = cast(Any, worker)._reconcile_canceled_scheduler_job

    reconcile(stale)
    reconcile(stale)

    disposition = queue.get_scheduler_cancel_disposition(job.job_id, cluster=job.cluster)
    assert disposition is not None
    assert disposition.complete is True
    assert disposition.identity_resolution == "superseded"
    assert disposition.dispositions == []
    assert queue.get_scheduler_cancel_pending(job.job_id, cluster=job.cluster) is None

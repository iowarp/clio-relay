"""Concurrency regressions for durable scheduler-cancellation claims."""

from __future__ import annotations

import json
import subprocess
import threading
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

from pytest import MonkeyPatch

from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.endpoint import EndpointWorker
from clio_relay.errors import RelayError
from clio_relay.models import (
    Cursor,
    EndpointRole,
    JarvisRunSpec,
    JobKind,
    JobState,
    RelayJob,
    RelayTask,
    SchedulerCancelDisposition,
    SchedulerCancelDispositionState,
    SchedulerCancelPending,
    SchedulerPhase,
    SchedulerStatus,
)
from clio_relay.relay_ops import cancel_job
from clio_relay.scheduler_providers import ExternalSchedulerProvider


class _BarrierScheduler(ExternalSchedulerProvider):
    """Hold the first cancellation long enough for a racing worker to contend."""

    def __init__(self) -> None:
        self.cancel_calls: list[str] = []
        self._calls_lock = threading.Lock()
        self._cancel_barrier = threading.Barrier(2)

    def cancel(self, scheduler_job_id: str) -> subprocess.CompletedProcess[str]:
        """Record the invocation and synchronize a possible duplicate caller."""
        with self._calls_lock:
            self.cancel_calls.append(scheduler_job_id)
        with suppress(threading.BrokenBarrierError):
            self._cancel_barrier.wait(timeout=0.5)
        return subprocess.CompletedProcess(
            [self.name, scheduler_job_id],
            1,
            "",
            "deterministic retryable failure",
        )


class _BlockingConfirmationScheduler(ExternalSchedulerProvider):
    """Hold the first confirmation poll while recording any duplicate poll."""

    def __init__(self, *, recovered_phase: SchedulerPhase = SchedulerPhase.UNKNOWN) -> None:
        self.poll_calls: list[str] = []
        self.first_poll_entered = threading.Event()
        self.release_first_poll = threading.Event()
        self._calls_lock = threading.Lock()
        self._recovered_phase = recovered_phase

    def poll(self, scheduler_job_id: str) -> SchedulerStatus:
        """Block the first caller and return the configured phase to a recovered caller."""
        with self._calls_lock:
            self.poll_calls.append(scheduler_job_id)
            call_number = len(self.poll_calls)
        if call_number == 1:
            self.first_poll_entered.set()
            if not self.release_first_poll.wait(timeout=5):
                raise AssertionError("timed out waiting to release the first confirmation poll")
            phase = SchedulerPhase.UNKNOWN
        else:
            phase = self._recovered_phase
        return SchedulerStatus(
            scheduler=self.name,
            scheduler_job_id=scheduler_job_id,
            phase=phase,
            reason="cancellation confirmation test observation",
        )


class _OversizedFailureScheduler(ExternalSchedulerProvider):
    """Return a provider diagnostic far larger than any durable record."""

    def cancel(self, scheduler_job_id: str) -> subprocess.CompletedProcess[str]:
        """Return one deterministic oversized cancellation failure."""
        return subprocess.CompletedProcess(
            [self.name, scheduler_job_id],
            1,
            "",
            "first scheduler cause\n" + ("x" * 300_000) + "\nfinal scheduler status",
        )


class _MismatchedConfirmationScheduler(ExternalSchedulerProvider):
    """Return a terminal observation for an identity that was never requested."""

    def poll(self, scheduler_job_id: str) -> SchedulerStatus:
        """Deliberately violate both provider response identity fields."""
        return SchedulerStatus(
            scheduler="slurm",
            scheduler_job_id=f"other-{scheduler_job_id}",
            phase=SchedulerPhase.CANCELED,
            record_found=True,
            reason="wrong scheduler record",
        )


class _OversizedStatusScheduler(ExternalSchedulerProvider):
    """Return a correctly identified status carrying oversized provider text."""

    def poll(self, scheduler_job_id: str) -> SchedulerStatus:
        """Return a large status that must be normalized before persistence."""
        detail = "provider detail start\n" + ("y" * 300_000) + "\nprovider detail end"
        return SchedulerStatus(
            scheduler=self.name,
            scheduler_job_id=scheduler_job_id,
            phase=SchedulerPhase.RUNNING,
            reason=detail,
            queue_position_note=detail,
        )


class _OversizedPollFailureScheduler(ExternalSchedulerProvider):
    """Raise an oversized provider exception before a status exists."""

    def poll(self, scheduler_job_id: str) -> SchedulerStatus:
        """Raise a diagnostic that must fit one poll-failure event."""
        del scheduler_job_id
        raise RelayError("poll cause\n" + ("z" * 300_000) + "\npoll summary")


def _canceled_job_with_owned_scheduler_identity(
    queue: ClioCoreQueue,
    *,
    scheduler_job_id: str,
) -> RelayJob:
    """Create canceled relay work with an authenticated scheduler identity."""
    job = queue.submit_job(
        RelayJob(
            cluster="ares",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["sleep", "60"]),
            idempotency_key=f"owned-scheduler-cancel-{scheduler_job_id}",
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
                    "scheduler_job_ids": [scheduler_job_id],
                    "scheduler_job_ownership": [
                        {
                            "scheduler_job_id": scheduler_job_id,
                            "scheduler_provider": "external",
                            "relay_job_id": job.job_id,
                            "task_id": task.task_id,
                            "execution_id": f"execution-{scheduler_job_id}",
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


def _claim_ready_scheduler_identity(
    queue: ClioCoreQueue,
    job: RelayJob,
    *,
    scheduler_job_id: str,
) -> None:
    """Register and finalize one owned cancellation identity without a worker."""
    queue.register_scheduler_cancel_identity(
        job.job_id,
        cluster=job.cluster,
        scheduler_job_id=scheduler_job_id,
        provider="external",
        ownership_verified=True,
    )
    queue.finalize_scheduler_cancel_identities(job.job_id, cluster=job.cluster)


def _accept_scheduler_cancellation_without_polling(
    queue: ClioCoreQueue,
    job: RelayJob,
    *,
    scheduler_job_id: str,
    now: datetime,
) -> None:
    """Persist one accepted cancellation so confirmation work is immediately due."""
    _claim_ready_scheduler_identity(queue, job, scheduler_job_id=scheduler_job_id)
    claim = queue.claim_scheduler_cancel_attempt(
        job.job_id,
        cluster=job.cluster,
        scheduler_job_id=scheduler_job_id,
        provider="external",
        lease_seconds=5,
        now=now,
    )
    assert claim is not None
    accepted = queue.record_scheduler_cancel_attempt(
        job.job_id,
        cluster=job.cluster,
        scheduler_job_id=scheduler_job_id,
        provider="external",
        claim_id=claim.claim_id,
        accepted=True,
        error=None,
        max_attempts=5,
        retry_delay_seconds=2,
        now=now,
    )
    assert accepted is not None
    assert accepted.dispositions[0].state is SchedulerCancelDispositionState.CANCEL_REQUESTED


def test_oversized_cancel_failure_is_bounded_in_state_and_event(tmp_path: Path) -> None:
    """Provider stderr cannot strand a claim or exceed a durable event record."""
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    scheduler_job_id = "oversized-cancel-24680"
    job = _canceled_job_with_owned_scheduler_identity(
        queue,
        scheduler_job_id=scheduler_job_id,
    )
    stale = queue.get_scheduler_cancel_pending(job.job_id, cluster=job.cluster)
    assert stale is not None
    scheduler = _OversizedFailureScheduler()
    worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="ares",
        queue=queue,
        scheduler_provider=scheduler,
    )
    worker.scheduler_cancel_retry_base_seconds = 3_600
    try:
        cast(Any, worker)._reconcile_canceled_scheduler_job(stale)
    finally:
        worker.close()

    reloaded = ClioCoreQueue(settings.core_dir).get_scheduler_cancel_pending(
        job.job_id,
        cluster=job.cluster,
    )
    assert reloaded is not None
    error = reloaded.dispositions[0].last_error
    assert error is not None
    assert error.startswith("first scheduler cause")
    assert error.endswith("final scheduler status")
    assert len(error.encode("utf-8")) <= 4_096
    events, _ = queue.drain_events(Cursor(job_id=job.job_id), limit=100)
    failed = [event for event in events if event.event_type == "scheduler.cancel_failed"]
    assert len(failed) == 1
    event_error = failed[0].payload["stderr"]
    assert isinstance(event_error, str)
    assert len(event_error.encode("utf-8")) <= 4_096


def test_mismatched_terminal_status_cannot_confirm_requested_job(tmp_path: Path) -> None:
    """A provider response for another scheduler identity remains nonterminal."""
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    scheduler_job_id = "requested-confirmation-13579"
    job = _canceled_job_with_owned_scheduler_identity(
        queue,
        scheduler_job_id=scheduler_job_id,
    )
    _accept_scheduler_cancellation_without_polling(
        queue,
        job,
        scheduler_job_id=scheduler_job_id,
        now=datetime.now(UTC) - timedelta(seconds=1),
    )
    scheduler = _MismatchedConfirmationScheduler()
    worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="ares",
        queue=queue,
        scheduler_provider=scheduler,
    )
    try:
        cast(Any, worker)._confirm_scheduler_cancellation(
            job,
            scheduler,
            scheduler_job_id,
        )
    finally:
        worker.close()

    pending = queue.get_scheduler_cancel_pending(job.job_id, cluster=job.cluster)
    assert pending is not None
    disposition = pending.dispositions[0]
    assert disposition.state is SchedulerCancelDispositionState.CANCEL_REQUESTED
    assert disposition.confirmation_attempts == 1
    assert disposition.last_error is not None
    assert "mismatched identity" in disposition.last_error
    tasks, truncated = queue.scan_job_tasks(job.job_id, limit=10)
    assert truncated is False
    status = tasks[0].metadata["scheduler_status"]
    assert status["scheduler"] == "external"
    assert status["scheduler_job_id"] == scheduler_job_id
    assert status["phase"] == SchedulerPhase.UNKNOWN.value


def test_oversized_scheduler_status_is_bounded_before_task_and_event_write(
    tmp_path: Path,
) -> None:
    """Every provider-owned status string is bounded at the scheduler boundary."""
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    scheduler_job_id = "oversized-status-97531"
    job = _canceled_job_with_owned_scheduler_identity(
        queue,
        scheduler_job_id=scheduler_job_id,
    )
    task = queue.scan_job_tasks(job.job_id, limit=10)[0][0]
    scheduler = _OversizedStatusScheduler()
    worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="ares",
        queue=queue,
        scheduler_provider=scheduler,
    )
    try:
        cast(Any, worker)._refresh_scheduler_status(
            job,
            [scheduler_job_id],
            task_id=task.task_id,
            force=True,
        )
    finally:
        worker.close()

    persisted_tasks = queue.scan_job_tasks(job.job_id, limit=10)[0]
    persisted_task = next(item for item in persisted_tasks if item.task_id == task.task_id)
    status = persisted_task.metadata["scheduler_status"]
    for field_name in ("reason", "queue_position_note"):
        value = status[field_name]
        assert isinstance(value, str)
        assert value.startswith("provider detail start")
        assert value.endswith("provider detail end")
        assert len(value.encode("utf-8")) <= 4_096
    events, _ = queue.drain_events(Cursor(job_id=job.job_id), limit=100)
    running = [event for event in events if event.event_type == "scheduler.running"]
    assert len(running) == 1
    assert len(json.dumps(running[0].payload).encode("utf-8")) < 262_144


def test_oversized_scheduler_poll_exception_is_bounded_before_event_write(
    tmp_path: Path,
) -> None:
    """A provider exception cannot exceed the durable scheduler event limit."""
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    scheduler_job_id = "oversized-poll-error-86420"
    job = _canceled_job_with_owned_scheduler_identity(
        queue,
        scheduler_job_id=scheduler_job_id,
    )
    task = queue.scan_job_tasks(job.job_id, limit=10)[0][0]
    worker = EndpointWorker(
        role=EndpointRole.WORKER,
        settings=settings,
        cluster="ares",
        queue=queue,
        scheduler_provider=_OversizedPollFailureScheduler(),
    )
    try:
        cast(Any, worker)._refresh_scheduler_status(
            job,
            [scheduler_job_id],
            task_id=task.task_id,
            force=True,
        )
    finally:
        worker.close()

    events, _ = queue.drain_events(Cursor(job_id=job.job_id), limit=100)
    failed = [event for event in events if event.event_type == "scheduler.poll_failed"]
    assert len(failed) == 1
    error = failed[0].payload["error"]
    assert isinstance(error, str)
    assert error.startswith("poll cause")
    assert error.endswith("poll summary")
    assert len(error.encode("utf-8")) <= 4_096


def test_concurrent_worker_slots_invoke_scheduler_cancel_exactly_once(tmp_path: Path) -> None:
    """Two stale worker snapshots cannot cross the provider boundary together."""
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    setup_queue = ClioCoreQueue(settings.core_dir)
    scheduler_job_id = "concurrent-owned-24680"
    job = _canceled_job_with_owned_scheduler_identity(
        setup_queue,
        scheduler_job_id=scheduler_job_id,
    )
    stale = setup_queue.get_scheduler_cancel_pending(job.job_id, cluster=job.cluster)
    assert stale is not None
    scheduler = _BarrierScheduler()
    workers = [
        EndpointWorker(
            role=EndpointRole.WORKER,
            settings=settings,
            cluster="ares",
            queue=ClioCoreQueue(settings.core_dir),
            scheduler_provider=scheduler,
        )
        for _ in range(2)
    ]
    for worker in workers:
        worker.scheduler_cancel_retry_base_seconds = 3_600
    start = threading.Barrier(3)
    errors: list[BaseException] = []
    errors_lock = threading.Lock()

    def reconcile(worker: EndpointWorker) -> None:
        try:
            start.wait(timeout=5)
            cast(Any, worker)._reconcile_canceled_scheduler_job(stale)
        except BaseException as exc:  # pragma: no cover - asserted in the parent thread
            with errors_lock:
                errors.append(exc)

    threads = [threading.Thread(target=reconcile, args=(worker,)) for worker in workers]
    for thread in threads:
        thread.start()
    start.wait(timeout=5)
    for thread in threads:
        thread.join(timeout=10)

    assert all(not thread.is_alive() for thread in threads)
    assert errors == []
    assert scheduler.cancel_calls == [scheduler_job_id]
    pending = setup_queue.get_scheduler_cancel_pending(job.job_id, cluster=job.cluster)
    assert pending is not None
    disposition = pending.dispositions[0]
    assert disposition.state is SchedulerCancelDispositionState.RETRY_WAIT
    assert disposition.attempts == 1
    assert disposition.attempt_claim_id is None


def test_concurrent_worker_slots_poll_cancel_confirmation_exactly_once(tmp_path: Path) -> None:
    """Two due snapshots cannot spend duplicate confirmation attempts."""
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    setup_queue = ClioCoreQueue(settings.core_dir)
    scheduler_job_id = "concurrent-confirmation-86420"
    job = _canceled_job_with_owned_scheduler_identity(
        setup_queue,
        scheduler_job_id=scheduler_job_id,
    )
    _accept_scheduler_cancellation_without_polling(
        setup_queue,
        job,
        scheduler_job_id=scheduler_job_id,
        now=datetime.now(UTC) - timedelta(seconds=1),
    )
    stale = setup_queue.get_scheduler_cancel_pending(job.job_id, cluster=job.cluster)
    assert stale is not None
    scheduler = _BlockingConfirmationScheduler()
    workers = [
        EndpointWorker(
            role=EndpointRole.WORKER,
            settings=settings,
            cluster="ares",
            queue=ClioCoreQueue(settings.core_dir),
            scheduler_provider=scheduler,
        )
        for _ in range(2)
    ]
    for worker in workers:
        worker.scheduler_cancel_retry_base_seconds = 3_600
    start = threading.Barrier(3)
    one_worker_finished = threading.Event()
    errors: list[BaseException] = []
    errors_lock = threading.Lock()

    def reconcile(worker: EndpointWorker) -> None:
        try:
            start.wait(timeout=5)
            cast(Any, worker)._reconcile_canceled_scheduler_job(stale)
        except BaseException as exc:  # pragma: no cover - asserted in the parent thread
            with errors_lock:
                errors.append(exc)
        finally:
            one_worker_finished.set()

    threads = [threading.Thread(target=reconcile, args=(worker,)) for worker in workers]
    for thread in threads:
        thread.start()
    start.wait(timeout=5)
    assert scheduler.first_poll_entered.wait(timeout=5)
    assert one_worker_finished.wait(timeout=5)
    scheduler.release_first_poll.set()
    for thread in threads:
        thread.join(timeout=10)

    assert all(not thread.is_alive() for thread in threads)
    assert errors == []
    assert scheduler.poll_calls == [scheduler_job_id]
    pending = setup_queue.get_scheduler_cancel_pending(job.job_id, cluster=job.cluster)
    assert pending is not None
    disposition = pending.dispositions[0]
    assert disposition.state is SchedulerCancelDispositionState.CANCEL_REQUESTED
    assert disposition.confirmation_attempts == 1
    assert disposition.confirmation_claim_id is None
    assert disposition.confirmation_claimed_at is None
    assert disposition.confirmation_claim_expires_at is None


def test_expired_confirmation_claim_recovers_without_spending_an_attempt(
    tmp_path: Path,
) -> None:
    """A crashed poller leaves a lease another worker can recover without budget loss."""
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    first_queue = ClioCoreQueue(settings.core_dir)
    scheduler_job_id = "confirmation-crash-75319"
    job = _canceled_job_with_owned_scheduler_identity(
        first_queue,
        scheduler_job_id=scheduler_job_id,
    )
    accepted_at = datetime(2026, 7, 14, 15, 0, tzinfo=UTC)
    _accept_scheduler_cancellation_without_polling(
        first_queue,
        job,
        scheduler_job_id=scheduler_job_id,
        now=accepted_at,
    )
    first_claim = first_queue.claim_scheduler_cancel_confirmation(
        job.job_id,
        cluster=job.cluster,
        scheduler_job_id=scheduler_job_id,
        provider="external",
        lease_seconds=5,
        now=accepted_at,
    )
    assert first_claim is not None
    assert first_claim.confirmation_attempt == 1

    recovering_queue = ClioCoreQueue(settings.core_dir)
    due_before_expiry, _ = recovering_queue.scan_due_scheduler_cancellations(
        cluster=job.cluster,
        limit=10,
        now=accepted_at + timedelta(seconds=4),
    )
    assert due_before_expiry == []
    assert (
        recovering_queue.claim_scheduler_cancel_confirmation(
            job.job_id,
            cluster=job.cluster,
            scheduler_job_id=scheduler_job_id,
            provider="external",
            lease_seconds=5,
            now=accepted_at + timedelta(seconds=4),
        )
        is None
    )

    recovered_claim = recovering_queue.claim_scheduler_cancel_confirmation(
        job.job_id,
        cluster=job.cluster,
        scheduler_job_id=scheduler_job_id,
        provider="external",
        lease_seconds=5,
        now=accepted_at + timedelta(seconds=5),
    )
    assert recovered_claim is not None
    assert recovered_claim.claim_id != first_claim.claim_id
    assert recovered_claim.confirmation_attempt == first_claim.confirmation_attempt == 1
    assert (
        first_queue.record_scheduler_cancel_observation(
            job.job_id,
            cluster=job.cluster,
            scheduler_job_id=scheduler_job_id,
            provider="external",
            claim_id=first_claim.claim_id,
            phase=SchedulerPhase.UNKNOWN,
            not_found=False,
            error="late result from crashed confirmation poller",
            max_confirmation_attempts=5,
            retry_delay_seconds=30,
            now=accepted_at + timedelta(seconds=6),
        )
        is None
    )
    after_stale = recovering_queue.get_scheduler_cancel_pending(
        job.job_id,
        cluster=job.cluster,
    )
    assert after_stale is not None
    assert after_stale.dispositions[0].confirmation_attempts == 0

    updated = recovering_queue.record_scheduler_cancel_observation(
        job.job_id,
        cluster=job.cluster,
        scheduler_job_id=scheduler_job_id,
        provider="external",
        claim_id=recovered_claim.claim_id,
        phase=SchedulerPhase.UNKNOWN,
        not_found=False,
        error="scheduler still canceling",
        max_confirmation_attempts=5,
        retry_delay_seconds=30,
        now=accepted_at + timedelta(seconds=6),
    )
    assert updated is not None
    disposition = updated.dispositions[0]
    assert disposition.state is SchedulerCancelDispositionState.CANCEL_REQUESTED
    assert disposition.confirmation_attempts == 1
    assert disposition.confirmation_claim_id is None


def test_recovered_canceled_confirmation_survives_late_unknown_without_worker_error(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    """A late stale UNKNOWN cannot overwrite CANCELED or escape a worker slot."""
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    setup_queue = ClioCoreQueue(settings.core_dir)
    scheduler_job_id = "terminal-confirmation-race-64208"
    job = _canceled_job_with_owned_scheduler_identity(
        setup_queue,
        scheduler_job_id=scheduler_job_id,
    )
    clock = [datetime(2026, 7, 14, 16, 0, tzinfo=UTC)]
    monkeypatch.setattr("clio_relay.endpoint.utc_now", lambda: clock[0])
    _accept_scheduler_cancellation_without_polling(
        setup_queue,
        job,
        scheduler_job_id=scheduler_job_id,
        now=clock[0],
    )
    stale = setup_queue.get_scheduler_cancel_pending(job.job_id, cluster=job.cluster)
    assert stale is not None
    scheduler = _BlockingConfirmationScheduler(recovered_phase=SchedulerPhase.CANCELED)
    workers = [
        EndpointWorker(
            role=EndpointRole.WORKER,
            settings=settings,
            cluster="ares",
            queue=ClioCoreQueue(settings.core_dir),
            scheduler_provider=scheduler,
        )
        for _ in range(2)
    ]
    for worker in workers:
        worker.scheduler_cancel_confirmation_claim_lease_seconds = 5
    errors: list[BaseException] = []
    errors_lock = threading.Lock()

    def reconcile(worker: EndpointWorker) -> None:
        try:
            cast(Any, worker)._reconcile_canceled_scheduler_job(stale)
        except BaseException as exc:  # pragma: no cover - asserted in the parent thread
            with errors_lock:
                errors.append(exc)

    stale_thread = threading.Thread(target=reconcile, args=(workers[0],))
    stale_thread.start()
    assert scheduler.first_poll_entered.wait(timeout=5)
    clock[0] += timedelta(seconds=5)

    recovered_thread = threading.Thread(target=reconcile, args=(workers[1],))
    recovered_thread.start()
    recovered_thread.join(timeout=10)
    assert not recovered_thread.is_alive()
    completed = setup_queue.get_scheduler_cancel_disposition(job.job_id, cluster=job.cluster)
    assert completed is not None
    assert completed.dispositions[0].state is SchedulerCancelDispositionState.CANCELED
    assert completed.dispositions[0].confirmation_attempts == 1

    scheduler.release_first_poll.set()
    stale_thread.join(timeout=10)
    assert not stale_thread.is_alive()
    assert errors == []
    assert scheduler.poll_calls == [scheduler_job_id, scheduler_job_id]
    persisted = setup_queue.get_scheduler_cancel_disposition(job.job_id, cluster=job.cluster)
    assert persisted == completed
    task = setup_queue.list_tasks(job.job_id)[0]
    scheduler_status = cast(dict[str, object], task.metadata["scheduler_status"])
    assert scheduler_status["phase"] == SchedulerPhase.CANCELED.value


def test_expired_scheduler_cancel_claim_is_recovered_without_spending_an_attempt(
    tmp_path: Path,
) -> None:
    """A process crash leaves a bounded lease that another queue can reclaim."""
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    first_queue = ClioCoreQueue(settings.core_dir)
    scheduler_job_id = "crash-recovery-13579"
    job = _canceled_job_with_owned_scheduler_identity(
        first_queue,
        scheduler_job_id=scheduler_job_id,
    )
    _claim_ready_scheduler_identity(
        first_queue,
        job,
        scheduler_job_id=scheduler_job_id,
    )
    acquired_at = datetime(2026, 7, 14, 12, 0, tzinfo=UTC)
    first_claim = first_queue.claim_scheduler_cancel_attempt(
        job.job_id,
        cluster=job.cluster,
        scheduler_job_id=scheduler_job_id,
        provider="external",
        lease_seconds=5,
        now=acquired_at,
    )
    assert first_claim is not None

    recovering_queue = ClioCoreQueue(settings.core_dir)
    due_before_expiry, _ = recovering_queue.scan_due_scheduler_cancellations(
        cluster=job.cluster,
        limit=10,
        now=acquired_at + timedelta(seconds=4),
    )
    assert due_before_expiry == []
    assert (
        recovering_queue.claim_scheduler_cancel_attempt(
            job.job_id,
            cluster=job.cluster,
            scheduler_job_id=scheduler_job_id,
            provider="external",
            lease_seconds=5,
            now=acquired_at + timedelta(seconds=4),
        )
        is None
    )

    recovered_claim = recovering_queue.claim_scheduler_cancel_attempt(
        job.job_id,
        cluster=job.cluster,
        scheduler_job_id=scheduler_job_id,
        provider="external",
        lease_seconds=5,
        now=acquired_at + timedelta(seconds=5),
    )
    assert recovered_claim is not None
    assert recovered_claim.claim_id != first_claim.claim_id
    assert recovered_claim.attempt == first_claim.attempt == 1
    assert (
        first_queue.record_scheduler_cancel_attempt(
            job.job_id,
            cluster=job.cluster,
            scheduler_job_id=scheduler_job_id,
            provider="external",
            claim_id=first_claim.claim_id,
            accepted=False,
            error="late result from crashed claimant",
            max_attempts=5,
            retry_delay_seconds=30,
            now=acquired_at + timedelta(seconds=6),
        )
        is None
    )

    updated = recovering_queue.record_scheduler_cancel_attempt(
        job.job_id,
        cluster=job.cluster,
        scheduler_job_id=scheduler_job_id,
        provider="external",
        claim_id=recovered_claim.claim_id,
        accepted=False,
        error="scheduler temporarily unavailable",
        max_attempts=5,
        retry_delay_seconds=30,
        now=acquired_at + timedelta(seconds=6),
    )
    assert updated is not None
    disposition = updated.dispositions[0]
    assert disposition.state is SchedulerCancelDispositionState.RETRY_WAIT
    assert disposition.attempts == 1
    assert disposition.attempt_claim_id is None
    assert disposition.attempt_claimed_at is None
    assert disposition.attempt_claim_expires_at is None


def test_terminalized_scheduler_cancel_ignores_late_claim_completion(tmp_path: Path) -> None:
    """A superseding terminal transition makes a late in-flight result a no-op."""
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    scheduler_job_id = "terminal-race-97531"
    job = _canceled_job_with_owned_scheduler_identity(
        queue,
        scheduler_job_id=scheduler_job_id,
    )
    _claim_ready_scheduler_identity(queue, job, scheduler_job_id=scheduler_job_id)
    claim = queue.claim_scheduler_cancel_attempt(
        job.job_id,
        cluster=job.cluster,
        scheduler_job_id=scheduler_job_id,
        provider="external",
        lease_seconds=5,
    )
    assert claim is not None

    terminal = queue.complete_scheduler_cancel_identity_scan(
        job.job_id,
        cluster=job.cluster,
        superseded=True,
    )
    assert terminal.complete is True
    assert terminal.dispositions[0].attempt_claim_id is None
    assert (
        queue.record_scheduler_cancel_attempt(
            job.job_id,
            cluster=job.cluster,
            scheduler_job_id=scheduler_job_id,
            provider="external",
            claim_id=claim.claim_id,
            accepted=True,
            error=None,
            max_attempts=5,
            retry_delay_seconds=2,
        )
        is None
    )
    persisted = queue.get_scheduler_cancel_disposition(job.job_id, cluster=job.cluster)
    assert persisted == terminal


def test_maximum_legacy_pending_record_remains_writable_with_cancel_claims(
    tmp_path: Path,
) -> None:
    """Unset claim fields cannot inflate a valid 1,000-disposition legacy record."""
    root = tmp_path / "core"
    queue = ClioCoreQueue(root)
    queue.initialize()
    job_id = "job_legacy_max_cancel"
    cluster = "ares"
    legacy = SchedulerCancelPending(
        job_id=job_id,
        cluster=cluster,
        identity_resolution="resolved",
        dispositions=[
            SchedulerCancelDisposition(scheduler_job_id=str(index), provider="slurm")
            for index in range(1_000)
        ],
    )
    legacy_document = legacy.model_dump(mode="json")
    for disposition in legacy_document["dispositions"]:
        for field in (
            "attempt_claim_id",
            "attempt_claimed_at",
            "attempt_claim_expires_at",
            "confirmation_claim_id",
            "confirmation_claimed_at",
            "confirmation_claim_expires_at",
        ):
            disposition.pop(field)
    pending_path = cast(Any, queue)._scheduler_cancel_record_path(
        "scheduler_cancel_pending",
        cluster,
        job_id,
    )
    pending_path.parent.mkdir(parents=True, exist_ok=True)
    legacy_bytes = json.dumps(legacy_document, indent=2).encode("utf-8")
    assert len(legacy_bytes) <= 262_144
    pending_path.write_bytes(legacy_bytes)

    reloaded = queue.get_scheduler_cancel_pending(job_id, cluster=cluster)
    assert reloaded is not None
    assert len(reloaded.dispositions) == 1_000
    acquired_at = datetime(2026, 7, 14, 14, 0, tzinfo=UTC)
    claim = queue.claim_scheduler_cancel_attempt(
        job_id,
        cluster=cluster,
        scheduler_job_id="0",
        provider="slurm",
        lease_seconds=5,
        now=acquired_at,
    )
    assert claim is not None
    claimed_document = json.loads(pending_path.read_bytes())
    claimed = claimed_document["dispositions"][0]
    assert claimed["attempt_claim_id"] == claim.claim_id
    assert "attempt_claimed_at" in claimed
    assert "attempt_claim_expires_at" in claimed
    assert all(
        "attempt_claim_id" not in disposition
        for disposition in claimed_document["dispositions"][1:]
    )

    updated = queue.record_scheduler_cancel_attempt(
        job_id,
        cluster=cluster,
        scheduler_job_id="0",
        provider="slurm",
        claim_id=claim.claim_id,
        accepted=False,
        error="retry",
        max_attempts=5,
        retry_delay_seconds=1,
        now=acquired_at + timedelta(seconds=1),
    )
    assert updated is not None
    assert pending_path.stat().st_size <= 262_144
    cleared_document = json.loads(pending_path.read_bytes())
    assert all(
        "attempt_claim_id" not in disposition
        and "attempt_claimed_at" not in disposition
        and "attempt_claim_expires_at" not in disposition
        for disposition in cleared_document["dispositions"]
    )

    recovered = queue.claim_scheduler_cancel_attempt(
        job_id,
        cluster=cluster,
        scheduler_job_id="0",
        provider="slurm",
        lease_seconds=5,
        now=acquired_at + timedelta(seconds=3),
    )
    assert recovered is not None
    accepted = queue.record_scheduler_cancel_attempt(
        job_id,
        cluster=cluster,
        scheduler_job_id="0",
        provider="slurm",
        claim_id=recovered.claim_id,
        accepted=True,
        error=None,
        max_attempts=5,
        retry_delay_seconds=1,
        now=acquired_at + timedelta(seconds=3),
    )
    assert accepted is not None
    confirmation_claim = queue.claim_scheduler_cancel_confirmation(
        job_id,
        cluster=cluster,
        scheduler_job_id="0",
        provider="slurm",
        lease_seconds=5,
        now=acquired_at + timedelta(seconds=3),
    )
    assert confirmation_claim is not None
    confirmation_document = json.loads(pending_path.read_bytes())
    confirming = confirmation_document["dispositions"][0]
    assert confirming["confirmation_claim_id"] == confirmation_claim.claim_id
    assert "confirmation_claimed_at" in confirming
    assert "confirmation_claim_expires_at" in confirming
    assert all(
        "confirmation_claim_id" not in disposition
        for disposition in confirmation_document["dispositions"][1:]
    )
    terminal = queue.complete_scheduler_cancel_identity_scan(
        job_id,
        cluster=cluster,
        superseded=True,
    )
    assert terminal.complete is True
    completed_path = cast(Any, queue)._scheduler_cancel_record_path(
        "scheduler_cancel_dispositions",
        cluster,
        job_id,
    )
    completed_document = json.loads(completed_path.read_bytes())
    assert all(
        "attempt_claim_id" not in disposition
        and "attempt_claimed_at" not in disposition
        and "attempt_claim_expires_at" not in disposition
        and "confirmation_claim_id" not in disposition
        and "confirmation_claimed_at" not in disposition
        and "confirmation_claim_expires_at" not in disposition
        for disposition in completed_document["dispositions"]
    )
    assert queue.get_scheduler_cancel_disposition(job_id, cluster=cluster) == terminal

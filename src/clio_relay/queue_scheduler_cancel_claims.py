"""Scheduler-cancellation attempt/confirmation claim ownership.

Owns the four public methods that atomically claim or record one external
scheduler-cancellation attempt or confirmation poll: ``claim_scheduler_
cancel_attempt``, ``record_scheduler_cancel_attempt``,
``claim_scheduler_cancel_confirmation``, ``record_scheduler_cancel_
observation``. Distinct from ``queue_scheduler_cancel_state`` (CQ11), which
owns the durable pending/disposition record lifecycle these claims operate
against, and from ``queue_scheduler_cancel_records`` (CQ4), the pure codec
those claims decode into (``SchedulerCancelAttemptClaim``,
``SchedulerCancelConfirmationClaim``). Independent of the rest of CQ15's
lease-capacity/index/recovery family -- it never touches a lease record.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Literal
from uuid import uuid4

from clio_relay import (
    queue_context,
    queue_layout,
    queue_scheduler_cancel_records,
    queue_store_write,
)
from clio_relay.command_evidence import bounded_error_detail
from clio_relay.errors import QueueConflictError
from clio_relay.identifiers import validate_durable_record_id
from clio_relay.models import (
    SchedulerCancelDisposition,
    SchedulerCancelDispositionState,
    SchedulerCancelPending,
    SchedulerPhase,
    utc_now,
)

SchedulerCancelAttemptClaim = queue_scheduler_cancel_records.SchedulerCancelAttemptClaim
SchedulerCancelConfirmationClaim = queue_scheduler_cancel_records.SchedulerCancelConfirmationClaim


class QueueSchedulerCancelClaimsMixin:
    """Own scheduler-cancellation attempt/confirmation claim/record methods."""

    _storage_root: Path
    _lock: queue_context.QueueLockProtocol
    _store_adapter: queue_context.QueueStoreProtocol

    if TYPE_CHECKING:

        def initialize(self) -> None: ...
        def _scheduler_cancel_record_path(
            self,
            family: Literal["scheduler_cancel_pending", "scheduler_cancel_dispositions"],
            cluster: str,
            job_id: str,
        ) -> Path: ...
        def _persist_scheduler_cancel_record_unlocked(
            self, record: SchedulerCancelPending
        ) -> SchedulerCancelPending: ...

    def claim_scheduler_cancel_attempt(
        self,
        job_id: str,
        *,
        cluster: str,
        scheduler_job_id: str,
        provider: str,
        lease_seconds: float,
        now: datetime | None = None,
    ) -> SchedulerCancelAttemptClaim | None:
        """Atomically claim one due external cancellation attempt.

        The claim is persisted while holding the cross-process queue lock. An
        unexpired claim excludes every other worker, while an abandoned claim
        becomes recoverable after its bounded lease expires.
        """
        job_id = queue_layout.QueueLayout.require_durable_record_id(job_id, field="job_id")
        if not provider:
            raise ValueError("scheduler cancellation provider must not be empty")
        if not (
            queue_layout.MIN_SCHEDULER_CANCEL_CLAIM_LEASE_SECONDS
            <= lease_seconds
            <= queue_layout.MAX_SCHEDULER_CANCEL_CLAIM_LEASE_SECONDS
        ):
            raise ValueError(
                "scheduler cancellation claim lease must be between "
                f"{queue_layout.MIN_SCHEDULER_CANCEL_CLAIM_LEASE_SECONDS:g} and "
                f"{queue_layout.MAX_SCHEDULER_CANCEL_CLAIM_LEASE_SECONDS:g} seconds"
            )
        observed_at = now or utc_now()
        self.initialize()
        with self._lock:
            completed_path = self._scheduler_cancel_record_path(
                "scheduler_cancel_dispositions",
                cluster,
                job_id,
            )
            completed = self._store_adapter.read_optional(completed_path, SchedulerCancelPending)
            if completed is not None:
                if (
                    completed.job_id != job_id
                    or completed.cluster != cluster
                    or not completed.complete
                ):
                    raise QueueConflictError(
                        f"scheduler cancellation disposition identity mismatch: {completed_path}"
                    )
                queue_store_write.unlink_durable_path(
                    self._scheduler_cancel_record_path(
                        "scheduler_cancel_pending",
                        cluster,
                        job_id,
                    ),
                    missing_ok=True,
                )
                return None
            pending_path = self._scheduler_cancel_record_path(
                "scheduler_cancel_pending",
                cluster,
                job_id,
            )
            record = self._store_adapter.read_optional(pending_path, SchedulerCancelPending)
            if record is None:
                raise QueueConflictError(f"scheduler cancellation is not pending: {job_id}")
            if record.job_id != job_id or record.cluster != cluster:
                raise QueueConflictError(
                    f"scheduler cancellation identity mismatch: {pending_path}"
                )
            if record.identity_resolution != "resolved":
                return None
            dispositions = list(record.dispositions)
            index = next(
                (
                    position
                    for position, item in enumerate(dispositions)
                    if item.scheduler_job_id == scheduler_job_id
                ),
                None,
            )
            if index is None:
                raise QueueConflictError(
                    f"scheduler cancellation identity is not registered: {scheduler_job_id}"
                )
            current = dispositions[index]
            if current.state not in {
                SchedulerCancelDispositionState.PENDING,
                SchedulerCancelDispositionState.RETRY_WAIT,
            }:
                return None
            if current.next_attempt_at is not None and current.next_attempt_at > observed_at:
                return None
            if (
                current.attempt_claim_id is not None
                and current.attempt_claim_expires_at is not None
                and current.attempt_claim_expires_at > observed_at
            ):
                return None
            if current.provider is not None and current.provider != provider:
                raise QueueConflictError(
                    "scheduler cancellation provider changed for "
                    f"{scheduler_job_id}: {current.provider} != {provider}"
                )
            claim_id = validate_durable_record_id(f"cancelclaim_{uuid4().hex}")
            expires_at = observed_at + timedelta(seconds=lease_seconds)
            dispositions[index] = current.model_copy(
                update={
                    "provider": provider,
                    "attempt_claim_id": claim_id,
                    "attempt_claimed_at": observed_at,
                    "attempt_claim_expires_at": expires_at,
                    "updated_at": observed_at,
                }
            )
            updated = record.model_copy(
                update={"dispositions": dispositions, "updated_at": observed_at}
            )
            self._persist_scheduler_cancel_record_unlocked(updated)
            return SchedulerCancelAttemptClaim(
                claim_id=claim_id,
                scheduler_job_id=scheduler_job_id,
                provider=provider,
                attempt=current.attempts + 1,
                claimed_at=observed_at,
                expires_at=expires_at,
            )

    def record_scheduler_cancel_attempt(
        self,
        job_id: str,
        *,
        cluster: str,
        scheduler_job_id: str,
        provider: str,
        claim_id: str,
        accepted: bool,
        error: str | None,
        max_attempts: int,
        retry_delay_seconds: float,
        now: datetime | None = None,
    ) -> SchedulerCancelPending | None:
        """Persist a claimed attempt, or ignore a stale claimant idempotently."""
        job_id = queue_layout.QueueLayout.require_durable_record_id(job_id, field="job_id")
        claim_id = validate_durable_record_id(claim_id)
        observed_at = now or utc_now()
        self.initialize()
        with self._lock:
            completed_path = self._scheduler_cancel_record_path(
                "scheduler_cancel_dispositions",
                cluster,
                job_id,
            )
            completed = self._store_adapter.read_optional(completed_path, SchedulerCancelPending)
            if completed is not None:
                if (
                    completed.job_id != job_id
                    or completed.cluster != cluster
                    or not completed.complete
                ):
                    raise QueueConflictError(
                        f"scheduler cancellation disposition identity mismatch: {completed_path}"
                    )
                queue_store_write.unlink_durable_path(
                    self._scheduler_cancel_record_path(
                        "scheduler_cancel_pending",
                        cluster,
                        job_id,
                    ),
                    missing_ok=True,
                )
                return None
            pending_path = self._scheduler_cancel_record_path(
                "scheduler_cancel_pending",
                cluster,
                job_id,
            )
            record = self._store_adapter.read_optional(pending_path, SchedulerCancelPending)
            if record is None:
                raise QueueConflictError(f"scheduler cancellation is not pending: {job_id}")
            if record.job_id != job_id or record.cluster != cluster:
                raise QueueConflictError(
                    f"scheduler cancellation identity mismatch: {pending_path}"
                )
            dispositions = list(record.dispositions)
            index = next(
                (
                    position
                    for position, item in enumerate(dispositions)
                    if item.scheduler_job_id == scheduler_job_id
                ),
                None,
            )
            if index is None:
                raise QueueConflictError(
                    f"scheduler cancellation identity is not registered: {scheduler_job_id}"
                )
            current = dispositions[index]
            if current.attempt_claim_id != claim_id:
                return None
            if current.provider is not None and current.provider != provider:
                raise QueueConflictError(
                    "scheduler cancellation provider changed for "
                    f"{scheduler_job_id}: {current.provider} != {provider}"
                )
            attempts = current.attempts + 1
            bounded_error = bounded_error_detail(error)
            if accepted:
                state = SchedulerCancelDispositionState.CANCEL_REQUESTED
                # Make the first confirmation immediately claimable.  The
                # successful worker still polls eagerly, while a crash between
                # acceptance and polling leaves due work for another worker.
                next_attempt_at = observed_at
                last_error = None
            elif attempts >= max_attempts:
                state = SchedulerCancelDispositionState.EXHAUSTED
                next_attempt_at = None
                last_error = bounded_error or "scheduler cancellation failed"
            else:
                state = SchedulerCancelDispositionState.RETRY_WAIT
                next_attempt_at = observed_at + timedelta(seconds=retry_delay_seconds)
                last_error = bounded_error or "scheduler cancellation failed"
            dispositions[index] = SchedulerCancelDisposition.model_validate(
                {
                    **current.model_dump(),
                    "provider": provider,
                    "state": state,
                    "attempts": attempts,
                    "next_attempt_at": next_attempt_at,
                    "last_error": last_error,
                    "attempt_claim_id": None,
                    "attempt_claimed_at": None,
                    "attempt_claim_expires_at": None,
                    "updated_at": observed_at,
                },
            )
            updated = record.model_copy(
                update={"dispositions": dispositions, "updated_at": observed_at}
            )
            return self._persist_scheduler_cancel_record_unlocked(updated)

    def claim_scheduler_cancel_confirmation(
        self,
        job_id: str,
        *,
        cluster: str,
        scheduler_job_id: str,
        provider: str,
        lease_seconds: float,
        now: datetime | None = None,
    ) -> SchedulerCancelConfirmationClaim | None:
        """Atomically claim one due scheduler cancellation confirmation poll."""
        job_id = queue_layout.QueueLayout.require_durable_record_id(job_id, field="job_id")
        if not provider:
            raise ValueError("scheduler cancellation provider must not be empty")
        if not (
            queue_layout.MIN_SCHEDULER_CANCEL_CLAIM_LEASE_SECONDS
            <= lease_seconds
            <= queue_layout.MAX_SCHEDULER_CANCEL_CLAIM_LEASE_SECONDS
        ):
            raise ValueError(
                "scheduler cancellation confirmation claim lease must be between "
                f"{queue_layout.MIN_SCHEDULER_CANCEL_CLAIM_LEASE_SECONDS:g} and "
                f"{queue_layout.MAX_SCHEDULER_CANCEL_CLAIM_LEASE_SECONDS:g} seconds"
            )
        observed_at = now or utc_now()
        self.initialize()
        with self._lock:
            completed_path = self._scheduler_cancel_record_path(
                "scheduler_cancel_dispositions",
                cluster,
                job_id,
            )
            completed = self._store_adapter.read_optional(completed_path, SchedulerCancelPending)
            if completed is not None:
                if (
                    completed.job_id != job_id
                    or completed.cluster != cluster
                    or not completed.complete
                ):
                    raise QueueConflictError(
                        f"scheduler cancellation disposition identity mismatch: {completed_path}"
                    )
                queue_store_write.unlink_durable_path(
                    self._scheduler_cancel_record_path(
                        "scheduler_cancel_pending",
                        cluster,
                        job_id,
                    ),
                    missing_ok=True,
                )
                return None
            pending_path = self._scheduler_cancel_record_path(
                "scheduler_cancel_pending",
                cluster,
                job_id,
            )
            record = self._store_adapter.read_optional(pending_path, SchedulerCancelPending)
            if record is None:
                raise QueueConflictError(f"scheduler cancellation is not pending: {job_id}")
            if record.job_id != job_id or record.cluster != cluster:
                raise QueueConflictError(
                    f"scheduler cancellation identity mismatch: {pending_path}"
                )
            if record.identity_resolution != "resolved":
                return None
            dispositions = list(record.dispositions)
            index = next(
                (
                    position
                    for position, item in enumerate(dispositions)
                    if item.scheduler_job_id == scheduler_job_id
                ),
                None,
            )
            if index is None:
                raise QueueConflictError(
                    f"scheduler cancellation identity is not registered: {scheduler_job_id}"
                )
            current = dispositions[index]
            if current.state is not SchedulerCancelDispositionState.CANCEL_REQUESTED:
                return None
            if current.next_attempt_at is not None and current.next_attempt_at > observed_at:
                return None
            if (
                current.confirmation_claim_id is not None
                and current.confirmation_claim_expires_at is not None
                and current.confirmation_claim_expires_at > observed_at
            ):
                return None
            if current.provider is not None and current.provider != provider:
                raise QueueConflictError(
                    "scheduler cancellation provider changed for "
                    f"{scheduler_job_id}: {current.provider} != {provider}"
                )
            claim_id = validate_durable_record_id(f"confirmclaim_{uuid4().hex}")
            expires_at = observed_at + timedelta(seconds=lease_seconds)
            dispositions[index] = current.model_copy(
                update={
                    "provider": provider,
                    "confirmation_claim_id": claim_id,
                    "confirmation_claimed_at": observed_at,
                    "confirmation_claim_expires_at": expires_at,
                    "updated_at": observed_at,
                }
            )
            updated = record.model_copy(
                update={"dispositions": dispositions, "updated_at": observed_at}
            )
            self._persist_scheduler_cancel_record_unlocked(updated)
            return SchedulerCancelConfirmationClaim(
                claim_id=claim_id,
                scheduler_job_id=scheduler_job_id,
                provider=provider,
                confirmation_attempt=current.confirmation_attempts + 1,
                claimed_at=observed_at,
                expires_at=expires_at,
            )

    def record_scheduler_cancel_observation(
        self,
        job_id: str,
        *,
        cluster: str,
        scheduler_job_id: str,
        provider: str,
        claim_id: str,
        phase: SchedulerPhase,
        not_found: bool,
        error: str | None,
        max_confirmation_attempts: int,
        retry_delay_seconds: float,
        now: datetime | None = None,
    ) -> SchedulerCancelPending | None:
        """Persist a claimed confirmation, or ignore a stale claimant idempotently."""
        job_id = queue_layout.QueueLayout.require_durable_record_id(job_id, field="job_id")
        claim_id = validate_durable_record_id(claim_id)
        observed_at = now or utc_now()
        self.initialize()
        with self._lock:
            completed_path = self._scheduler_cancel_record_path(
                "scheduler_cancel_dispositions",
                cluster,
                job_id,
            )
            completed = self._store_adapter.read_optional(completed_path, SchedulerCancelPending)
            if completed is not None:
                if (
                    completed.job_id != job_id
                    or completed.cluster != cluster
                    or not completed.complete
                ):
                    raise QueueConflictError(
                        f"scheduler cancellation disposition identity mismatch: {completed_path}"
                    )
                queue_store_write.unlink_durable_path(
                    self._scheduler_cancel_record_path(
                        "scheduler_cancel_pending",
                        cluster,
                        job_id,
                    ),
                    missing_ok=True,
                )
                return None
            pending_path = self._scheduler_cancel_record_path(
                "scheduler_cancel_pending",
                cluster,
                job_id,
            )
            record = self._store_adapter.read_optional(pending_path, SchedulerCancelPending)
            if record is None:
                raise QueueConflictError(f"scheduler cancellation is not pending: {job_id}")
            if record.job_id != job_id or record.cluster != cluster:
                raise QueueConflictError(
                    f"scheduler cancellation identity mismatch: {pending_path}"
                )
            dispositions = list(record.dispositions)
            index = next(
                (
                    position
                    for position, item in enumerate(dispositions)
                    if item.scheduler_job_id == scheduler_job_id
                ),
                None,
            )
            if index is None:
                raise QueueConflictError(
                    f"scheduler cancellation identity is not registered: {scheduler_job_id}"
                )
            current = dispositions[index]
            if current.confirmation_claim_id != claim_id:
                return None
            if current.provider is not None and current.provider != provider:
                raise QueueConflictError(
                    "scheduler cancellation provider changed for "
                    f"{scheduler_job_id}: {current.provider} != {provider}"
                )
            confirmations = current.confirmation_attempts + 1
            bounded_error = bounded_error_detail(error)
            if phase is SchedulerPhase.CANCELED:
                state = SchedulerCancelDispositionState.CANCELED
                next_attempt_at = None
                last_error = None
            elif phase in {SchedulerPhase.COMPLETED, SchedulerPhase.FAILED}:
                state = SchedulerCancelDispositionState.TERMINAL
                next_attempt_at = None
                last_error = None
            elif not_found:
                state = SchedulerCancelDispositionState.NOT_FOUND
                next_attempt_at = None
                last_error = None
            elif confirmations >= max_confirmation_attempts:
                state = SchedulerCancelDispositionState.EXHAUSTED
                next_attempt_at = None
                last_error = bounded_error or (
                    f"scheduler cancellation was not confirmed terminal: {phase.value}"
                )
            else:
                state = SchedulerCancelDispositionState.CANCEL_REQUESTED
                next_attempt_at = observed_at + timedelta(seconds=retry_delay_seconds)
                last_error = bounded_error
            dispositions[index] = SchedulerCancelDisposition.model_validate(
                {
                    **current.model_dump(),
                    "state": state,
                    "confirmation_attempts": confirmations,
                    "next_attempt_at": next_attempt_at,
                    "last_error": last_error,
                    "confirmation_claim_id": None,
                    "confirmation_claimed_at": None,
                    "confirmation_claim_expires_at": None,
                    "updated_at": observed_at,
                },
            )
            updated = record.model_copy(
                update={"dispositions": dispositions, "updated_at": observed_at}
            )
            return self._persist_scheduler_cancel_record_unlocked(updated)

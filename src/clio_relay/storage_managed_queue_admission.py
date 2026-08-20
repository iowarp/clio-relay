"""Reserve-before-admit and terminal-release for genuinely new queue admission.

Owns :class:`StorageManagedQueueAdmissionMixin`, the method group that wraps
each idempotent admission boundary (``submit_job``, the input-artifact-ingest
lifecycle) in a durable storage reservation, plus the shared release/verify
helpers ``StorageManagedQueue`` and :mod:`storage_managed_queue_leasing` both
call after a job settles into a terminal state.

This class extends :class:`~clio_relay.core_queue.ClioCoreQueue` directly
(not a bare mixin) purely so ``super().submit_job(...)`` and friends resolve
correctly under both the real MRO and static type checking; it is composed
into ``StorageManagedQueue`` alongside
:class:`~clio_relay.storage_managed_queue_leasing.StorageManagedQueueLeasingMixin`
and is never instantiated on its own.
"""

from __future__ import annotations

from datetime import datetime

from clio_relay.core_queue import MAX_INPUT_INGEST_RECOVERY_BATCH, ClioCoreQueue
from clio_relay.errors import NotFoundError, RelayError
from clio_relay.models import (
    TERMINAL_STATES,
    InputArtifactIngestPolicy,
    JobState,
    RelayJob,
)
from clio_relay.storage_policy import StoragePolicyError, StorageReason
from clio_relay.storage_runtime_core import StorageRuntime
from clio_relay.storage_runtime_errors import (
    StorageAdmissionError,
    _denied_decision,
    _policy_error_decision,
)


class StorageManagedQueueAdmissionMixin(ClioCoreQueue):
    """Own reserve-before-admit submission and the input-ingest retry lifecycle."""

    storage_runtime: StorageRuntime

    def submit_job(self, job: RelayJob) -> RelayJob:
        """Reserve storage before a genuinely new idempotent queue admission."""
        estimate = self.storage_runtime.estimate(job)
        try:
            with self.storage_runtime.policy.admission_lock():
                resolution = self.resolve_idempotent_submission(job)
                canonical = job.model_copy(update={"job_id": resolution.canonical_job_id})
                if resolution.state in {"existing", "retired"}:
                    saved = super().submit_job(canonical)
                    return self._verify_existing_reservation(saved)
                if resolution.state not in {"new", "reserved"}:
                    raise StorageAdmissionError(
                        _denied_decision(
                            StorageReason.INVALID_REQUEST,
                            "queue returned an unsupported idempotency resolution state",
                            details={"state": resolution.state},
                        )
                    )
            tree_snapshot = self.storage_runtime.policy.capture_admission_snapshot()
            with self.storage_runtime.policy.admission_lock():
                resolution = self.resolve_idempotent_submission(job)
                canonical = job.model_copy(update={"job_id": resolution.canonical_job_id})
                if resolution.state in {"existing", "retired"}:
                    saved = super().submit_job(canonical)
                    return self._verify_existing_reservation(saved)
                if resolution.state not in {"new", "reserved"}:
                    raise StorageAdmissionError(
                        _denied_decision(
                            StorageReason.INVALID_REQUEST,
                            "queue returned an unsupported idempotency resolution state",
                            details={"state": resolution.state},
                        )
                    )
                self.storage_runtime.ensure_new_intake_allowed()
                decision = self.storage_runtime.policy.reserve(
                    canonical.job_id,
                    core_bytes=estimate.core_bytes,
                    spool_bytes=estimate.spool_bytes,
                    tree_snapshot=tree_snapshot,
                )
                if not decision.allowed:
                    raise StorageAdmissionError(decision)
                try:
                    saved = super().submit_job(canonical)
                except BaseException:
                    self._release_failed_admission(canonical.job_id)
                    raise
                if saved.job_id != canonical.job_id:
                    self._release_reservation(canonical.job_id, terminal_job=None)
                    return self._verify_existing_reservation(saved)
                if saved.state in TERMINAL_STATES:
                    self._release_reservation(saved.job_id, terminal_job=saved)
                return saved
        except StoragePolicyError as exc:
            raise StorageAdmissionError(_policy_error_decision(exc)) from exc

    def begin_input_ingest(
        self,
        job_id: str,
        *,
        attempt_id: str,
        policy: InputArtifactIngestPolicy | None = None,
    ) -> tuple[RelayJob, bool]:
        """Claim an ingest, restoring storage admission for an exact failed retry."""
        try:
            with self.storage_runtime.policy.admission_lock():
                current = self.get_job(job_id)
                if current.state is not JobState.FAILED:
                    if current.state not in TERMINAL_STATES:
                        self._verify_existing_reservation(current)
                    return super().begin_input_ingest(
                        job_id,
                        attempt_id=attempt_id,
                        policy=policy,
                    )
            tree_snapshot = self.storage_runtime.policy.capture_admission_snapshot()
            with self.storage_runtime.policy.admission_lock():
                current = self.get_job(job_id)
                if current.state is not JobState.FAILED:
                    if current.state not in TERMINAL_STATES:
                        self._verify_existing_reservation(current)
                    return super().begin_input_ingest(
                        job_id,
                        attempt_id=attempt_id,
                        policy=policy,
                    )
                estimate = self.storage_runtime.estimate(current)
                self.storage_runtime.ensure_new_intake_allowed()
                decision = self.storage_runtime.policy.reserve(
                    current.job_id,
                    core_bytes=estimate.core_bytes,
                    spool_bytes=estimate.spool_bytes,
                    tree_snapshot=tree_snapshot,
                )
                if not decision.allowed:
                    raise StorageAdmissionError(decision)
                try:
                    saved, changed = super().begin_input_ingest(
                        job_id,
                        attempt_id=attempt_id,
                        policy=policy,
                    )
                except BaseException:
                    self._release_reservation(current.job_id, terminal_job=None)
                    raise
                if saved.state in TERMINAL_STATES:
                    self._release_reservation(saved.job_id, terminal_job=saved)
                return saved, changed
        except StoragePolicyError as exc:
            raise StorageAdmissionError(_policy_error_decision(exc)) from exc

    def fail_input_ingest(
        self,
        job_id: str,
        *,
        attempt_id: str,
        error: str,
    ) -> tuple[RelayJob, bool]:
        """Release reserved capacity after the exact ingest attempt fails."""
        try:
            with self.storage_runtime.policy.admission_lock():
                saved, changed = super().fail_input_ingest(
                    job_id,
                    attempt_id=attempt_id,
                    error=error,
                )
                if saved.state in TERMINAL_STATES:
                    self._release_reservation(saved.job_id, terminal_job=saved)
                return saved, changed
        except StoragePolicyError as exc:
            raise StorageAdmissionError(_policy_error_decision(exc)) from exc

    def recover_abandoned_input_ingests(
        self,
        *,
        cluster: str,
        stale_before: datetime | None = None,
        limit: int = MAX_INPUT_INGEST_RECOVERY_BATCH,
    ) -> list[RelayJob]:
        """Release capacity for bounded synchronous ingests recovered as failed."""
        try:
            with self.storage_runtime.policy.admission_lock():
                recovered = super().recover_abandoned_input_ingests(
                    cluster=cluster,
                    stale_before=stale_before,
                    limit=limit,
                )
                for job in recovered:
                    if job.state in TERMINAL_STATES:
                        self._release_reservation(job.job_id, terminal_job=job)
                return recovered
        except StoragePolicyError as exc:
            raise StorageAdmissionError(_policy_error_decision(exc)) from exc

    def complete_input_ingest(
        self,
        job_id: str,
        *,
        attempt_id: str | None = None,
    ) -> tuple[RelayJob, bool]:
        """Release reserved capacity after atomic ingest completion or replay."""
        try:
            with self.storage_runtime.policy.admission_lock():
                saved, changed = super().complete_input_ingest(
                    job_id,
                    attempt_id=attempt_id,
                )
                self._release_reservation(saved.job_id, terminal_job=saved)
                return saved, changed
        except StoragePolicyError as exc:
            raise StorageAdmissionError(_policy_error_decision(exc)) from exc

    def _verify_existing_reservation(self, job: RelayJob) -> RelayJob:
        if job.state in TERMINAL_STATES:
            self._release_reservation(job.job_id, terminal_job=job)
            return job
        estimate = self.storage_runtime.estimate(job)
        decision = self.storage_runtime.policy.verify_reservation(
            job.job_id,
            core_bytes=estimate.core_bytes,
            spool_bytes=estimate.spool_bytes,
        )
        if not decision.allowed:
            self.storage_runtime.block_new_intake(decision)
            raise StorageAdmissionError(decision)
        return job

    def _release_failed_admission(self, job_id: str) -> None:
        try:
            existing = self.get_job(job_id)
        except NotFoundError:
            self._release_reservation(job_id, terminal_job=None)
            return
        if existing.state in TERMINAL_STATES:
            self._release_reservation(job_id, terminal_job=existing)

    def _release_reservation(
        self,
        job_id: str,
        *,
        terminal_job: RelayJob | None,
    ) -> None:
        decision = self.storage_runtime.policy.release(job_id)
        if decision.allowed:
            return
        self.storage_runtime.block_new_intake(decision)
        if terminal_job is None:
            raise StorageAdmissionError(decision)
        try:
            super().append_event(
                terminal_job.job_id,
                "storage.reservation_release_failed",
                "Terminal job storage reservation could not be released",
                payload=decision.to_dict(),
            )
        except RelayError:
            return

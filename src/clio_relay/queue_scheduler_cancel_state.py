"""Durable scheduler-cancellation pending/disposition state ownership.

Owns the record-path derivation and unlocked read/write primitives for
scheduler-cancellation pending and terminal-disposition records -- the
CQ4-IO-01 typed deviation: CQ4 left these four I/O-bearing helpers in the
facade because ``queue_scheduler_cancel_records`` is a store-independent
codec module (``test_cq4_codecs_are_store_independent``) and they scan,
read, write, and unlink durable state. This owner also holds every public
facade method that creates, reads, scans, registers an identity against, or
closes that durable pending state. Attempt/confirmation claim behavior
(``queue_scheduler_cancel_claims.py``) is a distinct, later owner.
"""

from __future__ import annotations

import hashlib
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Literal

from clio_relay import (
    queue_context,
    queue_index_state,
    queue_layout,
    queue_scheduler_cancel_records,
    queue_store_read,
    queue_store_write,
)
from clio_relay.errors import QueueConflictError, queue_conflict_from_cause
from clio_relay.models import (
    RelayJob,
    SchedulerCancelDisposition,
    SchedulerCancelDispositionState,
    SchedulerCancelPending,
    utc_now,
)

logger = logging.getLogger(__name__)


def _stable_ref_token(*values: str) -> str:
    return hashlib.sha256("\x00".join(values).encode("utf-8")).hexdigest()[:32]


class QueueSchedulerCancelStateMixin:
    """Own durable scheduler-cancellation pending/disposition state."""

    _storage_root: Path
    _lock: queue_context.QueueLockProtocol
    _store_adapter: queue_context.QueueStoreProtocol

    def ensure_scheduler_cancel_pending(
        self,
        job_id: str,
        *,
        reason: str,
    ) -> SchedulerCancelPending:
        """Ensure retryable scheduler cancellation work exists for one job."""
        job_id = queue_layout.QueueLayout.require_durable_record_id(job_id, field="job_id")
        self._store_adapter.initialize()
        queue_index_state.require_index_migration_complete(self._storage_root)
        with self._lock:
            job = queue_store_read.read_required_job(self._storage_root, job_id)
            return self._ensure_scheduler_cancel_pending_unlocked(
                job,
                requested_at=utc_now(),
                reason=reason,
            )

    def get_scheduler_cancel_pending(
        self,
        job_id: str,
        *,
        cluster: str,
    ) -> SchedulerCancelPending | None:
        """Return exact pending scheduler cancellation state when present."""
        job_id = queue_layout.QueueLayout.require_durable_record_id(job_id, field="job_id")
        path = self._scheduler_cancel_record_path(
            "scheduler_cancel_pending",
            cluster,
            job_id,
        )
        record = queue_store_read.read_optional(self._storage_root, path, SchedulerCancelPending)
        if record is not None and (record.job_id != job_id or record.cluster != cluster):
            raise QueueConflictError(f"scheduler cancellation identity mismatch: {path}")
        return record

    def get_scheduler_cancel_disposition(
        self,
        job_id: str,
        *,
        cluster: str,
    ) -> SchedulerCancelPending | None:
        """Return terminal scheduler cancellation evidence when present."""
        job_id = queue_layout.QueueLayout.require_durable_record_id(job_id, field="job_id")
        path = self._scheduler_cancel_record_path(
            "scheduler_cancel_dispositions",
            cluster,
            job_id,
        )
        record = queue_store_read.read_optional(self._storage_root, path, SchedulerCancelPending)
        if record is not None and (
            record.job_id != job_id or record.cluster != cluster or not record.complete
        ):
            raise QueueConflictError(
                f"scheduler cancellation disposition identity mismatch: {path}"
            )
        return record

    def scan_due_scheduler_cancellations(
        self,
        *,
        cluster: str,
        limit: int,
        now: datetime | None = None,
    ) -> tuple[list[SchedulerCancelPending], bool]:
        """Return a bounded due batch from one cluster's pending-cancellation index."""
        if limit < 1 or limit > queue_layout.DEFAULT_EXACT_RECORD_LIMIT:
            raise ValueError(
                f"scheduler cancellation batch limit must be between 1 and "
                f"{queue_layout.DEFAULT_EXACT_RECORD_LIMIT}"
            )
        self._store_adapter.initialize()
        queue_index_state.require_index_migration_complete(self._storage_root)
        with self._lock:
            records, index_truncated = queue_store_read.scan_many(
                self._storage_root / "scheduler_cancel_pending" / _stable_ref_token(cluster),
                SchedulerCancelPending,
                limit=queue_layout.MAX_ACTIVE_JOB_RECORDS,
            )
            active_records: list[SchedulerCancelPending] = []
            for record in records:
                completed_path = self._scheduler_cancel_record_path(
                    "scheduler_cancel_dispositions",
                    record.cluster,
                    record.job_id,
                )
                completed = queue_store_read.read_optional(
                    self._storage_root,
                    completed_path,
                    SchedulerCancelPending,
                )
                if completed is not None:
                    if not completed.complete:
                        raise QueueConflictError(
                            f"scheduler cancellation disposition is not terminal: {completed_path}"
                        )
                    queue_store_write.unlink_durable_path(
                        self._scheduler_cancel_record_path(
                            "scheduler_cancel_pending",
                            record.cluster,
                            record.job_id,
                        ),
                        missing_ok=True,
                    )
                    continue
                active_records.append(record)
            records = active_records
        observed_at = now or utc_now()
        due = [
            record
            for record in records
            if queue_scheduler_cancel_records.scheduler_cancel_record_is_due(record, observed_at)
        ]
        due.sort(key=queue_scheduler_cancel_records.scheduler_cancel_due_sort_key)
        return due[:limit], index_truncated or len(due) > limit

    def register_scheduler_cancel_identity(
        self,
        job_id: str,
        *,
        cluster: str,
        scheduler_job_id: str,
        provider: str | None,
        ownership_verified: bool,
    ) -> SchedulerCancelPending:
        """Add a verified pending identity or a terminal refused disposition."""
        return self.register_scheduler_cancel_identity_once(
            job_id,
            cluster=cluster,
            scheduler_job_id=scheduler_job_id,
            provider=provider,
            ownership_verified=ownership_verified,
        ).record

    def register_scheduler_cancel_identity_once(
        self,
        job_id: str,
        *,
        cluster: str,
        scheduler_job_id: str,
        provider: str | None,
        ownership_verified: bool,
    ) -> queue_scheduler_cancel_records.SchedulerCancelIdentityRegistration:
        """Register an identity and report whether this call created its disposition."""
        job_id = queue_layout.QueueLayout.require_durable_record_id(job_id, field="job_id")
        self._store_adapter.initialize()
        with self._lock:
            record = self._require_scheduler_cancel_pending_unlocked(job_id, cluster=cluster)
            dispositions = list(record.dispositions)
            existing_index = next(
                (
                    index
                    for index, item in enumerate(dispositions)
                    if item.scheduler_job_id == scheduler_job_id
                ),
                None,
            )
            state = (
                SchedulerCancelDispositionState.PENDING
                if ownership_verified
                else SchedulerCancelDispositionState.REFUSED
            )
            candidate = SchedulerCancelDisposition(
                scheduler_job_id=scheduler_job_id,
                provider=provider,
                state=state,
                last_error=(
                    None if ownership_verified else "scheduler identity ownership unverified"
                ),
            )
            if existing_index is None:
                dispositions.append(candidate)
            else:
                existing = dispositions[existing_index]
                if (
                    existing.state is not SchedulerCancelDispositionState.REFUSED
                    or not ownership_verified
                ):
                    return queue_scheduler_cancel_records.SchedulerCancelIdentityRegistration(
                        record=record,
                        disposition_created=False,
                    )
                dispositions[existing_index] = candidate.model_copy(
                    update={
                        "attempts": existing.attempts,
                        "confirmation_attempts": existing.confirmation_attempts,
                        "updated_at": utc_now(),
                    }
                )
            updated = record.model_copy(
                update={
                    "dispositions": dispositions,
                    "updated_at": utc_now(),
                }
            )
            persisted = self._persist_scheduler_cancel_record_unlocked(updated)
            return queue_scheduler_cancel_records.SchedulerCancelIdentityRegistration(
                record=persisted,
                disposition_created=existing_index is None,
            )

    def finalize_scheduler_cancel_identities(
        self,
        job_id: str,
        *,
        cluster: str,
    ) -> SchedulerCancelPending:
        """Declare the current durable identity set complete before attempts begin."""
        job_id = queue_layout.QueueLayout.require_durable_record_id(job_id, field="job_id")
        self._store_adapter.initialize()
        with self._lock:
            record = self._require_scheduler_cancel_pending_unlocked(job_id, cluster=cluster)
            if not record.dispositions:
                raise QueueConflictError(
                    f"scheduler cancellation has no identities to finalize: {job_id}"
                )
            updated = record.model_copy(
                update={"identity_resolution": "resolved", "updated_at": utc_now()}
            )
            return self._persist_scheduler_cancel_record_unlocked(updated)

    def complete_scheduler_cancel_identity_scan(
        self,
        job_id: str,
        *,
        cluster: str,
        superseded: bool = False,
    ) -> SchedulerCancelPending:
        """Close pending work when no scheduler identity exists or relay state won the race."""
        job_id = queue_layout.QueueLayout.require_durable_record_id(job_id, field="job_id")
        self._store_adapter.initialize()
        with self._lock:
            record = self._require_scheduler_cancel_pending_unlocked(job_id, cluster=cluster)
            if record.dispositions and not superseded:
                return record
            dispositions = record.dispositions
            if superseded:
                dispositions = [
                    item.model_copy(
                        update={
                            "attempt_claim_id": None,
                            "attempt_claimed_at": None,
                            "attempt_claim_expires_at": None,
                            "confirmation_claim_id": None,
                            "confirmation_claimed_at": None,
                            "confirmation_claim_expires_at": None,
                        }
                    )
                    for item in dispositions
                ]
            updated = record.model_copy(
                update={
                    "identity_resolution": "superseded" if superseded else "none",
                    "dispositions": dispositions,
                    "updated_at": utc_now(),
                }
            )
            return self._persist_scheduler_cancel_record_unlocked(updated)

    def _scheduler_cancel_record_path(
        self,
        family: Literal["scheduler_cancel_pending", "scheduler_cancel_dispositions"],
        cluster: str,
        job_id: str,
    ) -> Path:
        return (
            self._storage_root
            / family
            / _stable_ref_token(cluster)
            / f"{queue_layout.QueueLayout.durable_key(job_id)}.json"
        )

    def _ensure_scheduler_cancel_pending_unlocked(
        self,
        job: RelayJob,
        *,
        requested_at: datetime,
        reason: str,
    ) -> SchedulerCancelPending:
        pending_path = self._scheduler_cancel_record_path(
            "scheduler_cancel_pending",
            job.cluster,
            job.job_id,
        )
        existing = queue_store_read.read_optional(
            self._storage_root,
            pending_path,
            SchedulerCancelPending,
        )
        if existing is not None:
            if existing.job_id != job.job_id or existing.cluster != job.cluster:
                raise QueueConflictError(
                    f"scheduler cancellation identity mismatch: {pending_path}"
                )
            return existing
        completed_path = self._scheduler_cancel_record_path(
            "scheduler_cancel_dispositions",
            job.cluster,
            job.job_id,
        )
        completed = queue_store_read.read_optional(
            self._storage_root,
            completed_path,
            SchedulerCancelPending,
        )
        if completed is not None:
            if completed.job_id != job.job_id or completed.cluster != job.cluster:
                raise QueueConflictError(
                    f"scheduler cancellation disposition identity mismatch: {completed_path}"
                )
            return completed
        pending_root = (
            self._storage_root / "scheduler_cancel_pending" / _stable_ref_token(job.cluster)
        )
        count = 0
        try:
            with os.scandir(pending_root) as entries:
                for entry in entries:
                    if not entry.name.endswith(".json"):
                        raise QueueConflictError(
                            f"scheduler cancellation index contains an unsafe record: {entry.path}"
                        )
                    count += 1
                    if count >= queue_layout.MAX_ACTIVE_JOB_RECORDS:
                        raise QueueConflictError(
                            "scheduler cancellation capacity reached for cluster; retry after "
                            "pending cancellation work drains"
                        )
        except FileNotFoundError:
            pass
        except OSError as exc:
            raise queue_conflict_from_cause(
                "cannot inspect scheduler cancellation capacity",
                cause=exc,
                logger=logger,
            ) from exc
        record = SchedulerCancelPending(
            job_id=job.job_id,
            cluster=job.cluster,
            requested_at=requested_at,
            reason=reason,
        )
        queue_store_write.write_model(self._storage_root, pending_path, record)
        return record

    def _require_scheduler_cancel_pending_unlocked(
        self,
        job_id: str,
        *,
        cluster: str,
    ) -> SchedulerCancelPending:
        path = self._scheduler_cancel_record_path(
            "scheduler_cancel_pending",
            cluster,
            job_id,
        )
        record = queue_store_read.read_optional(self._storage_root, path, SchedulerCancelPending)
        if record is None:
            raise QueueConflictError(f"scheduler cancellation is not pending: {job_id}")
        if record.job_id != job_id or record.cluster != cluster:
            raise QueueConflictError(f"scheduler cancellation identity mismatch: {path}")
        return record

    def _persist_scheduler_cancel_record_unlocked(
        self,
        record: SchedulerCancelPending,
    ) -> SchedulerCancelPending:
        pending_path = self._scheduler_cancel_record_path(
            "scheduler_cancel_pending",
            record.cluster,
            record.job_id,
        )
        if not record.complete:
            queue_store_write.write_model(self._storage_root, pending_path, record)
            return record
        completed_path = self._scheduler_cancel_record_path(
            "scheduler_cancel_dispositions",
            record.cluster,
            record.job_id,
        )
        queue_store_write.write_model(self._storage_root, completed_path, record)
        queue_store_write.unlink_durable_path(pending_path, missing_ok=True)
        return record

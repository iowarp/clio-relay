"""Canonical structured job-progress record ownership.

Owns every public method that appends, reads, pages, or resolves the latest
``ProgressRecord`` for a job. A progress write is canonical-plus-derived: one
canonical record under ``progress/``, one per-job derived copy under
``progress_by_job/``, and one per-job ordered-sequence copy converged through
the CQ7 order-index owner's job-index primitives (``queue_order_index.py``).
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from pydantic import BaseModel

from clio_relay import queue_context, queue_index_state, queue_layout, queue_store_read
from clio_relay.errors import QueueConflictError
from clio_relay.models import ProgressRecord, RelayEvent, RelayJob


class QueueProgressMixin:
    """Own canonical progress records: append, list, page, and latest lookup."""

    _storage_root: Path
    _lock: queue_context.QueueLockProtocol
    _store_adapter: queue_context.QueueStoreProtocol

    if TYPE_CHECKING:

        def get_job(self, job_id: str) -> RelayJob: ...
        def append_event(
            self,
            job_id: str,
            event_type: str,
            message: str,
            *,
            locked: bool = False,
            payload: dict[str, object] | None = None,
        ) -> RelayEvent: ...
        def _job_record_path(self, family: str, job_id: str, record_id: str) -> Path: ...
        def _job_index_exists(self, job_id: str) -> bool: ...
        def _read_job_index(self, job_id: str) -> dict[str, object] | None: ...
        def _next_job_record_sequence_unlocked(self, job_id: str, count_field: str) -> int: ...
        def _write_ordered_job_record(
            self, family: str, job_id: str, sequence: int, record: BaseModel
        ) -> None: ...
        def _increment_job_index_unlocked(
            self, job_id: str, field: str, **updates: object
        ) -> None: ...
        def _read_ordered_job_page[Record: BaseModel](
            self,
            job_id: str,
            *,
            family: str,
            model: type[Record],
            cursor: int,
            limit: int,
            count_field: str,
        ) -> tuple[list[Record], int | None, int]: ...

    def append_progress(self, progress: ProgressRecord) -> ProgressRecord:
        """Record a structured job progress observation."""
        queue_layout.QueueLayout.require_durable_record_id(
            progress.progress_id, field="progress_id"
        )
        queue_layout.QueueLayout.require_durable_record_id(progress.job_id, field="job_id")
        self._store_adapter.initialize()
        with self._lock:
            self.get_job(progress.job_id)
            sequence = self._next_job_record_sequence_unlocked(progress.job_id, "progress_count")
            saved = progress.model_copy(update={"sequence": sequence})
            self._store_adapter.write(
                self._storage_root / "progress" / f"{saved.progress_id}.json", saved
            )
            self._store_adapter.write(
                self._job_record_path("progress_by_job", saved.job_id, saved.progress_id),
                saved,
            )
            self._write_ordered_job_record("progress", saved.job_id, sequence, saved)
            self._increment_job_index_unlocked(
                progress.job_id,
                "progress_count",
                latest_progress_id=saved.progress_id,
            )
            self.append_event(
                progress.job_id,
                "progress.updated",
                progress.message or f"Progress updated: {progress.label}",
                locked=True,
                payload={
                    "progress_id": progress.progress_id,
                    "label": progress.label,
                    "current": progress.current,
                    "total": progress.total,
                    "unit": progress.unit,
                    "message": progress.message,
                    "source_event_seq": progress.source_event_seq,
                },
            )
        return saved

    def list_progress(self, job_id: str) -> list[ProgressRecord]:
        """Return structured progress observations for a job."""
        job_id = queue_layout.QueueLayout.require_durable_record_id(job_id, field="job_id")
        self._store_adapter.initialize()
        if self._job_index_exists(job_id):
            return sorted(
                queue_store_read.read_many(
                    self._storage_root
                    / "progress_by_job"
                    / queue_layout.QueueLayout.durable_key(job_id),
                    ProgressRecord,
                    identity_field="progress_id",
                ),
                key=lambda progress: progress.created_at,
            )
        return sorted(
            [
                progress
                for progress in queue_store_read.read_many(
                    self._storage_root / "progress",
                    ProgressRecord,
                    identity_field="progress_id",
                )
                if progress.job_id == job_id
            ],
            key=lambda progress: progress.created_at,
        )

    def list_progress_page(
        self,
        job_id: str,
        *,
        cursor: int = 1,
        limit: int = 100,
    ) -> tuple[list[ProgressRecord], int | None, int]:
        """Read one stable progress page from the per-job sequence index."""
        job_id = queue_layout.QueueLayout.require_durable_record_id(job_id, field="job_id")
        return self._read_ordered_job_page(
            job_id,
            family="progress",
            model=ProgressRecord,
            cursor=cursor,
            limit=limit,
            count_field="progress_count",
        )

    def latest_job_progress(
        self,
        job_id: str,
    ) -> tuple[ProgressRecord | None, int, bool]:
        """Read exact latest progress and indexed count without scanning other jobs."""
        job_id = queue_layout.QueueLayout.require_durable_record_id(job_id, field="job_id")
        index = self._read_job_index(job_id)
        if index is not None:
            count = queue_index_state.index_integer(index, "progress_count")
            progress_id = index.get("latest_progress_id")
            if not isinstance(progress_id, str):
                return None, count, False
            progress = self._store_adapter.read_optional(
                self._job_record_path("progress_by_job", job_id, progress_id),
                ProgressRecord,
            )
            if progress is None:
                raise QueueConflictError(f"progress index points to a missing record: {job_id}")
            return progress, count, False
        progress, truncated = queue_store_read.scan_many(
            self._storage_root / "progress",
            ProgressRecord,
            limit=queue_layout.DEFAULT_EXACT_RECORD_LIMIT,
            identity_field="progress_id",
        )
        matched = [item for item in progress if item.job_id == job_id]
        latest = max(matched, key=lambda item: item.created_at, default=None)
        return latest, len(matched), truncated

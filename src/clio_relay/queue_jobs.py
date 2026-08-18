"""Canonical job-record ownership: submission, state, and writes.

Owns every public method that submits, reads, pages, scans, or transitions a
``RelayJob``, plus its unlocked write/derived-index primitives. Two typed
deviations: ``_is_sha256_digest`` keeps a private duplicate rather than
reaching into another owner's copy -- both ``queue_job_gc`` (CQ18) and
``queue_input_ingest`` (CQ13) have since landed, but per-owner duplication
of this six-line pure predicate is the resolved design (§13.3: six holders,
one consumer -- see ``queue_job_gc.py``'s module docstring for the full
census); ``write_job`` is the real, patchable ``_write_job_unlocked`` body,
kept as a thin wrapper so unmoved callers elsewhere keep working.
"""

from __future__ import annotations

import logging
import os
import stat
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, cast

from pydantic import BaseModel

from clio_relay import (
    queue_context,
    queue_idempotency,
    queue_index_state,
    queue_layout,
    queue_order_index,
    queue_owner_session_records,
    queue_scheduler_cancel_records,
    queue_store_read,
    queue_store_write,
)
from clio_relay.errors import QueueConflictError, queue_conflict_from_cause
from clio_relay.models import (
    TERMINAL_STATES,
    InputArtifactIngestPolicy,
    JobKind,
    JobState,
    JobTombstone,
    OwnerSessionJobMembership,
    RelayEvent,
    RelayJob,
    SchedulerCancelPending,
    UsedArtifactRef,
    prepare_owned_jarvis_run_submission,
    utc_now,
)
from clio_relay.pagination import MAX_RESPONSE_PAGE_RECORDS, validate_response_page_limit

logger = logging.getLogger(__name__)
_UNSET = queue_layout.UNSET


def _is_sha256_digest(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def write_job(queue: QueueJobsMixin, job: RelayJob) -> None:
    """Write a canonical job and replayable derived-index transition."""
    queue._migrate_execution_cleanup_shard_unlocked(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        job.cluster,
        queue._execution_cleanup_shard(job.job_id),  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        limit=queue_layout.DEFAULT_EXACT_RECORD_LIMIT + 1,
    )
    intent_path = queue._write_transition_intent_unlocked(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        "job_sync",
        job.job_id,
        {
            "job_id": job.job_id,
            "updated_at": job.updated_at.isoformat(),
        },
    )
    queue._store_adapter.write(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        queue._storage_root / "jobs" / f"{job.job_id}.json",  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        job,
    )
    queue._sync_job_derived_unlocked(job)  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    queue_store_write.unlink_durable_path(intent_path, missing_ok=True)


class QueueJobsMixin:
    """Own canonical job records: submission, state transitions, and writes."""

    _storage_root: Path
    _lock: queue_context.QueueLockProtocol
    _store_adapter: queue_context.QueueStoreProtocol

    if TYPE_CHECKING:

        def _recover_pending_transitions_unlocked(self) -> list[RelayJob]: ...
        def _migrate_execution_cleanup_shard_unlocked(
            self, cluster: str, shard: int, *, limit: int
        ) -> bool: ...
        @staticmethod
        def _execution_cleanup_shard(job_id: str) -> int: ...
        def _write_transition_intent_unlocked(
            self, kind: str, identity: str, payload: dict[str, object]
        ) -> Path: ...
        def _assert_owner_session_intake_open_unlocked(
            self, metadata: dict[str, object], *, require_active: bool = False
        ) -> None: ...
        def _assert_input_ingest_quota_unlocked(
            self, job: RelayJob, *, policy: InputArtifactIngestPolicy | None = None
        ) -> None: ...
        def _artifact_use_records_unlocked(
            self, job: RelayJob, *, allocate_sequences: bool
        ) -> list[UsedArtifactRef]: ...
        def _ensure_artifact_use_indexes_unlocked(self, job: RelayJob) -> None: ...
        def _ensure_scheduler_cancel_pending_unlocked(
            self, job: RelayJob, *, requested_at: datetime, reason: str
        ) -> SchedulerCancelPending: ...
        def _write_committed_idempotency_record(
            self, key_path: Path, job: RelayJob, job_digest: str
        ) -> None: ...
        def _replay_retired_job(
            self, submitted: RelayJob, idempotency_record: dict[str, object], *, job_digest: str
        ) -> RelayJob: ...
        def append_event(
            self,
            job_id: str,
            event_type: str,
            message: str,
            *,
            locked: bool = False,
            payload: dict[str, object] | None = None,
        ) -> RelayEvent: ...
        def _initialize_job_index_unlocked(self, job_id: str) -> None: ...
        def _job_submission_order_key_unlocked(
            self, job: RelayJob
        ) -> tuple[int, datetime, str]: ...
        def _read_global_order_page[RecordT: BaseModel](
            self,
            *,
            family: str,
            model: type[RecordT],
            identity_field: str,
            cursor: int,
            limit: int,
            predicate: Callable[[RecordT], bool] | None = None,
        ) -> tuple[list[RecordT], int | None, int]: ...
        def _update_job_index_unlocked(self, job_id: str, **updates: object) -> None: ...
        def _owner_session_membership_dir(
            self, owner_session_id: str, *, session_generation_id: str | None
        ) -> Path: ...
        def _sync_owner_session_job_membership_unlocked(self, job: RelayJob) -> None: ...
        def _sync_scheduler_source_unlocked(
            self, job_id: str, *, source_id: str, metadata: dict[str, object]
        ) -> None: ...

    def submit_job(self, job: RelayJob) -> RelayJob:
        """Submit a job, returning the existing record for a repeated idempotency key."""
        queue_layout.QueueLayout.require_durable_record_id(job.job_id, field="job_id")
        queue_owner_session_records._validate_new_owner_session_metadata(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            job.metadata
        )
        self._store_adapter.initialize()
        queue_index_state.require_index_migration_complete(self._storage_root)
        key_path = (
            self._storage_root
            / "idempotency"
            / f"{queue_idempotency._idempotency_key_filename(job.idempotency_key)}.json"  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        )
        with self._lock:
            self._recover_pending_transitions_unlocked()
            raw_existing: object | None = None
            if key_path.exists():
                raw_existing = self._store_adapter.read_json_document(key_path)
                if not isinstance(raw_existing, dict):
                    raise QueueConflictError(f"idempotency record is not an object: {key_path}")
                typed_existing = cast(dict[str, object], raw_existing)
                canonical_job_id = typed_existing.get("job_id")
                if (
                    not queue_layout.safe_global_record_id(canonical_job_id)
                    or typed_existing.get("idempotency_key") != job.idempotency_key
                    or typed_existing.get("state") not in {"reserved", "committed", "retired"}
                ):
                    raise QueueConflictError(
                        f"invalid idempotency record for key: {job.idempotency_key}"
                    )
                job = job.model_copy(update={"job_id": cast(str, canonical_job_id)})
            job = prepare_owned_jarvis_run_submission(job)
            job_digest = queue_idempotency._job_idempotency_digest(job)  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            if job.submission_digest not in {None, job_digest}:
                raise QueueConflictError("submitted job carries a mismatched submission_digest")
            job = job.model_copy(update={"submission_digest": job_digest})
            if raw_existing is None:
                self._artifact_use_records_unlocked(job, allocate_sequences=False)
            else:
                assert isinstance(raw_existing, dict)
                existing = cast(dict[str, object], raw_existing)
                existing_job_id = existing.get("job_id")
                existing_digest = existing.get("job_digest")
                existing_state = existing.get("state")
                if (
                    not queue_layout.safe_global_record_id(existing_job_id)
                    or existing.get("idempotency_key") != job.idempotency_key
                    or existing_state not in {"reserved", "committed", "retired"}
                ):
                    raise QueueConflictError(
                        f"invalid idempotency record for key: {job.idempotency_key}"
                    )
                if existing_digest is None and existing_state == "reserved":
                    existing["job_digest"] = job_digest
                    existing_digest = job_digest
                    self._store_adapter.write_json(key_path, existing)
                elif not _is_sha256_digest(existing_digest):
                    raise QueueConflictError(
                        f"idempotency key was reused with a different job payload: "
                        f"{job.idempotency_key}"
                    )
                if existing_digest != job_digest:
                    raise QueueConflictError(
                        f"idempotency key was reused with a different job payload: "
                        f"{job.idempotency_key}"
                    )
                existing_job_id = cast(str, existing_job_id)
                if existing_state == "retired":
                    return self._replay_retired_job(job, existing, job_digest=job_digest)
                existing_job = self._store_adapter.read_optional(
                    self._storage_root / "jobs" / f"{existing_job_id}.json",
                    RelayJob,
                )
                if existing_job is not None:
                    if existing_job.idempotency_key != job.idempotency_key or (
                        existing_job.submission_digest is not None
                        and existing_job.submission_digest != job_digest
                    ):
                        raise QueueConflictError(
                            f"idempotency target identity mismatch: {existing_job_id}"
                        )
                    queue_order_index.ensure_global(
                        self._store_adapter, "jobs", existing_job.job_id
                    )
                    self._initialize_job_index_unlocked(existing_job.job_id)
                    self._ensure_artifact_use_indexes_unlocked(existing_job)
                    self._write_job_unlocked(existing_job)
                    existing_request = (
                        queue_scheduler_cancel_records.scheduler_cancellation_request(existing_job)
                    )
                    if (
                        existing_request is not None
                        and existing_request.get("cancel_scheduler") is True
                    ):
                        self._ensure_scheduler_cancel_pending_unlocked(
                            existing_job,
                            requested_at=(
                                queue_scheduler_cancel_records.cancellation_requested_at(
                                    existing_request
                                )
                                or existing_job.updated_at
                            ),
                            reason="operator_request",
                        )
                    self._ensure_job_queued_event(existing_job)
                    if existing_state == "reserved":
                        self._write_committed_idempotency_record(key_path, existing_job, job_digest)
                    return existing_job
                if existing_state != "reserved":
                    raise QueueConflictError(
                        f"idempotency key points to missing job: {job.idempotency_key}"
                    )
                self._assert_owner_session_intake_open_unlocked(job.metadata)
                self._assert_input_ingest_quota_unlocked(job)
                self._ensure_active_job_capacity_unlocked(job)
                job = job.model_copy(update={"job_id": existing_job_id})
            if raw_existing is None:
                self._assert_owner_session_intake_open_unlocked(job.metadata)
                self._assert_input_ingest_quota_unlocked(job)
                self._ensure_active_job_capacity_unlocked(job)
                self._store_adapter.write_json(
                    key_path,
                    {
                        "state": "reserved",
                        "job_id": job.job_id,
                        "idempotency_key": job.idempotency_key,
                        "job_digest": job_digest,
                        "created_at": utc_now().isoformat(),
                    },
                )
            queue_order_index.ensure_global(self._store_adapter, "jobs", job.job_id)
            self._initialize_job_index_unlocked(job.job_id)
            self._ensure_artifact_use_indexes_unlocked(job)
            self._write_job_unlocked(job)
            self._store_adapter.write_json(
                key_path,
                queue_idempotency._committed_idempotency_record(job, job_digest),  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            )
            self.append_event(job.job_id, "job.queued", "Job queued", locked=True)
        return job

    def get_job(self, job_id: str) -> RelayJob:
        """Return a job by id."""
        return queue_store_read.read_required_job(self._storage_root, job_id)

    def get_job_tombstone(self, job_id: str) -> JobTombstone | None:
        """Return the durable terminal tombstone for a retired job, if present."""
        job_id = queue_layout.QueueLayout.require_durable_record_id(job_id, field="job_id")
        self._store_adapter.initialize()
        tombstone = self._store_adapter.read_optional(
            self._storage_root
            / "job_tombstones"
            / f"{queue_layout.QueueLayout.durable_key(job_id)}.json",
            JobTombstone,
        )
        if tombstone is not None and tombstone.job_id != job_id:
            raise QueueConflictError(f"canonical job tombstone identity mismatch: {job_id}")
        return tombstone

    def list_jobs(self) -> list[RelayJob]:
        """Return all jobs in durable submission order."""
        self._store_adapter.initialize()
        jobs = list(
            queue_store_read.read_many(
                self._storage_root / "jobs",
                RelayJob,
                identity_field="job_id",
            )
        )
        with self._lock:
            return sorted(jobs, key=self._job_submission_order_key_unlocked)

    def list_jobs_page(
        self,
        *,
        cursor: int = 1,
        limit: int = 100,
        cluster: str | None = None,
        state: JobState | None = None,
        kind: JobKind | None = None,
        include_terminal: bool = True,
    ) -> tuple[list[RelayJob], int | None, int]:
        """Read one global job source window with optional in-window filters.

        ``total`` is the durable submission-sequence high-water mark. Retired jobs and
        crash-reserved gaps remain sequence positions, so a page can contain fewer than
        ``limit`` records while still returning a ``next_cursor``.
        """

        def matches(job: RelayJob) -> bool:
            return (
                (cluster is None or job.cluster == cluster)
                and (state is None or job.state == state)
                and (kind is None or job.kind == kind)
                and (include_terminal or job.state not in TERMINAL_STATES)
            )

        return self._read_global_order_page(
            family="jobs",
            model=RelayJob,
            identity_field="job_id",
            cursor=cursor,
            limit=limit,
            predicate=matches,
        )

    def scan_jobs(self, *, limit: int) -> tuple[list[RelayJob], bool]:
        """Read a bounded global submission window and report whether more exists."""
        if limit < 1:
            raise ValueError("job scan limit must be at least 1")
        cursor = 1
        remaining_source_positions = limit
        jobs: list[RelayJob] = []
        next_cursor = cast(int | None, cursor)
        while remaining_source_positions > 0 and next_cursor is not None:
            page_limit = min(MAX_RESPONSE_PAGE_RECORDS, remaining_source_positions)
            page, next_cursor, _total = self.list_jobs_page(
                cursor=cursor,
                limit=page_limit,
            )
            jobs.extend(page)
            remaining_source_positions -= page_limit
            if next_cursor is not None:
                cursor = next_cursor
        return jobs, next_cursor is not None

    def scan_active_jobs(self, *, limit: int) -> tuple[list[RelayJob], bool]:
        """Read bounded active jobs without touching terminal history."""
        self._store_adapter.initialize()
        with self._lock:
            self._recover_pending_transitions_unlocked()
            self._repair_active_job_index_unlocked()
            indexed_jobs, truncated = queue_store_read.scan_many(
                self._storage_root / "jobs_active",
                RelayJob,
                limit=limit,
            )
            jobs = [self.get_job(indexed.job_id) for indexed in indexed_jobs]
            return sorted(jobs, key=self._job_submission_order_key_unlocked), truncated

    def active_job_capacity(self) -> dict[str, int | bool]:
        """Return explicit active-job admission capacity and current occupancy."""
        self._store_adapter.initialize()
        with self._lock:
            self._recover_pending_transitions_unlocked()
            count, over_capacity = self._store_adapter.bounded_regular_json_count(
                self._storage_root / "jobs_active",
                limit=queue_layout.MAX_ACTIVE_JOB_RECORDS,
                label="active job index",
            )
            try:
                self._repair_active_job_index_unlocked()
            except (QueueConflictError, ValueError):
                pass
            else:
                count, over_capacity = self._store_adapter.bounded_regular_json_count(
                    self._storage_root / "jobs_active",
                    limit=queue_layout.MAX_ACTIVE_JOB_RECORDS,
                    label="active job index",
                )
        return {
            "limit": queue_layout.MAX_ACTIVE_JOB_RECORDS,
            "used": count,
            "remaining": max(0, queue_layout.MAX_ACTIVE_JOB_RECORDS - count),
            "over_capacity": over_capacity,
        }

    def list_owner_session_jobs_page(
        self,
        owner_session_id: str,
        *,
        session_generation_id: str | None,
        cursor: str | None = None,
        limit: int = 500,
        cluster: str | None = None,
        include_terminal: bool = False,
    ) -> tuple[list[RelayJob], str | None, int, int]:
        """Read one generation-scoped membership window without global job history."""
        if not owner_session_id:
            raise ValueError("owner_session_id must not be empty")
        if session_generation_id is not None:
            session_generation_id = queue_layout.QueueLayout.require_durable_record_id(
                session_generation_id,
                field="session_generation_id",
            )
        limit = validate_response_page_limit(limit)
        self._store_adapter.initialize()
        queue_index_state.require_index_migration_complete(self._storage_root)
        directory = self._owner_session_membership_dir(
            owner_session_id,
            session_generation_id=session_generation_id,
        )
        count, over_capacity = self._store_adapter.bounded_regular_json_count(
            directory,
            limit=queue_layout.MAX_ACTIVE_JOB_RECORDS,
            label="owner-session job membership",
        )
        if over_capacity:
            raise QueueConflictError("owner-session job membership exceeds its supported capacity")
        all_names = sorted(path.name for path in directory.glob("*.json") if path.is_file())
        if len(all_names) != count:
            raise QueueConflictError("owner-session job membership changed during paging")
        source_total = len(all_names)
        names = all_names
        if cursor is not None:
            if not cursor.endswith(".json") or Path(cursor).name != cursor:
                raise ValueError("owner-session membership cursor is invalid")
            names = [name for name in names if name > cursor]
        window = names[:limit]
        next_cursor = window[-1] if len(names) > len(window) and window else None
        jobs: list[RelayJob] = []
        for name in window:
            membership = queue_store_read.read_json_file(
                directory / name, OwnerSessionJobMembership
            )
            if (
                membership.owner_session_id != owner_session_id
                or membership.session_generation_id != session_generation_id
            ):
                raise QueueConflictError(
                    f"owner-session membership identity mismatch: {directory / name}"
                )
            job = self.get_job(membership.job_id)
            if job.metadata.get("owner_session_id") != owner_session_id or (
                job.metadata.get("owner_session_generation_id") != session_generation_id
            ):
                raise QueueConflictError(
                    f"owner-session membership target mismatch: {membership.job_id}"
                )
            if cluster is not None and job.cluster != cluster:
                continue
            if not include_terminal and job.state in TERMINAL_STATES:
                continue
            jobs.append(job)
        return jobs, next_cursor, source_total, len(window)

    def update_job_metadata(
        self,
        job_id: str,
        metadata: dict[str, object],
    ) -> RelayJob:
        """Merge durable execution metadata without changing job state."""
        job_id = queue_layout.QueueLayout.require_durable_record_id(job_id, field="job_id")
        self._store_adapter.initialize()
        with self._lock:
            self._recover_pending_transitions_unlocked()
            job = self.get_job(job_id)
            updated_metadata = dict(job.metadata)
            updated_metadata.update(metadata)
            if job.metadata.get("owner_session_id") is None:
                queue_owner_session_records._validate_new_owner_session_metadata(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
                    updated_metadata
                )
            updated = job.model_copy(update={"updated_at": utc_now(), "metadata": updated_metadata})
            self._write_job_unlocked(updated)
            return updated

    def update_job_state(
        self,
        job_id: str,
        state: JobState,
        *,
        message: str | None = None,
        error: str | None = None,
        leased_by: str | None | object = _UNSET,
    ) -> RelayJob:
        """Update a job state and append a state event."""
        job_id = queue_layout.QueueLayout.require_durable_record_id(job_id, field="job_id")
        if isinstance(leased_by, str):
            queue_layout.QueueLayout.require_durable_record_id(leased_by, field="leased_by")
        self._store_adapter.initialize()
        with self._lock:
            self._recover_pending_transitions_unlocked()
            job = self.get_job(job_id)
            if job.state in TERMINAL_STATES and state != job.state:
                raise QueueConflictError(
                    f"cannot change terminal job {job_id} from {job.state} to {state}"
                )
            updates: dict[str, object] = {
                "state": state,
                "updated_at": utc_now(),
                "last_error": error,
            }
            if leased_by is not _UNSET:
                updates["leased_by"] = leased_by
            job = job.model_copy(update=updates)
            self._write_job_unlocked(job)
            self.append_event(
                job_id,
                f"job.{state.value}",
                message or f"Job {state.value}",
                locked=True,
                payload={"state": state.value, "error": error},
            )
        return job

    def cancel_job_if_active(
        self,
        job_id: str,
        *,
        cancel_scheduler: bool,
        expected_state: JobState | None = None,
        expected_updated_at: datetime | None = None,
    ) -> tuple[RelayJob, bool]:
        """Atomically cancel an active job if its optional snapshot still matches.

        The cancellation request, event, and terminal transition share one queue
        lock. A worker completion that wins the lock remains terminal, while a
        stale cleanup plan cannot cancel a job that was leased or otherwise
        updated after discovery.
        """
        job_id = queue_layout.QueueLayout.require_durable_record_id(job_id, field="job_id")
        self._store_adapter.initialize()
        with self._lock:
            self._recover_pending_transitions_unlocked()
            job = self.get_job(job_id)
            if job.state in TERMINAL_STATES:
                return job, False
            if expected_state is not None and job.state is not expected_state:
                return job, False
            if expected_updated_at is not None and job.updated_at != expected_updated_at:
                return job, False
            requested_at = utc_now()
            metadata = dict(job.metadata)
            metadata["cancellation_request"] = {
                "schema_version": "clio-relay.cancellation-request.v1",
                "requested_at": requested_at.isoformat(),
                "previous_state": job.state.value,
                "cancel_scheduler": cancel_scheduler,
            }
            cancellation_requested = job.model_copy(
                update={
                    "updated_at": requested_at,
                    "metadata": metadata,
                }
            )
            if cancel_scheduler:
                self._ensure_scheduler_cancel_pending_unlocked(
                    cancellation_requested,
                    requested_at=requested_at,
                    reason="operator_request",
                )
            if job.state is JobState.QUEUED:
                queued_request = dict(metadata["cancellation_request"])
                queued_request["acknowledged_at"] = requested_at.isoformat()
                queued_request["cleanup_acknowledged"] = True
                metadata["cancellation_request"] = queued_request
                cancellation_requested = cancellation_requested.model_copy(
                    update={
                        "state": JobState.CANCELED,
                        "leased_by": None,
                        "last_error": None,
                        "metadata": metadata,
                    }
                )
            self._write_job_unlocked(cancellation_requested)
            self.append_event(
                job_id,
                "job.cancel_requested",
                "Cancellation requested",
                locked=True,
                payload={
                    "previous_state": job.state.value,
                    "cancel_scheduler": cancel_scheduler,
                },
            )
            if cancellation_requested.state is JobState.CANCELED:
                self.append_event(
                    job_id,
                    "job.canceled",
                    "Job canceled",
                    locked=True,
                    payload={
                        "state": JobState.CANCELED.value,
                        "error": None,
                        "cleanup_acknowledged": True,
                    },
                )
            return cancellation_requested, True

    def acknowledge_job_cancellation(self, job_id: str) -> RelayJob:
        """Terminalize a requested cancellation after worker cleanup succeeds."""
        job_id = queue_layout.QueueLayout.require_durable_record_id(job_id, field="job_id")
        self._store_adapter.initialize()
        with self._lock:
            self._recover_pending_transitions_unlocked()
            job = self.get_job(job_id)
            if job.state is JobState.CANCELED:
                return job
            if job.state in TERMINAL_STATES:
                raise QueueConflictError(
                    f"cannot acknowledge cancellation for terminal job {job_id}: {job.state}"
                )
            request = job.metadata.get("cancellation_request")
            if not isinstance(request, dict):
                raise QueueConflictError(f"job {job_id} has no durable cancellation request")
            acknowledged_at = utc_now()
            metadata = dict(job.metadata)
            typed_request = dict(cast(dict[str, object], request))
            typed_request["acknowledged_at"] = acknowledged_at.isoformat()
            typed_request["cleanup_acknowledged"] = True
            metadata["cancellation_request"] = typed_request
            canceled = job.model_copy(
                update={
                    "state": JobState.CANCELED,
                    "leased_by": None,
                    "updated_at": acknowledged_at,
                    "last_error": None,
                    "metadata": metadata,
                }
            )
            self._write_job_unlocked(canceled)
            self.append_event(
                job_id,
                "job.canceled",
                "Job cancellation cleanup acknowledged",
                locked=True,
                payload={
                    "state": JobState.CANCELED.value,
                    "error": None,
                    "cleanup_acknowledged": True,
                },
            )
            return canceled

    def _ensure_job_queued_event(self, job: RelayJob) -> None:
        event_dir = self._storage_root / "events" / job.job_id
        if (event_dir / f"{1:020d}.json").is_file():
            return
        self._update_job_index_unlocked(job.job_id, latest_event_seq=0)
        self.append_event(job.job_id, "job.queued", "Job queued", locked=True)

    def _ensure_active_job_capacity_unlocked(self, job: RelayJob) -> None:
        """Reject a new active record before it can exceed the serviceable bound."""
        if job.state is not JobState.QUEUED:
            return
        directory = self._storage_root / "jobs_active"
        initial_count, _initial_over_capacity = self._store_adapter.bounded_regular_json_count(
            directory,
            limit=queue_layout.MAX_ACTIVE_JOB_RECORDS,
            label="active job index",
        )
        try:
            self._repair_active_job_index_unlocked()
        except (QueueConflictError, ValueError) as exc:
            if initial_count >= queue_layout.MAX_ACTIVE_JOB_RECORDS:
                raise QueueConflictError(
                    "active_job_capacity_reached: active job capacity "
                    f"{queue_layout.MAX_ACTIVE_JOB_RECORDS} reached and the index could not be "
                    "safely reconciled"
                ) from exc
            raise
        count = 0
        try:
            with os.scandir(directory) as entries:
                for entry in entries:
                    if not entry.name.endswith(".json"):
                        raise QueueConflictError(
                            f"active job index contains an unsafe record: {entry.path}"
                        )
                    entry_stat = entry.stat(follow_symlinks=False)
                    if not stat.S_ISREG(entry_stat.st_mode) or queue_layout.record_is_reparse(
                        entry_stat
                    ):
                        raise QueueConflictError(
                            f"active job index contains an unsafe record: {entry.path}"
                        )
                    count += 1
                    if count >= queue_layout.MAX_ACTIVE_JOB_RECORDS:
                        raise QueueConflictError(
                            "active_job_capacity_reached: active job capacity "
                            f"{queue_layout.MAX_ACTIVE_JOB_RECORDS} reached; cancel or drain "
                            "active work before submitting another job"
                        )
        except OSError as exc:
            raise queue_conflict_from_cause(
                "cannot inspect active job capacity",
                cause=exc,
                logger=logger,
            ) from exc

    def _write_job_unlocked(self, job: RelayJob) -> None:
        """Write a canonical job and replayable derived-index transition."""
        write_job(self, job)

    def _sync_job_derived_unlocked(self, job: RelayJob) -> None:
        """Converge every mutable job index from one canonical job record."""
        self._sync_owner_session_job_membership_unlocked(job)
        self._sync_scheduler_source_unlocked(
            job.job_id,
            source_id="job",
            metadata=job.metadata,
        )
        active_path = self._storage_root / "jobs_active" / f"{job.job_id}.json"
        queued_path = self._storage_root / "jobs_queued" / f"{job.job_id}.json"
        if job.state in TERMINAL_STATES:
            queue_store_write.unlink_durable_path(active_path, missing_ok=True)
            queue_store_write.unlink_durable_path(queued_path, missing_ok=True)
            return
        self._store_adapter.write(active_path, job)
        if job.state is JobState.QUEUED:
            self._store_adapter.write(queued_path, job)
        else:
            queue_store_write.unlink_durable_path(queued_path, missing_ok=True)

    def _repair_active_job_index_unlocked(self) -> None:
        """Remove stale capacity entries and refresh every indexed active job."""
        paths = queue_store_read.bounded_json_record_paths(
            self._storage_root / "jobs_active",
            limit=queue_layout.MAX_ACTIVE_JOB_RECORDS,
            label="active job index",
        )
        for path in paths:
            indexed = queue_store_read.read_json_file(path, RelayJob)
            canonical = self._store_adapter.read_optional(
                self._storage_root / "jobs" / f"{indexed.job_id}.json",
                RelayJob,
            )
            if canonical is None or canonical.state in TERMINAL_STATES:
                queue_store_write.unlink_durable_path(path, missing_ok=True)
                queue_store_write.unlink_durable_path(
                    self._storage_root / "jobs_queued" / f"{indexed.job_id}.json",
                    missing_ok=True,
                )
                continue
            self._store_adapter.write(path, canonical)
            queued_path = self._storage_root / "jobs_queued" / f"{canonical.job_id}.json"
            if canonical.state is JobState.QUEUED:
                self._store_adapter.write(queued_path, canonical)
            else:
                queue_store_write.unlink_durable_path(queued_path, missing_ok=True)

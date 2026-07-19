"""Durable queue/state boundary used as the relay's clio-core adapter.

The implementation in this repository is intentionally a filesystem-backed
record store so it can run everywhere during development. The public class is
named around the clio-core contract: callers depend on record families,
idempotency, leases, and cursor replay rather than a database choice.
"""

from __future__ import annotations

import errno
import hashlib
import heapq
import json
import os
import stat
import threading
import time
from collections.abc import Callable, Iterable
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import TracebackType
from typing import Any, Literal, TypeVar, cast
from uuid import uuid4

from filelock import FileLock, Timeout
from pydantic import BaseModel

from clio_relay.browser_gateway import BrowserAttachmentRecord
from clio_relay.cluster_config import (
    ensure_private_configuration_directory,
    ensure_private_configuration_path,
    open_private_atomic_file,
)
from clio_relay.command_evidence import bounded_error_detail
from clio_relay.errors import ConfigurationError, NotFoundError, QueueConflictError
from clio_relay.filesystem_paths import internal_filesystem_path, logical_filesystem_path
from clio_relay.identifiers import filesystem_key, validate_durable_record_id
from clio_relay.models import (
    TERMINAL_STATES,
    ArtifactRef,
    ArtifactUserOrderHead,
    Cursor,
    EndpointRegistration,
    GatewaySession,
    GatewaySessionState,
    JobGcPhase,
    JobKind,
    JobState,
    JobTombstone,
    Lease,
    McpAdmissionClass,
    McpCallSpec,
    MonitorRule,
    OwnerSessionClosure,
    OwnerSessionJobMembership,
    ProgressRecord,
    RelayEvent,
    RelayJob,
    RelayTask,
    SchedulerCancelDisposition,
    SchedulerCancelDispositionState,
    SchedulerCancelPending,
    SchedulerPhase,
    TaskTimelineEvent,
    TerminalJobGcPlan,
    TerminalJobGcResult,
    UsedArtifactRef,
    is_owned_jarvis_run_spec,
    prepare_owned_jarvis_run_submission,
    utc_now,
)
from clio_relay.pagination import (
    MAX_RESPONSE_PAGE_RECORDS,
    validate_gc_batch_size,
    validate_record_cursor,
    validate_response_page_limit,
)
from clio_relay.worker_concurrency import KindConcurrencyInput, normalize_kind_concurrency
from clio_relay.worker_lifetime_lock import (
    LockedCoreIdentity,
    exclusive_migration_lifetime,
    require_active_locked_core,
)

Record = TypeVar("Record", bound=BaseModel)
_LeaseExpiryReference = tuple[int, str, JobKind, str, str, str, str]
_UNSET = object()
# Queue mutations can legitimately serialize behind several durable writes on
# slower filesystems (notably Windows endpoint storage).  Keep the wait bounded,
# but allow enough time for a healthy owner to finish its critical section.
DEFAULT_CORE_LOCK_TIMEOUT_SECONDS = 30.0
MIN_SCHEDULER_CANCEL_CLAIM_LEASE_SECONDS = 1.0
MAX_SCHEDULER_CANCEL_CLAIM_LEASE_SECONDS = 300.0
ATOMIC_REPLACE_ATTEMPTS = 25
ATOMIC_REPLACE_RETRY_SECONDS = 0.02
WRITE_STAGING_FAMILY = "write_staging"
WRITE_STAGING_MAX_LEFTOVERS = 10_000
OWNER_SESSION_CLOSURE_WRITE_ATTEMPTS = 3
JOB_INDEX_SCHEMA = "clio-relay.job-index.v1"
INDEX_MIGRATION_SCHEMA = "clio-relay.index-migration.v1"
LEASE_OPERATIONAL_INDEX_SCHEMA = "clio-relay.lease-operational-index.v2"
LEASE_CAPACITY_AGGREGATE_SCHEMA = "clio-relay.lease-capacity-aggregate.v1"
LEASE_CAPACITY_CHECKPOINT_SCHEMA = "clio-relay.lease-capacity-checkpoint.v1"
LEASE_CAPACITY_AUDIT_SCHEMA = "clio-relay.lease-capacity-audit.v1"
DEFAULT_EXACT_RECORD_LIMIT = 1_000
MAX_ACTIVE_JOB_RECORDS = 10_000
MAX_LIVE_LEASE_RECORDS = MAX_ACTIVE_JOB_RECORDS
MAX_LEASE_CAPACITY_SCOPES = MAX_LIVE_LEASE_RECORDS
MAX_LEASE_CAPACITY_RECORD_BYTES = 4 * 1_048_576
MAX_BOUNDED_SCAN_RECORDS = 10_000
MAX_GATEWAY_INDEX_RECORDS = 10_000
MAX_SCHEDULER_METADATA_RECORDS = 1_000
MAX_TRANSITION_INTENT_RECORDS = 10_000
MAX_ARTIFACT_USES_PER_JOB = 1_000
MAX_ARTIFACT_CONSUMERS = 10_000
ARTIFACT_USER_CURSOR_PREFIX = "edge_"
ARTIFACT_USER_CURSOR_DIGITS = 20
ENDPOINT_FRESH_BUCKET_SECONDS = 60
MAX_ENDPOINT_FRESH_SECONDS = 3_600
MAX_ENDPOINT_FRESH_CLUSTER_ROOTS = 1_000
ORDER_INDEX_SCHEMA = "clio-relay.job-record-order.v1"
RETENTION_INDEX_SCHEMA = "clio-relay.job-retention-index.v1"
GLOBAL_ORDER_INDEX_SCHEMA = "clio-relay.global-record-order.v1"
GC_TRASH_SCHEMA = "clio-relay.gc-trash.v1"
MAX_GC_PURGE_DEPTH = 4_096
MAX_GC_PURGE_SCAN_ENTRIES = 10_000
DEFAULT_RECORD_MAX_BYTES = 1_048_576
LEGACY_OUTPUT_MIGRATION_SCHEMA = "clio-relay.legacy-output-migration.v1"
LEGACY_OUTPUT_COMPATIBILITY_SCHEMA = "clio-relay.legacy-output-compatibility.v1"
LEGACY_OUTPUT_RECEIPT_SCHEMA = "clio-relay.legacy-output-receipt.v1"
# v0.9 wrote one complete subprocess callback twice in every output event: once
# as ``message`` and once as ``payload.text``.  Keep the compatibility reader
# bounded independently from the ordinary event limit.  New events remain
# limited to 256 KiB and endpoint output is split into 64-KiB chunks.
MAX_LEGACY_OUTPUT_RECORD_BYTES = 16 * 1_048_576
MAX_LEGACY_OUTPUT_MIGRATION_BYTES = 256 * 1_048_576
MAX_LEGACY_OUTPUT_MIGRATION_RECORDS = 10_000
MAX_LEGACY_EVENT_AUDIT_DIRECTORIES = 100_000
MAX_LEGACY_EVENT_AUDIT_RECORDS = 1_000_000
RECORD_FAMILY_MAX_BYTES: dict[str, int] = {
    "active_gateway_refs_by_job": 1_048_576,
    "active_gateway_refs_by_session": 65_536,
    "active_monitor_rules_by_job": 262_144,
    "active_tasks_by_job": 1_048_576,
    "artifacts": 262_144,
    "artifacts_by_job": 262_144,
    "artifact_user_order": 262_144,
    "artifact_users": 262_144,
    "artifact_order_by_job": 262_144,
    "endpoints_fresh": 65_536,
    "endpoints_fresh_by_id": 65_536,
    "events": 262_144,
    "gc_runs": 65_536,
    "gateway_sessions": 1_048_576,
    "global_order": 65_536,
    "gateway_reverse_refs_by_session": 65_536,
    "gateways_by_artifact": 1_048_576,
    "gateways_by_scheduler": 1_048_576,
    "idempotency": 65_536,
    "job_indexes": 65_536,
    "job_tombstones": 65_536,
    "jobs": 1_048_576,
    "jobs_active": 1_048_576,
    "jobs_queued": 1_048_576,
    "leases": 65_536,
    "legacy_output_archives": MAX_LEGACY_OUTPUT_RECORD_BYTES,
    "legacy_output_receipts": 65_536,
    "legacy_output_retired": 65_536,
    "leases_by_job": 65_536,
    "lease_indexes": 65_536,
    "lease_capacity": MAX_LEASE_CAPACITY_RECORD_BYTES,
    "migrations": 262_144,
    "monitor_rules": 262_144,
    "monitor_rules_by_job": 262_144,
    "owner_sessions": 65_536,
    "owner_session_jobs": 65_536,
    "owner_session_legacy_jobs": 65_536,
    "progress": 262_144,
    "progress_by_job": 262_144,
    "progress_order_by_job": 262_144,
    "scheduler_jobs": 65_536,
    "scheduler_cancel_pending": 262_144,
    "scheduler_cancel_dispositions": 262_144,
    "scheduler_protections_by_job": 65_536,
    "scheduler_refs_by_job": 65_536,
    "task_event_heads": 65_536,
    "task_events": 262_144,
    "tasks": 1_048_576,
    "tasks_by_job": 1_048_576,
    "task_order_by_job": 1_048_576,
    "transition_intents": 16_777_216,
    "used_artifacts_by_job": 262_144,
}


class _TransientRecordReplacement(RuntimeError):
    """Signal that an atomic replacement invalidated one bounded read attempt."""


class LegacyQueueStateError(QueueConflictError):
    """Machine-readable refusal for unsafe pre-1.0 canonical queue state."""

    def __init__(
        self,
        *,
        family: str,
        path: Path,
        reason: str,
        action: str | None = None,
    ) -> None:
        self.report: dict[str, str] = {
            "schema_version": "clio-relay.legacy-state-audit.v1",
            "family": family,
            "path": str(logical_filesystem_path(path)),
            "reason": reason,
            "action": action
            or (
                "move the unsafe state aside or export records with portable durable IDs "
                "before retrying"
            ),
        }
        super().__init__(json.dumps(self.report, sort_keys=True))


_ORDER_FAMILIES = ("tasks", "artifacts", "progress")
_GLOBAL_ORDER_FAMILIES = (
    "endpoints",
    "jobs",
    "gateway_sessions",
    "monitor_rules",
)
_RETENTION_INDEX_FAMILIES = (
    "jobs",
    "tasks",
    "artifacts",
    "monitor_rules",
    "gateway_sessions",
)
_OPERATIONAL_INDEX_FAMILIES = (
    "endpoints",
    "jobs",
    "gateway_sessions",
    "leases",
)
_GC_TERMINAL_SCHEDULER_PHASES = {
    SchedulerPhase.COMPLETED.value,
    SchedulerPhase.FAILED.value,
    SchedulerPhase.CANCELED.value,
}


class _FairBoundedFileLock:
    """Serialize local waiters fairly before taking one cross-process file lock.

    ``filelock`` retries a busy Windows lock by polling.  Several hot threads in
    one relay process can repeatedly acquire the short-lived filesystem lock in
    those polling gaps and starve an older local waiter until its bounded
    timeout expires.  Ticket admission prevents local overtaking while the
    underlying ``FileLock`` preserves cross-process exclusion.  Both waits
    share one deadline so lock failure remains explicit and bounded.
    """

    def __init__(self, lock_file: str, *, timeout: float) -> None:
        self.lock_file = lock_file
        self.timeout = timeout
        self._file_lock = FileLock(lock_file, timeout=timeout)
        self._condition = threading.Condition()
        self._owner_thread_id: int | None = None
        self._owner_depth = 0
        self._next_ticket = 0
        self._serving_ticket = 0
        self._abandoned_tickets: set[int] = set()

    def __enter__(self) -> _FairBoundedFileLock:
        self.acquire()
        return self

    @property
    def is_locked(self) -> bool:
        """Report this thread's underlying ``FileLock`` ownership state."""
        return self._file_lock.is_locked

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        del exc_type, exc_value, traceback
        self.release()

    def acquire(self, *, timeout: float | None = None) -> None:
        """Acquire local admission and the filesystem lock within one deadline."""
        bounded_timeout = self.timeout if timeout is None else timeout
        if bounded_timeout < 0:
            raise ValueError("lock timeout must be non-negative")
        deadline = time.monotonic() + bounded_timeout
        thread_id = threading.get_ident()
        with self._condition:
            if self._owner_thread_id == thread_id:
                self._owner_depth += 1
                return
            ticket = self._next_ticket
            self._next_ticket += 1
            while self._owner_thread_id is not None or ticket != self._serving_ticket:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    self._abandoned_tickets.add(ticket)
                    self._skip_abandoned_tickets_locked()
                    self._condition.notify_all()
                    raise Timeout(self.lock_file)
                self._condition.wait(timeout=remaining)
            self._owner_thread_id = thread_id
            self._owner_depth = 1

        try:
            remaining = max(0.0, deadline - time.monotonic())
            self._file_lock.acquire(timeout=remaining)
        except BaseException:
            with self._condition:
                self._owner_thread_id = None
                self._owner_depth = 0
                self._serving_ticket += 1
                self._skip_abandoned_tickets_locked()
                self._condition.notify_all()
            raise

    def release(self) -> None:
        """Release one reentrant level and admit the next local ticket."""
        thread_id = threading.get_ident()
        with self._condition:
            if self._owner_thread_id != thread_id or self._owner_depth == 0:
                raise RuntimeError("core queue lock released by a non-owner thread")
            if self._owner_depth > 1:
                self._owner_depth -= 1
                return
            self._file_lock.release()
            self._owner_thread_id = None
            self._owner_depth = 0
            self._serving_ticket += 1
            self._skip_abandoned_tickets_locked()
            self._condition.notify_all()

    def _skip_abandoned_tickets_locked(self) -> None:
        while self._serving_ticket in self._abandoned_tickets:
            self._abandoned_tickets.remove(self._serving_ticket)
            self._serving_ticket += 1


@dataclass(frozen=True, slots=True)
class IdempotentSubmissionResolution:
    """Read-only canonical identity resolution for storage admission."""

    state: Literal["new", "reserved", "existing", "retired"]
    canonical_job_id: str
    existing_job: RelayJob | None = None


@dataclass(frozen=True, slots=True)
class SchedulerCancelIdentityRegistration:
    """Atomic result for one durable scheduler-cancellation identity registration."""

    record: SchedulerCancelPending
    disposition_created: bool


@dataclass(frozen=True, slots=True)
class SchedulerCancelAttemptClaim:
    """Exclusive durable lease for one external scheduler cancellation attempt."""

    claim_id: str
    scheduler_job_id: str
    provider: str
    attempt: int
    claimed_at: datetime
    expires_at: datetime


@dataclass(frozen=True, slots=True)
class SchedulerCancelConfirmationClaim:
    """Exclusive durable lease for one scheduler cancellation confirmation poll."""

    claim_id: str
    scheduler_job_id: str
    provider: str
    confirmation_attempt: int
    claimed_at: datetime
    expires_at: datetime


@dataclass(frozen=True, slots=True)
class _LeaseIndexIdentity:
    """Exact immutable fields used by every operational lease reference."""

    lease_id: str
    job_id: str
    endpoint_id: str
    cluster: str
    job_kind: JobKind
    expires_at: datetime


@dataclass(frozen=True, slots=True)
class _LeaseCapacityAggregate:
    """Validated O(1) lease admission counts for one durable epoch."""

    epoch_id: str
    generation: int
    checkpoint_id: str
    global_live_leases: int
    cluster_kind_counts: dict[str, dict[JobKind, int]]
    document_sha256: str


@dataclass(frozen=True, slots=True)
class _LeaseCapacityCheckpoint:
    """Independent anchor for one exact lease-capacity aggregate generation."""

    epoch_id: str
    generation: int
    checkpoint_id: str
    aggregate_document_sha256: str
    document_sha256: str


@dataclass(frozen=True, slots=True)
class _LeaseCapacityPair:
    """One mutually bound aggregate/checkpoint pair."""

    aggregate: _LeaseCapacityAggregate
    checkpoint: _LeaseCapacityCheckpoint


@dataclass(frozen=True, slots=True)
class _LegacyOutputAudit:
    """Bounded totals established before any legacy-output migration write."""

    marker_complete: bool
    event_records: int
    migration_records: int
    archive_bytes: int
    migration_keys: tuple[tuple[str, int], ...] = ()
    receipt_manifest_sha256: str | None = None


@dataclass(frozen=True, slots=True)
class _LegacyOutputRecord:
    """One exact v0.9 output event and its deterministic replacement."""

    original: RelayEvent
    original_bytes: bytes
    original_sha256: str
    archive_relative_path: str
    replacement: RelayEvent
    replacement_bytes: bytes
    replacement_sha256: str
    representation: Literal["payload_text", "archive"]


class ClioCoreQueue:
    """Durable queue facade for endpoint, job, task, lease, event, cursor, and artifact records."""

    def __init__(
        self,
        root: Path,
        *,
        lock_timeout_seconds: float = DEFAULT_CORE_LOCK_TIMEOUT_SECONDS,
    ) -> None:
        if lock_timeout_seconds <= 0:
            raise ValueError("lock_timeout_seconds must be positive")
        self.root = logical_filesystem_path(root)
        self._storage_root = internal_filesystem_path(self.root, force_extended=True)
        self._lock_timeout_seconds = lock_timeout_seconds
        self._lock = _FairBoundedFileLock(
            str(self._storage_root / ".lock"),
            timeout=lock_timeout_seconds,
        )
        self._initialized = False
        self._migration_lifetime_guarded = False

    def _audit_legacy_state_before_initialization(self) -> _LegacyOutputAudit:
        """Refuse unsafe v0.9 canonical state before creating or changing files."""
        try:
            root_stat = os.lstat(self._storage_root)
        except FileNotFoundError:
            return _LegacyOutputAudit(
                marker_complete=False,
                event_records=0,
                migration_records=0,
                archive_bytes=0,
            )
        except OSError as error:
            raise LegacyQueueStateError(
                family="root",
                path=self._storage_root,
                reason=f"cannot inspect queue root: {type(error).__name__}",
            ) from error
        if not stat.S_ISDIR(root_stat.st_mode) or _record_is_reparse(root_stat):
            raise LegacyQueueStateError(
                family="root",
                path=self._storage_root,
                reason="queue root is not an owned directory",
            )
        lock_path = self._storage_root / ".lock"
        try:
            lock_stat = os.lstat(lock_path)
        except FileNotFoundError:
            pass
        except OSError as error:
            raise LegacyQueueStateError(
                family="root",
                path=lock_path,
                reason=f"cannot inspect queue lock: {type(error).__name__}",
            ) from error
        else:
            if not stat.S_ISREG(lock_stat.st_mode) or _record_is_reparse(lock_stat):
                raise LegacyQueueStateError(
                    family="root",
                    path=lock_path,
                    reason="queue lock is not an owned regular file",
                )

        record_families: tuple[tuple[str, type[BaseModel], str], ...] = (
            ("endpoints", EndpointRegistration, "endpoint_id"),
            ("jobs", RelayJob, "job_id"),
            ("tasks", RelayTask, "task_id"),
            ("leases", Lease, "lease_id"),
            ("artifacts", ArtifactRef, "artifact_id"),
            ("progress", ProgressRecord, "progress_id"),
            ("gateway_sessions", GatewaySession, "session_id"),
            ("monitor_rules", MonitorRule, "rule_id"),
        )
        for family, model, identity_field in record_families:
            self._audit_legacy_record_family(
                family,
                model=model,
                identity_field=identity_field,
            )
        legacy_output_audit = self._audit_legacy_output_state_before_initialization()
        self._audit_legacy_event_family(
            "task_events",
            model=TaskTimelineEvent,
            identity_field="task_id",
        )
        self._audit_legacy_record_family(
            "cursors",
            model=Cursor,
            identity_field="job_id",
        )
        self._audit_legacy_idempotency_family()
        return legacy_output_audit

    def _require_legacy_family_directory(self, family: str) -> Path | None:
        directory = self._storage_root / family
        try:
            directory_stat = os.lstat(directory)
        except FileNotFoundError:
            return None
        except OSError as error:
            raise LegacyQueueStateError(
                family=family,
                path=directory,
                reason=f"cannot inspect canonical family: {type(error).__name__}",
            ) from error
        if not stat.S_ISDIR(directory_stat.st_mode) or _record_is_reparse(directory_stat):
            raise LegacyQueueStateError(
                family=family,
                path=directory,
                reason="canonical family is not an owned directory",
            )
        return directory

    def _bounded_legacy_family_entries(self, family: str) -> list[Path]:
        directory = self._require_legacy_family_directory(family)
        if directory is None:
            return []
        paths: list[Path] = []
        try:
            with os.scandir(directory) as entries:
                for entry in entries:
                    if len(paths) >= MAX_BOUNDED_SCAN_RECORDS:
                        raise LegacyQueueStateError(
                            family=family,
                            path=directory,
                            reason=(
                                "canonical family exceeds the bounded legacy audit limit of "
                                f"{MAX_BOUNDED_SCAN_RECORDS} entries"
                            ),
                        )
                    paths.append(Path(entry.path))
        except LegacyQueueStateError:
            raise
        except OSError as error:
            raise LegacyQueueStateError(
                family=family,
                path=directory,
                reason=f"cannot scan canonical family: {type(error).__name__}",
            ) from error
        return paths

    def _audit_legacy_record_family(
        self,
        family: str,
        *,
        model: type[Record],
        identity_field: str,
    ) -> None:
        for path in self._bounded_legacy_family_entries(family):
            self._require_legacy_regular_json(family, path)
            record_id = path.name.removesuffix(".json")
            self._require_legacy_durable_id(family, path, record_id)
            try:
                record = self._read_json_file(path, model)
            except (OSError, ValueError, QueueConflictError) as error:
                raise LegacyQueueStateError(
                    family=family,
                    path=path,
                    reason=f"canonical record is invalid: {type(error).__name__}",
                ) from error
            if getattr(record, identity_field, None) != record_id:
                raise LegacyQueueStateError(
                    family=family,
                    path=path,
                    reason=f"filename/content identity mismatch for {identity_field}",
                )

    def _legacy_output_marker_path(self) -> Path:
        return self._storage_root / "migrations" / "legacy-output-v1.json"

    def _legacy_output_archive_path(self, job_id: str, seq: int) -> Path:
        return self._storage_root / "legacy_output_archives" / job_id / f"{seq:020d}.json"

    def _legacy_output_receipt_path(self, job_id: str, seq: int) -> Path:
        return self._storage_root / "legacy_output_receipts" / job_id / f"{seq:020d}.json"

    def _read_legacy_output_marker(self) -> _LegacyOutputAudit | None:
        marker_path = self._legacy_output_marker_path()
        if _path_lstat(marker_path) is None:
            return None
        migrations = self._require_legacy_family_directory("migrations")
        if migrations is None:
            raise LegacyQueueStateError(
                family="migrations",
                path=marker_path,
                reason="legacy-output marker has no owned migrations directory",
            )
        try:
            raw = self._read_json_document(marker_path)
        except (OSError, ValueError, QueueConflictError) as error:
            raise LegacyQueueStateError(
                family="migrations",
                path=marker_path,
                reason=f"legacy-output marker is invalid: {type(error).__name__}",
            ) from error
        expected_keys = {
            "schema_version",
            "complete",
            "event_records",
            "migration_records",
            "archive_bytes",
            "receipt_manifest_sha256",
        }
        if not isinstance(raw, dict):
            raise LegacyQueueStateError(
                family="migrations",
                path=marker_path,
                reason="legacy-output marker has an unknown schema shape",
            )
        marker = cast(dict[str, object], raw)
        if set(marker) != expected_keys:
            raise LegacyQueueStateError(
                family="migrations",
                path=marker_path,
                reason="legacy-output marker has an unknown schema shape",
            )
        event_records = marker.get("event_records")
        migration_records = marker.get("migration_records")
        archive_bytes = marker.get("archive_bytes")
        receipt_manifest_sha256 = marker.get("receipt_manifest_sha256")
        if (
            marker.get("schema_version") != LEGACY_OUTPUT_MIGRATION_SCHEMA
            or marker.get("complete") is not True
            or isinstance(event_records, bool)
            or not isinstance(event_records, int)
            or not 0 <= event_records <= MAX_LEGACY_EVENT_AUDIT_RECORDS
            or isinstance(migration_records, bool)
            or not isinstance(migration_records, int)
            or not 0 <= migration_records <= MAX_LEGACY_OUTPUT_MIGRATION_RECORDS
            or isinstance(archive_bytes, bool)
            or not isinstance(archive_bytes, int)
            or not 0 <= archive_bytes <= MAX_LEGACY_OUTPUT_MIGRATION_BYTES
            or not _is_sha256_digest(receipt_manifest_sha256)
        ):
            raise LegacyQueueStateError(
                family="migrations",
                path=marker_path,
                reason="legacy-output marker fields are invalid",
            )
        for family in (
            "legacy_output_archives",
            "legacy_output_receipts",
            "legacy_output_retired",
        ):
            family_path = self._storage_root / family
            if self._require_legacy_family_directory(family) is None:
                raise LegacyQueueStateError(
                    family=family,
                    path=family_path,
                    reason=("legacy-output completion marker requires its owned record directory"),
                )
        return _LegacyOutputAudit(
            marker_complete=True,
            event_records=event_records,
            migration_records=migration_records,
            archive_bytes=archive_bytes,
            receipt_manifest_sha256=cast(str, receipt_manifest_sha256),
        )

    def _iter_legacy_event_paths(
        self,
        family: str,
        *,
        max_directories: int,
        max_records: int,
    ) -> Iterable[tuple[Path, str, int]]:
        directory = self._require_legacy_family_directory(family)
        if directory is None:
            return
        directory_count = 0
        record_count = 0
        try:
            with os.scandir(directory) as identity_entries:
                for identity_entry in identity_entries:
                    directory_count += 1
                    identity_directory = Path(identity_entry.path)
                    if directory_count > max_directories:
                        raise LegacyQueueStateError(
                            family=family,
                            path=directory,
                            reason=(
                                "event identity directories exceed the bounded legacy audit "
                                f"limit of {max_directories}"
                            ),
                        )
                    try:
                        directory_stat = os.lstat(identity_directory)
                    except OSError as error:
                        raise LegacyQueueStateError(
                            family=family,
                            path=identity_directory,
                            reason=(
                                f"cannot inspect event identity directory: {type(error).__name__}"
                            ),
                        ) from error
                    if not stat.S_ISDIR(directory_stat.st_mode) or _record_is_reparse(
                        directory_stat
                    ):
                        raise LegacyQueueStateError(
                            family=family,
                            path=identity_directory,
                            reason="event identity entry is not an owned directory",
                        )
                    identity = self._require_legacy_durable_id(
                        family,
                        identity_directory,
                        identity_directory.name,
                    )
                    try:
                        with os.scandir(identity_directory) as event_entries:
                            for event_entry in event_entries:
                                record_count += 1
                                path = Path(event_entry.path)
                                if record_count > max_records:
                                    raise LegacyQueueStateError(
                                        family=family,
                                        path=identity_directory,
                                        reason=(
                                            "event family exceeds the bounded legacy audit "
                                            f"limit of {max_records} records"
                                        ),
                                    )
                                self._require_legacy_regular_json(family, path)
                                sequence_text = path.name.removesuffix(".json")
                                if (
                                    len(sequence_text) != 20
                                    or not sequence_text.isascii()
                                    or not sequence_text.isdigit()
                                ):
                                    raise LegacyQueueStateError(
                                        family=family,
                                        path=path,
                                        reason=(
                                            "event filename is not a canonical 20-digit sequence"
                                        ),
                                    )
                                yield path, identity, int(sequence_text)
                    except LegacyQueueStateError:
                        raise
                    except OSError as error:
                        raise LegacyQueueStateError(
                            family=family,
                            path=identity_directory,
                            reason=(
                                f"cannot scan event identity directory: {type(error).__name__}"
                            ),
                        ) from error
        except LegacyQueueStateError:
            raise
        except OSError as error:
            raise LegacyQueueStateError(
                family=family,
                path=directory,
                reason=f"cannot scan canonical event family: {type(error).__name__}",
            ) from error

    def _read_v09_legacy_output_record(
        self,
        path: Path,
        *,
        job_id: str,
        seq: int,
    ) -> _LegacyOutputRecord:
        path_stat = os.lstat(path)
        ordinary_limit = RECORD_FAMILY_MAX_BYTES["events"]
        if path_stat.st_size <= ordinary_limit:
            raise ValueError("legacy output source is not oversized")
        if path_stat.st_size > MAX_LEGACY_OUTPUT_RECORD_BYTES:
            raise QueueConflictError(
                "legacy output event exceeds the bounded compatibility limit of "
                f"{MAX_LEGACY_OUTPUT_RECORD_BYTES} bytes"
            )
        original_bytes = _read_bounded_record_bytes_once(
            path,
            limit=MAX_LEGACY_OUTPUT_RECORD_BYTES,
        )
        try:
            raw = json.loads(original_bytes)
        except json.JSONDecodeError as error:
            raise ValueError("legacy output event is not valid JSON") from error
        expected_keys = {
            "job_id",
            "seq",
            "event_type",
            "message",
            "level",
            "created_at",
            "payload",
        }
        if not isinstance(raw, dict):
            raise ValueError("legacy output event has an unknown top-level shape")
        document = cast(dict[str, object], raw)
        if set(document) != expected_keys:
            raise ValueError("legacy output event has an unknown top-level shape")
        payload = document.get("payload")
        if not isinstance(payload, dict):
            raise ValueError("legacy output event payload is not the exact v0.9 shape")
        typed_payload = cast(dict[str, object], payload)
        if set(typed_payload) != {"stream", "text"}:
            raise ValueError("legacy output event payload is not the exact v0.9 shape")
        event_type = document.get("event_type")
        stream = typed_payload.get("stream")
        text = typed_payload.get("text")
        message = document.get("message")
        if (
            event_type not in {"stdout.delta", "stderr.delta"}
            or stream not in {"stdout", "stderr"}
            or event_type != f"{stream}.delta"
            or document.get("level") != "info"
            or not isinstance(text, str)
            or not isinstance(message, str)
            or (text.rstrip("\n") or f"{stream} output") != message
        ):
            raise ValueError("legacy output event is not an exact duplicated v0.9 delta")
        original = RelayEvent.model_validate(document)
        if original.job_id != job_id or original.seq != seq:
            raise ValueError("legacy output filename/content identity mismatch")
        original_sha256 = hashlib.sha256(original_bytes).hexdigest()
        archive_relative_path = (
            Path("legacy_output_archives") / job_id / f"{seq:020d}.json"
        ).as_posix()

        def replacement(representation: Literal["payload_text", "archive"]) -> RelayEvent:
            compatibility = {
                "schema_version": LEGACY_OUTPUT_COMPATIBILITY_SCHEMA,
                "archive_path": archive_relative_path,
                "archive_sha256": original_sha256,
                "archive_size_bytes": len(original_bytes),
                "original_message_utf8_bytes": len(message.encode("utf-8")),
                "original_payload_text_utf8_bytes": len(text.encode("utf-8")),
                "representation": representation,
            }
            replacement_message = (
                f"Legacy {stream} output preserved in payload.text "
                f"({len(text.encode('utf-8'))} UTF-8 bytes)"
                if representation == "payload_text"
                else f"Legacy {stream} output archived ({len(text.encode('utf-8'))} UTF-8 bytes)"
            )
            replacement_payload: dict[str, object] = {
                "stream": stream,
                "legacy_output": compatibility,
            }
            if representation == "payload_text":
                replacement_payload["text"] = text
            return original.model_copy(
                update={
                    "message": replacement_message,
                    "payload": replacement_payload,
                }
            )

        representation: Literal["payload_text", "archive"] = "payload_text"
        replacement_record = replacement(representation)
        replacement_bytes = replacement_record.model_dump_json(indent=2).encode("utf-8")
        if len(replacement_bytes) > ordinary_limit:
            representation = "archive"
            replacement_record = replacement(representation)
            replacement_bytes = replacement_record.model_dump_json(indent=2).encode("utf-8")
        if len(replacement_bytes) > ordinary_limit:
            raise QueueConflictError("legacy output compatibility event exceeds the event limit")
        return _LegacyOutputRecord(
            original=original,
            original_bytes=original_bytes,
            original_sha256=original_sha256,
            archive_relative_path=archive_relative_path,
            replacement=replacement_record,
            replacement_bytes=replacement_bytes,
            replacement_sha256=hashlib.sha256(replacement_bytes).hexdigest(),
            representation=representation,
        )

    @staticmethod
    def _legacy_output_receipt(record: _LegacyOutputRecord) -> dict[str, object]:
        return {
            "schema_version": LEGACY_OUTPUT_RECEIPT_SCHEMA,
            "job_id": record.original.job_id,
            "seq": record.original.seq,
            "event_type": record.original.event_type,
            "archive_path": record.archive_relative_path,
            "archive_sha256": record.original_sha256,
            "archive_size_bytes": len(record.original_bytes),
            "replacement_sha256": record.replacement_sha256,
            "replacement_size_bytes": len(record.replacement_bytes),
            "representation": record.representation,
        }

    def _validate_legacy_output_archive(
        self,
        path: Path,
        record: _LegacyOutputRecord,
    ) -> None:
        try:
            archived = _read_bounded_record_bytes(path)
        except (OSError, QueueConflictError) as error:
            raise LegacyQueueStateError(
                family="legacy_output_archives",
                path=path,
                reason=f"legacy output archive is invalid: {type(error).__name__}",
            ) from error
        if archived != record.original_bytes:
            raise LegacyQueueStateError(
                family="legacy_output_archives",
                path=path,
                reason="legacy output archive does not exactly match its original event",
            )

    def _validate_legacy_output_receipt(
        self,
        path: Path,
        record: _LegacyOutputRecord,
    ) -> None:
        raw = self._read_legacy_output_receipt_document(
            path,
            job_id=record.original.job_id,
            seq=record.original.seq,
        )
        if raw != self._legacy_output_receipt(record):
            raise LegacyQueueStateError(
                family=_record_family(path),
                path=path,
                reason="legacy output receipt does not match archive and replacement",
            )

    def _read_legacy_output_receipt_document(
        self,
        path: Path,
        *,
        job_id: str,
        seq: int,
    ) -> dict[str, object]:
        """Read one self-validating active or retired migration receipt."""
        family = _record_family(path)
        try:
            raw = self._read_json_document(path)
        except (OSError, ValueError, QueueConflictError) as error:
            raise LegacyQueueStateError(
                family=family,
                path=path,
                reason=f"legacy output receipt is invalid: {type(error).__name__}",
            ) from error
        expected_keys = {
            "schema_version",
            "job_id",
            "seq",
            "event_type",
            "archive_path",
            "archive_sha256",
            "archive_size_bytes",
            "replacement_sha256",
            "replacement_size_bytes",
            "representation",
        }
        if not isinstance(raw, dict):
            raise LegacyQueueStateError(
                family=family,
                path=path,
                reason="legacy output receipt has an unknown schema shape",
            )
        receipt = cast(dict[str, object], raw)
        if set(receipt) != expected_keys:
            raise LegacyQueueStateError(
                family=family,
                path=path,
                reason="legacy output receipt has an unknown schema shape",
            )
        archive_size = receipt.get("archive_size_bytes")
        replacement_size = receipt.get("replacement_size_bytes")
        expected_archive_path = (
            Path("legacy_output_archives") / job_id / f"{seq:020d}.json"
        ).as_posix()
        if (
            receipt.get("schema_version") != LEGACY_OUTPUT_RECEIPT_SCHEMA
            or receipt.get("job_id") != job_id
            or receipt.get("seq") != seq
            or receipt.get("event_type") not in {"stdout.delta", "stderr.delta"}
            or receipt.get("archive_path") != expected_archive_path
            or not _is_sha256_digest(receipt.get("archive_sha256"))
            or isinstance(archive_size, bool)
            or not isinstance(archive_size, int)
            or not RECORD_FAMILY_MAX_BYTES["events"] < archive_size
            or archive_size > MAX_LEGACY_OUTPUT_RECORD_BYTES
            or not _is_sha256_digest(receipt.get("replacement_sha256"))
            or isinstance(replacement_size, bool)
            or not isinstance(replacement_size, int)
            or not 0 < replacement_size <= RECORD_FAMILY_MAX_BYTES["events"]
            or receipt.get("representation") not in {"payload_text", "archive"}
        ):
            raise LegacyQueueStateError(
                family=family,
                path=path,
                reason="legacy output receipt fields are invalid",
            )
        return receipt

    @staticmethod
    def _legacy_output_receipt_manifest_sha256(
        receipt_paths: dict[tuple[str, int], Path],
    ) -> str:
        """Hash exact immutable receipt bytes independently of active/retired location."""
        digest = hashlib.sha256()
        for (job_id, seq), path in sorted(receipt_paths.items()):
            identity = f"{job_id}\0{seq:020d}\0".encode()
            receipt_bytes = _read_bounded_record_bytes(path)
            digest.update(len(identity).to_bytes(8, "big"))
            digest.update(identity)
            digest.update(len(receipt_bytes).to_bytes(8, "big"))
            digest.update(receipt_bytes)
        return digest.hexdigest()

    def _record_from_legacy_compatibility_event(
        self,
        path: Path,
        event: RelayEvent,
        event_bytes: bytes,
        *,
        job_id: str,
        seq: int,
    ) -> _LegacyOutputRecord:
        payload = event.payload
        compatibility = payload.get("legacy_output")
        if not isinstance(compatibility, dict):
            raise LegacyQueueStateError(
                family="events",
                path=path,
                reason="legacy output compatibility metadata is not an object",
            )
        typed_compatibility = cast(dict[str, object], compatibility)
        expected_keys = {
            "schema_version",
            "archive_path",
            "archive_sha256",
            "archive_size_bytes",
            "original_message_utf8_bytes",
            "original_payload_text_utf8_bytes",
            "representation",
        }
        if (
            set(typed_compatibility) != expected_keys
            or typed_compatibility.get("schema_version") != LEGACY_OUTPUT_COMPATIBILITY_SCHEMA
        ):
            raise LegacyQueueStateError(
                family="events",
                path=path,
                reason="legacy output compatibility metadata has an unknown schema",
            )
        representation = typed_compatibility.get("representation")
        expected_payload_keys = (
            {"stream", "text", "legacy_output"}
            if representation == "payload_text"
            else {"stream", "legacy_output"}
        )
        if (
            representation not in {"payload_text", "archive"}
            or set(payload) != expected_payload_keys
        ):
            raise LegacyQueueStateError(
                family="events",
                path=path,
                reason="legacy output compatibility payload has an unknown shape",
            )
        archive_path = self._legacy_output_archive_path(job_id, seq)
        if (
            typed_compatibility.get("archive_path")
            != archive_path.relative_to(self._storage_root).as_posix()
        ):
            raise LegacyQueueStateError(
                family="events",
                path=path,
                reason="legacy output compatibility archive path is not canonical",
            )
        try:
            original = self._read_v09_legacy_output_record(
                archive_path,
                job_id=job_id,
                seq=seq,
            )
        except (OSError, ValueError, QueueConflictError) as error:
            raise LegacyQueueStateError(
                family="legacy_output_archives",
                path=archive_path,
                reason=f"legacy output archive is invalid: {type(error).__name__}",
            ) from error
        if event != original.replacement or event_bytes != original.replacement_bytes:
            raise LegacyQueueStateError(
                family="events",
                path=path,
                reason="legacy output compatibility event does not match its exact archive",
            )
        return original

    def _audit_one_legacy_output_event(
        self,
        path: Path,
        *,
        job_id: str,
        seq: int,
    ) -> _LegacyOutputRecord | None:
        try:
            path_stat = os.lstat(path)
            if path_stat.st_size > RECORD_FAMILY_MAX_BYTES["events"]:
                record = self._read_v09_legacy_output_record(
                    path,
                    job_id=job_id,
                    seq=seq,
                )
                archive_path = self._legacy_output_archive_path(job_id, seq)
                if _path_lstat(archive_path) is not None:
                    self._validate_legacy_output_archive(archive_path, record)
                receipt_path = self._legacy_output_receipt_path(job_id, seq)
                if _path_lstat(receipt_path) is not None:
                    raise LegacyQueueStateError(
                        family="legacy_output_receipts",
                        path=receipt_path,
                        reason="receipt exists before the compatibility event replacement",
                    )
                return record
            event_bytes = _read_bounded_record_bytes(path)
            event = RelayEvent.model_validate_json(event_bytes)
        except LegacyQueueStateError:
            raise
        except (OSError, ValueError, QueueConflictError) as error:
            raise LegacyQueueStateError(
                family="events",
                path=path,
                reason=f"event record is invalid: {type(error).__name__}: {error}",
            ) from error
        if event.job_id != job_id:
            raise LegacyQueueStateError(
                family="events",
                path=path,
                reason="event directory/content identity mismatch for job_id",
            )
        if event.seq != seq:
            raise LegacyQueueStateError(
                family="events",
                path=path,
                reason="event filename/content sequence mismatch",
            )
        if "legacy_output" not in event.payload:
            return None
        record = self._record_from_legacy_compatibility_event(
            path,
            event,
            event_bytes,
            job_id=job_id,
            seq=seq,
        )
        receipt_path = self._legacy_output_receipt_path(job_id, seq)
        if _path_lstat(receipt_path) is not None:
            self._validate_legacy_output_receipt(receipt_path, record)
        return record

    def _iter_legacy_output_auxiliary_paths(
        self,
        family: Literal[
            "legacy_output_archives",
            "legacy_output_receipts",
            "legacy_output_retired",
        ],
    ) -> Iterable[tuple[Path, str, int]]:
        yield from self._iter_legacy_event_paths(
            family,
            max_directories=MAX_LEGACY_OUTPUT_MIGRATION_RECORDS,
            max_records=MAX_LEGACY_OUTPUT_MIGRATION_RECORDS,
        )

    def _audit_legacy_output_auxiliary_state(self) -> None:
        for family in ("legacy_output_archives", "legacy_output_receipts"):
            for path, job_id, seq in self._iter_legacy_output_auxiliary_paths(family):
                event_path = self._storage_root / "events" / job_id / f"{seq:020d}.json"
                if _path_lstat(event_path) is None:
                    raise LegacyQueueStateError(
                        family=family,
                        path=path,
                        reason="legacy output auxiliary record has no canonical event",
                    )
                record = self._audit_one_legacy_output_event(
                    event_path,
                    job_id=job_id,
                    seq=seq,
                )
                if record is None:
                    raise LegacyQueueStateError(
                        family=family,
                        path=path,
                        reason="legacy output auxiliary record points to an ordinary event",
                    )
                archive_path = self._legacy_output_archive_path(job_id, seq)
                if _path_lstat(archive_path) is None:
                    raise LegacyQueueStateError(
                        family=family,
                        path=path,
                        reason="legacy output archive is missing",
                    )
                self._validate_legacy_output_archive(archive_path, record)
                if family == "legacy_output_receipts":
                    if event_path.stat().st_size > RECORD_FAMILY_MAX_BYTES["events"]:
                        raise LegacyQueueStateError(
                            family=family,
                            path=path,
                            reason="receipt exists before the compatibility event replacement",
                        )
                    self._validate_legacy_output_receipt(path, record)

        for path, _job_id, _seq in self._iter_legacy_output_auxiliary_paths(
            "legacy_output_retired"
        ):
            raise LegacyQueueStateError(
                family="legacy_output_retired",
                path=path,
                reason="retired receipt exists before legacy-output migration completed",
            )

    def _validate_retired_legacy_output_receipt(
        self,
        path: Path,
        receipt: dict[str, object],
        *,
        job_id: str,
        seq: int,
    ) -> None:
        """Validate durable GC evidence and every canonical component still present."""
        tombstone = self._read_optional(self._job_tombstone_path(job_id), JobTombstone)
        if tombstone is None or tombstone.job_id != job_id or not tombstone.records_trash_started:
            raise LegacyQueueStateError(
                family="legacy_output_retired",
                path=path,
                reason="retired receipt has no authorized terminal-job GC tombstone",
            )
        archive_path = self._legacy_output_archive_path(job_id, seq)
        event_path = self._storage_root / "events" / job_id / f"{seq:020d}.json"
        archive_present = _path_lstat(archive_path) is not None
        event_present = _path_lstat(event_path) is not None
        record: _LegacyOutputRecord | None = None
        if archive_present:
            try:
                record = self._read_v09_legacy_output_record(
                    archive_path,
                    job_id=job_id,
                    seq=seq,
                )
            except (OSError, ValueError, QueueConflictError) as error:
                raise LegacyQueueStateError(
                    family="legacy_output_archives",
                    path=archive_path,
                    reason=f"retired legacy output archive is invalid: {type(error).__name__}",
                ) from error
            self._validate_legacy_output_archive(archive_path, record)
            if receipt != self._legacy_output_receipt(record):
                raise LegacyQueueStateError(
                    family="legacy_output_retired",
                    path=path,
                    reason="retired receipt does not match its remaining archive",
                )
        if not event_present:
            return
        try:
            event_bytes = _read_bounded_record_bytes(event_path)
            event = RelayEvent.model_validate_json(event_bytes)
        except (OSError, ValueError, QueueConflictError) as error:
            raise LegacyQueueStateError(
                family="events",
                path=event_path,
                reason=f"retired legacy output event is invalid: {type(error).__name__}",
            ) from error
        if (
            event.job_id != job_id
            or event.seq != seq
            or event.event_type != receipt.get("event_type")
            or len(event_bytes) != receipt.get("replacement_size_bytes")
            or hashlib.sha256(event_bytes).hexdigest() != receipt.get("replacement_sha256")
        ):
            raise LegacyQueueStateError(
                family="events",
                path=event_path,
                reason="retired legacy output event does not match its receipt",
            )
        compatibility = event.payload.get("legacy_output")
        if not isinstance(compatibility, dict):
            raise LegacyQueueStateError(
                family="events",
                path=event_path,
                reason="retired legacy output event has no compatibility metadata",
            )
        typed_compatibility = cast(dict[str, object], compatibility)
        if (
            typed_compatibility.get("schema_version") != LEGACY_OUTPUT_COMPATIBILITY_SCHEMA
            or typed_compatibility.get("archive_path") != receipt.get("archive_path")
            or typed_compatibility.get("archive_sha256") != receipt.get("archive_sha256")
            or typed_compatibility.get("archive_size_bytes") != receipt.get("archive_size_bytes")
            or typed_compatibility.get("representation") != receipt.get("representation")
        ):
            raise LegacyQueueStateError(
                family="events",
                path=event_path,
                reason="retired compatibility metadata does not match its receipt",
            )
        if record is not None and (
            event != record.replacement or event_bytes != record.replacement_bytes
        ):
            raise LegacyQueueStateError(
                family="events",
                path=event_path,
                reason="retired compatibility event does not match its remaining archive",
            )

    def _audit_completed_legacy_output_state(self, marker: _LegacyOutputAudit) -> None:
        """Boundedly verify every active or GC-retired migration receipt."""
        receipts: dict[tuple[str, int], tuple[str, Path, dict[str, object]]] = {}
        receipt_paths: dict[tuple[str, int], Path] = {}
        archive_bytes = 0
        for family in ("legacy_output_receipts", "legacy_output_retired"):
            for path, job_id, seq in self._iter_legacy_output_auxiliary_paths(family):
                key = (job_id, seq)
                if key in receipts:
                    raise LegacyQueueStateError(
                        family=family,
                        path=path,
                        reason="legacy output identity exists in active and retired receipts",
                    )
                receipt = self._read_legacy_output_receipt_document(
                    path,
                    job_id=job_id,
                    seq=seq,
                )
                receipts[key] = (family, path, receipt)
                receipt_paths[key] = path
                archive_bytes += cast(int, receipt["archive_size_bytes"])
                if len(receipts) > MAX_LEGACY_OUTPUT_MIGRATION_RECORDS:
                    raise LegacyQueueStateError(
                        family=family,
                        path=path,
                        reason="legacy output receipts exceed the bounded migration limit",
                    )

        if (
            len(receipts) != marker.migration_records
            or archive_bytes != marker.archive_bytes
            or self._legacy_output_receipt_manifest_sha256(receipt_paths)
            != marker.receipt_manifest_sha256
        ):
            raise LegacyQueueStateError(
                family="migrations",
                path=self._legacy_output_marker_path(),
                reason=("legacy-output marker totals do not match active and retired receipts"),
            )

        for (job_id, seq), (family, path, receipt) in receipts.items():
            if family == "legacy_output_retired":
                self._validate_retired_legacy_output_receipt(
                    path,
                    receipt,
                    job_id=job_id,
                    seq=seq,
                )
                continue
            event_path = self._storage_root / "events" / job_id / f"{seq:020d}.json"
            archive_path = self._legacy_output_archive_path(job_id, seq)
            if _path_lstat(event_path) is None or _path_lstat(archive_path) is None:
                raise LegacyQueueStateError(
                    family=family,
                    path=path,
                    reason="active receipt is missing its canonical event or archive",
                )
            record = self._audit_one_legacy_output_event(
                event_path,
                job_id=job_id,
                seq=seq,
            )
            if record is None or event_path.stat().st_size > RECORD_FAMILY_MAX_BYTES["events"]:
                raise LegacyQueueStateError(
                    family=family,
                    path=path,
                    reason="active receipt does not point to a compatibility event",
                )
            self._validate_legacy_output_archive(archive_path, record)
            self._validate_legacy_output_receipt(path, record)

        for archive_path, job_id, seq in self._iter_legacy_output_auxiliary_paths(
            "legacy_output_archives"
        ):
            if (job_id, seq) not in receipts:
                raise LegacyQueueStateError(
                    family="legacy_output_archives",
                    path=archive_path,
                    reason="legacy output archive has no active or retired receipt",
                )

    def _audit_legacy_output_state_before_initialization(self) -> _LegacyOutputAudit:
        marker = self._read_legacy_output_marker()
        if marker is not None:
            with self._lock:
                locked_marker = self._read_legacy_output_marker()
                if locked_marker is None or locked_marker != marker:
                    raise QueueConflictError(
                        "legacy-output completion marker changed while taking the queue lock"
                    )
                self._audit_completed_legacy_output_state(locked_marker)
                return locked_marker
        event_records = 0
        migration_records = 0
        archive_bytes = 0
        migration_keys: list[tuple[str, int]] = []
        for path, job_id, seq in self._iter_legacy_event_paths(
            "events",
            max_directories=MAX_LEGACY_EVENT_AUDIT_DIRECTORIES,
            max_records=MAX_LEGACY_EVENT_AUDIT_RECORDS,
        ):
            event_records += 1
            record = self._audit_one_legacy_output_event(
                path,
                job_id=job_id,
                seq=seq,
            )
            if record is None:
                continue
            migration_records += 1
            migration_keys.append((job_id, seq))
            archive_bytes += len(record.original_bytes)
            if migration_records > MAX_LEGACY_OUTPUT_MIGRATION_RECORDS:
                raise LegacyQueueStateError(
                    family="events",
                    path=path,
                    reason=(
                        "legacy output migration exceeds the bounded record limit of "
                        f"{MAX_LEGACY_OUTPUT_MIGRATION_RECORDS}"
                    ),
                )
            if archive_bytes > MAX_LEGACY_OUTPUT_MIGRATION_BYTES:
                raise LegacyQueueStateError(
                    family="events",
                    path=path,
                    reason=(
                        "legacy output migration exceeds the bounded aggregate byte limit "
                        f"of {MAX_LEGACY_OUTPUT_MIGRATION_BYTES}"
                    ),
                )
        self._audit_legacy_output_auxiliary_state()
        return _LegacyOutputAudit(
            marker_complete=False,
            event_records=event_records,
            migration_records=migration_records,
            archive_bytes=archive_bytes,
            migration_keys=tuple(migration_keys),
        )

    def _write_legacy_output_archive(
        self,
        path: Path,
        record: _LegacyOutputRecord,
    ) -> None:
        if _path_lstat(path) is not None:
            self._validate_legacy_output_archive(path, record)
            return
        self._write_bytes(
            path,
            record.original_bytes,
            max_bytes=MAX_LEGACY_OUTPUT_RECORD_BYTES,
        )
        self._validate_legacy_output_archive(path, record)

    def _write_legacy_output_receipt(
        self,
        path: Path,
        record: _LegacyOutputRecord,
    ) -> None:
        if _path_lstat(path) is not None:
            self._validate_legacy_output_receipt(path, record)
            return
        self._write_json(path, self._legacy_output_receipt(record))
        self._validate_legacy_output_receipt(path, record)

    def _migrate_legacy_output_events_unlocked(
        self,
        audit: _LegacyOutputAudit,
    ) -> None:
        """Archive and replace exact v0.9 output records after a complete audit."""
        if audit.marker_complete:
            return
        if len(audit.migration_keys) != audit.migration_records:
            raise QueueConflictError("legacy output migration plan is incomplete")
        migration_records = 0
        archive_bytes = 0
        for job_id, seq in audit.migration_keys:
            path = self._storage_root / "events" / job_id / f"{seq:020d}.json"
            if _path_lstat(path) is None:
                raise QueueConflictError(
                    f"legacy output event disappeared after its complete audit: {path}"
                )
            was_oversized = path.stat().st_size > RECORD_FAMILY_MAX_BYTES["events"]
            record = self._audit_one_legacy_output_event(
                path,
                job_id=job_id,
                seq=seq,
            )
            if record is None:
                continue
            migration_records += 1
            archive_bytes += len(record.original_bytes)
            archive_path = self._legacy_output_archive_path(job_id, seq)
            receipt_path = self._legacy_output_receipt_path(job_id, seq)
            if was_oversized:
                self._write_legacy_output_archive(archive_path, record)
                self._after_legacy_output_migration_phase("archive", path)
                current = _read_bounded_record_bytes_once(
                    path,
                    limit=MAX_LEGACY_OUTPUT_RECORD_BYTES,
                )
                if current != record.original_bytes:
                    raise QueueConflictError(
                        f"legacy output event changed after validation: {path}"
                    )
                self._write_bytes(
                    path,
                    record.replacement_bytes,
                    max_bytes=RECORD_FAMILY_MAX_BYTES["events"],
                )
                self._after_legacy_output_migration_phase("replacement", path)
            self._write_legacy_output_receipt(receipt_path, record)
            self._after_legacy_output_migration_phase("receipt", path)
        observed = _LegacyOutputAudit(
            marker_complete=False,
            event_records=audit.event_records,
            migration_records=migration_records,
            archive_bytes=archive_bytes,
            migration_keys=audit.migration_keys,
        )
        if observed != audit:
            raise QueueConflictError("legacy output state changed after its complete audit")
        self._audit_legacy_output_auxiliary_state()
        receipt_paths = {
            (job_id, seq): path
            for path, job_id, seq in self._iter_legacy_output_auxiliary_paths(
                "legacy_output_receipts"
            )
        }
        if len(receipt_paths) != migration_records:
            raise QueueConflictError("legacy output receipt manifest is incomplete")
        receipt_manifest_sha256 = self._legacy_output_receipt_manifest_sha256(receipt_paths)
        marker: dict[str, object] = {
            "schema_version": LEGACY_OUTPUT_MIGRATION_SCHEMA,
            "complete": True,
            "event_records": audit.event_records,
            "migration_records": migration_records,
            "archive_bytes": archive_bytes,
            "receipt_manifest_sha256": receipt_manifest_sha256,
        }
        self._write_json(self._legacy_output_marker_path(), marker)
        self._after_legacy_output_migration_phase(
            "marker",
            self._legacy_output_marker_path(),
        )
        durable_marker = self._read_legacy_output_marker()
        if durable_marker is None or durable_marker != _LegacyOutputAudit(
            marker_complete=True,
            event_records=audit.event_records,
            migration_records=migration_records,
            archive_bytes=archive_bytes,
            receipt_manifest_sha256=receipt_manifest_sha256,
        ):
            raise QueueConflictError("legacy output migration marker was not durable")

    def _require_legacy_output_migration_authorized(
        self,
        audit: _LegacyOutputAudit,
        *,
        migrate_legacy_output: bool,
    ) -> None:
        if audit.marker_complete or audit.migration_records == 0 or migrate_legacy_output:
            return
        raise LegacyQueueStateError(
            family="events",
            path=self._storage_root / "events",
            reason=(
                f"{audit.migration_records} exact v0.9 output event(s) require an explicitly "
                "authorized compatibility migration"
            ),
            action=(
                "stop and verify every process that can write this queue, then run "
                "clio-relay init --migrate-legacy-output"
            ),
        )

    @staticmethod
    def _after_legacy_output_migration_phase(_phase: str, _path: Path) -> None:
        """Fault-injection seam after each durable legacy-output phase."""

    def _audit_legacy_event_family(
        self,
        family: str,
        *,
        model: type[Record],
        identity_field: str,
    ) -> None:
        record_count = 0
        for directory in self._bounded_legacy_family_entries(family):
            try:
                directory_stat = os.lstat(directory)
            except OSError as error:
                raise LegacyQueueStateError(
                    family=family,
                    path=directory,
                    reason=f"cannot inspect event identity directory: {type(error).__name__}",
                ) from error
            if not stat.S_ISDIR(directory_stat.st_mode) or _record_is_reparse(directory_stat):
                raise LegacyQueueStateError(
                    family=family,
                    path=directory,
                    reason="event identity entry is not an owned directory",
                )
            self._require_legacy_durable_id(family, directory, directory.name)
            try:
                with os.scandir(directory) as entries:
                    for entry in entries:
                        record_count += 1
                        path = Path(entry.path)
                        if record_count > MAX_BOUNDED_SCAN_RECORDS:
                            raise LegacyQueueStateError(
                                family=family,
                                path=directory,
                                reason=(
                                    "event family exceeds the bounded legacy audit limit of "
                                    f"{MAX_BOUNDED_SCAN_RECORDS} records"
                                ),
                            )
                        self._require_legacy_regular_json(family, path)
                        sequence_text = path.name.removesuffix(".json")
                        if (
                            len(sequence_text) != 20
                            or not sequence_text.isascii()
                            or not sequence_text.isdigit()
                        ):
                            raise LegacyQueueStateError(
                                family=family,
                                path=path,
                                reason="event filename is not a canonical 20-digit sequence",
                            )
                        try:
                            record = self._read_json_file(path, model)
                        except (OSError, ValueError, QueueConflictError) as error:
                            raise LegacyQueueStateError(
                                family=family,
                                path=path,
                                reason=f"event record is invalid: {type(error).__name__}",
                            ) from error
                        if getattr(record, identity_field, None) != directory.name:
                            raise LegacyQueueStateError(
                                family=family,
                                path=path,
                                reason=(
                                    "event directory/content identity mismatch for "
                                    f"{identity_field}"
                                ),
                            )
                        if getattr(record, "seq", None) != int(sequence_text):
                            raise LegacyQueueStateError(
                                family=family,
                                path=path,
                                reason="event filename/content sequence mismatch",
                            )
            except LegacyQueueStateError:
                raise
            except OSError as error:
                raise LegacyQueueStateError(
                    family=family,
                    path=directory,
                    reason=f"cannot scan event identity directory: {type(error).__name__}",
                ) from error

    def _audit_legacy_idempotency_family(self) -> None:
        family = "idempotency"
        for path in self._bounded_legacy_family_entries(family):
            self._require_legacy_regular_json(family, path)
            filename = path.name.removesuffix(".json")
            digest = filename.removeprefix("key_")
            if (
                not filename.startswith("key_")
                or len(digest) != 64
                or any(character not in "0123456789abcdef" for character in digest)
            ):
                raise LegacyQueueStateError(
                    family=family,
                    path=path,
                    reason="idempotency filename is not a canonical SHA-256",
                )
            try:
                raw = self._read_json_document(path)
            except (OSError, ValueError, QueueConflictError) as error:
                raise LegacyQueueStateError(
                    family=family,
                    path=path,
                    reason=f"idempotency record is invalid: {type(error).__name__}",
                ) from error
            if not isinstance(raw, dict):
                raise LegacyQueueStateError(
                    family=family,
                    path=path,
                    reason="idempotency record is not an object",
                )
            document = cast(dict[str, object], raw)
            self._require_legacy_durable_id(family, path, document.get("job_id"))
            idempotency_key = document.get("idempotency_key")
            if not isinstance(idempotency_key, str) or not idempotency_key:
                raise LegacyQueueStateError(
                    family=family,
                    path=path,
                    reason="idempotency record has no string idempotency_key",
                )
            if _idempotency_key_filename(idempotency_key) != filename:
                raise LegacyQueueStateError(
                    family=family,
                    path=path,
                    reason="idempotency filename/content identity mismatch",
                )

    @staticmethod
    def _require_legacy_regular_json(family: str, path: Path) -> None:
        try:
            path_stat = os.lstat(path)
        except OSError as error:
            raise LegacyQueueStateError(
                family=family,
                path=path,
                reason=f"cannot inspect canonical record: {type(error).__name__}",
            ) from error
        if (
            not path.name.endswith(".json")
            or not stat.S_ISREG(path_stat.st_mode)
            or _record_is_reparse(path_stat)
        ):
            raise LegacyQueueStateError(
                family=family,
                path=path,
                reason="canonical record is not an owned .json regular file",
            )

    @staticmethod
    def _require_legacy_durable_id(family: str, path: Path, value: object) -> str:
        if not isinstance(value, str):
            raise LegacyQueueStateError(
                family=family,
                path=path,
                reason="canonical identity is not a string",
            )
        try:
            return validate_durable_record_id(value)
        except ValueError as error:
            raise LegacyQueueStateError(
                family=family,
                path=path,
                reason=f"canonical identity is not portable: {error}",
            ) from error

    def initialize(
        self,
        *,
        migrate_legacy_output: bool = False,
        locked_core: LockedCoreIdentity | None = None,
    ) -> None:
        """Create the record families used by the queue."""
        if locked_core is not None:
            if not migrate_legacy_output or self._migration_lifetime_guarded:
                raise ConfigurationError(
                    "locked-core authority is only valid for the outer migration scope"
                )
            require_active_locked_core(locked_core)
            self._initialize_under_locked_core(locked_core)
            return
        if migrate_legacy_output and not self._migration_lifetime_guarded:
            with exclusive_migration_lifetime(self.root) as locked_core:
                self.initialize(
                    migrate_legacy_output=True,
                    locked_core=locked_core,
                )
            return
        if self._initialized:
            with self._lock:
                self._ensure_extended_migration_state()
            return
        # The first pass is deliberately read-only: an unsafe family must fail
        # before initialize creates even the migration/archive directories.
        legacy_output_audit = self._audit_legacy_state_before_initialization()
        self._require_legacy_output_migration_authorized(
            legacy_output_audit,
            migrate_legacy_output=migrate_legacy_output,
        )
        for family in (
            "endpoints",
            "endpoints_fresh",
            "endpoints_fresh_by_id",
            "jobs",
            "tasks",
            "leases",
            "lease_indexes",
            "lease_identity_refs",
            "leases_by_endpoint",
            "leases_by_cluster_kind",
            "leases_by_expiry",
            "lease_capacity",
            "events",
            "legacy_output_archives",
            "legacy_output_receipts",
            "legacy_output_retired",
            "artifacts",
            "artifact_user_order",
            "artifact_users",
            "progress",
            "task_events",
            "gateway_sessions",
            "gateway_reverse_refs_by_session",
            "gateways_by_artifact",
            "gateways_by_scheduler",
            "active_gateway_refs_by_job",
            "active_gateway_refs_by_session",
            "idempotency",
            "monitor_rules",
            "monitor_rules_by_job",
            "active_monitor_rules_by_job",
            "owner_sessions",
            "owner_session_jobs",
            "owner_session_legacy_jobs",
            "job_indexes",
            "tasks_by_job",
            "leases_by_job",
            "artifacts_by_job",
            "used_artifacts_by_job",
            "progress_by_job",
            "jobs_active",
            "jobs_queued",
            "task_event_heads",
            "migrations",
            "task_order_by_job",
            "transition_intents",
            "artifact_order_by_job",
            "progress_order_by_job",
            "active_tasks_by_job",
            "scheduler_refs_by_job",
            "scheduler_protections_by_job",
            "scheduler_jobs",
            "scheduler_cancel_pending",
            "scheduler_cancel_dispositions",
            "job_tombstones",
            "gc_runs",
            "gc_trash",
            "global_order",
        ):
            (self._storage_root / family).mkdir(parents=True, exist_ok=True)
        for family in _GLOBAL_ORDER_FAMILIES:
            (self._storage_root / "global_order" / family / "by_id").mkdir(
                parents=True,
                exist_ok=True,
            )
            (self._storage_root / "global_order" / family / "entries").mkdir(
                parents=True,
                exist_ok=True,
            )
        with self._lock:
            # Revalidate once under the cross-process lock before the one-time
            # migration.  This closes the audit/write race without retaining an
            # unbounded path plan in memory; only the independently bounded set of
            # migration keys is retained, so writes do not require a third history
            # scan.  The durable completion marker makes both passes skip deep
            # event history on every later startup.
            legacy_output_audit = self._audit_legacy_state_before_initialization()
            self._require_legacy_output_migration_authorized(
                legacy_output_audit,
                migrate_legacy_output=migrate_legacy_output,
            )
            self._purge_write_staging_unlocked()
            self._migrate_legacy_output_events_unlocked(legacy_output_audit)
            migration_path = self._storage_root / "migrations" / "index-v1.json"
            if not migration_path.exists():
                has_legacy_jobs = (
                    next((self._storage_root / "jobs").glob("*.json"), None) is not None
                )
                retention_checkpoints = {
                    family: {
                        "cursor": None,
                        "complete": (
                            next((self._storage_root / family).glob("*.json"), None) is None
                        ),
                    }
                    for family in _RETENTION_INDEX_FAMILIES
                }
                has_legacy_retention = any(
                    checkpoint["complete"] is not True
                    for checkpoint in retention_checkpoints.values()
                )
                global_order_checkpoints = {
                    family: {
                        "cursor": None,
                        "complete": (
                            next((self._storage_root / family).glob("*.json"), None) is None
                        ),
                    }
                    for family in _GLOBAL_ORDER_FAMILIES
                }
                has_legacy_global_order = any(
                    checkpoint["complete"] is not True
                    for checkpoint in global_order_checkpoints.values()
                )
                operational_checkpoints = {
                    family: {
                        "cursor": None,
                        "complete": (
                            next((self._storage_root / family).glob("*.json"), None) is None
                        ),
                        **(
                            {"schema_version": LEASE_OPERATIONAL_INDEX_SCHEMA}
                            if family == "leases"
                            else {}
                        ),
                    }
                    for family in _OPERATIONAL_INDEX_FAMILIES
                }
                has_legacy_operational = any(
                    checkpoint["complete"] is not True
                    for checkpoint in operational_checkpoints.values()
                )
                has_canonical_leases = (
                    next((self._storage_root / "leases").glob("*.json"), None) is not None
                )
                lease_capacity_complete = (
                    not has_canonical_leases
                    and not _lease_operational_records_present(self._storage_root)
                )
                lease_capacity_checkpoint: dict[str, object] = {
                    "complete": lease_capacity_complete,
                    "schema_version": LEASE_CAPACITY_AGGREGATE_SCHEMA,
                }
                if lease_capacity_complete:
                    empty_capacity = _new_lease_capacity_pair({}, generation=0)
                    self._write_lease_capacity_pair_unlocked(empty_capacity)
                    lease_capacity_checkpoint.update(
                        {
                            "epoch_id": empty_capacity.aggregate.epoch_id,
                            "generation": empty_capacity.aggregate.generation,
                            "record_count": 0,
                        }
                    )
                self._write_json(
                    migration_path,
                    {
                        "schema_version": INDEX_MIGRATION_SCHEMA,
                        "complete": (
                            not has_legacy_jobs
                            and not has_legacy_retention
                            and not has_legacy_global_order
                            and not has_legacy_operational
                            and lease_capacity_complete
                            and not _lease_operational_records_present(self._storage_root)
                        ),
                        "families": {
                            family: {"cursor": None, "complete": not has_legacy_jobs}
                            for family in ("jobs", "tasks", "leases", "artifacts", "progress")
                        },
                        "finalize": {"cursor": None, "complete": not has_legacy_jobs},
                        "order_families": {
                            family: {"cursor": None, "complete": not has_legacy_jobs}
                            for family in _ORDER_FAMILIES
                        },
                        "global_order_families": global_order_checkpoints,
                        "retention_families": retention_checkpoints,
                        "operational_families": operational_checkpoints,
                        "lease_operational_repair": {
                            "complete": not _lease_operational_records_present(self._storage_root),
                            "schema_version": LEASE_OPERATIONAL_INDEX_SCHEMA,
                        },
                        "lease_capacity_aggregate": lease_capacity_checkpoint,
                    },
                )
            else:
                # A torn aggregate/checkpoint pair is valid only while its exact
                # transition intent remains durable. Replay that authorization
                # before deciding the migration checkpoint itself is corrupt.
                self._recover_pending_transitions_unlocked()
                self._ensure_extended_migration_state()
            self._recover_pending_transitions_unlocked()
            self._initialized = True

    def _initialize_under_locked_core(self, locked_core: LockedCoreIdentity) -> None:
        """Pin all migration I/O to one authenticated locked core identity."""
        require_active_locked_core(locked_core)
        original_storage_root = self._storage_root
        try:
            queue_root_before = os.stat(original_storage_root)
        except OSError as exc:
            raise ConfigurationError(
                f"migration queue root identity cannot be verified: {exc}"
            ) from exc
        expected_identity = (locked_core.device, locked_core.inode)
        if (queue_root_before.st_dev, queue_root_before.st_ino) != expected_identity:
            raise ConfigurationError("migration queue root does not match its core lifetime lock")
        # Pin every migration read and write to the canonical directory whose
        # inode is locked. A stable mount alias remains accepted, while an
        # in-flight alias retarget can never redirect writes to an unlocked root.
        self.root = logical_filesystem_path(locked_core.root)
        self._storage_root = internal_filesystem_path(
            locked_core.root,
            force_extended=True,
        )
        self._lock = _FairBoundedFileLock(
            str(self._storage_root / ".lock"),
            timeout=self._lock_timeout_seconds,
        )
        self._migration_lifetime_guarded = True
        try:
            self.initialize(migrate_legacy_output=True)
        finally:
            self._migration_lifetime_guarded = False
        try:
            queue_root_after = os.stat(original_storage_root)
        except OSError as exc:
            raise ConfigurationError(f"migration queue root identity changed: {exc}") from exc
        if (queue_root_after.st_dev, queue_root_after.st_ino) != expected_identity:
            raise ConfigurationError("migration queue root identity changed while locked")

    def reconcile_pending_transitions(self) -> None:
        """Replay bounded write-ahead transitions left by another process."""
        self.initialize()
        with self._lock:
            self._recover_pending_transitions_unlocked()

    def index_migration_status(self) -> dict[str, object]:
        """Return the crash-safe v0.9 queue-index migration checkpoint."""
        self.initialize()
        return self._read_index_migration_state()

    def migrate_indexes_batch(self, *, batch_size: int = 500) -> dict[str, object]:
        """Migrate at most one bounded record batch from the v0.9 flat layout."""
        if batch_size < 1 or batch_size > 10_000:
            raise ValueError("index migration batch_size must be between 1 and 10000")
        self.initialize()
        with self._lock:
            state = self._read_index_migration_state()
            if state.get("complete") is True:
                return state
            raw_families = state.get("families")
            if not isinstance(raw_families, dict):
                raise QueueConflictError("index migration families are invalid")
            families = cast(dict[str, object], raw_families)
            model_by_family: dict[str, type[BaseModel]] = {
                "jobs": RelayJob,
                "tasks": RelayTask,
                "leases": Lease,
                "artifacts": ArtifactRef,
                "progress": ProgressRecord,
            }
            for family, model in model_by_family.items():
                raw_checkpoint = families.get(family)
                if not isinstance(raw_checkpoint, dict):
                    raise QueueConflictError(f"index migration checkpoint is invalid: {family}")
                checkpoint = cast(dict[str, object], raw_checkpoint)
                if checkpoint.get("complete") is True:
                    continue
                cursor = checkpoint.get("cursor")
                if cursor is not None and not isinstance(cursor, str):
                    raise QueueConflictError(f"index migration cursor is invalid: {family}")
                paths, has_more = _migration_batch_paths(
                    self._storage_root / family,
                    cursor=cursor,
                    limit=batch_size,
                )
                for path in paths:
                    record = self._read_json_file(path, model)
                    self._migrate_record_unlocked(family, record)
                if paths:
                    checkpoint["cursor"] = paths[-1].name
                checkpoint["complete"] = not has_more
                self._write_index_migration_state(state)
                return state
            raw_order_families = state.get("order_families")
            if not isinstance(raw_order_families, dict):
                raise QueueConflictError("order-index migration families are invalid")
            order_families = cast(dict[str, object], raw_order_families)
            order_models: dict[str, type[BaseModel]] = {
                "tasks": RelayTask,
                "artifacts": ArtifactRef,
                "progress": ProgressRecord,
            }
            for family, model in order_models.items():
                raw_checkpoint = order_families.get(family)
                if not isinstance(raw_checkpoint, dict):
                    raise QueueConflictError(
                        f"order-index migration checkpoint is invalid: {family}"
                    )
                checkpoint = cast(dict[str, object], raw_checkpoint)
                if checkpoint.get("complete") is True:
                    continue
                cursor = checkpoint.get("cursor")
                if cursor is not None and not isinstance(cursor, str):
                    raise QueueConflictError(f"order-index migration cursor is invalid: {family}")
                paths, has_more = _migration_batch_paths(
                    self._storage_root / family,
                    cursor=cursor,
                    limit=batch_size,
                )
                for path in paths:
                    record = self._read_json_file(path, model)
                    self._migrate_order_record_unlocked(family, record)
                if paths:
                    checkpoint["cursor"] = paths[-1].name
                checkpoint["complete"] = not has_more
                self._write_index_migration_state(state)
                return state
            raw_global_order_families = state.get("global_order_families")
            if not isinstance(raw_global_order_families, dict):
                raise QueueConflictError("global-order migration families are invalid")
            global_order_families = cast(dict[str, object], raw_global_order_families)
            global_order_models: dict[str, tuple[type[BaseModel], str]] = {
                "endpoints": (EndpointRegistration, "endpoint_id"),
                "jobs": (RelayJob, "job_id"),
                "gateway_sessions": (GatewaySession, "session_id"),
                "monitor_rules": (MonitorRule, "rule_id"),
            }
            for family, (model, identity_field) in global_order_models.items():
                raw_checkpoint = global_order_families.get(family)
                if not isinstance(raw_checkpoint, dict):
                    raise QueueConflictError(
                        f"global-order migration checkpoint is invalid: {family}"
                    )
                checkpoint = cast(dict[str, object], raw_checkpoint)
                if checkpoint.get("complete") is True:
                    continue
                cursor = checkpoint.get("cursor")
                if cursor is not None and not isinstance(cursor, str):
                    raise QueueConflictError(f"global-order migration cursor is invalid: {family}")
                paths, has_more = _migration_batch_paths(
                    self._storage_root / family,
                    cursor=cursor,
                    limit=batch_size,
                )
                for path in paths:
                    record = self._read_json_file(path, model)
                    record_id = getattr(record, identity_field, None)
                    if not isinstance(record_id, str) or not record_id:
                        raise QueueConflictError(f"global-order record identity is invalid: {path}")
                    self._ensure_global_order_entry_unlocked(family, record_id)
                if paths:
                    checkpoint["cursor"] = paths[-1].name
                checkpoint["complete"] = not has_more
                self._write_index_migration_state(state)
                return state
            raw_retention_families = state.get("retention_families")
            if not isinstance(raw_retention_families, dict):
                raise QueueConflictError("retention-index migration families are invalid")
            retention_families = cast(dict[str, object], raw_retention_families)
            retention_models: dict[str, type[BaseModel]] = {
                "jobs": RelayJob,
                "tasks": RelayTask,
                "artifacts": ArtifactRef,
                "monitor_rules": MonitorRule,
                "gateway_sessions": GatewaySession,
            }
            for family, model in retention_models.items():
                raw_checkpoint = retention_families.get(family)
                if not isinstance(raw_checkpoint, dict):
                    raise QueueConflictError(
                        f"retention-index migration checkpoint is invalid: {family}"
                    )
                checkpoint = cast(dict[str, object], raw_checkpoint)
                if checkpoint.get("complete") is True:
                    continue
                cursor = checkpoint.get("cursor")
                if cursor is not None and not isinstance(cursor, str):
                    raise QueueConflictError(
                        f"retention-index migration cursor is invalid: {family}"
                    )
                paths, has_more = _migration_batch_paths(
                    self._storage_root / family,
                    cursor=cursor,
                    limit=batch_size,
                )
                for path in paths:
                    record = self._read_json_file(path, model)
                    self._migrate_retention_record_unlocked(family, record)
                if paths:
                    checkpoint["cursor"] = paths[-1].name
                checkpoint["complete"] = not has_more
                self._write_index_migration_state(state)
                return state
            raw_lease_repair = state.get("lease_operational_repair")
            if not isinstance(raw_lease_repair, dict):
                raise QueueConflictError("lease operational-index repair checkpoint is invalid")
            lease_repair = cast(dict[str, object], raw_lease_repair)
            if lease_repair.get("complete") is not True:
                intent_path, repair_payload = self._prepare_lease_capacity_rebuild_intent_unlocked(
                    identity="migration-v2",
                    limit=MAX_LIVE_LEASE_RECORDS,
                )
                repaired = self._apply_lease_index_repair_intent_unlocked(
                    intent_path,
                    repair_payload,
                )
                lease_repair.update(
                    {
                        "complete": True,
                        "schema_version": LEASE_OPERATIONAL_INDEX_SCHEMA,
                        "record_count": repaired,
                    }
                )
                self._write_index_migration_state(state)
                return state
            raw_capacity_checkpoint = state.get("lease_capacity_aggregate")
            if not isinstance(raw_capacity_checkpoint, dict):
                raise QueueConflictError("lease capacity migration checkpoint is invalid")
            capacity_checkpoint = cast(dict[str, object], raw_capacity_checkpoint)
            if capacity_checkpoint.get("complete") is not True:
                intent_path, repair_payload = self._prepare_lease_capacity_rebuild_intent_unlocked(
                    identity="migration-capacity-v1",
                    limit=MAX_LIVE_LEASE_RECORDS,
                )
                repaired = self._apply_lease_index_repair_intent_unlocked(
                    intent_path,
                    repair_payload,
                )
                capacity = self._read_lease_capacity_aggregate_unlocked()
                capacity_checkpoint.update(
                    {
                        "complete": True,
                        "schema_version": LEASE_CAPACITY_AGGREGATE_SCHEMA,
                        "epoch_id": capacity.aggregate.epoch_id,
                        "generation": capacity.aggregate.generation,
                        "record_count": repaired,
                    }
                )
                self._write_index_migration_state(state)
                return state
            raw_operational_families = state.get("operational_families")
            if not isinstance(raw_operational_families, dict):
                raise QueueConflictError("operational-index migration families are invalid")
            operational_families = cast(dict[str, object], raw_operational_families)
            operational_models: dict[str, type[BaseModel]] = {
                "endpoints": EndpointRegistration,
                "jobs": RelayJob,
                "gateway_sessions": GatewaySession,
                "leases": Lease,
            }
            for family, model in operational_models.items():
                raw_checkpoint = operational_families.get(family)
                if not isinstance(raw_checkpoint, dict):
                    raise QueueConflictError(
                        f"operational-index migration checkpoint is invalid: {family}"
                    )
                checkpoint = cast(dict[str, object], raw_checkpoint)
                if checkpoint.get("complete") is True:
                    continue
                cursor = checkpoint.get("cursor")
                if cursor is not None and not isinstance(cursor, str):
                    raise QueueConflictError(
                        f"operational-index migration cursor is invalid: {family}"
                    )
                paths, has_more = _migration_batch_paths(
                    self._storage_root / family,
                    cursor=cursor,
                    limit=batch_size,
                )
                for path in paths:
                    record = self._read_json_file(path, model)
                    self._migrate_operational_record_unlocked(family, record)
                if paths:
                    checkpoint["cursor"] = paths[-1].name
                checkpoint["complete"] = not has_more
                self._write_index_migration_state(state)
                return state
            raw_finalize = state.get("finalize")
            if not isinstance(raw_finalize, dict):
                raise QueueConflictError("index migration finalize checkpoint is invalid")
            finalize = cast(dict[str, object], raw_finalize)
            if finalize.get("complete") is not True:
                cursor = finalize.get("cursor")
                if cursor is not None and not isinstance(cursor, str):
                    raise QueueConflictError("index migration finalize cursor is invalid")
                paths, has_more = _migration_batch_paths(
                    self._storage_root / "jobs",
                    cursor=cursor,
                    limit=batch_size,
                )
                for path in paths:
                    job = self._read_json_file(path, RelayJob)
                    self._finalize_job_index_unlocked(job.job_id)
                if paths:
                    finalize["cursor"] = paths[-1].name
                finalize["complete"] = not has_more
                self._write_index_migration_state(state)
                return state
            self._reconcile_index_migration_sources_unlocked()
            capacity = self._read_lease_capacity_aggregate_unlocked()
            raw_capacity_checkpoint = state.get("lease_capacity_aggregate")
            if not isinstance(raw_capacity_checkpoint, dict):
                raise QueueConflictError("lease capacity migration checkpoint is invalid")
            cast(dict[str, object], raw_capacity_checkpoint).update(
                {
                    "complete": True,
                    "schema_version": LEASE_CAPACITY_AGGREGATE_SCHEMA,
                    "epoch_id": capacity.aggregate.epoch_id,
                    "generation": capacity.aggregate.generation,
                    "record_count": capacity.aggregate.global_live_leases,
                }
            )
            state["complete"] = True
            self._write_index_migration_state(state)
            return state

    def repair_lease_operational_indexes(
        self,
        *,
        limit: int = MAX_LIVE_LEASE_RECORDS,
    ) -> dict[str, object]:
        """Rebuild and prune every lease operational index under one durable intent."""
        if limit < 1 or limit > MAX_LIVE_LEASE_RECORDS:
            raise ValueError(
                f"lease index repair limit must be between 1 and {MAX_LIVE_LEASE_RECORDS}"
            )
        self.initialize()
        with self._lock:
            self._recover_pending_transitions_unlocked()
            intent_path, repair_payload = self._prepare_lease_capacity_rebuild_intent_unlocked(
                identity="operator",
                limit=limit,
            )
            record_count = self._apply_lease_index_repair_intent_unlocked(
                intent_path,
                repair_payload,
            )
            capacity = self._read_lease_capacity_aggregate_unlocked()
            state = self._read_index_migration_state()
            raw_checkpoint = state.get("lease_operational_repair")
            if not isinstance(raw_checkpoint, dict):
                raise QueueConflictError("lease operational-index repair checkpoint is invalid")
            cast(dict[str, object], raw_checkpoint).update(
                {
                    "complete": True,
                    "schema_version": LEASE_OPERATIONAL_INDEX_SCHEMA,
                    "record_count": record_count,
                }
            )
            state["lease_capacity_aggregate"] = {
                "complete": True,
                "schema_version": LEASE_CAPACITY_AGGREGATE_SCHEMA,
                "epoch_id": capacity.aggregate.epoch_id,
                "generation": capacity.aggregate.generation,
                "record_count": record_count,
            }
            state["complete"] = _index_migration_components_complete(state)
            self._write_index_migration_state(state)
        return {
            "schema_version": LEASE_OPERATIONAL_INDEX_SCHEMA,
            "capacity_schema_version": LEASE_CAPACITY_AGGREGATE_SCHEMA,
            "capacity_epoch_id": capacity.aggregate.epoch_id,
            "capacity_generation": capacity.aggregate.generation,
            "record_count": record_count,
            "complete": True,
        }

    def audit_lease_capacity(
        self,
        *,
        limit: int = MAX_LIVE_LEASE_RECORDS,
    ) -> dict[str, object]:
        """Compare canonical leases, every operational index, and the aggregate."""
        if limit < 1 or limit > MAX_LIVE_LEASE_RECORDS:
            raise ValueError(
                f"lease capacity audit limit must be between 1 and {MAX_LIVE_LEASE_RECORDS}"
            )
        try:
            self.initialize()
            with self._lock:
                self._recover_pending_transitions_unlocked()
                self._require_index_migration_complete()
                return self._audit_lease_capacity_unlocked(limit=limit)
        except (OSError, QueueConflictError) as exc:
            return {
                "schema_version": LEASE_CAPACITY_AUDIT_SCHEMA,
                "valid": False,
                "scan_truncated": False,
                "result_truncated": False,
                "limit": limit,
                "checked_at": utc_now().isoformat(),
                "mismatches": [
                    {
                        "type": "audit_error",
                        "detail": bounded_error_detail(str(exc)) or type(exc).__name__,
                    }
                ],
            }

    def _audit_lease_capacity_unlocked(self, *, limit: int) -> dict[str, object]:
        indexed, canonical_counts = self._canonical_lease_capacity_records_unlocked(limit=limit)
        mismatches: list[dict[str, object]] = []
        result_truncated = False

        def mismatch(kind: str, **details: object) -> None:
            nonlocal result_truncated
            if len(mismatches) >= 100:
                result_truncated = True
                return
            mismatches.append({"type": kind, **details})

        expected_by_reference = {
            _lease_reference(identity): identity for _lease, _job, identity in indexed
        }
        expected_references = set(expected_by_reference)
        expiry_refs, expiry_truncated = self._scan_expiry_refs(limit=limit)
        identity_refs, identity_truncated = self._scan_lease_identity_refs(limit=limit)
        scan_truncated = expiry_truncated or identity_truncated
        observed_expiry_references = {
            (lease_token, identity_token) for *_, lease_token, identity_token in expiry_refs
        }
        observed_identity_references = set(identity_refs)
        for label, observed in (
            ("expiry", observed_expiry_references),
            ("identity", observed_identity_references),
        ):
            for reference in sorted(expected_references - observed):
                mismatch(
                    "missing_operational_reference",
                    index=label,
                    reference=".".join(reference),
                )
            for reference in sorted(observed - expected_references):
                mismatch(
                    "orphaned_operational_reference",
                    index=label,
                    reference=".".join(reference),
                )

        manifest_paths = self._bounded_json_record_paths(
            self._storage_root / "lease_indexes",
            limit=limit,
            label="lease operational manifest index",
        )
        observed_manifest_references: set[tuple[str, str]] = set()
        for path in manifest_paths:
            lease_token = path.stem
            identity = self._read_lease_index_identity_by_token(lease_token)
            reference = _lease_reference(identity)
            if reference in observed_manifest_references:
                mismatch(
                    "duplicate_operational_manifest",
                    lease_id=identity.lease_id,
                    reference=".".join(reference),
                )
            observed_manifest_references.add(reference)
            expected_identity = expected_by_reference.get(reference)
            if expected_identity != identity:
                mismatch(
                    "operational_manifest_mismatch",
                    lease_id=identity.lease_id,
                    reference=".".join(reference),
                )
        for reference in sorted(expected_references - observed_manifest_references):
            mismatch("missing_operational_manifest", reference=".".join(reference))

        expected_by_scope: dict[tuple[str, JobKind], set[tuple[str, str]]] = {}
        cluster_labels: dict[str, str] = {}
        expected_by_endpoint: dict[str, set[tuple[str, str]]] = {}
        endpoint_labels: dict[str, str] = {}
        for reference, identity in expected_by_reference.items():
            cluster_token = _lease_cluster_token(identity.cluster)
            cluster_labels[cluster_token] = identity.cluster
            expected_by_scope.setdefault((cluster_token, identity.job_kind), set()).add(reference)
            endpoint_token = _lease_endpoint_token(identity.endpoint_id)
            endpoint_labels[endpoint_token] = identity.endpoint_id
            expected_by_endpoint.setdefault(endpoint_token, set()).add(reference)

        observed_scope_references: dict[tuple[str, JobKind], set[tuple[str, str]]] = {}
        scope_root = self._storage_root / "leases_by_cluster_kind"
        self._require_safe_lease_index_directory(scope_root, create=False)
        scope_entries = 0
        with os.scandir(scope_root) as cluster_entries:
            for cluster_entry in cluster_entries:
                scope_entries += 1
                if scope_entries > MAX_LEASE_CAPACITY_SCOPES:
                    raise QueueConflictError("lease cluster-kind index exceeds its scope bound")
                cluster_path = Path(cluster_entry.path)
                cluster_stat = os.lstat(cluster_path)
                if (
                    not _is_short_ref_token(cluster_entry.name)
                    or not stat.S_ISDIR(cluster_stat.st_mode)
                    or _record_is_reparse(cluster_stat)
                ):
                    raise QueueConflictError(
                        f"lease cluster-kind index contains an unsafe cluster scope: {cluster_path}"
                    )
                self._require_safe_lease_index_directory(cluster_path, create=False)
                with os.scandir(cluster_path) as kind_entries:
                    for kind_entry in kind_entries:
                        scope_entries += 1
                        if scope_entries > MAX_LEASE_CAPACITY_SCOPES * 2:
                            raise QueueConflictError(
                                "lease cluster-kind index exceeds its scope bound"
                            )
                        try:
                            kind = JobKind(kind_entry.name)
                        except ValueError as exc:
                            raise QueueConflictError(
                                f"lease cluster-kind index has an invalid kind: {kind_entry.path}"
                            ) from exc
                        kind_path = Path(kind_entry.path)
                        kind_stat = os.lstat(kind_path)
                        if not stat.S_ISDIR(kind_stat.st_mode) or _record_is_reparse(kind_stat):
                            raise QueueConflictError(
                                "lease cluster-kind index contains an unsafe kind scope: "
                                f"{kind_path}"
                            )
                        references, truncated = self._scan_lease_scope_refs(
                            kind_path,
                            scope=("cluster-kind", cluster_entry.name, kind.value),
                            limit=limit,
                            label=(f"lease cluster-kind index {cluster_entry.name}/{kind.value}"),
                        )
                        scan_truncated = scan_truncated or truncated
                        observed_scope_references[(cluster_entry.name, kind)] = set(references)
        for scope in sorted(
            set(expected_by_scope) | set(observed_scope_references),
            key=lambda item: (item[0], item[1].value),
        ):
            expected = expected_by_scope.get(scope, set())
            observed = observed_scope_references.get(scope, set())
            if expected != observed:
                mismatch(
                    "cluster_kind_scope_mismatch",
                    cluster_token=scope[0],
                    cluster=cluster_labels.get(scope[0]),
                    job_kind=scope[1].value,
                    expected_count=len(expected),
                    observed_count=len(observed),
                )

        endpoint_root = self._storage_root / "leases_by_endpoint"
        self._require_safe_lease_index_directory(endpoint_root, create=False)
        observed_endpoint_tokens: set[str] = set()
        with os.scandir(endpoint_root) as endpoint_entries:
            for endpoint_entry in endpoint_entries:
                if len(observed_endpoint_tokens) >= limit:
                    scan_truncated = True
                    break
                endpoint_path = Path(endpoint_entry.path)
                endpoint_stat = os.lstat(endpoint_path)
                if (
                    not _is_short_ref_token(endpoint_entry.name)
                    or not stat.S_ISDIR(endpoint_stat.st_mode)
                    or _record_is_reparse(endpoint_stat)
                ):
                    raise QueueConflictError(
                        f"lease endpoint index contains an unsafe scope: {endpoint_path}"
                    )
                observed_endpoint_tokens.add(endpoint_entry.name)
        for endpoint_token in sorted(set(expected_by_endpoint) | observed_endpoint_tokens):
            endpoint_id = endpoint_labels.get(endpoint_token)
            if endpoint_id is None:
                mismatch("orphaned_endpoint_scope", endpoint_token=endpoint_token)
                continue
            observed, truncated = self._scan_lease_endpoint_refs(endpoint_id, limit=limit)
            scan_truncated = scan_truncated or truncated
            expected = expected_by_endpoint[endpoint_token]
            if set(observed) != expected:
                mismatch(
                    "endpoint_scope_mismatch",
                    endpoint_token=endpoint_token,
                    endpoint_id=endpoint_id,
                    expected_count=len(expected),
                    observed_count=len(observed),
                )

        aggregate_pair = self._read_lease_capacity_aggregate_unlocked()
        aggregate_counts = aggregate_pair.aggregate.cluster_kind_counts
        all_capacity_scopes = {
            (cluster_token, kind)
            for cluster_token, kind_counts in canonical_counts.items()
            for kind in kind_counts
        } | {
            (cluster_token, kind)
            for cluster_token, kind_counts in aggregate_counts.items()
            for kind in kind_counts
        }
        for cluster_token, kind in sorted(
            all_capacity_scopes,
            key=lambda item: (item[0], item[1].value),
        ):
            expected_count = canonical_counts.get(cluster_token, {}).get(kind, 0)
            observed_count = aggregate_counts.get(cluster_token, {}).get(kind, 0)
            if expected_count != observed_count:
                mismatch(
                    "aggregate_scope_mismatch",
                    cluster_token=cluster_token,
                    cluster=cluster_labels.get(cluster_token),
                    job_kind=kind.value,
                    expected_count=expected_count,
                    observed_count=observed_count,
                )
        if aggregate_pair.aggregate.global_live_leases != len(indexed):
            mismatch(
                "aggregate_global_mismatch",
                expected_count=len(indexed),
                observed_count=aggregate_pair.aggregate.global_live_leases,
            )
        return {
            "schema_version": LEASE_CAPACITY_AUDIT_SCHEMA,
            "valid": not mismatches and not scan_truncated,
            "scan_truncated": scan_truncated,
            "result_truncated": result_truncated,
            "limit": limit,
            "checked_at": utc_now().isoformat(),
            "canonical": {
                "global_live_leases": len(indexed),
                "cluster_kind_counts": _serialized_lease_capacity_counts(canonical_counts),
            },
            "operational_indexes": {
                "manifests": len(observed_manifest_references),
                "identity_references": len(observed_identity_references),
                "expiry_references": len(observed_expiry_references),
                "cluster_kind_references": sum(
                    len(references) for references in observed_scope_references.values()
                ),
                "endpoint_references": sum(
                    len(references) for references in expected_by_endpoint.values()
                ),
            },
            "aggregate": {
                "epoch_id": aggregate_pair.aggregate.epoch_id,
                "generation": aggregate_pair.aggregate.generation,
                "checkpoint_id": aggregate_pair.aggregate.checkpoint_id,
                "global_live_leases": aggregate_pair.aggregate.global_live_leases,
                "cluster_kind_counts": _serialized_lease_capacity_counts(aggregate_counts),
                "document_sha256": aggregate_pair.aggregate.document_sha256,
                "checkpoint_document_sha256": aggregate_pair.checkpoint.document_sha256,
            },
            "mismatches": mismatches,
        }

    def _apply_lease_index_repair_intent_unlocked(
        self,
        intent_path: Path,
        payload: dict[str, object],
    ) -> int:
        limit = payload.get("limit")
        if (
            isinstance(limit, bool)
            or not isinstance(limit, int)
            or limit < 1
            or limit > MAX_LIVE_LEASE_RECORDS
        ):
            raise QueueConflictError(f"invalid lease index repair intent: {intent_path}")
        indexed, counts = self._canonical_lease_capacity_records_unlocked(limit=limit)
        raw_target = payload.get("lease_capacity_rebuild")
        if raw_target is None:
            migration_state = self._read_index_migration_state()
            raw_capacity_checkpoint = migration_state.get("lease_capacity_aggregate")
            if (
                isinstance(raw_capacity_checkpoint, dict)
                and cast(dict[str, object], raw_capacity_checkpoint).get("complete") is True
            ):
                raise QueueConflictError(
                    f"lease index repair intent has no capacity target: {intent_path}"
                )
            target = _new_lease_capacity_pair(counts, generation=1)
        else:
            target = _lease_capacity_pair_from_payload(
                raw_target,
                label=f"lease index repair capacity target {intent_path}",
            )
        if (
            target.aggregate.cluster_kind_counts != counts
            or target.aggregate.global_live_leases != len(indexed)
        ):
            raise QueueConflictError(
                f"lease index repair capacity target disagrees with canonical leases: {intent_path}"
            )
        self._clear_lease_operational_indexes_unlocked()
        for lease, job, _identity in indexed:
            self._sync_lease_operational_indexes_unlocked(lease, job=job)
        self._lease_capacity_record_paths_unlocked(allow_missing=True)
        self._write_lease_capacity_pair_unlocked(target)
        restore_complete = payload.get("restore_migration_complete", False)
        if not isinstance(restore_complete, bool):
            raise QueueConflictError(
                f"lease index repair migration policy is invalid: {intent_path}"
            )
        migration_state = self._read_index_migration_state()
        migration_state["lease_operational_repair"] = {
            "complete": True,
            "schema_version": LEASE_OPERATIONAL_INDEX_SCHEMA,
            "record_count": len(indexed),
        }
        migration_state["lease_capacity_aggregate"] = {
            "complete": True,
            "schema_version": LEASE_CAPACITY_AGGREGATE_SCHEMA,
            "epoch_id": target.aggregate.epoch_id,
            "generation": target.aggregate.generation,
            "record_count": len(indexed),
        }
        if restore_complete:
            migration_state["complete"] = _index_migration_components_complete(migration_state)
        self._write_index_migration_state(migration_state)
        self._before_lease_capacity_intent_removal("lease_index_repair", intent_path)
        _unlink_durable_path(intent_path, missing_ok=True)
        return len(indexed)

    def _clear_lease_operational_indexes_unlocked(self) -> None:
        roots = tuple(
            self._storage_root / family
            for family in (
                "lease_indexes",
                "lease_identity_refs",
                "leases_by_endpoint",
                "leases_by_cluster_kind",
                "leases_by_expiry",
            )
        )
        files: list[Path] = []
        directories: list[Path] = []
        remaining = MAX_LIVE_LEASE_RECORDS * 8 + 10_000

        def inspect(directory: Path, *, depth: int) -> None:
            nonlocal remaining
            if depth > 3:
                raise QueueConflictError(
                    f"lease operational index exceeds its maximum depth: {directory}"
                )
            self._require_safe_lease_index_directory(directory, create=depth == 0)
            try:
                with os.scandir(directory) as entries:
                    for entry in entries:
                        remaining -= 1
                        if remaining < 0:
                            raise QueueConflictError(
                                "lease operational index repair exceeded its entry bound"
                            )
                        entry_path = Path(entry.path)
                        entry_stat = os.lstat(entry.path)
                        if stat.S_ISDIR(entry_stat.st_mode) and not _record_is_reparse(entry_stat):
                            inspect(entry_path, depth=depth + 1)
                            directories.append(entry_path)
                            continue
                        if (
                            not stat.S_ISREG(entry_stat.st_mode)
                            or _record_is_reparse(entry_stat)
                            or entry_stat.st_nlink != 1
                        ):
                            raise QueueConflictError(
                                f"lease operational index contains an unsafe entry: {entry_path}"
                            )
                        files.append(entry_path)
            except OSError as exc:
                raise QueueConflictError(
                    f"cannot inspect lease operational index {directory}: {exc}"
                ) from exc

        for root in roots:
            inspect(root, depth=0)
        for path in files:
            _unlink_durable_path(path)
        for path in sorted(directories, key=lambda item: len(item.parts), reverse=True):
            path.rmdir()

    def register_endpoint(self, endpoint: EndpointRegistration) -> EndpointRegistration:
        """Create or refresh an endpoint registration with exact identity continuity."""
        self._require_durable_record_id(endpoint.endpoint_id, field="endpoint_id")
        self.initialize()
        self._require_index_migration_complete()
        with self._lock:
            existing = self._read_optional(
                self._storage_root / "endpoints" / f"{endpoint.endpoint_id}.json",
                EndpointRegistration,
            )
            if existing is not None:
                existing_identity = (
                    existing.role,
                    existing.cluster,
                    existing.hostname,
                    existing.pid,
                    existing.registered_at,
                )
                requested_identity = (
                    endpoint.role,
                    endpoint.cluster,
                    endpoint.hostname,
                    endpoint.pid,
                    endpoint.registered_at,
                )
                if existing_identity != requested_identity:
                    raise QueueConflictError(
                        "endpoint identity or registration generation changed before heartbeat: "
                        f"{endpoint.endpoint_id}"
                    )
                endpoint = existing.model_copy(
                    update={"last_seen_at": utc_now(), "metadata": endpoint.metadata}
                )
            self._ensure_global_order_entry_unlocked("endpoints", endpoint.endpoint_id)
            self._write(self._storage_root / "endpoints" / f"{endpoint.endpoint_id}.json", endpoint)
            self._index_fresh_endpoint_unlocked(endpoint)
        return endpoint

    def resolve_idempotent_submission(
        self,
        job: RelayJob,
    ) -> IdempotentSubmissionResolution:
        """Resolve canonical idempotency identity without repairing or writing records."""
        self._require_durable_record_id(job.job_id, field="job_id")
        _validate_new_owner_session_metadata(job.metadata)
        self.initialize()
        self._require_index_migration_complete()
        key_path = (
            self._storage_root
            / "idempotency"
            / f"{_idempotency_key_filename(job.idempotency_key)}.json"
        )
        with self._lock:
            try:
                raw = self._read_json_document(key_path)
            except FileNotFoundError:
                job = prepare_owned_jarvis_run_submission(job)
                job_digest = _job_idempotency_digest(job)
                if job.submission_digest not in {None, job_digest}:
                    raise QueueConflictError(
                        "submitted job carries a mismatched submission_digest"
                    ) from None
                return IdempotentSubmissionResolution(
                    state="new",
                    canonical_job_id=job.job_id,
                )
            if not isinstance(raw, dict):
                raise QueueConflictError(f"idempotency record is not an object: {key_path}")
            record = cast(dict[str, object], raw)
            canonical_job_id = record.get("job_id")
            recorded_digest = record.get("job_digest")
            state = record.get("state")
            if (
                not _safe_global_record_id(canonical_job_id)
                or record.get("idempotency_key") != job.idempotency_key
                or state not in {"reserved", "committed", "retired"}
            ):
                raise QueueConflictError(
                    f"idempotency key was reused with a different or invalid job payload: "
                    f"{job.idempotency_key}"
                )
            canonical_job_id = cast(str, canonical_job_id)
            job = prepare_owned_jarvis_run_submission(
                job.model_copy(update={"job_id": canonical_job_id})
            )
            job_digest = _job_idempotency_digest(job)
            if job.submission_digest not in {None, job_digest}:
                raise QueueConflictError("submitted job carries a mismatched submission_digest")
            if not _is_sha256_digest(recorded_digest) or recorded_digest != job_digest:
                raise QueueConflictError(
                    f"idempotency key was reused with a different or invalid job payload: "
                    f"{job.idempotency_key}"
                )
            submitted = job.model_copy(update={"submission_digest": job_digest})
            if state == "retired":
                retired = self._replay_retired_job(
                    submitted,
                    record,
                    job_digest=job_digest,
                )
                return IdempotentSubmissionResolution(
                    state="retired",
                    canonical_job_id=canonical_job_id,
                    existing_job=retired,
                )
            existing = self._read_optional(
                self._storage_root / "jobs" / f"{canonical_job_id}.json",
                RelayJob,
            )
            if existing is not None:
                if existing.idempotency_key != job.idempotency_key or (
                    existing.submission_digest is not None
                    and existing.submission_digest != job_digest
                ):
                    raise QueueConflictError(
                        f"idempotency target identity mismatch: {canonical_job_id}"
                    )
                return IdempotentSubmissionResolution(
                    state="existing",
                    canonical_job_id=canonical_job_id,
                    existing_job=existing,
                )
            if state == "committed":
                raise QueueConflictError(
                    f"idempotency key points to missing job: {job.idempotency_key}"
                )
            return IdempotentSubmissionResolution(
                state="reserved",
                canonical_job_id=canonical_job_id,
            )

    def submit_job(self, job: RelayJob) -> RelayJob:
        """Submit a job, returning the existing record for a repeated idempotency key."""
        self._require_durable_record_id(job.job_id, field="job_id")
        _validate_new_owner_session_metadata(job.metadata)
        self.initialize()
        self._require_index_migration_complete()
        key_path = (
            self._storage_root
            / "idempotency"
            / f"{_idempotency_key_filename(job.idempotency_key)}.json"
        )
        with self._lock:
            self._recover_pending_transitions_unlocked()
            raw_existing: object | None = None
            if key_path.exists():
                raw_existing = self._read_json_document(key_path)
                if not isinstance(raw_existing, dict):
                    raise QueueConflictError(f"idempotency record is not an object: {key_path}")
                typed_existing = cast(dict[str, object], raw_existing)
                canonical_job_id = typed_existing.get("job_id")
                if (
                    not _safe_global_record_id(canonical_job_id)
                    or typed_existing.get("idempotency_key") != job.idempotency_key
                    or typed_existing.get("state") not in {"reserved", "committed", "retired"}
                ):
                    raise QueueConflictError(
                        f"invalid idempotency record for key: {job.idempotency_key}"
                    )
                job = job.model_copy(update={"job_id": cast(str, canonical_job_id)})
            job = prepare_owned_jarvis_run_submission(job)
            job_digest = _job_idempotency_digest(job)
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
                    not _safe_global_record_id(existing_job_id)
                    or existing.get("idempotency_key") != job.idempotency_key
                    or existing_state not in {"reserved", "committed", "retired"}
                ):
                    raise QueueConflictError(
                        f"invalid idempotency record for key: {job.idempotency_key}"
                    )
                if existing_digest is None and existing_state == "reserved":
                    existing["job_digest"] = job_digest
                    existing_digest = job_digest
                    self._write_json(key_path, existing)
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
                existing_job = self._read_optional(
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
                    self._ensure_global_order_entry_unlocked("jobs", existing_job.job_id)
                    self._initialize_job_index_unlocked(existing_job.job_id)
                    self._ensure_artifact_use_indexes_unlocked(existing_job)
                    self._write_job_unlocked(existing_job)
                    existing_request = _scheduler_cancellation_request(existing_job)
                    if (
                        existing_request is not None
                        and existing_request.get("cancel_scheduler") is True
                    ):
                        self._ensure_scheduler_cancel_pending_unlocked(
                            existing_job,
                            requested_at=(
                                _cancellation_requested_at(existing_request)
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
                self._ensure_active_job_capacity_unlocked(job)
                job = job.model_copy(update={"job_id": existing_job_id})
            if raw_existing is None:
                self._assert_owner_session_intake_open_unlocked(job.metadata)
                self._ensure_active_job_capacity_unlocked(job)
                self._write_json(
                    key_path,
                    {
                        "state": "reserved",
                        "job_id": job.job_id,
                        "idempotency_key": job.idempotency_key,
                        "job_digest": job_digest,
                        "created_at": utc_now().isoformat(),
                    },
                )
            self._ensure_global_order_entry_unlocked("jobs", job.job_id)
            self._initialize_job_index_unlocked(job.job_id)
            self._ensure_artifact_use_indexes_unlocked(job)
            self._write_job_unlocked(job)
            self._write_json(
                key_path,
                _committed_idempotency_record(job, job_digest),
            )
            self.append_event(job.job_id, "job.queued", "Job queued", locked=True)
        return job

    def _replay_retired_job(
        self,
        submitted: RelayJob,
        idempotency_record: dict[str, object],
        *,
        job_digest: str,
    ) -> RelayJob:
        job_id = idempotency_record.get("job_id")
        if not isinstance(job_id, str) or not job_id:
            raise QueueConflictError("retired idempotency record has no job_id")
        tombstone = self._read_optional(
            self._storage_root / "job_tombstones" / f"{self._durable_key(job_id)}.json",
            JobTombstone,
        )
        if tombstone is None:
            raise QueueConflictError(
                f"retired idempotency record points to a missing tombstone: {job_id}"
            )
        if tombstone.job_digest != job_digest or tombstone.idempotency_key != (
            submitted.idempotency_key
        ):
            raise QueueConflictError(f"retired idempotency identity mismatch: {job_id}")
        metadata = dict(submitted.metadata)
        metadata["retired_job"] = {
            "schema_version": tombstone.schema_version,
            "phase": tombstone.phase.value,
            "gc_started_at": tombstone.gc_started_at.isoformat(),
        }
        return submitted.model_copy(
            update={
                "job_id": tombstone.job_id,
                "cluster": tombstone.cluster,
                "kind": tombstone.kind,
                "state": tombstone.final_state,
                "created_at": tombstone.created_at,
                "updated_at": tombstone.updated_at,
                "attempts": tombstone.attempts,
                "last_error": tombstone.last_error,
                "leased_by": None,
                "metadata": metadata,
            }
        )

    def get_job(self, job_id: str) -> RelayJob:
        """Return a job by id."""
        job_id = self._require_durable_record_id(job_id, field="job_id")
        path = self._storage_root / "jobs" / f"{job_id}.json"
        job = self._read_optional(path, RelayJob)
        if job is None:
            raise NotFoundError(f"job not found: {job_id}")
        if job.job_id != job_id:
            raise QueueConflictError(f"canonical job identity mismatch: {path}")
        return job

    def get_job_tombstone(self, job_id: str) -> JobTombstone | None:
        """Return the durable terminal tombstone for a retired job, if present."""
        job_id = self._require_durable_record_id(job_id, field="job_id")
        self.initialize()
        tombstone = self._read_optional(
            self._storage_root / "job_tombstones" / f"{self._durable_key(job_id)}.json",
            JobTombstone,
        )
        if tombstone is not None and tombstone.job_id != job_id:
            raise QueueConflictError(f"canonical job tombstone identity mismatch: {job_id}")
        return tombstone

    def plan_terminal_job_gc(self, job_id: str) -> TerminalJobGcPlan:
        """Build a read-only, fail-closed terminal-job collection plan."""
        job_id = self._require_durable_record_id(job_id, field="job_id")
        self.initialize()
        tombstone = self.get_job_tombstone(job_id)
        if tombstone is not None:
            return TerminalJobGcPlan(
                job_id=job_id,
                expected_updated_at=tombstone.updated_at,
                eligible=True,
            )
        try:
            job = self.get_job(job_id)
        except NotFoundError:
            raise
        protections = self._terminal_job_gc_protections(job)
        return TerminalJobGcPlan(
            job_id=job.job_id,
            expected_updated_at=job.updated_at,
            eligible=not protections,
            protections=protections,
        )

    def collect_terminal_job(
        self,
        job_id: str,
        *,
        execute: bool = False,
        batch_size: int = 100,
        expected_updated_at: datetime | None = None,
        external_quarantine_id: str | None = None,
    ) -> TerminalJobGcResult:
        """Dry-run or advance core GC after an outer coordinator quarantines spool data."""
        job_id = self._require_durable_record_id(job_id, field="job_id")
        batch_size = validate_gc_batch_size(batch_size)
        plan = self.plan_terminal_job_gc(job_id)
        if expected_updated_at is not None and plan.expected_updated_at != expected_updated_at:
            stale = plan.model_copy(
                update={
                    "eligible": False,
                    "protections": [*plan.protections, "job_snapshot_changed"],
                }
            )
            return TerminalJobGcResult(plan=stale)
        if not execute or not plan.eligible:
            return TerminalJobGcResult(plan=plan)
        actions = 0
        with self._lock:
            tombstone = self.get_job_tombstone(job_id)
            if tombstone is None:
                if not isinstance(external_quarantine_id, str) or not external_quarantine_id:
                    blocked = plan.model_copy(
                        update={
                            "eligible": False,
                            "protections": [
                                *plan.protections,
                                "external_spool_quarantine_unconfirmed",
                            ],
                        }
                    )
                    return TerminalJobGcResult(plan=blocked, dry_run=False)
                job = self.get_job(job_id)
                current_plan = TerminalJobGcPlan(
                    job_id=job.job_id,
                    expected_updated_at=job.updated_at,
                    eligible=False,
                    protections=self._terminal_job_gc_protections(job),
                )
                current_plan = current_plan.model_copy(
                    update={"eligible": not current_plan.protections}
                )
                if (
                    not current_plan.eligible
                    or current_plan.expected_updated_at != plan.expected_updated_at
                ):
                    if current_plan.expected_updated_at != plan.expected_updated_at:
                        current_plan = current_plan.model_copy(
                            update={
                                "eligible": False,
                                "protections": [
                                    *current_plan.protections,
                                    "job_snapshot_changed",
                                ],
                            }
                        )
                    return TerminalJobGcResult(plan=current_plan, dry_run=False)
                tombstone = JobTombstone(
                    job_id=job.job_id,
                    cluster=job.cluster,
                    kind=job.kind,
                    final_state=job.state,
                    idempotency_key=job.idempotency_key,
                    job_digest=self._read_committed_job_digest(job),
                    created_at=job.created_at,
                    updated_at=job.updated_at,
                    attempts=job.attempts,
                    last_error=job.last_error,
                    external_quarantine_id=external_quarantine_id,
                )
                self._write(self._job_tombstone_path(job_id), tombstone)
                actions += 1
                self._after_gc_checkpoint(JobGcPhase.PREPARED)
                if actions >= batch_size:
                    return self._gc_result(plan, tombstone, actions)
            if tombstone.phase is JobGcPhase.PREPARED:
                self._retire_idempotency_unlocked(tombstone)
                tombstone = self._advance_tombstone(tombstone, JobGcPhase.IDEMPOTENCY_RETIRED)
                actions += 1
                self._after_gc_checkpoint(JobGcPhase.IDEMPOTENCY_RETIRED)
                if actions >= batch_size:
                    return self._gc_result(plan, tombstone, actions)
            if tombstone.phase is JobGcPhase.IDEMPOTENCY_RETIRED:
                if not tombstone.records_trash_started:
                    current_job = self._read_optional(
                        self._storage_root / "jobs" / f"{job_id}.json",
                        RelayJob,
                    )
                    if current_job is not None:
                        protections = self._terminal_job_gc_protections(current_job)
                        protections = [
                            protection
                            for protection in protections
                            if protection != "idempotency_record_ambiguous"
                        ]
                        if current_job.updated_at != tombstone.updated_at:
                            protections.append("job_snapshot_changed")
                        if protections:
                            blocked = plan.model_copy(
                                update={"eligible": False, "protections": protections}
                            )
                            return TerminalJobGcResult(
                                plan=blocked,
                                dry_run=False,
                                phase=tombstone.phase,
                                actions=actions,
                                tombstone=tombstone,
                            )
                    tombstone = tombstone.model_copy(
                        update={"records_trash_started": True, "gc_updated_at": utc_now()}
                    )
                    self._write(self._job_tombstone_path(job_id), tombstone)
                    actions += 1
                    if actions >= batch_size:
                        return self._gc_result(plan, tombstone, actions)
                moved, complete = self._trash_job_roots_unlocked(
                    tombstone,
                    limit=batch_size - actions,
                )
                actions += moved
                if complete:
                    tombstone = self._advance_tombstone(
                        tombstone,
                        JobGcPhase.RECORDS_TRASHED,
                        removed=moved,
                    )
                    self._after_gc_checkpoint(JobGcPhase.RECORDS_TRASHED)
                elif moved:
                    tombstone = self._record_gc_progress(tombstone, removed=moved)
                if actions >= batch_size or not complete:
                    return self._gc_result(plan, tombstone, actions)
            if tombstone.phase is JobGcPhase.RECORDS_TRASHED:
                processed, complete, tombstone = self._trash_job_references_unlocked(
                    tombstone,
                    limit=batch_size - actions,
                )
                actions += processed
                if complete:
                    tombstone = self._advance_tombstone(
                        tombstone,
                        JobGcPhase.REFERENCES_TRASHED,
                        removed=processed,
                    )
                    self._after_gc_checkpoint(JobGcPhase.REFERENCES_TRASHED)
                elif processed:
                    tombstone = self._record_gc_progress(tombstone, removed=processed)
                if actions >= batch_size or not complete:
                    return self._gc_result(plan, tombstone, actions)
            if tombstone.phase is JobGcPhase.REFERENCES_TRASHED:
                tombstone = self._advance_tombstone(tombstone, JobGcPhase.PURGING)
                self._after_gc_checkpoint(JobGcPhase.PURGING)
            if tombstone.phase is JobGcPhase.PURGING:
                removed, empty = _purge_tree_batch(
                    self._job_gc_trash_path(job_id),
                    limit=batch_size - actions,
                )
                actions += removed
                if empty:
                    tombstone = self._advance_tombstone(
                        tombstone,
                        JobGcPhase.COMPLETE,
                        removed=removed,
                    )
                    self._after_gc_checkpoint(JobGcPhase.COMPLETE)
                elif removed:
                    tombstone = self._record_gc_progress(tombstone, removed=removed)
            return self._gc_result(plan, tombstone, actions)

    def list_jobs(self) -> list[RelayJob]:
        """Return all jobs in durable submission order."""
        self.initialize()
        jobs = list(
            self._read_many(
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
        next_cursor: int | None = cursor
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
        self.initialize()
        with self._lock:
            self._recover_pending_transitions_unlocked()
            self._repair_active_job_index_unlocked()
            indexed_jobs, truncated = self._scan_many(
                self._storage_root / "jobs_active",
                RelayJob,
                limit=limit,
            )
            jobs = [self.get_job(indexed.job_id) for indexed in indexed_jobs]
            return sorted(jobs, key=self._job_submission_order_key_unlocked), truncated

    def active_job_capacity(self) -> dict[str, int | bool]:
        """Return explicit active-job admission capacity and current occupancy."""
        self.initialize()
        with self._lock:
            self._recover_pending_transitions_unlocked()
            count, over_capacity = _bounded_regular_json_count(
                self._storage_root / "jobs_active",
                limit=MAX_ACTIVE_JOB_RECORDS,
                label="active job index",
            )
            try:
                self._repair_active_job_index_unlocked()
            except (QueueConflictError, ValueError):
                pass
            else:
                count, over_capacity = _bounded_regular_json_count(
                    self._storage_root / "jobs_active",
                    limit=MAX_ACTIVE_JOB_RECORDS,
                    label="active job index",
                )
        return {
            "limit": MAX_ACTIVE_JOB_RECORDS,
            "used": count,
            "remaining": max(0, MAX_ACTIVE_JOB_RECORDS - count),
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
            session_generation_id = self._require_durable_record_id(
                session_generation_id,
                field="session_generation_id",
            )
        limit = validate_response_page_limit(limit)
        self.initialize()
        self._require_index_migration_complete()
        directory = self._owner_session_membership_dir(
            owner_session_id,
            session_generation_id=session_generation_id,
        )
        count, over_capacity = _bounded_regular_json_count(
            directory,
            limit=MAX_ACTIVE_JOB_RECORDS,
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
            membership = self._read_json_file(directory / name, OwnerSessionJobMembership)
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
        job_id = self._require_durable_record_id(job_id, field="job_id")
        self.initialize()
        with self._lock:
            self._recover_pending_transitions_unlocked()
            job = self.get_job(job_id)
            updated_metadata = dict(job.metadata)
            updated_metadata.update(metadata)
            if job.metadata.get("owner_session_id") is None:
                _validate_new_owner_session_metadata(updated_metadata)
            updated = job.model_copy(update={"updated_at": utc_now(), "metadata": updated_metadata})
            self._write_job_unlocked(updated)
            return updated

    def list_endpoints(self, cluster: str | None = None) -> list[EndpointRegistration]:
        """Return registered endpoints, optionally filtered by cluster."""
        self.initialize()
        endpoints = list(
            self._read_many(
                self._storage_root / "endpoints",
                EndpointRegistration,
                identity_field="endpoint_id",
            )
        )
        if cluster is not None:
            endpoints = [endpoint for endpoint in endpoints if endpoint.cluster == cluster]
        return sorted(endpoints, key=lambda endpoint: endpoint.registered_at)

    def list_endpoints_page(
        self,
        *,
        cursor: int = 1,
        limit: int = 100,
        cluster: str | None = None,
    ) -> tuple[list[EndpointRegistration], int | None, int]:
        """Read one global endpoint source window with an in-window cluster filter."""

        def matches(endpoint: EndpointRegistration) -> bool:
            return cluster is None or endpoint.cluster == cluster

        return self._read_global_order_page(
            family="endpoints",
            model=EndpointRegistration,
            identity_field="endpoint_id",
            cursor=cursor,
            limit=limit,
            predicate=matches,
        )

    def scan_endpoints(
        self,
        *,
        limit: int,
        cluster: str | None = None,
    ) -> tuple[list[EndpointRegistration], bool]:
        """Read a bounded endpoint snapshot."""
        endpoints, truncated = self._scan_many(
            self._storage_root / "endpoints",
            EndpointRegistration,
            limit=limit,
            identity_field="endpoint_id",
        )
        if cluster is not None:
            endpoints = [endpoint for endpoint in endpoints if endpoint.cluster == cluster]
        return sorted(endpoints, key=lambda endpoint: endpoint.registered_at), truncated

    def scan_fresh_endpoints(
        self,
        *,
        limit: int,
        fresh_seconds: int,
        cluster: str | None = None,
        now: datetime | None = None,
    ) -> tuple[list[EndpointRegistration], bool]:
        """Read only recent endpoint buckets, independent of endpoint history size."""
        if limit < 1 or limit > MAX_BOUNDED_SCAN_RECORDS:
            raise ValueError(
                f"endpoint scan limit must be between 1 and {MAX_BOUNDED_SCAN_RECORDS}"
            )
        if fresh_seconds < 1 or fresh_seconds > MAX_ENDPOINT_FRESH_SECONDS:
            raise ValueError(f"fresh_seconds must be between 1 and {MAX_ENDPOINT_FRESH_SECONDS}")
        self.initialize()
        self._require_index_migration_complete()
        observed_at = now or utc_now()
        cutoff = observed_at - timedelta(seconds=fresh_seconds)
        first_bucket = _endpoint_fresh_bucket(cutoff)
        last_bucket = _endpoint_fresh_bucket(observed_at)
        roots: list[Path]
        overflow = False
        if cluster is not None:
            roots = [self._storage_root / "endpoints_fresh" / _stable_ref_token(cluster)]
        else:
            roots = []
            with os.scandir(self._storage_root / "endpoints_fresh") as entries:
                for entry in entries:
                    if not entry.is_dir(follow_symlinks=False):
                        raise QueueConflictError(
                            f"fresh endpoint index contains an unsafe root: {entry.path}"
                        )
                    if len(roots) >= MAX_ENDPOINT_FRESH_CLUSTER_ROOTS:
                        overflow = True
                        break
                    roots.append(Path(entry.path))
            roots.sort(key=lambda path: path.name)
        by_id: dict[str, EndpointRegistration] = {}
        for cluster_root in roots:
            for bucket in range(last_bucket, first_bucket - 1, -1):
                remaining = limit - len(by_id)
                if remaining <= 0:
                    overflow = True
                    break
                bucket_root = cluster_root / f"{bucket:020d}"
                if not bucket_root.is_dir():
                    continue
                bucket_endpoints, truncated = self._scan_many(
                    bucket_root,
                    EndpointRegistration,
                    limit=remaining,
                )
                overflow = overflow or truncated
                for indexed_endpoint in bucket_endpoints:
                    endpoint = self.get_endpoint(indexed_endpoint.endpoint_id)
                    if endpoint is None:
                        continue
                    if endpoint.last_seen_at < cutoff:
                        continue
                    if (
                        endpoint.last_seen_at > observed_at
                        and indexed_endpoint.last_seen_at > observed_at
                    ):
                        continue
                    if cluster is not None and endpoint.cluster != cluster:
                        raise QueueConflictError(
                            f"fresh endpoint cluster index mismatch: {endpoint.endpoint_id}"
                        )
                    previous = by_id.get(endpoint.endpoint_id)
                    if previous is None or previous.last_seen_at < endpoint.last_seen_at:
                        by_id[endpoint.endpoint_id] = endpoint
            if len(by_id) >= limit:
                break
        endpoints = sorted(by_id.values(), key=lambda endpoint: endpoint.registered_at)
        return endpoints, overflow

    def get_endpoint(self, endpoint_id: str) -> EndpointRegistration | None:
        """Return one exact endpoint registration when present."""
        endpoint_id = self._require_durable_record_id(endpoint_id, field="endpoint_id")
        endpoint = self._read_optional(
            self._storage_root / "endpoints" / f"{endpoint_id}.json",
            EndpointRegistration,
        )
        if endpoint is not None and endpoint.endpoint_id != endpoint_id:
            raise QueueConflictError(f"canonical endpoint identity mismatch: {endpoint_id}")
        return endpoint

    def list_leases(self, cluster: str | None = None) -> list[Lease]:
        """Return active and expired leases, optionally filtered by job cluster."""
        self.initialize()
        with self._lock:
            self._recover_pending_transitions_unlocked()
            leases = list(
                self._read_many(
                    self._storage_root / "leases",
                    Lease,
                    identity_field="lease_id",
                )
            )
            if cluster is not None:
                matched: list[Lease] = []
                for lease in leases:
                    try:
                        job = self.get_job(lease.job_id)
                    except NotFoundError:
                        continue
                    if job.cluster == cluster:
                        matched.append(lease)
                leases = matched
            return sorted(leases, key=lambda lease: lease.acquired_at)

    def scan_leases(
        self,
        *,
        limit: int,
        cluster: str | None = None,
    ) -> tuple[list[Lease], bool]:
        """Read a bounded durable lease snapshot."""
        self.initialize()
        with self._lock:
            self._recover_pending_transitions_unlocked()
            leases, truncated = self._scan_many(
                self._storage_root / "leases",
                Lease,
                limit=limit,
                identity_field="lease_id",
            )
            if cluster is not None:
                matched: list[Lease] = []
                for lease in leases:
                    try:
                        job = self.get_job(lease.job_id)
                    except NotFoundError:
                        continue
                    if job.cluster == cluster:
                        matched.append(lease)
                leases = matched
            return sorted(leases, key=lambda lease: lease.acquired_at), truncated

    def scan_job_leases(self, job_id: str, *, limit: int) -> tuple[list[Lease], bool]:
        """Read bounded leases from the exact per-job index under writer exclusion."""
        job_id = self._require_durable_record_id(job_id, field="job_id")
        self.initialize()
        with self._lock:
            self._recover_pending_transitions_unlocked()
            directory = self._storage_root / "leases_by_job" / self._durable_key(job_id)
            if self._job_index_exists(job_id):
                leases, truncated = self._scan_many(directory, Lease, limit=limit)
                return sorted(leases, key=lambda lease: lease.acquired_at), truncated
            leases, truncated = self._scan_many(self._storage_root / "leases", Lease, limit=limit)
            return [lease for lease in leases if lease.job_id == job_id], truncated

    def _lease_index_identity(
        self,
        lease: Lease,
        *,
        job: RelayJob,
    ) -> _LeaseIndexIdentity:
        """Bind a lease to the immutable job attributes used by operational indexes."""
        if lease.job_id != job.job_id:
            raise QueueConflictError(f"lease job identity mismatch: {lease.lease_id}/{job.job_id}")
        for value, label in (
            (lease.lease_id, "lease id"),
            (lease.job_id, "lease job id"),
            (lease.endpoint_id, "lease endpoint id"),
        ):
            self._require_durable_record_id(value, field=label.replace(" ", "_"))
        return _LeaseIndexIdentity(
            lease_id=lease.lease_id,
            job_id=lease.job_id,
            endpoint_id=lease.endpoint_id,
            cluster=job.cluster,
            job_kind=job.kind,
            expires_at=lease.expires_at,
        )

    def _lease_capacity_directory(self) -> Path:
        return self._storage_root / "lease_capacity"

    def _lease_capacity_record_paths_unlocked(
        self,
        *,
        allow_missing: bool,
    ) -> dict[str, Path]:
        """Validate the fixed two-file aggregate inventory without following links."""
        directory = self._lease_capacity_directory()
        try:
            directory_stat = os.lstat(directory)
        except FileNotFoundError:
            if allow_missing:
                return {}
            raise QueueConflictError(f"lease capacity directory is missing: {directory}") from None
        if not stat.S_ISDIR(directory_stat.st_mode) or _record_is_reparse(directory_stat):
            raise QueueConflictError(f"lease capacity directory is unsafe: {directory}")
        if os.name != "nt" and hasattr(os, "geteuid") and directory_stat.st_uid != os.geteuid():
            raise QueueConflictError(f"lease capacity directory is not owned: {directory}")
        allowed = {"aggregate.json", "checkpoint.json"}
        paths: dict[str, Path] = {}
        try:
            with os.scandir(directory) as entries:
                for entry in entries:
                    if len(paths) >= 2:
                        raise QueueConflictError(
                            "lease capacity directory exceeds its fixed two-record inventory"
                        )
                    path = Path(entry.path)
                    entry_stat = os.lstat(path)
                    if entry.name not in allowed:
                        raise QueueConflictError(
                            f"lease capacity directory contains an unexpected record: {path}"
                        )
                    _validate_record_stat(entry_stat, path=path)
                    if entry_stat.st_size > MAX_LEASE_CAPACITY_RECORD_BYTES:
                        raise QueueConflictError(
                            f"lease capacity record exceeds its byte bound: {path}"
                        )
                    paths[entry.name] = path
        except QueueConflictError:
            raise
        except OSError as exc:
            raise QueueConflictError(f"cannot inspect lease capacity directory: {exc}") from exc
        if not allow_missing and set(paths) != allowed:
            missing = ", ".join(sorted(allowed - set(paths)))
            raise QueueConflictError(f"lease capacity pair is incomplete; missing {missing}")
        return paths

    def _read_lease_capacity_components_unlocked(
        self,
        *,
        allow_missing: bool,
    ) -> tuple[_LeaseCapacityAggregate | None, _LeaseCapacityCheckpoint | None]:
        paths = self._lease_capacity_record_paths_unlocked(allow_missing=allow_missing)
        aggregate_path = paths.get("aggregate.json")
        checkpoint_path = paths.get("checkpoint.json")
        aggregate = (
            None
            if aggregate_path is None
            else _lease_capacity_aggregate_from_document(
                _read_unique_json_document(aggregate_path),
                label=f"lease capacity aggregate {aggregate_path}",
            )
        )
        checkpoint = (
            None
            if checkpoint_path is None
            else _lease_capacity_checkpoint_from_document(
                _read_unique_json_document(checkpoint_path),
                label=f"lease capacity checkpoint {checkpoint_path}",
            )
        )
        return aggregate, checkpoint

    def _read_lease_capacity_aggregate_unlocked(self) -> _LeaseCapacityPair:
        """Read and mutually validate the fixed aggregate/checkpoint pair."""
        aggregate, checkpoint = self._read_lease_capacity_components_unlocked(allow_missing=False)
        if aggregate is None or checkpoint is None:
            raise QueueConflictError("lease capacity pair is incomplete")
        pair = _LeaseCapacityPair(aggregate=aggregate, checkpoint=checkpoint)
        _validate_lease_capacity_pair(pair, label="lease capacity pair")
        return pair

    def _write_lease_capacity_pair_unlocked(self, pair: _LeaseCapacityPair) -> None:
        """Atomically replace each side of a journal-protected capacity pair."""
        _validate_lease_capacity_pair(pair, label="lease capacity write")
        directory = self._lease_capacity_directory()
        self._require_safe_write_directory(directory)
        self._write_json(
            directory / "aggregate.json",
            _lease_capacity_aggregate_document(pair.aggregate),
        )
        self._after_lease_capacity_aggregate_write(pair.aggregate)
        self._write_json(
            directory / "checkpoint.json",
            _lease_capacity_checkpoint_document(pair.checkpoint),
        )
        self._after_lease_capacity_checkpoint_write(pair.checkpoint)

    def _after_lease_capacity_aggregate_write(
        self,
        _aggregate: _LeaseCapacityAggregate,
    ) -> None:
        """Fault-injection seam after the aggregate replacement."""

    def _after_lease_capacity_checkpoint_write(
        self,
        _checkpoint: _LeaseCapacityCheckpoint,
    ) -> None:
        """Fault-injection seam after the checkpoint replacement."""

    def _before_lease_capacity_intent_removal(self, _kind: str, _path: Path) -> None:
        """Fault-injection seam after convergence and before journal removal."""

    def _prepare_lease_capacity_transition_unlocked(
        self,
        *,
        scope_deltas: dict[tuple[str, JobKind], int],
        include_rollback: bool = False,
    ) -> dict[str, object]:
        """Create exact before/after generations for one lease transition."""
        before = self._read_lease_capacity_aggregate_unlocked()
        counts = {
            cluster_token: dict(kind_counts)
            for cluster_token, kind_counts in before.aggregate.cluster_kind_counts.items()
        }
        for (cluster, kind), delta in scope_deltas.items():
            if isinstance(delta, bool) or delta == 0:
                raise QueueConflictError(
                    "lease capacity transition delta must be a nonzero integer"
                )
            cluster_token = _lease_cluster_token(cluster)
            kind_counts = counts.setdefault(cluster_token, {})
            next_count = kind_counts.get(kind, 0) + delta
            if next_count < 0:
                raise QueueConflictError(
                    f"lease capacity transition underflow: {cluster}/{kind.value}"
                )
            if next_count == 0:
                kind_counts.pop(kind, None)
            else:
                kind_counts[kind] = next_count
            if not kind_counts:
                counts.pop(cluster_token, None)
        after = _new_lease_capacity_pair(
            counts,
            epoch_id=before.aggregate.epoch_id,
            generation=before.aggregate.generation + 1,
        )
        transition: dict[str, object] = {
            "before": _lease_capacity_pair_payload(before),
            "after": _lease_capacity_pair_payload(after),
        }
        if include_rollback:
            rollback = _new_lease_capacity_pair(
                before.aggregate.cluster_kind_counts,
                epoch_id=before.aggregate.epoch_id,
                generation=after.aggregate.generation + 1,
            )
            transition["rollback"] = _lease_capacity_pair_payload(rollback)
        return transition

    def _apply_lease_capacity_transition_unlocked(
        self,
        transition_value: object,
        *,
        target: Literal["after", "rollback"],
        label: str,
    ) -> _LeaseCapacityPair:
        """Converge a possibly torn pair when every component is journal-authorized."""
        if not isinstance(transition_value, dict):
            raise QueueConflictError(f"{label} has no lease capacity transition")
        transition = cast(dict[str, object], transition_value)
        allowed_fields = {"before", "after", "rollback"}
        if not {"before", "after"}.issubset(transition) or not set(transition).issubset(
            allowed_fields
        ):
            raise QueueConflictError(f"{label} lease capacity transition is invalid")
        pairs = {
            name: _lease_capacity_pair_from_payload(value, label=f"{label} {name}")
            for name, value in transition.items()
        }
        selected = pairs.get(target)
        if selected is None:
            raise QueueConflictError(f"{label} has no authorized {target} capacity generation")
        aggregates = tuple(pair.aggregate for pair in pairs.values())
        checkpoints = tuple(pair.checkpoint for pair in pairs.values())
        current_aggregate, current_checkpoint = self._read_lease_capacity_components_unlocked(
            allow_missing=True
        )
        if current_aggregate is not None and not any(
            current_aggregate == aggregate for aggregate in aggregates
        ):
            raise QueueConflictError(f"{label} found an unauthorized aggregate generation")
        if current_checkpoint is not None and not any(
            current_checkpoint == checkpoint for checkpoint in checkpoints
        ):
            raise QueueConflictError(f"{label} found an unauthorized checkpoint generation")
        if current_aggregate is None and current_checkpoint is None:
            raise QueueConflictError(f"{label} found both capacity records missing")
        self._write_lease_capacity_pair_unlocked(selected)
        return selected

    def _canonical_lease_capacity_records_unlocked(
        self,
        *,
        limit: int,
    ) -> tuple[
        list[tuple[Lease, RelayJob, _LeaseIndexIdentity]],
        dict[str, dict[JobKind, int]],
    ]:
        """Read bounded canonical leases and derive their exact aggregate scopes."""
        leases, truncated = self._scan_many(
            self._storage_root / "leases",
            Lease,
            limit=limit,
        )
        if truncated:
            raise QueueConflictError(
                f"lease capacity rebuild exceeded its safety bound of {limit} records"
            )
        indexed: list[tuple[Lease, RelayJob, _LeaseIndexIdentity]] = []
        counts: dict[str, dict[JobKind, int]] = {}
        clusters_by_token: dict[str, str] = {}
        references: set[tuple[str, str]] = set()
        lease_tokens: set[str] = set()
        for lease in leases:
            job = self._read_optional(
                self._storage_root / "jobs" / f"{lease.job_id}.json",
                RelayJob,
            )
            if job is None:
                raise QueueConflictError(
                    f"lease capacity rebuild cannot resolve job: {lease.lease_id}/{lease.job_id}"
                )
            identity = self._lease_index_identity(lease, job=job)
            reference = _lease_reference(identity)
            if reference in references or reference[0] in lease_tokens:
                raise QueueConflictError(
                    f"lease capacity rebuild found an identity collision: {lease.lease_id}"
                )
            references.add(reference)
            lease_tokens.add(reference[0])
            cluster_token = _lease_cluster_token(job.cluster)
            previous_cluster = clusters_by_token.setdefault(cluster_token, job.cluster)
            if previous_cluster != job.cluster:
                raise QueueConflictError(
                    "lease capacity rebuild found a cluster-token collision: "
                    f"{previous_cluster}/{job.cluster}"
                )
            kind_counts = counts.setdefault(cluster_token, {})
            kind_counts[job.kind] = kind_counts.get(job.kind, 0) + 1
            indexed.append((lease, job, identity))
        return indexed, _normalize_lease_capacity_counts(counts)

    def _prepare_lease_capacity_rebuild_intent_unlocked(
        self,
        *,
        identity: str,
        limit: int,
    ) -> tuple[Path, dict[str, object]]:
        """Persist a deterministic target epoch before any repair-side mutation."""
        _indexed, counts = self._canonical_lease_capacity_records_unlocked(limit=limit)
        target = _new_lease_capacity_pair(counts, generation=1)
        payload: dict[str, object] = {
            "limit": limit,
            "lease_capacity_rebuild": _lease_capacity_pair_payload(target),
            "restore_migration_complete": identity == "operator",
        }
        return (
            self._write_transition_intent_unlocked(
                "lease_index_repair",
                identity,
                payload,
            ),
            payload,
        )

    def _lease_index_path(self, lease_id: str) -> Path:
        return self._lease_index_path_from_token(_lease_index_token(lease_id))

    def _lease_index_path_from_token(self, lease_token: str) -> Path:
        return self._storage_root / "lease_indexes" / f"{lease_token}.json"

    def _lease_identity_ref_path(
        self,
        identity: _LeaseIndexIdentity,
    ) -> Path:
        lease_token, identity_token = _lease_reference(identity)
        return self._lease_identity_ref_path_from_tokens(lease_token, identity_token)

    def _lease_identity_ref_path_from_tokens(
        self,
        lease_token: str,
        identity_token: str,
    ) -> Path:
        return self._storage_root / "lease_identity_refs" / f"{lease_token}.{identity_token}.ref"

    def _lease_endpoint_directory(self, endpoint_id: str) -> Path:
        return self._lease_endpoint_directory_from_token(_lease_endpoint_token(endpoint_id))

    def _lease_endpoint_directory_from_token(self, endpoint_token: str) -> Path:
        return self._storage_root / "leases_by_endpoint" / endpoint_token

    def _lease_cluster_kind_directory(self, cluster: str, kind: JobKind) -> Path:
        return (
            self._storage_root
            / "leases_by_cluster_kind"
            / _lease_cluster_token(cluster)
            / kind.value
        )

    def _lease_endpoint_ref_path(self, identity: _LeaseIndexIdentity) -> Path:
        return self._lease_endpoint_directory(identity.endpoint_id) / _lease_scope_ref_name(
            identity,
            "endpoint",
            _lease_endpoint_token(identity.endpoint_id),
        )

    def _lease_endpoint_guard_path(self, identity: _LeaseIndexIdentity) -> Path:
        return self._lease_endpoint_ref_path(identity).with_suffix(".guard")

    def _lease_cluster_kind_ref_path(self, identity: _LeaseIndexIdentity) -> Path:
        return self._lease_cluster_kind_directory(
            identity.cluster,
            identity.job_kind,
        ) / _lease_scope_ref_name(
            identity,
            "cluster-kind",
            _lease_cluster_token(identity.cluster),
            identity.job_kind.value,
        )

    def _lease_expiry_ref_path(self, identity: _LeaseIndexIdentity) -> Path:
        return self._storage_root / "leases_by_expiry" / _lease_expiry_ref_name(identity)

    def _write_lease_index_identity_unlocked(self, identity: _LeaseIndexIdentity) -> None:
        path = self._lease_index_path(identity.lease_id)
        self._require_safe_lease_index_directory(path.parent, create=True)
        if os.path.lexists(path):
            existing = self._read_lease_index_identity_by_token(
                _lease_index_token(identity.lease_id)
            )
            if existing.lease_id != identity.lease_id:
                raise QueueConflictError(
                    f"lease operational index token collision: {identity.lease_id}"
                )
        self._write_json(
            path,
            _lease_index_document(identity),
        )

    def _read_lease_index_identity(self, lease_id: str) -> _LeaseIndexIdentity:
        identity = self._read_lease_index_identity_by_token(_lease_index_token(lease_id))
        if identity.lease_id != lease_id:
            raise QueueConflictError(
                f"lease operational index identity mismatch: {self._lease_index_path(lease_id)}"
            )
        return identity

    def _read_lease_index_identity_by_token(
        self,
        lease_token: str,
        identity_token: str | None = None,
    ) -> _LeaseIndexIdentity:
        path = self._lease_index_path_from_token(lease_token)
        self._require_safe_lease_index_directory(path.parent, create=False)
        try:
            raw = self._read_json_document(path)
        except FileNotFoundError as exc:
            raise QueueConflictError(f"lease operational index is missing: {lease_token}") from exc
        identity = _lease_index_identity_from_document(
            raw,
            label=f"lease operational index {path}",
        )
        if _lease_index_token(identity.lease_id) != lease_token:
            raise QueueConflictError(f"lease operational index identity mismatch: {path}")
        if identity_token is not None and _lease_identity_token(identity) != identity_token:
            raise QueueConflictError(f"lease operational index binding mismatch: {path}")
        return identity

    def _validate_lease_index_identity(
        self,
        lease: Lease,
        identity: _LeaseIndexIdentity,
    ) -> None:
        if (
            lease.lease_id != identity.lease_id
            or lease.job_id != identity.job_id
            or lease.endpoint_id != identity.endpoint_id
            or lease.expires_at != identity.expires_at
        ):
            raise QueueConflictError(
                f"canonical lease and operational index disagree: {lease.lease_id}"
            )

    def _sync_lease_operational_indexes_unlocked(
        self,
        lease: Lease,
        *,
        job: RelayJob,
        previous_lease: Lease | None = None,
    ) -> _LeaseIndexIdentity:
        """Converge exact endpoint, cluster-kind, and expiry refs for one lease."""
        identity = self._lease_index_identity(lease, job=job)
        previous: _LeaseIndexIdentity | None = None
        if previous_lease is not None:
            previous = self._lease_index_identity(previous_lease, job=job)
            if (
                previous.lease_id != identity.lease_id
                or previous.job_id != identity.job_id
                or previous.endpoint_id != identity.endpoint_id
            ):
                raise QueueConflictError(
                    f"lease renewal changed immutable identity: {identity.lease_id}"
                )
            for stale_path in (
                self._lease_endpoint_ref_path(previous),
                self._lease_endpoint_guard_path(previous),
                self._lease_cluster_kind_ref_path(previous),
                self._lease_expiry_ref_path(previous),
                self._lease_identity_ref_path(previous),
            ):
                if stale_path not in {
                    self._lease_endpoint_ref_path(identity),
                    self._lease_endpoint_guard_path(identity),
                    self._lease_cluster_kind_ref_path(identity),
                    self._lease_expiry_ref_path(identity),
                    self._lease_identity_ref_path(identity),
                }:
                    self._require_safe_lease_index_directory(
                        stale_path.parent,
                        create=False,
                    )
                    _unlink_durable_path(stale_path, missing_ok=True)
        self._write_lease_index_identity_unlocked(identity)
        for path in (
            self._lease_identity_ref_path(identity),
            self._lease_endpoint_ref_path(identity),
            self._lease_endpoint_guard_path(identity),
            self._lease_cluster_kind_ref_path(identity),
            self._lease_expiry_ref_path(identity),
        ):
            self._require_safe_lease_index_directory(path.parent, create=True)
            self._write_text(path, "")
        return identity

    def _delete_lease_operational_indexes_unlocked(
        self,
        identity: _LeaseIndexIdentity,
        *,
        allow_foreign_manifest: bool = False,
    ) -> None:
        index_path = self._lease_index_path(identity.lease_id)
        self._require_safe_lease_index_directory(index_path.parent, create=False)
        owns_manifest = os.path.lexists(index_path)
        if owns_manifest:
            indexed = self._read_lease_index_identity_by_token(
                _lease_index_token(identity.lease_id)
            )
            if indexed != identity:
                if not allow_foreign_manifest:
                    raise QueueConflictError(
                        f"lease operational index token is occupied: {identity.lease_id}"
                    )
                owns_manifest = False
        for path in (
            self._lease_endpoint_ref_path(identity),
            self._lease_endpoint_guard_path(identity),
            self._lease_cluster_kind_ref_path(identity),
            self._lease_expiry_ref_path(identity),
            self._lease_identity_ref_path(identity),
        ):
            self._require_safe_lease_index_directory(path.parent, create=False)
            _unlink_durable_path(path, missing_ok=True)
        endpoint_directory = self._lease_endpoint_directory(identity.endpoint_id)
        if endpoint_directory.exists():
            with os.scandir(endpoint_directory) as entries:
                endpoint_empty = next(entries, None) is None
            if endpoint_empty:
                endpoint_directory.rmdir()
        if owns_manifest and os.path.lexists(index_path):
            _unlink_durable_path(index_path)

    def _require_safe_lease_index_directory(
        self,
        directory: Path,
        *,
        create: bool,
    ) -> bool:
        try:
            relative = directory.relative_to(self._storage_root)
        except ValueError as exc:
            raise QueueConflictError(
                f"lease index directory escaped queue root: {directory}"
            ) from exc
        if not relative.parts or relative.parts[0] not in {
            "lease_indexes",
            "lease_identity_refs",
            "leases_by_endpoint",
            "leases_by_cluster_kind",
            "leases_by_expiry",
        }:
            raise QueueConflictError(f"unsupported lease index directory: {directory}")
        try:
            root_stat = os.lstat(self._storage_root)
        except FileNotFoundError as exc:
            raise QueueConflictError(f"queue root is missing: {self.root}") from exc
        if not stat.S_ISDIR(root_stat.st_mode) or _record_is_reparse(root_stat):
            raise QueueConflictError(f"queue root is unsafe: {self.root}")
        current = self._storage_root
        for part in relative.parts:
            current /= part
            try:
                current_stat = os.lstat(current)
            except FileNotFoundError:
                if not create:
                    return False
                current.mkdir()
                current_stat = os.lstat(current)
            if not stat.S_ISDIR(current_stat.st_mode) or _record_is_reparse(current_stat):
                raise QueueConflictError(f"lease index ancestry is unsafe: {current}")
        return True

    def _scan_lease_scope_refs(
        self,
        directory: Path,
        *,
        scope: tuple[str, ...],
        limit: int,
        label: str,
    ) -> tuple[list[tuple[str, str]], bool]:
        """Enumerate structurally bound zero-byte refs without opening lease JSON."""
        if limit < 1:
            raise ValueError("lease reference scan limit must be at least 1")
        try:
            directory_stat = os.lstat(directory)
        except FileNotFoundError:
            return [], False
        if not stat.S_ISDIR(directory_stat.st_mode) or _record_is_reparse(directory_stat):
            raise QueueConflictError(f"{label} is not a safe directory: {directory}")
        self._require_safe_lease_index_directory(directory, create=False)
        lease_refs: list[tuple[str, str]] = []
        try:
            with os.scandir(directory) as entries:
                for entry in entries:
                    if len(lease_refs) >= limit:
                        return sorted(lease_refs), True
                    lease_ref = _lease_reference_from_scope_ref(entry.name, *scope)
                    entry_stat = os.lstat(entry.path)
                    if (
                        lease_ref is None
                        or not stat.S_ISREG(entry_stat.st_mode)
                        or _record_is_reparse(entry_stat)
                        or entry_stat.st_size != 0
                        or entry_stat.st_nlink != 1
                    ):
                        raise QueueConflictError(
                            f"{label} contains an unsafe lease reference: {entry.path}"
                        )
                    lease_refs.append(lease_ref)
        except OSError as exc:
            raise QueueConflictError(f"cannot scan {label}: {exc}") from exc
        return sorted(lease_refs), False

    def _scan_expiry_refs(
        self,
        *,
        limit: int,
    ) -> tuple[list[_LeaseExpiryReference], bool]:
        """Enumerate bounded expiry identities entirely from validated filenames."""
        directory = self._storage_root / "leases_by_expiry"
        self._require_safe_lease_index_directory(directory, create=False)
        try:
            directory_stat = os.lstat(directory)
        except FileNotFoundError:
            return [], False
        if not stat.S_ISDIR(directory_stat.st_mode) or _record_is_reparse(directory_stat):
            raise QueueConflictError(f"lease expiry index is not a safe directory: {directory}")
        refs: list[_LeaseExpiryReference] = []
        try:
            with os.scandir(directory) as entries:
                for entry in entries:
                    if len(refs) >= limit:
                        return sorted(refs), True
                    parsed = _parse_lease_expiry_ref_name(entry.name)
                    entry_stat = os.lstat(entry.path)
                    if (
                        parsed is None
                        or not stat.S_ISREG(entry_stat.st_mode)
                        or _record_is_reparse(entry_stat)
                        or entry_stat.st_size != 0
                        or entry_stat.st_nlink != 1
                    ):
                        raise QueueConflictError(
                            f"lease expiry index contains an unsafe reference: {entry.path}"
                        )
                    refs.append(parsed)
        except OSError as exc:
            raise QueueConflictError(f"cannot scan lease expiry index: {exc}") from exc
        return sorted(refs), False

    def _scan_lease_identity_refs(
        self,
        *,
        limit: int,
    ) -> tuple[list[tuple[str, str]], bool]:
        """Enumerate bounded identity sentinels without opening manifest JSON."""
        if limit < 1:
            raise ValueError("lease identity reference scan limit must be at least 1")
        directory = self._storage_root / "lease_identity_refs"
        try:
            directory_stat = os.lstat(directory)
        except FileNotFoundError:
            return [], False
        if not stat.S_ISDIR(directory_stat.st_mode) or _record_is_reparse(directory_stat):
            raise QueueConflictError(
                f"lease identity reference index is not a safe directory: {directory}"
            )
        self._require_safe_lease_index_directory(directory, create=False)
        refs: list[tuple[str, str]] = []
        try:
            with os.scandir(directory) as entries:
                for entry in entries:
                    if len(refs) >= limit:
                        return sorted(refs), True
                    parsed = _parse_lease_identity_ref_name(entry.name)
                    entry_stat = os.lstat(entry.path)
                    if (
                        parsed is None
                        or not stat.S_ISREG(entry_stat.st_mode)
                        or _record_is_reparse(entry_stat)
                        or entry_stat.st_size != 0
                        or entry_stat.st_nlink != 1
                    ):
                        raise QueueConflictError(
                            "lease identity reference index contains an unsafe "
                            f"reference: {entry.path}"
                        )
                    refs.append(parsed)
        except OSError as exc:
            raise QueueConflictError(f"cannot scan lease identity reference index: {exc}") from exc
        return sorted(refs), False

    def _scan_lease_endpoint_refs(
        self,
        endpoint_id: str,
        *,
        limit: int,
    ) -> tuple[list[tuple[str, str]], bool]:
        """Validate redundant refs from exactly one endpoint shard."""
        if limit < 1:
            raise ValueError("lease endpoint reference scan limit must be at least 1")
        directory = self._lease_endpoint_directory(endpoint_id)
        try:
            directory_stat = os.lstat(directory)
        except FileNotFoundError:
            return [], False
        if not stat.S_ISDIR(directory_stat.st_mode) or _record_is_reparse(directory_stat):
            raise QueueConflictError(f"lease endpoint index is not a safe directory: {directory}")
        self._require_safe_lease_index_directory(directory, create=False)
        endpoint_token = _lease_endpoint_token(endpoint_id)
        references: set[tuple[str, str]] = set()
        guards: set[tuple[str, str]] = set()
        file_count = 0
        try:
            with os.scandir(directory) as entries:
                for entry in entries:
                    file_count += 1
                    if file_count > limit * 2:
                        return sorted(references), True
                    entry_stat = os.lstat(entry.path)
                    if entry.name.endswith(".guard"):
                        parsed = _lease_reference_from_scope_ref(
                            f"{entry.name[: -len('.guard')]}.ref",
                            "endpoint",
                            endpoint_token,
                        )
                        target = guards
                    else:
                        parsed = _lease_reference_from_scope_ref(
                            entry.name,
                            "endpoint",
                            endpoint_token,
                        )
                        target = references
                    if (
                        parsed is None
                        or not stat.S_ISREG(entry_stat.st_mode)
                        or _record_is_reparse(entry_stat)
                        or entry_stat.st_size != 0
                        or entry_stat.st_nlink != 1
                        or parsed in target
                    ):
                        raise QueueConflictError(
                            f"lease endpoint index contains an unsafe reference: {entry.path}"
                        )
                    target.add(parsed)
        except OSError as exc:
            raise QueueConflictError(f"cannot scan lease endpoint index: {exc}") from exc
        if references != guards:
            raise QueueConflictError(
                f"lease endpoint references and guards disagree: {endpoint_id}"
            )
        return sorted(references), False

    def _require_empty_lease_ref(
        self,
        path: Path,
        *,
        label: str,
    ) -> None:
        self._require_safe_lease_index_directory(path.parent, create=False)
        try:
            entry_stat = os.lstat(path)
        except FileNotFoundError as exc:
            raise QueueConflictError(f"{label} is missing: {path}") from exc
        if (
            not stat.S_ISREG(entry_stat.st_mode)
            or _record_is_reparse(entry_stat)
            or entry_stat.st_size != 0
            or entry_stat.st_nlink != 1
        ):
            raise QueueConflictError(f"{label} is unsafe: {path}")

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
        job_id = self._require_durable_record_id(job_id, field="job_id")
        if isinstance(leased_by, str):
            self._require_durable_record_id(leased_by, field="leased_by")
        self.initialize()
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
        job_id = self._require_durable_record_id(job_id, field="job_id")
        self.initialize()
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
        job_id = self._require_durable_record_id(job_id, field="job_id")
        self.initialize()
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

    def ensure_scheduler_cancel_pending(
        self,
        job_id: str,
        *,
        reason: str,
    ) -> SchedulerCancelPending:
        """Ensure retryable scheduler cancellation work exists for one job."""
        job_id = self._require_durable_record_id(job_id, field="job_id")
        self.initialize()
        self._require_index_migration_complete()
        with self._lock:
            job = self.get_job(job_id)
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
        job_id = self._require_durable_record_id(job_id, field="job_id")
        path = self._scheduler_cancel_record_path(
            "scheduler_cancel_pending",
            cluster,
            job_id,
        )
        record = self._read_optional(path, SchedulerCancelPending)
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
        job_id = self._require_durable_record_id(job_id, field="job_id")
        path = self._scheduler_cancel_record_path(
            "scheduler_cancel_dispositions",
            cluster,
            job_id,
        )
        record = self._read_optional(path, SchedulerCancelPending)
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
        if limit < 1 or limit > DEFAULT_EXACT_RECORD_LIMIT:
            raise ValueError(
                f"scheduler cancellation batch limit must be between 1 and "
                f"{DEFAULT_EXACT_RECORD_LIMIT}"
            )
        self.initialize()
        self._require_index_migration_complete()
        with self._lock:
            records, index_truncated = self._scan_many(
                self._storage_root / "scheduler_cancel_pending" / _stable_ref_token(cluster),
                SchedulerCancelPending,
                limit=MAX_ACTIVE_JOB_RECORDS,
            )
            active_records: list[SchedulerCancelPending] = []
            for record in records:
                completed_path = self._scheduler_cancel_record_path(
                    "scheduler_cancel_dispositions",
                    record.cluster,
                    record.job_id,
                )
                completed = self._read_optional(completed_path, SchedulerCancelPending)
                if completed is not None:
                    if not completed.complete:
                        raise QueueConflictError(
                            f"scheduler cancellation disposition is not terminal: {completed_path}"
                        )
                    _unlink_durable_path(
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
        due = [record for record in records if _scheduler_cancel_record_is_due(record, observed_at)]
        due.sort(key=_scheduler_cancel_due_sort_key)
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
    ) -> SchedulerCancelIdentityRegistration:
        """Register an identity and report whether this call created its disposition."""
        job_id = self._require_durable_record_id(job_id, field="job_id")
        self.initialize()
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
                    return SchedulerCancelIdentityRegistration(
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
            return SchedulerCancelIdentityRegistration(
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
        job_id = self._require_durable_record_id(job_id, field="job_id")
        self.initialize()
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
        job_id = self._require_durable_record_id(job_id, field="job_id")
        if not provider:
            raise ValueError("scheduler cancellation provider must not be empty")
        if not (
            MIN_SCHEDULER_CANCEL_CLAIM_LEASE_SECONDS
            <= lease_seconds
            <= MAX_SCHEDULER_CANCEL_CLAIM_LEASE_SECONDS
        ):
            raise ValueError(
                "scheduler cancellation claim lease must be between "
                f"{MIN_SCHEDULER_CANCEL_CLAIM_LEASE_SECONDS:g} and "
                f"{MAX_SCHEDULER_CANCEL_CLAIM_LEASE_SECONDS:g} seconds"
            )
        observed_at = now or utc_now()
        self.initialize()
        with self._lock:
            completed_path = self._scheduler_cancel_record_path(
                "scheduler_cancel_dispositions",
                cluster,
                job_id,
            )
            completed = self._read_optional(completed_path, SchedulerCancelPending)
            if completed is not None:
                if (
                    completed.job_id != job_id
                    or completed.cluster != cluster
                    or not completed.complete
                ):
                    raise QueueConflictError(
                        f"scheduler cancellation disposition identity mismatch: {completed_path}"
                    )
                _unlink_durable_path(
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
            record = self._read_optional(pending_path, SchedulerCancelPending)
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
        job_id = self._require_durable_record_id(job_id, field="job_id")
        claim_id = validate_durable_record_id(claim_id)
        observed_at = now or utc_now()
        self.initialize()
        with self._lock:
            completed_path = self._scheduler_cancel_record_path(
                "scheduler_cancel_dispositions",
                cluster,
                job_id,
            )
            completed = self._read_optional(completed_path, SchedulerCancelPending)
            if completed is not None:
                if (
                    completed.job_id != job_id
                    or completed.cluster != cluster
                    or not completed.complete
                ):
                    raise QueueConflictError(
                        f"scheduler cancellation disposition identity mismatch: {completed_path}"
                    )
                _unlink_durable_path(
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
            record = self._read_optional(pending_path, SchedulerCancelPending)
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
        job_id = self._require_durable_record_id(job_id, field="job_id")
        if not provider:
            raise ValueError("scheduler cancellation provider must not be empty")
        if not (
            MIN_SCHEDULER_CANCEL_CLAIM_LEASE_SECONDS
            <= lease_seconds
            <= MAX_SCHEDULER_CANCEL_CLAIM_LEASE_SECONDS
        ):
            raise ValueError(
                "scheduler cancellation confirmation claim lease must be between "
                f"{MIN_SCHEDULER_CANCEL_CLAIM_LEASE_SECONDS:g} and "
                f"{MAX_SCHEDULER_CANCEL_CLAIM_LEASE_SECONDS:g} seconds"
            )
        observed_at = now or utc_now()
        self.initialize()
        with self._lock:
            completed_path = self._scheduler_cancel_record_path(
                "scheduler_cancel_dispositions",
                cluster,
                job_id,
            )
            completed = self._read_optional(completed_path, SchedulerCancelPending)
            if completed is not None:
                if (
                    completed.job_id != job_id
                    or completed.cluster != cluster
                    or not completed.complete
                ):
                    raise QueueConflictError(
                        f"scheduler cancellation disposition identity mismatch: {completed_path}"
                    )
                _unlink_durable_path(
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
            record = self._read_optional(pending_path, SchedulerCancelPending)
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
        job_id = self._require_durable_record_id(job_id, field="job_id")
        claim_id = validate_durable_record_id(claim_id)
        observed_at = now or utc_now()
        self.initialize()
        with self._lock:
            completed_path = self._scheduler_cancel_record_path(
                "scheduler_cancel_dispositions",
                cluster,
                job_id,
            )
            completed = self._read_optional(completed_path, SchedulerCancelPending)
            if completed is not None:
                if (
                    completed.job_id != job_id
                    or completed.cluster != cluster
                    or not completed.complete
                ):
                    raise QueueConflictError(
                        f"scheduler cancellation disposition identity mismatch: {completed_path}"
                    )
                _unlink_durable_path(
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
            record = self._read_optional(pending_path, SchedulerCancelPending)
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

    def complete_scheduler_cancel_identity_scan(
        self,
        job_id: str,
        *,
        cluster: str,
        superseded: bool = False,
    ) -> SchedulerCancelPending:
        """Close pending work when no scheduler identity exists or relay state won the race."""
        job_id = self._require_durable_record_id(job_id, field="job_id")
        self.initialize()
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

    def _recover_stale_jobs_for_admission_unlocked(
        self,
        *,
        cluster: str,
        max_attempts: int,
    ) -> list[_LeaseExpiryReference] | None:
        """Recover stale work and retain an unchanged bounded expiry snapshot."""
        refs, truncated = self._scan_expiry_refs(limit=MAX_LIVE_LEASE_RECORDS)
        if truncated:
            raise QueueConflictError("lease recovery index exceeded its safety bound")
        _recovered, changed = self._recover_stale_jobs_from_expiry_refs_unlocked(
            cluster=cluster,
            max_attempts=max_attempts,
            refs=refs,
        )
        return None if changed else refs

    def acquire_next_job(
        self,
        endpoint_id: str,
        *,
        cluster: str,
        ttl_seconds: int = 300,
        max_attempts: int = 3,
        kind_concurrency: KindConcurrencyInput | None = None,
        mcp_admission_class: McpAdmissionClass | None = None,
        mcp_admission_limit: int | None = None,
    ) -> Lease | None:
        """Lease the next queued job accepted by one atomic worker lane.

        ``mcp_admission_class`` is a strict lane filter.  Workload lanes accept
        every non-MCP job plus workload-class MCP jobs; control lanes accept
        only explicitly classified MCP control queries.  The optional limit is
        checked against active durable leases while the same queue lock selects
        and leases the next job.
        """
        endpoint_id = self._require_durable_record_id(endpoint_id, field="endpoint_id")
        normalized_kind_concurrency = normalize_kind_concurrency(kind_concurrency)
        if mcp_admission_class is not None and not isinstance(  # pyright: ignore[reportUnnecessaryIsInstance]
            mcp_admission_class,
            McpAdmissionClass,
        ):
            raise ConfigurationError("worker MCP admission class is invalid")
        if mcp_admission_limit is not None:
            if mcp_admission_class is None:
                raise ConfigurationError("worker MCP admission limit requires an admission class")
            if (
                isinstance(mcp_admission_limit, bool)
                or not isinstance(mcp_admission_limit, int)  # pyright: ignore[reportUnnecessaryIsInstance]
                or mcp_admission_limit < 1
            ):
                raise ConfigurationError("worker MCP admission limit must be at least 1")
        self.initialize()
        with self._lock:
            self._recover_pending_transitions_unlocked()
            self._require_index_migration_complete()
            reusable_expiry_refs = self._recover_stale_jobs_for_admission_unlocked(
                cluster=cluster,
                max_attempts=max_attempts,
            )
            active = self._active_lease_for_endpoint(
                endpoint_id,
                expiry_refs=reusable_expiry_refs,
            )
            if active is not None:
                active_job = self.get_job(active.job_id)
                if mcp_admission_class is not None and not _job_matches_mcp_admission_class(
                    active_job,
                    mcp_admission_class,
                ):
                    raise QueueConflictError(
                        "endpoint active lease does not match its MCP admission lane: "
                        f"{endpoint_id}"
                    )
                return active
            active_counts, global_lease_total = self._lease_capacity_snapshot(
                cluster=cluster,
                expiry_refs=reusable_expiry_refs,
            )
            if global_lease_total >= MAX_LIVE_LEASE_RECORDS:
                return None
            mcp_admission_at_limit = False
            active_mcp_workload_count: int | None = None
            if mcp_admission_class is not None and mcp_admission_limit is not None:
                mcp_admission_at_limit = (
                    self._active_mcp_admission_count_unlocked(
                        cluster=cluster,
                        admission_class=mcp_admission_class,
                        expiry_refs=reusable_expiry_refs,
                    )
                    >= mcp_admission_limit
                )
            queued_jobs, _ = self._scan_many(
                self._storage_root / "jobs_queued",
                RelayJob,
                limit=MAX_ACTIVE_JOB_RECORDS,
            )
            for job in sorted(queued_jobs, key=self._job_submission_order_key_unlocked):
                if job.cluster != cluster or job.state != JobState.QUEUED:
                    continue
                if mcp_admission_class is not None and not _job_matches_mcp_admission_class(
                    job,
                    mcp_admission_class,
                ):
                    continue
                if mcp_admission_at_limit and job.kind is JobKind.MCP_CALL:
                    continue
                if self._job_has_pending_execution_cleanup_unlocked(job.cluster, job.job_id):
                    continue
                kind_limit = normalized_kind_concurrency.get(job.kind)
                active_kind_count = active_counts.get(job.kind, 0)
                if job.kind is JobKind.MCP_CALL and mcp_admission_class is not None:
                    if mcp_admission_class is McpAdmissionClass.CONTROL_QUERY:
                        # Control queries have their own explicit, atomic admission cap.
                        # A workload MCP ceiling must never consume the reserved lane.
                        kind_limit = None
                    else:
                        if active_mcp_workload_count is None:
                            active_mcp_workload_count = self._active_mcp_admission_count_unlocked(
                                cluster=cluster,
                                admission_class=McpAdmissionClass.WORKLOAD,
                                expiry_refs=reusable_expiry_refs,
                            )
                        active_kind_count = active_mcp_workload_count
                if kind_limit is not None and active_kind_count >= kind_limit:
                    continue
                return self._lease_job_unlocked(
                    job,
                    endpoint_id,
                    ttl_seconds=ttl_seconds,
                    validated_global_total=global_lease_total,
                )
        return None

    def _active_mcp_admission_count_unlocked(
        self,
        *,
        cluster: str,
        admission_class: McpAdmissionClass,
        expiry_refs: list[_LeaseExpiryReference] | None,
    ) -> int:
        """Count one MCP admission class from bounded, validated live leases."""
        if expiry_refs is None:
            expiry_refs, truncated = self._scan_expiry_refs(limit=MAX_LIVE_LEASE_RECORDS)
            if truncated:
                raise QueueConflictError("lease expiry index exceeded its safety bound")
        cluster_token = _lease_cluster_token(cluster)
        count = 0
        for (
            expires_key,
            indexed_cluster,
            job_kind,
            endpoint_token,
            job_token,
            lease_token,
            identity_token,
        ) in expiry_refs:
            if indexed_cluster != cluster_token or job_kind is not JobKind.MCP_CALL:
                continue
            identity = self._read_lease_index_identity_by_token(
                lease_token,
                identity_token,
            )
            if (
                identity.cluster != cluster
                or identity.job_kind is not JobKind.MCP_CALL
                or _lease_endpoint_token(identity.endpoint_id) != endpoint_token
                or _lease_job_token(identity.job_id) != job_token
                or _lease_expiry_key(identity.expires_at) != expires_key
            ):
                raise QueueConflictError(
                    f"lease expiry admission identity mismatch: {identity.lease_id}"
                )
            lease = self._read_optional(
                self._storage_root / "leases" / f"{identity.lease_id}.json",
                Lease,
            )
            if lease is None:
                raise QueueConflictError(
                    f"lease expiry admission index is orphaned: {identity.lease_id}"
                )
            self._validate_lease_index_identity(lease, identity)
            job = self.get_job(identity.job_id)
            if (
                job.cluster != cluster
                or job.kind is not JobKind.MCP_CALL
                or job.leased_by != identity.endpoint_id
            ):
                raise QueueConflictError(
                    f"active MCP admission lease changed job identity: {identity.lease_id}"
                )
            if _job_matches_mcp_admission_class(job, admission_class):
                count += 1
        return count

    def acquire_job(
        self,
        job_id: str,
        endpoint_id: str,
        *,
        cluster: str,
        ttl_seconds: int = 300,
        max_attempts: int = 3,
        kind_concurrency: KindConcurrencyInput | None = None,
    ) -> Lease | None:
        """Atomically lease one exact queued job when its kind has capacity.

        Unlike :meth:`acquire_next_job`, this method never leases a different
        operator or validation workload while attempting an exact admission.
        """
        job_id = self._require_durable_record_id(job_id, field="job_id")
        endpoint_id = self._require_durable_record_id(endpoint_id, field="endpoint_id")
        normalized_kind_concurrency = normalize_kind_concurrency(kind_concurrency)
        self.initialize()
        with self._lock:
            self._recover_pending_transitions_unlocked()
            self._require_index_migration_complete()
            reusable_expiry_refs = self._recover_stale_jobs_for_admission_unlocked(
                cluster=cluster,
                max_attempts=max_attempts,
            )
            active = self._active_lease_for_endpoint(
                endpoint_id,
                expiry_refs=reusable_expiry_refs,
            )
            if active is not None:
                if active.job_id == job_id:
                    return active
                return None
            job = self.get_job(job_id)
            if job.cluster != cluster or job.state != JobState.QUEUED:
                return None
            if self._job_has_pending_execution_cleanup_unlocked(job.cluster, job.job_id):
                return None
            kind_limit = normalized_kind_concurrency.get(job.kind)
            active_counts, global_lease_total = self._lease_capacity_snapshot(
                cluster=cluster,
                expiry_refs=reusable_expiry_refs,
            )
            if global_lease_total >= MAX_LIVE_LEASE_RECORDS:
                return None
            if kind_limit is not None and active_counts.get(job.kind, 0) >= kind_limit:
                return None
            return self._lease_job_unlocked(
                job,
                endpoint_id,
                ttl_seconds=ttl_seconds,
                validated_global_total=global_lease_total,
            )

    def submit_and_acquire_job(
        self,
        job: RelayJob,
        endpoint_id: str,
        *,
        ttl_seconds: int = 300,
        max_attempts: int = 3,
        kind_concurrency: KindConcurrencyInput | None = None,
    ) -> tuple[RelayJob, Lease | None]:
        """Atomically submit and attempt an exact lease for controlled admission.

        The submitted job remains queued when its configured kind limit is
        saturated. Holding the queue lock across both operations prevents a
        worker from executing a bounded admission probe between submission and
        the exact lease decision.
        """
        self._require_durable_record_id(job.job_id, field="job_id")
        endpoint_id = self._require_durable_record_id(endpoint_id, field="endpoint_id")
        normalized_kind_concurrency = normalize_kind_concurrency(kind_concurrency)
        self.initialize()
        with self._lock:
            self._recover_pending_transitions_unlocked()
            self._require_index_migration_complete()
            submitted = self.submit_job(job)
            if submitted.cluster != job.cluster:
                raise QueueConflictError(
                    f"submitted job {submitted.job_id} changed cluster identity"
                )
            reusable_expiry_refs = self._recover_stale_jobs_for_admission_unlocked(
                cluster=submitted.cluster,
                max_attempts=max_attempts,
            )
            active = self._active_lease_for_endpoint(
                endpoint_id,
                expiry_refs=reusable_expiry_refs,
            )
            if active is not None:
                return submitted, active if active.job_id == submitted.job_id else None
            submitted = self.get_job(submitted.job_id)
            if submitted.state != JobState.QUEUED:
                return submitted, None
            if self._job_has_pending_execution_cleanup_unlocked(
                submitted.cluster,
                submitted.job_id,
            ):
                return submitted, None
            kind_limit = normalized_kind_concurrency.get(submitted.kind)
            active_counts, global_lease_total = self._lease_capacity_snapshot(
                cluster=submitted.cluster,
                expiry_refs=reusable_expiry_refs,
            )
            if global_lease_total >= MAX_LIVE_LEASE_RECORDS:
                return submitted, None
            if kind_limit is not None and active_counts.get(submitted.kind, 0) >= kind_limit:
                return submitted, None
            lease = self._lease_job_unlocked(
                submitted,
                endpoint_id,
                ttl_seconds=ttl_seconds,
                validated_global_total=global_lease_total,
            )
            return self.get_job(submitted.job_id), lease

    def _lease_job_unlocked(
        self,
        job: RelayJob,
        endpoint_id: str,
        *,
        ttl_seconds: int,
        validated_global_total: int | None = None,
    ) -> Lease:
        """Persist one lease and its job transition while the queue lock is held."""
        if validated_global_total is None:
            _counts, validated_global_total = self._lease_capacity_snapshot(cluster=job.cluster)
        if validated_global_total >= MAX_LIVE_LEASE_RECORDS:
            raise QueueConflictError(
                "active lease population reached its safety bound of "
                f"{MAX_LIVE_LEASE_RECORDS} records"
            )
        lease = Lease.new(job.job_id, endpoint_id, ttl_seconds)
        leased_job = job.model_copy(
            update={
                "state": JobState.LEASED,
                "leased_by": endpoint_id,
                "attempts": job.attempts + 1,
                "updated_at": utc_now(),
            }
        )
        capacity_transition = self._prepare_lease_capacity_transition_unlocked(
            scope_deltas={(job.cluster, job.kind): 1},
            include_rollback=True,
        )
        intent_path = self._write_transition_intent_unlocked(
            "lease_acquire",
            lease.lease_id,
            {
                "job_id": job.job_id,
                "lease": lease.model_dump(mode="json"),
                "original_job": job.model_dump(mode="json"),
                "target_job": leased_job.model_dump(mode="json"),
                "target_updated_at": leased_job.updated_at.isoformat(),
                "lease_capacity_transition": capacity_transition,
            },
        )
        self._write_job_unlocked(leased_job)
        self._write(self._storage_root / "leases" / f"{lease.lease_id}.json", lease)
        self._write(self._job_record_path("leases_by_job", job.job_id, lease.lease_id), lease)
        self._sync_lease_operational_indexes_unlocked(lease, job=leased_job)
        self._after_lease_operational_index_write(lease)
        self._apply_lease_capacity_transition_unlocked(
            capacity_transition,
            target="after",
            label=f"lease acquisition {lease.lease_id}",
        )
        self._before_lease_capacity_intent_removal("lease_acquire", intent_path)
        _unlink_durable_path(intent_path, missing_ok=True)
        self.append_event(
            job.job_id,
            "job.leased",
            f"Job leased by {endpoint_id}",
            locked=True,
            payload={"lease_id": lease.lease_id},
        )
        return lease

    def _after_lease_operational_index_write(self, _lease: Lease) -> None:
        """Fault-injection seam after every acquisition index is durable."""

    def _active_lease_counts_by_kind(self, *, cluster: str) -> dict[JobKind, int]:
        """Count structurally validated refs without opening global lease JSON."""
        counts, _global_total = self._lease_capacity_snapshot(cluster=cluster)
        return counts

    def lease_admission_capacity_snapshot(
        self,
        *,
        cluster: str,
    ) -> tuple[dict[JobKind, int], int]:
        """Return structurally validated pre-recovery lease admission counts."""
        self.initialize()
        self._require_index_migration_complete()
        with self._lock:
            self._recover_pending_transitions_unlocked()
            counts, global_total = self._lease_capacity_snapshot(cluster=cluster)
            return dict(counts), global_total

    def _lease_capacity_snapshot(
        self,
        *,
        cluster: str,
        expiry_refs: list[_LeaseExpiryReference] | None = None,
    ) -> tuple[dict[JobKind, int], int]:
        """Return O(1) journaled admission counts from two fixed records."""
        del expiry_refs
        pair = self._read_lease_capacity_aggregate_unlocked()
        counts = pair.aggregate.cluster_kind_counts.get(_lease_cluster_token(cluster), {})
        return dict(counts), pair.aggregate.global_live_leases

    def _exact_lease_capacity_snapshot(
        self,
        *,
        cluster: str,
        expiry_refs: list[_LeaseExpiryReference] | None = None,
    ) -> tuple[dict[JobKind, int], int]:
        """Audit exact expiry, identity, and cluster-kind operational indexes."""
        if expiry_refs is None:
            expiry_refs, expiry_truncated = self._scan_expiry_refs(
                limit=MAX_LIVE_LEASE_RECORDS,
            )
            if expiry_truncated:
                raise QueueConflictError(
                    "active lease population exceeded its safety bound of "
                    f"{MAX_LIVE_LEASE_RECORDS} records"
                )
        expiry_pairs = [
            (lease_token, identity_token) for *_, lease_token, identity_token in expiry_refs
        ]
        if len(set(expiry_pairs)) != len(expiry_pairs) or len(
            {lease_token for lease_token, _identity_token in expiry_pairs}
        ) != len(expiry_pairs):
            raise QueueConflictError("lease expiry index contains duplicate identities")
        identity_refs, identity_truncated = self._scan_lease_identity_refs(
            limit=MAX_LIVE_LEASE_RECORDS,
        )
        if identity_truncated:
            raise QueueConflictError(
                "active lease population exceeded its safety bound of "
                f"{MAX_LIVE_LEASE_RECORDS} records"
            )
        if set(identity_refs) != set(expiry_pairs):
            raise QueueConflictError("lease identity and expiry indexes disagree")
        cluster_token = _lease_cluster_token(cluster)
        expected_by_kind: dict[JobKind, set[tuple[str, str]]] = {kind: set() for kind in JobKind}
        for (
            _expires,
            indexed_cluster,
            kind,
            _endpoint_token,
            _job_token,
            lease_token,
            identity_token,
        ) in expiry_refs:
            if indexed_cluster == cluster_token:
                expected_by_kind[kind].add((lease_token, identity_token))
        counts: dict[JobKind, int] = {}
        total = 0
        for kind in JobKind:
            lease_refs, truncated = self._scan_lease_scope_refs(
                self._lease_cluster_kind_directory(cluster, kind),
                scope=("cluster-kind", cluster_token, kind.value),
                limit=MAX_LIVE_LEASE_RECORDS,
                label=f"lease cluster-kind index {cluster}/{kind.value}",
            )
            if truncated:
                raise QueueConflictError(
                    "active lease population exceeded its safety bound of "
                    f"{MAX_LIVE_LEASE_RECORDS} records"
                )
            observed = set(lease_refs)
            if observed != expected_by_kind[kind]:
                raise QueueConflictError(
                    f"lease cluster-kind and expiry indexes disagree: {cluster}/{kind.value}"
                )
            if observed:
                counts[kind] = len(observed)
                total += len(observed)
        if total > MAX_LIVE_LEASE_RECORDS:
            raise QueueConflictError(
                "active lease population exceeded its safety bound of "
                f"{MAX_LIVE_LEASE_RECORDS} records"
            )
        return counts, len(expiry_refs)

    def renew_lease(self, lease_id: str, *, ttl_seconds: int = 300) -> Lease | None:
        """Extend an active lease TTL."""
        lease_id = self._require_durable_record_id(lease_id, field="lease_id")
        self.initialize()
        self._require_index_migration_complete()
        with self._lock:
            self._recover_pending_transitions_unlocked()
            path = self._storage_root / "leases" / f"{lease_id}.json"
            lease = self._read_optional(path, Lease)
            if lease is None:
                return None
            if lease.lease_id != lease_id:
                raise QueueConflictError(f"canonical lease identity mismatch: {path}")
            job = self.get_job(lease.job_id)
            renewed = Lease.new(lease.job_id, lease.endpoint_id, ttl_seconds)
            renewed = renewed.model_copy(update={"lease_id": lease.lease_id})
            capacity_transition = self._prepare_lease_capacity_transition_unlocked(scope_deltas={})
            intent_path = self._write_transition_intent_unlocked(
                "lease_sync",
                renewed.lease_id,
                {
                    "lease": renewed.model_dump(mode="json"),
                    "previous_lease": lease.model_dump(mode="json"),
                    "job": job.model_dump(mode="json"),
                    "lease_capacity_transition": capacity_transition,
                },
            )
            self._write(path, renewed)
            self._write(
                self._job_record_path("leases_by_job", lease.job_id, lease.lease_id),
                renewed,
            )
            self._sync_lease_operational_indexes_unlocked(
                renewed,
                job=job,
                previous_lease=lease,
            )
            self._apply_lease_capacity_transition_unlocked(
                capacity_transition,
                target="after",
                label=f"lease renewal {renewed.lease_id}",
            )
            self._before_lease_capacity_intent_removal("lease_sync", intent_path)
            _unlink_durable_path(intent_path, missing_ok=True)
            return renewed

    def recover_stale_jobs(self, *, cluster: str, max_attempts: int = 3) -> list[RelayJob]:
        """Requeue or fail jobs whose worker lease expired."""
        self.initialize()
        self._require_index_migration_complete()
        with self._lock:
            self._recover_pending_transitions_unlocked()
            return self._recover_stale_jobs_unlocked(
                cluster=cluster,
                max_attempts=max_attempts,
            )

    def recover_stale_job(
        self,
        job_id: str,
        *,
        cluster: str,
        max_attempts: int = 3,
    ) -> RelayJob | None:
        """Recover exactly one job when its durable worker lease is expired."""
        job_id = self._require_durable_record_id(job_id, field="job_id")
        if max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        self.initialize()
        self._require_index_migration_complete()
        with self._lock:
            self._recover_pending_transitions_unlocked()
            job = self.get_job(job_id)
            if job.cluster != cluster:
                raise QueueConflictError(
                    f"job {job_id} belongs to cluster {job.cluster}, "
                    f"not requested cluster {cluster}"
                )
            job_leases, leases_truncated = self.scan_job_leases(
                job_id,
                limit=DEFAULT_EXACT_RECORD_LIMIT,
            )
            if leases_truncated:
                raise QueueConflictError(f"job lease index exceeded its safety bound: {job_id}")
            expired = [lease for lease in job_leases if lease.is_expired()]
            if not expired:
                return None
            if job.state not in {JobState.LEASED, JobState.RUNNING}:
                for lease in expired:
                    self._delete_lease_unlocked(lease, job=job)
                return None
            if self._job_has_pending_execution_cleanup_after_migration_unlocked(
                cluster,
                job.job_id,
            ):
                return None
            if self._job_has_scheduler_observation_unlocked(job):
                return None
            return self._recover_expired_leases_unlocked(
                job,
                expired,
                max_attempts=max_attempts,
            )

    def _recover_expired_leases_unlocked(
        self,
        job: RelayJob,
        expired: list[Lease],
        *,
        max_attempts: int,
    ) -> RelayJob:
        """Apply one intent-first job transition and all associated lease deletions."""
        if not expired:
            raise ValueError("stale recovery requires at least one expired lease")
        previous_state = job.state
        if previous_state not in {JobState.LEASED, JobState.RUNNING}:
            raise QueueConflictError(f"job is not recoverable from a worker lease: {job.job_id}")
        if job.attempts >= max_attempts:
            updated = job.model_copy(
                update={
                    "state": JobState.FAILED,
                    "leased_by": None,
                    "updated_at": utc_now(),
                    "last_error": "expired lease exceeded retry limit",
                }
            )
            event_type = "job.failed"
            message = "Job failed after expired lease retry limit"
        else:
            updated = job.model_copy(
                update={
                    "state": JobState.QUEUED,
                    "leased_by": None,
                    "updated_at": utc_now(),
                }
            )
            event_type = "job.requeued"
            message = "Job requeued after expired worker lease"
        lease_ids = [lease.lease_id for lease in expired]
        if len(set(lease_ids)) != len(lease_ids) or any(
            lease.job_id != job.job_id for lease in expired
        ):
            raise QueueConflictError(f"stale recovery lease identity mismatch: {job.job_id}")
        for lease in expired:
            expected = self._lease_index_identity(lease, job=job)
            indexed = self._read_lease_index_identity(lease.lease_id)
            self._validate_lease_index_identity(lease, indexed)
            if indexed != expected:
                raise QueueConflictError(
                    f"stale recovery lease index changed identity: {lease.lease_id}"
                )
            for path, label in (
                (self._lease_identity_ref_path(indexed), "lease identity reference"),
                (self._lease_endpoint_ref_path(indexed), "lease endpoint reference"),
                (self._lease_endpoint_guard_path(indexed), "lease endpoint guard"),
                (
                    self._lease_cluster_kind_ref_path(indexed),
                    "lease cluster-kind reference",
                ),
                (self._lease_expiry_ref_path(indexed), "lease expiry reference"),
            ):
                self._require_empty_lease_ref(path, label=label)
        event_dir = self._storage_root / "events" / job.job_id
        event = RelayEvent(
            job_id=job.job_id,
            seq=self._next_event_seq(job.job_id, event_dir),
            event_type=event_type,
            message=message,
            payload={
                "state": updated.state.value,
                "expired_lease_ids": lease_ids,
                "previous_state": previous_state.value,
                **(
                    {"error": "expired lease exceeded retry limit"}
                    if updated.state == JobState.FAILED
                    else {}
                ),
            },
        )
        transition_identity = _stable_ref_token(
            job.job_id,
            updated.updated_at.isoformat(),
            *sorted(lease_ids),
        )
        capacity_transition = self._prepare_lease_capacity_transition_unlocked(
            scope_deltas={(job.cluster, job.kind): -len(expired)}
        )
        intent_payload: dict[str, object] = {
            "job_id": job.job_id,
            "original_job": job.model_dump(mode="json"),
            "target_job": updated.model_dump(mode="json"),
            "leases": [lease.model_dump(mode="json") for lease in expired],
            "event": event.model_dump(mode="json"),
            "lease_capacity_transition": capacity_transition,
        }
        intent_path = self._write_transition_intent_unlocked(
            "stale_lease_recovery",
            transition_identity,
            intent_payload,
        )
        return self._apply_stale_lease_recovery_intent_unlocked(
            intent_path,
            intent_payload,
        )

    def _apply_stale_lease_recovery_intent_unlocked(
        self,
        intent_path: Path,
        payload: dict[str, object],
    ) -> RelayJob:
        """Replay an exact stale job transition and every lease/index deletion."""
        original = RelayJob.model_validate(payload.get("original_job"))
        target = RelayJob.model_validate(payload.get("target_job"))
        event = RelayEvent.model_validate(payload.get("event"))
        raw_leases = payload.get("leases")
        if not isinstance(raw_leases, list):
            raise QueueConflictError(f"invalid stale recovery leases: {intent_path}")
        leases = [Lease.model_validate(item) for item in cast(list[object], raw_leases)]
        target_updates: dict[str, object] = {
            "state": target.state,
            "leased_by": None,
            "updated_at": target.updated_at,
        }
        if target.state is JobState.FAILED:
            target_updates["last_error"] = "expired lease exceeded retry limit"
        expected_target = original.model_copy(update=target_updates)
        expected_event_type = "job.failed" if target.state is JobState.FAILED else "job.requeued"
        expected_message = (
            "Job failed after expired lease retry limit"
            if target.state is JobState.FAILED
            else "Job requeued after expired worker lease"
        )
        expected_payload: dict[str, object] = {
            "state": target.state.value,
            "expired_lease_ids": [lease.lease_id for lease in leases],
            "previous_state": original.state.value,
        }
        if target.state is JobState.FAILED:
            expected_payload["error"] = "expired lease exceeded retry limit"
        if (
            payload.get("job_id") != original.job_id
            or target.job_id != original.job_id
            or target.cluster != original.cluster
            or target.kind != original.kind
            or original.state not in {JobState.LEASED, JobState.RUNNING}
            or target.state not in {JobState.QUEUED, JobState.FAILED}
            or target.leased_by is not None
            or target != expected_target
            or event.job_id != original.job_id
            or event.event_type != expected_event_type
            or event.message != expected_message
            or event.payload != expected_payload
            or event.seq < 1
            or not leases
            or len({lease.lease_id for lease in leases}) != len(leases)
            or any(lease.job_id != original.job_id for lease in leases)
        ):
            raise QueueConflictError(f"stale recovery intent identity mismatch: {intent_path}")
        current = self._read_optional(
            self._storage_root / "jobs" / f"{original.job_id}.json",
            RelayJob,
        )
        if current != original and current != target:
            raise QueueConflictError(
                f"stale recovery job changed after intent creation: {original.job_id}"
            )
        for lease in leases:
            canonical_lease = self._read_optional(
                self._storage_root / "leases" / f"{lease.lease_id}.json",
                Lease,
            )
            if canonical_lease is not None and canonical_lease != lease:
                raise QueueConflictError(
                    f"stale recovery canonical lease changed: {lease.lease_id}"
                )
            index_path = self._lease_index_path(lease.lease_id)
            if canonical_lease is None and current == original:
                raise QueueConflictError(
                    f"stale recovery canonical lease is missing: {lease.lease_id}"
                )
            if os.path.lexists(index_path):
                expected_identity = self._lease_index_identity(lease, job=original)
                indexed_identity = self._read_lease_index_identity(lease.lease_id)
                if indexed_identity != expected_identity:
                    raise QueueConflictError(
                        f"stale recovery lease index changed: {lease.lease_id}"
                    )
            elif canonical_lease is not None:
                raise QueueConflictError(f"stale recovery lease index is missing: {lease.lease_id}")
        self._before_stale_recovery_job_write(target, leases)
        self._write_job_unlocked(target)
        self._write_recovery_event_unlocked(event)
        self._after_stale_recovery_job_write(target, leases)
        for lease in leases:
            identity = self._lease_index_identity(lease, job=original)
            self._delete_lease_unlocked(
                lease,
                job=original,
                intent_path=intent_path,
                identity=identity,
                finalize_intent=False,
            )
        capacity_transition = payload.get("lease_capacity_transition")
        if capacity_transition is not None:
            self._apply_lease_capacity_transition_unlocked(
                capacity_transition,
                target="after",
                label=f"stale lease recovery {original.job_id}",
            )
            self._before_lease_capacity_intent_removal(
                "stale_lease_recovery",
                intent_path,
            )
        elif self._lease_capacity_migration_complete_unlocked():
            raise QueueConflictError(
                f"stale recovery intent has no capacity transition: {intent_path}"
            )
        _unlink_durable_path(intent_path, missing_ok=True)
        return target

    def _write_recovery_event_unlocked(self, event: RelayEvent) -> None:
        event_path = self._storage_root / "events" / event.job_id / f"{event.seq:020d}.json"
        existing = self._read_optional(event_path, RelayEvent)
        if existing is not None and existing != event:
            raise QueueConflictError(
                f"stale recovery event sequence changed: {event.job_id}/{event.seq}"
            )
        if existing is None:
            self._write(event_path, event)
        index = self._read_job_index(event.job_id)
        if index is None:
            raise QueueConflictError(f"stale recovery job index is missing: {event.job_id}")
        if _index_integer(index, "latest_event_seq") < event.seq:
            self._update_job_index_unlocked(event.job_id, latest_event_seq=event.seq)

    def _before_stale_recovery_job_write(
        self,
        _target: RelayJob,
        _leases: list[Lease],
    ) -> None:
        """Fault-injection seam after intent persistence and before the job write."""

    def _after_stale_recovery_job_write(
        self,
        _target: RelayJob,
        _leases: list[Lease],
    ) -> None:
        """Fault-injection seam after the job/event write and before lease deletion."""

    def release_lease(self, lease_id: str) -> None:
        """Remove a lease record."""
        lease_id = self._require_durable_record_id(lease_id, field="lease_id")
        self.initialize()
        with self._lock:
            self._recover_pending_transitions_unlocked()
            path = self._storage_root / "leases" / f"{lease_id}.json"
            lease = self._read_optional(path, Lease)
            if lease is not None:
                if lease.lease_id != lease_id:
                    raise QueueConflictError(f"canonical lease identity mismatch: {path}")
                self._delete_lease_unlocked(lease)

    def _delete_lease_unlocked(
        self,
        lease: Lease,
        *,
        job: RelayJob | None = None,
        intent_path: Path | None = None,
        identity: _LeaseIndexIdentity | None = None,
        finalize_intent: bool = True,
    ) -> None:
        """Delete a canonical lease and every exact index through one replayable intent."""
        if identity is None:
            if job is None:
                try:
                    job = self.get_job(lease.job_id)
                except NotFoundError:
                    identity = self._read_lease_index_identity(lease.lease_id)
            if identity is None:
                if job is None:
                    raise QueueConflictError(
                        f"cannot resolve lease operational identity: {lease.lease_id}"
                    )
                identity = self._lease_index_identity(lease, job=job)
        self._validate_lease_index_identity(lease, identity)
        index_path = self._lease_index_path(lease.lease_id)
        if os.path.lexists(index_path):
            indexed = self._read_lease_index_identity(lease.lease_id)
            if indexed != identity:
                raise QueueConflictError(
                    f"lease operational identity changed before deletion: {lease.lease_id}"
                )
        elif intent_path is None:
            raise QueueConflictError(
                f"lease operational index is missing before deletion: {lease.lease_id}"
            )
        owned_intent = intent_path
        capacity_transition: object | None = None
        if owned_intent is None:
            capacity_transition = self._prepare_lease_capacity_transition_unlocked(
                scope_deltas={(identity.cluster, identity.job_kind): -1}
            )
            owned_intent = self._write_transition_intent_unlocked(
                "lease_delete",
                lease.lease_id,
                {
                    "job_id": lease.job_id,
                    "lease_id": lease.lease_id,
                    "lease": lease.model_dump(mode="json"),
                    "index": _lease_index_document(identity),
                    "lease_capacity_transition": capacity_transition,
                },
            )
        _unlink_durable_path(
            self._storage_root / "leases" / f"{lease.lease_id}.json",
            missing_ok=True,
        )
        self._after_lease_canonical_delete(lease)
        _unlink_durable_path(
            self._job_record_path("leases_by_job", lease.job_id, lease.lease_id),
            missing_ok=True,
        )
        self._delete_lease_operational_indexes_unlocked(identity)
        self._after_lease_index_delete(lease)
        if finalize_intent:
            if capacity_transition is None:
                raise QueueConflictError(
                    f"lease deletion has no capacity transition: {lease.lease_id}"
                )
            self._apply_lease_capacity_transition_unlocked(
                capacity_transition,
                target="after",
                label=f"lease deletion {lease.lease_id}",
            )
            self._before_lease_capacity_intent_removal("lease_delete", owned_intent)
            _unlink_durable_path(owned_intent, missing_ok=True)

    def _after_lease_canonical_delete(self, _lease: Lease) -> None:
        """Fault-injection seam after the canonical lease record is removed."""

    def _after_lease_index_delete(self, _lease: Lease) -> None:
        """Fault-injection seam after every derived lease index is removed."""

    def append_task(self, task: RelayTask) -> RelayTask:
        """Create a task record."""
        self._require_durable_record_id(task.task_id, field="task_id")
        self._require_durable_record_id(task.job_id, field="job_id")
        self.initialize()
        with self._lock:
            self._recover_pending_transitions_unlocked()
            self.get_job(task.job_id)
            sequence = self._next_job_record_sequence_unlocked(task.job_id, "task_count")
            saved = task.model_copy(update={"sequence": sequence})
            self._write_task_unlocked(saved)
            self.append_event(
                task.job_id,
                "task.queued",
                f"Task queued: {task.name}",
                locked=True,
                payload={"task_id": task.task_id, "name": task.name},
            )
        return saved

    def update_task_state(
        self,
        task_id: str,
        state: JobState,
        *,
        message: str | None = None,
        metadata: dict[str, object] | None = None,
    ) -> RelayTask:
        """Update a task state and append a task event."""
        task_id = self._require_durable_record_id(task_id, field="task_id")
        self.initialize()
        with self._lock:
            self._recover_pending_transitions_unlocked()
            path = self._storage_root / "tasks" / f"{task_id}.json"
            task = self._read_optional(path, RelayTask)
            if task is None:
                raise NotFoundError(f"task not found: {task_id}")
            if task.task_id != task_id:
                raise QueueConflictError(f"canonical task identity mismatch: {path}")
            update_metadata = dict(task.metadata)
            if metadata:
                update_metadata.update(metadata)
            updated = task.model_copy(
                update={
                    "state": state,
                    "updated_at": utc_now(),
                    "metadata": update_metadata,
                }
            )
            self._write_task_unlocked(updated)
            self.append_event(
                updated.job_id,
                f"task.{state.value}",
                message or f"Task {updated.name} {state.value}",
                locked=True,
                payload={
                    "task_id": updated.task_id,
                    "name": updated.name,
                    "state": state.value,
                },
            )
            return updated

    def update_task_metadata(
        self,
        task_id: str,
        metadata: dict[str, object],
    ) -> RelayTask:
        """Merge task metadata without changing task state or emitting a task event."""
        task_id = self._require_durable_record_id(task_id, field="task_id")
        self.initialize()
        with self._lock:
            self._recover_pending_transitions_unlocked()
            path = self._storage_root / "tasks" / f"{task_id}.json"
            task = self._read_optional(path, RelayTask)
            if task is None:
                raise NotFoundError(f"task not found: {task_id}")
            if task.task_id != task_id:
                raise QueueConflictError(f"canonical task identity mismatch: {path}")
            updated_metadata = dict(task.metadata)
            updated_metadata.update(metadata)
            updated = task.model_copy(
                update={"updated_at": utc_now(), "metadata": updated_metadata}
            )
            self._write_task_unlocked(updated)
            return updated

    def list_tasks(self, job_id: str | None = None) -> list[RelayTask]:
        """Return durable task records, optionally filtered by job id."""
        if job_id is not None:
            job_id = self._require_durable_record_id(job_id, field="job_id")
        self.initialize()
        with self._lock:
            self._recover_pending_transitions_unlocked()
            if job_id is not None and self._job_index_exists(job_id):
                tasks = list(
                    self._read_many(
                        self._storage_root / "tasks_by_job" / self._durable_key(job_id),
                        RelayTask,
                        identity_field="task_id",
                    )
                )
            else:
                tasks = list(
                    self._read_many(
                        self._storage_root / "tasks",
                        RelayTask,
                        identity_field="task_id",
                    )
                )
                if job_id is not None:
                    tasks = [task for task in tasks if task.job_id == job_id]
            return sorted(tasks, key=lambda task: task.created_at)

    def list_tasks_page(
        self,
        job_id: str,
        *,
        cursor: int = 1,
        limit: int = 100,
    ) -> tuple[list[RelayTask], int | None, int]:
        """Read one stable task page from the per-job sequence index."""
        job_id = self._require_durable_record_id(job_id, field="job_id")
        self.initialize()
        with self._lock:
            self._recover_pending_transitions_unlocked()
            return self._read_ordered_job_page(
                job_id,
                family="task",
                model=RelayTask,
                cursor=cursor,
                limit=limit,
                count_field="task_count",
            )

    def scan_job_tasks(self, job_id: str, *, limit: int) -> tuple[list[RelayTask], bool]:
        """Read bounded task records from one exact job index."""
        job_id = self._require_durable_record_id(job_id, field="job_id")
        self.initialize()
        with self._lock:
            self._recover_pending_transitions_unlocked()
            indexed = self._job_index_exists(job_id)
            directory = (
                self._storage_root / "tasks_by_job" / self._durable_key(job_id)
                if indexed
                else self._storage_root / "tasks"
            )
            tasks, truncated = self._scan_many(
                directory,
                RelayTask,
                limit=limit,
                identity_field="task_id",
            )
            if not indexed:
                tasks = [task for task in tasks if task.job_id == job_id]
            return sorted(tasks, key=lambda task: task.created_at), truncated

    def append_task_event(self, event: TaskTimelineEvent) -> TaskTimelineEvent:
        """Append a structured task timeline event with a per-task sequence."""
        self._require_durable_record_id(event.task_id, field="task_id")
        for artifact_id in event.artifact_refs:
            self._require_durable_record_id(artifact_id, field="artifact_id")
        self.initialize()
        with self._lock:
            task = self.get_task(event.task_id)
            event_dir = self._storage_root / "task_events" / event.task_id
            event_dir.mkdir(parents=True, exist_ok=True)
            seq = self._next_task_event_seq(event.task_id, event_dir)
            saved = event.model_copy(update={"seq": seq})
            self._write(event_dir / f"{seq:020d}.json", saved)
            self._write_json(
                self._storage_root / "task_event_heads" / f"{event.task_id}.json",
                {"task_id": event.task_id, "latest_seq": seq},
            )
            self.append_event(
                task.job_id,
                f"task.timeline.{event.event_type}",
                event.summary,
                locked=True,
                payload={
                    "task_id": event.task_id,
                    "task_event_seq": seq,
                    "event_type": event.event_type,
                    "label": event.label,
                    "status": event.status.value,
                },
            )
            return saved

    def drain_task_events(
        self,
        task_id: str,
        *,
        cursor: int = 1,
        limit: int = 100,
    ) -> tuple[list[TaskTimelineEvent], int]:
        """Drain structured task timeline events from a task cursor."""
        task_id = self._require_durable_record_id(task_id, field="task_id")
        cursor = validate_record_cursor(cursor, field_name="task event cursor")
        limit = validate_response_page_limit(limit, field_name="task event limit")
        self.initialize()
        self.get_task(task_id)
        event_directory = self._storage_root / "task_events" / task_id
        durable_latest_seq = _last_contiguous_sequence(event_directory)
        head_path = self._storage_root / "task_event_heads" / f"{task_id}.json"
        try:
            raw_head = self._read_json_document(head_path)
        except FileNotFoundError:
            latest_seq = durable_latest_seq
        else:
            if not isinstance(raw_head, dict):
                raise QueueConflictError(f"task event head is not an object: {head_path}")
            head = cast(dict[str, object], raw_head)
            recorded_latest_seq = head.get("latest_seq")
            if (
                head.get("task_id") != task_id
                or isinstance(recorded_latest_seq, bool)
                or not isinstance(recorded_latest_seq, int)
                or recorded_latest_seq < 0
            ):
                raise QueueConflictError(f"invalid task event head identity: {head_path}")
            if recorded_latest_seq > durable_latest_seq:
                raise QueueConflictError(f"task event head exceeds durable records: {task_id}")
            latest_seq = durable_latest_seq
        stop = min(latest_seq + 1, cursor + limit)
        drained: list[TaskTimelineEvent] = []
        for sequence in range(cursor, stop):
            event = self._read_optional(
                self._storage_root / "task_events" / task_id / f"{sequence:020d}.json",
                TaskTimelineEvent,
            )
            if event is None or event.seq != sequence or event.task_id != task_id:
                raise QueueConflictError(
                    f"task event index is missing sequence {sequence}: {task_id}"
                )
            drained.append(event)
        next_cursor = cursor if not drained else drained[-1].seq + 1
        return drained, next_cursor

    def get_task(self, task_id: str) -> RelayTask:
        """Return a task by id."""
        task_id = self._require_durable_record_id(task_id, field="task_id")
        task = self._read_optional(self._storage_root / "tasks" / f"{task_id}.json", RelayTask)
        if task is None:
            raise NotFoundError(f"task not found: {task_id}")
        if task.task_id != task_id:
            raise QueueConflictError(f"canonical task identity mismatch: {task_id}")
        return task

    def register_execution_cleanup(
        self,
        task_id: str,
        metadata: dict[str, object],
    ) -> RelayTask:
        """Atomically update a task and make its execution cleanup discoverable."""
        task_id = self._require_durable_record_id(task_id, field="task_id")
        self.initialize()
        with self._lock:
            path = self._storage_root / "tasks" / f"{task_id}.json"
            task = self._read_optional(path, RelayTask)
            if task is None:
                raise NotFoundError(f"task not found: {task_id}")
            if task.task_id != task_id:
                raise QueueConflictError(f"canonical task identity mismatch: {path}")
            updated_metadata = dict(task.metadata)
            updated_metadata.update(metadata)
            updated = task.model_copy(
                update={"updated_at": utc_now(), "metadata": updated_metadata}
            )
            cluster = updated.metadata.get("cluster")
            if not isinstance(cluster, str) or not cluster:
                raise QueueConflictError(
                    f"task {task_id} requires cluster metadata for execution cleanup"
                )
            shard = self._execution_cleanup_shard(updated.job_id)
            self._migrate_execution_cleanup_shard_unlocked(
                cluster,
                shard,
                limit=DEFAULT_EXACT_RECORD_LIMIT + 1,
            )
            pending_job_path = self._execution_cleanup_job_path(cluster, updated.job_id)
            pending_job_path.mkdir(parents=True, exist_ok=True)
            pending_stat = os.stat(pending_job_path, follow_symlinks=False)
            if not stat.S_ISDIR(pending_stat.st_mode):
                raise QueueConflictError(
                    f"execution cleanup job index is not a directory: {pending_job_path}"
                )
            self._fsync_execution_cleanup_directory(pending_job_path.parent)
            self._write(
                self._execution_cleanup_path(cluster, updated.job_id, updated.task_id),
                updated,
            )
            self._write(path, updated)
            self._write(
                self._job_record_path("tasks_by_job", updated.job_id, updated.task_id),
                updated,
            )
            if updated.sequence is not None:
                self._write_ordered_job_record("task", updated.job_id, updated.sequence, updated)
            self._sync_task_retention_indexes_unlocked(updated)
            return updated

    def acknowledge_execution_cleanup(
        self,
        job_id: str,
        task_id: str,
        *,
        metadata: dict[str, object],
    ) -> RelayTask:
        """Persist cleanup evidence before removing one durable retry marker."""
        job_id = self._require_durable_record_id(job_id, field="job_id")
        task_id = self._require_durable_record_id(task_id, field="task_id")
        self.initialize()
        with self._lock:
            path = self._storage_root / "tasks" / f"{task_id}.json"
            task = self._read_optional(path, RelayTask)
            if task is None:
                raise NotFoundError(f"task not found: {task_id}")
            if task.task_id != task_id:
                raise QueueConflictError(f"canonical task identity mismatch: {path}")
            if task.job_id != job_id:
                raise QueueConflictError(
                    f"task {task_id} belongs to job {task.job_id}, not requested job {job_id}"
                )
            updated_metadata = dict(task.metadata)
            updated_metadata.update(metadata)
            updated = task.model_copy(
                update={"updated_at": utc_now(), "metadata": updated_metadata}
            )
            cluster = updated.metadata.get("cluster")
            if not isinstance(cluster, str) or not cluster:
                raise QueueConflictError(
                    f"task {task_id} requires cluster metadata for execution cleanup"
                )
            self._write(path, updated)
            self._write(
                self._job_record_path("tasks_by_job", updated.job_id, updated.task_id),
                updated,
            )
            if updated.sequence is not None:
                self._write_ordered_job_record("task", updated.job_id, updated.sequence, updated)
            self._sync_task_retention_indexes_unlocked(updated)
            self._after_execution_cleanup_canonical_ack(updated)
            pending_path = self._execution_cleanup_path(cluster, job_id, task_id)
            _unlink_durable_path(pending_path, missing_ok=True)
            self._fsync_execution_cleanup_directory(pending_path.parent)
            try:
                pending_path.parent.rmdir()
            except FileNotFoundError:
                pass
            except OSError:
                if not any(pending_path.parent.iterdir()):
                    raise
            self._fsync_execution_cleanup_directory(pending_path.parent.parent)
            return updated

    @staticmethod
    def _after_execution_cleanup_canonical_ack(_task: RelayTask) -> None:
        """Fault-injection seam after durable acknowledgment and before marker unlink."""

    def migrate_execution_cleanup_plan(
        self,
        job_id: str,
        task_id: str,
        *,
        cleanup: dict[str, object],
    ) -> RelayTask:
        """Crash-safely upgrade an anchored legacy marker to staged cleanup."""
        job_id = self._require_durable_record_id(job_id, field="job_id")
        task_id = self._require_durable_record_id(task_id, field="task_id")
        self.initialize()
        with self._lock:
            path = self._storage_root / "tasks" / f"{task_id}.json"
            task = self._read_optional(path, RelayTask)
            if task is None:
                raise NotFoundError(f"task not found: {task_id}")
            if task.task_id != task_id:
                raise QueueConflictError(f"canonical task identity mismatch: {path}")
            if task.job_id != job_id:
                raise QueueConflictError(
                    f"task {task_id} belongs to job {task.job_id}, not requested job {job_id}"
                )
            raw_existing = task.metadata.get("execution_cleanup")
            if not isinstance(raw_existing, dict):
                raise QueueConflictError(f"task {task_id} has no legacy execution cleanup state")
            existing = cast(dict[str, object], raw_existing)
            if existing.get("schema_version") != "clio-relay.execution-cleanup.v1":
                raise QueueConflictError(f"task {task_id} execution cleanup schema is unsupported")
            if cleanup.get("schema_version") != "clio-relay.execution-cleanup.v1":
                raise QueueConflictError(f"task {task_id} migration cleanup schema is unsupported")
            raw_new_sidecars = cleanup.get("sidecars")
            if not isinstance(raw_new_sidecars, dict) or not raw_new_sidecars:
                raise QueueConflictError(f"task {task_id} migration has no staged sidecars")
            raw_existing_sidecars = existing.get("sidecars")
            if raw_existing_sidecars is not None and raw_existing_sidecars != raw_new_sidecars:
                raise QueueConflictError(
                    f"task {task_id} already has a conflicting execution cleanup plan"
                )
            cluster = task.metadata.get("cluster")
            if not isinstance(cluster, str) or not cluster:
                raise QueueConflictError(
                    f"task {task_id} requires cluster metadata for execution cleanup"
                )
            pending_path = self._execution_cleanup_path(cluster, job_id, task_id)
            if not pending_path.is_file():
                raise QueueConflictError(
                    f"execution cleanup marker disappeared before plan migration: {task_id}"
                )
            migrated_at = utc_now()
            migrated_cleanup = {
                **cleanup,
                "migrated_from_legacy": raw_existing_sidecars is None,
                "migrated_at": cleanup.get("migrated_at", migrated_at.isoformat()),
            }
            updated = task.model_copy(
                update={
                    "updated_at": migrated_at,
                    "metadata": {**task.metadata, "execution_cleanup": migrated_cleanup},
                }
            )
            # Marker first: a crash before the canonical write is repaired from
            # this exact staged record by the restart reconciliation path.
            self._write(pending_path, updated)
            self._write(path, updated)
            self._write(
                self._job_record_path("tasks_by_job", updated.job_id, updated.task_id),
                updated,
            )
            if updated.sequence is not None:
                self._write_ordered_job_record("task", updated.job_id, updated.sequence, updated)
            self._sync_task_retention_indexes_unlocked(updated)
            return updated

    def stage_execution_cleanup_sidecar(
        self,
        job_id: str,
        task_id: str,
        *,
        role: str,
        source_name: str,
        quarantine_name: str,
    ) -> RelayTask:
        """Persist one exact sidecar quarantine before acknowledging cleanup."""
        job_id = self._require_durable_record_id(job_id, field="job_id")
        task_id = self._require_durable_record_id(task_id, field="task_id")
        self.initialize()
        with self._lock:
            path = self._storage_root / "tasks" / f"{task_id}.json"
            task = self._read_optional(path, RelayTask)
            if task is None:
                raise NotFoundError(f"task not found: {task_id}")
            if task.task_id != task_id:
                raise QueueConflictError(f"canonical task identity mismatch: {path}")
            if task.job_id != job_id:
                raise QueueConflictError(
                    f"task {task_id} belongs to job {task.job_id}, not requested job {job_id}"
                )
            raw_cleanup = task.metadata.get("execution_cleanup")
            if not isinstance(raw_cleanup, dict):
                raise QueueConflictError(f"task {task_id} has no execution cleanup state")
            cleanup = cast(dict[str, object], raw_cleanup)
            if cleanup.get("schema_version") != "clio-relay.execution-cleanup.v1":
                raise QueueConflictError(f"task {task_id} execution cleanup schema is unsupported")
            raw_sidecars = cleanup.get("sidecars")
            if not isinstance(raw_sidecars, dict):
                raise QueueConflictError(f"task {task_id} has no staged execution sidecars")
            sidecars = cast(dict[str, object], raw_sidecars)
            raw_state = sidecars.get(role)
            if not isinstance(raw_state, dict):
                raise QueueConflictError(f"task {task_id} has no staged {role} execution sidecar")
            state = cast(dict[str, object], raw_state)
            if (
                state.get("schema_version") != "clio-relay.execution-sidecar-cleanup.v1"
                or state.get("source_name") != source_name
                or state.get("quarantine_name") != quarantine_name
            ):
                raise QueueConflictError(
                    f"task {task_id} {role} execution sidecar quarantine did not match"
                )
            staged_at = utc_now()
            updated_cleanup = {
                **cleanup,
                "acknowledgment_stage": "quarantining",
                "sidecars": {
                    **sidecars,
                    role: {
                        **state,
                        "stage": "quarantined",
                        "quarantined_at": staged_at.isoformat(),
                    },
                },
            }
            updated_metadata = {
                **task.metadata,
                "execution_cleanup": updated_cleanup,
            }
            updated = task.model_copy(
                update={"updated_at": staged_at, "metadata": updated_metadata}
            )
            cluster = updated.metadata.get("cluster")
            if not isinstance(cluster, str) or not cluster:
                raise QueueConflictError(
                    f"task {task_id} requires cluster metadata for execution cleanup"
                )
            pending_path = self._execution_cleanup_path(cluster, job_id, task_id)
            if not pending_path.is_file():
                if task.metadata.get("execution_sidecars_quarantined") is True:
                    return task
                raise QueueConflictError(
                    f"execution cleanup marker disappeared before sidecar staging: {task_id}"
                )
            # Canonical state is written first. A crash before the marker refresh is
            # recoverable because cleanup scans always reload the canonical task.
            self._write(path, updated)
            self._write(
                self._job_record_path("tasks_by_job", updated.job_id, updated.task_id),
                updated,
            )
            if updated.sequence is not None:
                self._write_ordered_job_record("task", updated.job_id, updated.sequence, updated)
            self._sync_task_retention_indexes_unlocked(updated)
            self._write(pending_path, updated)
            return updated

    def scan_execution_cleanup(
        self,
        *,
        cluster: str,
        limit: int,
    ) -> tuple[list[RelayTask], bool]:
        """Read one fair, bounded cleanup shard and durably advance the scan cursor."""
        self.initialize()
        cluster_key = self._label_key(cluster, domain="cluster")
        cursor_path = self._storage_root / "execution_cleanup_scan_cursors" / f"{cluster_key}.json"
        with self._lock:
            try:
                raw_cursor = self._read_json_document(cursor_path)
            except FileNotFoundError:
                raw_cursor = None
            cursor = 0
            if raw_cursor is not None:
                if not isinstance(raw_cursor, dict):
                    raise QueueConflictError(
                        f"execution cleanup cursor is not an object: {cursor_path}"
                    )
                cursor_document = cast(dict[str, object], raw_cursor)
                raw_shard = cursor_document.get("next_shard")
                if (
                    not isinstance(raw_shard, int)
                    or isinstance(raw_shard, bool)
                    or not 0 <= raw_shard < 256
                ):
                    raise QueueConflictError(f"execution cleanup cursor is invalid: {cursor_path}")
                cursor = raw_shard
            selected_shard: int | None = None
            markers: list[RelayTask] = []
            truncated = False
            for offset in range(256):
                shard = (cursor + offset) % 256
                shard_path = self._execution_cleanup_shard_path(cluster, shard)
                if not shard_path.exists():
                    continue
                shard_markers, shard_truncated = self._scan_execution_cleanup_shard_unlocked(
                    cluster,
                    shard,
                    limit=limit,
                )
                selected_shard = shard
                markers = shard_markers
                truncated = shard_truncated
                if shard_markers or shard_truncated:
                    break
            if selected_shard is not None:
                self._write_json(
                    cursor_path,
                    {
                        "cluster": cluster,
                        "next_shard": (selected_shard + 1) % 256,
                        "updated_at": utc_now().isoformat(),
                    },
                )
            other_markers = any(
                self._execution_cleanup_shard_has_pending_paths(cluster, shard)
                for shard in range(256)
                if shard != selected_shard
            )
        matching = [marker for marker in markers if marker.metadata.get("cluster") == cluster]
        has_more = truncated or other_markers
        return sorted(matching, key=lambda marker: marker.created_at), has_more

    def job_has_pending_execution_cleanup(self, job_id: str, *, cluster: str) -> bool:
        """Return whether cleanup state currently makes a queued job ineligible."""
        job_id = self._require_durable_record_id(job_id, field="job_id")
        self.initialize()
        self._require_index_migration_complete()
        with self._lock:
            self._recover_pending_transitions_unlocked()
            job = self.get_job(job_id)
            if job.cluster != cluster:
                raise QueueConflictError(
                    f"job {job_id} belongs to cluster {job.cluster}, not requested cluster "
                    f"{cluster}"
                )
            return self._job_has_pending_execution_cleanup_unlocked(cluster, job_id)

    def _scan_execution_cleanup_shard_unlocked(
        self,
        cluster: str,
        shard: int,
        *,
        limit: int,
    ) -> tuple[list[RelayTask], bool]:
        migration_complete = self._migrate_execution_cleanup_shard_unlocked(
            cluster,
            shard,
            limit=limit,
        )
        shard_path = self._execution_cleanup_shard_path(cluster, shard)
        markers: list[RelayTask] = []
        for pending_job_path in shard_path.glob("*.pending"):
            try:
                pending_stat = os.stat(pending_job_path, follow_symlinks=False)
            except FileNotFoundError:
                continue
            if not stat.S_ISDIR(pending_stat.st_mode):
                raise QueueConflictError(
                    f"execution cleanup job index is not a directory: {pending_job_path}"
                )
            marker_seen = False
            for marker_path in pending_job_path.glob("*.json"):
                marker_seen = True
                if len(markers) >= limit:
                    return markers, True
                marker = self._read_json_file(marker_path, RelayTask)
                if marker.metadata.get("cluster") != cluster:
                    raise QueueConflictError(
                        f"execution cleanup marker has the wrong cluster: {marker_path}"
                    )
                if self._execution_cleanup_shard(marker.job_id) != shard:
                    raise QueueConflictError(
                        f"execution cleanup marker has the wrong shard: {marker_path}"
                    )
                markers.append(marker)
            if not marker_seen:
                try:
                    pending_job_path.rmdir()
                except FileNotFoundError:
                    pass
                except OSError as exc:
                    raise QueueConflictError(
                        f"could not repair empty execution cleanup index {pending_job_path}: {exc}"
                    ) from exc
                self._fsync_execution_cleanup_directory(shard_path)
        return markers, not migration_complete

    def _migrate_execution_cleanup_shard_unlocked(
        self,
        cluster: str,
        shard: int,
        *,
        limit: int,
    ) -> bool:
        receipt_path = self._execution_cleanup_migration_receipt_path(cluster, shard)
        if self._execution_cleanup_shard_migration_complete_unlocked(cluster, shard):
            return True
        if receipt_path.exists():
            raise QueueConflictError(
                f"execution cleanup migration receipt is invalid: {receipt_path}"
            )
        shard_path = self._execution_cleanup_shard_path(cluster, shard)
        for moved, legacy_path in enumerate(shard_path.glob("*.json")):
            if moved >= limit:
                return False
            marker = self._read_json_file(legacy_path, RelayTask)
            if marker.metadata.get("cluster") != cluster:
                raise QueueConflictError(
                    f"legacy execution cleanup marker has the wrong cluster: {legacy_path}"
                )
            if self._execution_cleanup_shard(marker.job_id) != shard:
                raise QueueConflictError(
                    f"legacy execution cleanup marker has the wrong shard: {legacy_path}"
                )
            target = self._execution_cleanup_path(cluster, marker.job_id, marker.task_id)
            target.parent.mkdir(parents=True, exist_ok=True)
            if target.exists():
                existing = self._read_json_file(target, RelayTask)
                if existing.task_id != marker.task_id or existing.job_id != marker.job_id:
                    raise QueueConflictError(
                        f"execution cleanup migration target conflicts: {target}"
                    )
                _unlink_durable_path(legacy_path)
            else:
                legacy_path.replace(target)
            self._fsync_execution_cleanup_directory(target.parent)
            self._fsync_execution_cleanup_directory(shard_path)
        self._write_json(
            receipt_path,
            {
                "schema_version": "clio-relay.execution-cleanup-migration.v1",
                "cluster": cluster,
                "shard": shard,
                "completed": True,
                "completed_at": utc_now().isoformat(),
            },
        )
        return True

    def _execution_cleanup_shard_migration_complete_unlocked(
        self,
        cluster: str,
        shard: int,
    ) -> bool:
        """Read a migration receipt without mutating queue state."""
        receipt_path = self._execution_cleanup_migration_receipt_path(cluster, shard)
        try:
            raw_receipt = self._read_json_document(receipt_path)
        except (FileNotFoundError, OSError, json.JSONDecodeError):
            return False
        if not isinstance(raw_receipt, dict):
            return False
        receipt = cast(dict[str, object], raw_receipt)
        return (
            receipt.get("schema_version") == "clio-relay.execution-cleanup-migration.v1"
            and receipt.get("cluster") == cluster
            and receipt.get("shard") == shard
            and receipt.get("completed") is True
        )

    def _job_has_pending_execution_cleanup_unlocked(self, cluster: str, job_id: str) -> bool:
        shard = self._execution_cleanup_shard(job_id)
        if not self._execution_cleanup_shard_migration_complete_unlocked(cluster, shard):
            return True
        pending_job_path = self._execution_cleanup_job_path(cluster, job_id)
        return pending_job_path.exists() or pending_job_path.is_symlink()

    def _job_has_pending_execution_cleanup_after_migration_unlocked(
        self,
        cluster: str,
        job_id: str,
    ) -> bool:
        """Resolve legacy cleanup state before deciding stale-job ownership."""
        self._migrate_execution_cleanup_shard_unlocked(
            cluster,
            self._execution_cleanup_shard(job_id),
            limit=DEFAULT_EXACT_RECORD_LIMIT + 1,
        )
        return self._job_has_pending_execution_cleanup_unlocked(cluster, job_id)

    def _execution_cleanup_path(self, cluster: str, job_id: str, task_id: str) -> Path:
        return self._execution_cleanup_job_path(cluster, job_id) / (
            f"{self._durable_key(task_id)}.json"
        )

    def _execution_cleanup_job_path(self, cluster: str, job_id: str) -> Path:
        return (
            self._execution_cleanup_shard_path(
                cluster,
                self._execution_cleanup_shard(job_id),
            )
            / f"{self._durable_key(job_id)}.pending"
        )

    def _execution_cleanup_migration_receipt_path(self, cluster: str, shard: int) -> Path:
        return (
            self._storage_root
            / "execution_cleanup_migrations"
            / self._label_key(cluster, domain="cluster")
            / f"{shard:02x}.json"
        )

    def _execution_cleanup_shard_has_pending_paths(self, cluster: str, shard: int) -> bool:
        shard_path = self._execution_cleanup_shard_path(cluster, shard)
        return (
            next(shard_path.glob("*.json"), None) is not None
            or next(shard_path.glob("*.pending"), None) is not None
        )

    @staticmethod
    def _fsync_execution_cleanup_directory(path: Path) -> None:
        try:
            descriptor = os.open(path, os.O_RDONLY)
        except OSError:
            return
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    def _execution_cleanup_shard_path(self, cluster: str, shard: int) -> Path:
        return (
            self._storage_root
            / "execution_cleanup_pending"
            / self._label_key(cluster, domain="cluster")
            / f"{shard:02x}"
        )

    @staticmethod
    def _execution_cleanup_shard(job_id: str) -> int:
        return hashlib.sha256(job_id.encode("utf-8")).digest()[0]

    def append_event(
        self,
        job_id: str,
        event_type: str,
        message: str,
        *,
        locked: bool = False,
        payload: dict[str, object] | None = None,
    ) -> RelayEvent:
        """Append an event with a per-job monotonic sequence number."""
        job_id = self._require_durable_record_id(job_id, field="job_id")
        if locked:
            return self._append_event_unlocked(job_id, event_type, message, payload or {})
        with self._lock:
            return self._append_event_unlocked(job_id, event_type, message, payload or {})

    def drain_events(self, cursor: Cursor, *, limit: int = 100) -> tuple[list[RelayEvent], Cursor]:
        """Drain events from a cursor and return the advanced cursor."""
        self._require_durable_record_id(cursor.job_id, field="job_id")
        drained, next_seq = self.read_event_page(
            cursor.job_id,
            next_seq=cursor.next_seq,
            limit=limit,
        )
        advanced = Cursor(job_id=cursor.job_id, next_seq=next_seq)
        return drained, advanced

    def read_event_page(
        self,
        job_id: str,
        *,
        next_seq: int = 1,
        limit: int = 100,
    ) -> tuple[list[RelayEvent], int]:
        """Read one bounded contiguous event page without updating a consumer cursor."""
        job_id = self._require_durable_record_id(job_id, field="job_id")
        if next_seq < 1:
            raise ValueError("event sequence must be greater than or equal to 1")
        if limit < 1:
            raise ValueError("event page limit must be greater than or equal to 1")
        self.initialize()
        event_dir = self._storage_root / "events" / job_id
        events: list[RelayEvent] = []
        candidate_seq = next_seq
        while len(events) < limit:
            event = self._read_optional(
                event_dir / f"{candidate_seq:020d}.json",
                RelayEvent,
            )
            if event is None:
                break
            if event.job_id != job_id or event.seq != candidate_seq:
                raise QueueConflictError(f"event filename/content identity mismatch: {event_dir}")
            events.append(event)
            candidate_seq += 1
        return events, candidate_seq

    def append_artifact(self, artifact: ArtifactRef) -> ArtifactRef:
        """Index an artifact reference."""
        self._require_durable_record_id(artifact.artifact_id, field="artifact_id")
        self._require_durable_record_id(artifact.job_id, field="job_id")
        self.initialize()
        with self._lock:
            self.get_job(artifact.job_id)
            sequence = self._next_job_record_sequence_unlocked(artifact.job_id, "artifact_count")
            saved = artifact.model_copy(update={"sequence": sequence})
            self._write(self._storage_root / "artifacts" / f"{saved.artifact_id}.json", saved)
            self._write(
                self._job_record_path("artifacts_by_job", saved.job_id, saved.artifact_id),
                saved,
            )
            (self._storage_root / "artifact_users" / saved.artifact_id).mkdir(
                parents=True,
                exist_ok=True,
            )
            self._initialize_artifact_user_order_unlocked(saved.artifact_id)
            self._write_ordered_job_record("artifact", saved.job_id, sequence, saved)
            self._link_gateways_for_artifact_unlocked(saved)
            self._increment_job_index_unlocked(artifact.job_id, "artifact_count")
            self.append_event(
                artifact.job_id,
                "artifact.created",
                f"Artifact indexed: {artifact.uri}",
                locked=True,
                payload={"artifact_id": artifact.artifact_id, "uri": artifact.uri},
            )
        return saved

    def list_artifacts(self, job_id: str) -> list[ArtifactRef]:
        """Return artifact refs for a job."""
        job_id = self._require_durable_record_id(job_id, field="job_id")
        self.initialize()
        if self._job_index_exists(job_id):
            return list(
                self._read_many(
                    self._storage_root / "artifacts_by_job" / self._durable_key(job_id),
                    ArtifactRef,
                    identity_field="artifact_id",
                )
            )
        return [
            artifact
            for artifact in self._read_many(
                self._storage_root / "artifacts",
                ArtifactRef,
                identity_field="artifact_id",
            )
            if artifact.job_id == job_id
        ]

    def list_artifacts_page(
        self,
        job_id: str,
        *,
        cursor: int = 1,
        limit: int = 100,
    ) -> tuple[list[ArtifactRef], int | None, int]:
        """Read one stable artifact page from the per-job sequence index."""
        job_id = self._require_durable_record_id(job_id, field="job_id")
        return self._read_ordered_job_page(
            job_id,
            family="artifact",
            model=ArtifactRef,
            cursor=cursor,
            limit=limit,
            count_field="artifact_count",
        )

    def job_artifact_count(self, job_id: str) -> tuple[int, bool]:
        """Return the exact indexed artifact count or a bounded legacy lower bound."""
        job_id = self._require_durable_record_id(job_id, field="job_id")
        index = self._read_job_index(job_id)
        if index is not None:
            return _index_integer(index, "artifact_count"), False
        artifacts, truncated = self._scan_many(
            self._storage_root / "artifacts",
            ArtifactRef,
            limit=DEFAULT_EXACT_RECORD_LIMIT,
            identity_field="artifact_id",
        )
        return sum(artifact.job_id == job_id for artifact in artifacts), truncated

    def get_artifact(self, artifact_id: str) -> ArtifactRef:
        """Return an artifact by id."""
        artifact_id = self._require_durable_record_id(artifact_id, field="artifact_id")
        path = self._storage_root / "artifacts" / f"{artifact_id}.json"
        artifact = self._read_optional(path, ArtifactRef)
        if artifact is None:
            raise NotFoundError(f"artifact not found: {artifact_id}")
        if artifact.artifact_id != artifact_id:
            raise QueueConflictError(f"canonical artifact identity mismatch: {path}")
        return artifact

    def list_used_artifacts_page(
        self,
        job_id: str,
        *,
        cursor: str | None = None,
        limit: int = 100,
    ) -> tuple[list[UsedArtifactRef], str | None, int]:
        """Return one bounded stable page of artifacts consumed by a job."""
        job_id = self._require_durable_record_id(job_id, field="job_id")
        if cursor is not None:
            cursor = self._require_durable_record_id(cursor, field="cursor")
        limit = validate_response_page_limit(limit)
        self.initialize()
        job = self.get_job(job_id)
        records, next_cursor, total = self._read_artifact_use_page(
            self._storage_root / "used_artifacts_by_job" / job_id,
            cursor=cursor,
            limit=limit,
            capacity=MAX_ARTIFACT_USES_PER_JOB,
            identity_field="artifact_id",
            label=f"used artifacts for job {job_id}",
        )
        expected = {item.artifact_id: item.sha256 for item in job.used_artifact_refs}
        if total != len(expected):
            raise QueueConflictError(f"used-artifact index is incomplete for job: {job_id}")
        for record in records:
            expected_sha256 = expected.get(record.artifact_id)
            if record.consumer_job_id != job_id or expected_sha256 != record.sha256:
                raise QueueConflictError(f"used-artifact index identity mismatch for job: {job_id}")
            self._validate_artifact_use_record(record)
        return records, next_cursor, total

    def list_artifact_users_page(
        self,
        artifact_id: str,
        *,
        cursor: str | None = None,
        limit: int = 100,
    ) -> tuple[list[UsedArtifactRef], str | None, int]:
        """Return one bounded stable page of jobs that consumed an artifact."""
        artifact_id = self._require_durable_record_id(artifact_id, field="artifact_id")
        cursor_sequence = 0 if cursor is None else _artifact_user_cursor_sequence(cursor)
        limit = validate_response_page_limit(limit)
        self.initialize()
        self.get_artifact(artifact_id)
        order_root = self._artifact_user_order_root(artifact_id)
        entry_paths = self._bounded_json_record_paths(
            order_root / "entries",
            limit=MAX_ARTIFACT_CONSUMERS,
            label=f"ordered consumers of artifact {artifact_id}",
        )
        reverse_paths = self._bounded_json_record_paths(
            self._storage_root / "artifact_users" / artifact_id,
            limit=MAX_ARTIFACT_CONSUMERS,
            label=f"consumers of artifact {artifact_id}",
        )
        mapping_paths = self._bounded_json_record_paths(
            order_root / "by_consumer",
            limit=MAX_ARTIFACT_CONSUMERS,
            label=f"consumer order mappings for artifact {artifact_id}",
        )
        if len(entry_paths) != len(reverse_paths) or len(mapping_paths) < len(entry_paths):
            raise QueueConflictError(
                f"artifact-user ordered index is incomplete for artifact: {artifact_id}"
            )
        latest_sequence = self._read_artifact_user_order_head(artifact_id)
        ordered_paths: list[tuple[int, Path]] = []
        for path in entry_paths:
            sequence = _artifact_user_entry_sequence(path)
            if sequence > latest_sequence:
                raise QueueConflictError(
                    f"artifact-user order entry exceeds its head: {artifact_id}"
                )
            ordered_paths.append((sequence, path))
        ordered_paths.sort(key=lambda item: item[0])
        remaining = [item for item in ordered_paths if item[0] > cursor_sequence]
        window = remaining[: limit + 1]
        has_more = len(window) > limit
        records: list[UsedArtifactRef] = []
        for sequence, path in window[:limit]:
            record = self._read_json_file(path, UsedArtifactRef)
            if record.artifact_id != artifact_id or record.sequence != sequence:
                raise QueueConflictError(
                    f"artifact-user order identity mismatch for artifact: {artifact_id}"
                )
            reverse = self._read_optional(
                self._storage_root
                / "artifact_users"
                / artifact_id
                / f"{record.consumer_job_id}.json",
                UsedArtifactRef,
            )
            mapping = self._read_optional(
                order_root / "by_consumer" / f"{record.consumer_job_id}.json",
                UsedArtifactRef,
            )
            if reverse != record or mapping != record:
                raise QueueConflictError(
                    f"artifact-user ordered index disagrees for artifact: {artifact_id}"
                )
            records.append(record)
        for record in records:
            self._validate_artifact_use_record(record)
        next_cursor = _artifact_user_cursor(records[-1].sequence) if has_more and records else None
        return records, next_cursor, len(ordered_paths)

    def _read_artifact_use_page(
        self,
        directory: Path,
        *,
        cursor: str | None,
        limit: int,
        capacity: int,
        identity_field: Literal["artifact_id", "consumer_job_id"],
        label: str,
    ) -> tuple[list[UsedArtifactRef], str | None, int]:
        paths = self._bounded_json_record_paths(
            directory,
            limit=capacity,
            label=label,
        )
        paths.sort(key=lambda path: path.name)
        total = len(paths)
        if cursor is not None:
            paths = [path for path in paths if path.stem > cursor]
        window = paths[: limit + 1]
        has_more = len(window) > limit
        records: list[UsedArtifactRef] = []
        for path in window[:limit]:
            record = self._read_json_file(path, UsedArtifactRef)
            identity = getattr(record, identity_field)
            if identity != path.stem:
                raise QueueConflictError(f"{label} filename/content identity mismatch: {path}")
            records.append(record)
        next_cursor = getattr(records[-1], identity_field) if has_more and records else None
        return records, next_cursor, total

    def _ensure_artifact_use_indexes_unlocked(self, job: RelayJob) -> None:
        """Validate and idempotently persist all immutable consumed-artifact edges."""
        records = self._artifact_use_records_unlocked(job, allocate_sequences=True)
        forward_directory = self._storage_root / "used_artifacts_by_job" / job.job_id
        for record in records:
            order_root = self._artifact_user_order_root(record.artifact_id)
            self._write_immutable_artifact_use_record(
                order_root / "by_consumer" / f"{record.consumer_job_id}.json",
                record,
            )
            self._write_immutable_artifact_use_record(
                forward_directory / f"{record.artifact_id}.json",
                record,
            )
            reverse_directory = self._storage_root / "artifact_users" / record.artifact_id
            reverse_directory.mkdir(parents=True, exist_ok=True)
            self._write_immutable_artifact_use_record(
                reverse_directory / f"{record.consumer_job_id}.json",
                record,
            )
            self._write_immutable_artifact_use_record(
                order_root / "entries" / f"{record.sequence:020d}.json",
                record,
            )

    def _artifact_use_records_unlocked(
        self,
        job: RelayJob,
        *,
        allocate_sequences: bool,
    ) -> list[UsedArtifactRef]:
        """Resolve dependencies, optionally reserving their durable edge sequences."""
        expected_ids = {item.artifact_id for item in job.used_artifact_refs}
        forward_directory = self._storage_root / "used_artifacts_by_job" / job.job_id
        existing_paths = self._bounded_json_record_paths(
            forward_directory,
            limit=MAX_ARTIFACT_USES_PER_JOB,
            label=f"used artifacts for job {job.job_id}",
        )
        unexpected = {path.stem for path in existing_paths}.difference(expected_ids)
        if unexpected:
            raise QueueConflictError(
                f"used-artifact edge set changed for job {job.job_id}: {sorted(unexpected)[0]}"
            )
        records: list[UsedArtifactRef] = []
        for use in job.used_artifact_refs:
            artifact = self._read_optional(
                self._storage_root / "artifacts" / f"{use.artifact_id}.json",
                ArtifactRef,
            )
            if artifact is None:
                raise QueueConflictError(f"used artifact not found: {use.artifact_id}")
            if artifact.artifact_id != use.artifact_id:
                raise QueueConflictError(f"canonical artifact identity mismatch: {use.artifact_id}")
            canonical_sha256 = artifact.sha256
            if not _is_sha256_digest(canonical_sha256):
                raise QueueConflictError(
                    f"used artifact is not content-addressed: {use.artifact_id}"
                )
            canonical_sha256 = cast(str, canonical_sha256)
            if canonical_sha256 != use.sha256:
                raise QueueConflictError(f"used artifact digest mismatch: {use.artifact_id}")
            producer = self._read_optional(
                self._storage_root / "jobs" / f"{artifact.job_id}.json",
                RelayJob,
            )
            if producer is None or producer.job_id != artifact.job_id:
                raise QueueConflictError(
                    f"used artifact producer is not retained: {use.artifact_id}"
                )
            _require_artifact_lineage_owner_match(consumer=job, producer=producer)
            existing_forward = self._read_optional(
                forward_directory / f"{artifact.artifact_id}.json",
                UsedArtifactRef,
            )
            reverse_directory = self._storage_root / "artifact_users" / artifact.artifact_id
            reverse_path = reverse_directory / f"{job.job_id}.json"
            existing_reverse = self._read_optional(reverse_path, UsedArtifactRef)
            order_root = self._artifact_user_order_root(artifact.artifact_id)
            order_head = self._read_artifact_user_order_head(artifact.artifact_id)
            mapping_path = order_root / "by_consumer" / f"{job.job_id}.json"
            existing_mapping = self._read_optional(mapping_path, UsedArtifactRef)
            reverse_paths = self._bounded_json_record_paths(
                reverse_directory,
                limit=MAX_ARTIFACT_CONSUMERS,
                label=f"consumers of artifact {artifact.artifact_id}",
            )
            mapping_paths = self._bounded_json_record_paths(
                order_root / "by_consumer",
                limit=MAX_ARTIFACT_CONSUMERS,
                label=f"consumer order mappings for artifact {artifact.artifact_id}",
            )
            existing_records = [
                record
                for record in (existing_forward, existing_reverse, existing_mapping)
                if record is not None
            ]
            if not existing_records and max(len(reverse_paths), len(mapping_paths)) >= (
                MAX_ARTIFACT_CONSUMERS
            ):
                raise QueueConflictError(
                    f"artifact consumer capacity is exhausted: {artifact.artifact_id}"
                )
            if existing_records:
                record = existing_records[0]
                if any(existing != record for existing in existing_records[1:]) or (
                    record.artifact_id != artifact.artifact_id
                    or record.consumer_job_id != job.job_id
                    or record.producer_job_id != artifact.job_id
                    or record.sha256 != canonical_sha256
                ):
                    raise QueueConflictError(
                        f"immutable used-artifact edge identity changed: {artifact.artifact_id}"
                    )
                if order_head < record.sequence:
                    raise QueueConflictError(
                        f"artifact-user order head is behind its edge: {artifact.artifact_id}"
                    )
                entry = self._read_optional(
                    order_root / "entries" / f"{record.sequence:020d}.json",
                    UsedArtifactRef,
                )
                if entry is not None and entry != record:
                    raise QueueConflictError(
                        f"artifact-user order entry changed: {artifact.artifact_id}"
                    )
                records.append(record)
                continue
            if not allocate_sequences:
                continue
            record = self._reserve_artifact_user_order_unlocked(
                artifact_id=artifact.artifact_id,
                consumer_job_id=job.job_id,
                producer_job_id=artifact.job_id,
                sha256=canonical_sha256,
                created_at=job.created_at,
            )
            records.append(record)
        return records

    def _artifact_user_order_root(self, artifact_id: str) -> Path:
        return self._storage_root / "artifact_user_order" / artifact_id

    def _initialize_artifact_user_order_unlocked(self, artifact_id: str) -> None:
        root = self._artifact_user_order_root(artifact_id)
        root_existed = root.exists()
        head_path = root / "head.json"
        if not root_existed:
            self._write(
                head_path,
                ArtifactUserOrderHead(
                    artifact_id=artifact_id,
                    latest_sequence=0,
                ),
            )
        elif not head_path.exists():
            raise QueueConflictError(
                f"artifact-user order head is missing from initialized index: {artifact_id}"
            )
        (root / "entries").mkdir(parents=True, exist_ok=True)
        (root / "by_consumer").mkdir(parents=True, exist_ok=True)

    def _read_artifact_user_order_head(self, artifact_id: str) -> int:
        path = self._artifact_user_order_root(artifact_id) / "head.json"
        head = self._read_optional(path, ArtifactUserOrderHead)
        if head is None:
            if path.parent.exists():
                raise QueueConflictError(f"artifact-user order head is missing: {path}")
            return 0
        if head.artifact_id != artifact_id:
            raise QueueConflictError(f"artifact-user order head identity mismatch: {path}")
        return head.latest_sequence

    def _reserve_artifact_user_order_unlocked(
        self,
        *,
        artifact_id: str,
        consumer_job_id: str,
        producer_job_id: str,
        sha256: str,
        created_at: datetime,
    ) -> UsedArtifactRef:
        """Reserve one monotonic edge identity, leaving safe gaps after crashes."""
        root = self._artifact_user_order_root(artifact_id)
        self._initialize_artifact_user_order_unlocked(artifact_id)
        mapping_path = root / "by_consumer" / f"{consumer_job_id}.json"
        existing = self._read_optional(mapping_path, UsedArtifactRef)
        if existing is not None:
            return existing
        latest_sequence = self._read_artifact_user_order_head(artifact_id)
        if latest_sequence >= 2**63 - 1:
            raise QueueConflictError(f"artifact-user sequence exhausted: {artifact_id}")
        sequence = latest_sequence + 1
        record = UsedArtifactRef(
            artifact_id=artifact_id,
            consumer_job_id=consumer_job_id,
            producer_job_id=producer_job_id,
            sequence=sequence,
            sha256=sha256,
            created_at=created_at,
        )
        self._write(
            root / "head.json",
            ArtifactUserOrderHead(
                artifact_id=artifact_id,
                latest_sequence=sequence,
            ),
        )
        self._write_immutable_artifact_use_record(mapping_path, record)
        return record

    def _write_immutable_artifact_use_record(
        self,
        path: Path,
        record: UsedArtifactRef,
    ) -> None:
        existing = self._read_optional(path, UsedArtifactRef)
        if existing is not None:
            if existing != record:
                raise QueueConflictError(f"immutable used-artifact edge changed: {path}")
            return
        self._write(path, record)

    def _validate_artifact_use_record(self, record: UsedArtifactRef) -> None:
        artifact = self.get_artifact(record.artifact_id)
        if artifact.job_id != record.producer_job_id or artifact.sha256 != record.sha256:
            raise QueueConflictError(
                f"used-artifact edge no longer matches canonical artifact: {record.artifact_id}"
            )
        consumer = self.get_job(record.consumer_job_id)
        producer = self.get_job(record.producer_job_id)
        _require_artifact_lineage_owner_match(consumer=consumer, producer=producer)
        pinned = {item.artifact_id: item.sha256 for item in consumer.used_artifact_refs}
        if pinned.get(record.artifact_id) != record.sha256:
            raise QueueConflictError(
                f"used-artifact edge no longer matches consumer job: {record.consumer_job_id}"
            )

    def append_progress(self, progress: ProgressRecord) -> ProgressRecord:
        """Record a structured job progress observation."""
        self._require_durable_record_id(progress.progress_id, field="progress_id")
        self._require_durable_record_id(progress.job_id, field="job_id")
        self.initialize()
        with self._lock:
            self.get_job(progress.job_id)
            sequence = self._next_job_record_sequence_unlocked(progress.job_id, "progress_count")
            saved = progress.model_copy(update={"sequence": sequence})
            self._write(self._storage_root / "progress" / f"{saved.progress_id}.json", saved)
            self._write(
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
        job_id = self._require_durable_record_id(job_id, field="job_id")
        self.initialize()
        if self._job_index_exists(job_id):
            return sorted(
                self._read_many(
                    self._storage_root / "progress_by_job" / self._durable_key(job_id),
                    ProgressRecord,
                    identity_field="progress_id",
                ),
                key=lambda progress: progress.created_at,
            )
        return sorted(
            [
                progress
                for progress in self._read_many(
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
        job_id = self._require_durable_record_id(job_id, field="job_id")
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
        job_id = self._require_durable_record_id(job_id, field="job_id")
        index = self._read_job_index(job_id)
        if index is not None:
            count = _index_integer(index, "progress_count")
            progress_id = index.get("latest_progress_id")
            if not isinstance(progress_id, str):
                return None, count, False
            progress = self._read_optional(
                self._job_record_path("progress_by_job", job_id, progress_id),
                ProgressRecord,
            )
            if progress is None:
                raise QueueConflictError(f"progress index points to a missing record: {job_id}")
            return progress, count, False
        progress, truncated = self._scan_many(
            self._storage_root / "progress",
            ProgressRecord,
            limit=DEFAULT_EXACT_RECORD_LIMIT,
            identity_field="progress_id",
        )
        matched = [item for item in progress if item.job_id == job_id]
        latest = max(matched, key=lambda item: item.created_at, default=None)
        return latest, len(matched), truncated

    def create_gateway_session(self, session: GatewaySession) -> GatewaySession:
        """Create a durable scheduler-backed gateway session record."""
        self._require_durable_record_id(session.session_id, field="session_id")
        self.initialize()
        self._require_index_migration_complete()
        with self._lock:
            self._recover_pending_transitions_unlocked()
            existing = self._read_optional(
                self._storage_root / "gateway_sessions" / f"{session.session_id}.json",
                GatewaySession,
            )
            if existing is not None:
                if existing.session_id != session.session_id:
                    raise QueueConflictError(
                        f"canonical gateway session identity mismatch: {session.session_id}"
                    )
                raise QueueConflictError(f"gateway session already exists: {session.session_id}")
            _validate_owner_session_identity_metadata(session.metadata, allow_legacy=False)
            self._assert_owner_session_intake_open_unlocked(
                session.metadata,
                require_active=True,
            )
            self._ensure_global_order_entry_unlocked(
                "gateway_sessions",
                session.session_id,
            )
            self._write_gateway_session_unlocked(session)
        return session

    def get_gateway_session(self, session_id: str) -> GatewaySession:
        """Return a gateway session by id."""
        session_id = self._require_durable_record_id(session_id, field="session_id")
        session = self._read_optional(
            self._storage_root / "gateway_sessions" / f"{session_id}.json",
            GatewaySession,
        )
        if session is None:
            raise NotFoundError(f"gateway session not found: {session_id}")
        if session.session_id != session_id:
            raise QueueConflictError(f"canonical gateway session identity mismatch: {session_id}")
        return session

    def list_gateway_sessions(self, cluster: str | None = None) -> list[GatewaySession]:
        """Return durable gateway sessions, optionally filtered by cluster."""
        self.initialize()
        sessions = list(
            self._read_many(
                self._storage_root / "gateway_sessions",
                GatewaySession,
                identity_field="session_id",
            )
        )
        if cluster is not None:
            sessions = [session for session in sessions if session.cluster == cluster]
        return sorted(sessions, key=lambda session: session.created_at)

    def list_gateway_sessions_page(
        self,
        *,
        cursor: int = 1,
        limit: int = 100,
        cluster: str | None = None,
        state: GatewaySessionState | None = None,
    ) -> tuple[list[GatewaySession], int | None, int]:
        """Read one global gateway-session source window with in-window filters."""

        def matches(session: GatewaySession) -> bool:
            return (cluster is None or session.cluster == cluster) and (
                state is None or session.state == state
            )

        return self._read_global_order_page(
            family="gateway_sessions",
            model=GatewaySession,
            identity_field="session_id",
            cursor=cursor,
            limit=limit,
            predicate=matches,
        )

    def scan_gateway_sessions(
        self,
        *,
        limit: int,
        cluster: str | None = None,
        state: GatewaySessionState | None = None,
    ) -> tuple[list[GatewaySession], bool]:
        """Read one bounded gateway-session source window and truncation state."""

        def matches(session: GatewaySession) -> bool:
            return (cluster is None or session.cluster == cluster) and (
                state is None or session.state == state
            )

        return self._scan_global_order(
            family="gateway_sessions",
            model=GatewaySession,
            identity_field="session_id",
            limit=limit,
            predicate=matches,
        )

    def update_gateway_session(
        self,
        session_id: str,
        *,
        state: GatewaySessionState | None = None,
        metadata: dict[str, object] | None = None,
        expected_updated_at: object = None,
        allow_owned_runtime_close: object = False,
        reject_relay_managed_fields: object = False,
        **updates: object,
    ) -> GatewaySession:
        """Merge gateway state using an optional optimistic transition guard."""
        session_id = self._require_durable_record_id(session_id, field="session_id")
        self.initialize()
        self._require_index_migration_complete()
        with self._lock:
            self._recover_pending_transitions_unlocked()
            session = self.get_gateway_session(session_id)
            if expected_updated_at is not None and not isinstance(expected_updated_at, datetime):
                raise ValueError("expected_updated_at must be an aware datetime")
            if expected_updated_at is not None and session.updated_at != expected_updated_at:
                raise QueueConflictError(
                    f"gateway session changed during a runtime transition: {session_id}"
                )
            self._ensure_global_order_entry_unlocked(
                "gateway_sessions",
                session.session_id,
            )
            is_owned_runtime = session.metadata.get("owner") == "clio-relay" and isinstance(
                session.gateway.get("runtime_spec"), dict
            )
            if (
                reject_relay_managed_fields is True
                and "gateway" in updates
                and _has_relay_managed_gateway_state(session.gateway)
            ):
                raise QueueConflictError(
                    "generic gateway updates cannot replace relay-managed runtime state: "
                    f"{session_id}"
                )
            if (
                state == GatewaySessionState.CLOSED
                and session.state != GatewaySessionState.CLOSED
                and is_owned_runtime
                and allow_owned_runtime_close is not True
            ):
                raise QueueConflictError(
                    "owned runtime gateway sessions must be closed with stop-runtime so "
                    "connectors are proven stopped first"
                )
            if session.state == GatewaySessionState.CLOSED:
                if state is not None and state != GatewaySessionState.CLOSED:
                    raise QueueConflictError(f"cannot reopen closed gateway session: {session_id}")
                if updates and allow_owned_runtime_close is not True:
                    raise QueueConflictError(f"cannot update closed gateway session: {session_id}")
            current_teardown_intent = session.gateway.get("teardown_intent")
            if current_teardown_intent is not None and "gateway" in updates:
                replacement_gateway = updates.get("gateway")
                if (
                    not isinstance(replacement_gateway, dict)
                    or cast(dict[str, object], replacement_gateway).get("teardown_intent")
                    != current_teardown_intent
                ):
                    raise QueueConflictError(
                        "a committed gateway teardown intent cannot be removed or changed: "
                        f"{session_id}"
                    )
            merged_metadata = dict(session.metadata)
            if metadata:
                merged_metadata.update(metadata)
            payload = dict(updates)
            if state is not None:
                payload["state"] = state
            payload["metadata"] = merged_metadata
            payload["updated_at"] = utc_now()
            updated = session.model_copy(update=payload)
            self._write_gateway_session_unlocked(updated)
            return updated

    def prepare_gateway_teardown_intent(
        self,
        session_id: str,
        *,
        cancel_scheduler_job: bool,
    ) -> GatewaySession:
        """Atomically create or validate one immutable gateway cleanup policy."""
        session_id = self._require_durable_record_id(session_id, field="session_id")
        self.initialize()
        self._require_index_migration_complete()
        with self._lock:
            self._recover_pending_transitions_unlocked()
            session = self.get_gateway_session(session_id)
            raw_intent = session.gateway.get("teardown_intent")
            if raw_intent is not None:
                if not isinstance(raw_intent, dict):
                    raise QueueConflictError("gateway teardown intent is invalid")
                intent = cast(dict[str, object], raw_intent)
                operation_id = intent.get("operation_id")
                created_at = intent.get("created_at")
                if (
                    intent.get("schema_version") != "clio-relay.gateway-teardown-intent.v1"
                    or intent.get("gateway_session_id") != session_id
                    or not isinstance(operation_id, str)
                    or not operation_id.startswith("gateway_cleanup_")
                    or not _safe_global_record_id(operation_id)
                    or not isinstance(created_at, str)
                    or not isinstance(intent.get("cancel_scheduler_job"), bool)
                ):
                    raise QueueConflictError("gateway teardown intent is invalid")
                try:
                    parsed_created_at = datetime.fromisoformat(created_at)
                except ValueError as exc:
                    raise QueueConflictError("gateway teardown intent time is invalid") from exc
                if parsed_created_at.tzinfo is None:
                    raise QueueConflictError("gateway teardown intent time is naive")
                if intent.get("cancel_scheduler_job") is not cancel_scheduler_job:
                    raise QueueConflictError(
                        "gateway cleanup policy changed during retry; resume with the original "
                        f"cancel_scheduler_job={intent.get('cancel_scheduler_job')} policy"
                    )
                return session
            if session.state == GatewaySessionState.CLOSED:
                raise QueueConflictError(
                    f"closed gateway session has no durable teardown intent: {session_id}"
                )
            gateway = {
                **session.gateway,
                "teardown_intent": {
                    "schema_version": "clio-relay.gateway-teardown-intent.v1",
                    "operation_id": f"gateway_cleanup_{uuid4().hex}",
                    "gateway_session_id": session_id,
                    "cancel_scheduler_job": cancel_scheduler_job,
                    "created_at": utc_now().isoformat(),
                },
            }
            updated = session.model_copy(update={"gateway": gateway, "updated_at": utc_now()})
            self._write_gateway_session_unlocked(updated)
            return updated

    def prepare_gateway_browser_attachment(
        self,
        session_id: str,
        *,
        attachment: BrowserAttachmentRecord,
        browser_proxy_intent: dict[str, object],
    ) -> GatewaySession:
        """Atomically reserve the sole browser attachment slot for one exact identity."""
        session_id = self._require_durable_record_id(session_id, field="session_id")
        if attachment.state != "starting":
            raise ValueError("prepared browser attachment must be in starting state")
        _validate_browser_proxy_intent(
            browser_proxy_intent,
            attachment_id=attachment.attachment_id,
            expected_state="starting",
        )
        self.initialize()
        self._require_index_migration_complete()
        with self._lock:
            self._recover_pending_transitions_unlocked()
            session = self.get_gateway_session(session_id)
            _require_browser_attachment_session_ready(session)
            existing = _browser_attachment_record(session, required=False)
            if existing is not None and existing.state != "revoked":
                raise QueueConflictError(
                    "gateway already has a browser attachment transition: "
                    f"{existing.attachment_id} ({existing.state})"
                )
            if existing is not None and existing.attachment_id == attachment.attachment_id:
                raise QueueConflictError("revoked browser attachment identities cannot be reused")
            transport = _gateway_mapping(session.gateway, "transport")
            if transport.get("browser_proxy") is not None:
                raise QueueConflictError("gateway has a browser proxy without an active slot")
            intents = _gateway_mapping(session.gateway, "ownership_intents")
            current_intent = intents.get("browser_proxy")
            if isinstance(current_intent, dict) and cast(dict[str, object], current_intent).get(
                "state"
            ) not in {"not_started", "absent_verified"}:
                raise QueueConflictError("gateway browser proxy ownership is not absent")
            intents["browser_proxy"] = dict(browser_proxy_intent)
            gateway = {
                **session.gateway,
                "browser_attachment": attachment.model_dump(mode="json"),
                "ownership_intents": intents,
            }
            return self._write_browser_attachment_transition_unlocked(
                session,
                gateway=gateway,
            )

    def complete_gateway_browser_attachment(
        self,
        session_id: str,
        *,
        attachment: BrowserAttachmentRecord,
        browser_proxy: dict[str, object],
        browser_proxy_intent: dict[str, object],
    ) -> GatewaySession:
        """Atomically publish one started proxy without overwriting newer gateway state."""
        session_id = self._require_durable_record_id(session_id, field="session_id")
        if attachment.state != "active" or attachment.proxy_process_id is None:
            raise ValueError("completed browser attachment must be active with a proxy pid")
        _validate_browser_proxy_identity(
            browser_proxy,
            attachment_id=attachment.attachment_id,
            proxy_process_id=attachment.proxy_process_id,
        )
        _validate_browser_proxy_intent(
            browser_proxy_intent,
            attachment_id=attachment.attachment_id,
            expected_state="recorded",
        )
        self.initialize()
        self._require_index_migration_complete()
        with self._lock:
            self._recover_pending_transitions_unlocked()
            session = self.get_gateway_session(session_id)
            _require_browser_attachment_session_ready(session)
            current = _browser_attachment_record(session, required=True)
            assert current is not None
            if current.state == "active" and current == attachment:
                return session
            if current.state != "starting":
                raise QueueConflictError(
                    "browser attachment cannot complete from "
                    f"{current.state}: {current.attachment_id}"
                )
            _require_same_browser_attachment(current, attachment)
            intents = _gateway_mapping(session.gateway, "ownership_intents")
            current_intent = intents.get("browser_proxy")
            if not isinstance(current_intent, dict):
                raise QueueConflictError("browser attachment has no starting ownership intent")
            _validate_browser_proxy_intent(
                cast(dict[str, object], current_intent),
                attachment_id=attachment.attachment_id,
                expected_state="starting",
            )
            _require_browser_proxy_ownership_consistent(
                cast(dict[str, object], current_intent),
                browser_proxy,
                browser_proxy_intent,
            )
            intents["browser_proxy"] = dict(browser_proxy_intent)
            transport = _gateway_mapping(session.gateway, "transport")
            if transport.get("browser_proxy") is not None:
                raise QueueConflictError("browser attachment proxy was already published")
            transport["browser_proxy"] = dict(browser_proxy)
            gateway = {
                **session.gateway,
                "browser_attachment": attachment.model_dump(mode="json"),
                "ownership_intents": intents,
                "transport": transport,
            }
            return self._write_browser_attachment_transition_unlocked(
                session,
                gateway=gateway,
            )

    def begin_gateway_browser_attachment_revoke(
        self,
        session_id: str,
        *,
        attachment_id: str,
    ) -> GatewaySession:
        """Atomically move the exact current attachment into revocation."""
        session_id = self._require_durable_record_id(session_id, field="session_id")
        if not attachment_id:
            raise ValueError("attachment_id must not be empty")
        self.initialize()
        self._require_index_migration_complete()
        with self._lock:
            self._recover_pending_transitions_unlocked()
            session = self.get_gateway_session(session_id)
            current = _browser_attachment_record(session, required=True)
            assert current is not None
            if current.attachment_id != attachment_id:
                raise QueueConflictError(
                    "browser attachment changed before revocation: "
                    f"{current.attachment_id} != {attachment_id}"
                )
            if current.state in {"revoking", "revoked"}:
                return session
            revoking = current.model_copy(update={"state": "revoking"})
            gateway = {
                **session.gateway,
                "browser_attachment": revoking.model_dump(mode="json"),
            }
            return self._write_browser_attachment_transition_unlocked(
                session,
                gateway=gateway,
            )

    def finish_gateway_browser_attachment_revoke(
        self,
        session_id: str,
        *,
        attachment: BrowserAttachmentRecord,
        browser_proxy_absent_intent: dict[str, object] | None = None,
        metadata: dict[str, object] | None = None,
    ) -> GatewaySession:
        """Atomically finish exact revocation while retaining concurrent teardown state."""
        session_id = self._require_durable_record_id(session_id, field="session_id")
        if attachment.state not in {"revoked", "failed"}:
            raise ValueError("finished browser attachment must be revoked or failed")
        if attachment.state == "revoked":
            if browser_proxy_absent_intent is None:
                raise ValueError("revoked browser attachment requires an absent proxy intent")
            _validate_browser_proxy_intent(
                browser_proxy_absent_intent,
                attachment_id=attachment.attachment_id,
                expected_state="absent_verified",
            )
        elif browser_proxy_absent_intent is not None:
            raise ValueError("failed browser attachment cannot claim proxy absence")
        self.initialize()
        self._require_index_migration_complete()
        with self._lock:
            self._recover_pending_transitions_unlocked()
            session = self.get_gateway_session(session_id)
            current = _browser_attachment_record(session, required=True)
            assert current is not None
            if current.attachment_id != attachment.attachment_id:
                raise QueueConflictError(
                    "browser attachment changed before revocation completed: "
                    f"{current.attachment_id} != {attachment.attachment_id}"
                )
            if current.state == "revoked":
                _require_same_browser_attachment(current, attachment)
                return session
            if current.state == "failed" and attachment.state == "failed":
                _require_same_browser_attachment(current, attachment)
                return session
            if current.state not in {"revoking", "failed"} or (
                current.state == "failed" and attachment.state != "revoked"
            ):
                raise QueueConflictError(
                    "browser attachment cannot finish revocation from "
                    f"{current.state}: {current.attachment_id}"
                )
            _require_same_browser_attachment(current, attachment)
            gateway = dict(session.gateway)
            gateway["browser_attachment"] = attachment.model_dump(mode="json")
            if attachment.state == "revoked":
                assert browser_proxy_absent_intent is not None
                transport = _gateway_mapping(session.gateway, "transport")
                current_proxy = transport.get("browser_proxy")
                if isinstance(current_proxy, dict):
                    _validate_browser_proxy_identity(
                        cast(dict[str, object], current_proxy),
                        attachment_id=attachment.attachment_id,
                        proxy_process_id=attachment.proxy_process_id,
                    )
                transport.pop("browser_proxy", None)
                intents = _gateway_mapping(session.gateway, "ownership_intents")
                current_intent = intents.get("browser_proxy")
                if isinstance(current_intent, dict):
                    _validate_browser_proxy_intent(
                        cast(dict[str, object], current_intent),
                        attachment_id=attachment.attachment_id,
                    )
                    _require_browser_proxy_ownership_consistent(
                        cast(dict[str, object], current_intent),
                        browser_proxy_absent_intent,
                    )
                intents["browser_proxy"] = dict(browser_proxy_absent_intent)
                gateway["transport"] = transport
                gateway["ownership_intents"] = intents
            return self._write_browser_attachment_transition_unlocked(
                session,
                gateway=gateway,
                metadata=metadata,
            )

    def _write_browser_attachment_transition_unlocked(
        self,
        session: GatewaySession,
        *,
        gateway: dict[str, Any],
        metadata: dict[str, object] | None = None,
    ) -> GatewaySession:
        """Persist one lock-held attachment transition against the latest session."""
        merged_metadata = dict(session.metadata)
        if metadata:
            merged_metadata.update(metadata)
        updated = session.model_copy(
            update={
                "gateway": gateway,
                "metadata": merged_metadata,
                "updated_at": utc_now(),
            }
        )
        self._write_gateway_session_unlocked(updated)
        return updated

    def set_owner_session_closing(
        self,
        owner_session_id: str,
        *,
        session_generation_id: str,
        operation_id: str | None = None,
        stop_worker: bool = False,
        cancel_jobs: bool = False,
        cancel_scheduler_jobs: bool = False,
    ) -> dict[str, object]:
        """Quiesce one generation and persist its immutable cleanup policy."""
        if not owner_session_id:
            raise ValueError("owner_session_id must not be empty")
        session_generation_id = self._require_durable_record_id(
            session_generation_id,
            field="session_generation_id",
        )
        if operation_id is not None and (
            not operation_id.startswith("cleanup_") or not _safe_global_record_id(operation_id)
        ):
            raise ValueError("operation_id must be a safe cleanup_ identifier")
        if cancel_scheduler_jobs and not cancel_jobs:
            raise ValueError("cancel_scheduler_jobs requires cancel_jobs")
        self.initialize()
        path = (
            self._storage_root
            / "owner_sessions"
            / f"{self._label_key(owner_session_id, domain='owner-session')}.closing.json"
        )
        with self._lock:
            existing_closing = self._read_owner_session_transition_record(path)
            existing_generation = self._validate_owner_session_closing_record(
                owner_session_id,
                existing_closing,
            )
            active_generation = self._owner_session_active_generation(owner_session_id)
            safely_closed_retry = False
            if active_generation != session_generation_id:
                existing_closure = self._read_optional(
                    self._owner_session_closed_path(
                        owner_session_id,
                        session_generation_id=session_generation_id,
                    ),
                    OwnerSessionClosure,
                )
                safely_closed_retry = (
                    active_generation is None
                    and existing_generation == session_generation_id
                    and existing_closure is not None
                    and existing_closure.owner_session_id == owner_session_id
                    and existing_closure.session_generation_id == session_generation_id
                    and not existing_closure.residual_resource_ids
                )
                if not safely_closed_retry:
                    raise QueueConflictError(
                        f"owner session active generation does not match closing request: "
                        f"{owner_session_id}"
                    )
            if existing_closing is not None and (
                existing_closing.get("owner_session_id") != owner_session_id
                or existing_closing.get("closing") is not True
                or existing_closing.get("session_generation_id") != session_generation_id
            ):
                raise QueueConflictError(
                    f"owner session generation changed before quiescence: {owner_session_id}"
                )
            expected_policy = {
                "stop_worker": stop_worker,
                "cancel_jobs": cancel_jobs,
                "cancel_scheduler_jobs": cancel_scheduler_jobs,
            }
            existing_intent = self._validate_owner_session_cleanup_intent(
                owner_session_id,
                session_generation_id,
                None if existing_closing is None else existing_closing.get("cleanup_intent"),
                required=False,
            )
            if existing_intent is None and safely_closed_retry:
                raise QueueConflictError(
                    "closed owner session has no durable cleanup policy for retry: "
                    f"{owner_session_id}"
                )
            if existing_intent is not None:
                observed_policy = {
                    key: existing_intent[key]
                    for key in (
                        "stop_worker",
                        "cancel_jobs",
                        "cancel_scheduler_jobs",
                    )
                }
                if observed_policy != expected_policy:
                    raise QueueConflictError(
                        f"owner session cleanup policy changed during retry: {owner_session_id}"
                    )
                if operation_id is not None and existing_intent["operation_id"] != operation_id:
                    raise QueueConflictError(
                        f"owner session cleanup operation changed during retry: {owner_session_id}"
                    )
                return existing_intent
            cleanup_intent: dict[str, object] = {
                "schema_version": "clio-relay.owner-session-cleanup-intent.v1",
                "operation_id": operation_id or f"cleanup_{uuid4().hex}",
                "owner_session_id": owner_session_id,
                "session_generation_id": session_generation_id,
                **expected_policy,
                "created_at": utc_now().isoformat(),
            }
            self._write_json(
                path,
                {
                    "owner_session_id": owner_session_id,
                    "session_generation_id": session_generation_id,
                    "closing": True,
                    "cleanup_intent": cleanup_intent,
                    "updated_at": utc_now().isoformat(),
                },
            )
            return cleanup_intent

    def get_owner_session_cleanup_intent(
        self,
        owner_session_id: str,
        *,
        session_generation_id: str,
    ) -> dict[str, object] | None:
        """Return the immutable cleanup intent for one exact closing generation."""
        if not owner_session_id:
            raise ValueError("owner_session_id must not be empty")
        session_generation_id = self._require_durable_record_id(
            session_generation_id,
            field="session_generation_id",
        )
        self.initialize()
        path = (
            self._storage_root
            / "owner_sessions"
            / f"{self._label_key(owner_session_id, domain='owner-session')}.closing.json"
        )
        with self._lock:
            closing = self._read_owner_session_transition_record(path)
            closing_generation = self._validate_owner_session_closing_record(
                owner_session_id,
                closing,
            )
            if closing_generation is None:
                return None
            if closing_generation != session_generation_id:
                raise QueueConflictError(
                    f"owner session closing generation does not match request: {owner_session_id}"
                )
            return self._validate_owner_session_cleanup_intent(
                owner_session_id,
                session_generation_id,
                None if closing is None else closing.get("cleanup_intent"),
                required=True,
            )

    def mirror_owner_session_generation_open(
        self,
        owner_session_id: str,
        *,
        session_generation_id: str,
    ) -> dict[str, object]:
        """Mirror a remotely verified generation into this queue's admission boundary.

        The caller must verify the authoritative remote session before invoking this
        method. The mirror never reopens the same closed generation and never erases
        an unfinished local cleanup transition.
        """
        if not owner_session_id:
            raise ValueError("owner_session_id must not be empty")
        session_generation_id = self._require_durable_record_id(
            session_generation_id,
            field="session_generation_id",
        )
        self.initialize()
        active_path = self._owner_session_active_path(owner_session_id)
        closing_path = (
            self._storage_root
            / "owner_sessions"
            / f"{self._label_key(owner_session_id, domain='owner-session')}.closing.json"
        )
        with self._lock:
            active = self._read_owner_session_transition_record(active_path)
            closing = self._read_owner_session_transition_record(closing_path)
            active_generation = self._validate_owner_session_active_record(
                owner_session_id,
                active,
            )
            closing_generation = self._validate_owner_session_closing_record(
                owner_session_id,
                closing,
            )
            if active_generation not in {None, session_generation_id}:
                raise QueueConflictError(
                    f"owner session active generation does not match remote mirror: "
                    f"{owner_session_id}"
                )
            previous_generation_id: str | None = None
            if closing_generation is not None:
                prior_closure = self.get_owner_session_closed(
                    owner_session_id,
                    session_generation_id=closing_generation,
                )
                safely_closed_prior_generation = (
                    closing_generation != session_generation_id
                    and prior_closure is not None
                    and not prior_closure.residual_resource_ids
                )
                if not safely_closed_prior_generation:
                    raise QueueConflictError(
                        f"owner session has unfinished local cleanup and rejects remote mirror: "
                        f"{owner_session_id}"
                    )
                previous_generation_id = closing_generation
                _unlink_durable_path(closing_path, missing_ok=True)
                closing_generation = None
            if (
                self.get_owner_session_closed(
                    owner_session_id,
                    session_generation_id=session_generation_id,
                )
                is not None
            ):
                raise QueueConflictError(
                    f"owner session generation is already closed: {owner_session_id}"
                )
            if active_generation is None:
                self._write_json(
                    active_path,
                    {
                        "owner_session_id": owner_session_id,
                        "session_generation_id": session_generation_id,
                        "previous_session_generation_id": previous_generation_id,
                        "active": True,
                        "mirrored_remote_authority": True,
                        "updated_at": utc_now().isoformat(),
                    },
                )
            return {
                "schema_version": "clio-relay.owner-session-admission-status.v1",
                "owner_session_id": owner_session_id,
                "session_generation_id": session_generation_id,
                "active_generation_id": session_generation_id,
                "closing_generation_id": closing_generation,
                "active": True,
                "closing": False,
                "closed": False,
                "open": True,
                "cleanup_intent": None,
            }

    def owner_session_generation_status(
        self,
        owner_session_id: str,
        *,
        session_generation_id: str,
    ) -> dict[str, object]:
        """Return exact machine-readable admission state for one generation."""
        if not owner_session_id:
            raise ValueError("owner_session_id must not be empty")
        session_generation_id = self._require_durable_record_id(
            session_generation_id,
            field="session_generation_id",
        )
        self.initialize()
        closing_path = (
            self._storage_root
            / "owner_sessions"
            / f"{self._label_key(owner_session_id, domain='owner-session')}.closing.json"
        )
        with self._lock:
            active_generation = self._owner_session_active_generation(owner_session_id)
            closing = self._read_owner_session_transition_record(closing_path)
            closing_generation = self._validate_owner_session_closing_record(
                owner_session_id,
                closing,
            )
            cleanup_intent = (
                self._validate_owner_session_cleanup_intent(
                    owner_session_id,
                    session_generation_id,
                    closing.get("cleanup_intent"),
                    required=True,
                )
                if closing_generation == session_generation_id and closing is not None
                else None
            )
            closed = (
                self.get_owner_session_closed(
                    owner_session_id,
                    session_generation_id=session_generation_id,
                )
                is not None
            )
            exact_active = active_generation == session_generation_id
            exact_closing = closing_generation == session_generation_id
            return {
                "schema_version": "clio-relay.owner-session-admission-status.v1",
                "owner_session_id": owner_session_id,
                "session_generation_id": session_generation_id,
                "active_generation_id": active_generation,
                "closing_generation_id": closing_generation,
                "active": exact_active,
                "closing": exact_closing,
                "closed": closed,
                "open": exact_active and closing_generation is None and not closed,
                "cleanup_intent": cleanup_intent,
            }

    def prepare_owner_session_start(
        self,
        owner_session_id: str,
        *,
        recorded_generation_id: str | None,
        candidate_generation_id: str,
    ) -> str:
        """Atomically select the only generation allowed to start under a transition lock."""
        if not owner_session_id:
            raise ValueError("owner_session_id must not be empty")
        if recorded_generation_id is not None:
            recorded_generation_id = self._require_durable_record_id(
                recorded_generation_id,
                field="recorded_generation_id",
            )
        candidate_generation_id = self._require_durable_record_id(
            candidate_generation_id,
            field="candidate_generation_id",
        )
        self.initialize()
        closing_path = (
            self._storage_root
            / "owner_sessions"
            / f"{self._label_key(owner_session_id, domain='owner-session')}.closing.json"
        )
        with self._lock:
            active = self._read_owner_session_transition_record(
                self._owner_session_active_path(owner_session_id)
            )
            closing = self._read_owner_session_transition_record(closing_path)
            active_generation = self._validate_owner_session_active_record(
                owner_session_id,
                active,
            )
            closing_generation = self._validate_owner_session_closing_record(
                owner_session_id,
                closing,
            )
            if active_generation is not None:
                if closing_generation is not None:
                    previous_generation = (
                        None if active is None else active.get("previous_session_generation_id")
                    )
                    closure = self.get_owner_session_closed(
                        owner_session_id,
                        session_generation_id=closing_generation,
                    )
                    if (
                        previous_generation != closing_generation
                        or recorded_generation_id not in {None, closing_generation}
                        or closure is None
                        or closure.residual_resource_ids
                    ):
                        raise QueueConflictError(
                            f"owner session has an unfinished generation transition: "
                            f"{owner_session_id}"
                        )
                    _unlink_durable_path(closing_path, missing_ok=True)
                    return active_generation
                if recorded_generation_id not in {None, active_generation}:
                    previous_generation = (
                        None if active is None else active.get("previous_session_generation_id")
                    )
                    previous_closure = (
                        self.get_owner_session_closed(
                            owner_session_id,
                            session_generation_id=previous_generation,
                        )
                        if isinstance(previous_generation, str)
                        else None
                    )
                    if (
                        recorded_generation_id != previous_generation
                        or previous_closure is None
                        or previous_closure.residual_resource_ids
                    ):
                        raise QueueConflictError(
                            f"recorded owner session generation does not match active core state: "
                            f"{owner_session_id}"
                        )
                return active_generation
            if closing_generation is not None:
                if recorded_generation_id != closing_generation:
                    raise QueueConflictError(
                        f"recorded owner session generation does not match closure state: "
                        f"{owner_session_id}"
                    )
                closure = self.get_owner_session_closed(
                    owner_session_id,
                    session_generation_id=closing_generation,
                )
                if closure is None or closure.residual_resource_ids:
                    raise QueueConflictError(
                        f"owner session generation is not safely closed: {owner_session_id}"
                    )
                if candidate_generation_id == closing_generation:
                    raise QueueConflictError(
                        f"new owner session generation must differ from the closed generation: "
                        f"{owner_session_id}"
                    )
                selected_generation = candidate_generation_id
                previous_generation_id: str | None = closing_generation
            else:
                if recorded_generation_id is not None:
                    raise QueueConflictError(
                        f"recorded owner session generation has no core state: {owner_session_id}"
                    )
                selected_generation = candidate_generation_id
                previous_generation_id = None
            self._write_json(
                self._owner_session_active_path(owner_session_id),
                {
                    "owner_session_id": owner_session_id,
                    "session_generation_id": selected_generation,
                    "previous_session_generation_id": previous_generation_id,
                    "active": True,
                    "updated_at": utc_now().isoformat(),
                },
            )
            if closing_generation is not None:
                _unlink_durable_path(closing_path, missing_ok=True)
            return selected_generation

    def clear_owner_session_closing(
        self,
        owner_session_id: str,
        *,
        session_generation_id: str,
    ) -> None:
        """Assert an exact active generation; never erase a closing transition."""
        if not owner_session_id:
            raise ValueError("owner_session_id must not be empty")
        session_generation_id = self._require_durable_record_id(
            session_generation_id,
            field="session_generation_id",
        )
        self.initialize()
        path = (
            self._storage_root
            / "owner_sessions"
            / f"{self._label_key(owner_session_id, domain='owner-session')}.closing.json"
        )
        with self._lock:
            if self._read_owner_session_transition_record(path) is not None:
                raise QueueConflictError(
                    f"owner session closing state cannot be cleared by resume: {owner_session_id}"
                )
            if self._owner_session_active_generation(owner_session_id) != (session_generation_id):
                raise QueueConflictError(
                    f"owner session active generation does not match resume: {owner_session_id}"
                )

    def reopen_owner_session(
        self,
        owner_session_id: str,
        *,
        previous_session_generation_id: str,
        session_generation_id: str,
    ) -> None:
        """Activate a new generation only after exact prior-generation closure."""
        if not owner_session_id:
            raise ValueError("owner_session_id must not be empty")
        previous_session_generation_id = self._require_durable_record_id(
            previous_session_generation_id,
            field="previous_session_generation_id",
        )
        session_generation_id = self._require_durable_record_id(
            session_generation_id,
            field="session_generation_id",
        )
        selected = self.prepare_owner_session_start(
            owner_session_id,
            recorded_generation_id=previous_session_generation_id,
            candidate_generation_id=session_generation_id,
        )
        if selected != session_generation_id:
            raise QueueConflictError(
                f"owner session reopen selected an existing generation: {owner_session_id}"
            )

    def set_owner_session_closed(
        self,
        owner_session_id: str,
        *,
        session_generation_id: str,
        residual_resource_ids: list[str] | None = None,
        legacy_unversioned_job_ids: list[str] | None = None,
    ) -> OwnerSessionClosure:
        """Record verified teardown completion for an owner session generation."""
        if not owner_session_id:
            raise ValueError("owner_session_id must not be empty")
        session_generation_id = self._require_durable_record_id(
            session_generation_id,
            field="session_generation_id",
        )
        raw_legacy_job_ids = legacy_unversioned_job_ids or []
        legacy_job_ids = sorted(set(raw_legacy_job_ids))
        if raw_legacy_job_ids != legacy_job_ids:
            raise ValueError("legacy_unversioned_job_ids must be unique and sorted")
        if len(legacy_job_ids) > 1_000:
            raise ValueError("legacy_unversioned_job_ids cannot exceed 1000 entries")
        if any(not _safe_owner_legacy_job_id(job_id) for job_id in legacy_job_ids):
            raise ValueError("legacy_unversioned_job_ids contains an unsafe job id")
        if legacy_job_ids and residual_resource_ids:
            raise QueueConflictError(
                "legacy jobs cannot be covered while owner-session resources remain"
            )
        self.initialize()
        closing_path = (
            self._storage_root
            / "owner_sessions"
            / f"{self._label_key(owner_session_id, domain='owner-session')}.closing.json"
        )
        with self._lock:
            try:
                raw_closing = self._read_json_document(closing_path)
            except (FileNotFoundError, OSError, QueueConflictError):
                raw_closing = None
            if not isinstance(raw_closing, dict):
                raise QueueConflictError(
                    f"owner session must be closing before it can be closed: {owner_session_id}"
                )
            closing = cast(dict[str, object], raw_closing)
            closing_generation = closing.get("session_generation_id")
            if (
                closing.get("owner_session_id") != owner_session_id
                or closing.get("closing") is not True
                or (closing_generation is not None and not isinstance(closing_generation, str))
            ):
                raise QueueConflictError(
                    f"owner session closing proof is invalid: {owner_session_id}"
                )
            if session_generation_id != closing_generation:
                raise QueueConflictError(
                    f"owner session generation changed before closure: {owner_session_id}"
                )
            closure = OwnerSessionClosure(
                owner_session_id=owner_session_id,
                session_generation_id=session_generation_id,
                residual_resource_ids=residual_resource_ids or [],
            )
            for legacy_job_id in legacy_job_ids:
                legacy_job = self.get_job(legacy_job_id)
                if (
                    legacy_job.metadata.get("owner_session_id") != owner_session_id
                    or legacy_job.metadata.get("owner_session_generation_id") is not None
                ):
                    raise QueueConflictError(
                        f"legacy owner-session coverage identity mismatch: {legacy_job_id}"
                    )
            closure_path = self._owner_session_closed_path(
                owner_session_id,
                session_generation_id=session_generation_id,
            )
            active_generation = self._owner_session_active_generation(owner_session_id)
            existing_closure = self._read_optional(closure_path, OwnerSessionClosure)
            if active_generation not in {None, session_generation_id} or (
                active_generation is None and existing_closure is None
            ):
                raise QueueConflictError(
                    f"owner session active generation does not match closure: {owner_session_id}"
                )
            closure = self._write_immutable_owner_session_closure_unlocked(
                closure_path,
                closure,
            )
            if legacy_job_ids:
                legacy_closure = OwnerSessionClosure(
                    owner_session_id=owner_session_id,
                    session_generation_id=None,
                    covered_by_session_generation_id=session_generation_id,
                    covered_legacy_job_ids=legacy_job_ids,
                )
                self._write_immutable_owner_session_closure_unlocked(
                    self._owner_session_closed_path(owner_session_id),
                    legacy_closure,
                )
            _unlink_durable_path(
                self._owner_session_active_path(owner_session_id),
                missing_ok=True,
            )
            if not closing_path.is_file():
                raise QueueConflictError(
                    f"owner session closing proof disappeared: {owner_session_id}"
                )
            return closure

    def _write_immutable_owner_session_closure_unlocked(
        self,
        path: Path,
        closure: OwnerSessionClosure,
    ) -> OwnerSessionClosure:
        for attempt in range(OWNER_SESSION_CLOSURE_WRITE_ATTEMPTS):
            existing = self._read_optional(path, OwnerSessionClosure)
            if existing is not None:
                if existing != closure.model_copy(update={"closed_at": existing.closed_at}):
                    raise QueueConflictError(
                        f"owner session closure history changed: {closure.owner_session_id}"
                    )
                return existing
            try:
                self._write(path, closure)
            except FileNotFoundError as exc:
                if attempt + 1 >= OWNER_SESSION_CLOSURE_WRITE_ATTEMPTS:
                    raise QueueConflictError(
                        "owner session closure directory did not remain available: "
                        f"{closure.owner_session_id}"
                    ) from exc
                continue
            persisted = self._read_optional(path, OwnerSessionClosure)
            if persisted is None:
                if attempt + 1 >= OWNER_SESSION_CLOSURE_WRITE_ATTEMPTS:
                    raise QueueConflictError(
                        f"owner session closure did not remain durable: {closure.owner_session_id}"
                    )
                continue
            if persisted != closure.model_copy(update={"closed_at": persisted.closed_at}):
                raise QueueConflictError(
                    f"owner session closure history changed: {closure.owner_session_id}"
                )
            return persisted
        raise AssertionError("owner session closure retry loop exhausted without an outcome")

    def get_owner_session_closed(
        self,
        owner_session_id: str,
        *,
        session_generation_id: str | None = None,
    ) -> OwnerSessionClosure | None:
        """Return exact verified closure history for one owner-session generation."""
        if not owner_session_id:
            raise ValueError("owner_session_id must not be empty")
        if session_generation_id is not None:
            session_generation_id = self._require_durable_record_id(
                session_generation_id,
                field="session_generation_id",
            )
        self.initialize()
        closure = self._read_optional(
            self._owner_session_closed_path(
                owner_session_id,
                session_generation_id=session_generation_id,
            ),
            OwnerSessionClosure,
        )
        if closure is None:
            return None
        if (
            closure.owner_session_id != owner_session_id
            or closure.session_generation_id != session_generation_id
        ):
            raise QueueConflictError(f"owner session closure identity mismatch: {owner_session_id}")
        return closure

    def _owner_session_closed_path(
        self,
        owner_session_id: str,
        *,
        session_generation_id: str | None = None,
    ) -> Path:
        if session_generation_id is None:
            return (
                self._storage_root
                / "owner_sessions"
                / f"{self._label_key(owner_session_id, domain='owner-session')}.closed.json"
            )
        path = (
            self._storage_root
            / "owner_sessions"
            / f"{self._label_key(owner_session_id, domain='owner-session')}.closures"
            / f"{_stable_ref_token(session_generation_id)}.json"
        )
        return path

    def _owner_session_active_path(self, owner_session_id: str) -> Path:
        return (
            self._storage_root
            / "owner_sessions"
            / f"{self._label_key(owner_session_id, domain='owner-session')}.active.json"
        )

    def _owner_session_membership_dir(
        self,
        owner_session_id: str,
        *,
        session_generation_id: str | None,
    ) -> Path:
        owner_token = _stable_ref_token(owner_session_id)
        if session_generation_id is None:
            return self._storage_root / "owner_session_legacy_jobs" / owner_token
        return (
            self._storage_root
            / "owner_session_jobs"
            / owner_token
            / _stable_ref_token(session_generation_id)
        )

    def _assert_owner_session_intake_open_unlocked(
        self,
        metadata: dict[str, object],
        *,
        require_active: bool = False,
    ) -> None:
        """Enforce owner generation and closing state at the durable write boundary."""
        identity = _owner_session_identity(metadata, allow_legacy=False)
        if identity is None:
            return
        owner_session_id, session_generation_id = identity
        admission_session_id = metadata.get("owner_session_admission_id", owner_session_id)
        if not isinstance(admission_session_id, str) or not _safe_global_record_id(
            admission_session_id
        ):
            raise QueueConflictError("owner_session_admission_id must be a safe identifier")
        closing_path = (
            self._storage_root
            / "owner_sessions"
            / f"{self._durable_key(admission_session_id)}.closing.json"
        )
        closing = self._read_owner_session_transition_record(closing_path)
        if self._validate_owner_session_closing_record(admission_session_id, closing) is not None:
            raise QueueConflictError(
                f"owner session generation is closing and rejects new work: {owner_session_id}"
            )
        active_generation = self._owner_session_active_generation(admission_session_id)
        if require_active and active_generation is None:
            raise QueueConflictError(
                f"owner session generation has no active admission state: {owner_session_id}"
            )
        if active_generation is not None and active_generation != session_generation_id:
            raise QueueConflictError(
                f"owner session generation does not match active intake: {owner_session_id}"
            )
        if (
            self.get_owner_session_closed(
                admission_session_id,
                session_generation_id=session_generation_id,
            )
            is not None
        ):
            raise QueueConflictError(
                f"owner session generation is already closed: {owner_session_id}"
            )

    def _sync_owner_session_job_membership_unlocked(self, job: RelayJob) -> None:
        """Persist generation membership independently of active/terminal job state."""
        identity = _owner_session_identity(job.metadata, allow_legacy=True)
        if identity is None:
            return
        owner_session_id, session_generation_id = identity
        membership = OwnerSessionJobMembership(
            owner_session_id=owner_session_id,
            session_generation_id=session_generation_id,
            job_id=job.job_id,
            cluster=job.cluster,
            state=job.state,
            created_at=job.created_at,
            updated_at=job.updated_at,
        )
        directory = self._owner_session_membership_dir(
            owner_session_id,
            session_generation_id=session_generation_id,
        )
        target = directory / f"{_stable_ref_token(job.job_id)}.json"
        if not target.exists():
            count, over_capacity = _bounded_regular_json_count(
                directory,
                limit=MAX_ACTIVE_JOB_RECORDS,
                label="owner-session job membership",
            )
            if over_capacity or count >= MAX_ACTIVE_JOB_RECORDS:
                raise QueueConflictError(
                    "owner_session_job_capacity_reached: owner-session generation job "
                    f"capacity {MAX_ACTIVE_JOB_RECORDS} reached"
                )
        self._write(target, membership)

    def _owner_session_active_generation(self, owner_session_id: str) -> str | None:
        active = self._read_owner_session_transition_record(
            self._owner_session_active_path(owner_session_id)
        )
        return self._validate_owner_session_active_record(owner_session_id, active)

    @staticmethod
    def _validate_owner_session_active_record(
        owner_session_id: str,
        active: dict[str, object] | None,
    ) -> str | None:
        if active is None:
            return None
        generation = active.get("session_generation_id")
        previous_generation = active.get("previous_session_generation_id")
        if (
            active.get("owner_session_id") != owner_session_id
            or active.get("active") is not True
            or not isinstance(generation, str)
            or not _safe_global_record_id(generation)
            or (previous_generation is not None and not _safe_global_record_id(previous_generation))
        ):
            raise QueueConflictError(f"owner session active record is invalid: {owner_session_id}")
        return generation

    @staticmethod
    def _validate_owner_session_closing_record(
        owner_session_id: str,
        closing: dict[str, object] | None,
    ) -> str | None:
        if closing is None:
            return None
        generation = closing.get("session_generation_id")
        if (
            closing.get("owner_session_id") != owner_session_id
            or closing.get("closing") is not True
            or not isinstance(generation, str)
            or not _safe_global_record_id(generation)
        ):
            raise QueueConflictError(f"owner session closing record is invalid: {owner_session_id}")
        return generation

    @staticmethod
    def _validate_owner_session_cleanup_intent(
        owner_session_id: str,
        session_generation_id: str,
        raw_intent: object,
        *,
        required: bool,
    ) -> dict[str, object] | None:
        """Validate the immutable policy attached to one closing generation."""
        if raw_intent is None and not required:
            return None
        if not isinstance(raw_intent, dict):
            raise QueueConflictError(f"owner session cleanup intent is invalid: {owner_session_id}")
        intent = cast(dict[str, object], raw_intent)
        operation_id = intent.get("operation_id")
        created_at = intent.get("created_at")
        stop_worker = intent.get("stop_worker")
        cancel_jobs = intent.get("cancel_jobs")
        cancel_scheduler_jobs = intent.get("cancel_scheduler_jobs")
        if (
            intent.get("schema_version") != "clio-relay.owner-session-cleanup-intent.v1"
            or intent.get("owner_session_id") != owner_session_id
            or intent.get("session_generation_id") != session_generation_id
            or not isinstance(operation_id, str)
            or not operation_id.startswith("cleanup_")
            or not _safe_global_record_id(operation_id)
            or not isinstance(created_at, str)
            or not isinstance(stop_worker, bool)
            or not isinstance(cancel_jobs, bool)
            or not isinstance(cancel_scheduler_jobs, bool)
            or (cancel_scheduler_jobs and not cancel_jobs)
        ):
            raise QueueConflictError(f"owner session cleanup intent is invalid: {owner_session_id}")
        try:
            parsed_created_at = datetime.fromisoformat(created_at)
        except ValueError as exc:
            raise QueueConflictError(
                f"owner session cleanup intent time is invalid: {owner_session_id}"
            ) from exc
        if parsed_created_at.tzinfo is None:
            raise QueueConflictError(
                f"owner session cleanup intent time is naive: {owner_session_id}"
            )
        return intent

    def _read_owner_session_transition_record(self, path: Path) -> dict[str, object] | None:
        try:
            raw = self._read_json_document(path)
        except FileNotFoundError:
            return None
        if not isinstance(raw, dict):
            raise QueueConflictError(f"owner session transition record is invalid: {path}")
        return cast(dict[str, object], raw)

    def owner_session_is_closing(self, owner_session_id: str) -> bool:
        """Return whether new work is quiesced for an owned relay session."""
        if not owner_session_id:
            raise ValueError("owner_session_id must not be empty")
        self.initialize()
        path = (
            self._storage_root
            / "owner_sessions"
            / f"{self._label_key(owner_session_id, domain='owner-session')}.closing.json"
        )
        try:
            payload = self._read_json_document(path)
        except (FileNotFoundError, QueueConflictError, OSError):
            return False
        if not isinstance(payload, dict):
            return False
        document = cast(dict[str, object], payload)
        return (
            document.get("owner_session_id") == owner_session_id and document.get("closing") is True
        )

    def close_gateway_session(self, session_id: str) -> GatewaySession:
        """Mark a gateway session closed."""
        session_id = self._require_durable_record_id(session_id, field="session_id")
        return self.update_gateway_session(session_id, state=GatewaySessionState.CLOSED)

    def append_monitor_rule(self, rule: MonitorRule) -> MonitorRule:
        """Create a durable monitor rule."""
        self._require_durable_record_id(rule.rule_id, field="rule_id")
        self._require_durable_record_id(rule.job_id, field="job_id")
        self.initialize()
        self._require_index_migration_complete()
        with self._lock:
            self.get_job(rule.job_id)
            self._ensure_global_order_entry_unlocked("monitor_rules", rule.rule_id)
            self._write(self._storage_root / "monitor_rules" / f"{rule.rule_id}.json", rule)
            self._sync_monitor_rule_indexes_unlocked(rule)
            self.append_event(
                rule.job_id,
                "monitor.rule.created",
                f"Monitor rule created: {rule.rule_id}",
                locked=True,
                payload={"rule_id": rule.rule_id, "pattern": rule.pattern},
            )
        return rule

    def list_monitor_rules(self, job_id: str | None = None) -> list[MonitorRule]:
        """Return monitor rules, optionally filtered by job id."""
        if job_id is not None:
            job_id = self._require_durable_record_id(job_id, field="job_id")
        self.initialize()
        if job_id is not None and self._job_index_exists(job_id):
            rules = list(
                self._read_many(
                    self._storage_root / "monitor_rules_by_job" / self._durable_key(job_id),
                    MonitorRule,
                    identity_field="rule_id",
                )
            )
        else:
            rules = list(
                self._read_many(
                    self._storage_root / "monitor_rules",
                    MonitorRule,
                    identity_field="rule_id",
                )
            )
            if job_id is not None:
                rules = [rule for rule in rules if rule.job_id == job_id]
        return sorted(rules, key=lambda rule: rule.created_at)

    def list_monitor_rules_page(
        self,
        *,
        cursor: int = 1,
        limit: int = 100,
        job_id: str | None = None,
        enabled: bool | None = None,
    ) -> tuple[list[MonitorRule], int | None, int]:
        """Read one global monitor-rule source window with in-window filters."""
        if job_id is not None:
            job_id = self._require_durable_record_id(job_id, field="job_id")

        def matches(rule: MonitorRule) -> bool:
            return (job_id is None or rule.job_id == job_id) and (
                enabled is None or rule.enabled is enabled
            )

        return self._read_global_order_page(
            family="monitor_rules",
            model=MonitorRule,
            identity_field="rule_id",
            cursor=cursor,
            limit=limit,
            predicate=matches,
        )

    def scan_monitor_rules(
        self,
        *,
        limit: int,
        job_id: str | None = None,
        enabled: bool | None = None,
    ) -> tuple[list[MonitorRule], bool]:
        """Read one bounded monitor-rule source window and truncation state."""
        if job_id is not None:
            job_id = self._require_durable_record_id(job_id, field="job_id")

        def matches(rule: MonitorRule) -> bool:
            return (job_id is None or rule.job_id == job_id) and (
                enabled is None or rule.enabled is enabled
            )

        return self._scan_global_order(
            family="monitor_rules",
            model=MonitorRule,
            identity_field="rule_id",
            limit=limit,
            predicate=matches,
        )

    def update_monitor_rule(self, rule: MonitorRule) -> MonitorRule:
        """Persist a monitor rule update."""
        self._require_durable_record_id(rule.rule_id, field="rule_id")
        self._require_durable_record_id(rule.job_id, field="job_id")
        self.initialize()
        self._require_index_migration_complete()
        with self._lock:
            existing = self._read_optional(
                self._storage_root / "monitor_rules" / f"{rule.rule_id}.json",
                MonitorRule,
            )
            if existing is None:
                raise NotFoundError(f"monitor rule not found: {rule.rule_id}")
            if existing.rule_id != rule.rule_id:
                raise QueueConflictError(
                    f"canonical monitor rule identity mismatch: {rule.rule_id}"
                )
            if existing.job_id != rule.job_id:
                raise QueueConflictError(f"monitor rule cannot change job: {rule.rule_id}")
            self._ensure_global_order_entry_unlocked("monitor_rules", rule.rule_id)
            self._write(self._storage_root / "monitor_rules" / f"{rule.rule_id}.json", rule)
            self._sync_monitor_rule_indexes_unlocked(rule)
        return rule

    def _sync_task_retention_indexes_unlocked(self, task: RelayTask) -> None:
        active_path = self._job_record_path(
            "active_tasks_by_job",
            task.job_id,
            task.task_id,
        )
        if task.state in TERMINAL_STATES:
            _unlink_durable_path(active_path, missing_ok=True)
        else:
            self._write(active_path, task)
        self._sync_scheduler_source_unlocked(
            task.job_id,
            source_id=f"task:{task.task_id}",
            metadata=task.metadata,
        )

    def _sync_monitor_rule_indexes_unlocked(self, rule: MonitorRule) -> None:
        indexed_path = self._job_record_path(
            "monitor_rules_by_job",
            rule.job_id,
            rule.rule_id,
        )
        active_path = self._job_record_path(
            "active_monitor_rules_by_job",
            rule.job_id,
            rule.rule_id,
        )
        self._write(indexed_path, rule)
        if rule.enabled and rule.triggered_at is None:
            self._write(active_path, rule)
        else:
            _unlink_durable_path(active_path, missing_ok=True)

    def _sync_scheduler_source_unlocked(
        self,
        job_id: str,
        *,
        source_id: str,
        metadata: dict[str, object],
    ) -> None:
        scheduler_ids, ambiguous = _metadata_scheduler_gc_state(metadata)
        source_token = _stable_ref_token(source_id)
        manifest_path = self._job_record_path(
            "scheduler_refs_by_job",
            job_id,
            source_token,
        )
        protection_path = self._job_record_path(
            "scheduler_protections_by_job",
            job_id,
            source_token,
        )
        old_ids: set[str] = set()
        try:
            raw_manifest = self._read_json_document(manifest_path)
        except FileNotFoundError:
            raw_manifest = None
        if raw_manifest is not None:
            if not isinstance(raw_manifest, dict):
                raise QueueConflictError(f"scheduler reference is not an object: {manifest_path}")
            manifest = cast(dict[str, object], raw_manifest)
            raw_old_ids = manifest.get("scheduler_ids")
            if not isinstance(raw_old_ids, list) or not all(
                isinstance(value, str) and value for value in cast(list[object], raw_old_ids)
            ):
                raise QueueConflictError(f"scheduler reference is invalid: {manifest_path}")
            old_ids = set(cast(list[str], raw_old_ids))
        for scheduler_id in old_ids - scheduler_ids:
            _unlink_durable_path(
                self._scheduler_reverse_ref_path(scheduler_id, job_id, source_id),
                missing_ok=True,
            )
            gateway_paths = self._bounded_json_record_paths(
                self._gateway_reverse_directory("scheduler", scheduler_id),
                limit=MAX_GATEWAY_INDEX_RECORDS,
                label=f"scheduler gateway reverse index {scheduler_id}",
            )
            for gateway_path in gateway_paths:
                gateway = self._read_json_file(gateway_path, GatewaySession)
                self._unlink_active_gateway_job_ref_unlocked(
                    gateway.session_id,
                    job_id,
                    relation_kind="scheduler",
                    relation_key=scheduler_id,
                    source_id=source_id,
                )
        if scheduler_ids or ambiguous:
            self._write_json(
                manifest_path,
                {
                    "job_id": job_id,
                    "source_id": source_id,
                    "scheduler_ids": sorted(scheduler_ids),
                    "ambiguous": ambiguous,
                },
            )
        else:
            _unlink_durable_path(manifest_path, missing_ok=True)
        if ambiguous:
            self._write_json(
                protection_path,
                {"job_id": job_id, "source_id": source_id, "ambiguous": True},
            )
        else:
            _unlink_durable_path(protection_path, missing_ok=True)
        for scheduler_id in scheduler_ids:
            self._write_json(
                self._scheduler_reverse_ref_path(scheduler_id, job_id, source_id),
                {
                    "scheduler_id": scheduler_id,
                    "job_id": job_id,
                    "source_id": source_id,
                },
            )
            gateway_paths = self._bounded_json_record_paths(
                self._gateway_reverse_directory("scheduler", scheduler_id),
                limit=MAX_GATEWAY_INDEX_RECORDS,
                label=f"scheduler gateway reverse index {scheduler_id}",
            )
            for gateway_path in gateway_paths:
                gateway = self._read_json_file(gateway_path, GatewaySession)
                if gateway.state is not GatewaySessionState.CLOSED:
                    self._link_active_gateway_job_unlocked(
                        gateway,
                        job_id,
                        relation_kind="scheduler",
                        relation_key=scheduler_id,
                        source_id=source_id,
                    )

    def _index_gateway_session_unlocked(self, session: GatewaySession) -> None:
        if session.state is GatewaySessionState.CLOSED:
            return
        for job_id in _gateway_direct_job_ids(session):
            self._link_active_gateway_job_unlocked(
                session,
                job_id,
                relation_kind="direct",
                relation_key=job_id,
            )
        for artifact_id in _gateway_direct_artifact_ids(session):
            self._write_gateway_reverse_ref_unlocked("artifact", artifact_id, session)
            artifact = self._read_optional(
                self._storage_root / "artifacts" / f"{artifact_id}.json",
                ArtifactRef,
            )
            if artifact is not None:
                self._link_active_gateway_job_unlocked(
                    session,
                    artifact.job_id,
                    relation_kind="artifact",
                    relation_key=artifact_id,
                )
        if session.scheduler_job_id:
            scheduler_id = session.scheduler_job_id
            self._write_gateway_reverse_ref_unlocked("scheduler", scheduler_id, session)
            scheduler_paths = self._bounded_json_record_paths(
                self._gateway_scheduler_jobs_directory(scheduler_id),
                limit=MAX_GATEWAY_INDEX_RECORDS,
                label=f"scheduler job reverse index {scheduler_id}",
            )
            for path in scheduler_paths:
                raw_ref = self._read_json_document(path)
                if not isinstance(raw_ref, dict):
                    raise QueueConflictError(f"scheduler reverse reference is invalid: {path}")
                scheduler_ref = cast(dict[str, object], raw_ref)
                job_id = scheduler_ref.get("job_id")
                source_id = scheduler_ref.get("source_id")
                if not isinstance(job_id, str) or not isinstance(source_id, str):
                    raise QueueConflictError(f"scheduler reverse reference is invalid: {path}")
                self._link_active_gateway_job_unlocked(
                    session,
                    job_id,
                    relation_kind="scheduler",
                    relation_key=scheduler_id,
                    source_id=source_id,
                )

    def _sync_gateway_session_derived_unlocked(self, session_id: str) -> None:
        """Clear stale gateway references and rebuild them from the canonical record."""
        session = self._read_optional(
            self._storage_root / "gateway_sessions" / f"{session_id}.json",
            GatewaySession,
        )
        self._unindex_gateway_session_id_unlocked(session_id, preserve=None)
        if session is not None:
            self._index_gateway_session_unlocked(session)

    def _unindex_gateway_session_unlocked(
        self,
        session: GatewaySession,
        *,
        preserve: GatewaySession | None = None,
    ) -> None:
        preserved = (
            preserve
            if preserve is not None and preserve.state is not GatewaySessionState.CLOSED
            else None
        )
        self._unindex_gateway_session_id_unlocked(
            session.session_id,
            preserve=preserved,
        )

    def _unindex_gateway_session_id_unlocked(
        self,
        session_id: str,
        *,
        preserve: GatewaySession | None,
    ) -> None:
        """Remove gateway backlinks by stable identity, optionally preserving live relations."""
        active_backlinks = (
            self._storage_root / "active_gateway_refs_by_session" / self._durable_key(session_id)
        )
        active_paths = self._bounded_json_record_paths(
            active_backlinks,
            limit=MAX_GATEWAY_INDEX_RECORDS,
            label=f"active gateway backlinks {session_id}",
        )
        for path in active_paths:
            raw_ref = self._read_json_document(path)
            if not isinstance(raw_ref, dict):
                raise QueueConflictError(f"gateway job reference is invalid: {path}")
            job_ref = cast(dict[str, object], raw_ref)
            if preserve is not None and _gateway_relation_is_preserved(job_ref, preserve):
                continue
            job_id = job_ref.get("job_id")
            record_name = job_ref.get("record_name")
            if not isinstance(job_id, str) or not isinstance(record_name, str):
                raise QueueConflictError(f"gateway job reference is invalid: {path}")
            _unlink_durable_path(
                self._storage_root
                / "active_gateway_refs_by_job"
                / self._durable_key(job_id)
                / record_name,
                missing_ok=True,
            )
            _unlink_durable_path(path, missing_ok=True)
        reverse_backlinks = (
            self._storage_root / "gateway_reverse_refs_by_session" / self._durable_key(session_id)
        )
        reverse_paths = self._bounded_json_record_paths(
            reverse_backlinks,
            limit=MAX_GATEWAY_INDEX_RECORDS,
            label=f"gateway reverse backlinks {session_id}",
        )
        for path in reverse_paths:
            raw_ref = self._read_json_document(path)
            if not isinstance(raw_ref, dict):
                raise QueueConflictError(f"gateway reverse reference is invalid: {path}")
            reverse_ref = cast(dict[str, object], raw_ref)
            if preserve is not None and _gateway_relation_is_preserved(reverse_ref, preserve):
                continue
            family = reverse_ref.get("family")
            key = reverse_ref.get("relation_key")
            record_name = reverse_ref.get("record_name")
            if (
                family not in {"artifact", "scheduler"}
                or not isinstance(key, str)
                or not isinstance(record_name, str)
            ):
                raise QueueConflictError(f"gateway reverse reference is invalid: {path}")
            _unlink_durable_path(
                self._gateway_reverse_directory(cast(str, family), key) / record_name,
                missing_ok=True,
            )
            _unlink_durable_path(path, missing_ok=True)

    def _write_gateway_reverse_ref_unlocked(
        self,
        relation_kind: str,
        relation_key: str,
        session: GatewaySession,
    ) -> None:
        record_name = f"{self._durable_key(session.session_id)}.json"
        self._write(
            self._gateway_reverse_directory(relation_kind, relation_key) / record_name,
            session,
        )
        self._write_json(
            self._storage_root
            / "gateway_reverse_refs_by_session"
            / self._durable_key(session.session_id)
            / f"{_stable_ref_token(relation_kind, relation_key)}.json",
            {
                "session_id": session.session_id,
                "family": relation_kind,
                "relation_kind": relation_kind,
                "relation_key": relation_key,
                "record_name": record_name,
            },
        )

    def _link_gateways_for_artifact_unlocked(self, artifact: ArtifactRef) -> None:
        gateway_paths = self._bounded_json_record_paths(
            self._gateway_reverse_directory("artifact", artifact.artifact_id),
            limit=MAX_GATEWAY_INDEX_RECORDS,
            label=f"artifact gateway reverse index {artifact.artifact_id}",
        )
        for gateway_path in gateway_paths:
            gateway = self._read_json_file(gateway_path, GatewaySession)
            if gateway.state is not GatewaySessionState.CLOSED:
                self._link_active_gateway_job_unlocked(
                    gateway,
                    artifact.job_id,
                    relation_kind="artifact",
                    relation_key=artifact.artifact_id,
                )

    def _link_active_gateway_job_unlocked(
        self,
        session: GatewaySession,
        job_id: str,
        *,
        relation_kind: str,
        relation_key: str,
        source_id: str | None = None,
    ) -> None:
        token = _stable_ref_token(
            session.session_id,
            relation_kind,
            relation_key,
            source_id or "",
        )
        record_name = f"{token}.json"
        backlink_name = f"{_stable_ref_token(job_id, record_name)}.json"
        document: dict[str, object] = {
            "session_id": session.session_id,
            "job_id": job_id,
            "relation_kind": relation_kind,
            "relation_key": relation_key,
            "source_id": source_id,
            "record_name": record_name,
        }
        self._write_json(
            self._storage_root
            / "active_gateway_refs_by_job"
            / self._durable_key(job_id)
            / record_name,
            document,
        )
        self._write_json(
            self._storage_root
            / "active_gateway_refs_by_session"
            / self._durable_key(session.session_id)
            / backlink_name,
            document,
        )

    def _unlink_active_gateway_job_ref_unlocked(
        self,
        session_id: str,
        job_id: str,
        *,
        relation_kind: str,
        relation_key: str,
        source_id: str | None = None,
    ) -> None:
        record_name = (
            f"{_stable_ref_token(session_id, relation_kind, relation_key, source_id or '')}.json"
        )
        _unlink_durable_path(
            self._storage_root
            / "active_gateway_refs_by_job"
            / self._durable_key(job_id)
            / record_name,
            missing_ok=True,
        )
        _unlink_durable_path(
            self._storage_root
            / "active_gateway_refs_by_session"
            / self._durable_key(session_id)
            / f"{_stable_ref_token(job_id, record_name)}.json",
            missing_ok=True,
        )

    def _gateway_reverse_directory(self, relation_kind: str, relation_key: str) -> Path:
        if relation_kind not in {"artifact", "scheduler"}:
            raise QueueConflictError(f"unsupported gateway reference kind: {relation_kind}")
        return self._storage_root / f"gateways_by_{relation_kind}" / _stable_ref_token(relation_key)

    def _gateway_scheduler_jobs_directory(self, scheduler_id: str) -> Path:
        return self._storage_root / "scheduler_jobs" / _stable_ref_token(scheduler_id)

    def _scheduler_reverse_ref_path(
        self,
        scheduler_id: str,
        job_id: str,
        source_id: str,
    ) -> Path:
        return (
            self._gateway_scheduler_jobs_directory(scheduler_id)
            / f"{_stable_ref_token(job_id, source_id)}.json"
        )

    def _append_event_unlocked(
        self,
        job_id: str,
        event_type: str,
        message: str,
        payload: dict[str, object],
    ) -> RelayEvent:
        event_dir = self._storage_root / "events" / job_id
        event_dir.mkdir(parents=True, exist_ok=True)
        seq = self._next_event_seq(job_id, event_dir)
        event = RelayEvent(
            job_id=job_id,
            seq=seq,
            event_type=event_type,
            message=message,
            payload=payload,
        )
        self._write(event_dir / f"{seq:020d}.json", event)
        self._update_job_index_unlocked(job_id, latest_event_seq=seq)
        return event

    def latest_job_event(self, job_id: str) -> tuple[RelayEvent | None, bool]:
        """Read the exact indexed event head without enumerating the event directory."""
        job_id = self._require_durable_record_id(job_id, field="job_id")
        index = self._read_job_index(job_id)
        if index is not None:
            latest_seq = _index_integer(index, "latest_event_seq")
            if latest_seq == 0:
                return None, False
            event = self._read_optional(
                self._storage_root / "events" / job_id / f"{latest_seq:020d}.json",
                RelayEvent,
            )
            if event is None:
                raise QueueConflictError(f"event index points to a missing record: {job_id}")
            if event.job_id != job_id or event.seq != latest_seq:
                raise QueueConflictError(f"event index identity mismatch: {job_id}")
            return event, False
        event_dir = self._storage_root / "events" / job_id
        latest: RelayEvent | None = None
        for seq in range(1, DEFAULT_EXACT_RECORD_LIMIT + 1):
            event = self._read_optional(event_dir / f"{seq:020d}.json", RelayEvent)
            if event is None:
                return latest, False
            if event.job_id != job_id or event.seq != seq:
                raise QueueConflictError(f"event filename/content identity mismatch: {job_id}")
            latest = event
        truncated = (event_dir / f"{DEFAULT_EXACT_RECORD_LIMIT + 1:020d}.json").exists()
        return latest, truncated

    def _next_event_seq(self, job_id: str, event_dir: Path) -> int:
        index = self._read_job_index(job_id)
        if index is not None:
            candidate = _index_integer(index, "latest_event_seq") + 1
            while (event_dir / f"{candidate:020d}.json").exists():
                candidate += 1
                if candidate > DEFAULT_EXACT_RECORD_LIMIT + _index_integer(
                    index, "latest_event_seq"
                ):
                    raise QueueConflictError(f"event head recovery exceeded bound: {job_id}")
            return candidate
        for candidate in range(1, DEFAULT_EXACT_RECORD_LIMIT + 1):
            if not (event_dir / f"{candidate:020d}.json").exists():
                return candidate
        raise QueueConflictError(f"legacy event sequence requires index migration: {job_id}")

    def _next_task_event_seq(self, task_id: str, directory: Path) -> int:
        head_path = self._storage_root / "task_event_heads" / f"{task_id}.json"
        try:
            raw = self._read_json_document(head_path)
        except FileNotFoundError:
            return _last_contiguous_sequence(directory) + 1
        except (OSError, QueueConflictError) as exc:
            raise QueueConflictError(f"invalid task event head {head_path}: {exc}") from exc
        if not isinstance(raw, dict):
            raise QueueConflictError(f"task event head is not an object: {head_path}")
        head = cast(dict[str, object], raw)
        latest_seq = head.get("latest_seq")
        if (
            head.get("task_id") != task_id
            or not isinstance(latest_seq, int)
            or isinstance(latest_seq, bool)
            or latest_seq < 0
        ):
            raise QueueConflictError(f"invalid task event head identity: {head_path}")
        candidate = latest_seq + 1
        for _ in range(DEFAULT_EXACT_RECORD_LIMIT):
            if not (directory / f"{candidate:020d}.json").exists():
                return candidate
            candidate += 1
        raise QueueConflictError(f"task event head recovery exceeded bound: {task_id}")

    def _active_lease_for_endpoint(
        self,
        endpoint_id: str,
        *,
        expiry_refs: list[_LeaseExpiryReference] | None = None,
    ) -> Lease | None:
        if expiry_refs is None:
            expiry_refs, expiry_truncated = self._scan_expiry_refs(limit=MAX_LIVE_LEASE_RECORDS)
            if expiry_truncated:
                raise QueueConflictError("lease expiry index exceeded its safety bound")
        lease_refs, truncated = self._scan_lease_endpoint_refs(
            endpoint_id,
            limit=MAX_LIVE_LEASE_RECORDS,
        )
        if truncated:
            raise QueueConflictError("lease endpoint index exceeded its safety bound")
        endpoint_token = _lease_endpoint_token(endpoint_id)
        expected_refs = [
            (lease_token, identity_token)
            for (
                _expires,
                _cluster_token,
                _kind,
                indexed_endpoint_token,
                _job_token,
                lease_token,
                identity_token,
            ) in expiry_refs
            if indexed_endpoint_token == endpoint_token
        ]
        if len(expected_refs) != len(set(expected_refs)):
            raise QueueConflictError(
                f"lease expiry index duplicates endpoint identity: {endpoint_id}"
            )
        if set(lease_refs) != set(expected_refs):
            raise QueueConflictError(f"lease endpoint and expiry indexes disagree: {endpoint_id}")
        active: list[Lease] = []
        for lease_token, identity_token in lease_refs:
            identity = self._read_lease_index_identity_by_token(
                lease_token,
                identity_token,
            )
            lease = self._read_optional(
                self._storage_root / "leases" / f"{identity.lease_id}.json",
                Lease,
            )
            if lease is None:
                raise QueueConflictError(f"lease endpoint index is orphaned: {identity.lease_id}")
            self._validate_lease_index_identity(lease, identity)
            if identity.endpoint_id != endpoint_id:
                raise QueueConflictError(
                    f"lease endpoint index identity mismatch: {identity.lease_id}"
                )
            self._require_empty_lease_ref(
                self._lease_identity_ref_path(identity),
                label="lease identity reference",
            )
            self._require_empty_lease_ref(
                self._lease_cluster_kind_ref_path(identity),
                label="lease cluster-kind reference",
            )
            self._require_empty_lease_ref(
                self._lease_expiry_ref_path(identity),
                label="lease expiry reference",
            )
            if not lease.is_expired():
                active.append(lease)
        if len(active) > 1:
            raise QueueConflictError(f"endpoint has multiple active durable leases: {endpoint_id}")
        return active[0] if active else None

    def _due_expired_leases_unlocked(
        self,
        *,
        cluster: str,
        now: datetime,
        refs: list[_LeaseExpiryReference] | None = None,
    ) -> list[Lease]:
        if refs is None:
            refs, truncated = self._scan_expiry_refs(limit=MAX_LIVE_LEASE_RECORDS)
            if truncated:
                raise QueueConflictError("lease recovery index exceeded its safety bound")
        due_key = _lease_expiry_key(now)
        cluster_token = _lease_cluster_token(cluster)
        due: list[Lease] = []
        for (
            expires_key,
            indexed_cluster,
            kind,
            endpoint_token,
            job_token,
            lease_token,
            identity_token,
        ) in refs:
            if indexed_cluster != cluster_token or expires_key > due_key:
                continue
            identity = self._read_lease_index_identity_by_token(
                lease_token,
                identity_token,
            )
            lease = self._read_optional(
                self._storage_root / "leases" / f"{identity.lease_id}.json",
                Lease,
            )
            if lease is None:
                raise QueueConflictError(f"lease expiry index is orphaned: {identity.lease_id}")
            self._validate_lease_index_identity(lease, identity)
            if (
                identity.cluster != cluster
                or identity.job_kind != kind
                or _lease_endpoint_token(identity.endpoint_id) != endpoint_token
                or _lease_job_token(identity.job_id) != job_token
                or _lease_expiry_key(identity.expires_at) != expires_key
            ):
                raise QueueConflictError(
                    f"lease expiry index identity mismatch: {identity.lease_id}"
                )
            self._require_empty_lease_ref(
                self._lease_identity_ref_path(identity),
                label="lease identity reference",
            )
            self._require_empty_lease_ref(
                self._lease_endpoint_ref_path(identity),
                label="lease endpoint reference",
            )
            self._require_empty_lease_ref(
                self._lease_endpoint_guard_path(identity),
                label="lease endpoint guard",
            )
            self._require_empty_lease_ref(
                self._lease_cluster_kind_ref_path(identity),
                label="lease cluster-kind reference",
            )
            if lease.is_expired(now):
                due.append(lease)
        return sorted(due, key=lambda lease: (lease.expires_at, lease.lease_id))

    def _recover_stale_jobs_unlocked(self, *, cluster: str, max_attempts: int) -> list[RelayJob]:
        refs, truncated = self._scan_expiry_refs(limit=MAX_LIVE_LEASE_RECORDS)
        if truncated:
            raise QueueConflictError("lease recovery index exceeded its safety bound")
        recovered, _changed = self._recover_stale_jobs_from_expiry_refs_unlocked(
            cluster=cluster,
            max_attempts=max_attempts,
            refs=refs,
        )
        return recovered

    def _recover_stale_jobs_from_expiry_refs_unlocked(
        self,
        *,
        cluster: str,
        max_attempts: int,
        refs: list[_LeaseExpiryReference],
    ) -> tuple[list[RelayJob], bool]:
        """Recover from one bounded expiry snapshot and report index mutation."""
        recovered: list[RelayJob] = []
        changed = False
        leases_by_job: dict[str, list[Lease]] = {}
        now = utc_now()
        for lease in self._due_expired_leases_unlocked(
            cluster=cluster,
            now=now,
            refs=refs,
        ):
            leases_by_job.setdefault(lease.job_id, []).append(lease)
        for job_id, leases in leases_by_job.items():
            job = self._read_optional(self._storage_root / "jobs" / f"{job_id}.json", RelayJob)
            if job is None:
                for lease in leases:
                    self._delete_lease_unlocked(lease)
                    changed = True
                continue
            if job.cluster != cluster:
                raise QueueConflictError(
                    f"lease expiry cluster identity mismatch: {job.job_id}/{cluster}"
                )
            if job.state in TERMINAL_STATES or job.state not in {
                JobState.LEASED,
                JobState.RUNNING,
            }:
                for lease in leases:
                    self._delete_lease_unlocked(lease, job=job)
                    changed = True
                continue
            if self._job_has_pending_execution_cleanup_after_migration_unlocked(
                cluster,
                job.job_id,
            ):
                continue
            if self._job_has_scheduler_observation_unlocked(job):
                continue
            updated = self._recover_expired_leases_unlocked(
                job,
                leases,
                max_attempts=max_attempts,
            )
            recovered.append(updated)
            changed = True
        return recovered, changed

    def _job_has_scheduler_observation_unlocked(self, job: RelayJob) -> bool:
        index = self._read_job_index(job.job_id)
        if index is None or index.get("retention_schema_version") != RETENTION_INDEX_SCHEMA:
            raise QueueConflictError(
                f"scheduler observation index is unavailable for job: {job.job_id}"
            )
        for family in ("scheduler_protections_by_job", "scheduler_refs_by_job"):
            paths = self._bounded_json_record_paths(
                self._storage_root / family / self._durable_key(job.job_id),
                limit=MAX_BOUNDED_SCAN_RECORDS,
                label=f"{family} for {job.job_id}",
            )
            if paths:
                return True
        return False

    def _ensure_job_queued_event(self, job: RelayJob) -> None:
        event_dir = self._storage_root / "events" / job.job_id
        if (event_dir / f"{1:020d}.json").is_file():
            return
        self._update_job_index_unlocked(job.job_id, latest_event_seq=0)
        self.append_event(job.job_id, "job.queued", "Job queued", locked=True)

    def _write_committed_idempotency_record(
        self,
        key_path: Path,
        job: RelayJob,
        job_digest: str,
    ) -> None:
        self._write_json(key_path, _committed_idempotency_record(job, job_digest))

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
            / f"{self._durable_key(job_id)}.json"
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
        existing = self._read_optional(pending_path, SchedulerCancelPending)
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
        completed = self._read_optional(completed_path, SchedulerCancelPending)
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
                    if count >= MAX_ACTIVE_JOB_RECORDS:
                        raise QueueConflictError(
                            "scheduler cancellation capacity reached for cluster; retry after "
                            "pending cancellation work drains"
                        )
        except FileNotFoundError:
            pass
        except OSError as exc:
            raise QueueConflictError(
                f"cannot inspect scheduler cancellation capacity: {exc}"
            ) from exc
        record = SchedulerCancelPending(
            job_id=job.job_id,
            cluster=job.cluster,
            requested_at=requested_at,
            reason=reason,
        )
        self._write(pending_path, record)
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
        record = self._read_optional(path, SchedulerCancelPending)
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
            self._write(pending_path, record)
            return record
        completed_path = self._scheduler_cancel_record_path(
            "scheduler_cancel_dispositions",
            record.cluster,
            record.job_id,
        )
        self._write(completed_path, record)
        _unlink_durable_path(pending_path, missing_ok=True)
        return record

    def _ensure_active_job_capacity_unlocked(self, job: RelayJob) -> None:
        """Reject a new active record before it can exceed the serviceable bound."""
        if job.state is not JobState.QUEUED:
            return
        directory = self._storage_root / "jobs_active"
        initial_count, _initial_over_capacity = _bounded_regular_json_count(
            directory,
            limit=MAX_ACTIVE_JOB_RECORDS,
            label="active job index",
        )
        try:
            self._repair_active_job_index_unlocked()
        except (QueueConflictError, ValueError) as exc:
            if initial_count >= MAX_ACTIVE_JOB_RECORDS:
                raise QueueConflictError(
                    "active_job_capacity_reached: active job capacity "
                    f"{MAX_ACTIVE_JOB_RECORDS} reached and the index could not be safely "
                    "reconciled"
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
                    if not stat.S_ISREG(entry_stat.st_mode) or _record_is_reparse(entry_stat):
                        raise QueueConflictError(
                            f"active job index contains an unsafe record: {entry.path}"
                        )
                    count += 1
                    if count >= MAX_ACTIVE_JOB_RECORDS:
                        raise QueueConflictError(
                            "active_job_capacity_reached: active job capacity "
                            f"{MAX_ACTIVE_JOB_RECORDS} reached; cancel or drain active work "
                            "before submitting another job"
                        )
        except OSError as exc:
            raise QueueConflictError(f"cannot inspect active job capacity: {exc}") from exc

    def _index_fresh_endpoint_unlocked(self, endpoint: EndpointRegistration) -> None:
        """Move one endpoint's mutable presence record into its current time bucket."""
        cluster_identity = endpoint.cluster or "__desktop__"
        cluster_token = _stable_ref_token(cluster_identity)
        bucket = _endpoint_fresh_bucket(endpoint.last_seen_at)
        mapping_path = (
            self._storage_root
            / "endpoints_fresh_by_id"
            / f"{_stable_ref_token(endpoint.endpoint_id)}.json"
        )
        previous: dict[str, object] | None = None
        try:
            raw_previous = self._read_json_document(mapping_path)
        except FileNotFoundError:
            raw_previous = None
        if raw_previous is not None:
            if not isinstance(raw_previous, dict):
                raise QueueConflictError(f"fresh endpoint mapping is invalid: {mapping_path}")
            previous = cast(dict[str, object], raw_previous)
            if (
                previous.get("schema_version") != "clio-relay.endpoint-fresh-index.v1"
                or previous.get("endpoint_id") != endpoint.endpoint_id
                or not isinstance(previous.get("cluster_token"), str)
                or isinstance(previous.get("bucket"), bool)
                or not isinstance(previous.get("bucket"), int)
            ):
                raise QueueConflictError(
                    f"fresh endpoint mapping identity mismatch: {mapping_path}"
                )
        target = (
            self._storage_root
            / "endpoints_fresh"
            / cluster_token
            / f"{bucket:020d}"
            / f"{endpoint.endpoint_id}.json"
        )
        if previous is not None:
            previous_target = (
                self._storage_root
                / "endpoints_fresh"
                / cast(str, previous["cluster_token"])
                / f"{cast(int, previous['bucket']):020d}"
                / f"{endpoint.endpoint_id}.json"
            )
            if previous_target != target:
                _unlink_durable_path(previous_target, missing_ok=True)
        self._write(target, endpoint)
        self._write_json(
            mapping_path,
            {
                "schema_version": "clio-relay.endpoint-fresh-index.v1",
                "endpoint_id": endpoint.endpoint_id,
                "cluster_token": cluster_token,
                "bucket": bucket,
                "last_seen_at": endpoint.last_seen_at.isoformat(),
            },
        )

    def _write_job_unlocked(self, job: RelayJob) -> None:
        """Write a canonical job and replayable derived-index transition."""
        self._migrate_execution_cleanup_shard_unlocked(
            job.cluster,
            self._execution_cleanup_shard(job.job_id),
            limit=DEFAULT_EXACT_RECORD_LIMIT + 1,
        )
        intent_path = self._write_transition_intent_unlocked(
            "job_sync",
            job.job_id,
            {
                "job_id": job.job_id,
                "updated_at": job.updated_at.isoformat(),
            },
        )
        self._write(self._storage_root / "jobs" / f"{job.job_id}.json", job)
        self._sync_job_derived_unlocked(job)
        _unlink_durable_path(intent_path, missing_ok=True)

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
            _unlink_durable_path(active_path, missing_ok=True)
            _unlink_durable_path(queued_path, missing_ok=True)
            return
        self._write(active_path, job)
        if job.state is JobState.QUEUED:
            self._write(queued_path, job)
        else:
            _unlink_durable_path(queued_path, missing_ok=True)

    def _write_task_unlocked(self, task: RelayTask) -> None:
        """Write one task and make its per-job and scheduler indexes replayable."""
        intent_path = self._write_transition_intent_unlocked(
            "task_sync",
            task.task_id,
            {"job_id": task.job_id, "task_id": task.task_id},
        )
        self._write(self._storage_root / "tasks" / f"{task.task_id}.json", task)
        self._sync_task_derived_unlocked(task)
        _unlink_durable_path(intent_path, missing_ok=True)

    def _sync_task_derived_unlocked(self, task: RelayTask) -> None:
        """Converge task indexes and scheduler references from the canonical task."""
        self._initialize_job_index_unlocked(task.job_id)
        self._write(
            self._job_record_path("tasks_by_job", task.job_id, task.task_id),
            task,
        )
        if task.sequence is not None:
            self._write_ordered_job_record("task", task.job_id, task.sequence, task)
            index = self._read_job_index(task.job_id)
            if index is not None and _index_integer(index, "task_count") < task.sequence:
                self._update_job_index_unlocked(task.job_id, task_count=task.sequence)
        self._sync_task_retention_indexes_unlocked(task)

    def _write_gateway_session_unlocked(self, session: GatewaySession) -> None:
        """Write one canonical gateway and replayably converge every backlink."""
        intent_path = self._write_transition_intent_unlocked(
            "gateway_sync",
            session.session_id,
            {"session_id": session.session_id},
        )
        self._write(
            self._storage_root / "gateway_sessions" / f"{session.session_id}.json",
            session,
        )
        self._after_gateway_canonical_write(session)
        self._sync_gateway_session_derived_unlocked(session.session_id)
        _unlink_durable_path(intent_path, missing_ok=True)

    def _after_gateway_canonical_write(self, _session: GatewaySession) -> None:
        """Fault-injection seam after a canonical gateway transition."""

    def _write_transition_intent_unlocked(
        self,
        kind: str,
        identity: str,
        payload: dict[str, object],
    ) -> Path:
        """Persist a bounded write-ahead intent before a canonical/index transition."""
        path = (
            self._storage_root
            / "transition_intents"
            / f"{kind}-{_stable_ref_token(kind, identity)}.json"
        )
        self._write_json(
            path,
            {
                "schema_version": "clio-relay.queue-transition-intent.v1",
                "kind": kind,
                "identity": identity,
                "created_at": utc_now().isoformat(),
                "payload": payload,
            },
        )
        return path

    def _recover_pending_transitions_unlocked(self) -> list[RelayJob]:
        """Replay pending intents when the bounded journal is nonempty."""
        if next((self._storage_root / "transition_intents").glob("*.json"), None) is not None:
            return self._reconcile_transition_intents_unlocked()
        return []

    def _reconcile_transition_intents_unlocked(self) -> list[RelayJob]:
        """Replay interrupted queue transitions from canonical records or exact intents."""
        paths = self._bounded_json_record_paths(
            self._storage_root / "transition_intents",
            limit=MAX_TRANSITION_INTENT_RECORDS,
            label="queue transition intent directory",
        )
        intents: list[tuple[Path, dict[str, object]]] = []
        recovered_stale_jobs: list[RelayJob] = []
        for path in paths:
            raw = self._read_json_document(path)
            if not isinstance(raw, dict):
                raise QueueConflictError(f"queue transition intent is not an object: {path}")
            intent = cast(dict[str, object], raw)
            if intent.get("schema_version") != "clio-relay.queue-transition-intent.v1":
                raise QueueConflictError(f"unsupported queue transition intent: {path}")
            if not isinstance(intent.get("kind"), str) or not isinstance(
                intent.get("payload"), dict
            ):
                raise QueueConflictError(f"invalid queue transition intent: {path}")
            intents.append((path, intent))

        order = {
            "lease_index_repair": 0,
            "lease_acquire": 1,
            "lease_sync": 2,
            "lease_delete": 3,
            "stale_lease_recovery": 4,
            "job_sync": 5,
            "task_sync": 6,
            "gateway_sync": 7,
        }
        for path, intent in sorted(
            intents,
            key=lambda item: order.get(cast(str, item[1]["kind"]), 99),
        ):
            kind = cast(str, intent["kind"])
            payload = cast(dict[str, object], intent["payload"])
            if kind == "lease_index_repair":
                self._apply_lease_index_repair_intent_unlocked(path, payload)
                continue
            if kind == "lease_acquire":
                self._reconcile_lease_acquire_intent_unlocked(path, payload)
                continue
            if kind == "lease_sync":
                lease = Lease.model_validate(payload.get("lease"))
                previous = Lease.model_validate(payload.get("previous_lease"))
                job = RelayJob.model_validate(payload.get("job"))
                if lease.job_id != job.job_id or previous.lease_id != lease.lease_id:
                    raise QueueConflictError(f"lease synchronization identity mismatch: {path}")
                self._write(self._storage_root / "leases" / f"{lease.lease_id}.json", lease)
                self._write(
                    self._job_record_path("leases_by_job", lease.job_id, lease.lease_id),
                    lease,
                )
                self._sync_lease_operational_indexes_unlocked(
                    lease,
                    job=job,
                    previous_lease=previous,
                )
                capacity_transition = payload.get("lease_capacity_transition")
                if capacity_transition is not None:
                    self._apply_lease_capacity_transition_unlocked(
                        capacity_transition,
                        target="after",
                        label=f"lease synchronization {lease.lease_id}",
                    )
                    self._before_lease_capacity_intent_removal("lease_sync", path)
                elif self._lease_capacity_migration_complete_unlocked():
                    raise QueueConflictError(
                        f"lease synchronization intent has no capacity transition: {path}"
                    )
                _unlink_durable_path(path, missing_ok=True)
                continue
            if kind == "lease_delete":
                lease_id = payload.get("lease_id")
                job_id = payload.get("job_id")
                if (
                    not isinstance(lease_id, str)
                    or not lease_id
                    or not isinstance(job_id, str)
                    or not job_id
                ):
                    raise QueueConflictError(f"invalid lease deletion intent: {path}")
                lease: Lease | None = None
                identity: _LeaseIndexIdentity | None = None
                if payload.get("lease") is not None or payload.get("index") is not None:
                    lease = Lease.model_validate(payload.get("lease"))
                    identity = _lease_index_identity_from_document(
                        payload.get("index"),
                        label=f"lease deletion index {path}",
                    )
                    self._validate_lease_index_identity(lease, identity)
                    if lease_id != lease.lease_id or job_id != lease.job_id:
                        raise QueueConflictError(f"lease deletion intent identity mismatch: {path}")
                _unlink_durable_path(
                    self._storage_root / "leases" / f"{lease_id}.json",
                    missing_ok=True,
                )
                _unlink_durable_path(
                    self._job_record_path("leases_by_job", job_id, lease_id),
                    missing_ok=True,
                )
                if identity is not None:
                    self._delete_lease_operational_indexes_unlocked(identity)
                capacity_transition = payload.get("lease_capacity_transition")
                if capacity_transition is not None:
                    self._apply_lease_capacity_transition_unlocked(
                        capacity_transition,
                        target="after",
                        label=f"lease deletion {lease_id}",
                    )
                    self._before_lease_capacity_intent_removal("lease_delete", path)
                elif self._lease_capacity_migration_complete_unlocked():
                    raise QueueConflictError(
                        f"lease deletion intent has no capacity transition: {path}"
                    )
                _unlink_durable_path(path, missing_ok=True)
                continue
            if kind == "stale_lease_recovery":
                recovered_stale_jobs.append(
                    self._apply_stale_lease_recovery_intent_unlocked(path, payload)
                )
                continue
            if kind == "job_sync":
                job_id = payload.get("job_id")
                if not isinstance(job_id, str) or not job_id:
                    raise QueueConflictError(f"invalid job transition intent: {path}")
                job = self._read_optional(self._storage_root / "jobs" / f"{job_id}.json", RelayJob)
                if job is not None:
                    self._sync_job_derived_unlocked(job)
                _unlink_durable_path(path, missing_ok=True)
                continue
            if kind == "task_sync":
                task_id = payload.get("task_id")
                if not isinstance(task_id, str) or not task_id:
                    raise QueueConflictError(f"invalid task transition intent: {path}")
                task = self._read_optional(
                    self._storage_root / "tasks" / f"{task_id}.json", RelayTask
                )
                if task is not None:
                    self._sync_task_derived_unlocked(task)
                _unlink_durable_path(path, missing_ok=True)
                continue
            if kind == "gateway_sync":
                session_id = payload.get("session_id")
                if not isinstance(session_id, str) or not session_id:
                    raise QueueConflictError(f"invalid gateway transition intent: {path}")
                self._sync_gateway_session_derived_unlocked(session_id)
                _unlink_durable_path(path, missing_ok=True)
                continue
            raise QueueConflictError(f"unsupported queue transition intent kind {kind!r}: {path}")
        return recovered_stale_jobs

    def _reconcile_lease_acquire_intent_unlocked(
        self,
        path: Path,
        payload: dict[str, object],
    ) -> None:
        """Abort an interrupted lease handoff unless later canonical work superseded it."""
        lease = Lease.model_validate(payload.get("lease"))
        original_job = RelayJob.model_validate(payload.get("original_job"))
        target_job = RelayJob.model_validate(payload.get("target_job"))
        target_updated_at = payload.get("target_updated_at")
        if (
            lease.job_id != original_job.job_id
            or target_job.job_id != original_job.job_id
            or target_job.cluster != original_job.cluster
            or target_job.kind != original_job.kind
            or target_job.state is not JobState.LEASED
            or target_job.leased_by != lease.endpoint_id
            or target_job.updated_at.isoformat() != target_updated_at
            or not isinstance(target_updated_at, str)
        ):
            raise QueueConflictError(f"lease acquisition intent identity mismatch: {path}")
        current = self._read_optional(
            self._storage_root / "jobs" / f"{lease.job_id}.json",
            RelayJob,
        )
        target_is_current = (
            current is not None
            and current.updated_at.isoformat() == target_updated_at
            and current.state is JobState.LEASED
            and current.leased_by == lease.endpoint_id
        )
        if target_is_current:
            self._write(self._storage_root / "jobs" / f"{original_job.job_id}.json", original_job)
            self._sync_job_derived_unlocked(original_job)
        lease_path = self._storage_root / "leases" / f"{lease.lease_id}.json"
        indexed_path = self._job_record_path("leases_by_job", lease.job_id, lease.lease_id)
        identity = self._lease_index_identity(lease, job=original_job)
        preserve_acquisition = (
            current is not None
            and not target_is_current
            and (
                current.state in {JobState.LEASED, JobState.RUNNING}
                and current.leased_by == lease.endpoint_id
            )
        )
        if preserve_acquisition:
            assert current is not None
            self._write(lease_path, lease)
            self._write(indexed_path, lease)
            self._sync_lease_operational_indexes_unlocked(lease, job=current)
        else:
            _unlink_durable_path(lease_path, missing_ok=True)
            _unlink_durable_path(indexed_path, missing_ok=True)
            self._delete_lease_operational_indexes_unlocked(
                identity,
                allow_foreign_manifest=True,
            )
        capacity_transition = payload.get("lease_capacity_transition")
        if capacity_transition is not None:
            self._apply_lease_capacity_transition_unlocked(
                capacity_transition,
                target="after" if preserve_acquisition else "rollback",
                label=f"lease acquisition recovery {lease.lease_id}",
            )
            self._before_lease_capacity_intent_removal("lease_acquire", path)
        elif self._lease_capacity_migration_complete_unlocked():
            raise QueueConflictError(f"lease acquisition intent has no capacity transition: {path}")
        _unlink_durable_path(path, missing_ok=True)

    def _repair_active_job_index_unlocked(self) -> None:
        """Remove stale capacity entries and refresh every indexed active job."""
        paths = self._bounded_json_record_paths(
            self._storage_root / "jobs_active",
            limit=MAX_ACTIVE_JOB_RECORDS,
            label="active job index",
        )
        for path in paths:
            indexed = self._read_json_file(path, RelayJob)
            canonical = self._read_optional(
                self._storage_root / "jobs" / f"{indexed.job_id}.json",
                RelayJob,
            )
            if canonical is None or canonical.state in TERMINAL_STATES:
                _unlink_durable_path(path, missing_ok=True)
                _unlink_durable_path(
                    self._storage_root / "jobs_queued" / f"{indexed.job_id}.json",
                    missing_ok=True,
                )
                continue
            self._write(path, canonical)
            queued_path = self._storage_root / "jobs_queued" / f"{canonical.job_id}.json"
            if canonical.state is JobState.QUEUED:
                self._write(queued_path, canonical)
            else:
                _unlink_durable_path(queued_path, missing_ok=True)

    def _terminal_job_gc_protections(self, job: RelayJob) -> list[str]:
        protections: list[str] = []
        if job.state not in TERMINAL_STATES:
            protections.append("job_not_terminal")
        migration = self._read_index_migration_state()
        if migration.get("complete") is not True:
            protections.append("index_migration_incomplete")
        if job.metadata.get("retention_hold") is True:
            protections.append("retention_hold")
        try:
            pending_execution_cleanup = self._job_has_pending_execution_cleanup_unlocked(
                job.cluster,
                job.job_id,
            )
        except (OSError, ValueError, QueueConflictError):
            protections.append("execution_cleanup_state_ambiguous")
        else:
            if pending_execution_cleanup:
                protections.append("pending_execution_cleanup")
        pending_scheduler_cancel = self._scheduler_cancel_record_path(
            "scheduler_cancel_pending",
            job.cluster,
            job.job_id,
        )
        if pending_scheduler_cancel.is_file():
            protections.append("pending_scheduler_cancellation")
        owner_session_id = job.metadata.get("owner_session_id")
        if isinstance(owner_session_id, str) and owner_session_id:
            expected_generation = job.metadata.get("owner_session_generation_id")
            if expected_generation is not None and not isinstance(expected_generation, str):
                protections.append("owner_session_state_ambiguous")
            else:
                try:
                    closure = self.get_owner_session_closed(
                        owner_session_id,
                        session_generation_id=expected_generation,
                    )
                    covering_closure = (
                        self.get_owner_session_closed(
                            owner_session_id,
                            session_generation_id=closure.covered_by_session_generation_id,
                        )
                        if expected_generation is None and closure is not None
                        else None
                    )
                except (OSError, ValueError, QueueConflictError):
                    protections.append("owner_session_state_ambiguous")
                else:
                    if closure is None:
                        protections.append("owner_session_state_ambiguous")
                    elif expected_generation is None:
                        if covering_closure is None or covering_closure.residual_resource_ids:
                            protections.append("owner_session_legacy_coverage_ambiguous")
                        elif job.job_id not in closure.covered_legacy_job_ids:
                            protections.append("owner_session_legacy_job_not_covered")
                    elif closure.residual_resource_ids:
                        protections.append("owner_session_residual_resources")
        key_path = (
            self._storage_root
            / "idempotency"
            / (f"{_idempotency_key_filename(job.idempotency_key)}.json")
        )
        try:
            raw_idempotency = self._read_json_document(key_path)
        except FileNotFoundError:
            protections.append("idempotency_record_missing")
        else:
            if not isinstance(raw_idempotency, dict):
                protections.append("idempotency_record_ambiguous")
            else:
                idempotency = cast(dict[str, object], raw_idempotency)
                committed_digest = idempotency.get("job_digest")
                if (
                    idempotency.get("state") != "committed"
                    or idempotency.get("job_id") != job.job_id
                    or not _is_sha256_digest(committed_digest)
                    or (
                        job.submission_digest is not None
                        and job.submission_digest != committed_digest
                    )
                ):
                    protections.append("idempotency_record_ambiguous")
        index = self._read_job_index(job.job_id)
        if index is None or index.get("retention_schema_version") != RETENTION_INDEX_SCHEMA:
            protections.append("retention_index_ambiguous")
        indexed_protections = (
            ("leases_by_job", "lease_records_present", "lease_records_ambiguous"),
            ("active_tasks_by_job", "active_task_records", "task_records_ambiguous"),
            (
                "scheduler_protections_by_job",
                "scheduler_state_active_or_ambiguous",
                "scheduler_records_ambiguous",
            ),
            (
                "active_monitor_rules_by_job",
                "enabled_monitor_rule",
                "monitor_rule_records_ambiguous",
            ),
            (
                "active_gateway_refs_by_job",
                "active_gateway_record",
                "gateway_records_ambiguous",
            ),
        )
        for family, present_protection, ambiguous_protection in indexed_protections:
            present, ambiguous = self._indexed_gc_entry_state(family, job.job_id)
            if ambiguous:
                protections.append(ambiguous_protection)
            elif present:
                protections.append(present_protection)
        protections.extend(self._artifact_lineage_gc_protections(job))
        return list(dict.fromkeys(protections))

    def _artifact_lineage_gc_protections(self, job: RelayJob) -> list[str]:
        """Protect producer artifacts while any retained consumer still uses them."""
        try:
            artifacts = self.list_artifacts(job.job_id)
            for artifact in artifacts:
                reverse_paths = self._bounded_json_record_paths(
                    self._storage_root / "artifact_users" / artifact.artifact_id,
                    limit=MAX_ARTIFACT_CONSUMERS,
                    label=f"consumers of artifact {artifact.artifact_id}",
                )
                order_root = self._artifact_user_order_root(artifact.artifact_id)
                self._read_artifact_user_order_head(artifact.artifact_id)
                entry_paths = self._bounded_json_record_paths(
                    order_root / "entries",
                    limit=MAX_ARTIFACT_CONSUMERS,
                    label=f"ordered consumers of artifact {artifact.artifact_id}",
                )
                mapping_paths = self._bounded_json_record_paths(
                    order_root / "by_consumer",
                    limit=MAX_ARTIFACT_CONSUMERS,
                    label=f"consumer order mappings for artifact {artifact.artifact_id}",
                )
                if (
                    len(reverse_paths) != len(entry_paths)
                    or len(mapping_paths) < len(entry_paths)
                    or (mapping_paths and not reverse_paths)
                ):
                    return ["artifact_lineage_state_ambiguous"]
                for path in reverse_paths:
                    record = self._read_json_file(path, UsedArtifactRef)
                    if (
                        record.artifact_id != artifact.artifact_id
                        or record.producer_job_id != job.job_id
                        or record.consumer_job_id != path.stem
                    ):
                        return ["artifact_lineage_state_ambiguous"]
                    self._validate_artifact_use_record(record)
                    return ["artifact_used_by_retained_job"]
        except (OSError, ValueError, NotFoundError, QueueConflictError):
            return ["artifact_lineage_state_ambiguous"]
        return []

    def _indexed_gc_entry_state(self, family: str, job_id: str) -> tuple[bool, bool]:
        directory = self._storage_root / family / self._durable_key(job_id)
        try:
            directory_stat = os.lstat(directory)
            if not stat.S_ISDIR(directory_stat.st_mode) or _record_is_reparse(directory_stat):
                return False, True
            with os.scandir(directory) as entries:
                entry = next(entries, None)
            if entry is None:
                return False, False
            path = Path(entry.path)
            if not entry.name.endswith(".json"):
                return False, True
            if family == "leases_by_job":
                record: BaseModel | dict[str, object] = self._read_json_file(path, Lease)
            elif family == "active_tasks_by_job":
                record = self._read_json_file(path, RelayTask)
            elif family == "active_monitor_rules_by_job":
                record = self._read_json_file(path, MonitorRule)
            else:
                raw = self._read_json_document(path)
                if not isinstance(raw, dict):
                    return False, True
                record = cast(dict[str, object], raw)
            if isinstance(record, (Lease, RelayTask, MonitorRule)):
                return record.job_id == job_id, record.job_id != job_id
            indexed_job_id = record.get("job_id")
            return indexed_job_id == job_id, indexed_job_id != job_id
        except (OSError, ValueError, QueueConflictError):
            return False, True

    def _job_tombstone_path(self, job_id: str) -> Path:
        return self._storage_root / "job_tombstones" / f"{self._durable_key(job_id)}.json"

    def _job_gc_trash_path(self, job_id: str) -> Path:
        return self._storage_root / "gc_trash" / self._durable_key(job_id)

    def _read_committed_job_digest(self, job: RelayJob) -> str:
        key_path = (
            self._storage_root
            / "idempotency"
            / f"{_idempotency_key_filename(job.idempotency_key)}.json"
        )
        raw = self._read_json_document(key_path)
        if not isinstance(raw, dict):
            raise QueueConflictError(f"idempotency record is not an object: {key_path}")
        record = cast(dict[str, object], raw)
        digest = record.get("job_digest")
        if (
            record.get("state") != "committed"
            or record.get("job_id") != job.job_id
            or not _is_sha256_digest(digest)
            or (job.submission_digest is not None and job.submission_digest != digest)
        ):
            raise QueueConflictError(f"committed idempotency identity is invalid: {job.job_id}")
        return cast(str, digest)

    def _retire_idempotency_unlocked(self, tombstone: JobTombstone) -> None:
        key_path = (
            self._storage_root
            / "idempotency"
            / (f"{_idempotency_key_filename(tombstone.idempotency_key)}.json")
        )
        raw = self._read_json_document(key_path)
        if not isinstance(raw, dict):
            raise QueueConflictError(f"idempotency record is not an object: {key_path}")
        current = cast(dict[str, object], raw)
        if current.get("state") == "retired":
            if (
                current.get("job_id") != tombstone.job_id
                or current.get("job_digest") != tombstone.job_digest
            ):
                raise QueueConflictError("retired idempotency record identity changed")
            return
        if (
            current.get("state") != "committed"
            or current.get("job_id") != tombstone.job_id
            or current.get("job_digest") != tombstone.job_digest
        ):
            raise QueueConflictError("idempotency record changed before retirement")
        self._write_json(
            key_path,
            {
                **current,
                "state": "retired",
                "retired_at": utc_now().isoformat(),
                "tombstone": self._job_tombstone_path(tombstone.job_id).name,
            },
        )

    def _advance_tombstone(
        self,
        tombstone: JobTombstone,
        phase: JobGcPhase,
        *,
        removed: int = 0,
    ) -> JobTombstone:
        updated = tombstone.model_copy(
            update={
                "phase": phase,
                "gc_updated_at": utc_now(),
                "removed_records": tombstone.removed_records + removed,
            }
        )
        self._write(self._job_tombstone_path(tombstone.job_id), updated)
        return updated

    def _record_gc_progress(
        self,
        tombstone: JobTombstone,
        *,
        removed: int,
    ) -> JobTombstone:
        updated = tombstone.model_copy(
            update={
                "gc_updated_at": utc_now(),
                "removed_records": tombstone.removed_records + removed,
            }
        )
        self._write(self._job_tombstone_path(tombstone.job_id), updated)
        return updated

    def _retire_legacy_output_receipts_unlocked(self, tombstone: JobTombstone) -> bool:
        """Atomically retain small migration receipts before job data enters GC trash."""
        if not tombstone.records_trash_started:
            raise QueueConflictError(
                f"legacy output receipts cannot retire before GC authorization: {tombstone.job_id}"
            )
        job_id = self._durable_key(tombstone.job_id)
        source = self._storage_root / "legacy_output_receipts" / job_id
        destination = self._storage_root / "legacy_output_retired" / job_id
        source_stat = _path_lstat(source)
        destination_stat = _path_lstat(destination)
        if source_stat is not None and destination_stat is not None:
            raise QueueConflictError(
                f"active and retired legacy output receipts both exist: {tombstone.job_id}"
            )
        if source_stat is None:
            if destination_stat is not None and (
                not stat.S_ISDIR(destination_stat.st_mode) or _record_is_reparse(destination_stat)
            ):
                raise QueueConflictError(
                    f"retired legacy output receipt root is unsafe: {destination}"
                )
            return False
        if not stat.S_ISDIR(source_stat.st_mode) or _record_is_reparse(source_stat):
            raise QueueConflictError(f"active legacy output receipt root is unsafe: {source}")
        moved = _move_gc_path(source, destination)
        if moved:
            self._fsync_write_directory(source.parent)
            self._fsync_write_directory(destination.parent)
        retired_stat = os.lstat(destination)
        if not stat.S_ISDIR(retired_stat.st_mode) or _record_is_reparse(retired_stat):
            raise QueueConflictError(f"retired legacy output receipt root is unsafe: {destination}")
        return moved

    def _trash_job_roots_unlocked(
        self,
        tombstone: JobTombstone,
        *,
        limit: int,
    ) -> tuple[int, bool]:
        if limit <= 0:
            return 0, False
        job_id = tombstone.job_id
        safe_job_id = self._durable_key(job_id)
        trash = self._job_gc_trash_path(job_id)
        directory_families = (
            "events",
            "legacy_output_archives",
            "tasks_by_job",
            "leases_by_job",
            "artifacts_by_job",
            "used_artifacts_by_job",
            "progress_by_job",
            "task_order_by_job",
            "artifact_order_by_job",
            "progress_order_by_job",
            "active_tasks_by_job",
            "scheduler_refs_by_job",
            "scheduler_protections_by_job",
            "monitor_rules_by_job",
            "active_monitor_rules_by_job",
            "active_gateway_refs_by_job",
        )
        moves: list[tuple[Path, Path]] = [
            (
                self._storage_root / family / safe_job_id,
                trash / "owned" / family,
            )
            for family in directory_families
        ]
        moves.extend(
            (
                self._storage_root / family / filename,
                trash / "root_records" / family / filename,
            )
            for family, filename in (
                ("jobs", f"{job_id}.json"),
                ("jobs_active", f"{job_id}.json"),
                ("jobs_queued", f"{job_id}.json"),
                ("job_indexes", f"{safe_job_id}.json"),
            )
        )
        actions = 0
        if self._retire_legacy_output_receipts_unlocked(tombstone):
            actions += 1
        for source, destination in moves:
            if actions >= limit:
                break
            if _move_gc_path(source, destination):
                actions += 1
        complete = not (
            self._storage_root / "legacy_output_receipts" / safe_job_id
        ).exists() and all(not source.exists() for source, _destination in moves)
        return actions, complete

    def _trash_job_references_unlocked(
        self,
        tombstone: JobTombstone,
        *,
        limit: int,
    ) -> tuple[int, bool, JobTombstone]:
        if limit <= 0:
            return 0, False, tombstone
        trash = self._job_gc_trash_path(tombstone.job_id)
        actions = 0
        references: tuple[tuple[str, type[BaseModel]], ...] = (
            ("tasks_by_job", RelayTask),
            ("leases_by_job", Lease),
            ("artifacts_by_job", ArtifactRef),
            ("progress_by_job", ProgressRecord),
            ("monitor_rules_by_job", MonitorRule),
        )
        for family, model in references:
            source_dir = trash / "owned" / family
            while actions < limit:
                paths, _has_more = _migration_batch_paths(
                    source_dir,
                    cursor=None,
                    limit=1,
                )
                if not paths:
                    break
                path = paths[0]
                record = self._read_json_file(path, model)
                self._trash_primary_record_unlocked(record, trash=trash)
                processed = trash / "processed" / family / path.name
                _move_gc_path(path, processed)
                actions += 1
            if actions >= limit:
                return actions, False, tombstone
        used_source_dir = trash / "owned" / "used_artifacts_by_job"
        while actions < limit:
            paths, _has_more = _migration_batch_paths(
                used_source_dir,
                cursor=None,
                limit=1,
            )
            if not paths:
                break
            path = paths[0]
            record = self._read_json_file(path, UsedArtifactRef)
            if record.consumer_job_id != tombstone.job_id or record.artifact_id != path.stem:
                raise QueueConflictError(f"used-artifact reference is invalid: {path}")
            reverse_path = (
                self._storage_root
                / "artifact_users"
                / record.artifact_id
                / f"{record.consumer_job_id}.json"
            )
            order_root = self._artifact_user_order_root(record.artifact_id)
            mapping_path = order_root / "by_consumer" / f"{record.consumer_job_id}.json"
            entry_path = order_root / "entries" / f"{record.sequence:020d}.json"
            reverse = self._read_optional(reverse_path, UsedArtifactRef)
            if reverse is not None and reverse != record:
                raise QueueConflictError(f"used-artifact reverse reference changed: {reverse_path}")
            mapping = self._read_optional(mapping_path, UsedArtifactRef)
            if mapping is not None and mapping != record:
                raise QueueConflictError(f"used-artifact order mapping changed: {mapping_path}")
            entry = self._read_optional(entry_path, UsedArtifactRef)
            if entry is not None and entry != record:
                raise QueueConflictError(f"used-artifact order entry changed: {entry_path}")
            _unlink_durable_path(reverse_path, missing_ok=True)
            _unlink_durable_path(entry_path, missing_ok=True)
            _unlink_durable_path(mapping_path, missing_ok=True)
            _move_gc_path(
                path,
                trash / "processed" / "used_artifacts_by_job" / path.name,
            )
            actions += 1
        if actions >= limit:
            return actions, False, tombstone
        scheduler_source_dir = trash / "owned" / "scheduler_refs_by_job"
        while actions < limit:
            paths, _has_more = _migration_batch_paths(
                scheduler_source_dir,
                cursor=None,
                limit=1,
            )
            if not paths:
                break
            path = paths[0]
            raw_ref = self._read_json_document(path)
            if not isinstance(raw_ref, dict):
                raise QueueConflictError(f"scheduler reference is invalid: {path}")
            scheduler_ref = cast(dict[str, object], raw_ref)
            raw_ids = scheduler_ref.get("scheduler_ids")
            source_id = scheduler_ref.get("source_id")
            if not isinstance(raw_ids, list) or not isinstance(source_id, str):
                raise QueueConflictError(f"scheduler reference is invalid: {path}")
            for scheduler_id in cast(list[object], raw_ids):
                if not isinstance(scheduler_id, str):
                    raise QueueConflictError(f"scheduler reference is invalid: {path}")
                _unlink_durable_path(
                    self._scheduler_reverse_ref_path(
                        scheduler_id,
                        tombstone.job_id,
                        source_id,
                    ),
                    missing_ok=True,
                )
            _move_gc_path(path, trash / "processed" / "scheduler_refs_by_job" / path.name)
            actions += 1
        references_empty = all(
            next((trash / "owned" / family).glob("*.json"), None) is None
            for family, _model in references
        )
        used_references_empty = next(used_source_dir.glob("*.json"), None) is None
        scheduler_references_empty = next(scheduler_source_dir.glob("*.json"), None) is None
        return (
            actions,
            references_empty and used_references_empty and scheduler_references_empty,
            tombstone,
        )

    def _trash_primary_record_unlocked(self, record: BaseModel, *, trash: Path) -> None:
        if isinstance(record, RelayTask):
            _move_gc_path(
                self._storage_root / "tasks" / f"{record.task_id}.json",
                trash / "primary" / "tasks" / f"{record.task_id}.json",
            )
            _move_gc_path(
                self._storage_root / "task_events" / record.task_id,
                trash / "primary" / "task_events" / record.task_id,
            )
            _move_gc_path(
                self._storage_root / "task_event_heads" / f"{record.task_id}.json",
                trash / "primary" / "task_event_heads" / f"{record.task_id}.json",
            )
            return
        if isinstance(record, Lease):
            _move_gc_path(
                self._storage_root / "leases" / f"{record.lease_id}.json",
                trash / "primary" / "leases" / f"{record.lease_id}.json",
            )
            return
        if isinstance(record, ArtifactRef):
            reverse_directory = self._storage_root / "artifact_users" / record.artifact_id
            order_root = self._artifact_user_order_root(record.artifact_id)
            self._read_artifact_user_order_head(record.artifact_id)
            if self._bounded_json_record_paths(
                reverse_directory,
                limit=MAX_ARTIFACT_CONSUMERS,
                label=f"consumers of artifact {record.artifact_id}",
            ):
                raise QueueConflictError(
                    f"artifact still has retained consumers: {record.artifact_id}"
                )
            if self._bounded_json_record_paths(
                order_root / "entries",
                limit=MAX_ARTIFACT_CONSUMERS,
                label=f"ordered consumers of artifact {record.artifact_id}",
            ) or self._bounded_json_record_paths(
                order_root / "by_consumer",
                limit=MAX_ARTIFACT_CONSUMERS,
                label=f"consumer order mappings for artifact {record.artifact_id}",
            ):
                raise QueueConflictError(
                    f"artifact still has ordered consumer state: {record.artifact_id}"
                )
            _move_gc_path(
                self._storage_root / "artifacts" / f"{record.artifact_id}.json",
                trash / "primary" / "artifacts" / f"{record.artifact_id}.json",
            )
            _move_gc_path(
                reverse_directory,
                trash / "primary" / "artifact_users" / record.artifact_id,
            )
            _move_gc_path(
                order_root,
                trash / "primary" / "artifact_user_order" / record.artifact_id,
            )
            return
        if isinstance(record, ProgressRecord):
            _move_gc_path(
                self._storage_root / "progress" / f"{record.progress_id}.json",
                trash / "primary" / "progress" / f"{record.progress_id}.json",
            )
            return
        if isinstance(record, MonitorRule):
            _move_gc_path(
                self._storage_root / "monitor_rules" / f"{record.rule_id}.json",
                trash / "primary" / "monitor_rules" / f"{record.rule_id}.json",
            )
            return
        raise QueueConflictError("unsupported GC reference record")

    @staticmethod
    def _after_gc_checkpoint(_phase: JobGcPhase) -> None:
        """Fault-injection seam invoked only after a durable GC phase checkpoint."""

    @staticmethod
    def _gc_result(
        plan: TerminalJobGcPlan,
        tombstone: JobTombstone,
        actions: int,
    ) -> TerminalJobGcResult:
        return TerminalJobGcResult(
            plan=plan,
            dry_run=False,
            phase=tombstone.phase,
            complete=tombstone.phase is JobGcPhase.COMPLETE,
            actions=actions,
            tombstone=tombstone,
        )

    def _ensure_global_order_entry_unlocked(self, family: str, record_id: str) -> int:
        """Return one durable global sequence, repairing an interrupted entry write."""
        if family not in _GLOBAL_ORDER_FAMILIES:
            raise QueueConflictError(f"unsupported global-order family: {family}")
        if not _safe_global_record_id(record_id):
            raise QueueConflictError(f"unsafe global-order record id: {record_id!r}")
        root = self._storage_root / "global_order" / family
        mapping_path = root / "by_id" / f"{_stable_ref_token(record_id)}.json"
        mapping = self._read_global_order_record_optional(
            mapping_path,
            family=family,
        )
        latest_sequence = self._read_global_order_head(family)
        if mapping is not None:
            mapped_id, sequence = mapping
            if mapped_id != record_id or sequence > latest_sequence:
                raise QueueConflictError(
                    f"global-order mapping identity mismatch: {family}/{record_id}"
                )
            self._ensure_global_order_sequence_record_unlocked(
                family,
                record_id,
                sequence,
            )
            return sequence
        if latest_sequence >= 2**63 - 1:
            raise QueueConflictError(f"global-order sequence exhausted: {family}")
        sequence = latest_sequence + 1
        self._write_json(
            root / "head.json",
            {
                "schema_version": GLOBAL_ORDER_INDEX_SCHEMA,
                "family": family,
                "latest_sequence": sequence,
            },
        )
        document: dict[str, object] = {
            "schema_version": GLOBAL_ORDER_INDEX_SCHEMA,
            "family": family,
            "record_id": record_id,
            "sequence": sequence,
        }
        self._write_json(mapping_path, document)
        self._ensure_global_order_sequence_record_unlocked(
            family,
            record_id,
            sequence,
        )
        return sequence

    def _job_submission_order_key_unlocked(
        self,
        job: RelayJob,
    ) -> tuple[int, datetime, str]:
        """Return the durable total-order key for one submitted job."""
        sequence = self._ensure_global_order_entry_unlocked("jobs", job.job_id)
        return sequence, job.created_at, job.job_id

    def _ensure_global_order_sequence_record_unlocked(
        self,
        family: str,
        record_id: str,
        sequence: int,
    ) -> None:
        entry_path = (
            self._storage_root / "global_order" / family / "entries" / f"{sequence:020d}.json"
        )
        existing = self._read_global_order_record_optional(entry_path, family=family)
        if existing is not None:
            if existing != (record_id, sequence):
                raise QueueConflictError(f"global-order sequence collision: {family}/{sequence}")
            return
        self._write_json(
            entry_path,
            {
                "schema_version": GLOBAL_ORDER_INDEX_SCHEMA,
                "family": family,
                "record_id": record_id,
                "sequence": sequence,
            },
        )

    def _read_global_order_head(self, family: str) -> int:
        path = self._storage_root / "global_order" / family / "head.json"
        try:
            raw = self._read_json_document(path)
        except FileNotFoundError:
            return 0
        if not isinstance(raw, dict):
            raise QueueConflictError(f"global-order head is not an object: {path}")
        document = cast(dict[str, object], raw)
        latest_sequence = document.get("latest_sequence")
        if (
            document.get("schema_version") != GLOBAL_ORDER_INDEX_SCHEMA
            or document.get("family") != family
            or isinstance(latest_sequence, bool)
            or not isinstance(latest_sequence, int)
            or latest_sequence < 1
            or latest_sequence >= 2**63
        ):
            raise QueueConflictError(f"invalid global-order head: {path}")
        return latest_sequence

    def _read_global_order_record_optional(
        self,
        path: Path,
        *,
        family: str,
    ) -> tuple[str, int] | None:
        try:
            raw = self._read_json_document(path)
        except FileNotFoundError:
            return None
        if not isinstance(raw, dict):
            raise QueueConflictError(f"global-order record is not an object: {path}")
        document = cast(dict[str, object], raw)
        record_id = document.get("record_id")
        sequence = document.get("sequence")
        if (
            document.get("schema_version") != GLOBAL_ORDER_INDEX_SCHEMA
            or document.get("family") != family
            or not _safe_global_record_id(record_id)
            or isinstance(sequence, bool)
            or not isinstance(sequence, int)
            or sequence < 1
            or sequence >= 2**63
        ):
            raise QueueConflictError(f"invalid global-order record: {path}")
        return cast(str, record_id), sequence

    def _read_global_order_page(
        self,
        *,
        family: str,
        model: type[Record],
        identity_field: str,
        cursor: int,
        limit: int,
        predicate: Callable[[Record], bool] | None = None,
    ) -> tuple[list[Record], int | None, int]:
        """Read one bounded global source window in durable sequence order."""
        cursor = validate_record_cursor(cursor)
        limit = validate_response_page_limit(limit)
        self.initialize()
        self._require_index_migration_complete()
        latest_sequence = self._read_global_order_head(family)
        if cursor > latest_sequence:
            return [], None, latest_sequence
        stop = min(latest_sequence + 1, cursor + limit)
        records: list[Record] = []
        root = self._storage_root / "global_order" / family
        for sequence in range(cursor, stop):
            entry = self._read_global_order_record_optional(
                root / "entries" / f"{sequence:020d}.json",
                family=family,
            )
            if entry is None:
                continue
            record_id, recorded_sequence = entry
            if recorded_sequence != sequence:
                raise QueueConflictError(
                    f"global-order sequence identity mismatch: {family}/{sequence}"
                )
            mapping = self._read_global_order_record_optional(
                root / "by_id" / f"{_stable_ref_token(record_id)}.json",
                family=family,
            )
            if mapping != entry:
                raise QueueConflictError(
                    f"global-order reverse mapping mismatch: {family}/{record_id}"
                )
            record = self._read_optional(self._storage_root / family / f"{record_id}.json", model)
            if record is None:
                continue
            if getattr(record, identity_field, None) != record_id:
                raise QueueConflictError(
                    f"global-order target identity mismatch: {family}/{record_id}"
                )
            if predicate is None or predicate(record):
                records.append(record)
        next_cursor = stop if stop <= latest_sequence else None
        return records, next_cursor, latest_sequence

    def _scan_global_order(
        self,
        *,
        family: str,
        model: type[Record],
        identity_field: str,
        limit: int,
        predicate: Callable[[Record], bool] | None = None,
    ) -> tuple[list[Record], bool]:
        """Read at most one bounded number of durable global source positions."""
        if isinstance(limit, bool):
            raise ValueError("scan limit must be an integer")
        if limit < 1 or limit > MAX_BOUNDED_SCAN_RECORDS:
            raise ValueError(f"scan limit must be between 1 and {MAX_BOUNDED_SCAN_RECORDS}")
        records: list[Record] = []
        cursor = 1
        remaining = limit
        while remaining > 0:
            page_limit = min(remaining, MAX_RESPONSE_PAGE_RECORDS)
            page, next_cursor, total = self._read_global_order_page(
                family=family,
                model=model,
                identity_field=identity_field,
                cursor=cursor,
                limit=page_limit,
                predicate=predicate,
            )
            records.extend(page)
            consumed = min(page_limit, max(0, total - cursor + 1))
            remaining -= consumed
            if next_cursor is None:
                return records, False
            cursor = next_cursor
        return records, cursor <= self._read_global_order_head(family)

    def _ensure_extended_migration_state(self) -> None:
        state = self._read_index_migration_state()
        changed = False
        if not isinstance(state.get("order_families"), dict):
            state["order_families"] = {
                family: {
                    "cursor": None,
                    "complete": next((self._storage_root / family).glob("*.json"), None) is None,
                }
                for family in _ORDER_FAMILIES
            }
            changed = True
        if not isinstance(state.get("retention_families"), dict):
            state["retention_families"] = {
                family: {
                    "cursor": None,
                    "complete": next((self._storage_root / family).glob("*.json"), None) is None,
                }
                for family in _RETENTION_INDEX_FAMILIES
            }
            changed = True
        if not isinstance(state.get("global_order_families"), dict):
            state["global_order_families"] = {
                family: {
                    "cursor": None,
                    "complete": next((self._storage_root / family).glob("*.json"), None) is None,
                }
                for family in _GLOBAL_ORDER_FAMILIES
            }
            changed = True
        else:
            global_order_state = cast(
                dict[str, object],
                state["global_order_families"],
            )
            for family in _GLOBAL_ORDER_FAMILIES:
                if not isinstance(global_order_state.get(family), dict):
                    global_order_state[family] = {
                        "cursor": None,
                        "complete": next(
                            (self._storage_root / family).glob("*.json"),
                            None,
                        )
                        is None,
                    }
                    changed = True
        if not isinstance(state.get("operational_families"), dict):
            state["operational_families"] = {
                family: {
                    "cursor": None,
                    "complete": next((self._storage_root / family).glob("*.json"), None) is None,
                    **(
                        {"schema_version": LEASE_OPERATIONAL_INDEX_SCHEMA}
                        if family == "leases"
                        else {}
                    ),
                }
                for family in _OPERATIONAL_INDEX_FAMILIES
            }
            changed = True
        else:
            operational_state = cast(dict[str, object], state["operational_families"])
            for family in _OPERATIONAL_INDEX_FAMILIES:
                if not isinstance(operational_state.get(family), dict):
                    operational_state[family] = {
                        "cursor": None,
                        "complete": next((self._storage_root / family).glob("*.json"), None)
                        is None,
                        **(
                            {"schema_version": LEASE_OPERATIONAL_INDEX_SCHEMA}
                            if family == "leases"
                            else {}
                        ),
                    }
                    changed = True
            raw_lease_checkpoint = operational_state.get("leases")
            if isinstance(raw_lease_checkpoint, dict):
                lease_checkpoint = cast(dict[str, object], raw_lease_checkpoint)
                if lease_checkpoint.get("schema_version") != LEASE_OPERATIONAL_INDEX_SCHEMA:
                    lease_checkpoint.update(
                        {
                            "cursor": None,
                            "complete": next(
                                (self._storage_root / "leases").glob("*.json"),
                                None,
                            )
                            is None,
                            "schema_version": LEASE_OPERATIONAL_INDEX_SCHEMA,
                        }
                    )
                    changed = True
        if not isinstance(state.get("lease_operational_repair"), dict):
            state["lease_operational_repair"] = {
                "complete": not _lease_operational_records_present(self._storage_root),
                "schema_version": LEASE_OPERATIONAL_INDEX_SCHEMA,
            }
            changed = True
        else:
            raw_lease_repair = cast(
                dict[str, object],
                state["lease_operational_repair"],
            )
            if raw_lease_repair.get("schema_version") != LEASE_OPERATIONAL_INDEX_SCHEMA:
                raw_lease_repair.update(
                    {
                        "complete": False,
                        "schema_version": LEASE_OPERATIONAL_INDEX_SCHEMA,
                    }
                )
                changed = True
        pending_transition = (
            next((self._storage_root / "transition_intents").glob("*.json"), None) is not None
        )
        raw_capacity = state.get("lease_capacity_aggregate")
        if not isinstance(raw_capacity, dict):
            state["lease_capacity_aggregate"] = {
                "complete": False,
                "schema_version": LEASE_CAPACITY_AGGREGATE_SCHEMA,
            }
            changed = True
        else:
            capacity_checkpoint = cast(dict[str, object], raw_capacity)
            complete = capacity_checkpoint.get("complete") is True
            valid_complete_fields = (
                _is_capacity_identity(capacity_checkpoint.get("epoch_id"))
                and isinstance(capacity_checkpoint.get("generation"), int)
                and not isinstance(capacity_checkpoint.get("generation"), bool)
                and cast(int, capacity_checkpoint.get("generation")) >= 0
                and isinstance(capacity_checkpoint.get("record_count"), int)
                and not isinstance(capacity_checkpoint.get("record_count"), bool)
                and 0
                <= cast(int, capacity_checkpoint.get("record_count"))
                <= MAX_LIVE_LEASE_RECORDS
            )
            if capacity_checkpoint.get("schema_version") != LEASE_CAPACITY_AGGREGATE_SCHEMA or (
                complete and not valid_complete_fields
            ):
                state["lease_capacity_aggregate"] = {
                    "complete": False,
                    "schema_version": LEASE_CAPACITY_AGGREGATE_SCHEMA,
                }
                changed = True
            elif complete:
                try:
                    current_capacity = self._read_lease_capacity_aggregate_unlocked()
                except (OSError, QueueConflictError):
                    if not pending_transition:
                        capacity_checkpoint.clear()
                        capacity_checkpoint.update(
                            {
                                "complete": False,
                                "schema_version": LEASE_CAPACITY_AGGREGATE_SCHEMA,
                            }
                        )
                        changed = True
                else:
                    migrated_generation = cast(int, capacity_checkpoint["generation"])
                    if (
                        current_capacity.aggregate.epoch_id != capacity_checkpoint.get("epoch_id")
                        or current_capacity.aggregate.generation < migrated_generation
                    ) and not pending_transition:
                        capacity_checkpoint.clear()
                        capacity_checkpoint.update(
                            {
                                "complete": False,
                                "schema_version": LEASE_CAPACITY_AGGREGATE_SCHEMA,
                            }
                        )
                        changed = True
        raw_order = cast(dict[str, object], state["order_families"])
        raw_retention = cast(dict[str, object], state["retention_families"])
        raw_global_order = cast(dict[str, object], state["global_order_families"])
        raw_operational = cast(dict[str, object], state["operational_families"])
        raw_lease_repair = cast(dict[str, object], state["lease_operational_repair"])
        raw_capacity = cast(dict[str, object], state["lease_capacity_aggregate"])
        incomplete = False
        for raw_checkpoint in (
            *raw_order.values(),
            *raw_global_order.values(),
            *raw_retention.values(),
            *raw_operational.values(),
        ):
            if not isinstance(raw_checkpoint, dict):
                incomplete = True
                break
            checkpoint = cast(dict[str, object], raw_checkpoint)
            if checkpoint.get("complete") is not True:
                incomplete = True
                break
        if raw_lease_repair.get("complete") is not True:
            incomplete = True
        if raw_capacity.get("complete") is not True:
            incomplete = True
        if incomplete and state.get("complete") is True:
            state["complete"] = False
            changed = True
        if changed:
            self._write_index_migration_state(state)

    def _read_index_migration_state(self) -> dict[str, object]:
        path = self._storage_root / "migrations" / "index-v1.json"
        try:
            raw = self._read_json_document(path)
        except (OSError, QueueConflictError) as exc:
            raise QueueConflictError(f"invalid index migration state {path}: {exc}") from exc
        if not isinstance(raw, dict):
            raise QueueConflictError(f"index migration state is not an object: {path}")
        state = cast(dict[str, object], raw)
        if state.get("schema_version") != INDEX_MIGRATION_SCHEMA:
            raise QueueConflictError(f"unsupported index migration state: {path}")
        return state

    def _write_index_migration_state(self, state: dict[str, object]) -> None:
        self._write_json(self._storage_root / "migrations" / "index-v1.json", state)

    def _require_index_migration_complete(self) -> None:
        if self._read_index_migration_state().get("complete") is not True:
            raise QueueConflictError(
                "queue indexes require migration; run `clio-relay queue migrate-indexes` "
                "before starting workers"
            )

    def _lease_capacity_migration_complete_unlocked(self) -> bool:
        state = self._read_index_migration_state()
        raw_checkpoint = state.get("lease_capacity_aggregate")
        return (
            isinstance(raw_checkpoint, dict)
            and cast(dict[str, object], raw_checkpoint).get("complete") is True
        )

    def _migrate_record_unlocked(self, family: str, record: BaseModel) -> None:
        if family == "jobs" and isinstance(record, RelayJob):
            self._initialize_job_index_unlocked(record.job_id)
            self._ensure_artifact_use_indexes_unlocked(record)
            self._write_job_unlocked(record)
            return
        if family == "tasks" and isinstance(record, RelayTask):
            self._write(
                self._job_record_path("tasks_by_job", record.job_id, record.task_id),
                record,
            )
            return
        if family == "leases" and isinstance(record, Lease):
            self._write(
                self._job_record_path("leases_by_job", record.job_id, record.lease_id),
                record,
            )
            return
        if family == "artifacts" and isinstance(record, ArtifactRef):
            self._write(
                self._job_record_path("artifacts_by_job", record.job_id, record.artifact_id),
                record,
            )
            (self._storage_root / "artifact_users" / record.artifact_id).mkdir(
                parents=True,
                exist_ok=True,
            )
            self._initialize_artifact_user_order_unlocked(record.artifact_id)
            return
        if family == "progress" and isinstance(record, ProgressRecord):
            self._write(
                self._job_record_path("progress_by_job", record.job_id, record.progress_id),
                record,
            )
            return
        raise QueueConflictError(f"index migration record mismatch: {family}")

    def _reconcile_index_migration_sources_unlocked(self) -> None:
        """Rebuild every migrated index from one bounded canonical-source snapshot.

        A pre-1.0 writer can add a flat record after a family's cursor has reached
        the end of that directory.  Migration completion therefore cannot trust
        cursors alone.  This final pass runs while the queue lock is held, validates
        every canonical source family against the normal bounded-scan limit, and
        idempotently projects each record into every index it owns before the
        completion marker is written.
        """
        source_models: dict[str, type[BaseModel]] = {
            "jobs": RelayJob,
            "tasks": RelayTask,
            "leases": Lease,
            "artifacts": ArtifactRef,
            "progress": ProgressRecord,
            "endpoints": EndpointRegistration,
            "gateway_sessions": GatewaySession,
            "monitor_rules": MonitorRule,
        }
        source_records: dict[str, list[BaseModel]] = {}
        for family, model in source_models.items():
            records, truncated = self._scan_many(
                self._storage_root / family,
                model,
                limit=MAX_BOUNDED_SCAN_RECORDS,
            )
            if truncated:
                raise QueueConflictError(
                    "index migration final reconciliation exceeded its safety bound of "
                    f"{MAX_BOUNDED_SCAN_RECORDS} records for {family}"
                )
            source_records[family] = records

        for family in ("jobs", "tasks", "leases", "artifacts", "progress"):
            for record in source_records[family]:
                self._migrate_record_unlocked(family, record)

        for family in _ORDER_FAMILIES:
            for record in source_records[family]:
                self._migrate_order_record_unlocked(family, record)

        global_order_identity_fields = {
            "endpoints": "endpoint_id",
            "jobs": "job_id",
            "gateway_sessions": "session_id",
            "monitor_rules": "rule_id",
        }
        for family, identity_field in global_order_identity_fields.items():
            for record in source_records[family]:
                record_id = getattr(record, identity_field, None)
                if not isinstance(record_id, str) or not record_id:
                    raise QueueConflictError(
                        f"global-order record identity is invalid: {family}/{identity_field}"
                    )
                self._ensure_global_order_entry_unlocked(family, record_id)

        for family in _RETENTION_INDEX_FAMILIES:
            for record in source_records[family]:
                self._migrate_retention_record_unlocked(family, record)

        for family in _OPERATIONAL_INDEX_FAMILIES:
            if family == "leases":
                continue
            for record in source_records[family]:
                self._migrate_operational_record_unlocked(family, record)

        lease_repair_intent, lease_repair_payload = (
            self._prepare_lease_capacity_rebuild_intent_unlocked(
                identity="migration-v1-final-reconcile",
                limit=MAX_LIVE_LEASE_RECORDS,
            )
        )
        self._apply_lease_index_repair_intent_unlocked(
            lease_repair_intent,
            lease_repair_payload,
        )

        for record in source_records["jobs"]:
            if not isinstance(record, RelayJob):
                raise QueueConflictError("job finalization record is invalid")
            self._finalize_job_index_unlocked(record.job_id)

    def _migrate_retention_record_unlocked(self, family: str, record: BaseModel) -> None:
        if family == "jobs" and isinstance(record, RelayJob):
            self._initialize_job_index_unlocked(record.job_id)
            self._update_job_index_unlocked(
                record.job_id,
                retention_schema_version=RETENTION_INDEX_SCHEMA,
            )
            self._sync_scheduler_source_unlocked(
                record.job_id,
                source_id="job",
                metadata=record.metadata,
            )
            return
        if family == "tasks" and isinstance(record, RelayTask):
            self._sync_task_retention_indexes_unlocked(record)
            return
        if family == "artifacts" and isinstance(record, ArtifactRef):
            self._link_gateways_for_artifact_unlocked(record)
            return
        if family == "monitor_rules" and isinstance(record, MonitorRule):
            self._sync_monitor_rule_indexes_unlocked(record)
            return
        if family == "gateway_sessions" and isinstance(record, GatewaySession):
            self._index_gateway_session_unlocked(record)
            return
        raise QueueConflictError(f"retention-index migration record mismatch: {family}")

    def _migrate_operational_record_unlocked(self, family: str, record: BaseModel) -> None:
        """Build operational indexes introduced after the original v1 migration."""
        if family == "endpoints" and isinstance(record, EndpointRegistration):
            self._index_fresh_endpoint_unlocked(record)
            return
        if family == "jobs" and isinstance(record, RelayJob):
            self._sync_owner_session_job_membership_unlocked(record)
            request = _scheduler_cancellation_request(record)
            if request is not None and request.get("cancel_scheduler") is True:
                self._ensure_scheduler_cancel_pending_unlocked(
                    record,
                    requested_at=_cancellation_requested_at(request) or record.updated_at,
                    reason="operator_request",
                )
            return
        if family == "gateway_sessions" and isinstance(record, GatewaySession):
            _validate_owner_session_identity_metadata(record.metadata, allow_legacy=True)
            return
        if family == "leases" and isinstance(record, Lease):
            job = self.get_job(record.job_id)
            self._sync_lease_operational_indexes_unlocked(record, job=job)
            return
        raise QueueConflictError(f"operational-index migration record mismatch: {family}")

    def _finalize_job_index_unlocked(self, job_id: str) -> None:
        self._initialize_job_index_unlocked(job_id)
        safe_job_id = self._durable_key(job_id)
        task_count = _last_contiguous_sequence(
            self._storage_root / "task_order_by_job" / safe_job_id
        )
        artifact_count = _last_contiguous_sequence(
            self._storage_root / "artifact_order_by_job" / safe_job_id
        )
        progress_count = _last_contiguous_sequence(
            self._storage_root / "progress_order_by_job" / safe_job_id
        )
        latest_progress = (
            self._read_optional(
                self._storage_root
                / "progress_order_by_job"
                / safe_job_id
                / f"{progress_count:020d}.json",
                ProgressRecord,
            )
            if progress_count > 0
            else None
        )
        latest_event_seq = _last_contiguous_sequence(self._storage_root / "events" / job_id)
        self._update_job_index_unlocked(
            job_id,
            task_count=task_count,
            artifact_count=artifact_count,
            progress_count=progress_count,
            latest_progress_id=(None if latest_progress is None else latest_progress.progress_id),
            latest_event_seq=latest_event_seq,
        )

    def _initialize_job_index_unlocked(self, job_id: str) -> None:
        index_path = self._storage_root / "job_indexes" / f"{self._durable_key(job_id)}.json"
        for family in (
            "tasks_by_job",
            "leases_by_job",
            "artifacts_by_job",
            "used_artifacts_by_job",
            "progress_by_job",
            "task_order_by_job",
            "artifact_order_by_job",
            "progress_order_by_job",
            "active_tasks_by_job",
            "scheduler_refs_by_job",
            "scheduler_protections_by_job",
            "monitor_rules_by_job",
            "active_monitor_rules_by_job",
            "active_gateway_refs_by_job",
        ):
            (self._storage_root / family / self._durable_key(job_id)).mkdir(
                parents=True, exist_ok=True
            )
        if index_path.exists():
            return
        self._write_json(
            index_path,
            {
                "schema_version": JOB_INDEX_SCHEMA,
                "order_schema_version": ORDER_INDEX_SCHEMA,
                "retention_schema_version": RETENTION_INDEX_SCHEMA,
                "job_id": job_id,
                "task_count": 0,
                "artifact_count": 0,
                "progress_count": 0,
                "latest_progress_id": None,
                "latest_event_seq": 0,
            },
        )

    def _job_index_exists(self, job_id: str) -> bool:
        return (self._storage_root / "job_indexes" / f"{self._durable_key(job_id)}.json").is_file()

    def _read_job_index(self, job_id: str) -> dict[str, object] | None:
        path = self._storage_root / "job_indexes" / f"{self._durable_key(job_id)}.json"
        try:
            raw = self._read_json_document(path)
        except FileNotFoundError:
            return None
        except (OSError, QueueConflictError) as exc:
            raise QueueConflictError(f"invalid job index {path}: {exc}") from exc
        if not isinstance(raw, dict):
            raise QueueConflictError(f"job index is not an object: {path}")
        index = cast(dict[str, object], raw)
        if index.get("schema_version") != JOB_INDEX_SCHEMA or index.get("job_id") != job_id:
            raise QueueConflictError(f"job index identity mismatch: {path}")
        for field in ("task_count", "artifact_count", "progress_count", "latest_event_seq"):
            _index_integer(index, field)
        latest_progress_id = index.get("latest_progress_id")
        if latest_progress_id is not None and not isinstance(latest_progress_id, str):
            raise QueueConflictError(f"invalid latest_progress_id in {path}")
        return index

    def _update_job_index_unlocked(self, job_id: str, **updates: object) -> None:
        index = self._read_job_index(job_id)
        if index is None:
            return
        index.update(updates)
        self._write_json(
            self._storage_root / "job_indexes" / f"{self._durable_key(job_id)}.json",
            index,
        )

    def _increment_job_index_unlocked(
        self,
        job_id: str,
        field: str,
        **updates: object,
    ) -> None:
        index = self._read_job_index(job_id)
        if index is None:
            return
        index[field] = _index_integer(index, field) + 1
        index.update(updates)
        self._write_json(
            self._storage_root / "job_indexes" / f"{self._durable_key(job_id)}.json",
            index,
        )

    def _next_job_record_sequence_unlocked(self, job_id: str, count_field: str) -> int:
        index = self._read_job_index(job_id)
        if index is None:
            raise QueueConflictError(f"job order index is missing: {job_id}")
        return _index_integer(index, count_field) + 1

    def _write_ordered_job_record(
        self,
        family: str,
        job_id: str,
        sequence: int,
        record: BaseModel,
    ) -> None:
        directory = self._storage_root / f"{family}_order_by_job" / self._durable_key(job_id)
        self._write(directory / f"{sequence:020d}.json", record)

    def _read_ordered_job_page(
        self,
        job_id: str,
        *,
        family: str,
        model: type[Record],
        cursor: int,
        limit: int,
        count_field: str,
    ) -> tuple[list[Record], int | None, int]:
        cursor = validate_record_cursor(cursor)
        limit = validate_response_page_limit(limit)
        self.initialize()
        self._require_index_migration_complete()
        index = self._read_job_index(job_id)
        if index is None:
            raise NotFoundError(f"job not found: {job_id}")
        total = _index_integer(index, count_field)
        if cursor > total:
            return [], None, total
        stop = min(total + 1, cursor + limit)
        records: list[Record] = []
        directory = self._storage_root / f"{family}_order_by_job" / self._durable_key(job_id)
        for sequence in range(cursor, stop):
            record = self._read_optional(directory / f"{sequence:020d}.json", model)
            if record is None:
                raise QueueConflictError(
                    f"{family} order index is missing sequence {sequence}: {job_id}"
                )
            records.append(record)
        next_cursor = stop if stop <= total else None
        return records, next_cursor, total

    def _migrate_order_record_unlocked(self, family: str, record: BaseModel) -> None:
        if family == "tasks" and isinstance(record, RelayTask):
            sequence = record.sequence or (
                _last_contiguous_sequence(
                    self._storage_root / "task_order_by_job" / self._durable_key(record.job_id)
                )
                + 1
            )
            saved = record.model_copy(update={"sequence": sequence})
            self._write(self._storage_root / "tasks" / f"{saved.task_id}.json", saved)
            self._write(self._job_record_path("tasks_by_job", saved.job_id, saved.task_id), saved)
            self._write_ordered_job_record("task", saved.job_id, sequence, saved)
            return
        if family == "artifacts" and isinstance(record, ArtifactRef):
            sequence = record.sequence or (
                _last_contiguous_sequence(
                    self._storage_root / "artifact_order_by_job" / self._durable_key(record.job_id)
                )
                + 1
            )
            saved = record.model_copy(update={"sequence": sequence})
            self._write(self._storage_root / "artifacts" / f"{saved.artifact_id}.json", saved)
            self._write(
                self._job_record_path("artifacts_by_job", saved.job_id, saved.artifact_id),
                saved,
            )
            (self._storage_root / "artifact_users" / saved.artifact_id).mkdir(
                parents=True,
                exist_ok=True,
            )
            self._initialize_artifact_user_order_unlocked(saved.artifact_id)
            self._write_ordered_job_record("artifact", saved.job_id, sequence, saved)
            return
        if family == "progress" and isinstance(record, ProgressRecord):
            sequence = record.sequence or (
                _last_contiguous_sequence(
                    self._storage_root / "progress_order_by_job" / self._durable_key(record.job_id)
                )
                + 1
            )
            saved = record.model_copy(update={"sequence": sequence})
            self._write(self._storage_root / "progress" / f"{saved.progress_id}.json", saved)
            self._write(
                self._job_record_path("progress_by_job", saved.job_id, saved.progress_id),
                saved,
            )
            self._write_ordered_job_record("progress", saved.job_id, sequence, saved)
            return
        raise QueueConflictError(f"order-index migration record mismatch: {family}")

    def _job_record_path(self, family: str, job_id: str, record_id: str) -> Path:
        return (
            self._storage_root
            / family
            / self._durable_key(job_id)
            / f"{self._durable_key(record_id)}.json"
        )

    @staticmethod
    def _durable_key(value: str) -> str:
        return ClioCoreQueue._require_durable_record_id(value, field="record_id")

    @staticmethod
    def _require_durable_record_id(value: str, *, field: str) -> str:
        try:
            return validate_durable_record_id(value)
        except ValueError as error:
            raise ValueError(f"invalid {field}: {error}") from error

    @staticmethod
    def _label_key(value: str, *, domain: str) -> str:
        return filesystem_key(value, domain=domain)

    def _write(self, path: Path, record: BaseModel) -> None:
        # Scheduler cancellation records may contain the contract maximum of
        # 1,000 dispositions.  Claim fields were added after v1.0.7, so writing
        # six explicit nulls per legacy disposition would make a previously
        # valid record exceed its durable family limit.  Missing optional fields
        # retain the same Pydantic defaults; an active claim is still serialized
        # in full because each of its values is non-null.
        exclude_none = isinstance(record, SchedulerCancelPending)
        self._write_text(path, record.model_dump_json(indent=2, exclude_none=exclude_none))

    def _write_json(self, path: Path, record: dict[str, object]) -> None:
        self._write_text(path, json.dumps(record))

    def _require_safe_write_directory(self, directory: Path) -> os.stat_result:
        """Create and validate one owner-controlled directory below the queue root."""
        try:
            logical_directory = logical_filesystem_path(directory)
            internal_directory = internal_filesystem_path(
                logical_directory,
                force_extended=True,
            )
        except ValueError as error:
            raise QueueConflictError(
                f"write directory has an unsupported path: {directory}"
            ) from error
        try:
            relative = internal_directory.relative_to(self._storage_root)
        except ValueError as error:
            raise QueueConflictError(
                f"write directory escaped queue root: {logical_directory}"
            ) from error
        if any(part in {"", ".", ".."} for part in relative.parts):
            raise QueueConflictError(f"write directory has unsafe ancestry: {logical_directory}")
        current = self._storage_root
        for part in relative.parts:
            current /= part
            try:
                current_stat = os.lstat(current)
            except FileNotFoundError:
                with suppress(FileExistsError):
                    current.mkdir(mode=0o700)
                current_stat = os.lstat(current)
            if not stat.S_ISDIR(current_stat.st_mode) or _record_is_reparse(current_stat):
                raise QueueConflictError(
                    f"write directory ancestry is unsafe: {logical_filesystem_path(current)}"
                )
            if os.name != "nt" and hasattr(os, "geteuid") and current_stat.st_uid != os.geteuid():
                raise QueueConflictError(
                    f"write directory is not owned by this user: {logical_filesystem_path(current)}"
                )
        return os.lstat(internal_directory)

    def _require_private_write_staging(self) -> tuple[Path, os.stat_result]:
        """Return the private non-reparse staging directory used for atomic writes."""
        staging = self._storage_root / WRITE_STAGING_FAMILY
        try:
            if not os.path.lexists(staging):
                ensure_private_configuration_directory(staging)
            if os.name != "nt":
                os.chmod(staging, 0o700)
            ensure_private_configuration_path(staging, directory=True)
        except (ConfigurationError, OSError) as error:
            raise QueueConflictError(
                f"queue write staging is not owner-private: {logical_filesystem_path(staging)}"
            ) from error
        staging_stat = self._require_safe_write_directory(staging)
        if not stat.S_ISDIR(staging_stat.st_mode) or _record_is_reparse(staging_stat):
            raise QueueConflictError(
                f"queue write staging is not a safe directory: {logical_filesystem_path(staging)}"
            )
        return staging, staging_stat

    def _purge_write_staging_unlocked(self) -> None:
        """Remove bounded crash leftovers while holding the cross-process queue lock."""
        staging, _ = self._require_private_write_staging()
        leftovers: list[Path] = []
        try:
            with os.scandir(staging) as entries:
                for entry in entries:
                    if len(leftovers) >= WRITE_STAGING_MAX_LEFTOVERS:
                        raise QueueConflictError(
                            f"queue write staging exceeds the bounded cleanup limit: {staging}"
                        )
                    path = Path(entry.path)
                    stem = entry.name.removesuffix(".tmp")
                    entry_stat = os.lstat(path)
                    if (
                        not entry.name.endswith(".tmp")
                        or len(stem) != 32
                        or any(character not in "0123456789abcdef" for character in stem)
                        or not stat.S_ISREG(entry_stat.st_mode)
                        or _record_is_reparse(entry_stat)
                        or entry_stat.st_nlink != 1
                    ):
                        raise QueueConflictError(
                            f"queue write staging contains an unsafe entry: {path}"
                        )
                    leftovers.append(path)
        except QueueConflictError:
            raise
        except OSError as error:
            raise QueueConflictError(f"cannot scan queue write staging: {staging}") from error
        for path in leftovers:
            _unlink_durable_path(path)
        if leftovers:
            self._fsync_write_directory(staging)

    def _write_text(self, path: Path, text: str) -> None:
        self._write_bytes(
            path,
            text.encode("utf-8"),
            max_bytes=_record_max_bytes(path),
        )

    def _write_bytes(self, path: Path, payload: bytes, *, max_bytes: int) -> None:
        """Atomically write owner-private bytes below the queue root."""
        try:
            logical_path = logical_filesystem_path(path)
            internal_path = internal_filesystem_path(logical_path, force_extended=True)
        except ValueError as error:
            raise QueueConflictError(f"queue write path is unsupported: {path}") from error
        if max_bytes < 1 or len(payload) > max_bytes:
            raise QueueConflictError(
                f"{_record_family(internal_path)} record exceeds the {max_bytes}-byte limit: "
                f"{logical_path}"
            )
        target_parent_stat = self._require_safe_write_directory(internal_path.parent)
        staging, staging_stat = self._require_private_write_staging()
        if staging_stat.st_dev != target_parent_stat.st_dev:
            raise QueueConflictError(
                "atomic queue replacement crosses filesystems: "
                f"{logical_filesystem_path(staging)} -> {logical_path.parent}"
            )
        temporary = staging / f"{uuid4().hex}.tmp"
        try:
            try:
                with open_private_atomic_file(temporary) as handle:
                    handle.write(payload)
                    handle.flush()
                    os.fsync(handle.fileno())
            except (ConfigurationError, OSError) as error:
                raise QueueConflictError(
                    "cannot create private staged queue record: "
                    f"{logical_filesystem_path(temporary)}"
                ) from error
            observed_staging = os.lstat(staging)
            observed_parent = os.lstat(internal_path.parent)
            if not os.path.samestat(staging_stat, observed_staging):
                raise QueueConflictError(
                    "queue write staging changed before replace: "
                    f"{logical_filesystem_path(staging)}"
                )
            if not os.path.samestat(target_parent_stat, observed_parent):
                raise QueueConflictError(
                    f"queue write target directory changed before replace: {logical_path.parent}"
                )
            for attempt in range(ATOMIC_REPLACE_ATTEMPTS):
                try:
                    temporary.replace(internal_path)
                    break
                except PermissionError:
                    if attempt + 1 >= ATOMIC_REPLACE_ATTEMPTS:
                        raise
                    time.sleep(ATOMIC_REPLACE_RETRY_SECONDS)
        finally:
            _unlink_durable_path(temporary, missing_ok=True)
        self._fsync_write_directory(staging)
        self._fsync_write_directory(internal_path.parent)

    @staticmethod
    def _fsync_write_directory(path: Path) -> None:
        """Persist directory metadata where the platform exposes directory fsync."""
        try:
            directory_fd = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        except OSError:
            return
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)

    @staticmethod
    def _read_optional(path: Path, model: type[Record]) -> Record | None:
        if not path.exists():
            return None
        try:
            return ClioCoreQueue._read_json_file(path, model)
        except FileNotFoundError:
            return None

    @classmethod
    def _read_many(
        cls,
        directory: Path,
        model: type[Record],
        *,
        identity_field: str | None = None,
    ) -> Iterable[Record]:
        identity_field = identity_field or _record_identity_field(model)
        paths, truncated = cls._scan_json_record_paths(
            directory,
            limit=MAX_BOUNDED_SCAN_RECORDS,
            label=f"canonical {identity_field} records",
        )
        if truncated:
            raise QueueConflictError(
                "canonical record family exceeds the bounded read limit of "
                f"{MAX_BOUNDED_SCAN_RECORDS}: {directory}"
            )
        records: list[Record] = []
        for path in paths:
            try:
                record = cls._read_json_file(path, model)
            except FileNotFoundError:
                continue
            if getattr(record, identity_field, None) != path.stem:
                raise QueueConflictError(
                    f"canonical {identity_field} filename/content identity mismatch: {path}"
                )
            records.append(record)
        return records

    @classmethod
    def _scan_many(
        cls,
        directory: Path,
        model: type[Record],
        *,
        limit: int,
        identity_field: str | None = None,
    ) -> tuple[list[Record], bool]:
        if limit < 1:
            raise ValueError("record scan limit must be at least 1")
        identity_field = identity_field or _record_identity_field(model)
        paths, truncated = cls._scan_json_record_paths(
            directory,
            limit=limit,
            label=f"canonical {identity_field} records",
        )
        records: list[Record] = []
        for path in paths:
            try:
                record = cls._read_json_file(path, model)
            except FileNotFoundError:
                continue
            if getattr(record, identity_field, None) != path.stem:
                raise QueueConflictError(
                    f"canonical {identity_field} filename/content identity mismatch: {path}"
                )
            records.append(record)
        return records, truncated

    @staticmethod
    def _scan_json_record_paths(
        directory: Path,
        *,
        limit: int,
        label: str,
    ) -> tuple[list[Path], bool]:
        """Scan regular JSON children without following a replaced directory or entry."""
        try:
            directory_stat = os.lstat(directory)
        except FileNotFoundError:
            return [], False
        except OSError as error:
            raise QueueConflictError(f"cannot inspect {label}: {directory}") from error
        if not stat.S_ISDIR(directory_stat.st_mode) or _record_is_reparse(directory_stat):
            raise QueueConflictError(f"{label} is not a safe directory: {directory}")
        paths: list[Path] = []
        try:
            with os.scandir(directory) as entries:
                for entry in entries:
                    try:
                        entry_stat = entry.stat(follow_symlinks=False)
                    except FileNotFoundError:
                        continue
                    path = Path(entry.path)
                    if (
                        not entry.name.endswith(".json")
                        or not stat.S_ISREG(entry_stat.st_mode)
                        or _record_is_reparse(entry_stat)
                    ):
                        raise QueueConflictError(f"{label} contains an unsafe record: {path}")
                    if len(paths) >= limit:
                        return paths, True
                    paths.append(path)
        except QueueConflictError:
            raise
        except OSError as error:
            raise QueueConflictError(f"cannot scan {label}: {directory}") from error
        return paths, False

    @staticmethod
    def _bounded_json_record_paths(
        directory: Path,
        *,
        limit: int,
        label: str,
    ) -> list[Path]:
        """Return bounded regular JSON children or fail closed on ambiguous layout."""
        try:
            directory_stat = os.lstat(directory)
        except FileNotFoundError:
            return []
        if not stat.S_ISDIR(directory_stat.st_mode) or _record_is_reparse(directory_stat):
            raise QueueConflictError(f"{label} is not a safe directory: {directory}")
        paths: list[Path] = []
        try:
            with os.scandir(directory) as entries:
                for entry in entries:
                    if len(paths) >= limit:
                        raise QueueConflictError(
                            f"{label} exceeded its safety bound of {limit} records"
                        )
                    try:
                        entry_stat = entry.stat(follow_symlinks=False)
                    except FileNotFoundError:
                        continue
                    path = Path(entry.path)
                    if (
                        not entry.name.endswith(".json")
                        or not stat.S_ISREG(entry_stat.st_mode)
                        or _record_is_reparse(entry_stat)
                    ):
                        raise QueueConflictError(f"{label} contains an unsafe record: {path}")
                    paths.append(path)
        except OSError as exc:
            raise QueueConflictError(f"cannot scan {label}: {exc}") from exc
        return paths

    @staticmethod
    def _read_json_file(path: Path, model: type[Record]) -> Record:
        last_error: OSError | json.JSONDecodeError | QueueConflictError | None = None
        for _ in range(ATOMIC_REPLACE_ATTEMPTS):
            try:
                return model.model_validate_json(_read_bounded_record_bytes(path))
            except (PermissionError, json.JSONDecodeError) as exc:
                last_error = exc
                time.sleep(ATOMIC_REPLACE_RETRY_SECONDS)
            except QueueConflictError as exc:
                if not _transient_record_access_conflict(exc):
                    raise
                last_error = exc
                time.sleep(ATOMIC_REPLACE_RETRY_SECONDS)
        if last_error is not None:
            raise last_error
        return model.model_validate_json(_read_bounded_record_bytes(path))

    @staticmethod
    def _read_json_document(path: Path) -> object:
        try:
            return json.loads(_read_bounded_record_bytes(path))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise QueueConflictError(f"invalid JSON record {path}: {exc}") from exc


def _read_unique_json_document(path: Path) -> object:
    """Read JSON while rejecting duplicate keys at every object depth."""

    def unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
        document: dict[str, object] = {}
        for key, value in pairs:
            if key in document:
                raise QueueConflictError(f"duplicate JSON key {key!r} in {path}")
            document[key] = value
        return document

    try:
        return json.loads(
            _read_bounded_record_bytes(path),
            object_pairs_hook=unique_object,
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise QueueConflictError(f"invalid JSON record {path}: {exc}") from exc


def _record_identity_field(model: type[BaseModel]) -> str:
    """Return the filename-bound identity field for a canonical queue model."""
    identity_fields: dict[type[BaseModel], str] = {
        ArtifactRef: "artifact_id",
        EndpointRegistration: "endpoint_id",
        GatewaySession: "session_id",
        Lease: "lease_id",
        MonitorRule: "rule_id",
        ProgressRecord: "progress_id",
        RelayJob: "job_id",
        RelayTask: "task_id",
        SchedulerCancelPending: "job_id",
    }
    try:
        return identity_fields[model]
    except KeyError as error:
        raise QueueConflictError(
            f"canonical record model has no filename identity contract: {model.__name__}"
        ) from error


def _require_browser_attachment_session_ready(session: GatewaySession) -> None:
    """Require the latest gateway state to remain eligible for a new attachment."""
    if session.metadata.get("owner") != "clio-relay":
        raise QueueConflictError("browser attachment requires an owned clio-relay runtime")
    if session.state is not GatewaySessionState.READY:
        raise QueueConflictError("browser attachment requires a ready gateway session")
    if session.gateway.get("teardown_intent") is not None:
        raise QueueConflictError("a gateway committed to teardown cannot issue attachments")
    if not isinstance(session.gateway.get("jarvis_runtime_binding"), dict):
        raise QueueConflictError("browser attachment requires a verified JARVIS binding")
    if not isinstance(session.gateway.get("runtime_spec"), dict):
        raise QueueConflictError("browser attachment requires an owned runtime specification")


def _browser_attachment_record(
    session: GatewaySession,
    *,
    required: bool,
) -> BrowserAttachmentRecord | None:
    """Parse the exact current browser attachment below one gateway session."""
    raw = session.gateway.get("browser_attachment")
    if raw is None:
        if required:
            raise QueueConflictError("gateway has no browser attachment")
        return None
    try:
        return BrowserAttachmentRecord.model_validate(raw)
    except ValueError as exc:
        raise QueueConflictError("gateway browser attachment record is invalid") from exc


def _gateway_mapping(gateway: dict[str, object], field: str) -> dict[str, object]:
    """Return a copy of one optional gateway mapping or fail on corrupt state."""
    raw = gateway.get(field)
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise QueueConflictError(f"gateway {field} record is invalid")
    return dict(cast(dict[str, object], raw))


def _validate_browser_proxy_intent(
    intent: dict[str, object],
    *,
    attachment_id: str,
    expected_state: str | None = None,
) -> None:
    """Bind one ownership transition to the exact browser attachment."""
    state = intent.get("state")
    if (
        intent.get("schema_version") != "clio-relay.gateway-ownership-intent.v1"
        or intent.get("attachment_id") != attachment_id
        or not isinstance(state, str)
        or (expected_state is not None and state != expected_state)
    ):
        raise QueueConflictError("browser proxy ownership intent identity is invalid")
    for field in ("owner_token", "connector_generation_id", "config_path"):
        value = intent.get(field)
        if not isinstance(value, str) or not value:
            raise QueueConflictError(f"browser proxy ownership intent has no {field}")


def _validate_browser_proxy_identity(
    proxy: dict[str, object],
    *,
    attachment_id: str,
    proxy_process_id: int | None,
) -> None:
    """Require one process record to belong to the exact attachment transition."""
    if proxy.get("attachment_id") != attachment_id:
        raise QueueConflictError("browser proxy attachment identity is invalid")
    pid = proxy.get("pid")
    if (
        proxy_process_id is None
        or not isinstance(pid, int)
        or isinstance(pid, bool)
        or pid != proxy_process_id
    ):
        raise QueueConflictError("browser proxy process identity is invalid")


def _require_browser_proxy_ownership_consistent(
    *documents: dict[str, object],
) -> None:
    """Require every transition document to retain the same bearer ownership identity."""
    for field in ("owner_token", "connector_generation_id", "config_path"):
        values = [document.get(field) for document in documents]
        if (
            not all(isinstance(value, str) and value for value in values)
            or len(set(cast(list[str], values))) != 1
        ):
            raise QueueConflictError(f"browser proxy {field} changed during transition")


def _require_same_browser_attachment(
    current: BrowserAttachmentRecord,
    proposed: BrowserAttachmentRecord,
) -> None:
    """Reject changes to capability identity across lifecycle-only transitions."""
    excluded = {"state", "proxy_process_id", "revoked_at"}
    if current.model_dump(exclude=excluded) != proposed.model_dump(exclude=excluded):
        raise QueueConflictError("browser attachment identity changed during transition")
    if (
        current.proxy_process_id is not None
        and proposed.proxy_process_id != current.proxy_process_id
    ):
        raise QueueConflictError("browser attachment proxy process changed during transition")


def _has_relay_managed_gateway_state(gateway: dict[str, object]) -> bool:
    """Return whether a gateway payload contains relay-owned runtime identity."""
    if {
        "runtime_spec",
        "jarvis_runtime_binding",
        "browser_attachment",
        "ownership_intents",
        "teardown_intent",
        "teardown",
        "detach",
    }.intersection(gateway):
        return True
    transport = gateway.get("transport")
    if not isinstance(transport, dict):
        return False
    return bool(
        {"browser_proxy", "desktop_connector", "remote_connector"}.intersection(
            cast(dict[str, object], transport)
        )
    )


def _metadata_scheduler_gc_state(metadata: dict[str, object]) -> tuple[set[str], bool]:
    scheduler_ids: set[str] = set()
    terminal_ids: set[str] = set()
    scheduler_marker_seen = False

    def observe(document: object) -> None:
        nonlocal scheduler_marker_seen
        if not isinstance(document, dict):
            return
        typed = cast(dict[str, object], document)
        scheduler_id = typed.get("scheduler_job_id")
        if isinstance(scheduler_id, str) and scheduler_id:
            scheduler_marker_seen = True
            scheduler_ids.add(scheduler_id)
            phase = typed.get("phase")
            if isinstance(phase, str) and phase.lower() in _GC_TERMINAL_SCHEDULER_PHASES:
                terminal_ids.add(scheduler_id)
        elif typed.get("scheduler") is not None or typed.get("scheduler_provider") is not None:
            scheduler_marker_seen = True

    observe(metadata.get("runtime_metadata"))
    observe(metadata)
    observe(metadata.get("scheduler_status"))
    for field in ("scheduler_statuses", "scheduler_job_ownership"):
        documents = metadata.get(field)
        if isinstance(documents, list):
            typed_documents = cast(list[object], documents)
            if len(typed_documents) > MAX_SCHEDULER_METADATA_RECORDS:
                raise QueueConflictError(
                    f"{field} exceeds {MAX_SCHEDULER_METADATA_RECORDS} records"
                )
            for document in typed_documents:
                observe(document)
    raw_ids = metadata.get("scheduler_job_ids")
    if isinstance(raw_ids, list):
        typed_ids = cast(list[object], raw_ids)
        if len(typed_ids) > MAX_SCHEDULER_METADATA_RECORDS:
            raise QueueConflictError(
                f"scheduler_job_ids exceeds {MAX_SCHEDULER_METADATA_RECORDS} records"
            )
        for raw_id in typed_ids:
            if isinstance(raw_id, str) and raw_id:
                scheduler_marker_seen = True
                scheduler_ids.add(raw_id)
    return scheduler_ids, scheduler_marker_seen and scheduler_ids != terminal_ids


def _validate_new_owner_session_metadata(metadata: dict[str, object]) -> None:
    _validate_owner_session_identity_metadata(metadata, allow_legacy=False)


def _validate_owner_session_identity_metadata(
    metadata: dict[str, object],
    *,
    allow_legacy: bool,
) -> None:
    """Validate complete owner-session identity metadata."""
    _owner_session_identity(metadata, allow_legacy=allow_legacy)


def _owner_session_identity(
    metadata: dict[str, object],
    *,
    allow_legacy: bool,
) -> tuple[str, str | None] | None:
    owner_session_id = metadata.get("owner_session_id")
    generation_id = metadata.get("owner_session_generation_id")
    admission_session_id = metadata.get("owner_session_admission_id")
    if owner_session_id is None:
        if generation_id is not None or admission_session_id is not None:
            raise QueueConflictError(
                "owner_session_generation_id and owner_session_admission_id require "
                "owner_session_id"
            )
        return None
    if not isinstance(owner_session_id, str) or not owner_session_id:
        raise QueueConflictError("owner_session_id must be a non-empty string")
    if admission_session_id is not None and (
        not isinstance(admission_session_id, str)
        or not _safe_global_record_id(admission_session_id)
    ):
        raise QueueConflictError("owner_session_admission_id must be a safe identifier")
    if generation_id is None and allow_legacy:
        return owner_session_id, None
    if not isinstance(generation_id, str):
        raise QueueConflictError("new owner-session records require owner_session_generation_id")
    try:
        validate_durable_record_id(generation_id)
    except ValueError as error:
        raise QueueConflictError(
            "owner_session_generation_id must be a portable durable identifier"
        ) from error
    return owner_session_id, generation_id


def _require_artifact_lineage_owner_match(
    *,
    consumer: RelayJob,
    producer: RelayJob,
) -> None:
    """Forbid lineage edges across exact owner-session generation boundaries."""
    consumer_identity = _owner_session_identity(consumer.metadata, allow_legacy=True)
    producer_identity = _owner_session_identity(producer.metadata, allow_legacy=True)
    if consumer_identity is None and producer_identity is None:
        return
    if (
        consumer_identity is None
        or producer_identity is None
        or consumer_identity[1] is None
        or producer_identity[1] is None
        or consumer_identity != producer_identity
    ):
        raise QueueConflictError(
            "used artifact owner session generation does not match the consuming job"
        )


def _safe_owner_legacy_job_id(job_id: object) -> bool:
    return _safe_global_record_id(job_id)


def _safe_global_record_id(record_id: object) -> bool:
    if not isinstance(record_id, str):
        return False
    try:
        validate_durable_record_id(record_id)
    except ValueError:
        return False
    return True


def _endpoint_fresh_bucket(value: datetime) -> int:
    """Return the UTC minute bucket used by the live endpoint index."""
    observed = value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    return int(observed.timestamp()) // ENDPOINT_FRESH_BUCKET_SECONDS


def _job_matches_mcp_admission_class(
    job: RelayJob,
    admission_class: McpAdmissionClass,
) -> bool:
    """Match one durable job to a strict MCP worker lane.

    Non-MCP and kind/spec-mismatched jobs remain workload so the ordinary lane
    can fail them explicitly.  They can never enter the privileged control
    lane.
    """
    if job.kind is not JobKind.MCP_CALL or not isinstance(job.spec, McpCallSpec):
        return admission_class is McpAdmissionClass.WORKLOAD
    return job.spec.admission_class is admission_class


def _scheduler_cancellation_request(job: RelayJob) -> dict[str, object] | None:
    raw = job.metadata.get("cancellation_request")
    if not isinstance(raw, dict):
        return None
    request = cast(dict[str, object], raw)
    if request.get("schema_version") != "clio-relay.cancellation-request.v1":
        return None
    return request


def _cancellation_requested_at(request: dict[str, object]) -> datetime | None:
    raw = request.get("requested_at")
    if not isinstance(raw, str):
        return None
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


def _scheduler_cancel_record_is_due(
    record: SchedulerCancelPending,
    now: datetime,
) -> bool:
    if record.complete:
        return False
    if record.identity_resolution == "pending":
        return True
    return any(_scheduler_cancel_disposition_is_due(item, now) for item in record.dispositions)


def _scheduler_cancel_disposition_is_due(
    disposition: SchedulerCancelDisposition,
    now: datetime,
) -> bool:
    """Return whether one disposition has work not held by a live attempt claim."""
    if disposition.state in {
        SchedulerCancelDispositionState.PENDING,
        SchedulerCancelDispositionState.RETRY_WAIT,
    }:
        if (
            disposition.attempt_claim_id is not None
            and disposition.attempt_claim_expires_at is not None
            and disposition.attempt_claim_expires_at > now
        ):
            return False
        return disposition.next_attempt_at is None or disposition.next_attempt_at <= now
    if disposition.state is not SchedulerCancelDispositionState.CANCEL_REQUESTED:
        return False
    if (
        disposition.confirmation_claim_id is not None
        and disposition.confirmation_claim_expires_at is not None
        and disposition.confirmation_claim_expires_at > now
    ):
        return False
    return disposition.next_attempt_at is None or disposition.next_attempt_at <= now


def _scheduler_cancel_due_sort_key(record: SchedulerCancelPending) -> tuple[datetime, str]:
    due_times = [
        item.attempt_claim_expires_at
        or item.confirmation_claim_expires_at
        or item.next_attempt_at
        or record.requested_at
        for item in record.dispositions
        if item.state
        in {
            SchedulerCancelDispositionState.PENDING,
            SchedulerCancelDispositionState.RETRY_WAIT,
            SchedulerCancelDispositionState.CANCEL_REQUESTED,
        }
    ]
    return (min(due_times, default=record.requested_at), record.job_id)


def _bounded_regular_json_count(
    directory: Path,
    *,
    limit: int,
    label: str,
) -> tuple[int, bool]:
    """Count a controlled record directory only through its supported capacity."""
    count = 0
    try:
        with os.scandir(directory) as entries:
            for entry in entries:
                if not entry.name.endswith(".json"):
                    raise QueueConflictError(f"{label} contains an unsafe record: {entry.path}")
                entry_stat = entry.stat(follow_symlinks=False)
                if not stat.S_ISREG(entry_stat.st_mode) or _record_is_reparse(entry_stat):
                    raise QueueConflictError(f"{label} contains an unsafe record: {entry.path}")
                count += 1
                if count > limit:
                    return limit, True
    except FileNotFoundError:
        return 0, False
    except OSError as exc:
        raise QueueConflictError(f"cannot inspect {label}: {exc}") from exc
    return count, False


def _gateway_direct_job_ids(session: GatewaySession) -> set[str]:
    job_ids: set[str] = set()
    for field in ("relay_job_id", "job_id"):
        value = session.metadata.get(field)
        if isinstance(value, str) and value:
            job_ids.add(value)
    for provenance in _gateway_source_provenance(session):
        value = provenance.get("source_relay_job_id")
        if isinstance(value, str) and value:
            job_ids.add(value)
    return job_ids


def _gateway_direct_artifact_ids(session: GatewaySession) -> set[str]:
    artifact_ids = {artifact_id for artifact_id in session.artifacts if artifact_id}
    for provenance in _gateway_source_provenance(session):
        value = provenance.get("source_relay_artifact_id")
        if isinstance(value, str) and value:
            artifact_ids.add(value)
    return artifact_ids


def _gateway_source_provenance(session: GatewaySession) -> tuple[dict[str, Any], ...]:
    provenance = [session.metadata]
    runtime_binding = session.gateway.get("jarvis_runtime_binding")
    if isinstance(runtime_binding, dict):
        provenance.append(cast(dict[str, Any], runtime_binding))
    return tuple(provenance)


def _gateway_relation_is_preserved(
    raw_ref: dict[str, object],
    session: GatewaySession,
) -> bool:
    relation_kind = raw_ref.get("relation_kind")
    relation_key = raw_ref.get("relation_key")
    if not isinstance(relation_kind, str) or not isinstance(relation_key, str):
        raise QueueConflictError("gateway relation reference is invalid")
    if relation_kind == "direct":
        return relation_key in _gateway_direct_job_ids(session)
    if relation_kind == "artifact":
        return relation_key in _gateway_direct_artifact_ids(session)
    if relation_kind == "scheduler":
        return relation_key == session.scheduler_job_id
    raise QueueConflictError(f"unsupported gateway relation kind: {relation_kind}")


def _stable_ref_token(*values: str) -> str:
    encoded = "\x00".join(values).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()[:32]


def _lease_operational_records_present(root: Path) -> bool:
    for family in (
        "lease_indexes",
        "lease_identity_refs",
        "leases_by_endpoint",
        "leases_by_cluster_kind",
        "leases_by_expiry",
    ):
        directory = root / family
        try:
            with os.scandir(directory) as entries:
                if next(entries, None) is not None:
                    return True
        except FileNotFoundError:
            continue
        except OSError:
            return True
    return False


def _lease_scope_ref_name(identity: _LeaseIndexIdentity, *scope: str) -> str:
    return _lease_scope_ref_name_from_tokens(*_lease_reference(identity), *scope)


def _lease_scope_ref_name_from_tokens(
    lease_token: str,
    identity_token: str,
    *scope: str,
) -> str:
    scope_token = _stable_ref_token(
        "lease-scope",
        lease_token,
        identity_token,
        *scope,
    )[:16]
    return f"{lease_token}.{identity_token}.{scope_token}.ref"


def _lease_index_document(identity: _LeaseIndexIdentity) -> dict[str, object]:
    return {
        "schema_version": LEASE_OPERATIONAL_INDEX_SCHEMA,
        "lease_id": identity.lease_id,
        "job_id": identity.job_id,
        "endpoint_id": identity.endpoint_id,
        "cluster": identity.cluster,
        "job_kind": identity.job_kind.value,
        "expires_at": identity.expires_at.isoformat(),
    }


def _lease_capacity_aggregate_document(
    aggregate: _LeaseCapacityAggregate,
) -> dict[str, object]:
    """Serialize one validated lease-capacity aggregate."""
    return {
        **_lease_capacity_aggregate_digest_payload(
            epoch_id=aggregate.epoch_id,
            generation=aggregate.generation,
            checkpoint_id=aggregate.checkpoint_id,
            global_live_leases=aggregate.global_live_leases,
            cluster_kind_counts=aggregate.cluster_kind_counts,
        ),
        "document_sha256": aggregate.document_sha256,
    }


def _lease_capacity_aggregate_digest_payload(
    *,
    epoch_id: str,
    generation: int,
    checkpoint_id: str,
    global_live_leases: int,
    cluster_kind_counts: dict[str, dict[JobKind, int]],
) -> dict[str, object]:
    serialized_counts = _serialized_lease_capacity_counts(cluster_kind_counts)
    return {
        "schema_version": LEASE_CAPACITY_AGGREGATE_SCHEMA,
        "epoch_id": epoch_id,
        "generation": generation,
        "checkpoint_id": checkpoint_id,
        "global_live_leases": global_live_leases,
        "cluster_kind_counts": serialized_counts,
    }


def _serialized_lease_capacity_counts(
    cluster_kind_counts: dict[str, dict[JobKind, int]],
) -> dict[str, dict[str, int]]:
    return {
        cluster_token: {
            kind.value: kind_counts[kind]
            for kind in sorted(kind_counts, key=lambda item: item.value)
        }
        for cluster_token, kind_counts in sorted(cluster_kind_counts.items())
    }


def _lease_capacity_checkpoint_document(
    checkpoint: _LeaseCapacityCheckpoint,
) -> dict[str, object]:
    """Serialize one validated lease-capacity checkpoint."""
    return {
        **_lease_capacity_checkpoint_digest_payload(
            epoch_id=checkpoint.epoch_id,
            generation=checkpoint.generation,
            checkpoint_id=checkpoint.checkpoint_id,
            aggregate_document_sha256=checkpoint.aggregate_document_sha256,
        ),
        "document_sha256": checkpoint.document_sha256,
    }


def _lease_capacity_checkpoint_digest_payload(
    *,
    epoch_id: str,
    generation: int,
    checkpoint_id: str,
    aggregate_document_sha256: str,
) -> dict[str, object]:
    return {
        "schema_version": LEASE_CAPACITY_CHECKPOINT_SCHEMA,
        "epoch_id": epoch_id,
        "generation": generation,
        "checkpoint_id": checkpoint_id,
        "aggregate_document_sha256": aggregate_document_sha256,
    }


def _canonical_document_sha256(document: dict[str, object]) -> str:
    encoded = json.dumps(document, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _new_lease_capacity_pair(
    counts: dict[str, dict[JobKind, int]],
    *,
    epoch_id: str | None = None,
    generation: int = 0,
    checkpoint_id: str | None = None,
) -> _LeaseCapacityPair:
    normalized = _normalize_lease_capacity_counts(counts)
    selected_epoch = epoch_id or uuid4().hex
    selected_checkpoint = checkpoint_id or uuid4().hex
    global_total = sum(sum(by_kind.values()) for by_kind in normalized.values())
    payload = _lease_capacity_aggregate_digest_payload(
        epoch_id=selected_epoch,
        generation=generation,
        checkpoint_id=selected_checkpoint,
        global_live_leases=global_total,
        cluster_kind_counts=normalized,
    )
    aggregate_digest = _canonical_document_sha256(payload)
    aggregate = _LeaseCapacityAggregate(
        epoch_id=selected_epoch,
        generation=generation,
        checkpoint_id=selected_checkpoint,
        global_live_leases=global_total,
        cluster_kind_counts=normalized,
        document_sha256=aggregate_digest,
    )
    checkpoint_payload = _lease_capacity_checkpoint_digest_payload(
        epoch_id=selected_epoch,
        generation=generation,
        checkpoint_id=selected_checkpoint,
        aggregate_document_sha256=aggregate_digest,
    )
    checkpoint = _LeaseCapacityCheckpoint(
        epoch_id=selected_epoch,
        generation=generation,
        checkpoint_id=selected_checkpoint,
        aggregate_document_sha256=aggregate_digest,
        document_sha256=_canonical_document_sha256(checkpoint_payload),
    )
    return _LeaseCapacityPair(aggregate=aggregate, checkpoint=checkpoint)


def _normalize_lease_capacity_counts(
    counts: dict[str, dict[JobKind, int]],
) -> dict[str, dict[JobKind, int]]:
    normalized: dict[str, dict[JobKind, int]] = {}
    scopes = 0
    total = 0
    for cluster_token, kind_counts in counts.items():
        if not _is_short_ref_token(cluster_token):
            raise QueueConflictError("lease capacity aggregate has an invalid cluster scope")
        selected: dict[JobKind, int] = {}
        for kind, count in kind_counts.items():
            if isinstance(count, bool) or count <= 0:
                raise QueueConflictError(
                    "lease capacity aggregate counts must be positive integers"
                )
            if kind in selected:
                raise QueueConflictError("lease capacity aggregate repeats a job kind")
            selected[kind] = count
            scopes += 1
            total += count
        if not selected:
            raise QueueConflictError("lease capacity aggregate contains an empty cluster scope")
        normalized[cluster_token] = selected
    if scopes > MAX_LEASE_CAPACITY_SCOPES:
        raise QueueConflictError(
            "lease capacity aggregate exceeds its nonzero scope bound of "
            f"{MAX_LEASE_CAPACITY_SCOPES}"
        )
    if total > MAX_LIVE_LEASE_RECORDS:
        raise QueueConflictError(
            f"lease capacity aggregate exceeds its live lease bound of {MAX_LIVE_LEASE_RECORDS}"
        )
    return normalized


def _lease_capacity_aggregate_from_document(
    value: object,
    *,
    label: str,
) -> _LeaseCapacityAggregate:
    if not isinstance(value, dict):
        raise QueueConflictError(f"{label} is not an object")
    document = cast(dict[str, object], value)
    expected_fields = {
        "schema_version",
        "epoch_id",
        "generation",
        "checkpoint_id",
        "global_live_leases",
        "cluster_kind_counts",
        "document_sha256",
    }
    if set(document) != expected_fields or document.get("schema_version") != (
        LEASE_CAPACITY_AGGREGATE_SCHEMA
    ):
        raise QueueConflictError(f"{label} has an unsupported schema or fields")
    epoch_id = document.get("epoch_id")
    generation = document.get("generation")
    checkpoint_id = document.get("checkpoint_id")
    global_total = document.get("global_live_leases")
    raw_counts = document.get("cluster_kind_counts")
    digest = document.get("document_sha256")
    if (
        not _is_capacity_identity(epoch_id)
        or isinstance(generation, bool)
        or not isinstance(generation, int)
        or generation < 0
        or not _is_capacity_identity(checkpoint_id)
        or isinstance(global_total, bool)
        or not isinstance(global_total, int)
        or not 0 <= global_total <= MAX_LIVE_LEASE_RECORDS
        or not isinstance(raw_counts, dict)
        or not _is_sha256_digest(digest)
    ):
        raise QueueConflictError(f"{label} has invalid identity or count fields")
    counts: dict[str, dict[JobKind, int]] = {}
    for cluster_token, raw_kind_counts in cast(dict[object, object], raw_counts).items():
        if not isinstance(cluster_token, str) or not isinstance(raw_kind_counts, dict):
            raise QueueConflictError(f"{label} has an invalid cluster scope")
        parsed: dict[JobKind, int] = {}
        for raw_kind, raw_count in cast(dict[object, object], raw_kind_counts).items():
            if not isinstance(raw_kind, str):
                raise QueueConflictError(f"{label} has an invalid job kind")
            try:
                kind = JobKind(raw_kind)
            except ValueError as exc:
                raise QueueConflictError(f"{label} has an invalid job kind") from exc
            if kind.value != raw_kind:
                raise QueueConflictError(f"{label} has a noncanonical job kind")
            if isinstance(raw_count, bool) or not isinstance(raw_count, int):
                raise QueueConflictError(f"{label} has an invalid lease count")
            parsed[kind] = raw_count
        counts[cluster_token] = parsed
    normalized = _normalize_lease_capacity_counts(counts)
    observed_total = sum(sum(by_kind.values()) for by_kind in normalized.values())
    if observed_total != global_total:
        raise QueueConflictError(f"{label} global and scoped counts disagree")
    payload = _lease_capacity_aggregate_digest_payload(
        epoch_id=cast(str, epoch_id),
        generation=generation,
        checkpoint_id=cast(str, checkpoint_id),
        global_live_leases=global_total,
        cluster_kind_counts=normalized,
    )
    if _canonical_document_sha256(payload) != digest:
        raise QueueConflictError(f"{label} checksum mismatch")
    return _LeaseCapacityAggregate(
        epoch_id=cast(str, epoch_id),
        generation=generation,
        checkpoint_id=cast(str, checkpoint_id),
        global_live_leases=global_total,
        cluster_kind_counts=normalized,
        document_sha256=cast(str, digest),
    )


def _lease_capacity_checkpoint_from_document(
    value: object,
    *,
    label: str,
) -> _LeaseCapacityCheckpoint:
    if not isinstance(value, dict):
        raise QueueConflictError(f"{label} is not an object")
    document = cast(dict[str, object], value)
    expected_fields = {
        "schema_version",
        "epoch_id",
        "generation",
        "checkpoint_id",
        "aggregate_document_sha256",
        "document_sha256",
    }
    if set(document) != expected_fields or document.get("schema_version") != (
        LEASE_CAPACITY_CHECKPOINT_SCHEMA
    ):
        raise QueueConflictError(f"{label} has an unsupported schema or fields")
    epoch_id = document.get("epoch_id")
    generation = document.get("generation")
    checkpoint_id = document.get("checkpoint_id")
    aggregate_digest = document.get("aggregate_document_sha256")
    digest = document.get("document_sha256")
    if (
        not _is_capacity_identity(epoch_id)
        or isinstance(generation, bool)
        or not isinstance(generation, int)
        or generation < 0
        or not _is_capacity_identity(checkpoint_id)
        or not _is_sha256_digest(aggregate_digest)
        or not _is_sha256_digest(digest)
    ):
        raise QueueConflictError(f"{label} has invalid identity fields")
    payload = _lease_capacity_checkpoint_digest_payload(
        epoch_id=cast(str, epoch_id),
        generation=generation,
        checkpoint_id=cast(str, checkpoint_id),
        aggregate_document_sha256=cast(str, aggregate_digest),
    )
    if _canonical_document_sha256(payload) != digest:
        raise QueueConflictError(f"{label} checksum mismatch")
    return _LeaseCapacityCheckpoint(
        epoch_id=cast(str, epoch_id),
        generation=generation,
        checkpoint_id=cast(str, checkpoint_id),
        aggregate_document_sha256=cast(str, aggregate_digest),
        document_sha256=cast(str, digest),
    )


def _validate_lease_capacity_pair(pair: _LeaseCapacityPair, *, label: str) -> None:
    aggregate = pair.aggregate
    checkpoint = pair.checkpoint
    if (
        checkpoint.epoch_id != aggregate.epoch_id
        or checkpoint.generation != aggregate.generation
        or checkpoint.checkpoint_id != aggregate.checkpoint_id
        or checkpoint.aggregate_document_sha256 != aggregate.document_sha256
    ):
        raise QueueConflictError(f"{label} aggregate and checkpoint disagree")


def _lease_capacity_pair_payload(pair: _LeaseCapacityPair) -> dict[str, object]:
    return {
        "aggregate": _lease_capacity_aggregate_document(pair.aggregate),
        "checkpoint": _lease_capacity_checkpoint_document(pair.checkpoint),
    }


def _lease_capacity_pair_from_payload(value: object, *, label: str) -> _LeaseCapacityPair:
    if not isinstance(value, dict):
        raise QueueConflictError(f"{label} is not a lease capacity pair")
    payload = cast(dict[str, object], value)
    if set(payload) != {"aggregate", "checkpoint"}:
        raise QueueConflictError(f"{label} is not a lease capacity pair")
    pair = _LeaseCapacityPair(
        aggregate=_lease_capacity_aggregate_from_document(
            payload.get("aggregate"),
            label=f"{label} aggregate",
        ),
        checkpoint=_lease_capacity_checkpoint_from_document(
            payload.get("checkpoint"),
            label=f"{label} checkpoint",
        ),
    )
    _validate_lease_capacity_pair(pair, label=label)
    return pair


def _is_capacity_identity(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 32
        and all(character in "0123456789abcdef" for character in value)
    )


def _lease_index_identity_from_document(
    value: object,
    *,
    label: str,
) -> _LeaseIndexIdentity:
    if not isinstance(value, dict):
        raise QueueConflictError(f"{label} is not an object")
    document = cast(dict[str, object], value)
    expires_at_value = document.get("expires_at")
    job_kind_value = document.get("job_kind")
    try:
        if not isinstance(expires_at_value, str) or not isinstance(job_kind_value, str):
            raise ValueError("lease index temporal or kind identity is invalid")
        expires_at = datetime.fromisoformat(expires_at_value)
        job_kind = JobKind(job_kind_value)
    except ValueError as exc:
        raise QueueConflictError(f"{label} has invalid fields") from exc
    lease_id = document.get("lease_id")
    job_id = document.get("job_id")
    endpoint_id = document.get("endpoint_id")
    cluster = document.get("cluster")
    if (
        document.get("schema_version") != LEASE_OPERATIONAL_INDEX_SCHEMA
        or not _safe_global_record_id(lease_id)
        or not _safe_global_record_id(job_id)
        or not _safe_global_record_id(endpoint_id)
        or not isinstance(cluster, str)
        or not cluster
        or expires_at.tzinfo is None
    ):
        raise QueueConflictError(f"{label} identity mismatch")
    return _LeaseIndexIdentity(
        lease_id=cast(str, lease_id),
        job_id=cast(str, job_id),
        endpoint_id=cast(str, endpoint_id),
        cluster=cluster,
        job_kind=job_kind,
        expires_at=expires_at,
    )


def _lease_reference_from_scope_ref(
    name: str,
    *scope: str,
) -> tuple[str, str] | None:
    parts = name.split(".")
    if len(parts) != 4 or parts[3] != "ref":
        return None
    lease_token, identity_token, _scope_token, _suffix = parts
    if not all(_is_short_ref_token(token) for token in parts[:3]):
        return None
    expected = _lease_scope_ref_name_from_tokens(
        lease_token,
        identity_token,
        *scope,
    )
    if name != expected:
        return None
    return lease_token, identity_token


def _lease_reference(identity: _LeaseIndexIdentity) -> tuple[str, str]:
    return _lease_index_token(identity.lease_id), _lease_identity_token(identity)


def _parse_lease_reference_key(value: str) -> tuple[str, str] | None:
    parts = value.split(".")
    if len(parts) != 2 or not all(_is_short_ref_token(token) for token in parts):
        return None
    return parts[0], parts[1]


def _parse_lease_identity_ref_name(name: str) -> tuple[str, str] | None:
    if not name.endswith(".ref"):
        return None
    return _parse_lease_reference_key(name[: -len(".ref")])


def _is_short_ref_token(value: str) -> bool:
    return len(value) == 16 and all(character in "0123456789abcdef" for character in value)


def _lease_index_token(lease_id: str) -> str:
    return _stable_ref_token("lease", lease_id)[:16]


def _lease_job_token(job_id: str) -> str:
    return _stable_ref_token("job", job_id)[:16]


def _lease_endpoint_token(endpoint_id: str) -> str:
    return _stable_ref_token("endpoint", endpoint_id)[:16]


def _lease_cluster_token(cluster: str) -> str:
    return _stable_ref_token("cluster", cluster)[:16]


def _lease_expiry_key(value: datetime) -> int:
    observed = value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    delta = observed.astimezone(UTC) - datetime(1970, 1, 1, tzinfo=UTC)
    return delta.days * 86_400_000_000 + delta.seconds * 1_000_000 + delta.microseconds


def _lease_expiry_ref_name(identity: _LeaseIndexIdentity) -> str:
    expires_key = _lease_expiry_key(identity.expires_at)
    cluster_token = _lease_cluster_token(identity.cluster)
    endpoint_token = _lease_endpoint_token(identity.endpoint_id)
    job_token = _lease_job_token(identity.job_id)
    kind_code = _lease_kind_code(identity.job_kind)
    lease_token, identity_token = _lease_reference(identity)
    return (
        f"{expires_key:020d}.{cluster_token}.{kind_code}."
        f"{endpoint_token}.{job_token}.{lease_token}.{identity_token}.ref"
    )


def _lease_kind_code(job_kind: JobKind) -> str:
    return {
        JobKind.JARVIS: "j",
        JobKind.REMOTE_AGENT: "r",
        JobKind.MCP_CALL: "m",
    }[job_kind]


def _lease_identity_token(identity: _LeaseIndexIdentity) -> str:
    return _lease_identity_token_from_parts(
        _lease_expiry_key(identity.expires_at),
        _lease_cluster_token(identity.cluster),
        _lease_kind_code(identity.job_kind),
        _lease_endpoint_token(identity.endpoint_id),
        _lease_job_token(identity.job_id),
        _lease_index_token(identity.lease_id),
    )


def _lease_identity_token_from_parts(
    expires_key: int,
    cluster_token: str,
    kind_code: str,
    endpoint_token: str,
    job_token: str,
    lease_token: str,
) -> str:
    return _stable_ref_token(
        "lease-identity-v2",
        f"{expires_key:020d}",
        cluster_token,
        kind_code,
        endpoint_token,
        job_token,
        lease_token,
    )[:16]


def _parse_lease_expiry_ref_name(
    name: str,
) -> tuple[int, str, JobKind, str, str, str, str] | None:
    parts = name.split(".")
    if len(parts) != 8 or parts[7] != "ref":
        return None
    (
        expires_raw,
        cluster_token,
        kind_code,
        endpoint_token,
        job_token,
        lease_token,
        identity_token,
        _suffix,
    ) = parts
    try:
        job_kind = {
            "j": JobKind.JARVIS,
            "r": JobKind.REMOTE_AGENT,
            "m": JobKind.MCP_CALL,
        }[kind_code]
        expires_key = int(expires_raw)
    except (KeyError, ValueError):
        return None
    if (
        len(expires_raw) != 20
        or not expires_raw.isdigit()
        or expires_key < 0
        or not all(
            _is_short_ref_token(token)
            for token in (
                cluster_token,
                endpoint_token,
                job_token,
                lease_token,
                identity_token,
            )
        )
        or identity_token
        != _lease_identity_token_from_parts(
            expires_key,
            cluster_token,
            kind_code,
            endpoint_token,
            job_token,
            lease_token,
        )
    ):
        return None
    return (
        expires_key,
        cluster_token,
        job_kind,
        endpoint_token,
        job_token,
        lease_token,
        identity_token,
    )


def _path_lstat(path: Path) -> os.stat_result | None:
    try:
        return os.lstat(path)
    except FileNotFoundError:
        return None


def _ensure_gc_parent(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    current = path
    while True:
        current_stat = os.lstat(current)
        if not stat.S_ISDIR(current_stat.st_mode) or _record_is_reparse(current_stat):
            raise QueueConflictError(f"GC destination contains an unsafe directory: {current}")
        if current.parent == current:
            return
        current = current.parent


def _move_gc_path(source: Path, destination: Path) -> bool:
    source_stat = _path_lstat(source)
    destination_stat = _path_lstat(destination)
    if source_stat is None:
        if destination_stat is not None:
            return False
        return False
    if destination_stat is not None:
        raise QueueConflictError(f"GC source and destination both exist: {source}")
    if stat.S_ISLNK(source_stat.st_mode) or _record_is_reparse(source_stat):
        raise QueueConflictError(f"GC refuses a symlink or reparse-point source: {source}")
    if not stat.S_ISREG(source_stat.st_mode) and not stat.S_ISDIR(source_stat.st_mode):
        raise QueueConflictError(f"GC refuses a non-file source: {source}")
    _ensure_gc_parent(destination.parent)
    if os.stat(source.parent).st_dev != os.stat(destination.parent).st_dev:
        raise QueueConflictError(f"GC move would cross filesystems: {source}")
    try:
        source.replace(destination)
    except OSError as exc:
        raise QueueConflictError(f"GC could not quarantine {source}: {exc}") from exc
    return True


def purge_quarantined_tree_batch(root: Path, *, limit: int) -> tuple[int, bool]:
    """Remove at most ``limit`` entries from one quarantined owned tree."""
    return _purge_tree_batch(root, limit=limit)


def _purge_tree_batch(root: Path, *, limit: int) -> tuple[int, bool]:
    if limit < 0 or limit > 100:
        raise ValueError("GC purge limit must be between 0 and 100")
    if limit == 0:
        return 0, _path_lstat(root) is None
    removed = 0
    while removed < limit:
        deleted = _purge_one_gc_entry(root, root=root)
        if not deleted:
            break
        removed += 1
    return removed, _path_lstat(root) is None


def _purge_one_gc_entry(path: Path, *, root: Path) -> bool:
    root_stat = _path_lstat(root)
    if root_stat is None:
        return False
    if not stat.S_ISDIR(root_stat.st_mode) or _record_is_reparse(root_stat):
        raise QueueConflictError(f"GC trash root is not a regular directory: {root}")
    candidate = path
    depth = 0
    inspected = 0
    while True:
        inspected += 1
        if inspected > MAX_GC_PURGE_SCAN_ENTRIES:
            raise QueueConflictError(f"GC trash traversal exceeded its entry bound: {root}")
        candidate_stat = _path_lstat(candidate)
        if candidate_stat is None:
            return False
        is_directory = stat.S_ISDIR(candidate_stat.st_mode)
        if (
            stat.S_ISLNK(candidate_stat.st_mode)
            or _record_is_reparse(candidate_stat)
            or not is_directory
        ):
            if candidate == root:
                raise QueueConflictError(f"GC trash root is not a regular directory: {root}")
            _remove_gc_candidate(
                root,
                candidate,
                root_stat=root_stat,
                candidate_stat=candidate_stat,
            )
            return True
        try:
            with os.scandir(candidate) as entries:
                entry = next(entries, None)
        except OSError as exc:
            raise QueueConflictError(
                f"GC could not scan quarantined directory {candidate}: {exc}"
            ) from exc
        after_scan = _path_lstat(candidate)
        if after_scan is None or not os.path.samestat(candidate_stat, after_scan):
            raise QueueConflictError(f"GC trash changed during traversal: {candidate}")
        if entry is None:
            _remove_gc_candidate(
                root,
                candidate,
                root_stat=root_stat,
                candidate_stat=candidate_stat,
            )
            return True
        depth += 1
        if depth > MAX_GC_PURGE_DEPTH:
            raise QueueConflictError(f"GC trash traversal exceeded its depth bound: {root}")
        candidate = Path(entry.path)


def _remove_gc_candidate(
    root: Path,
    candidate: Path,
    *,
    root_stat: os.stat_result,
    candidate_stat: os.stat_result,
) -> None:
    if os.name != "nt":
        _remove_gc_candidate_posix(root, candidate, candidate_stat=candidate_stat)
        return
    current_root = _path_lstat(root)
    current_candidate = _path_lstat(candidate)
    if (
        current_root is None
        or current_candidate is None
        or not os.path.samestat(root_stat, current_root)
        or not os.path.samestat(candidate_stat, current_candidate)
    ):
        raise QueueConflictError(f"GC trash changed before deletion: {candidate}")
    _validate_gc_candidate_ancestry(root, candidate)
    try:
        if stat.S_ISDIR(candidate_stat.st_mode):
            os.rmdir(candidate)
        else:
            candidate.unlink()
    except OSError as exc:
        raise QueueConflictError(
            f"GC could not remove quarantined path {candidate}: {exc}"
        ) from exc


def _remove_gc_candidate_posix(
    root: Path,
    candidate: Path,
    *,
    candidate_stat: os.stat_result,
) -> None:
    anchor = root if candidate != root else root.parent
    relative = candidate.relative_to(anchor)
    parts = relative.parts
    if not parts or any(part in {"", ".", ".."} for part in parts):
        raise QueueConflictError(f"GC candidate escaped its trash root: {candidate}")
    flags = (
        os.O_RDONLY
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_CLOEXEC", 0)
    )
    descriptors: list[int] = []
    try:
        descriptor = os.open(anchor, flags)
        descriptors.append(descriptor)
        for part in parts[:-1]:
            descriptor = os.open(part, flags, dir_fd=descriptor)
            descriptors.append(descriptor)
        name = parts[-1]
        current = os.stat(name, dir_fd=descriptor, follow_symlinks=False)
        if not os.path.samestat(candidate_stat, current):
            raise QueueConflictError(f"GC trash changed before deletion: {candidate}")
        if stat.S_ISDIR(current.st_mode):
            os.rmdir(name, dir_fd=descriptor)
        else:
            os.unlink(name, dir_fd=descriptor)
    except QueueConflictError:
        raise
    except OSError as exc:
        raise QueueConflictError(
            f"GC could not remove quarantined path {candidate}: {exc}"
        ) from exc
    finally:
        for descriptor in reversed(descriptors):
            os.close(descriptor)


def _validate_gc_candidate_ancestry(root: Path, candidate: Path) -> None:
    relative = candidate.relative_to(root) if candidate != root else Path()
    current = root
    for part in relative.parts[:-1]:
        current /= part
        current_stat = os.lstat(current)
        if not stat.S_ISDIR(current_stat.st_mode) or _record_is_reparse(current_stat):
            raise QueueConflictError(f"GC candidate has unsafe ancestry: {candidate}")


def _record_family(path: Path) -> str:
    if "global_order" in path.parts[:-1]:
        return "global_order"
    for part in reversed(path.parts[:-1]):
        if part in RECORD_FAMILY_MAX_BYTES:
            return part
    return "unknown"


def _record_max_bytes(path: Path) -> int:
    return RECORD_FAMILY_MAX_BYTES.get(_record_family(path), DEFAULT_RECORD_MAX_BYTES)


def _record_is_reparse(file_stat: os.stat_result) -> bool:
    attributes = getattr(file_stat, "st_file_attributes", 0) or 0
    return bool(attributes & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400))


def _validate_record_stat(file_stat: os.stat_result, *, path: Path) -> None:
    if not stat.S_ISREG(file_stat.st_mode) or _record_is_reparse(file_stat):
        raise QueueConflictError(f"durable record is not a regular owned file: {path}")
    if file_stat.st_nlink == 0:
        raise _TransientRecordReplacement(f"durable record was atomically unlinked: {path}")
    if file_stat.st_nlink != 1:
        raise QueueConflictError(f"durable record must not be hard linked: {path}")


def _record_stats_match(
    expected: os.stat_result,
    observed: os.stat_result,
    *,
    compare_ctime: bool,
) -> bool:
    """Return whether two observations describe one unchanged durable record."""
    shared_metadata_matches = (
        expected.st_mode,
        expected.st_nlink,
        expected.st_uid,
        expected.st_gid,
        expected.st_size,
        expected.st_mtime_ns,
        getattr(expected, "st_file_attributes", 0) or 0,
    ) == (
        observed.st_mode,
        observed.st_nlink,
        observed.st_uid,
        observed.st_gid,
        observed.st_size,
        observed.st_mtime_ns,
        getattr(observed, "st_file_attributes", 0) or 0,
    )
    return (
        os.path.samestat(expected, observed)
        and shared_metadata_matches
        and (not compare_ctime or expected.st_ctime_ns == observed.st_ctime_ns)
    )


def _read_bounded_record_bytes_once(path: Path, *, limit: int) -> bytes:
    """Read one stable record generation or identify a transient replacement."""
    before = os.lstat(path)
    _validate_record_stat(before, path=path)
    flags = (
        os.O_RDONLY
        | getattr(os, "O_BINARY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_NONBLOCK", 0)
    )
    descriptor = -1
    try:
        try:
            descriptor = os.open(path, flags)
        except FileNotFoundError as exc:
            raise _TransientRecordReplacement(
                f"durable record disappeared while opening: {path}"
            ) from exc
        opened = os.fstat(descriptor)
        _validate_record_stat(opened, path=path)
        try:
            after_open = os.lstat(path)
        except FileNotFoundError as exc:
            raise _TransientRecordReplacement(
                f"durable record disappeared after opening: {path}"
            ) from exc
        _validate_record_stat(after_open, path=path)
        if (
            not _record_stats_match(before, opened, compare_ctime=False)
            or not _record_stats_match(opened, after_open, compare_ctime=False)
            or not _record_stats_match(before, after_open, compare_ctime=True)
        ):
            raise _TransientRecordReplacement(f"durable record changed while opening: {path}")
        chunks: list[bytes] = []
        total = 0
        while total <= limit:
            before_chunk = os.fstat(descriptor)
            _validate_record_stat(before_chunk, path=path)
            if not _record_stats_match(opened, before_chunk, compare_ctime=True):
                raise _TransientRecordReplacement(f"durable record changed while reading: {path}")
            chunk = os.read(descriptor, min(65_536, limit + 1 - total))
            after_chunk = os.fstat(descriptor)
            _validate_record_stat(after_chunk, path=path)
            if not _record_stats_match(opened, after_chunk, compare_ctime=True):
                raise _TransientRecordReplacement(f"durable record changed while reading: {path}")
            if not chunk:
                break
            chunks.append(chunk)
            total += len(chunk)
        final = os.fstat(descriptor)
        _validate_record_stat(final, path=path)
        try:
            after_read = os.lstat(path)
        except FileNotFoundError as exc:
            raise _TransientRecordReplacement(
                f"durable record disappeared after reading: {path}"
            ) from exc
        _validate_record_stat(after_read, path=path)
        if (
            not _record_stats_match(opened, final, compare_ctime=True)
            or not _record_stats_match(final, after_read, compare_ctime=False)
            or not _record_stats_match(before, after_read, compare_ctime=True)
        ):
            raise _TransientRecordReplacement(f"durable record changed while reading: {path}")
        if total > limit:
            raise QueueConflictError(
                f"{_record_family(path)} record exceeds the {limit}-byte limit: {path}"
            )
        if total != final.st_size:
            raise _TransientRecordReplacement(f"durable record changed size while reading: {path}")
        return b"".join(chunks)
    except (_TransientRecordReplacement, QueueConflictError):
        raise
    except OSError as exc:
        raise QueueConflictError(f"cannot read durable record {path}: {exc}") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _read_bounded_record_bytes(path: Path) -> bytes:
    """Read one stable bounded record, retrying only atomic replacement races."""
    limit = _record_max_bytes(path)
    last_replacement: _TransientRecordReplacement | None = None
    for attempt in range(ATOMIC_REPLACE_ATTEMPTS):
        try:
            return _read_bounded_record_bytes_once(path, limit=limit)
        except FileNotFoundError as exc:
            if last_replacement is None:
                raise
            last_replacement = _TransientRecordReplacement(
                f"durable record remained absent during atomic replacement: {path}"
            )
            last_replacement.__cause__ = exc
        except _TransientRecordReplacement as exc:
            last_replacement = exc
        if attempt + 1 < ATOMIC_REPLACE_ATTEMPTS:
            time.sleep(ATOMIC_REPLACE_RETRY_SECONDS)
    raise QueueConflictError(
        f"durable record did not stabilize after {ATOMIC_REPLACE_ATTEMPTS} "
        f"atomic replacement attempts: {path}"
    ) from last_replacement


def _transient_record_access_conflict(error: QueueConflictError) -> bool:
    """Return whether a durable read failed only on a transient sharing denial."""
    cause = error.__cause__
    if not isinstance(cause, OSError):
        return False
    return (
        isinstance(cause, PermissionError)
        or cause.errno in {errno.EACCES, errno.EPERM}
        or getattr(cause, "winerror", None) in {5, 32, 33}
    )


def _unlink_durable_path(path: Path, *, missing_ok: bool = False) -> None:
    """Delete one durable path after bounded Windows sharing-violation retries."""
    for attempt in range(ATOMIC_REPLACE_ATTEMPTS):
        try:
            path.unlink(missing_ok=missing_ok)
            return
        except OSError as error:
            if (
                getattr(error, "winerror", None) not in {5, 32, 33}
                or attempt + 1 >= ATOMIC_REPLACE_ATTEMPTS
            ):
                raise
            time.sleep(ATOMIC_REPLACE_RETRY_SECONDS)


def _job_idempotency_digest(job: RelayJob) -> str:
    payload = job.model_dump(mode="json")
    for generated_field in {
        "job_id",
        "state",
        "created_at",
        "updated_at",
        "leased_by",
        "attempts",
        "last_error",
        "submission_digest",
    }:
        payload.pop(generated_field, None)
    # Preserve the pre-lineage digest for submissions without dependencies so
    # existing idempotency records remain replayable after this additive schema
    # upgrade. Non-empty dependency pins remain part of the canonical identity.
    if not payload.get("used_artifact_refs"):
        payload.pop("used_artifact_refs", None)
    # Preserve the pre-JARVIS-lock digest for generic MCP calls. The marker is
    # release authority only when explicitly present on the built-in route.
    raw_spec = payload.get("spec")
    if isinstance(raw_spec, dict):
        spec_payload = cast(dict[str, object], raw_spec)
        if spec_payload.get("expected_jarvis_cd_lock_binding") is None:
            spec_payload.pop("expected_jarvis_cd_lock_binding", None)
        # Preserve the pre-admission-lane digest for ordinary MCP work. Only an
        # explicit control-query promotion is new caller-visible identity.
        if (
            job.kind is JobKind.MCP_CALL
            and isinstance(job.spec, McpCallSpec)
            and job.spec.admission_class is McpAdmissionClass.WORKLOAD
        ):
            spec_payload.pop("admission_class", None)
        if is_owned_jarvis_run_spec(job.kind, job.spec):
            raw_arguments = spec_payload.get("arguments")
            if isinstance(raw_arguments, dict):
                # The relay injects this identity at admission. Like job_id and
                # timestamps, it is durable output rather than caller payload,
                # so excluding it keeps pre-handle idempotency records replayable.
                cast(dict[str, object], raw_arguments).pop("execution_id", None)
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _is_sha256_digest(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def _artifact_user_cursor(sequence: int) -> str:
    if sequence < 1 or sequence >= 2**63:
        raise QueueConflictError("artifact-user cursor sequence is outside its durable range")
    return f"{ARTIFACT_USER_CURSOR_PREFIX}{sequence:0{ARTIFACT_USER_CURSOR_DIGITS}d}"


def _artifact_user_cursor_sequence(cursor: str) -> int:
    try:
        validate_durable_record_id(cursor)
    except (TypeError, ValueError) as error:
        raise ValueError(f"invalid cursor: {error}") from error
    digits = cursor.removeprefix(ARTIFACT_USER_CURSOR_PREFIX)
    if (
        not cursor.startswith(ARTIFACT_USER_CURSOR_PREFIX)
        or len(digits) != ARTIFACT_USER_CURSOR_DIGITS
        or not digits.isdecimal()
    ):
        raise ValueError("invalid cursor: expected an artifact-user edge cursor")
    sequence = int(digits)
    if sequence < 1 or sequence >= 2**63:
        raise ValueError("invalid cursor: artifact-user edge sequence is outside its range")
    return sequence


def _artifact_user_entry_sequence(path: Path) -> int:
    stem = path.stem
    if len(stem) != ARTIFACT_USER_CURSOR_DIGITS or not stem.isdecimal():
        raise QueueConflictError(f"artifact-user order entry filename is invalid: {path}")
    sequence = int(stem)
    if sequence < 1 or sequence >= 2**63 or stem != f"{sequence:020d}":
        raise QueueConflictError(f"artifact-user order entry sequence is invalid: {path}")
    return sequence


def _index_integer(index: dict[str, object], field: str) -> int:
    value = index.get(field)
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise QueueConflictError(f"invalid job index integer: {field}")
    return value


def _index_migration_components_complete(state: dict[str, object]) -> bool:
    """Return whether every independently replayable index checkpoint is complete."""
    for field in (
        "families",
        "order_families",
        "global_order_families",
        "retention_families",
        "operational_families",
    ):
        raw_family = state.get(field)
        if not isinstance(raw_family, dict):
            return False
        if any(
            not isinstance(raw_checkpoint, dict)
            or cast(dict[str, object], raw_checkpoint).get("complete") is not True
            for raw_checkpoint in cast(dict[str, object], raw_family).values()
        ):
            return False
    for field in ("finalize", "lease_operational_repair", "lease_capacity_aggregate"):
        raw_checkpoint = state.get(field)
        if (
            not isinstance(raw_checkpoint, dict)
            or cast(dict[str, object], raw_checkpoint).get("complete") is not True
        ):
            return False
    return True


def _migration_batch_paths(
    directory: Path,
    *,
    cursor: str | None,
    limit: int,
) -> tuple[list[Path], bool]:
    candidates = heapq.nsmallest(
        limit + 1,
        (path for path in directory.glob("*.json") if cursor is None or path.name > cursor),
        key=lambda path: path.name,
    )
    return candidates[:limit], len(candidates) > limit


def _last_contiguous_sequence(directory: Path) -> int:
    if not (directory / f"{1:020d}.json").is_file():
        return 0
    low = 1
    high = 2
    while (directory / f"{high:020d}.json").is_file():
        low = high
        high *= 2
        if high > 2**63:
            raise QueueConflictError(f"record sequence exceeds supported range: {directory}")
    while low + 1 < high:
        middle = (low + high) // 2
        if (directory / f"{middle:020d}.json").is_file():
            low = middle
        else:
            high = middle
    return low


def _idempotency_key_filename(key: str) -> str:
    digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
    return f"key_{digest}"


def _committed_idempotency_record(job: RelayJob, job_digest: str) -> dict[str, object]:
    return {
        "state": "committed",
        "job_id": job.job_id,
        "idempotency_key": job.idempotency_key,
        "job_digest": job_digest,
        "created_at": job.created_at.isoformat(),
        "committed_at": utc_now().isoformat(),
    }

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
import logging
import os
import stat
from collections.abc import Iterable
from contextlib import suppress
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Literal, TypeVar, cast
from uuid import uuid4

from pydantic import BaseModel

from clio_relay import (
    queue_artifact_lineage,
    queue_artifacts,
    queue_context,
    queue_endpoints,
    queue_events,
    queue_idempotency,
    queue_index_state,
    queue_jarvis_inputs,
    queue_layout,
    queue_lease_records,
    queue_legacy_audit,
    queue_legacy_output_audit,
    queue_legacy_output_codec,
    queue_legacy_output_migration,
    queue_order_index,
    queue_owner_session_lifecycle,
    queue_owner_session_records,
    queue_scheduler_cancel_records,
    queue_store_lock,
    queue_store_read,
    queue_store_write,
)
from clio_relay.browser_gateway import BrowserAttachmentRecord
from clio_relay.cluster_config import (
    ensure_private_configuration_path,  # pyright: ignore[reportUnusedImport]  # noqa: F401 - live compatibility patch seam
)
from clio_relay.command_evidence import bounded_error_detail
from clio_relay.errors import (
    ConfigurationError,
    McpTaskIdentityConflictError,
    NotFoundError,
    QueueConflictError,
    queue_conflict_from_cause,
)
from clio_relay.filesystem_paths import internal_filesystem_path, logical_filesystem_path
from clio_relay.identifiers import validate_durable_record_id
from clio_relay.models import (
    INPUT_INGEST_POLICY_METADATA_KEY,
    TERMINAL_STATES,
    ArtifactRef,
    ArtifactUse,
    EndpointRegistration,
    GatewaySession,
    GatewaySessionState,
    InputArtifactIngestPolicy,
    InputArtifactSpec,
    JarvisPackageInputContractRecord,
    JarvisPackageInputRoute,
    JarvisPipelineInputBinding,
    JarvisPipelineInputBindings,
    JarvisPipelineInputLineage,
    JarvisPipelineInputRoute,
    JarvisRunInputManifest,
    JobGcPhase,
    JobKind,
    JobState,
    JobTombstone,
    Lease,
    McpAdmissionClass,
    McpCallSpec,
    MonitorRule,
    OwnerSessionJobMembership,
    ProgressRecord,
    RelayEvent,
    RelayJob,
    RelayMcpTaskProjection,
    RelayMcpTaskRecord,
    RelayTask,
    SchedulerCancelDisposition,
    SchedulerCancelDispositionState,
    SchedulerCancelPending,
    SchedulerPhase,
    TerminalJobGcPlan,
    TerminalJobGcResult,
    UsedArtifactRef,
    deterministic_input_artifact_id,
    prepare_owned_jarvis_run_submission,
    utc_now,
)
from clio_relay.pagination import (
    MAX_RESPONSE_PAGE_RECORDS,
    validate_gc_batch_size,
    validate_response_page_limit,
)
from clio_relay.remote_mcp import VIRTUAL_REMOTE_MCP_RELAY_CONTROL_FIELDS
from clio_relay.worker_concurrency import KindConcurrencyInput, normalize_kind_concurrency
from clio_relay.worker_lifetime_lock import (
    LockedCoreIdentity,
    exclusive_migration_lifetime,
    require_active_locked_core,
)

logger = logging.getLogger(__name__)
Record = TypeVar("Record", bound=BaseModel)
_LeaseExpiryReference = queue_layout.LeaseExpiryReference
_UNSET = queue_layout.UNSET
INPUT_INGEST_ATTEMPT_METADATA_KEY = queue_layout.INPUT_INGEST_ATTEMPT_METADATA_KEY
INPUT_INGEST_ATTEMPT_SCHEMA = queue_layout.INPUT_INGEST_ATTEMPT_SCHEMA
INPUT_INGEST_ORIGINAL_POLICY_METADATA_KEY = queue_layout.INPUT_INGEST_ORIGINAL_POLICY_METADATA_KEY
DEFAULT_INPUT_INGEST_ABANDONED_AFTER_SECONDS = (
    queue_layout.DEFAULT_INPUT_INGEST_ABANDONED_AFTER_SECONDS
)
MAX_INPUT_INGEST_RECOVERY_BATCH = queue_layout.MAX_INPUT_INGEST_RECOVERY_BATCH
DEFAULT_CORE_LOCK_TIMEOUT_SECONDS = queue_layout.DEFAULT_CORE_LOCK_TIMEOUT_SECONDS
MIN_SCHEDULER_CANCEL_CLAIM_LEASE_SECONDS = queue_layout.MIN_SCHEDULER_CANCEL_CLAIM_LEASE_SECONDS
MAX_SCHEDULER_CANCEL_CLAIM_LEASE_SECONDS = queue_layout.MAX_SCHEDULER_CANCEL_CLAIM_LEASE_SECONDS
ATOMIC_REPLACE_ATTEMPTS = queue_layout.ATOMIC_REPLACE_ATTEMPTS
ATOMIC_REPLACE_RETRY_SECONDS = queue_layout.ATOMIC_REPLACE_RETRY_SECONDS
WRITE_STAGING_FAMILY = queue_layout.WRITE_STAGING_FAMILY
WRITE_STAGING_MAX_LEFTOVERS = queue_layout.WRITE_STAGING_MAX_LEFTOVERS
OWNER_SESSION_CLOSURE_WRITE_ATTEMPTS = queue_layout.OWNER_SESSION_CLOSURE_WRITE_ATTEMPTS
JOB_INDEX_SCHEMA = queue_layout.JOB_INDEX_SCHEMA
INDEX_MIGRATION_SCHEMA = queue_layout.INDEX_MIGRATION_SCHEMA
LEASE_OPERATIONAL_INDEX_SCHEMA = queue_layout.LEASE_OPERATIONAL_INDEX_SCHEMA
_LEGACY_LEASE_OPERATIONAL_INDEX_SCHEMA = queue_layout.LEGACY_LEASE_OPERATIONAL_INDEX_SCHEMA
LEASE_CAPACITY_AGGREGATE_SCHEMA = queue_layout.LEASE_CAPACITY_AGGREGATE_SCHEMA
LEASE_CAPACITY_CHECKPOINT_SCHEMA = queue_layout.LEASE_CAPACITY_CHECKPOINT_SCHEMA
LEASE_CAPACITY_AUDIT_SCHEMA = queue_layout.LEASE_CAPACITY_AUDIT_SCHEMA
DEFAULT_EXACT_RECORD_LIMIT = queue_layout.DEFAULT_EXACT_RECORD_LIMIT
MAX_ACTIVE_JOB_RECORDS = queue_layout.MAX_ACTIVE_JOB_RECORDS
MAX_LIVE_LEASE_RECORDS = queue_layout.MAX_LIVE_LEASE_RECORDS
MAX_LEASE_CAPACITY_SCOPES = queue_layout.MAX_LEASE_CAPACITY_SCOPES
MAX_LEASE_CAPACITY_RECORD_BYTES = queue_layout.MAX_LEASE_CAPACITY_RECORD_BYTES
MAX_BOUNDED_SCAN_RECORDS = queue_layout.MAX_BOUNDED_SCAN_RECORDS
MAX_GATEWAY_INDEX_RECORDS = queue_layout.MAX_GATEWAY_INDEX_RECORDS
MAX_SCHEDULER_METADATA_RECORDS = queue_layout.MAX_SCHEDULER_METADATA_RECORDS
MAX_TRANSITION_INTENT_RECORDS = queue_layout.MAX_TRANSITION_INTENT_RECORDS
MAX_JARVIS_PACKAGE_INPUT_CONTRACT_RECORDS = queue_layout.MAX_JARVIS_PACKAGE_INPUT_CONTRACT_RECORDS
MAX_JARVIS_PIPELINE_INPUT_BINDING_RECORDS = queue_layout.MAX_JARVIS_PIPELINE_INPUT_BINDING_RECORDS
MAX_JARVIS_PIPELINE_INPUT_LINEAGE_RECORDS = queue_layout.MAX_JARVIS_PIPELINE_INPUT_LINEAGE_RECORDS
MAX_JARVIS_RUN_INPUT_MANIFEST_RECORDS = queue_layout.MAX_JARVIS_RUN_INPUT_MANIFEST_RECORDS
MAX_ARTIFACT_USES_PER_JOB = queue_layout.MAX_ARTIFACT_USES_PER_JOB
MAX_ARTIFACT_CONSUMERS = queue_layout.MAX_ARTIFACT_CONSUMERS
ARTIFACT_USER_CURSOR_PREFIX = queue_layout.ARTIFACT_USER_CURSOR_PREFIX
ARTIFACT_USER_CURSOR_DIGITS = queue_layout.ARTIFACT_USER_CURSOR_DIGITS
ENDPOINT_FRESH_BUCKET_SECONDS = queue_layout.ENDPOINT_FRESH_BUCKET_SECONDS
MAX_ENDPOINT_FRESH_SECONDS = queue_layout.MAX_ENDPOINT_FRESH_SECONDS
MAX_ENDPOINT_FRESH_CLUSTER_ROOTS = queue_layout.MAX_ENDPOINT_FRESH_CLUSTER_ROOTS
ORDER_INDEX_SCHEMA = queue_layout.ORDER_INDEX_SCHEMA
RETENTION_INDEX_SCHEMA = queue_layout.RETENTION_INDEX_SCHEMA
GLOBAL_ORDER_INDEX_SCHEMA = queue_layout.GLOBAL_ORDER_INDEX_SCHEMA
GC_TRASH_SCHEMA = queue_layout.GC_TRASH_SCHEMA
MAX_GC_PURGE_DEPTH = queue_layout.MAX_GC_PURGE_DEPTH
MAX_GC_PURGE_SCAN_ENTRIES = queue_layout.MAX_GC_PURGE_SCAN_ENTRIES
DEFAULT_RECORD_MAX_BYTES = queue_layout.DEFAULT_RECORD_MAX_BYTES
LEGACY_OUTPUT_MIGRATION_SCHEMA = queue_layout.LEGACY_OUTPUT_MIGRATION_SCHEMA
LEGACY_OUTPUT_COMPATIBILITY_SCHEMA = queue_layout.LEGACY_OUTPUT_COMPATIBILITY_SCHEMA
LEGACY_OUTPUT_RECEIPT_SCHEMA = queue_layout.LEGACY_OUTPUT_RECEIPT_SCHEMA
LEGACY_RECORD_AUDIT_SCHEMA = queue_layout.LEGACY_RECORD_AUDIT_SCHEMA
CANONICAL_RECORD_ACCESS_SCHEMA = queue_layout.CANONICAL_RECORD_ACCESS_SCHEMA
QUEUE_LAYOUT_SCHEMA = queue_layout.QUEUE_LAYOUT_SCHEMA
MAX_LEGACY_OUTPUT_RECORD_BYTES = queue_layout.MAX_LEGACY_OUTPUT_RECORD_BYTES
MAX_LEGACY_OUTPUT_MIGRATION_BYTES = queue_layout.MAX_LEGACY_OUTPUT_MIGRATION_BYTES
MAX_LEGACY_OUTPUT_MIGRATION_RECORDS = queue_layout.MAX_LEGACY_OUTPUT_MIGRATION_RECORDS
MAX_LEGACY_EVENT_AUDIT_DIRECTORIES = queue_layout.MAX_LEGACY_EVENT_AUDIT_DIRECTORIES
MAX_LEGACY_EVENT_AUDIT_RECORDS = queue_layout.MAX_LEGACY_EVENT_AUDIT_RECORDS
RECORD_FAMILY_MAX_BYTES = queue_layout.RECORD_FAMILY_MAX_BYTES


_TransientRecordReplacement = queue_layout.TransientRecordReplacement


_UnsafeQueueDirectoryProtection = queue_store_lock.UnsafeQueueDirectoryProtection
LegacyQueueStateError = queue_store_lock.LegacyQueueStateError
QueueSealRequiresExclusive = queue_store_lock.QueueSealRequiresExclusive
_ORDER_FAMILIES = queue_store_lock.ORDER_FAMILIES
_GLOBAL_ORDER_FAMILIES = queue_store_lock.GLOBAL_ORDER_FAMILIES
_RETENTION_INDEX_FAMILIES = queue_store_lock.RETENTION_INDEX_FAMILIES
_OPERATIONAL_INDEX_FAMILIES = queue_store_lock.OPERATIONAL_INDEX_FAMILIES
_INITIALIZED_QUEUE_FAMILIES = queue_store_lock.INITIALIZED_QUEUE_FAMILIES
_ADDITIVE_QUEUE_FAMILIES = queue_store_lock.ADDITIVE_QUEUE_FAMILIES
_LEGACY_ONLY_QUEUE_FAMILIES = queue_store_lock.LEGACY_ONLY_QUEUE_FAMILIES
_GC_TERMINAL_SCHEDULER_PHASES = queue_store_lock.GC_TERMINAL_SCHEDULER_PHASES
_FairBoundedFileLock = queue_store_lock.FairBoundedFileLock


IdempotentSubmissionResolution = queue_idempotency.IdempotentSubmissionResolution
_job_idempotency_digest = queue_idempotency._job_idempotency_digest  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
_idempotency_key_filename = queue_idempotency._idempotency_key_filename  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
_committed_idempotency_record = queue_idempotency._committed_idempotency_record  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001


SchedulerCancelIdentityRegistration = (
    queue_scheduler_cancel_records.SchedulerCancelIdentityRegistration
)
SchedulerCancelAttemptClaim = queue_scheduler_cancel_records.SchedulerCancelAttemptClaim
SchedulerCancelConfirmationClaim = queue_scheduler_cancel_records.SchedulerCancelConfirmationClaim
_LeaseIndexIdentity = queue_lease_records.LeaseIndexIdentity
_LeaseCapacityAggregate = queue_lease_records.LeaseCapacityAggregate
_LeaseCapacityCheckpoint = queue_lease_records.LeaseCapacityCheckpoint
_LeaseCapacityPair = queue_lease_records.LeaseCapacityPair


_LegacyOutputAudit = queue_legacy_output_codec.LegacyOutputAudit
_LegacyOutputRecord = queue_legacy_output_codec.LegacyOutputRecord


_artifact_with_sequence = queue_artifacts.artifact_with_sequence


class _QueueStoreAdapter:
    """Expose private facade store state to the extracted queue owners."""

    def __init__(self, queue: ClioCoreQueue) -> None:
        self._queue = queue

    @property
    def storage_root(self) -> Path:
        """Return the internal filesystem root for durable queue records."""
        return self._queue._storage_root  # pyright: ignore[reportPrivateUsage]

    def locked_storage_root(self) -> tuple[int | None, tuple[int, int] | None]:
        """Return the migration-pinned queue-root descriptor and identity."""
        return (
            self._queue._locked_storage_root_descriptor,  # pyright: ignore[reportPrivateUsage]
            self._queue._locked_storage_root_identity,  # pyright: ignore[reportPrivateUsage]
        )

    @property
    def lock(self) -> queue_context.QueueLockProtocol:
        """Return the shared queue storage lock."""
        return self._queue._lock  # pyright: ignore[reportPrivateUsage]

    def initialize(self) -> None:
        """Initialize and validate the durable store."""
        self._queue.initialize()

    def read_optional(self, path: Path, model: type[Record]) -> Record | None:
        """Read one optional typed record through the store-read owner."""
        return self._queue._read_optional(path, model)  # pyright: ignore[reportPrivateUsage]

    def read_json_document(self, path: Path) -> object:
        """Read one strict JSON document through the store-read owner."""
        return queue_store_read.read_json_document(path)

    def write(self, path: Path, record: BaseModel) -> None:
        """Persist one typed record through the store-write owner."""
        self._queue._write(path, record)  # pyright: ignore[reportPrivateUsage]

    def write_json(self, path: Path, record: dict[str, object]) -> None:
        """Persist one JSON object through the store-write owner."""
        queue_store_write.write_json(self.storage_root, path, record)

    def bounded_regular_json_count(
        self,
        directory: Path,
        *,
        limit: int,
        label: str,
    ) -> tuple[int, bool]:
        """Count bounded regular JSON records without following unsafe entries."""
        return _bounded_regular_json_count(directory, limit=limit, label=label)


class ClioCoreQueue(
    queue_owner_session_lifecycle.QueueOwnerSessionLifecycleMixin,
    queue_artifacts.QueueArtifactsMixin,
    queue_artifact_lineage.QueueArtifactLineageMixin,
    queue_endpoints.QueueEndpointsMixin,
    queue_idempotency.QueueIdempotencyMixin,
    queue_events.QueueEventsMixin,
    queue_legacy_audit.QueueLegacyAuditMixin,
    queue_legacy_output_audit.QueueLegacyOutputAuditMixin,
    queue_legacy_output_migration.QueueLegacyOutputMigrationMixin,
):
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
        self._locked_storage_root_descriptor: int | None = None
        self._locked_storage_root_identity: tuple[int, int] | None = None
        self._store_adapter: queue_context.QueueStoreProtocol = _QueueStoreAdapter(self)
        self._jarvis_input_store = self._store_adapter
        self._layout = queue_layout.QueueLayout(self._store_adapter)
        self._jarvis_inputs = queue_jarvis_inputs.QueueJarvisInputs(self._store_adapter)

    def _storage_root_stat(self) -> os.stat_result:
        """Inspect the queue root through its held descriptor when migration-pinned."""
        return self._layout.storage_root_stat()

    def _read_sealed_index_migration_state(
        self,
        *,
        allow_legacy_lease_schema: bool = False,
    ) -> dict[str, object]:
        """Read and strictly validate indexed-era state without repairing or scanning."""
        return queue_index_state.read_sealed_index_migration_state(
            self._storage_root,
            allow_legacy_lease_schema=allow_legacy_lease_schema,
            checkpoint_validator=self._require_sealed_checkpoint,
            document_reader=_read_unique_json_document,
        )

    def _upgrade_sealed_lease_operational_schema_unlocked(self) -> None:
        """Invalidate exact v1 lease indexes so the bounded v2 migration can rebuild them."""
        if not self._lock.is_locked:
            raise RuntimeError("sealed lease index upgrade requires the queue lock")
        state = self._read_sealed_index_migration_state(allow_legacy_lease_schema=True)
        operational = cast(dict[str, object], state["operational_families"])
        lease_checkpoint = cast(dict[str, object], operational["leases"])
        repair = cast(dict[str, object], state["lease_operational_repair"])
        if lease_checkpoint.get("schema_version") == LEASE_OPERATIONAL_INDEX_SCHEMA:
            return
        lease_checkpoint.clear()
        lease_checkpoint.update(
            {
                "cursor": None,
                "complete": False,
                "schema_version": LEASE_OPERATIONAL_INDEX_SCHEMA,
            }
        )
        repair.clear()
        repair.update(
            {
                "complete": False,
                "schema_version": LEASE_OPERATIONAL_INDEX_SCHEMA,
            }
        )
        state["complete"] = False
        self._write_index_migration_state(state)

    def initialize(
        self,
        *,
        migrate_legacy_output: bool = False,
        locked_core: LockedCoreIdentity | None = None,
        allow_exclusive_seal: bool = True,
    ) -> None:
        """Create the record families used by the queue."""
        if locked_core is not None:
            if self._migration_lifetime_guarded:
                raise ConfigurationError(
                    "locked-core authority is only valid for the outer initialization scope"
                )
            require_active_locked_core(locked_core)
            self._initialize_under_locked_core(
                locked_core,
                migrate_legacy_output=migrate_legacy_output,
            )
            return
        if migrate_legacy_output and not self._migration_lifetime_guarded:
            self._initialize_with_exclusive_lifetime(migrate_legacy_output=True)
            return
        if self._initialized:
            with self._lock:
                indexed_audit = self._read_legacy_record_audit_marker()
            if indexed_audit is not None:
                return
            self._initialized = False
        if (
            not self._migration_lifetime_guarded
            and _path_lstat(self._legacy_record_audit_marker_path()) is None
        ):
            if not allow_exclusive_seal:
                raise QueueSealRequiresExclusive(
                    "missing legacy-record audit seal requires exclusive writer-lifetime ownership"
                )
            self._initialize_with_exclusive_lifetime(migrate_legacy_output=migrate_legacy_output)
            return
        # The root and lock path are the only pre-lock filesystem state. A
        # missing seal is audited exactly once after taking that lock and before
        # any record-family, migration, or archive write.
        self._prepare_queue_root_for_lock()
        try:
            with self._lock:
                locked_indexed_audit = self._read_legacy_record_audit_marker(
                    allow_legacy_lease_schema=True
                )
                if locked_indexed_audit is None:
                    if not self._migration_lifetime_guarded:
                        raise QueueSealRequiresExclusive(
                            "missing legacy-record audit seal requires exclusive "
                            "writer-lifetime ownership"
                        )
                    legacy_output_audit = self._audit_legacy_state_before_initialization()
                    self._require_legacy_output_migration_authorized(
                        legacy_output_audit,
                        migrate_legacy_output=migrate_legacy_output,
                    )
                    for family in _INITIALIZED_QUEUE_FAMILIES:
                        (self._storage_root / family).mkdir(
                            mode=0o700,
                            parents=True,
                            exist_ok=True,
                        )
                    for family in _GLOBAL_ORDER_FAMILIES:
                        family_root = self._storage_root / "global_order" / family
                        family_root.mkdir(
                            mode=0o700,
                            exist_ok=True,
                        )
                        (family_root / "by_id").mkdir(
                            mode=0o700,
                            exist_ok=True,
                        )
                        (family_root / "entries").mkdir(
                            mode=0o700,
                            exist_ok=True,
                        )
                else:
                    legacy_output_audit = locked_indexed_audit
                for family in _ADDITIVE_QUEUE_FAMILIES:
                    directory = self._storage_root / family
                    directory_stat = self._require_safe_write_directory(directory)
                    self._require_owner_private_queue_directory(
                        family,
                        directory,
                        directory_stat,
                    )
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
                                for family in (
                                    "jobs",
                                    "tasks",
                                    "leases",
                                    "artifacts",
                                    "progress",
                                )
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
                                "complete": not _lease_operational_records_present(
                                    self._storage_root
                                ),
                                "schema_version": LEASE_OPERATIONAL_INDEX_SCHEMA,
                            },
                            "lease_capacity_aggregate": lease_capacity_checkpoint,
                        },
                    )
                else:
                    # A torn aggregate/checkpoint pair is valid only while its exact
                    # transition intent remains durable. Replay that authorization
                    # before deciding the migration checkpoint itself is corrupt.
                    if locked_indexed_audit is None:
                        self._recover_pending_transitions_unlocked()
                        self._ensure_extended_migration_state()
                self._recover_pending_transitions_unlocked()
                if locked_indexed_audit is None:
                    self._write_legacy_record_audit_marker_unlocked()
                else:
                    self._upgrade_sealed_lease_operational_schema_unlocked()
                    self._reconcile_sealed_lease_capacity_gate_unlocked()
                    self._read_sealed_index_migration_state()
                self._initialized = True
        except QueueSealRequiresExclusive:
            if not allow_exclusive_seal:
                raise
            self._initialize_with_exclusive_lifetime(migrate_legacy_output=migrate_legacy_output)

    def _initialize_with_exclusive_lifetime(self, *, migrate_legacy_output: bool) -> None:
        """Initialize under a pinned lifetime and preserve the public legacy-state contract."""
        try:
            with exclusive_migration_lifetime(self.root) as locked_core:
                self.initialize(
                    migrate_legacy_output=migrate_legacy_output,
                    locked_core=locked_core,
                )
        except _UnsafeQueueDirectoryProtection as exc:
            raise LegacyQueueStateError(
                family=exc.family,
                path=exc.path,
                reason="canonical family is not an owned directory",
            ) from exc

    def _initialize_under_locked_core(
        self,
        locked_core: LockedCoreIdentity,
        *,
        migrate_legacy_output: bool,
    ) -> None:
        """Pin initialization I/O to one authenticated exclusively locked core."""
        require_active_locked_core(locked_core)
        original_root = self.root
        original_storage_root = self._storage_root
        original_lock = self._lock
        original_storage_root_descriptor = self._locked_storage_root_descriptor
        original_storage_root_identity = self._locked_storage_root_identity
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
            locked_core.filesystem_root,
            force_extended=True,
        )
        self._locked_storage_root_descriptor = locked_core.filesystem_root_descriptor
        self._locked_storage_root_identity = expected_identity
        self._lock = _FairBoundedFileLock(
            str(self._storage_root / ".lock"),
            timeout=self._lock_timeout_seconds,
        )
        self._migration_lifetime_guarded = True
        try:
            self._repair_locked_queue_directory_permissions()
            self.initialize(migrate_legacy_output=migrate_legacy_output)
        finally:
            self._migration_lifetime_guarded = False
            self.root = original_root
            self._storage_root = original_storage_root
            self._lock = original_lock
            self._locked_storage_root_descriptor = original_storage_root_descriptor
            self._locked_storage_root_identity = original_storage_root_identity
            try:
                queue_root_after = os.stat(original_storage_root)
            except OSError as exc:
                raise ConfigurationError(f"migration queue root identity changed: {exc}") from exc
            if (queue_root_after.st_dev, queue_root_after.st_ino) != expected_identity:
                raise ConfigurationError("migration queue root identity changed while locked")

    def _repair_locked_queue_directory_permissions(self) -> None:
        """Privatize only fixed, owned queue directories under exclusive ownership."""
        if os.name == "nt":
            return
        root_descriptor = self._locked_storage_root_descriptor
        if root_descriptor is None:
            raise ConfigurationError("locked queue permission repair has no pinned root descriptor")
        relative_paths = [Path()]
        relative_paths.extend(Path(family) for family in _INITIALIZED_QUEUE_FAMILIES)
        relative_paths.extend(Path(family) for family in _ADDITIVE_QUEUE_FAMILIES)
        relative_paths.extend(Path(family) for family in _LEGACY_ONLY_QUEUE_FAMILIES)
        relative_paths.extend(Path("global_order") / family for family in _GLOBAL_ORDER_FAMILIES)
        relative_paths.extend(
            Path("global_order") / family / child
            for family in _GLOBAL_ORDER_FAMILIES
            for child in ("by_id", "entries")
        )
        getuid = getattr(os, "getuid", None)
        current_uid = getuid() if callable(getuid) else None
        flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
        flags |= getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        for relative_path in relative_paths:
            descriptor = os.dup(root_descriptor)
            try:
                os.set_inheritable(descriptor, False)
                missing = False
                for component in relative_path.parts:
                    try:
                        child_descriptor = os.open(component, flags, dir_fd=descriptor)
                    except FileNotFoundError:
                        missing = True
                        break
                    try:
                        os.set_inheritable(child_descriptor, False)
                    except BaseException:
                        with suppress(OSError):
                            os.close(child_descriptor)
                        raise
                    os.close(descriptor)
                    descriptor = child_descriptor
                if missing:
                    continue
                details = os.fstat(descriptor)
                if (
                    not stat.S_ISDIR(details.st_mode)
                    or _record_is_reparse(details)
                    or (current_uid is not None and details.st_uid != current_uid)
                ):
                    raise ConfigurationError(
                        "queue directory cannot be safely privatized: "
                        f"{self._storage_root / relative_path}"
                    )
                if stat.S_IMODE(details.st_mode) != 0o700:
                    os.fchmod(descriptor, 0o700)
            except OSError as exc:
                if exc.errno in {errno.ELOOP, errno.ENOTDIR} and relative_path.parts:
                    raise _UnsafeQueueDirectoryProtection(
                        family=relative_path.parts[0],
                        path=self.root / relative_path,
                        cause=exc,
                    ) from exc
                raise ConfigurationError(
                    "queue directory protections cannot be repaired through the pinned root: "
                    f"{self._storage_root / relative_path}: {exc}"
                ) from exc
            finally:
                with suppress(OSError):
                    os.close(descriptor)

    def _reconcile_sealed_lease_capacity_gate_unlocked(self) -> None:
        """Downgrade a sealed migration gate when its fixed capacity pair is corrupt."""
        state = self._read_index_migration_state()
        raw_checkpoint = state.get("lease_capacity_aggregate")
        if not isinstance(raw_checkpoint, dict):
            return
        checkpoint = cast(dict[str, object], raw_checkpoint)
        if checkpoint.get("complete") is not True:
            return
        try:
            current = self._read_lease_capacity_aggregate_unlocked()
        except (OSError, QueueConflictError):
            current = None
        migrated_generation = checkpoint.get("generation")
        valid = (
            current is not None
            and current.aggregate.epoch_id == checkpoint.get("epoch_id")
            and isinstance(migrated_generation, int)
            and not isinstance(migrated_generation, bool)
            and current.aggregate.generation >= migrated_generation
        )
        if valid:
            return
        checkpoint.clear()
        checkpoint.update(
            {
                "complete": False,
                "schema_version": LEASE_CAPACITY_AGGREGATE_SCHEMA,
            }
        )
        state["complete"] = False
        self._write_index_migration_state(state)

    def reconcile_pending_transitions(self) -> None:
        """Replay bounded write-ahead transitions left by another process."""
        self.initialize()
        with self._lock:
            self._recover_pending_transitions_unlocked()

    def index_migration_status(self) -> dict[str, object]:
        """Return the crash-safe v0.9 queue-index migration checkpoint."""
        self.initialize()
        return self._read_index_migration_state()

    def readiness_info(self) -> dict[str, object]:
        """Verify the fixed indexed layout and durable audit seal without writes."""
        audit = self._read_legacy_record_audit_marker()
        trust_contract = {
            "seal_trust_model": "owner_private_cooperative_same_uid_writers",
            "cryptographic_replay_protection": False,
            "record_integrity_verification": "on_access",
        }
        if audit is None:
            return {
                "schema_version": "clio-relay.queue-readiness.v1",
                "complete": False,
                "sealed": False,
                "repair_required": True,
                "inspection_mode": "fixed_layout_and_seal",
                "record_history_scanned": False,
                "records_examined": 0,
                **trust_contract,
                "bounds": {
                    "fixed_queue_family_count": len(_INITIALIZED_QUEUE_FAMILIES),
                    "fixed_global_order_family_count": len(_GLOBAL_ORDER_FAMILIES),
                },
            }
        return {
            "schema_version": "clio-relay.queue-readiness.v1",
            "complete": True,
            "sealed": True,
            "repair_required": False,
            "inspection_mode": "fixed_layout_and_seal",
            "record_history_scanned": False,
            "records_examined": 0,
            **trust_contract,
            "bounds": {
                "fixed_queue_family_count": len(_INITIALIZED_QUEUE_FAMILIES),
                "fixed_global_order_family_count": len(_GLOBAL_ORDER_FAMILIES),
            },
        }

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
                    record = self._read_canonical_record(path, model)
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
                    record = self._read_canonical_record(path, model)
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
                    record = self._read_canonical_record(path, model)
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
                    record = self._read_canonical_record(path, model)
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
                    record = self._read_canonical_record(path, model)
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
                    job = self._read_canonical_record(path, RelayJob)
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
                raise queue_conflict_from_cause(
                    f"cannot inspect lease operational index {directory}",
                    cause=exc,
                    logger=logger,
                ) from exc

        for root in roots:
            inspect(root, depth=0)
        for path in files:
            _unlink_durable_path(path)
        for path in sorted(directories, key=lambda item: len(item.parts), reverse=True):
            path.rmdir()

    def submit_job(self, job: RelayJob) -> RelayJob:
        """Submit a job, returning the existing record for a repeated idempotency key."""
        self._require_durable_record_id(job.job_id, field="job_id")
        queue_owner_session_records._validate_new_owner_session_metadata(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            job.metadata
        )
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
                self._assert_input_ingest_quota_unlocked(job)
                self._ensure_active_job_capacity_unlocked(job)
                job = job.model_copy(update={"job_id": existing_job_id})
            if raw_existing is None:
                self._assert_owner_session_intake_open_unlocked(job.metadata)
                self._assert_input_ingest_quota_unlocked(job)
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
                queue_owner_session_records._validate_new_owner_session_metadata(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
                    updated_metadata
                )
            updated = job.model_copy(update={"updated_at": utc_now(), "metadata": updated_metadata})
            self._write_job_unlocked(updated)
            return updated

    def get_jarvis_package_input_contract(
        self,
        route: JarvisPackageInputRoute,
    ) -> JarvisPackageInputContractRecord | None:
        """Load one exact checksum-bound package input contract record."""
        return self._jarvis_inputs.get_jarvis_package_input_contract(route)

    def put_jarvis_package_input_contract(
        self,
        record: JarvisPackageInputContractRecord,
    ) -> JarvisPackageInputContractRecord:
        """Persist immutable package semantics for one exact registered route."""
        return self._jarvis_inputs.put_jarvis_package_input_contract(record)

    def get_jarvis_pipeline_input_lineage(
        self,
        route: JarvisPipelineInputRoute,
    ) -> JarvisPipelineInputLineage | None:
        """Load one exact checksum-bound pipeline input lineage record."""
        return self._jarvis_inputs.get_jarvis_pipeline_input_lineage(route)

    def get_jarvis_pipeline_input_bindings(
        self,
        route: JarvisPipelineInputRoute,
    ) -> JarvisPipelineInputBindings | None:
        """Load current local-file bindings for one exact registered pipeline route."""
        return self._jarvis_inputs.get_jarvis_pipeline_input_bindings(route)

    def update_jarvis_pipeline_input_bindings(
        self,
        route: JarvisPipelineInputRoute,
        *,
        upserts: tuple[JarvisPipelineInputBinding, ...] = (),
        remove: tuple[tuple[str, str], ...] = (),
    ) -> JarvisPipelineInputBindings:
        """Atomically update exact step/setting bindings for one pipeline route."""
        return self._jarvis_inputs.update_jarvis_pipeline_input_bindings(
            route,
            upserts=upserts,
            remove=remove,
        )

    def get_jarvis_run_input_manifest(
        self,
        route: JarvisPipelineInputRoute,
        *,
        idempotency_key: str,
    ) -> JarvisRunInputManifest | None:
        """Load an immutable input manifest for one exact jarvis_run admission."""
        return self._jarvis_inputs.get_jarvis_run_input_manifest(
            route,
            idempotency_key=idempotency_key,
        )

    def put_jarvis_run_input_manifest(
        self,
        record: JarvisRunInputManifest,
    ) -> JarvisRunInputManifest:
        """Persist the first exact input manifest admitted for one run key."""
        return self._jarvis_inputs.put_jarvis_run_input_manifest(record)

    def merge_jarvis_pipeline_input_lineage(
        self,
        route: JarvisPipelineInputRoute,
        artifact_uses: tuple[ArtifactUse, ...],
        *,
        manifest_sha256: str,
    ) -> JarvisPipelineInputLineage:
        """Atomically merge staged inputs for one exact registered pipeline route."""
        return self._jarvis_inputs.merge_jarvis_pipeline_input_lineage(
            route,
            artifact_uses,
            manifest_sha256=manifest_sha256,
        )

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
            raise queue_conflict_from_cause(
                "cannot inspect lease capacity directory",
                cause=exc,
                logger=logger,
            ) from exc
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
            root_stat = self._storage_root_stat()
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
            raise queue_conflict_from_cause(
                f"cannot scan {label}",
                cause=exc,
                logger=logger,
            ) from exc
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
            raise queue_conflict_from_cause(
                "cannot scan lease expiry index",
                cause=exc,
                logger=logger,
            ) from exc
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
            raise queue_conflict_from_cause(
                "cannot scan lease identity reference index",
                cause=exc,
                logger=logger,
            ) from exc
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
            raise queue_conflict_from_cause(
                "cannot scan lease endpoint index",
                cause=exc,
                logger=logger,
            ) from exc
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
                if job.kind is JobKind.INPUT_INGEST:
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
            if job.kind is JobKind.INPUT_INGEST:
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
            if submitted.kind is JobKind.INPUT_INGEST:
                return submitted, None
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
        if job.kind is JobKind.INPUT_INGEST:
            raise QueueConflictError("input ingest jobs are never worker-leased")
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

    def put_mcp_task(self, task: RelayMcpTaskRecord) -> RelayMcpTaskRecord:
        """Durably create or replay one MCP projection of a relay job."""
        task = RelayMcpTaskRecord.model_validate(task.model_dump(mode="python"))
        self._require_durable_record_id(task.task_id, field="task_id")
        self._require_durable_record_id(task.job_id, field="job_id")
        self.initialize()
        with self._lock:
            self._recover_pending_transitions_unlocked()
            path = self._storage_root / "mcp_tasks" / f"{task.task_id}.json"
            existing = self._read_optional(path, RelayMcpTaskRecord)
            if existing is not None:
                requested = task.projection
                persisted = existing.projection
                route_fields = ("remote", "cluster", "route_revision")
                if (
                    existing.task_id != task.task_id
                    or existing.job_id != task.job_id
                    or persisted.tool_name != requested.tool_name
                    or persisted.profile != requested.profile
                    or _canonical_mcp_task_arguments(persisted.arguments)
                    != _canonical_mcp_task_arguments(requested.arguments)
                    or persisted.catalog_revision != requested.catalog_revision
                    or {field: persisted.initial_result.get(field) for field in route_fields}
                    != {field: requested.initial_result.get(field) for field in route_fields}
                ):
                    raise McpTaskIdentityConflictError(
                        f"MCP task identity was reused with different semantics: {task.task_id}"
                    )
                return existing
            self._write(path, task)
            return task

    def update_mcp_task_projection(
        self,
        task_id: str,
        projection: RelayMcpTaskProjection,
        *,
        expected_updated_at: datetime | None = None,
        state: JobState | None = None,
    ) -> RelayMcpTaskRecord:
        """Atomically replace one typed MCP projection with optional CAS protection."""
        projection = RelayMcpTaskProjection.model_validate(projection.model_dump(mode="python"))
        task_id = self._require_durable_record_id(task_id, field="task_id")
        self.initialize()
        with self._lock:
            self._recover_pending_transitions_unlocked()
            path = self._storage_root / "mcp_tasks" / f"{task_id}.json"
            task = self._read_optional(path, RelayMcpTaskRecord)
            if task is None:
                raise NotFoundError(f"MCP task not found: {task_id}")
            if task.task_id != task_id or task.job_id != task_id:
                raise QueueConflictError(f"canonical MCP task identity mismatch: {path}")
            if expected_updated_at is not None and task.updated_at != expected_updated_at:
                raise QueueConflictError(f"MCP task changed during update: {task_id}")
            updates: dict[str, object] = {
                "updated_at": utc_now(),
                "projection": projection,
            }
            if state is not None:
                updates["state"] = state
            updated = RelayMcpTaskRecord.model_validate(
                {
                    **task.model_dump(mode="python"),
                    **updates,
                }
            )
            self._write(path, updated)
            return updated

    def get_mcp_task(self, task_id: str) -> RelayMcpTaskRecord:
        """Return one durable MCP task projection by its relay job handle."""
        task_id = self._require_durable_record_id(task_id, field="task_id")
        task = self._read_optional(
            self._storage_root / "mcp_tasks" / f"{task_id}.json",
            RelayMcpTaskRecord,
        )
        if task is None:
            raise NotFoundError(f"MCP task not found: {task_id}")
        if task.task_id != task_id or task.job_id != task_id:
            raise QueueConflictError(f"canonical MCP task identity mismatch: {task_id}")
        return task

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
                    raise queue_conflict_from_cause(
                        f"could not repair empty execution cleanup index {pending_job_path}",
                        cause=exc,
                        logger=logger,
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

    def begin_input_ingest(
        self,
        job_id: str,
        *,
        attempt_id: str,
        policy: InputArtifactIngestPolicy | None = None,
    ) -> tuple[RelayJob, bool]:
        """Claim one synchronous ingest attempt, including an exact failed-job retry.

        Input ingestion is intentionally never worker-leased.  This explicit claim
        prevents concurrent HTTP requests from racing completion and gives crash
        recovery a durable timestamp and identity to terminalize.
        """
        job_id = self._require_durable_record_id(job_id, field="job_id")
        attempt_id = self._require_durable_record_id(attempt_id, field="attempt_id")
        self.initialize()
        with self._lock:
            self._recover_pending_transitions_unlocked()
            job = self.get_job(job_id)
            if job.kind is not JobKind.INPUT_INGEST or not isinstance(
                job.spec,
                InputArtifactSpec,
            ):
                raise QueueConflictError(f"job is not an input ingest: {job_id}")
            if job.state is JobState.SUCCEEDED:
                return job, False
            existing_attempt = _input_ingest_attempt(job)
            if job.state is JobState.RUNNING:
                if existing_attempt is not None and existing_attempt["attempt_id"] == attempt_id:
                    return job, False
                raise QueueConflictError(f"input ingest already has an active attempt: {job_id}")
            if job.state not in {JobState.QUEUED, JobState.FAILED}:
                raise QueueConflictError(
                    f"input ingest cannot begin from state {job.state.value}: {job_id}"
                )
            stored_policy_raw = job.metadata.get(INPUT_INGEST_POLICY_METADATA_KEY)
            try:
                stored_policy = InputArtifactIngestPolicy.model_validate(stored_policy_raw)
            except ValueError as exc:
                raise QueueConflictError("input ingest has no valid server quota policy") from exc
            effective_policy = policy or stored_policy
            self._assert_input_ingest_quota_unlocked(job, policy=effective_policy)
            now = utc_now()
            metadata = dict(job.metadata)
            original_policy_raw = metadata.get(INPUT_INGEST_ORIGINAL_POLICY_METADATA_KEY)
            if original_policy_raw is not None:
                try:
                    InputArtifactIngestPolicy.model_validate(original_policy_raw)
                except ValueError as exc:
                    raise QueueConflictError(
                        "input ingest original quota policy is invalid"
                    ) from exc
            policy_changed = effective_policy != stored_policy
            if policy_changed and INPUT_INGEST_ORIGINAL_POLICY_METADATA_KEY not in metadata:
                metadata[INPUT_INGEST_ORIGINAL_POLICY_METADATA_KEY] = stored_policy.model_dump(
                    mode="json"
                )
            metadata[INPUT_INGEST_POLICY_METADATA_KEY] = effective_policy.model_dump(mode="json")
            metadata[INPUT_INGEST_ATTEMPT_METADATA_KEY] = {
                "schema_version": INPUT_INGEST_ATTEMPT_SCHEMA,
                "attempt_id": attempt_id,
                "started_at": now.isoformat(),
                "outcome": "running",
            }
            started = job.model_copy(
                update={
                    "state": JobState.RUNNING,
                    "updated_at": now,
                    "last_error": None,
                    "leased_by": None,
                    "metadata": metadata,
                }
            )
            self._write_job_unlocked(started)
            self.append_event(
                job_id,
                "input_ingest.started",
                "Input artifact ingest attempt started",
                locked=True,
                payload={
                    "attempt_id": attempt_id,
                    "retry": job.state is JobState.FAILED,
                    "policy_changed": policy_changed,
                },
            )
            return started, True

    def fail_input_ingest(
        self,
        job_id: str,
        *,
        attempt_id: str,
        error: str,
    ) -> tuple[RelayJob, bool]:
        """Terminalize the exact failed ingest attempt without stranding capacity."""
        job_id = self._require_durable_record_id(job_id, field="job_id")
        attempt_id = self._require_durable_record_id(attempt_id, field="attempt_id")
        bounded_error = bounded_error_detail(error) or "input artifact ingest failed"
        self.initialize()
        with self._lock:
            self._recover_pending_transitions_unlocked()
            job = self.get_job(job_id)
            if job.kind is not JobKind.INPUT_INGEST or not isinstance(
                job.spec,
                InputArtifactSpec,
            ):
                raise QueueConflictError(f"job is not an input ingest: {job_id}")
            attempt = _input_ingest_attempt(job)
            if job.state is JobState.SUCCEEDED:
                return job, False
            if job.state is JobState.FAILED:
                if attempt is not None and attempt["attempt_id"] == attempt_id:
                    return job, False
                raise QueueConflictError(f"input ingest failed under another attempt: {job_id}")
            if (
                job.state is not JobState.RUNNING
                or attempt is None
                or attempt["attempt_id"] != attempt_id
                or attempt["outcome"] != "running"
            ):
                raise QueueConflictError(f"input ingest attempt identity changed: {job_id}")
            now = utc_now()
            metadata = dict(job.metadata)
            metadata[INPUT_INGEST_ATTEMPT_METADATA_KEY] = {
                **attempt,
                "completed_at": now.isoformat(),
                "outcome": "failed",
                "error": bounded_error,
            }
            failed = job.model_copy(
                update={
                    "state": JobState.FAILED,
                    "updated_at": now,
                    "last_error": bounded_error,
                    "leased_by": None,
                    "metadata": metadata,
                }
            )
            self._write_job_unlocked(failed)
            self.append_event(
                job_id,
                "job.failed",
                "Input artifact ingest failed",
                locked=True,
                payload={
                    "state": JobState.FAILED.value,
                    "attempt_id": attempt_id,
                    "error": bounded_error,
                },
            )
            return failed, True

    def recover_abandoned_input_ingests(
        self,
        *,
        cluster: str,
        stale_before: datetime | None = None,
        limit: int = MAX_INPUT_INGEST_RECOVERY_BATCH,
    ) -> list[RelayJob]:
        """Fail bounded orphaned synchronous ingests so quota and storage can recover."""
        if limit < 1 or limit > MAX_INPUT_INGEST_RECOVERY_BATCH:
            raise ValueError(
                "input ingest recovery limit must be between 1 and "
                f"{MAX_INPUT_INGEST_RECOVERY_BATCH}"
            )
        cutoff = stale_before or (
            utc_now() - timedelta(seconds=DEFAULT_INPUT_INGEST_ABANDONED_AFTER_SECONDS)
        )
        if cutoff.tzinfo is None or cutoff.utcoffset() is None:
            raise ValueError("input ingest recovery cutoff must include a timezone")
        self.initialize()
        recovered: list[RelayJob] = []
        with self._lock:
            self._recover_pending_transitions_unlocked()
            self._repair_active_job_index_unlocked()
            active, truncated = self._scan_many(
                self._storage_root / "jobs_active",
                RelayJob,
                limit=MAX_ACTIVE_JOB_RECORDS,
            )
            if truncated:
                raise QueueConflictError("active job index exceeded its safety bound")
            for indexed in sorted(active, key=self._job_submission_order_key_unlocked):
                if len(recovered) >= limit:
                    break
                job = self.get_job(indexed.job_id)
                if (
                    job.cluster != cluster
                    or job.kind is not JobKind.INPUT_INGEST
                    or job.state not in {JobState.QUEUED, JobState.RUNNING}
                    or job.updated_at > cutoff
                ):
                    continue
                now = utc_now()
                existing_attempt = _input_ingest_attempt(job)
                attempt_id = (
                    existing_attempt["attempt_id"]
                    if existing_attempt is not None
                    else f"ingest_recovery_{uuid4().hex}"
                )
                metadata = dict(job.metadata)
                metadata[INPUT_INGEST_ATTEMPT_METADATA_KEY] = {
                    "schema_version": INPUT_INGEST_ATTEMPT_SCHEMA,
                    "attempt_id": attempt_id,
                    "started_at": (
                        existing_attempt["started_at"]
                        if existing_attempt is not None
                        else job.updated_at.isoformat()
                    ),
                    "completed_at": now.isoformat(),
                    "outcome": "abandoned",
                    "error": "input ingest attempt ended without terminal reconciliation",
                }
                failed = job.model_copy(
                    update={
                        "state": JobState.FAILED,
                        "updated_at": now,
                        "last_error": (
                            "input ingest attempt ended without terminal reconciliation"
                        ),
                        "leased_by": None,
                        "metadata": metadata,
                    }
                )
                self._write_job_unlocked(failed)
                self.append_event(
                    job.job_id,
                    "job.failed",
                    "Abandoned input artifact ingest recovered",
                    locked=True,
                    payload={
                        "state": JobState.FAILED.value,
                        "attempt_id": attempt_id,
                        "previous_state": job.state.value,
                        "error": failed.last_error,
                    },
                )
                recovered.append(failed)
        return recovered

    def reconcile_input_artifact(
        self,
        artifact: ArtifactRef,
        *,
        attempt_id: str | None = None,
    ) -> ArtifactRef:
        """Idempotently index the single verified artifact of an ingest job."""
        self._require_durable_record_id(artifact.artifact_id, field="artifact_id")
        self._require_durable_record_id(artifact.job_id, field="job_id")
        self.initialize()
        with self._lock:
            self._recover_pending_transitions_unlocked()
            job = self.get_job(artifact.job_id)
            if job.kind is not JobKind.INPUT_INGEST or not isinstance(
                job.spec,
                InputArtifactSpec,
            ):
                raise QueueConflictError(
                    f"input artifact producer is not an ingest job: {artifact.job_id}"
                )
            if job.state not in {JobState.QUEUED, JobState.RUNNING, JobState.SUCCEEDED}:
                raise QueueConflictError(
                    f"input ingest job is not reconcilable from state {job.state.value}: "
                    f"{job.job_id}"
                )
            if attempt_id is not None:
                attempt_id = self._require_durable_record_id(attempt_id, field="attempt_id")
                attempt = _input_ingest_attempt(job)
                if (
                    job.state is not JobState.RUNNING
                    or attempt is None
                    or attempt["attempt_id"] != attempt_id
                    or attempt["outcome"] != "running"
                ):
                    raise QueueConflictError(
                        f"input ingest attempt identity changed before reconciliation: {job.job_id}"
                    )
            if (
                job.metadata.get("owner") != "clio-relay"
                or not isinstance(job.metadata.get("owner_session_id"), str)
                or not isinstance(job.metadata.get("owner_session_generation_id"), str)
            ):
                raise QueueConflictError(
                    f"input ingest job has no exact owner-session generation: {job.job_id}"
                )
            expected_artifact_id = deterministic_input_artifact_id(job.job_id)
            if artifact.artifact_id != expected_artifact_id:
                raise QueueConflictError(f"input artifact identity changed for job: {job.job_id}")
            if (
                artifact.kind != "input"
                or artifact.size_bytes != job.spec.size_bytes
                or artifact.sha256 != job.spec.sha256
                or artifact.metadata.get("schema_version") != "clio-relay.input-artifact.v1"
                or artifact.metadata.get("logical_name") != job.spec.logical_name
            ):
                raise QueueConflictError(
                    f"input artifact content identity changed for job: {job.job_id}"
                )

            canonical_path = self._storage_root / "artifacts" / f"{artifact.artifact_id}.json"
            existing = self._read_optional(canonical_path, ArtifactRef)
            if existing is None:
                sequence = self._next_job_record_sequence_unlocked(
                    artifact.job_id,
                    "artifact_count",
                )
                if sequence != 1:
                    raise QueueConflictError(
                        f"input ingest job already has another artifact: {job.job_id}"
                    )
                saved = artifact.model_copy(update={"sequence": sequence})
                self._write(canonical_path, saved)
            else:
                if existing.sequence != 1 or not _same_input_artifact(existing, artifact):
                    raise QueueConflictError(
                        f"canonical input artifact identity changed: {artifact.artifact_id}"
                    )
                saved = existing

            artifact_directory = (
                self._storage_root / "artifacts_by_job" / self._durable_key(job.job_id)
            )
            paths = self._bounded_json_record_paths(
                artifact_directory,
                limit=2,
                label=f"input artifacts for job {job.job_id}",
            )
            unexpected = [path for path in paths if path.stem != saved.artifact_id]
            if unexpected:
                raise QueueConflictError(
                    f"input ingest job has an unexpected artifact: {unexpected[0].stem}"
                )
            by_job_path = self._job_record_path(
                "artifacts_by_job",
                saved.job_id,
                saved.artifact_id,
            )
            existing_by_job = self._read_optional(by_job_path, ArtifactRef)
            if existing_by_job is not None and existing_by_job != saved:
                raise QueueConflictError(f"input artifact job index changed: {saved.artifact_id}")
            self._write(by_job_path, saved)
            order_path = (
                self._storage_root
                / "artifact_order_by_job"
                / self._durable_key(saved.job_id)
                / f"{saved.sequence:020d}.json"
            )
            existing_order = self._read_optional(order_path, ArtifactRef)
            if existing_order is not None and existing_order != saved:
                raise QueueConflictError(f"input artifact order index changed: {saved.artifact_id}")
            self._write(order_path, saved)
            index = self._read_job_index(saved.job_id)
            if index is None:
                raise QueueConflictError(f"input ingest job index is missing: {saved.job_id}")
            artifact_count = _index_integer(index, "artifact_count")
            if artifact_count not in {0, 1}:
                raise QueueConflictError(f"input ingest artifact count is invalid: {saved.job_id}")
            if artifact_count == 0:
                self._update_job_index_unlocked(saved.job_id, artifact_count=1)
            (self._storage_root / "artifact_users" / saved.artifact_id).mkdir(
                parents=True,
                exist_ok=True,
            )
            self._initialize_artifact_user_order_unlocked(saved.artifact_id)
            self._link_gateways_for_artifact_unlocked(saved)
            if not self._input_artifact_event_exists_unlocked(saved):
                self.append_event(
                    saved.job_id,
                    "artifact.created",
                    f"Input artifact indexed: {job.spec.logical_name}",
                    locked=True,
                    payload={
                        "artifact_id": saved.artifact_id,
                        "uri": saved.uri,
                        "kind": "input",
                        "logical_name": job.spec.logical_name,
                    },
                )
            return saved

    def _input_artifact_event_exists_unlocked(self, artifact: ArtifactRef) -> bool:
        index = self._read_job_index(artifact.job_id)
        if index is None:
            raise QueueConflictError(f"input ingest job index is missing: {artifact.job_id}")
        latest_event_seq = _index_integer(index, "latest_event_seq")
        if latest_event_seq > DEFAULT_EXACT_RECORD_LIMIT:
            raise QueueConflictError(
                f"input ingest event history exceeds its reconciliation bound: {artifact.job_id}"
            )
        for sequence in range(1, latest_event_seq + 1):
            event = self._read_optional(
                self._storage_root / "events" / artifact.job_id / f"{sequence:020d}.json",
                RelayEvent,
            )
            if event is None or event.job_id != artifact.job_id or event.seq != sequence:
                raise QueueConflictError(
                    f"input ingest event history is incomplete: {artifact.job_id}"
                )
            if (
                event.event_type == "artifact.created"
                and event.payload.get("artifact_id") == artifact.artifact_id
            ):
                return True
        return False

    def complete_input_ingest(
        self,
        job_id: str,
        *,
        attempt_id: str | None = None,
    ) -> tuple[RelayJob, bool]:
        """Idempotently terminalize an ingest job after its artifact is durable."""
        job_id = self._require_durable_record_id(job_id, field="job_id")
        self.initialize()
        with self._lock:
            self._recover_pending_transitions_unlocked()
            job = self.get_job(job_id)
            if job.kind is not JobKind.INPUT_INGEST or not isinstance(
                job.spec,
                InputArtifactSpec,
            ):
                raise QueueConflictError(f"job is not an input ingest: {job_id}")
            if job.state not in {JobState.QUEUED, JobState.RUNNING, JobState.SUCCEEDED}:
                raise QueueConflictError(
                    f"input ingest job has unexpected state: {job.state.value}"
                )
            attempt = _input_ingest_attempt(job)
            if attempt_id is not None:
                attempt_id = self._require_durable_record_id(attempt_id, field="attempt_id")
                if (
                    job.state is not JobState.RUNNING
                    or attempt is None
                    or attempt["attempt_id"] != attempt_id
                    or attempt["outcome"] != "running"
                ):
                    raise QueueConflictError(
                        f"input ingest attempt identity changed before completion: {job_id}"
                    )
            artifact_id = deterministic_input_artifact_id(job.job_id)
            artifact = self._read_optional(
                self._storage_root / "artifacts" / f"{artifact_id}.json",
                ArtifactRef,
            )
            if (
                artifact is None
                or artifact.job_id != job.job_id
                or artifact.kind != "input"
                or artifact.size_bytes != job.spec.size_bytes
                or artifact.sha256 != job.spec.sha256
            ):
                raise QueueConflictError(
                    f"input ingest cannot complete without its exact artifact: {job_id}"
                )
            changed = job.state in {JobState.QUEUED, JobState.RUNNING}
            if changed:
                metadata = dict(job.metadata)
                if attempt is not None:
                    metadata[INPUT_INGEST_ATTEMPT_METADATA_KEY] = {
                        **attempt,
                        "completed_at": utc_now().isoformat(),
                        "outcome": "succeeded",
                    }
                job = job.model_copy(
                    update={
                        "state": JobState.SUCCEEDED,
                        "updated_at": utc_now(),
                        "last_error": None,
                        "leased_by": None,
                        "metadata": metadata,
                    }
                )
                self._write_job_unlocked(job)
            if not self._input_ingest_succeeded_event_exists_unlocked(job.job_id):
                self.append_event(
                    job.job_id,
                    "job.succeeded",
                    "Input artifact ingested",
                    locked=True,
                    payload={"state": JobState.SUCCEEDED.value, "error": None},
                )
            return job, changed

    def _input_ingest_succeeded_event_exists_unlocked(self, job_id: str) -> bool:
        index = self._read_job_index(job_id)
        if index is None:
            raise QueueConflictError(f"input ingest job index is missing: {job_id}")
        latest_event_seq = _index_integer(index, "latest_event_seq")
        if latest_event_seq > DEFAULT_EXACT_RECORD_LIMIT:
            raise QueueConflictError(
                f"input ingest event history exceeds its reconciliation bound: {job_id}"
            )
        for sequence in range(1, latest_event_seq + 1):
            event = self._read_optional(
                self._storage_root / "events" / job_id / f"{sequence:020d}.json",
                RelayEvent,
            )
            if event is None or event.job_id != job_id or event.seq != sequence:
                raise QueueConflictError(f"input ingest event history is incomplete: {job_id}")
            if event.event_type == "job.succeeded":
                return True
        return False

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
            queue_owner_session_records._validate_owner_session_identity_metadata(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
                session.metadata,
                allow_legacy=False,
            )
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
            raise queue_conflict_from_cause(
                "cannot inspect active job capacity",
                cause=exc,
                logger=logger,
            ) from exc

    def _assert_input_ingest_quota_unlocked(
        self,
        job: RelayJob,
        *,
        policy: InputArtifactIngestPolicy | None = None,
    ) -> None:
        """Enforce bounded input totals for one exact owner-session generation."""
        if job.kind is not JobKind.INPUT_INGEST:
            return
        if not isinstance(job.spec, InputArtifactSpec):
            raise QueueConflictError("input ingest job has an invalid specification")
        identity = queue_owner_session_records._owner_session_identity(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            job.metadata,
            allow_legacy=False,
        )
        if identity is None or job.metadata.get("owner") != "clio-relay":
            raise QueueConflictError("input ingest requires exact relay owner-session identity")
        owner_session_id, session_generation_id = identity
        if session_generation_id is None:
            raise QueueConflictError("input ingest requires an owner-session generation")
        if policy is None:
            raw_policy = job.metadata.get(INPUT_INGEST_POLICY_METADATA_KEY)
            try:
                policy = InputArtifactIngestPolicy.model_validate(raw_policy)
            except ValueError as exc:
                raise QueueConflictError("input ingest has no valid server quota policy") from exc

        directory = self._owner_session_membership_dir(
            owner_session_id,
            session_generation_id=session_generation_id,
        )
        paths = self._bounded_json_record_paths(
            directory,
            limit=MAX_ACTIVE_JOB_RECORDS,
            label="owner-session input ingest quota membership",
        )
        file_count = 0
        total_bytes = 0
        for path in paths:
            membership = self._read_json_file(path, OwnerSessionJobMembership)
            if (
                membership.owner_session_id != owner_session_id
                or membership.session_generation_id != session_generation_id
            ):
                raise QueueConflictError(f"input ingest quota membership identity mismatch: {path}")
            existing = self._read_optional(
                self._storage_root / "jobs" / f"{membership.job_id}.json",
                RelayJob,
            )
            if existing is None:
                raise QueueConflictError(
                    f"input ingest quota membership has no producer: {membership.job_id}"
                )
            if existing.metadata.get("owner_session_id") != owner_session_id or (
                existing.metadata.get("owner_session_generation_id") != session_generation_id
            ):
                raise QueueConflictError(
                    f"input ingest quota producer identity changed: {membership.job_id}"
                )
            if existing.kind is not JobKind.INPUT_INGEST:
                continue
            if not isinstance(existing.spec, InputArtifactSpec):
                raise QueueConflictError(
                    f"input ingest quota producer has an invalid spec: {membership.job_id}"
                )
            if existing.job_id == job.job_id:
                # A failed/queued retry already appears in generation membership.
                # Exclude it here and add the candidate exactly once below.
                continue
            if not self._input_ingest_consumes_quota_unlocked(existing):
                continue
            file_count += 1
            total_bytes += existing.spec.size_bytes
            if file_count >= policy.max_file_count:
                raise QueueConflictError(
                    "input_ingest_file_count_limit_reached: owner-session generation "
                    f"limit is {policy.max_file_count}"
                )
            if total_bytes + job.spec.size_bytes > policy.max_total_bytes:
                raise QueueConflictError(
                    "input_ingest_total_bytes_limit_reached: owner-session generation "
                    f"limit is {policy.max_total_bytes} bytes"
                )
        if file_count + 1 > policy.max_file_count:
            raise QueueConflictError(
                "input_ingest_file_count_limit_reached: owner-session generation "
                f"limit is {policy.max_file_count}"
            )
        if total_bytes + job.spec.size_bytes > policy.max_total_bytes:
            raise QueueConflictError(
                "input_ingest_total_bytes_limit_reached: owner-session generation "
                f"limit is {policy.max_total_bytes} bytes"
            )

    def _input_ingest_consumes_quota_unlocked(self, job: RelayJob) -> bool:
        """Return whether an admitted ingest owns bytes or can still create them."""
        if job.kind is not JobKind.INPUT_INGEST or not isinstance(
            job.spec,
            InputArtifactSpec,
        ):
            raise QueueConflictError(f"input ingest quota producer is invalid: {job.job_id}")
        if job.state not in {JobState.FAILED, JobState.CANCELED}:
            return True
        artifact_id = deterministic_input_artifact_id(job.job_id)
        artifact = self._read_optional(
            self._storage_root / "artifacts" / f"{artifact_id}.json",
            ArtifactRef,
        )
        if artifact is None:
            return False
        if (
            artifact.artifact_id != artifact_id
            or artifact.job_id != job.job_id
            or artifact.kind != "input"
            or artifact.size_bytes != job.spec.size_bytes
            or artifact.sha256 != job.spec.sha256
            or artifact.metadata.get("schema_version") != job.spec.schema_version
            or artifact.metadata.get("logical_name") != job.spec.logical_name
        ):
            raise QueueConflictError(
                f"terminal input ingest artifact identity changed: {job.job_id}"
            )
        return True

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
        return self._reconcile_transition_intents_unlocked()

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
                ("transforms", f"{job_id}.json"),
                ("mcp_tasks", f"{job_id}.json"),
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
        pending_transition = bool(
            self._bounded_json_record_paths(
                self._storage_root / "transition_intents",
                limit=MAX_TRANSITION_INTENT_RECORDS,
                label="queue transition intent directory",
            )
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
        return queue_index_state.read_index_migration_state(self._storage_root)

    def _write_index_migration_state(self, state: dict[str, object]) -> None:
        queue_index_state.write_index_migration_state(self._storage_root, state)

    def _require_index_migration_complete(self) -> None:
        queue_index_state.require_index_migration_complete(self._storage_root)

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
            queue_owner_session_records._validate_owner_session_identity_metadata(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
                record.metadata,
                allow_legacy=True,
            )
            return
        if family == "leases" and isinstance(record, Lease):
            job = self.get_job(record.job_id)
            self._sync_lease_operational_indexes_unlocked(record, job=job)
            return
        raise QueueConflictError(f"operational-index migration record mismatch: {family}")

    def _migrate_order_record_unlocked(self, family: str, record: BaseModel) -> None:
        if family == "tasks" and isinstance(record, RelayTask):
            sequence = record.sequence or (
                queue_order_index.last_contiguous_sequence(
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
                queue_order_index.last_contiguous_sequence(
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
                queue_order_index.last_contiguous_sequence(
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
        return self._layout.job_record_path(family, job_id, record_id)

    @staticmethod
    def _durable_key(value: str) -> str:
        return queue_layout.QueueLayout.durable_key(value)

    @staticmethod
    def _require_durable_record_id(value: str, *, field: str) -> str:
        return queue_layout.QueueLayout.require_durable_record_id(value, field=field)

    @staticmethod
    def _label_key(value: str, *, domain: str) -> str:
        return queue_layout.QueueLayout.label_key(value, domain=domain)

    def _write(self, path: Path, record: BaseModel) -> None:
        queue_store_write.write_model(self._storage_root, path, record)

    def _write_json(self, path: Path, record: dict[str, object]) -> None:
        queue_store_write.write_json(self._storage_root, path, record)

    def _require_safe_write_directory(self, directory: Path) -> os.stat_result:
        return queue_store_write.require_safe_write_directory(self._storage_root, directory)

    def _purge_write_staging_unlocked(self) -> None:
        queue_store_write.purge_write_staging(self._storage_root)

    def _write_text(self, path: Path, text: str) -> None:
        queue_store_write.write_text(self._storage_root, path, text)

    def _write_bytes(self, path: Path, payload: bytes, *, max_bytes: int) -> None:
        queue_store_write.write_bytes(
            self._storage_root,
            path,
            payload,
            max_bytes=max_bytes,
        )

    @staticmethod
    def _fsync_write_directory(path: Path) -> None:
        queue_store_write.fsync_write_directory(path)

    def _read_canonical_record(self, path: Path, model: type[Record]) -> Record:
        return queue_store_read.read_canonical_record(self._storage_root, path, model)

    def _read_optional(self, path: Path, model: type[Record]) -> Record | None:
        record = queue_store_read.read_optional(self._storage_root, path, model)
        if isinstance(record, RelayEvent) and _is_canonical_event_path(
            self._storage_root,
            path,
            "events",
        ):
            self._validate_legacy_output_event_access(path, record)
        return record

    @classmethod
    def _read_many(
        cls,
        directory: Path,
        model: type[Record],
        *,
        identity_field: str | None = None,
    ) -> Iterable[Record]:
        del cls
        return queue_store_read.read_many(
            directory,
            model,
            identity_field=identity_field,
        )

    @classmethod
    def _scan_many(
        cls,
        directory: Path,
        model: type[Record],
        *,
        limit: int,
        identity_field: str | None = None,
    ) -> tuple[list[Record], bool]:
        del cls
        return queue_store_read.scan_many(
            directory,
            model,
            limit=limit,
            identity_field=identity_field,
        )

    @staticmethod
    def _bounded_json_record_paths(
        directory: Path,
        *,
        limit: int,
        label: str,
    ) -> list[Path]:
        return queue_store_read.bounded_json_record_paths(
            directory,
            limit=limit,
            label=label,
        )

    @staticmethod
    def _read_json_file(path: Path, model: type[Record]) -> Record:
        return queue_store_read.read_json_file(path, model)

    @staticmethod
    def _read_json_document(path: Path) -> object:
        return queue_store_read.read_json_document(path)


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
        raise queue_conflict_from_cause(
            f"invalid JSON record {path}",
            cause=exc,
            logger=logger,
        ) from exc


_record_identity_field = queue_layout.record_identity_field


def _is_canonical_event_path(storage_root: Path, path: Path, family: str) -> bool:
    return queue_layout.is_canonical_event_path(storage_root, path, family)


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


_safe_owner_legacy_job_id = queue_layout.safe_owner_legacy_job_id


def _safe_global_record_id(record_id: object) -> bool:
    return queue_layout.safe_global_record_id(record_id)


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
    return queue_scheduler_cancel_records.scheduler_cancellation_request(job)


def _cancellation_requested_at(request: dict[str, object]) -> datetime | None:
    return queue_scheduler_cancel_records.cancellation_requested_at(request)


def _scheduler_cancel_record_is_due(
    record: SchedulerCancelPending,
    now: datetime,
) -> bool:
    return queue_scheduler_cancel_records.scheduler_cancel_record_is_due(record, now)


def _scheduler_cancel_due_sort_key(record: SchedulerCancelPending) -> tuple[datetime, str]:
    return queue_scheduler_cancel_records.scheduler_cancel_due_sort_key(record)


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
        raise queue_conflict_from_cause(
            f"cannot inspect {label}",
            cause=exc,
            logger=logger,
        ) from exc
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
    artifact_ids: set[str] = set()
    candidates = list(session.artifacts)
    for provenance in _gateway_source_provenance(session):
        value = provenance.get("source_relay_artifact_id")
        if isinstance(value, str) and value:
            candidates.append(value)
    for candidate in candidates:
        try:
            artifact_ids.add(validate_durable_record_id(candidate))
        except ValueError:
            # Gateway artifacts may be external URIs. Only relay artifact IDs
            # participate in canonical artifact and retention indexes.
            continue
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
    return queue_lease_records.lease_scope_ref_name(identity, *scope)


def _lease_index_document(identity: _LeaseIndexIdentity) -> dict[str, object]:
    return queue_lease_records.lease_index_document(identity)


def _lease_capacity_aggregate_document(
    aggregate: _LeaseCapacityAggregate,
) -> dict[str, object]:
    return queue_lease_records.lease_capacity_aggregate_document(aggregate)


def _serialized_lease_capacity_counts(
    cluster_kind_counts: dict[str, dict[JobKind, int]],
) -> dict[str, dict[str, int]]:
    return queue_lease_records.serialized_lease_capacity_counts(cluster_kind_counts)


def _lease_capacity_checkpoint_document(
    checkpoint: _LeaseCapacityCheckpoint,
) -> dict[str, object]:
    return queue_lease_records.lease_capacity_checkpoint_document(checkpoint)


def _new_lease_capacity_pair(
    counts: dict[str, dict[JobKind, int]],
    *,
    epoch_id: str | None = None,
    generation: int = 0,
    checkpoint_id: str | None = None,
) -> _LeaseCapacityPair:
    return queue_lease_records.new_lease_capacity_pair(
        counts,
        epoch_id=epoch_id,
        generation=generation,
        checkpoint_id=checkpoint_id,
    )


def _normalize_lease_capacity_counts(
    counts: dict[str, dict[JobKind, int]],
) -> dict[str, dict[JobKind, int]]:
    return queue_lease_records.normalize_lease_capacity_counts(counts)


def _lease_capacity_aggregate_from_document(
    value: object,
    *,
    label: str,
) -> _LeaseCapacityAggregate:
    return queue_lease_records.lease_capacity_aggregate_from_document(value, label=label)


def _lease_capacity_checkpoint_from_document(
    value: object,
    *,
    label: str,
) -> _LeaseCapacityCheckpoint:
    return queue_lease_records.lease_capacity_checkpoint_from_document(value, label=label)


def _validate_lease_capacity_pair(pair: _LeaseCapacityPair, *, label: str) -> None:
    queue_lease_records.validate_lease_capacity_pair(pair, label=label)


def _lease_capacity_pair_payload(pair: _LeaseCapacityPair) -> dict[str, object]:
    return queue_lease_records.lease_capacity_pair_payload(pair)


def _lease_capacity_pair_from_payload(value: object, *, label: str) -> _LeaseCapacityPair:
    return queue_lease_records.lease_capacity_pair_from_payload(value, label=label)


def _is_capacity_identity(value: object) -> bool:
    return queue_lease_records.is_capacity_identity(value)


def _lease_index_identity_from_document(
    value: object,
    *,
    label: str,
) -> _LeaseIndexIdentity:
    return queue_lease_records.lease_index_identity_from_document(value, label=label)


def _lease_reference_from_scope_ref(
    name: str,
    *scope: str,
) -> tuple[str, str] | None:
    return queue_lease_records.lease_reference_from_scope_ref(name, *scope)


def _lease_reference(identity: _LeaseIndexIdentity) -> tuple[str, str]:
    return queue_lease_records.lease_reference(identity)


def _parse_lease_identity_ref_name(name: str) -> tuple[str, str] | None:
    return queue_lease_records.parse_lease_identity_ref_name(name)


def _is_short_ref_token(value: str) -> bool:
    return queue_lease_records.is_short_ref_token(value)


def _lease_index_token(lease_id: str) -> str:
    return queue_lease_records.lease_index_token(lease_id)


def _lease_job_token(job_id: str) -> str:
    return queue_lease_records.lease_job_token(job_id)


def _lease_endpoint_token(endpoint_id: str) -> str:
    return queue_lease_records.lease_endpoint_token(endpoint_id)


def _lease_cluster_token(cluster: str) -> str:
    return queue_lease_records.lease_cluster_token(cluster)


def _lease_expiry_key(value: datetime) -> int:
    return queue_lease_records.lease_expiry_key(value)


def _lease_expiry_ref_name(identity: _LeaseIndexIdentity) -> str:
    return queue_lease_records.lease_expiry_ref_name(identity)


def _lease_identity_token(identity: _LeaseIndexIdentity) -> str:
    return queue_lease_records.lease_identity_token(identity)


def _parse_lease_expiry_ref_name(
    name: str,
) -> tuple[int, str, JobKind, str, str, str, str] | None:
    return queue_lease_records.parse_lease_expiry_ref_name(name)


def _path_lstat(path: Path) -> os.stat_result | None:
    return queue_store_read.path_lstat(path)


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
        raise queue_conflict_from_cause(
            f"GC could not quarantine {source}",
            cause=exc,
            logger=logger,
        ) from exc
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
            raise queue_conflict_from_cause(
                f"GC could not scan quarantined directory {candidate}",
                cause=exc,
                logger=logger,
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
        raise queue_conflict_from_cause(
            f"GC could not remove quarantined path {candidate}",
            cause=exc,
            logger=logger,
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
        raise queue_conflict_from_cause(
            f"GC could not remove quarantined path {candidate}",
            cause=exc,
            logger=logger,
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


_record_max_bytes = queue_layout.record_max_bytes


def _record_is_reparse(file_stat: os.stat_result) -> bool:
    return queue_layout.record_is_reparse(file_stat)


def _validate_record_stat(file_stat: os.stat_result, *, path: Path) -> None:
    queue_layout.validate_record_stat(file_stat, path=path)


_record_stats_match = queue_layout.record_stats_match


def _read_bounded_record_bytes(path: Path) -> bytes:
    return queue_store_read.read_bounded_record_bytes(path)


_transient_record_access_conflict = queue_store_read.transient_record_access_conflict


def _unlink_durable_path(path: Path, *, missing_ok: bool = False) -> None:
    queue_store_write.unlink_durable_path(path, missing_ok=missing_ok)


def _canonical_mcp_task_arguments(arguments: dict[str, Any]) -> dict[str, Any]:
    """Strip clio-relay's own transport-control keys before a task identity compare.

    ``VIRTUAL_REMOTE_MCP_RELAY_CONTROL_FIELDS`` (``idempotency_key``,
    ``wait_for_terminal``, ``wait_timeout_seconds``, ``poll_seconds``,
    ``include_logs``, ``log_limit``) are consumed by clio-relay's own MCP
    transport and never forwarded to the remote server -- they cannot change
    the executed work. ``put_mcp_task``'s replay-vs-conflict check must
    compare the same canonical identity the job queue's own idempotency
    digest already uses (``_job_idempotency_digest`` never sees these keys,
    since they are stripped before a ``RelayJob.spec.arguments`` is built), or
    two dispatches of identical work that differ only in a transport control
    incorrectly raise ``QueueConflictError`` (clio-relay#218).
    """
    return {
        key: value
        for key, value in arguments.items()
        if key not in VIRTUAL_REMOTE_MCP_RELAY_CONTROL_FIELDS
    }


def _is_sha256_digest(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def _same_input_artifact(existing: ArtifactRef, requested: ArtifactRef) -> bool:
    """Compare immutable input-artifact identity while preserving its first timestamp."""
    return existing.model_dump(exclude={"sequence", "created_at"}) == requested.model_dump(
        exclude={"sequence", "created_at"}
    )


def _input_ingest_attempt(job: RelayJob) -> dict[str, str] | None:
    """Validate and return one durable synchronous-ingest attempt record."""
    raw = job.metadata.get(INPUT_INGEST_ATTEMPT_METADATA_KEY)
    if raw is None:
        return None
    if not isinstance(raw, dict):
        raise QueueConflictError(f"input ingest attempt metadata is invalid: {job.job_id}")
    attempt = cast(dict[str, object], raw)
    schema = attempt.get("schema_version")
    attempt_id = attempt.get("attempt_id")
    started_at = attempt.get("started_at")
    outcome = attempt.get("outcome")
    if (
        schema != INPUT_INGEST_ATTEMPT_SCHEMA
        or not isinstance(attempt_id, str)
        or not isinstance(started_at, str)
        or outcome not in {"running", "succeeded", "failed", "abandoned"}
    ):
        raise QueueConflictError(f"input ingest attempt metadata is invalid: {job.job_id}")
    try:
        validate_durable_record_id(attempt_id)
        started = datetime.fromisoformat(started_at)
    except (TypeError, ValueError) as exc:
        raise QueueConflictError(f"input ingest attempt metadata is invalid: {job.job_id}") from exc
    if started.tzinfo is None or started.utcoffset() is None:
        raise QueueConflictError(f"input ingest attempt timestamp is naive: {job.job_id}")
    result: dict[str, str] = {
        "schema_version": INPUT_INGEST_ATTEMPT_SCHEMA,
        "attempt_id": attempt_id,
        "started_at": started_at,
        "outcome": cast(str, outcome),
    }
    for field in ("completed_at", "error"):
        value = attempt.get(field)
        if value is not None:
            if not isinstance(value, str):
                raise QueueConflictError(f"input ingest attempt metadata is invalid: {job.job_id}")
            result[field] = value
    return result


def _index_integer(index: dict[str, object], field: str) -> int:
    return queue_index_state.index_integer(index, field)


def _index_migration_components_complete(state: dict[str, object]) -> bool:
    """Return whether every independently replayable index checkpoint is complete."""
    return queue_index_state.index_migration_components_complete(state)


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

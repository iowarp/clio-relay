"""Durable queue/state boundary used as the relay's clio-core adapter.

The implementation in this repository is intentionally a filesystem-backed
record store so it can run everywhere during development. The public class is
named around the clio-core contract: callers depend on record families,
idempotency, leases, and cursor replay rather than a database choice.
"""

from __future__ import annotations

import errno
import json
import logging
import os
import stat
from collections.abc import Iterable
from contextlib import suppress
from datetime import datetime
from pathlib import Path
from typing import TypeVar, cast

from pydantic import BaseModel

from clio_relay import (
    queue_artifact_lineage,
    queue_artifacts,
    queue_browser_attachments,
    queue_context,
    queue_endpoints,
    queue_events,
    queue_execution_cleanup,
    queue_execution_cleanup_markers,
    queue_gateway_indexes,
    queue_gateways,
    queue_idempotency,
    queue_index_state,
    queue_input_ingest,
    queue_jarvis_inputs,
    queue_job_gc,
    queue_job_gc_protections,
    queue_jobs,
    queue_layout,
    queue_lease_admission,
    queue_lease_capacity_audit,
    queue_lease_capacity_state,
    queue_lease_indexes,
    queue_lease_records,
    queue_lease_recovery,
    queue_leases,
    queue_legacy_audit,
    queue_legacy_output_audit,
    queue_legacy_output_migration,
    queue_monitor_rules,
    queue_order_index,
    queue_owner_session_lifecycle,
    queue_owner_session_records,
    queue_progress,
    queue_scheduler_cancel_claims,
    queue_scheduler_cancel_records,
    queue_scheduler_cancel_state,
    queue_store_lock,
    queue_store_read,
    queue_store_write,
    queue_tasks,
)
from clio_relay.errors import (
    ConfigurationError,
    QueueConflictError,
    queue_conflict_from_cause,
)
from clio_relay.filesystem_paths import internal_filesystem_path, logical_filesystem_path
from clio_relay.models import (
    INPUT_INGEST_POLICY_METADATA_KEY,
    ArtifactRef,
    ArtifactUse,
    EndpointRegistration,
    GatewaySession,
    InputArtifactIngestPolicy,
    InputArtifactSpec,
    JarvisPackageInputContractRecord,
    JarvisPackageInputRoute,
    JarvisPipelineInputBinding,
    JarvisPipelineInputBindings,
    JarvisPipelineInputLineage,
    JarvisPipelineInputRoute,
    JarvisRunInputManifest,
    JobKind,
    Lease,
    MonitorRule,
    OwnerSessionJobMembership,
    ProgressRecord,
    RelayEvent,
    RelayJob,
    RelayTask,
    utc_now,
)
from clio_relay.worker_lifetime_lock import (
    LockedCoreIdentity,
    exclusive_migration_lifetime,
    require_active_locked_core,
)

logger = logging.getLogger(__name__)
Record = TypeVar("Record", bound=BaseModel)
_LeaseExpiryReference = queue_layout.LeaseExpiryReference
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


SchedulerCancelIdentityRegistration = (
    queue_scheduler_cancel_records.SchedulerCancelIdentityRegistration
)
SchedulerCancelAttemptClaim = queue_scheduler_cancel_records.SchedulerCancelAttemptClaim
SchedulerCancelConfirmationClaim = queue_scheduler_cancel_records.SchedulerCancelConfirmationClaim
_LeaseIndexIdentity = queue_lease_records.LeaseIndexIdentity
_LeaseCapacityAggregate = queue_lease_records.LeaseCapacityAggregate
_LeaseCapacityCheckpoint = queue_lease_records.LeaseCapacityCheckpoint
_LeaseCapacityPair = queue_lease_records.LeaseCapacityPair


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
        return self._queue._read_json_document(path)  # pyright: ignore[reportPrivateUsage]

    def write(self, path: Path, record: BaseModel) -> None:
        """Persist one typed record through the store-write owner."""
        self._queue._write(path, record)  # pyright: ignore[reportPrivateUsage]

    def write_json(self, path: Path, record: dict[str, object]) -> None:
        """Persist one JSON object through the store-write owner."""
        self._queue._write_json(path, record)  # pyright: ignore[reportPrivateUsage]

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
    queue_tasks.QueueTasksMixin,
    queue_execution_cleanup_markers.QueueExecutionCleanupMarkersMixin,
    queue_progress.QueueProgressMixin,
    queue_jobs.QueueJobsMixin,
    queue_execution_cleanup.QueueExecutionCleanupMixin,
    queue_input_ingest.QueueInputIngestMixin,
    queue_scheduler_cancel_state.QueueSchedulerCancelStateMixin,
    queue_owner_session_lifecycle.QueueOwnerSessionLifecycleMixin,
    queue_artifacts.QueueArtifactsMixin,
    queue_artifact_lineage.QueueArtifactLineageMixin,
    queue_endpoints.QueueEndpointsMixin,
    queue_idempotency.QueueIdempotencyMixin,
    queue_events.QueueEventsMixin,
    queue_legacy_audit.QueueLegacyAuditMixin,
    queue_legacy_output_audit.QueueLegacyOutputAuditMixin,
    queue_legacy_output_migration.QueueLegacyOutputMigrationMixin,
    queue_lease_indexes.QueueLeaseIndexesMixin,
    queue_lease_capacity_state.QueueLeaseCapacityStateMixin,
    queue_lease_capacity_audit.QueueLeaseCapacityAuditMixin,
    queue_lease_recovery.QueueLeaseRecoveryMixin,
    queue_lease_admission.QueueLeaseAdmissionMixin,
    queue_leases.QueueLeasesMixin,
    queue_scheduler_cancel_claims.QueueSchedulerCancelClaimsMixin,
    queue_gateway_indexes.QueueGatewayIndexesMixin,
    queue_gateways.QueueGatewaysMixin,
    queue_browser_attachments.QueueBrowserAttachmentsMixin,
    queue_monitor_rules.QueueMonitorRulesMixin,
    queue_job_gc_protections.QueueJobGcProtectionsMixin,
    queue_job_gc.QueueJobGcMixin,
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
                        and not queue_lease_indexes.lease_operational_records_present(
                            self._storage_root
                        )
                    )
                    lease_capacity_checkpoint: dict[str, object] = {
                        "complete": lease_capacity_complete,
                        "schema_version": LEASE_CAPACITY_AGGREGATE_SCHEMA,
                    }
                    if lease_capacity_complete:
                        empty_capacity = queue_lease_records.new_lease_capacity_pair(
                            {}, generation=0
                        )
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
                                and not queue_lease_indexes.lease_operational_records_present(
                                    self._storage_root
                                )
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
                                "complete": (
                                    not queue_lease_indexes.lease_operational_records_present(
                                        self._storage_root
                                    )
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
                paths, has_more = queue_store_read.migration_batch_paths(
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
                paths, has_more = queue_store_read.migration_batch_paths(
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
                paths, has_more = queue_store_read.migration_batch_paths(
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
                paths, has_more = queue_store_read.migration_batch_paths(
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
                paths, has_more = queue_store_read.migration_batch_paths(
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
                paths, has_more = queue_store_read.migration_batch_paths(
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

    def _assert_input_ingest_quota_unlocked(
        self,
        job: RelayJob,
        *,
        policy: InputArtifactIngestPolicy | None = None,
    ) -> None:
        """Enforce bounded input totals for one exact owner-session generation.

        CQ13-IO-01 typed deviation: this stays facade-resident rather than
        moving to ``queue_input_ingest`` with the rest of its slice. Its own
        caller, ``queue_jobs.submit_job`` (an earlier-landed, budget-pinned
        owner at 786/800), must invoke it inline inside the submission lock;
        extracting it would create a reverse-rank ``queue_jobs ->
        queue_input_ingest`` self-call edge the architecture guard rejects
        (design doc §3's DAG requires a caller's collaborators to have
        already landed). No earlier-ranked owner has the ~90-line headroom
        to host it either (see the ratchet table in
        ``tests/test_core_queue_split_architecture.py``). This mirrors the
        CQ4-IO-01 deviation already documented on
        ``queue_scheduler_cancel_state.py`` for the same class of problem.
        Its own quota-consumption predicate, ``_input_ingest_consumes_quota_
        unlocked``, has exactly one caller (this method) and carries no such
        constraint, so it moved to ``queue_input_ingest`` as designed; the
        call below resolves through the composed ``ClioCoreQueue`` MRO.
        """
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

    def _write_transition_intent_unlocked(
        self,
        kind: str,
        identity: str,
        payload: dict[str, object],
    ) -> Path:
        """Persist a bounded write-ahead intent before a canonical/index transition."""
        token = queue_gateway_indexes._stable_ref_token(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            kind, identity
        )
        path = self._storage_root / "transition_intents" / f"{kind}-{token}.json"
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
                    identity = queue_lease_records.lease_index_identity_from_document(
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
                "complete": not queue_lease_indexes.lease_operational_records_present(
                    self._storage_root
                ),
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
                queue_lease_records.is_capacity_identity(capacity_checkpoint.get("epoch_id"))
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


def _is_canonical_event_path(storage_root: Path, path: Path, family: str) -> bool:
    return queue_layout.is_canonical_event_path(storage_root, path, family)


def _scheduler_cancellation_request(job: RelayJob) -> dict[str, object] | None:
    return queue_scheduler_cancel_records.scheduler_cancellation_request(job)


def _cancellation_requested_at(request: dict[str, object]) -> datetime | None:
    return queue_scheduler_cancel_records.cancellation_requested_at(request)


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


def _path_lstat(path: Path) -> os.stat_result | None:
    return queue_store_read.path_lstat(path)


def _record_is_reparse(file_stat: os.stat_result) -> bool:
    return queue_layout.record_is_reparse(file_stat)


def _read_bounded_record_bytes(path: Path) -> bytes:
    return queue_store_read.read_bounded_record_bytes(path)


def _unlink_durable_path(path: Path, *, missing_ok: bool = False) -> None:
    queue_store_write.unlink_durable_path(path, missing_ok=missing_ok)

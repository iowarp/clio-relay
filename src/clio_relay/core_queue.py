"""Durable queue/state boundary used as the relay's clio-core adapter.

The implementation in this repository is intentionally a filesystem-backed
record store so it can run everywhere during development. The public class is
named around the clio-core contract: callers depend on record families,
idempotency, leases, and cursor replay rather than a database choice.
"""

from __future__ import annotations

import logging
import os
import stat
from pathlib import Path
from typing import TYPE_CHECKING, TypeVar, cast

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
    queue_index_discovery,
    queue_index_migration,
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
    queue_owner_session_lifecycle,
    queue_owner_session_records,
    queue_progress,
    queue_scheduler_cancel_claims,
    queue_scheduler_cancel_records,
    queue_scheduler_cancel_state,
    queue_startup,
    queue_store_lock,
    queue_store_read,
    queue_store_write,
    queue_tasks,
    queue_transitions,
)
from clio_relay.errors import (
    QueueConflictError,
    queue_conflict_from_cause,
)
from clio_relay.filesystem_paths import internal_filesystem_path, logical_filesystem_path
from clio_relay.models import (
    INPUT_INGEST_POLICY_METADATA_KEY,
    InputArtifactIngestPolicy,
    InputArtifactSpec,
    JobKind,
    OwnerSessionJobMembership,
    RelayEvent,
    RelayJob,
    utc_now,
)

if TYPE_CHECKING:
    from clio_relay.worker_lifetime_lock import LockedCoreIdentity

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


LegacyQueueStateError = queue_store_lock.LegacyQueueStateError
QueueSealRequiresExclusive = queue_store_lock.QueueSealRequiresExclusive
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
    queue_jarvis_inputs.QueueJarvisInputsMixin,
    queue_index_discovery.QueueIndexDiscoveryMixin,
    queue_startup.QueueStartupMixin,
    queue_index_migration.QueueIndexMigrationMixin,
    queue_transitions.QueueTransitionsMixin,
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

    _lock: queue_context.QueueLockProtocol

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

    def initialize(
        self,
        *,
        migrate_legacy_output: bool = False,
        locked_core: LockedCoreIdentity | None = None,
        allow_exclusive_seal: bool = True,
    ) -> None:
        """Create the record families used by the queue.

        CQ19-ST-02 typed deviation: stays facade-resident as a thin dispatch
        to ``queue_startup.initialize`` (a bare module-level function, not a
        ``QueueStartupMixin`` method) rather than moving as a real owned
        method. Every owner across the whole rank range self-calls
        ``self.initialize()`` as the first line of nearly every public
        method; making ``initialize`` a real ``*Mixin`` method turned every
        one of those calls into a rank-ordered architecture-guard edge with
        no rank able to satisfy both "before every caller" and "after its
        own collaborators." Keeping the dispatch point off the owner
        manifest keeps those calls invisible to the guard again, exactly as
        before this slice when ``initialize`` lived here directly. See
        ``queue_startup.py``'s module docstring for the full account.
        """
        queue_startup.initialize(
            self,
            migrate_legacy_output=migrate_legacy_output,
            locked_core=locked_core,
            allow_exclusive_seal=allow_exclusive_seal,
        )

    def reconcile_pending_transitions(self) -> None:
        """Replay bounded write-ahead transitions left by another process."""
        self.initialize()
        with self._lock:
            self._recover_pending_transitions_unlocked()

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
        paths = queue_store_read.bounded_json_record_paths(
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

    def _job_record_path(self, family: str, job_id: str, record_id: str) -> Path:
        return self._layout.job_record_path(family, job_id, record_id)

    # CQ20-FA-01 typed deviation: the methods below (plus ``_job_record_path``
    # above) are the private "store adapter" family -- thin, single-call
    # forwards to ``queue_store_read``/``queue_store_write`` module functions
    # that stay facade-resident rather than moving to any one CQ20 owner.
    # Two independent constraints force this, mirroring CQ19-TI-01's own
    # "hub method, no clean hoist target" class:
    #   * ``_write``/``_write_json``/``_read_optional``/``_read_json_document``
    #     are called by ``_QueueStoreAdapter`` as ``self._queue._write(...)``
    #     etc (ledger §9.5): routing through the *instance* rather than the
    #     bare module function is what keeps ``monkeypatch.setattr(queue,
    #     "_write", ...)`` (an established, widely used test seam) live. A
    #     module-qualified rewrite would silently break every such patch.
    #   * ``_job_record_path``/``_write``/``_write_json``/``_read_optional``/
    #     ``_scan_many``/``_recover_pending_transitions_unlocked`` (above) are
    #     each self-called from many already-landed owners spanning the full
    #     rank range, and ``_scan_many`` is additionally inherited directly by
    #     ``storage_runtime.StorageManagedQueue(ClioCoreQueue)`` -- a real
    #     production subclass outside the queue-owner family entirely, not
    #     just another mixin caller. None of these calls are owned by any
    #     ``queue_*.py`` mixin, so they carry no architecture-guard edge
    #     regardless of rank (same invisibility CQ19-ST-02 established for
    #     ``initialize``).
    # Every other CQ19-era single-caller wrapper (``_storage_root_stat``,
    # ``_durable_key``, ``_require_durable_record_id``, ``_label_key``,
    # ``_require_safe_write_directory``, ``_purge_write_staging_unlocked``,
    # ``_write_text``, ``_read_canonical_record``, ``_bounded_json_record_
    # paths``, the dead ``_read_many``, and the ``_read_sealed_index_
    # migration_state``/``_read_unique_json_document`` pair) is dissolved in
    # CQ20: either its one real caller now calls the module function (or, for
    # the sealed-state pair, ``queue_legacy_audit``'s existing equivalent
    # ``_read_sealed_state``) directly, or -- ``_require_durable_record_id``/
    # ``_label_key`` -- it had zero production callers left at all. See the
    # design doc ledger §15 for the full accounting.

    def _write(self, path: Path, record: BaseModel) -> None:
        queue_store_write.write_model(self._storage_root, path, record)

    def _write_json(self, path: Path, record: dict[str, object]) -> None:
        queue_store_write.write_json(self._storage_root, path, record)

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
    def _read_json_file(path: Path, model: type[Record]) -> Record:
        return queue_store_read.read_json_file(path, model)

    @staticmethod
    def _read_json_document(path: Path) -> object:
        return queue_store_read.read_json_document(path)


def _is_canonical_event_path(storage_root: Path, path: Path, family: str) -> bool:
    return queue_layout.is_canonical_event_path(storage_root, path, family)


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


def _record_is_reparse(file_stat: os.stat_result) -> bool:
    return queue_layout.record_is_reparse(file_stat)

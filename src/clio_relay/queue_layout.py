"""Canonical durable-record layout and access validation."""

from __future__ import annotations

import os
import stat
from pathlib import Path

from pydantic import BaseModel

from clio_relay import queue_context
from clio_relay.errors import ConfigurationError, QueueConflictError
from clio_relay.identifiers import filesystem_key, validate_durable_record_id
from clio_relay.models import (
    ArtifactRef,
    Cursor,
    EndpointRegistration,
    GatewaySession,
    JobKind,
    JobTombstone,
    Lease,
    MonitorRule,
    ProgressRecord,
    RelayEvent,
    RelayJob,
    RelayTask,
    SchedulerCancelPending,
    TaskTimelineEvent,
    TransformRef,
)

LeaseExpiryReference = tuple[int, str, JobKind, str, str, str, str]
UNSET = object()
INPUT_INGEST_ATTEMPT_METADATA_KEY = "input_ingest_attempt"
INPUT_INGEST_ATTEMPT_SCHEMA = "clio-relay.input-ingest-attempt.v1"
INPUT_INGEST_ORIGINAL_POLICY_METADATA_KEY = "input_ingest_original_policy"
DEFAULT_INPUT_INGEST_ABANDONED_AFTER_SECONDS = 300
MAX_INPUT_INGEST_RECOVERY_BATCH = 256
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
LEGACY_LEASE_OPERATIONAL_INDEX_SCHEMA = "clio-relay.lease-operational-index.v1"
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
MAX_JARVIS_PACKAGE_INPUT_CONTRACT_RECORDS = 10_000
MAX_JARVIS_PIPELINE_INPUT_BINDING_RECORDS = 10_000
MAX_JARVIS_PIPELINE_INPUT_LINEAGE_RECORDS = 10_000
MAX_JARVIS_RUN_INPUT_MANIFEST_RECORDS = 10_000
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
LEGACY_RECORD_AUDIT_SCHEMA = "clio-relay.legacy-record-audit.v1"
CANONICAL_RECORD_ACCESS_SCHEMA = "clio-relay.canonical-record-access.v1"
QUEUE_LAYOUT_SCHEMA = "clio-relay.queue-layout.v1"
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
    "jarvis_package_input_contracts": 262_144,
    "jarvis_pipeline_input_bindings": 1_048_576,
    "jarvis_pipeline_input_lineage": 1_048_576,
    "jarvis_run_input_manifests": 1_048_576,
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
    "mcp_tasks": 1_048_576,
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
    "transforms": 262_144,
    "used_artifacts_by_job": 262_144,
}


class TransientRecordReplacement(RuntimeError):
    """Signal that an atomic replacement invalidated one bounded read attempt."""


class QueueLayout:
    """Own queue-root identity and canonical record path construction."""

    def __init__(self, store: queue_context.QueueStoreProtocol) -> None:
        self._store = store

    def storage_root_stat(self) -> os.stat_result:
        """Inspect the queue root through its held descriptor when migration-pinned."""
        descriptor, identity = self._store.locked_storage_root()
        if descriptor is None:
            return os.lstat(self._store.storage_root)
        try:
            details = os.fstat(descriptor)
        except OSError as exc:
            raise ConfigurationError("migration queue-root descriptor is unavailable") from exc
        if (details.st_dev, details.st_ino) != identity:
            raise ConfigurationError("migration queue-root descriptor identity changed")
        return details

    def job_record_path(self, family: str, job_id: str, record_id: str) -> Path:
        """Return one canonical per-job record path."""
        return (
            self._store.storage_root
            / family
            / self.durable_key(job_id)
            / f"{self.durable_key(record_id)}.json"
        )

    @staticmethod
    def durable_key(value: str) -> str:
        """Return one validated portable durable record identifier."""
        return QueueLayout.require_durable_record_id(value, field="record_id")

    @staticmethod
    def require_durable_record_id(value: str, *, field: str) -> str:
        """Validate one durable identifier and retain the facade error contract."""
        try:
            return validate_durable_record_id(value)
        except ValueError as error:
            raise ValueError(f"invalid {field}: {error}") from error

    @staticmethod
    def label_key(value: str, *, domain: str) -> str:
        """Return one deterministic filesystem key for an arbitrary label."""
        return filesystem_key(value, domain=domain)


def record_identity_field(model: type[BaseModel]) -> str:
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
        TransformRef: "job_id",
    }
    try:
        return identity_fields[model]
    except KeyError as error:
        raise QueueConflictError(
            f"canonical record model has no filename identity contract: {model.__name__}"
        ) from error


_CANONICAL_FLAT_RECORD_IDENTITIES: dict[
    str,
    tuple[type[BaseModel], str, str],
] = {
    "artifacts": (ArtifactRef, "artifact_id", "artifact"),
    "cursors": (Cursor, "job_id", "cursor"),
    "endpoints": (EndpointRegistration, "endpoint_id", "endpoint"),
    "gateway_sessions": (GatewaySession, "session_id", "gateway session"),
    "job_tombstones": (JobTombstone, "job_id", "job tombstone"),
    "jobs": (RelayJob, "job_id", "job"),
    "leases": (Lease, "lease_id", "lease"),
    "monitor_rules": (MonitorRule, "rule_id", "monitor rule"),
    "progress": (ProgressRecord, "progress_id", "progress"),
    "tasks": (RelayTask, "task_id", "task"),
    "transforms": (TransformRef, "job_id", "transform ref"),
}


def is_canonical_event_path(storage_root: Path, path: Path, family: str) -> bool:
    """Return whether ``path`` has one canonical identity/sequence event layout."""
    try:
        relative = path.relative_to(storage_root)
    except ValueError:
        return False
    return len(relative.parts) == 3 and relative.parts[0] == family


def validate_canonical_access(
    storage_root: Path,
    path: Path,
    record: BaseModel,
) -> None:
    """Validate filename-bound canonical identity for one individual record read."""
    try:
        relative = path.relative_to(storage_root)
    except ValueError:
        return
    flat_contract = (
        _CANONICAL_FLAT_RECORD_IDENTITIES.get(relative.parts[0])
        if len(relative.parts) == 2
        else None
    )
    if flat_contract is not None:
        expected_model, identity_field, label = flat_contract
        if not isinstance(record, expected_model):
            raise QueueConflictError(f"canonical {label} record type mismatch: {path}")
        identity = getattr(record, identity_field, None)
        try:
            filename_identity = validate_durable_record_id(path.stem)
        except ValueError as error:
            raise QueueConflictError(
                f"canonical {label} filename identity is invalid: {path}"
            ) from error
        if path.name != f"{filename_identity}.json" or identity != filename_identity:
            raise QueueConflictError(f"canonical {label} identity mismatch: {path}")
        return

    event_contract: tuple[str, type[BaseModel], str, str] | None = None
    if isinstance(record, RelayEvent) and is_canonical_event_path(storage_root, path, "events"):
        event_contract = ("events", RelayEvent, "job_id", "event")
    elif isinstance(record, TaskTimelineEvent) and is_canonical_event_path(
        storage_root,
        path,
        "task_events",
    ):
        event_contract = ("task_events", TaskTimelineEvent, "task_id", "task event")
    if event_contract is None:
        return
    _family, expected_model, identity_field, label = event_contract
    if not isinstance(record, expected_model):
        raise QueueConflictError(f"canonical {label} record type mismatch: {path}")
    try:
        directory_identity = validate_durable_record_id(path.parent.name)
    except ValueError as error:
        raise QueueConflictError(
            f"canonical {label} directory identity is invalid: {path.parent}"
        ) from error
    sequence_text = path.name.removesuffix(".json")
    if (
        path.name != f"{sequence_text}.json"
        or len(sequence_text) != 20
        or not sequence_text.isascii()
        or not sequence_text.isdigit()
        or getattr(record, identity_field, None) != directory_identity
        or getattr(record, "seq", None) != int(sequence_text)
    ):
        raise QueueConflictError(f"canonical {label} identity mismatch: {path}")


def safe_owner_legacy_job_id(job_id: object) -> bool:
    """Return whether one legacy owner-session job identifier is portable."""
    return safe_global_record_id(job_id)


def safe_global_record_id(record_id: object) -> bool:
    """Return whether one object is a portable durable record identifier."""
    if not isinstance(record_id, str):
        return False
    try:
        validate_durable_record_id(record_id)
    except ValueError:
        return False
    return True


def record_family(path: Path) -> str:
    """Return the durable record family governing one path's byte limit."""
    if "global_order" in path.parts[:-1]:
        return "global_order"
    for part in reversed(path.parts[:-1]):
        if part in RECORD_FAMILY_MAX_BYTES:
            return part
    return "unknown"


def record_max_bytes(path: Path) -> int:
    """Return the configured byte limit for one durable record path."""
    return RECORD_FAMILY_MAX_BYTES.get(record_family(path), DEFAULT_RECORD_MAX_BYTES)


def record_is_reparse(file_stat: os.stat_result) -> bool:
    """Return whether one stat result identifies a Windows reparse point."""
    attributes = getattr(file_stat, "st_file_attributes", 0) or 0
    return bool(attributes & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400))


def validate_record_stat(file_stat: os.stat_result, *, path: Path) -> None:
    """Require a stable, singly linked regular durable-record file."""
    if not stat.S_ISREG(file_stat.st_mode) or record_is_reparse(file_stat):
        raise QueueConflictError(f"durable record is not a regular owned file: {path}")
    if file_stat.st_nlink == 0:
        raise TransientRecordReplacement(f"durable record was atomically unlinked: {path}")
    if file_stat.st_nlink != 1:
        raise QueueConflictError(f"durable record must not be hard linked: {path}")


def record_stats_match(
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

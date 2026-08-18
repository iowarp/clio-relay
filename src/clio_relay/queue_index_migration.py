"""Bounded v0.9-flat-to-indexed record migration: the phased batch driver.

Owns the public, resumable batch driver (``migrate_indexes_batch``), its two
read-only status reporters (``index_migration_status``, ``readiness_info``),
the final cross-source reconciliation pass (``_reconcile_index_migration_
sources_unlocked``), and the four per-family projection dispatchers
(``_migrate_record_unlocked``, ``_migrate_retention_record_unlocked``,
``_migrate_operational_record_unlocked``, ``_migrate_order_record_
unlocked``) that fan each canonical record out to every index it owns.

Predecessors: CQ2-CQ18 (all landed) plus this slice's own ``queue_startup``
(rank 43, lands immediately before this owner -- CQ19-ST-01, see that
module's docstring: ``migrate_indexes_batch``/``index_migration_status``
self-call ``self.initialize()`` as their first line). Every per-family
projection call below reaches an already-landed owner through an ordinary
inherited ``self.`` call (a forward edge, stubbed under ``TYPE_CHECKING``);
the migration-record-batch lookup (``queue_store_read.migration_batch_
paths``) and the scheduler-cancellation-request decoder (``queue_
scheduler_cancel_records.scheduler_cancellation_request``/``cancellation_
requested_at``) are module-qualified, matching how ``core_queue.py`` already
called them before this move.

Failing-first sabotage (design doc CQ19 row, "one domain-migration lookup"):
every per-family batch in ``migrate_indexes_batch`` resolves its bounded
record-path listing through ``queue_index_migration.queue_store_read.
migration_batch_paths`` -- an isolated-namespace patch on that lookup fails
every batch phase identically.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

from pydantic import BaseModel

from clio_relay import (
    queue_layout,
    queue_order_index,
    queue_owner_session_records,
    queue_scheduler_cancel_records,
    queue_store_lock,
    queue_store_read,
)
from clio_relay.errors import QueueConflictError
from clio_relay.models import (
    ArtifactRef,
    EndpointRegistration,
    GatewaySession,
    Lease,
    MonitorRule,
    ProgressRecord,
    RelayJob,
    RelayTask,
)

if TYPE_CHECKING:
    from datetime import datetime
    from pathlib import Path

    from clio_relay import queue_context, queue_lease_records, queue_legacy_output_codec
    from clio_relay.models import SchedulerCancelPending
    from clio_relay.worker_lifetime_lock import LockedCoreIdentity

    LegacyOutputAudit = queue_legacy_output_codec.LegacyOutputAudit


class QueueIndexMigrationMixin:
    """Own the bounded v0.9-to-indexed migration batch driver and status reads."""

    _storage_root: Path
    _lock: queue_context.QueueLockProtocol

    if TYPE_CHECKING:

        def initialize(
            self,
            *,
            migrate_legacy_output: bool = False,
            locked_core: LockedCoreIdentity | None = None,
            allow_exclusive_seal: bool = True,
        ) -> None: ...
        def _read_index_migration_state(self) -> dict[str, object]: ...
        def _write_index_migration_state(self, state: dict[str, object]) -> None: ...
        def _read_canonical_record[Record: BaseModel](
            self, path: Path, model: type[Record]
        ) -> Record: ...
        @classmethod
        def _scan_many[Record: BaseModel](
            cls,
            directory: Path,
            model: type[Record],
            *,
            limit: int,
            identity_field: str | None = None,
        ) -> tuple[list[Record], bool]: ...
        def _write(self, path: Path, record: BaseModel) -> None: ...
        def _job_record_path(self, family: str, job_id: str, record_id: str) -> Path: ...
        @staticmethod
        def _durable_key(value: str) -> str: ...
        def _read_legacy_record_audit_marker(
            self,
            *,
            allow_legacy_lease_schema: bool = False,
        ) -> LegacyOutputAudit | None: ...
        def _prepare_lease_capacity_rebuild_intent_unlocked(
            self,
            *,
            identity: str,
            limit: int,
        ) -> tuple[Path, dict[str, object]]: ...
        def _apply_lease_index_repair_intent_unlocked(
            self,
            intent_path: Path,
            payload: dict[str, object],
        ) -> int: ...
        def _read_lease_capacity_aggregate_unlocked(
            self,
        ) -> queue_lease_records.LeaseCapacityPair: ...
        def _finalize_job_index_unlocked(self, job_id: str) -> None: ...
        def _ensure_global_order_entry_unlocked(self, family: str, record_id: str) -> int: ...

        # per-family projection collaborators (all already-landed owners)
        def _initialize_job_index_unlocked(self, job_id: str) -> None: ...
        def _ensure_artifact_use_indexes_unlocked(self, job: RelayJob) -> None: ...
        def _write_job_unlocked(self, job: RelayJob) -> None: ...
        def _initialize_artifact_user_order_unlocked(self, artifact_id: str) -> None: ...
        def _update_job_index_unlocked(self, job_id: str, **updates: object) -> None: ...
        def _sync_scheduler_source_unlocked(
            self, job_id: str, *, source_id: str, metadata: dict[str, object]
        ) -> None: ...
        def _sync_task_retention_indexes_unlocked(self, task: RelayTask) -> None: ...
        def _link_gateways_for_artifact_unlocked(self, artifact: ArtifactRef) -> None: ...
        def _sync_monitor_rule_indexes_unlocked(self, rule: MonitorRule) -> None: ...
        def _index_gateway_session_unlocked(self, session: GatewaySession) -> None: ...
        def _index_fresh_endpoint_unlocked(self, endpoint: EndpointRegistration) -> None: ...
        def _sync_owner_session_job_membership_unlocked(self, job: RelayJob) -> None: ...
        def get_job(self, job_id: str) -> RelayJob: ...
        def _sync_lease_operational_indexes_unlocked(
            self, lease: Lease, *, job: RelayJob, previous_lease: Lease | None = None
        ) -> queue_lease_records.LeaseIndexIdentity: ...
        def _ensure_scheduler_cancel_pending_unlocked(
            self, job: RelayJob, *, requested_at: datetime, reason: str
        ) -> SchedulerCancelPending: ...
        def _write_ordered_job_record(
            self,
            family: str,
            job_id: str,
            sequence: int,
            record: BaseModel,
        ) -> None: ...

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
                    "fixed_queue_family_count": len(queue_store_lock.INITIALIZED_QUEUE_FAMILIES),
                    "fixed_global_order_family_count": len(queue_store_lock.GLOBAL_ORDER_FAMILIES),
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
                "fixed_queue_family_count": len(queue_store_lock.INITIALIZED_QUEUE_FAMILIES),
                "fixed_global_order_family_count": len(queue_store_lock.GLOBAL_ORDER_FAMILIES),
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
                    limit=queue_layout.MAX_LIVE_LEASE_RECORDS,
                )
                repaired = self._apply_lease_index_repair_intent_unlocked(
                    intent_path,
                    repair_payload,
                )
                lease_repair.update(
                    {
                        "complete": True,
                        "schema_version": queue_layout.LEASE_OPERATIONAL_INDEX_SCHEMA,
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
                    limit=queue_layout.MAX_LIVE_LEASE_RECORDS,
                )
                repaired = self._apply_lease_index_repair_intent_unlocked(
                    intent_path,
                    repair_payload,
                )
                capacity = self._read_lease_capacity_aggregate_unlocked()
                capacity_checkpoint.update(
                    {
                        "complete": True,
                        "schema_version": queue_layout.LEASE_CAPACITY_AGGREGATE_SCHEMA,
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
                    "schema_version": queue_layout.LEASE_CAPACITY_AGGREGATE_SCHEMA,
                    "epoch_id": capacity.aggregate.epoch_id,
                    "generation": capacity.aggregate.generation,
                    "record_count": capacity.aggregate.global_live_leases,
                }
            )
            state["complete"] = True
            self._write_index_migration_state(state)
            return state

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
                limit=queue_layout.MAX_BOUNDED_SCAN_RECORDS,
            )
            if truncated:
                raise QueueConflictError(
                    "index migration final reconciliation exceeded its safety bound of "
                    f"{queue_layout.MAX_BOUNDED_SCAN_RECORDS} records for {family}"
                )
            source_records[family] = records

        for family in ("jobs", "tasks", "leases", "artifacts", "progress"):
            for record in source_records[family]:
                self._migrate_record_unlocked(family, record)

        for family in queue_store_lock.ORDER_FAMILIES:
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

        for family in queue_store_lock.RETENTION_INDEX_FAMILIES:
            for record in source_records[family]:
                self._migrate_retention_record_unlocked(family, record)

        for family in queue_store_lock.OPERATIONAL_INDEX_FAMILIES:
            if family == "leases":
                continue
            for record in source_records[family]:
                self._migrate_operational_record_unlocked(family, record)

        lease_repair_intent, lease_repair_payload = (
            self._prepare_lease_capacity_rebuild_intent_unlocked(
                identity="migration-v1-final-reconcile",
                limit=queue_layout.MAX_LIVE_LEASE_RECORDS,
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
                retention_schema_version=queue_layout.RETENTION_INDEX_SCHEMA,
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
            request = queue_scheduler_cancel_records.scheduler_cancellation_request(record)
            if request is not None and request.get("cancel_scheduler") is True:
                self._ensure_scheduler_cancel_pending_unlocked(
                    record,
                    requested_at=(
                        queue_scheduler_cancel_records.cancellation_requested_at(request)
                        or record.updated_at
                    ),
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

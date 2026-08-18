"""Terminal-job GC orchestration: eligibility protections and phased collection.

Owns the public read-only planning method (``plan_terminal_job_gc``), the
phased, resumable collection driver (``collect_terminal_job``), and every
protection-owner lookup and trash-staging primitive underneath them:
``_terminal_job_gc_protections`` (the fail-closed eligibility gate --
GC protection ordering is correctness-critical, a GC that collects a
protected job is data loss), ``_artifact_lineage_gc_protections``,
``_indexed_gc_entry_state``, the idempotency retire/digest primitives, and
the tombstone-phase trash-staging walk (``_trash_job_roots_unlocked``/
``_trash_job_references_unlocked``/``_trash_primary_record_unlocked``).

Predecessors: CQ6 (legacy output migration, ``_retire_legacy_output_
receipts_unlocked``), CQ9 (artifact lineage), CQ10 (owner-session closure),
CQ11/CQ4 (scheduler-cancel state), CQ12 (jobs), CQ13-CQ17 (all landed before
this slice). Each protection check below reaches its real owner through an
ordinary inherited ``self.`` call (a forward edge to an earlier-ranked
owner, stubbed under ``TYPE_CHECKING``) -- these are genuine composition
edges, not new call-site plumbing, since every collaborator method already
existed on its own owner before this slice.

The one design-mandated module-qualified seam (design row: "then
``queue_job_gc.queue_gc_storage.move_gc_path``") is the quarantine move used
throughout the trash-staging walk, plus the paired ``queue_gc_storage.
purge_tree_batch``/``after_gc_checkpoint`` lookups -- all three patchable
via ``monkeypatch.setattr(queue_job_gc, "queue_gc_storage", isolated_ns)``.

``_is_sha256_digest`` dedup (ledger §9.6/§10.4 follow-up): this slice is the
one ledger §9.6 names as owning the facade's copy's real disposition, since
its only two callers (``_terminal_job_gc_protections``, ``_read_committed_
job_digest``) move here. The facade's now-orphaned copy is deleted outright
(not moved) -- this module keeps its own private duplicate, matching the
already-established per-owner idiom (``queue_jobs.py``, ``queue_artifact_
lineage.py``, ``queue_lease_records.py``, ``queue_legacy_output_codec.py``
each already keep one). ``queue_jobs``/``queue_artifact_lineage`` keep
their own existing copies unchanged: importing this module's copy would be
a reverse-rank edge (both rank well before this owner), so per-owner
duplication -- not a single shared import -- is the correct resolution here,
not an oversight.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, cast

from pydantic import BaseModel

from clio_relay import (
    queue_context,
    queue_gc_storage,
    queue_idempotency,
    queue_layout,
    queue_store_read,
    queue_store_write,
)
from clio_relay.errors import NotFoundError, QueueConflictError
from clio_relay.models import (
    ArtifactRef,
    JobGcPhase,
    JobTombstone,
    Lease,
    MonitorRule,
    ProgressRecord,
    RelayJob,
    RelayTask,
    TerminalJobGcPlan,
    TerminalJobGcResult,
    UsedArtifactRef,
    utc_now,
)
from clio_relay.pagination import validate_gc_batch_size


def _is_sha256_digest(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


class QueueJobGcMixin:
    """Own terminal-job GC eligibility and phased trash-staging collection."""

    _storage_root: Path
    _lock: queue_context.QueueLockProtocol
    _store_adapter: queue_context.QueueStoreProtocol

    if TYPE_CHECKING:

        def get_job(self, job_id: str) -> RelayJob: ...
        def get_job_tombstone(self, job_id: str) -> JobTombstone | None: ...
        def _terminal_job_gc_protections(self, job: RelayJob) -> list[str]: ...
        def _artifact_user_order_root(self, artifact_id: str) -> Path: ...
        def _read_artifact_user_order_head(self, artifact_id: str) -> int: ...
        def _scheduler_reverse_ref_path(
            self,
            scheduler_id: str,
            job_id: str,
            source_id: str,
        ) -> Path: ...
        def _retire_legacy_output_receipts_unlocked(self, tombstone: JobTombstone) -> bool: ...

    def plan_terminal_job_gc(self, job_id: str) -> TerminalJobGcPlan:
        """Build a read-only, fail-closed terminal-job collection plan."""
        job_id = queue_layout.QueueLayout.require_durable_record_id(job_id, field="job_id")
        self._store_adapter.initialize()
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
        job_id = queue_layout.QueueLayout.require_durable_record_id(job_id, field="job_id")
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
                self._store_adapter.write(self._job_tombstone_path(job_id), tombstone)
                actions += 1
                queue_gc_storage.after_gc_checkpoint(JobGcPhase.PREPARED)
                if actions >= batch_size:
                    return self._gc_result(plan, tombstone, actions)
            if tombstone.phase is JobGcPhase.PREPARED:
                self._retire_idempotency_unlocked(tombstone)
                tombstone = self._advance_tombstone(tombstone, JobGcPhase.IDEMPOTENCY_RETIRED)
                actions += 1
                queue_gc_storage.after_gc_checkpoint(JobGcPhase.IDEMPOTENCY_RETIRED)
                if actions >= batch_size:
                    return self._gc_result(plan, tombstone, actions)
            if tombstone.phase is JobGcPhase.IDEMPOTENCY_RETIRED:
                if not tombstone.records_trash_started:
                    current_job = self._store_adapter.read_optional(
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
                    self._store_adapter.write(self._job_tombstone_path(job_id), tombstone)
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
                    queue_gc_storage.after_gc_checkpoint(JobGcPhase.RECORDS_TRASHED)
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
                    queue_gc_storage.after_gc_checkpoint(JobGcPhase.REFERENCES_TRASHED)
                elif processed:
                    tombstone = self._record_gc_progress(tombstone, removed=processed)
                if actions >= batch_size or not complete:
                    return self._gc_result(plan, tombstone, actions)
            if tombstone.phase is JobGcPhase.REFERENCES_TRASHED:
                tombstone = self._advance_tombstone(tombstone, JobGcPhase.PURGING)
                queue_gc_storage.after_gc_checkpoint(JobGcPhase.PURGING)
            if tombstone.phase is JobGcPhase.PURGING:
                removed, empty = queue_gc_storage.purge_tree_batch(
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
                    queue_gc_storage.after_gc_checkpoint(JobGcPhase.COMPLETE)
                elif removed:
                    tombstone = self._record_gc_progress(tombstone, removed=removed)
            return self._gc_result(plan, tombstone, actions)

    def _job_tombstone_path(self, job_id: str) -> Path:
        return (
            self._storage_root
            / "job_tombstones"
            / f"{queue_layout.QueueLayout.durable_key(job_id)}.json"
        )

    def _job_gc_trash_path(self, job_id: str) -> Path:
        return self._storage_root / "gc_trash" / queue_layout.QueueLayout.durable_key(job_id)

    def _read_committed_job_digest(self, job: RelayJob) -> str:
        key_path = (
            self._storage_root
            / "idempotency"
            / f"{queue_idempotency._idempotency_key_filename(job.idempotency_key)}.json"  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        )
        raw = queue_store_read.read_json_document(key_path)
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
            / (
                f"{queue_idempotency._idempotency_key_filename(tombstone.idempotency_key)}.json"  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            )
        )
        raw = queue_store_read.read_json_document(key_path)
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
        queue_store_write.write_json(
            self._storage_root,
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
        self._store_adapter.write(self._job_tombstone_path(tombstone.job_id), updated)
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
        self._store_adapter.write(self._job_tombstone_path(tombstone.job_id), updated)
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
        safe_job_id = queue_layout.QueueLayout.durable_key(job_id)
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
            if queue_gc_storage.move_gc_path(source, destination):
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
                paths, _has_more = queue_store_read.migration_batch_paths(
                    source_dir,
                    cursor=None,
                    limit=1,
                )
                if not paths:
                    break
                path = paths[0]
                record = queue_store_read.read_json_file(path, model)
                self._trash_primary_record_unlocked(record, trash=trash)
                processed = trash / "processed" / family / path.name
                queue_gc_storage.move_gc_path(path, processed)
                actions += 1
            if actions >= limit:
                return actions, False, tombstone
        used_source_dir = trash / "owned" / "used_artifacts_by_job"
        while actions < limit:
            paths, _has_more = queue_store_read.migration_batch_paths(
                used_source_dir,
                cursor=None,
                limit=1,
            )
            if not paths:
                break
            path = paths[0]
            record = queue_store_read.read_json_file(path, UsedArtifactRef)
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
            reverse = self._store_adapter.read_optional(reverse_path, UsedArtifactRef)
            if reverse is not None and reverse != record:
                raise QueueConflictError(f"used-artifact reverse reference changed: {reverse_path}")
            mapping = self._store_adapter.read_optional(mapping_path, UsedArtifactRef)
            if mapping is not None and mapping != record:
                raise QueueConflictError(f"used-artifact order mapping changed: {mapping_path}")
            entry = self._store_adapter.read_optional(entry_path, UsedArtifactRef)
            if entry is not None and entry != record:
                raise QueueConflictError(f"used-artifact order entry changed: {entry_path}")
            queue_store_write.unlink_durable_path(reverse_path, missing_ok=True)
            queue_store_write.unlink_durable_path(entry_path, missing_ok=True)
            queue_store_write.unlink_durable_path(mapping_path, missing_ok=True)
            queue_gc_storage.move_gc_path(
                path,
                trash / "processed" / "used_artifacts_by_job" / path.name,
            )
            actions += 1
        if actions >= limit:
            return actions, False, tombstone
        scheduler_source_dir = trash / "owned" / "scheduler_refs_by_job"
        while actions < limit:
            paths, _has_more = queue_store_read.migration_batch_paths(
                scheduler_source_dir,
                cursor=None,
                limit=1,
            )
            if not paths:
                break
            path = paths[0]
            raw_ref = queue_store_read.read_json_document(path)
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
                queue_store_write.unlink_durable_path(
                    self._scheduler_reverse_ref_path(
                        scheduler_id,
                        tombstone.job_id,
                        source_id,
                    ),
                    missing_ok=True,
                )
            queue_gc_storage.move_gc_path(
                path, trash / "processed" / "scheduler_refs_by_job" / path.name
            )
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
            queue_gc_storage.move_gc_path(
                self._storage_root / "tasks" / f"{record.task_id}.json",
                trash / "primary" / "tasks" / f"{record.task_id}.json",
            )
            queue_gc_storage.move_gc_path(
                self._storage_root / "task_events" / record.task_id,
                trash / "primary" / "task_events" / record.task_id,
            )
            queue_gc_storage.move_gc_path(
                self._storage_root / "task_event_heads" / f"{record.task_id}.json",
                trash / "primary" / "task_event_heads" / f"{record.task_id}.json",
            )
            return
        if isinstance(record, Lease):
            queue_gc_storage.move_gc_path(
                self._storage_root / "leases" / f"{record.lease_id}.json",
                trash / "primary" / "leases" / f"{record.lease_id}.json",
            )
            return
        if isinstance(record, ArtifactRef):
            reverse_directory = self._storage_root / "artifact_users" / record.artifact_id
            order_root = self._artifact_user_order_root(record.artifact_id)
            self._read_artifact_user_order_head(record.artifact_id)
            if queue_store_read.bounded_json_record_paths(
                reverse_directory,
                limit=queue_layout.MAX_ARTIFACT_CONSUMERS,
                label=f"consumers of artifact {record.artifact_id}",
            ):
                raise QueueConflictError(
                    f"artifact still has retained consumers: {record.artifact_id}"
                )
            if queue_store_read.bounded_json_record_paths(
                order_root / "entries",
                limit=queue_layout.MAX_ARTIFACT_CONSUMERS,
                label=f"ordered consumers of artifact {record.artifact_id}",
            ) or queue_store_read.bounded_json_record_paths(
                order_root / "by_consumer",
                limit=queue_layout.MAX_ARTIFACT_CONSUMERS,
                label=f"consumer order mappings for artifact {record.artifact_id}",
            ):
                raise QueueConflictError(
                    f"artifact still has ordered consumer state: {record.artifact_id}"
                )
            queue_gc_storage.move_gc_path(
                self._storage_root / "artifacts" / f"{record.artifact_id}.json",
                trash / "primary" / "artifacts" / f"{record.artifact_id}.json",
            )
            queue_gc_storage.move_gc_path(
                reverse_directory,
                trash / "primary" / "artifact_users" / record.artifact_id,
            )
            queue_gc_storage.move_gc_path(
                order_root,
                trash / "primary" / "artifact_user_order" / record.artifact_id,
            )
            return
        if isinstance(record, ProgressRecord):
            queue_gc_storage.move_gc_path(
                self._storage_root / "progress" / f"{record.progress_id}.json",
                trash / "primary" / "progress" / f"{record.progress_id}.json",
            )
            return
        if isinstance(record, MonitorRule):
            queue_gc_storage.move_gc_path(
                self._storage_root / "monitor_rules" / f"{record.rule_id}.json",
                trash / "primary" / "monitor_rules" / f"{record.rule_id}.json",
            )
            return
        raise QueueConflictError("unsupported GC reference record")

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

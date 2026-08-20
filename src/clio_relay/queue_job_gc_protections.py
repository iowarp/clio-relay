"""Terminal-job GC eligibility: the fail-closed protection-reason gate.

Owns ``_terminal_job_gc_protections`` -- the single fail-closed gate every
terminal-job GC decision (``queue_job_gc.plan_terminal_job_gc``/
``collect_terminal_job``) reads before ever quarantining a record. GC
protection ordering is correctness-critical: a GC that collects a still-
protected job is data loss, so every protection-owner lookup here is an
ordinary inherited ``self.`` call into the real, already-landed owner
(index migration, execution cleanup, scheduler-cancel state, owner-session
closure, idempotency, retention index, and five indexed per-job families),
never a re-derived or duplicated business-logic check.

Typed deviation (CQ18-JG-01): the design doc's single ``queue_job_gc.py``
target splits in two -- this owner (the pure, read-only eligibility check)
and ``queue_job_gc.py`` (the phased trash-staging orchestration that reads
it). Combined, the two exceeded the 800-line hard gate at ~890 real lines
even after every internal helper was moved out to ``queue_gc_storage.py``;
the eligibility check has no write behavior of its own and is a clean,
one-directional dependency of the orchestration half (orchestration calls
``self._terminal_job_gc_protections(job)``; this module never calls back
into orchestration), so the split follows the CQ15 ``queue_lease_admission``
precedent -- a forced, zero-cycle peer separation, not a design change.

``_artifact_lineage_gc_protections`` also lives here (its own producer/
consumer-reference-count guard is exclusively a protection check); the
sibling ``_indexed_gc_entry_state`` scan (leases/tasks/scheduler/monitor/
gateway family presence) is its shared per-family primitive.
"""

from __future__ import annotations

import os
import stat
from pathlib import Path
from typing import TYPE_CHECKING, Literal, cast

from pydantic import BaseModel

from clio_relay import queue_context, queue_idempotency, queue_layout, queue_store_read
from clio_relay.errors import NotFoundError, QueueConflictError
from clio_relay.models import (
    TERMINAL_STATES,
    ArtifactRef,
    Lease,
    MonitorRule,
    OwnerSessionClosure,
    RelayJob,
    RelayTask,
    UsedArtifactRef,
)


def _is_sha256_digest(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


class QueueJobGcProtectionsMixin:
    """Own the fail-closed terminal-job GC eligibility gate."""

    _storage_root: Path
    _lock: queue_context.QueueLockProtocol
    _store_adapter: queue_context.QueueStoreProtocol

    if TYPE_CHECKING:

        def _read_index_migration_state(self) -> dict[str, object]: ...
        def _job_has_pending_execution_cleanup_unlocked(
            self, cluster: str, job_id: str
        ) -> bool: ...
        def _scheduler_cancel_record_path(
            self,
            family: Literal["scheduler_cancel_pending", "scheduler_cancel_dispositions"],
            cluster: str,
            job_id: str,
        ) -> Path: ...
        def get_owner_session_closed(
            self,
            owner_session_id: str,
            *,
            session_generation_id: str | None = None,
        ) -> OwnerSessionClosure | None: ...
        def _read_job_index(self, job_id: str) -> dict[str, object] | None: ...
        def _artifact_user_order_root(self, artifact_id: str) -> Path: ...
        def _read_artifact_user_order_head(self, artifact_id: str) -> int: ...
        def _validate_artifact_use_record(self, record: UsedArtifactRef) -> None: ...
        def list_artifacts(self, job_id: str) -> list[ArtifactRef]: ...

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
            / (
                f"{queue_idempotency._idempotency_key_filename(job.idempotency_key)}.json"  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            )
        )
        try:
            raw_idempotency = queue_store_read.read_json_document(key_path)
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
        if (
            index is None
            or index.get("retention_schema_version") != queue_layout.RETENTION_INDEX_SCHEMA
        ):
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
                reverse_paths = queue_store_read.bounded_json_record_paths(
                    self._storage_root / "artifact_users" / artifact.artifact_id,
                    limit=queue_layout.MAX_ARTIFACT_CONSUMERS,
                    label=f"consumers of artifact {artifact.artifact_id}",
                )
                order_root = self._artifact_user_order_root(artifact.artifact_id)
                self._read_artifact_user_order_head(artifact.artifact_id)
                entry_paths = queue_store_read.bounded_json_record_paths(
                    order_root / "entries",
                    limit=queue_layout.MAX_ARTIFACT_CONSUMERS,
                    label=f"ordered consumers of artifact {artifact.artifact_id}",
                )
                mapping_paths = queue_store_read.bounded_json_record_paths(
                    order_root / "by_consumer",
                    limit=queue_layout.MAX_ARTIFACT_CONSUMERS,
                    label=f"consumer order mappings for artifact {artifact.artifact_id}",
                )
                if (
                    len(reverse_paths) != len(entry_paths)
                    or len(mapping_paths) < len(entry_paths)
                    or (mapping_paths and not reverse_paths)
                ):
                    return ["artifact_lineage_state_ambiguous"]
                for path in reverse_paths:
                    record = queue_store_read.read_json_file(path, UsedArtifactRef)
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
        directory = self._storage_root / family / queue_layout.QueueLayout.durable_key(job_id)
        try:
            directory_stat = os.lstat(directory)
            if not stat.S_ISDIR(directory_stat.st_mode) or queue_layout.record_is_reparse(
                directory_stat
            ):
                return False, True
            with os.scandir(directory) as entries:
                entry = next(entries, None)
            if entry is None:
                return False, False
            path = Path(entry.path)
            if not entry.name.endswith(".json"):
                return False, True
            if family == "leases_by_job":
                record: BaseModel | dict[str, object] = queue_store_read.read_json_file(path, Lease)
            elif family == "active_tasks_by_job":
                record = queue_store_read.read_json_file(path, RelayTask)
            elif family == "active_monitor_rules_by_job":
                record = queue_store_read.read_json_file(path, MonitorRule)
            else:
                raw = queue_store_read.read_json_document(path)
                if not isinstance(raw, dict):
                    return False, True
                record = cast(dict[str, object], raw)
            if isinstance(record, (Lease, RelayTask, MonitorRule)):
                return record.job_id == job_id, record.job_id != job_id
            indexed_job_id = record.get("job_id")
            return indexed_job_id == job_id, indexed_job_id != job_id
        except (OSError, ValueError, QueueConflictError):
            return False, True

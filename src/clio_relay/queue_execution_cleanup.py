"""Execution-cleanup shard layout, flat-to-shard migration, and detection.

Owns the durable execution-cleanup marker's shard filesystem layout and the
crash-safe flat-to-shard migration that upgrades legacy, unsharded markers in
place: ``scan_execution_cleanup`` (the worker-facing bounded shard scan),
``job_has_pending_execution_cleanup`` (the GC/lease-recovery eligibility
gate), and every ``_execution_cleanup_shard*``/``_migrate_execution_cleanup_
shard_unlocked`` primitive underneath them.

Typed deviation (CQ17-EC-01): the design doc's single ``queue_execution_
cleanup.py`` target is a genuine two-owner split, not the doc's one file.
``queue_jobs.write_job`` (CQ12, already landed) calls
``_migrate_execution_cleanup_shard_unlocked``/``_execution_cleanup_shard``
directly on every canonical job write (crash-safety ordering: the shard
migration for a job's cluster must complete before that job's own record is
replaced) -- a real, pre-existing edge, not one introduced by this slice
(``queue_jobs.py`` already carried ``TYPE_CHECKING`` stubs for both names).
That forces this module's rank strictly before ``queue_jobs``. But the
durable-marker *mutation* methods (``register_execution_cleanup``,
``acknowledge_execution_cleanup``, ``migrate_execution_cleanup_plan``,
``stage_execution_cleanup_sidecar``) need ``queue_tasks``'s ``_sync_task_
retention_indexes_unlocked`` (CQ14, rank strictly after this module and
after ``queue_jobs``) every time they persist an updated ``RelayTask``. A
single owner cannot satisfy "ranked before queue_jobs" and "ranked after
queue_tasks" at once (queue_jobs already ranks before queue_tasks), so the
marker-mutation methods split out to ``queue_execution_cleanup_markers.py``,
ranked after ``queue_tasks``. This module (the shard/detection machinery
``write_job`` and the lease-recovery family need) has zero dependency on
``queue_jobs`` or ``queue_tasks`` -- its own former ``self.get_job(...)``
call is replaced with the CQ9-ledger-precedent shared primitive
``queue_store_read.read_required_job``, exactly matching how ``queue_jobs.
get_job`` itself is implemented, so ``job_has_pending_execution_cleanup``'s
observable behavior (including the exact ``NotFoundError``) is unchanged.

Sabotage seam (design row: "Patch its shard read/write lookup and prove
flat-to-shard migration delegates"): ``_migrate_execution_cleanup_shard_
unlocked`` reads each legacy flat marker through the module-qualified
``queue_store_read.read_json_file`` lookup and persists its completion
receipt through ``queue_store_write.write_json`` -- both isolated-namespace
patchable, matching the established ``queue_gateway_indexes``/``queue_
legacy_audit`` idiom for owners that manage their own JSON documents
directly rather than through the canonical-record store adapter.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import stat
from pathlib import Path
from typing import TYPE_CHECKING, cast

from clio_relay import queue_context, queue_layout, queue_store_read, queue_store_write
from clio_relay.errors import QueueConflictError, queue_conflict_from_cause
from clio_relay.models import RelayJob, RelayTask, utc_now

logger = logging.getLogger(__name__)


class QueueExecutionCleanupMixin:
    """Own the execution-cleanup shard layout, migration, and detection."""

    _storage_root: Path
    _lock: queue_context.QueueLockProtocol
    _store_adapter: queue_context.QueueStoreProtocol

    if TYPE_CHECKING:

        def _require_index_migration_complete(self) -> None: ...
        def _recover_pending_transitions_unlocked(self) -> list[RelayJob]: ...

    def scan_execution_cleanup(
        self,
        *,
        cluster: str,
        limit: int,
    ) -> tuple[list[RelayTask], bool]:
        """Read one fair, bounded cleanup shard and durably advance the scan cursor."""
        self._store_adapter.initialize()
        cluster_key = queue_layout.QueueLayout.label_key(cluster, domain="cluster")
        cursor_path = self._storage_root / "execution_cleanup_scan_cursors" / f"{cluster_key}.json"
        with self._lock:
            try:
                raw_cursor = queue_store_read.read_json_document(cursor_path)
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
                queue_store_write.write_json(
                    self._storage_root,
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
        job_id = queue_layout.QueueLayout.require_durable_record_id(job_id, field="job_id")
        self._store_adapter.initialize()
        self._require_index_migration_complete()
        with self._lock:
            self._recover_pending_transitions_unlocked()
            job = queue_store_read.read_required_job(self._storage_root, job_id)
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
                marker = queue_store_read.read_json_file(marker_path, RelayTask)
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
            marker = queue_store_read.read_json_file(legacy_path, RelayTask)
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
                existing = queue_store_read.read_json_file(target, RelayTask)
                if existing.task_id != marker.task_id or existing.job_id != marker.job_id:
                    raise QueueConflictError(
                        f"execution cleanup migration target conflicts: {target}"
                    )
                queue_store_write.unlink_durable_path(legacy_path)
            else:
                legacy_path.replace(target)
            self._fsync_execution_cleanup_directory(target.parent)
            self._fsync_execution_cleanup_directory(shard_path)
        queue_store_write.write_json(
            self._storage_root,
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
            raw_receipt = queue_store_read.read_json_document(receipt_path)
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
            limit=queue_layout.DEFAULT_EXACT_RECORD_LIMIT + 1,
        )
        return self._job_has_pending_execution_cleanup_unlocked(cluster, job_id)

    def _execution_cleanup_path(self, cluster: str, job_id: str, task_id: str) -> Path:
        return self._execution_cleanup_job_path(cluster, job_id) / (
            f"{queue_layout.QueueLayout.durable_key(task_id)}.json"
        )

    def _execution_cleanup_job_path(self, cluster: str, job_id: str) -> Path:
        return (
            self._execution_cleanup_shard_path(
                cluster,
                self._execution_cleanup_shard(job_id),
            )
            / f"{queue_layout.QueueLayout.durable_key(job_id)}.pending"
        )

    def _execution_cleanup_migration_receipt_path(self, cluster: str, shard: int) -> Path:
        return (
            self._storage_root
            / "execution_cleanup_migrations"
            / queue_layout.QueueLayout.label_key(cluster, domain="cluster")
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
            / queue_layout.QueueLayout.label_key(cluster, domain="cluster")
            / f"{shard:02x}"
        )

    @staticmethod
    def _execution_cleanup_shard(job_id: str) -> int:
        return hashlib.sha256(job_id.encode("utf-8")).digest()[0]

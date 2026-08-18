"""Execution-cleanup durable marker mutation: register, acknowledge, migrate, stage.

Owns the public methods that mutate a task's ``execution_cleanup`` metadata
and its paired durable marker: ``register_execution_cleanup``,
``acknowledge_execution_cleanup``, ``migrate_execution_cleanup_plan``, and
``stage_execution_cleanup_sidecar``, plus the ``_after_execution_cleanup_
canonical_ack`` fault-injection seam.

Typed deviation (CQ17-EC-01, see ``queue_execution_cleanup.py``'s own
docstring for the full account): these four methods are split out of the
design doc's single ``queue_execution_cleanup.py`` target because every one
of them persists an updated ``RelayTask`` and therefore needs ``queue_
tasks``'s ``_sync_task_retention_indexes_unlocked`` (CQ14) -- ranked after
``queue_jobs`` (CQ12), which in turn must rank after the shard/detection
half of execution cleanup (``queue_execution_cleanup.py``, this module's own
predecessor) because ``queue_jobs.write_job`` calls into it directly. This
module therefore ranks after both ``queue_execution_cleanup`` and ``queue_
tasks``; every method here still delegates its shard-layout/migration
concerns to the earlier-ranked sibling through ordinary inherited ``self.``
calls (forward edges only, stubbed under ``TYPE_CHECKING``), never
duplicating any of that logic.
"""

from __future__ import annotations

import os
import stat
from pathlib import Path
from typing import TYPE_CHECKING, cast

from pydantic import BaseModel

from clio_relay import queue_context, queue_layout, queue_store_write
from clio_relay.errors import NotFoundError, QueueConflictError
from clio_relay.models import RelayTask, utc_now


class QueueExecutionCleanupMarkersMixin:
    """Own execution-cleanup marker mutation: register, acknowledge, migrate, stage."""

    _storage_root: Path
    _lock: queue_context.QueueLockProtocol
    _store_adapter: queue_context.QueueStoreProtocol

    if TYPE_CHECKING:

        @staticmethod
        def _execution_cleanup_shard(job_id: str) -> int: ...
        def _migrate_execution_cleanup_shard_unlocked(
            self, cluster: str, shard: int, *, limit: int
        ) -> bool: ...
        def _execution_cleanup_job_path(self, cluster: str, job_id: str) -> Path: ...
        def _execution_cleanup_path(self, cluster: str, job_id: str, task_id: str) -> Path: ...
        @staticmethod
        def _fsync_execution_cleanup_directory(path: Path) -> None: ...
        def _job_record_path(self, family: str, job_id: str, record_id: str) -> Path: ...
        def _write_ordered_job_record(
            self, family: str, job_id: str, sequence: int, record: BaseModel
        ) -> None: ...
        def _sync_task_retention_indexes_unlocked(self, task: RelayTask) -> None: ...

    def register_execution_cleanup(
        self,
        task_id: str,
        metadata: dict[str, object],
    ) -> RelayTask:
        """Atomically update a task and make its execution cleanup discoverable."""
        task_id = queue_layout.QueueLayout.require_durable_record_id(task_id, field="task_id")
        self._store_adapter.initialize()
        with self._lock:
            path = self._storage_root / "tasks" / f"{task_id}.json"
            task = self._store_adapter.read_optional(path, RelayTask)
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
                limit=queue_layout.DEFAULT_EXACT_RECORD_LIMIT + 1,
            )
            pending_job_path = self._execution_cleanup_job_path(cluster, updated.job_id)
            pending_job_path.mkdir(parents=True, exist_ok=True)
            pending_stat = os.stat(pending_job_path, follow_symlinks=False)
            if not stat.S_ISDIR(pending_stat.st_mode):
                raise QueueConflictError(
                    f"execution cleanup job index is not a directory: {pending_job_path}"
                )
            self._fsync_execution_cleanup_directory(pending_job_path.parent)
            self._store_adapter.write(
                self._execution_cleanup_path(cluster, updated.job_id, updated.task_id),
                updated,
            )
            self._store_adapter.write(path, updated)
            self._store_adapter.write(
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
        job_id = queue_layout.QueueLayout.require_durable_record_id(job_id, field="job_id")
        task_id = queue_layout.QueueLayout.require_durable_record_id(task_id, field="task_id")
        self._store_adapter.initialize()
        with self._lock:
            path = self._storage_root / "tasks" / f"{task_id}.json"
            task = self._store_adapter.read_optional(path, RelayTask)
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
            self._store_adapter.write(path, updated)
            self._store_adapter.write(
                self._job_record_path("tasks_by_job", updated.job_id, updated.task_id),
                updated,
            )
            if updated.sequence is not None:
                self._write_ordered_job_record("task", updated.job_id, updated.sequence, updated)
            self._sync_task_retention_indexes_unlocked(updated)
            self._after_execution_cleanup_canonical_ack(updated)
            pending_path = self._execution_cleanup_path(cluster, job_id, task_id)
            queue_store_write.unlink_durable_path(pending_path, missing_ok=True)
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
        job_id = queue_layout.QueueLayout.require_durable_record_id(job_id, field="job_id")
        task_id = queue_layout.QueueLayout.require_durable_record_id(task_id, field="task_id")
        self._store_adapter.initialize()
        with self._lock:
            path = self._storage_root / "tasks" / f"{task_id}.json"
            task = self._store_adapter.read_optional(path, RelayTask)
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
            self._store_adapter.write(pending_path, updated)
            self._store_adapter.write(path, updated)
            self._store_adapter.write(
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
        job_id = queue_layout.QueueLayout.require_durable_record_id(job_id, field="job_id")
        task_id = queue_layout.QueueLayout.require_durable_record_id(task_id, field="task_id")
        self._store_adapter.initialize()
        with self._lock:
            path = self._storage_root / "tasks" / f"{task_id}.json"
            task = self._store_adapter.read_optional(path, RelayTask)
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
            self._store_adapter.write(path, updated)
            self._store_adapter.write(
                self._job_record_path("tasks_by_job", updated.job_id, updated.task_id),
                updated,
            )
            if updated.sequence is not None:
                self._write_ordered_job_record("task", updated.job_id, updated.sequence, updated)
            self._sync_task_retention_indexes_unlocked(updated)
            self._store_adapter.write(pending_path, updated)
            return updated

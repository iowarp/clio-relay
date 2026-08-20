"""Canonical task and MCP task-projection record ownership.

Owns every public method that creates, reads, pages, scans, or transitions a
``RelayTask``, plus the durable ``RelayMcpTaskRecord`` projection handle the
#234 FastMCP admission/park machinery persists through ``put_mcp_task``,
``update_mcp_task_projection``, and ``get_mcp_task`` (design §5: this owner
does not move FastMCP admission or input parking itself -- only the durable
record boundary those call through). A task write is canonical-plus-derived,
converged through the CQ7 order-index owner's job-index primitives and the
CQ14-scoped ``_sync_task_retention_indexes_unlocked`` active/scheduler-source
convergence.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel

from clio_relay import (
    queue_context,
    queue_index_state,
    queue_layout,
    queue_store_read,
    queue_store_write,
)
from clio_relay.errors import McpTaskIdentityConflictError, NotFoundError, QueueConflictError
from clio_relay.models import (
    TERMINAL_STATES,
    JobState,
    RelayEvent,
    RelayJob,
    RelayMcpTaskProjection,
    RelayMcpTaskRecord,
    RelayTask,
    utc_now,
)
from clio_relay.remote_mcp import VIRTUAL_REMOTE_MCP_RELAY_CONTROL_FIELDS


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


class QueueTasksMixin:
    """Own canonical task and MCP task-projection records: CRUD and writes."""

    _storage_root: Path
    _lock: queue_context.QueueLockProtocol
    _store_adapter: queue_context.QueueStoreProtocol

    if TYPE_CHECKING:

        def get_job(self, job_id: str) -> RelayJob: ...
        def append_event(
            self,
            job_id: str,
            event_type: str,
            message: str,
            *,
            locked: bool = False,
            payload: dict[str, object] | None = None,
        ) -> RelayEvent: ...
        def _recover_pending_transitions_unlocked(self) -> list[RelayJob]: ...
        def _write_transition_intent_unlocked(
            self, kind: str, identity: str, payload: dict[str, object]
        ) -> Path: ...
        def _job_record_path(self, family: str, job_id: str, record_id: str) -> Path: ...
        def _job_index_exists(self, job_id: str) -> bool: ...
        def _initialize_job_index_unlocked(self, job_id: str) -> None: ...
        def _read_job_index(self, job_id: str) -> dict[str, object] | None: ...
        def _update_job_index_unlocked(self, job_id: str, **updates: object) -> None: ...
        def _next_job_record_sequence_unlocked(self, job_id: str, count_field: str) -> int: ...
        def _write_ordered_job_record(
            self, family: str, job_id: str, sequence: int, record: BaseModel
        ) -> None: ...
        def _read_ordered_job_page[Record: BaseModel](
            self,
            job_id: str,
            *,
            family: str,
            model: type[Record],
            cursor: int,
            limit: int,
            count_field: str,
        ) -> tuple[list[Record], int | None, int]: ...
        def _sync_scheduler_source_unlocked(
            self, job_id: str, *, source_id: str, metadata: dict[str, object]
        ) -> None: ...

    def append_task(self, task: RelayTask) -> RelayTask:
        """Create a task record."""
        queue_layout.QueueLayout.require_durable_record_id(task.task_id, field="task_id")
        queue_layout.QueueLayout.require_durable_record_id(task.job_id, field="job_id")
        self._store_adapter.initialize()
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
        queue_layout.QueueLayout.require_durable_record_id(task.task_id, field="task_id")
        queue_layout.QueueLayout.require_durable_record_id(task.job_id, field="job_id")
        self._store_adapter.initialize()
        with self._lock:
            self._recover_pending_transitions_unlocked()
            path = self._storage_root / "mcp_tasks" / f"{task.task_id}.json"
            existing = self._store_adapter.read_optional(path, RelayMcpTaskRecord)
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
            self._store_adapter.write(path, task)
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
        task_id = queue_layout.QueueLayout.require_durable_record_id(task_id, field="task_id")
        self._store_adapter.initialize()
        with self._lock:
            self._recover_pending_transitions_unlocked()
            path = self._storage_root / "mcp_tasks" / f"{task_id}.json"
            task = self._store_adapter.read_optional(path, RelayMcpTaskRecord)
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
            self._store_adapter.write(path, updated)
            return updated

    def get_mcp_task(self, task_id: str) -> RelayMcpTaskRecord:
        """Return one durable MCP task projection by its relay job handle."""
        task_id = queue_layout.QueueLayout.require_durable_record_id(task_id, field="task_id")
        task = self._store_adapter.read_optional(
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
        task_id = queue_layout.QueueLayout.require_durable_record_id(task_id, field="task_id")
        self._store_adapter.initialize()
        with self._lock:
            self._recover_pending_transitions_unlocked()
            path = self._storage_root / "tasks" / f"{task_id}.json"
            task = self._store_adapter.read_optional(path, RelayTask)
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
        task_id = queue_layout.QueueLayout.require_durable_record_id(task_id, field="task_id")
        self._store_adapter.initialize()
        with self._lock:
            self._recover_pending_transitions_unlocked()
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
            self._write_task_unlocked(updated)
            return updated

    def list_tasks(self, job_id: str | None = None) -> list[RelayTask]:
        """Return durable task records, optionally filtered by job id."""
        if job_id is not None:
            job_id = queue_layout.QueueLayout.require_durable_record_id(job_id, field="job_id")
        self._store_adapter.initialize()
        with self._lock:
            self._recover_pending_transitions_unlocked()
            if job_id is not None and self._job_index_exists(job_id):
                tasks = list(
                    queue_store_read.read_many(
                        self._storage_root
                        / "tasks_by_job"
                        / queue_layout.QueueLayout.durable_key(job_id),
                        RelayTask,
                        identity_field="task_id",
                    )
                )
            else:
                tasks = list(
                    queue_store_read.read_many(
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
        job_id = queue_layout.QueueLayout.require_durable_record_id(job_id, field="job_id")
        self._store_adapter.initialize()
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
        job_id = queue_layout.QueueLayout.require_durable_record_id(job_id, field="job_id")
        self._store_adapter.initialize()
        with self._lock:
            self._recover_pending_transitions_unlocked()
            indexed = self._job_index_exists(job_id)
            directory = (
                self._storage_root / "tasks_by_job" / queue_layout.QueueLayout.durable_key(job_id)
                if indexed
                else self._storage_root / "tasks"
            )
            tasks, truncated = queue_store_read.scan_many(
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
        task_id = queue_layout.QueueLayout.require_durable_record_id(task_id, field="task_id")
        task = self._store_adapter.read_optional(
            self._storage_root / "tasks" / f"{task_id}.json", RelayTask
        )
        if task is None:
            raise NotFoundError(f"task not found: {task_id}")
        if task.task_id != task_id:
            raise QueueConflictError(f"canonical task identity mismatch: {task_id}")
        return task

    def _write_task_unlocked(self, task: RelayTask) -> None:
        """Write one task and make its per-job and scheduler indexes replayable."""
        intent_path = self._write_transition_intent_unlocked(
            "task_sync",
            task.task_id,
            {"job_id": task.job_id, "task_id": task.task_id},
        )
        self._store_adapter.write(self._storage_root / "tasks" / f"{task.task_id}.json", task)
        self._sync_task_derived_unlocked(task)
        queue_store_write.unlink_durable_path(intent_path, missing_ok=True)

    def _sync_task_derived_unlocked(self, task: RelayTask) -> None:
        """Converge task indexes and scheduler references from the canonical task."""
        self._initialize_job_index_unlocked(task.job_id)
        self._store_adapter.write(
            self._job_record_path("tasks_by_job", task.job_id, task.task_id),
            task,
        )
        if task.sequence is not None:
            self._write_ordered_job_record("task", task.job_id, task.sequence, task)
            index = self._read_job_index(task.job_id)
            if (
                index is not None
                and queue_index_state.index_integer(index, "task_count") < task.sequence
            ):
                self._update_job_index_unlocked(task.job_id, task_count=task.sequence)
        self._sync_task_retention_indexes_unlocked(task)

    def _sync_task_retention_indexes_unlocked(self, task: RelayTask) -> None:
        active_path = self._job_record_path(
            "active_tasks_by_job",
            task.job_id,
            task.task_id,
        )
        if task.state in TERMINAL_STATES:
            queue_store_write.unlink_durable_path(active_path, missing_ok=True)
        else:
            self._store_adapter.write(active_path, task)
        self._sync_scheduler_source_unlocked(
            task.job_id,
            source_id=f"task:{task.task_id}",
            metadata=task.metadata,
        )

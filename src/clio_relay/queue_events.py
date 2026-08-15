"""Durable job and task event ownership."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import cast

from clio_relay import queue_layout, queue_order_index
from clio_relay.errors import NotFoundError, QueueConflictError, queue_conflict_from_cause
from clio_relay.models import Cursor, RelayEvent, RelayTask, TaskTimelineEvent
from clio_relay.pagination import validate_record_cursor, validate_response_page_limit


class QueueEventsMixin(queue_order_index.QueueOrderIndexMixin):
    """Own durable job and task event behavior for the queue facade."""

    def _get_event_task(self, task_id: str) -> RelayTask:
        root = self._store_adapter.storage_root
        task = self._store_adapter.read_optional(root / "tasks" / f"{task_id}.json", RelayTask)
        if task is None:
            raise NotFoundError(f"task not found: {task_id}")
        return task

    def append_task_event(self, event: TaskTimelineEvent) -> TaskTimelineEvent:
        """Append a structured task timeline event with a per-task sequence."""
        queue_layout.QueueLayout.require_durable_record_id(event.task_id, field="task_id")
        for artifact_id in event.artifact_refs:
            queue_layout.QueueLayout.require_durable_record_id(artifact_id, field="artifact_id")
        self._store_adapter.initialize()
        with self._store_adapter.lock:
            task = self._get_event_task(event.task_id)
            root = self._store_adapter.storage_root
            event_dir = root / "task_events" / event.task_id
            event_dir.mkdir(parents=True, exist_ok=True)
            seq = self._next_task_event_seq(event.task_id, event_dir)
            saved = event.model_copy(update={"seq": seq})
            self._store_adapter.write(event_dir / f"{seq:020d}.json", saved)
            self._store_adapter.write_json(
                root / "task_event_heads" / f"{event.task_id}.json",
                {"task_id": event.task_id, "latest_seq": seq},
            )
            QueueEventsMixin.append_event(
                self,
                task.job_id,
                f"task.timeline.{event.event_type}",
                event.summary,
                locked=True,
                payload={
                    "task_id": event.task_id,
                    "task_event_seq": seq,
                    "event_type": event.event_type,
                    "label": event.label,
                    "status": event.status.value,
                },
            )
            return saved

    def drain_task_events(
        self,
        task_id: str,
        *,
        cursor: int = 1,
        limit: int = 100,
    ) -> tuple[list[TaskTimelineEvent], int]:
        """Drain structured task timeline events from a task cursor."""
        task_id = queue_layout.QueueLayout.require_durable_record_id(task_id, field="task_id")
        cursor = validate_record_cursor(cursor, field_name="task event cursor")
        limit = validate_response_page_limit(limit, field_name="task event limit")
        self._store_adapter.initialize()
        self._get_event_task(task_id)
        root = self._store_adapter.storage_root
        event_dir = root / "task_events" / task_id
        durable_latest_seq = queue_order_index.last_contiguous_sequence(event_dir)
        head_path = root / "task_event_heads" / f"{task_id}.json"
        try:
            raw_head = self._store_adapter.read_json_document(head_path)
        except FileNotFoundError:
            latest_seq = durable_latest_seq
        else:
            if not isinstance(raw_head, dict):
                raise QueueConflictError(f"task event head is not an object: {head_path}")
            head = cast(dict[str, object], raw_head)
            recorded_latest_seq = head.get("latest_seq")
            if (
                head.get("task_id") != task_id
                or isinstance(recorded_latest_seq, bool)
                or not isinstance(recorded_latest_seq, int)
                or recorded_latest_seq < 0
            ):
                raise QueueConflictError(f"invalid task event head identity: {head_path}")
            if recorded_latest_seq > durable_latest_seq:
                raise QueueConflictError(f"task event head exceeds durable records: {task_id}")
            latest_seq = durable_latest_seq
        stop = min(latest_seq + 1, cursor + limit)
        drained: list[TaskTimelineEvent] = []
        for sequence in range(cursor, stop):
            event = self._store_adapter.read_optional(
                event_dir / f"{sequence:020d}.json",
                TaskTimelineEvent,
            )
            if event is None or event.seq != sequence or event.task_id != task_id:
                raise QueueConflictError(
                    f"task event index is missing sequence {sequence}: {task_id}"
                )
            drained.append(event)
        next_cursor = cursor if not drained else drained[-1].seq + 1
        return drained, next_cursor

    def append_event(
        self,
        job_id: str,
        event_type: str,
        message: str,
        *,
        locked: bool = False,
        payload: dict[str, object] | None = None,
    ) -> RelayEvent:
        """Append an event with a per-job monotonic sequence number."""
        job_id = queue_layout.QueueLayout.require_durable_record_id(job_id, field="job_id")
        if locked:
            return self._append_event_unlocked(job_id, event_type, message, payload or {})
        with self._store_adapter.lock:
            return self._append_event_unlocked(job_id, event_type, message, payload or {})

    def drain_events(
        self,
        cursor: Cursor,
        *,
        limit: int = 100,
    ) -> tuple[list[RelayEvent], Cursor]:
        """Drain events from a cursor and return the advanced cursor."""
        queue_layout.QueueLayout.require_durable_record_id(cursor.job_id, field="job_id")
        drained, next_seq = self.read_event_page(
            cursor.job_id,
            next_seq=cursor.next_seq,
            limit=limit,
        )
        return drained, Cursor(job_id=cursor.job_id, next_seq=next_seq)

    def read_event_page(
        self,
        job_id: str,
        *,
        next_seq: int = 1,
        limit: int = 100,
    ) -> tuple[list[RelayEvent], int]:
        """Read one bounded contiguous event page without updating a consumer cursor."""
        job_id = queue_layout.QueueLayout.require_durable_record_id(job_id, field="job_id")
        if next_seq < 1:
            raise ValueError("event sequence must be greater than or equal to 1")
        if limit < 1:
            raise ValueError("event page limit must be greater than or equal to 1")
        self._store_adapter.initialize()
        root = self._store_adapter.storage_root
        event_dir = root / "events" / job_id
        events: list[RelayEvent] = []
        candidate_seq = next_seq
        while len(events) < limit:
            event = self._store_adapter.read_optional(
                event_dir / f"{candidate_seq:020d}.json",
                RelayEvent,
            )
            if event is None:
                break
            if event.job_id != job_id or event.seq != candidate_seq:
                raise QueueConflictError(f"event filename/content identity mismatch: {event_dir}")
            events.append(event)
            candidate_seq += 1
        return events, candidate_seq

    def _append_event_unlocked(
        self,
        job_id: str,
        event_type: str,
        message: str,
        payload: dict[str, object],
    ) -> RelayEvent:
        root = self._store_adapter.storage_root
        event_dir = root / "events" / job_id
        event_dir.mkdir(parents=True, exist_ok=True)
        seq = self._next_event_seq(job_id, event_dir)
        event = RelayEvent(
            job_id=job_id,
            seq=seq,
            event_type=event_type,
            message=message,
            payload=payload,
        )
        self._store_adapter.write(event_dir / f"{seq:020d}.json", event)
        queue_order_index.increment_job_index(
            self._store_adapter,
            job_id,
            "latest_event_seq",
            latest_event_seq=seq,
        )
        return event

    def latest_job_event(self, job_id: str) -> tuple[RelayEvent | None, bool]:
        """Read the exact indexed event head without enumerating the event directory."""
        job_id = queue_layout.QueueLayout.require_durable_record_id(job_id, field="job_id")
        root = self._store_adapter.storage_root
        index = queue_order_index.read_job_index(self._store_adapter, job_id)
        if index is not None:
            latest_seq = queue_order_index.index_integer(index, "latest_event_seq")
            if latest_seq == 0:
                return None, False
            event = self._store_adapter.read_optional(
                root / "events" / job_id / f"{latest_seq:020d}.json",
                RelayEvent,
            )
            if event is None:
                raise QueueConflictError(f"event index points to a missing record: {job_id}")
            if event.job_id != job_id or event.seq != latest_seq:
                raise QueueConflictError(f"event index identity mismatch: {job_id}")
            return event, False
        event_dir = root / "events" / job_id
        latest: RelayEvent | None = None
        for seq in range(1, queue_layout.DEFAULT_EXACT_RECORD_LIMIT + 1):
            event = self._store_adapter.read_optional(event_dir / f"{seq:020d}.json", RelayEvent)
            if event is None:
                return latest, False
            if event.job_id != job_id or event.seq != seq:
                raise QueueConflictError(f"event filename/content identity mismatch: {job_id}")
            latest = event
        last_path = event_dir / f"{queue_layout.DEFAULT_EXACT_RECORD_LIMIT + 1:020d}.json"
        return latest, last_path.exists()

    def _next_event_seq(self, job_id: str, event_dir: Path) -> int:
        index = queue_order_index.read_job_index(self._store_adapter, job_id)
        if index is not None:
            indexed_seq = queue_order_index.index_integer(index, "latest_event_seq")
            candidate = indexed_seq + 1
            while (event_dir / f"{candidate:020d}.json").exists():
                candidate += 1
                if candidate > queue_layout.DEFAULT_EXACT_RECORD_LIMIT + indexed_seq:
                    raise QueueConflictError(f"event head recovery exceeded bound: {job_id}")
            return candidate
        for candidate in range(1, queue_layout.DEFAULT_EXACT_RECORD_LIMIT + 1):
            if not (event_dir / f"{candidate:020d}.json").exists():
                return candidate
        raise QueueConflictError(f"legacy event sequence requires index migration: {job_id}")

    def _next_task_event_seq(self, task_id: str, directory: Path) -> int:
        head_path = self._store_adapter.storage_root / "task_event_heads" / f"{task_id}.json"
        try:
            raw = self._store_adapter.read_json_document(head_path)
        except FileNotFoundError:
            return queue_order_index.last_contiguous_sequence(directory) + 1
        except (OSError, QueueConflictError) as exc:
            message = f"invalid task event head {head_path}"
            log = logging.getLogger(__name__)
            raise queue_conflict_from_cause(message, cause=exc, logger=log) from exc
        if not isinstance(raw, dict):
            raise QueueConflictError(f"task event head is not an object: {head_path}")
        head = cast(dict[str, object], raw)
        latest_seq = head.get("latest_seq")
        if (
            head.get("task_id") != task_id
            or not isinstance(latest_seq, int)
            or isinstance(latest_seq, bool)
            or latest_seq < 0
        ):
            raise QueueConflictError(f"invalid task event head identity: {head_path}")
        candidate = latest_seq + 1
        for _ in range(queue_layout.DEFAULT_EXACT_RECORD_LIMIT):
            if not (directory / f"{candidate:020d}.json").exists():
                return candidate
            candidate += 1
        raise QueueConflictError(f"task event head recovery exceeded bound: {task_id}")

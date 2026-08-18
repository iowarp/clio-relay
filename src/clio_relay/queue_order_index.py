"""Durable global-order and per-job order-index ownership."""

from __future__ import annotations

import hashlib
import logging
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import cast

from pydantic import BaseModel

from clio_relay import (
    pagination,
    queue_context,
    queue_index_state,
    queue_layout,
    queue_store_lock,
)
from clio_relay.errors import NotFoundError, QueueConflictError, queue_conflict_from_cause
from clio_relay.models import ProgressRecord, RelayJob

logger = logging.getLogger(__name__)


def _stable_ref_token(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:32]


def _read_global_record(
    store: queue_context.QueueStoreProtocol,
    path: Path,
    *,
    family: str,
) -> tuple[str, int] | None:
    try:
        raw = store.read_json_document(path)
    except FileNotFoundError:
        return None
    if not isinstance(raw, dict):
        raise QueueConflictError(f"global-order record is not an object: {path}")
    document = cast(dict[str, object], raw)
    record_id = document.get("record_id")
    sequence = document.get("sequence")
    if (
        document.get("schema_version") != queue_layout.GLOBAL_ORDER_INDEX_SCHEMA
        or document.get("family") != family
        or not queue_layout.safe_global_record_id(record_id)
        or isinstance(sequence, bool)
        or not isinstance(sequence, int)
        or sequence < 1
        or sequence >= 2**63
    ):
        raise QueueConflictError(f"invalid global-order record: {path}")
    return cast(str, record_id), sequence


def _read_global_head(store: queue_context.QueueStoreProtocol, family: str) -> int:
    path = store.storage_root / "global_order" / family / "head.json"
    try:
        raw = store.read_json_document(path)
    except FileNotFoundError:
        return 0
    if not isinstance(raw, dict):
        raise QueueConflictError(f"global-order head is not an object: {path}")
    document = cast(dict[str, object], raw)
    latest = document.get("latest_sequence")
    if (
        document.get("schema_version") != queue_layout.GLOBAL_ORDER_INDEX_SCHEMA
        or document.get("family") != family
        or isinstance(latest, bool)
        or not isinstance(latest, int)
        or latest < 1
        or latest >= 2**63
    ):
        raise QueueConflictError(f"invalid global-order head: {path}")
    return latest


def _ensure_global_sequence(
    store: queue_context.QueueStoreProtocol,
    family: str,
    record_id: str,
    sequence: int,
) -> None:
    path = store.storage_root / "global_order" / family / "entries" / f"{sequence:020d}.json"
    existing = _read_global_record(store, path, family=family)
    if existing is not None:
        if existing != (record_id, sequence):
            raise QueueConflictError(f"global-order sequence collision: {family}/{sequence}")
        return
    store.write_json(
        path,
        {
            "schema_version": queue_layout.GLOBAL_ORDER_INDEX_SCHEMA,
            "family": family,
            "record_id": record_id,
            "sequence": sequence,
        },
    )


def ensure_global(
    store: queue_context.QueueStoreProtocol,
    family: str,
    record_id: str,
) -> int:
    """Return one durable global sequence, repairing an interrupted entry write."""
    if family not in queue_store_lock.GLOBAL_ORDER_FAMILIES:
        raise QueueConflictError(f"unsupported global-order family: {family}")
    if not queue_layout.safe_global_record_id(record_id):
        raise QueueConflictError(f"unsafe global-order record id: {record_id!r}")
    root = store.storage_root / "global_order" / family
    mapping_path = root / "by_id" / f"{_stable_ref_token(record_id)}.json"
    mapping = _read_global_record(store, mapping_path, family=family)
    latest = _read_global_head(store, family)
    if mapping is not None:
        mapped_id, sequence = mapping
        if mapped_id != record_id or sequence > latest:
            raise QueueConflictError(
                f"global-order mapping identity mismatch: {family}/{record_id}"
            )
        _ensure_global_sequence(store, family, record_id, sequence)
        return sequence
    if latest >= 2**63 - 1:
        raise QueueConflictError(f"global-order sequence exhausted: {family}")
    sequence = latest + 1
    store.write_json(
        root / "head.json",
        {
            "schema_version": queue_layout.GLOBAL_ORDER_INDEX_SCHEMA,
            "family": family,
            "latest_sequence": sequence,
        },
    )
    document: dict[str, object] = {
        "schema_version": queue_layout.GLOBAL_ORDER_INDEX_SCHEMA,
        "family": family,
        "record_id": record_id,
        "sequence": sequence,
    }
    store.write_json(mapping_path, document)
    _ensure_global_sequence(store, family, record_id, sequence)
    return sequence


def last_contiguous_sequence(directory: Path) -> int:
    """Return the final member of one contiguous one-based record sequence."""
    if not (directory / f"{1:020d}.json").is_file():
        return 0
    low = 1
    high = 2
    while (directory / f"{high:020d}.json").is_file():
        low = high
        high *= 2
        if high > 2**63:
            raise QueueConflictError(f"record sequence exceeds supported range: {directory}")
    while low + 1 < high:
        middle = (low + high) // 2
        if (directory / f"{middle:020d}.json").is_file():
            low = middle
        else:
            high = middle
    return low


def read_job_index(
    store: queue_context.QueueStoreProtocol,
    job_id: str,
) -> dict[str, object] | None:
    """Read and validate one optional per-job order index."""
    key = queue_layout.QueueLayout.durable_key(job_id)
    path = store.storage_root / "job_indexes" / f"{key}.json"
    try:
        raw = store.read_json_document(path)
    except FileNotFoundError:
        return None
    except (OSError, QueueConflictError) as exc:
        message = f"invalid job index {path}"
        raise queue_conflict_from_cause(message, cause=exc, logger=logger) from exc
    if not isinstance(raw, dict):
        raise QueueConflictError(f"job index is not an object: {path}")
    index = cast(dict[str, object], raw)
    if (
        index.get("schema_version") != queue_layout.JOB_INDEX_SCHEMA
        or index.get("job_id") != job_id
    ):
        raise QueueConflictError(f"job index identity mismatch: {path}")
    for field in ("task_count", "artifact_count", "progress_count", "latest_event_seq"):
        queue_index_state.index_integer(index, field)
    latest_progress_id = index.get("latest_progress_id")
    if latest_progress_id is not None and not isinstance(latest_progress_id, str):
        raise QueueConflictError(f"invalid latest_progress_id in {path}")
    return index


def increment_job_index(
    store: queue_context.QueueStoreProtocol,
    job_id: str,
    field: str,
    **updates: object,
) -> None:
    """Increment one existing per-job index field and apply coupled updates."""
    index = read_job_index(store, job_id)
    if index is None:
        return
    index[field] = queue_index_state.index_integer(index, field) + 1
    index.update(updates)
    key = queue_layout.QueueLayout.durable_key(job_id)
    path = store.storage_root / "job_indexes" / f"{key}.json"
    store.write_json(path, index)


class QueueOrderIndexMixin:
    """Own global-order and per-job order-index behavior for the queue facade."""

    _store_adapter: queue_context.QueueStoreProtocol

    def _ensure_global_order_entry_unlocked(self, family: str, record_id: str) -> int:
        return ensure_global(self._store_adapter, family, record_id)

    def _job_submission_order_key_unlocked(self, job: RelayJob) -> tuple[int, datetime, str]:
        sequence = ensure_global(self._store_adapter, "jobs", job.job_id)
        return sequence, job.created_at, job.job_id

    def _read_global_order_page[Record: BaseModel](
        self,
        *,
        family: str,
        model: type[Record],
        identity_field: str,
        cursor: int,
        limit: int,
        predicate: Callable[[Record], bool] | None = None,
    ) -> tuple[list[Record], int | None, int]:
        cursor = pagination.validate_record_cursor(cursor)
        limit = pagination.validate_response_page_limit(limit)
        self._store_adapter.initialize()
        queue_index_state.require_index_migration_complete(self._store_adapter.storage_root)
        latest = _read_global_head(self._store_adapter, family)
        if cursor > latest:
            return [], None, latest
        stop = min(latest + 1, cursor + limit)
        records: list[Record] = []
        root = self._store_adapter.storage_root / "global_order" / family
        for sequence in range(cursor, stop):
            entry = _read_global_record(
                self._store_adapter,
                root / "entries" / f"{sequence:020d}.json",
                family=family,
            )
            if entry is None:
                continue
            record_id, recorded_sequence = entry
            if recorded_sequence != sequence:
                raise QueueConflictError(
                    f"global-order sequence identity mismatch: {family}/{sequence}"
                )
            mapping = _read_global_record(
                self._store_adapter,
                root / "by_id" / f"{_stable_ref_token(record_id)}.json",
                family=family,
            )
            if mapping != entry:
                raise QueueConflictError(
                    f"global-order reverse mapping mismatch: {family}/{record_id}"
                )
            record = self._store_adapter.read_optional(
                self._store_adapter.storage_root / family / f"{record_id}.json",
                model,
            )
            if record is None:
                continue
            if getattr(record, identity_field, None) != record_id:
                raise QueueConflictError(
                    f"global-order target identity mismatch: {family}/{record_id}"
                )
            if predicate is None or predicate(record):
                records.append(record)
        next_cursor = stop if stop <= latest else None
        return records, next_cursor, latest

    def _scan_global_order[Record: BaseModel](
        self,
        *,
        family: str,
        model: type[Record],
        identity_field: str,
        limit: int,
        predicate: Callable[[Record], bool] | None = None,
    ) -> tuple[list[Record], bool]:
        if isinstance(limit, bool):
            raise ValueError("scan limit must be an integer")
        maximum = queue_layout.MAX_BOUNDED_SCAN_RECORDS
        if limit < 1 or limit > maximum:
            raise ValueError(f"scan limit must be between 1 and {maximum}")
        records: list[Record] = []
        cursor = 1
        remaining = limit
        while remaining > 0:
            page_limit = min(remaining, pagination.MAX_RESPONSE_PAGE_RECORDS)
            page, next_cursor, total = self._read_global_order_page(
                family=family,
                model=model,
                identity_field=identity_field,
                cursor=cursor,
                limit=page_limit,
                predicate=predicate,
            )
            records.extend(page)
            remaining -= min(page_limit, max(0, total - cursor + 1))
            if next_cursor is None:
                return records, False
            cursor = next_cursor
        return records, cursor <= _read_global_head(self._store_adapter, family)

    def _finalize_job_index_unlocked(self, job_id: str) -> None:
        self._initialize_job_index_unlocked(job_id)
        root = self._store_adapter.storage_root
        key = queue_layout.QueueLayout.durable_key(job_id)
        task_count = last_contiguous_sequence(root / "task_order_by_job" / key)
        artifact_count = last_contiguous_sequence(root / "artifact_order_by_job" / key)
        progress_count = last_contiguous_sequence(root / "progress_order_by_job" / key)
        latest_progress = (
            self._store_adapter.read_optional(
                root / "progress_order_by_job" / key / f"{progress_count:020d}.json",
                ProgressRecord,
            )
            if progress_count > 0
            else None
        )
        self._update_job_index_unlocked(
            job_id,
            task_count=task_count,
            artifact_count=artifact_count,
            progress_count=progress_count,
            latest_progress_id=None if latest_progress is None else latest_progress.progress_id,
            latest_event_seq=last_contiguous_sequence(root / "events" / job_id),
        )

    def _initialize_job_index_unlocked(self, job_id: str) -> None:
        root = self._store_adapter.storage_root
        key = queue_layout.QueueLayout.durable_key(job_id)
        index_path = root / "job_indexes" / f"{key}.json"
        families = (  # noqa: SIM905
            """tasks_by_job leases_by_job artifacts_by_job used_artifacts_by_job
progress_by_job task_order_by_job artifact_order_by_job progress_order_by_job
active_tasks_by_job scheduler_refs_by_job scheduler_protections_by_job
monitor_rules_by_job active_monitor_rules_by_job active_gateway_refs_by_job"""
        ).split()
        for family in families:
            (root / family / key).mkdir(parents=True, exist_ok=True)
        if index_path.exists():
            return
        self._store_adapter.write_json(
            index_path,
            {
                "schema_version": queue_layout.JOB_INDEX_SCHEMA,
                "order_schema_version": queue_layout.ORDER_INDEX_SCHEMA,
                "retention_schema_version": queue_layout.RETENTION_INDEX_SCHEMA,
                "job_id": job_id,
                "task_count": 0,
                "artifact_count": 0,
                "progress_count": 0,
                "latest_progress_id": None,
                "latest_event_seq": 0,
            },
        )

    def _job_index_exists(self, job_id: str) -> bool:
        key = queue_layout.QueueLayout.durable_key(job_id)
        return (self._store_adapter.storage_root / "job_indexes" / f"{key}.json").is_file()

    def _read_job_index(self, job_id: str) -> dict[str, object] | None:
        return read_job_index(self._store_adapter, job_id)

    def _update_job_index_unlocked(self, job_id: str, **updates: object) -> None:
        index = read_job_index(self._store_adapter, job_id)
        if index is None:
            return
        index.update(updates)
        root = self._store_adapter.storage_root
        key = queue_layout.QueueLayout.durable_key(job_id)
        self._store_adapter.write_json(root / "job_indexes" / f"{key}.json", index)

    def _increment_job_index_unlocked(
        self,
        job_id: str,
        field: str,
        **updates: object,
    ) -> None:
        increment_job_index(self._store_adapter, job_id, field, **updates)

    def _next_job_record_sequence_unlocked(self, job_id: str, count_field: str) -> int:
        index = read_job_index(self._store_adapter, job_id)
        if index is None:
            raise QueueConflictError(f"job order index is missing: {job_id}")
        return queue_index_state.index_integer(index, count_field) + 1

    def _write_ordered_job_record(
        self,
        family: str,
        job_id: str,
        sequence: int,
        record: BaseModel,
    ) -> None:
        root = self._store_adapter.storage_root
        key = queue_layout.QueueLayout.durable_key(job_id)
        path = root / f"{family}_order_by_job" / key / f"{sequence:020d}.json"
        self._store_adapter.write(path, record)

    def _read_ordered_job_page[Record: BaseModel](
        self,
        job_id: str,
        *,
        family: str,
        model: type[Record],
        cursor: int,
        limit: int,
        count_field: str,
    ) -> tuple[list[Record], int | None, int]:
        cursor = pagination.validate_record_cursor(cursor)
        limit = pagination.validate_response_page_limit(limit)
        self._store_adapter.initialize()
        queue_index_state.require_index_migration_complete(self._store_adapter.storage_root)
        index = read_job_index(self._store_adapter, job_id)
        if index is None:
            raise NotFoundError(f"job not found: {job_id}")
        total = queue_index_state.index_integer(index, count_field)
        if cursor > total:
            return [], None, total
        stop = min(total + 1, cursor + limit)
        records: list[Record] = []
        root = self._store_adapter.storage_root
        key = queue_layout.QueueLayout.durable_key(job_id)
        directory = root / f"{family}_order_by_job" / key
        for sequence in range(cursor, stop):
            record = self._store_adapter.read_optional(
                directory / f"{sequence:020d}.json",
                model,
            )
            if record is None:
                raise QueueConflictError(
                    f"{family} order index is missing sequence {sequence}: {job_id}"
                )
            records.append(record)
        next_cursor = stop if stop <= total else None
        return records, next_cursor, total

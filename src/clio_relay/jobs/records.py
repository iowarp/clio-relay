"""Durable operation and ask records.

Every long call is an operation with a record on disk, so any relay process
can answer ``tasks/get``, resume after a restart and submit at most once.
Records are JSON files written atomically under a per-id file lock.
"""

from __future__ import annotations

import json
import os
import secrets
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime
from enum import StrEnum
from pathlib import Path
from typing import Any, Literal

from filelock import FileLock
from pydantic import BaseModel, Field

from clio_relay.jobs.handles import JobHandle, OutputRef, utc_now

OperationKind = Literal["run", "serve", "files"]


class OperationState(StrEnum):
    """Where an operation is. ``submitting`` is the single-submit fence."""

    AWAITING_INPUT = "awaiting_input"
    SUBMITTING = "submitting"
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

    @property
    def final(self) -> bool:
        """Return whether the operation can change state again."""
        return self in (
            OperationState.COMPLETED,
            OperationState.FAILED,
            OperationState.CANCELLED,
        )


class StateChange(BaseModel):
    """One entry of an operation's history."""

    state: OperationState
    at: datetime = Field(default_factory=utc_now)
    note: str | None = None


class OperationRecord(BaseModel):
    """The durable record of one ``run``, ``serve`` or ``files`` call."""

    operation_id: str
    kind: OperationKind
    site: str
    adapter: str
    spec: dict[str, Any]
    spec_digest: str
    input_digests: list[str] = Field(default_factory=list[str])
    handle: JobHandle | None = None
    state: OperationState
    history: list[StateChange] = Field(default_factory=list[StateChange])
    asks: list[str] = Field(default_factory=list[str])
    exit_code: int | None = None
    outputs: list[OutputRef] = Field(default_factory=list[OutputRef])
    result: dict[str, Any] | None = None
    error: dict[str, Any] | None = None
    task: bool = False
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)

    def move(self, state: OperationState, note: str | None = None) -> None:
        """Change state and append to the history. A final state never changes."""
        if self.state.final or state == self.state:
            return
        self.state = state
        self.updated_at = utc_now()
        self.history.append(StateChange(state=state, at=self.updated_at, note=note))


def new_id(prefix: str) -> str:
    """Return a new random id such as ``op_3f9c...``."""
    return f"{prefix}_{secrets.token_hex(12)}"


def _safe_id(record_id: str) -> str:
    if not record_id or any(ch in record_id for ch in "/\\:.") or len(record_id) > 128:
        raise ValueError(f"not a record id: {record_id!r}")
    return record_id


class RecordStore[RecordT: BaseModel]:
    """A directory of JSON records of one model, one file per id."""

    def __init__(self, root: Path, model: type[RecordT]) -> None:
        self.root = root
        self.model = model
        self._locks = root / ".locks"

    def _path(self, record_id: str) -> Path:
        return self.root / f"{_safe_id(record_id)}.json"

    def _ensure(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        self._locks.mkdir(parents=True, exist_ok=True, mode=0o700)

    @contextmanager
    def locked(self, record_id: str, timeout: float = 30.0) -> Iterator[None]:
        """Hold the record's lock, across processes."""
        self._ensure()
        with FileLock(str(self._locks / f"{_safe_id(record_id)}.lock"), timeout=timeout):
            yield

    def get(self, record_id: str) -> RecordT | None:
        """Return the record, or ``None`` if there is none."""
        try:
            path = self._path(record_id)
        except ValueError:
            return None
        try:
            text = path.read_text(encoding="utf-8")
        except FileNotFoundError:
            return None
        return self.model.model_validate_json(text)

    def put(self, record_id: str, record: RecordT) -> None:
        """Write the record atomically. Call under :meth:`locked` when racing is possible."""
        self._ensure()
        path = self._path(record_id)
        fd, tmp = tempfile.mkstemp(prefix=f".{path.name}.", dir=self.root)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                handle.write(record.model_dump_json(indent=2))
                handle.flush()
                os.fsync(handle.fileno())
            os.chmod(tmp, 0o600)
            os.replace(tmp, path)
        except BaseException:
            Path(tmp).unlink(missing_ok=True)
            raise

    def ids(self) -> list[str]:
        """Return every stored id, oldest file first."""
        if not self.root.is_dir():
            return []
        files = sorted(self.root.glob("*.json"), key=lambda item: item.stat().st_mtime)
        return [item.stem for item in files]

    def all(self) -> list[RecordT]:
        """Return every stored record, oldest first."""
        found: list[RecordT] = []
        for record_id in self.ids():
            record = self.get(record_id)
            if record is not None:
                found.append(record)
        return found


def write_json_atomic(path: Path, value: Any) -> None:
    """Write JSON atomically with owner-only permissions."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(value, handle, indent=2, sort_keys=True, default=str)
        os.chmod(tmp, 0o600)
        os.replace(tmp, path)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise

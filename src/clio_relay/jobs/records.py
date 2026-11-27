"""Durable operation and ask records.

Every long call is an operation with a record on disk, so any relay process
can answer ``tasks/get``, resume after a restart and submit at most once.
Records are JSON files written atomically under a per-id file lock.
"""

from __future__ import annotations

import json
import logging
import os
import secrets
import tempfile
from collections.abc import Callable, Generator
from contextlib import ExitStack, contextmanager
from contextvars import ContextVar
from datetime import datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import Any, Literal, cast

from filelock import FileLock, Timeout
from pydantic import BaseModel, Field

from clio_relay.jobs.handles import JobHandle, OutputRef, utc_now
from clio_relay.jobs.ownership import Owner

OperationKind = Literal["run", "serve", "files", "install"]

logger = logging.getLogger(__name__)


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
    owner: Owner | None = None
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)
    """When the state last moved. The submit grace and cut-short checks time from it."""
    changed_at: datetime | None = None
    """When anything a reader sees last changed: a component, a message, the state."""

    @property
    def last_change(self) -> datetime:
        """When the record last changed in a way a reader sees: a job card's revision."""
        if self.changed_at is None or self.changed_at < self.updated_at:
            return self.updated_at
        return self.changed_at

    def note_changes(self, previous: BaseModel | None) -> bool:
        """Stamp :attr:`changed_at` if this differs from ``previous``; say whether it does.

        A look that only finds the job as it was (``observed_at`` and the like)
        is no change. The stamp rises by at least a millisecond, so two changes
        never share a revision.
        """
        if not isinstance(previous, OperationRecord):
            return True
        if _seen(self) == _seen(previous):
            return False
        now = utc_now()
        floor = previous.last_change + timedelta(milliseconds=1)
        self.changed_at = max(now, floor, self.updated_at)
        return True

    def move(self, state: OperationState, note: str | None = None) -> None:
        """Change state and append to the history. A final state never changes."""
        if self.state.final or state == self.state:
            return
        self.state = state
        self.updated_at = utc_now()
        self.history.append(StateChange(state=state, at=self.updated_at, note=note))


_LOOK_KEYS = frozenset({"observed_at", "observation_failed_at"})
"""Result keys every look rewrites; they alone are no change."""


def _seen(record: OperationRecord) -> dict[str, Any]:
    seen = record.model_dump(mode="json", exclude={"updated_at", "changed_at"})
    result = seen.get("result")
    if isinstance(result, dict):
        seen["result"] = {
            key: value
            for key, value in cast(dict[str, Any], result).items()
            if key not in _LOOK_KEYS
        }
    return seen


def new_id(prefix: str) -> str:
    """Return a new random id such as ``op_3f9c...``."""
    return f"{prefix}_{secrets.token_hex(12)}"


def _safe_id(record_id: str) -> str:
    if not record_id or any(ch in record_id for ch in "/\\:.") or len(record_id) > 128:
        raise ValueError(f"not a record id: {record_id!r}")
    return record_id


_LEASES: set[str] = set()
"""The submit leases this process holds, by lock path."""
_HELD: ContextVar[frozenset[str]] = ContextVar("relay_submit_leases", default=frozenset())
"""The submit leases the current call chain holds: only it takes one again."""


class RecordStore[RecordT: BaseModel]:
    """A directory of JSON records of one model, one file per id."""

    def __init__(self, root: Path, model: type[RecordT]) -> None:
        self.root = root
        self.model = model
        self._locks = root / ".locks"
        self.on_change: Callable[[RecordT], None] | None = None
        """Told of each record :meth:`put` writes that changed (see ``note_changes``)."""

    def _path(self, record_id: str) -> Path:
        return self.root / f"{_safe_id(record_id)}.json"

    def _ensure(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        self._locks.mkdir(parents=True, exist_ok=True, mode=0o700)

    @contextmanager
    def locked(self, record_id: str, timeout: float = 30.0) -> Generator[None]:
        """Hold the record's lock, across processes."""
        self._ensure()
        with FileLock(str(self._locks / f"{_safe_id(record_id)}.lock"), timeout=timeout):
            yield

    @contextmanager
    def submitting(self, record_id: str, timeout: float = 0.0) -> Generator[None]:
        """Hold the record's submit lease while this call submits its job.

        The operating system drops the lease when the holder dies, so an
        observer can tell a submission still in hand from one a crash cut short.
        The call chain holding it takes it again (an install that then runs
        resubmits). Anyone else, another coroutine of this process or another
        process, gets :class:`filelock.Timeout`: one submission is in hand, and
        waiting would only block this process's event loop behind it.
        """
        self._ensure()
        path = str(self._locks / f"{_safe_id(record_id)}.submit.lock")
        held = _HELD.get()
        if path in held:
            yield
            return
        if path in _LEASES:
            raise Timeout(path)
        with FileLock(path, timeout=timeout):
            _LEASES.add(path)
            token = _HELD.set(held | {path})
            try:
                yield
            finally:
                _HELD.reset(token)
                _LEASES.discard(path)

    def claim_submit(self, record_id: str) -> ExitStack | None:
        """Take the submit lease for a ``with``, or return None while someone else holds it."""
        stack = ExitStack()
        try:
            stack.enter_context(self.submitting(record_id))
        except Timeout:
            return None
        return stack

    def submit_in_hand(self, record_id: str) -> bool:
        """Say whether a live process, this one or another, holds the submit lease."""
        self._ensure()
        path = str(self._locks / f"{_safe_id(record_id)}.submit.lock")
        if path in _LEASES:
            return True
        lease = FileLock(path, timeout=0)
        try:
            lease.acquire()
        except Timeout:
            return True
        lease.release()
        return False

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
        """Write the record atomically. Call under :meth:`locked` when racing is possible.

        An :class:`OperationRecord` that changed is stamped first, and
        :attr:`on_change` hears of it once it is written.
        """
        self._ensure()
        path = self._path(record_id)
        changed = True
        if isinstance(record, OperationRecord):
            try:
                previous = self.get(record_id)
            except (OSError, ValueError):
                previous = None
            changed = record.note_changes(previous)
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
        if changed and self.on_change is not None:
            try:
                self.on_change(record)
            except Exception:  # a listener must never undo a write
                logger.warning("telling of a change to %s", record_id, exc_info=True)

    def delete(self, record_id: str) -> bool:
        """Remove the record. Return whether it was there."""
        try:
            self._path(record_id).unlink()
        except (FileNotFoundError, ValueError):
            return False
        return True

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

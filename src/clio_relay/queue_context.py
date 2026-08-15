"""Typed storage context shared by extracted core-queue owners."""

from __future__ import annotations

from pathlib import Path
from types import TracebackType
from typing import Protocol, TypeVar

from pydantic import BaseModel

Record = TypeVar("Record", bound=BaseModel)


class QueueLockProtocol(Protocol):
    """Context-manager surface required from the queue's storage lock."""

    def __enter__(self) -> QueueLockProtocol:
        """Acquire the queue storage lock."""
        ...

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Release the queue storage lock."""
        ...


class QueueStoreProtocol(Protocol):
    """Storage operations available to extracted core-queue owners."""

    @property
    def storage_root(self) -> Path:
        """Return the internal filesystem root for durable queue records."""
        ...

    def locked_storage_root(self) -> tuple[int | None, tuple[int, int] | None]:
        """Return the migration-pinned queue-root descriptor and identity."""
        ...

    @property
    def lock(self) -> QueueLockProtocol:
        """Return the shared queue storage lock."""
        ...

    def initialize(self) -> None:
        """Initialize and validate the durable store."""
        ...

    def read_optional(self, path: Path, model: type[Record]) -> Record | None:
        """Read one optional typed record."""
        ...

    def write(self, path: Path, record: BaseModel) -> None:
        """Persist one typed record atomically."""
        ...

    def bounded_regular_json_count(
        self,
        directory: Path,
        *,
        limit: int,
        label: str,
    ) -> tuple[int, bool]:
        """Count bounded regular JSON records without following unsafe entries."""
        ...

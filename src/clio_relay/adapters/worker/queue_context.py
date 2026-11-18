from pathlib import Path
from types import TracebackType
from typing import Callable, Protocol  # noqa: UP035

from pydantic import BaseModel


class QueueLockProtocol(Protocol):
    """Context-manager surface required from the queue's storage lock."""

    def __enter__(self) -> "QueueLockProtocol":
        """Acquire the queue storage lock."""
        ...

    @property
    def is_locked(self) -> bool:
        """Return whether this process currently holds the queue lock."""
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

    locked_storage_root: Callable[[], tuple[int | None, tuple[int, int] | None]]

    @property
    def lock(self) -> QueueLockProtocol:
        """Return the shared queue storage lock."""
        ...

    def initialize(self) -> None:
        """Initialize and validate the durable store."""
        ...

    def read_optional[Record: BaseModel](self, path: Path, model: type[Record]) -> Record | None:
        """Read one optional typed record."""
        ...

    read_json_document: Callable[[Path], object]

    def write(self, path: Path, record: BaseModel) -> None:
        """Persist one typed record atomically."""
        ...

    write_json: Callable[[Path, dict[str, object]], None]

    def bounded_regular_json_count(
        self,
        directory: Path,
        *,
        limit: int,
        label: str,
    ) -> tuple[int, bool]:
        """Count bounded regular JSON records without following unsafe entries."""
        ...

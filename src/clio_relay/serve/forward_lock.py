"""The lock a relay process holds beside a desktop forward it runs (see ``forward_owner``)."""

from __future__ import annotations

import contextlib
from pathlib import Path

from filelock import FileLock, Timeout

OWNER_LOCK = "owner.lock"


class ForwardLock:
    """This process's hold on one forward, taken without waiting."""

    def __init__(self, directory: Path) -> None:
        self.directory = directory
        self._lock: FileLock | None = None

    def held(self) -> bool:
        """Say whether this process holds the forward."""
        return self._lock is not None

    def take(self) -> bool:
        """Hold the forward unless another live relay process does; say whether this one does."""
        if self._lock is not None:
            return True
        lock = FileLock(str(self.directory / OWNER_LOCK), timeout=0, thread_local=False)
        try:
            self.directory.mkdir(parents=True, exist_ok=True)
            lock.acquire()
        except (Timeout, OSError):
            return False
        self._lock = lock
        return True

    def release(self) -> None:
        """Let another relay process take the forward over."""
        if self._lock is not None:
            with contextlib.suppress(OSError):
                self._lock.release()
            self._lock = None

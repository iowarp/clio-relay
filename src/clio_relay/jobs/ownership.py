"""Which relay process owns an operation, and who else may act on it.

Any number of relay processes can share one state directory: one per client, a
CLI, a page server. Records are locked per id, so processes of one version act on
an operation in turn. A process of another version must not: its adapters, pins
and site scripts differ, and an older relay that resubmits a newer one's job
applies its own pins to it.

So each process holds a serve lease for as long as it lives, a file lock under
``operations/.serves`` that the operating system drops when the process dies,
and each operation names its owner: the lease's token and the relay version.
A process acts on an operation when it owns it, when the owner runs the same
version, or when the owner is gone and this relay is the same version or newer
(it then takes the operation over). Otherwise it only reports the record, which
the owner keeps current. A record written before owners were recorded is
taken over by whoever acts on it first.
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import TYPE_CHECKING

from filelock import FileLock, Timeout
from packaging.version import InvalidVersion, Version
from pydantic import BaseModel

if TYPE_CHECKING:
    from clio_relay.jobs.records import OperationRecord, RecordStore

#: A lease file this old with no holder is left by a process that died.
_PRUNE_AFTER_S = 60.0


class Owner(BaseModel):
    """The relay process that owns an operation."""

    serve: str
    version: str


def _at_least(mine: str, theirs: str) -> bool:
    try:
        return Version(mine) >= Version(theirs)
    except InvalidVersion:
        return mine == theirs


class ServeLease:
    """This process's hold on the operations it owns."""

    def __init__(self, root: Path, version: str, token: str) -> None:
        self.root = root
        self.version = version
        self.token = token
        self._lock: FileLock | None = None

    def _path(self, token: str) -> Path:
        if not token or any(ch in token for ch in "/\\:.") or len(token) > 128:
            raise ValueError(f"not a serve token: {token!r}")
        return self.root / f"{token}.lock"

    def owner(self) -> Owner:
        """Return this process as an owner, taking its lease the first time."""
        if self._lock is None:
            self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
            self._prune()
            lock = FileLock(str(self._path(self.token)), timeout=10, thread_local=False)
            lock.acquire()
            self._lock = lock
        return Owner(serve=self.token, version=self.version)

    def release(self) -> None:
        """Drop the lease; the operations it owns can then be taken over."""
        if self._lock is not None:
            self._lock.release()
            self._lock = None
            self._path(self.token).unlink(missing_ok=True)

    def alive(self, token: str) -> bool:
        """Say whether the process holding ``token``'s lease is still running."""
        if token == self.token:
            return self._lock is not None
        try:
            path = self._path(token)
        except ValueError:
            return False
        if not path.exists():
            return False
        probe = FileLock(str(path), timeout=0, thread_local=False)
        try:
            probe.acquire()
        except Timeout:
            return True
        probe.release()
        return False

    def may_act(self, owner: Owner | None) -> bool:
        """Say whether this process may move an operation ``owner`` owns."""
        if owner is None or owner.serve == self.token or owner.version == self.version:
            return True
        if self.alive(owner.serve):
            return False
        return _at_least(self.version, owner.version)

    def takes_over(self, owner: Owner | None) -> bool:
        """Say whether acting on the operation makes this process its owner."""
        if owner is None:
            return True
        return owner.serve != self.token and not self.alive(owner.serve)

    def acts_on(self, store: RecordStore[OperationRecord], record: OperationRecord) -> bool:
        """Say whether this process may move ``record``; take it over if its owner is gone.

        ``record`` gets the owner written, so a caller that writes it back keeps it.
        """
        if not self.may_act(record.owner):
            return False
        if self.takes_over(record.owner):
            with store.locked(record.operation_id):
                stored = store.get(record.operation_id)
                if stored is not None and self.takes_over(stored.owner):
                    stored.owner = self.owner()
                    store.put(record.operation_id, stored)
                record.owner = stored.owner if stored is not None else record.owner
        return True

    def note(self, owner: Owner) -> str:
        """What a process that only reports an operation says about it."""
        running = "running" if self.alive(owner.serve) else "not running"
        return (
            f"relay {owner.version} owns this operation ({running}); "
            f"this relay ({self.version}) only reports it"
        )

    def shown(self, owner: Owner | None, *, final: bool) -> dict[str, str] | None:
        """The owner as an operation view shows it, with a note when this process only reports."""
        if owner is None:
            return None
        view = {"relay": owner.version}
        if not final and not self.may_act(owner):
            view["note"] = self.note(owner)
        return view

    def _prune(self) -> None:
        now = time.time()
        for path in self.root.glob("*.lock"):
            try:
                if now - path.stat().st_mtime < _PRUNE_AFTER_S or self.alive(path.stem):
                    continue
                path.unlink()
            except OSError:
                continue

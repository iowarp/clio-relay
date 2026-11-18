"""Single-use action tokens for the Compute tab (RL32, LQ1).

Every button relay renders carries a token bound to one action and one subject
(a site id, or the adapter an add form is for). The action tools take the
subject from the token, never from the call, and refuse a call without a token,
with a token for another action, or with a token already spent. A token tried
for the wrong action is spent too.

A host may read the tab over one connection and send each action over another,
so each a separate relay process. Tokens therefore live in relay's state
directory, one file per token named by the token's SHA-256, and spending one
claims its file with an atomic rename: of two processes spending the same token,
exactly one wins. Without a directory they live in memory (tests).
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import os
import secrets
import threading
import time
import uuid
from collections import OrderedDict
from dataclasses import dataclass
from pathlib import Path

from clio_relay.jobs.records import write_json_atomic

TTL_S = 12 * 60 * 60
CAPACITY = 4096
_PRUNE_EVERY = 64
_SUFFIX = ".token"


@dataclass(frozen=True)
class Grant:
    """What one token allows."""

    action: str
    subject: str
    expires: float


class ActionTokens:
    """Mints and spends action tokens, at most ``capacity`` at a time."""

    def __init__(
        self, directory: Path | None = None, ttl_s: float = TTL_S, capacity: int = CAPACITY
    ) -> None:
        self._dir = directory
        self._ttl_s = ttl_s
        self._capacity = capacity
        self._grants: OrderedDict[str, Grant] = OrderedDict()
        self._lock = threading.Lock()
        self._minted = 0

    def mint(self, action: str, subject: str = "") -> str:
        """Return a new token for ``action`` on ``subject``."""
        token = secrets.token_urlsafe(24)
        grant = Grant(action, subject, time.time() + self._ttl_s)
        if self._dir is None:
            with self._lock:
                self._grants[token] = grant
                while len(self._grants) > self._capacity:
                    self._grants.popitem(last=False)
            return token
        path = self._path(token)
        write_json_atomic(
            path, {"action": action, "subject": subject, "expires": grant.expires}
        )
        with self._lock:
            self._minted += 1
            prune = self._minted % _PRUNE_EVERY == 0
        if prune:
            self.prune()
        return token

    def spend(self, token: str, action: str) -> Grant | None:
        """Spend ``token`` for ``action``. Return its grant, or None when it allows nothing."""
        if self._dir is None:
            with self._lock:
                grant = self._grants.pop(token, None)
        else:
            grant = self._claim(token)
        if grant is None or grant.action != action or grant.expires < time.time():
            return None
        return grant

    def _path(self, token: str) -> Path:
        assert self._dir is not None
        return self._dir / (hashlib.sha256(token.encode()).hexdigest() + _SUFFIX)

    def _claim(self, token: str) -> Grant | None:
        path = self._path(token)
        claimed = path.with_name(f"{path.name}.{uuid.uuid4().hex}.spent")
        try:
            os.rename(path, claimed)
        except OSError:
            return None
        try:
            data = json.loads(claimed.read_text(encoding="utf-8"))
            return Grant(str(data["action"]), str(data["subject"]), float(data["expires"]))
        except (OSError, ValueError, KeyError, TypeError):
            return None
        finally:
            with contextlib.suppress(OSError):
                claimed.unlink()

    def prune(self) -> None:
        """Drop expired tokens, and the oldest ones beyond capacity."""
        if self._dir is None:
            return
        now = time.time()
        live: list[tuple[float, Path]] = []
        try:
            entries = list(os.scandir(self._dir))
        except OSError:
            return
        for entry in entries:
            path = Path(entry.path)
            try:
                modified = entry.stat().st_mtime
            except OSError:
                continue
            if not entry.name.endswith(_SUFFIX):
                # A claim or a write that never finished.
                if modified < now - 60:
                    with contextlib.suppress(OSError):
                        path.unlink()
                continue
            if modified < now - self._ttl_s:
                with contextlib.suppress(OSError):
                    path.unlink()
                continue
            live.append((modified, path))
        live.sort()
        for _, path in live[: max(0, len(live) - self._capacity)]:
            with contextlib.suppress(OSError):
                path.unlink()

    def __len__(self) -> int:
        if self._dir is None:
            with self._lock:
                return len(self._grants)
        try:
            return sum(1 for item in self._dir.iterdir() if item.name.endswith(_SUFFIX))
        except OSError:
            return 0

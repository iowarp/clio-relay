"""Single-use action tokens for the Compute tab (RL32, LQ1).

Every button relay renders carries a token bound to one action and one subject
(a site id, or the adapter an add form is for). The action tools take the
subject from the token, never from the call, and refuse a call without a token,
with a token for another action, or with a token already spent. A token tried
for the wrong action is spent too. Tokens live in memory: a restarted relay has
spent them all, and the tab's next read renders fresh ones.
"""

from __future__ import annotations

import secrets
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass

TTL_S = 12 * 60 * 60
CAPACITY = 4096


@dataclass(frozen=True)
class Grant:
    """What one token allows."""

    action: str
    subject: str
    expires: float


class ActionTokens:
    """Mints and spends action tokens, at most ``capacity`` at a time."""

    def __init__(self, ttl_s: float = TTL_S, capacity: int = CAPACITY) -> None:
        self._ttl_s = ttl_s
        self._capacity = capacity
        self._grants: OrderedDict[str, Grant] = OrderedDict()
        self._lock = threading.Lock()

    def mint(self, action: str, subject: str = "") -> str:
        """Return a new token for ``action`` on ``subject``."""
        token = secrets.token_urlsafe(24)
        with self._lock:
            self._grants[token] = Grant(action, subject, time.monotonic() + self._ttl_s)
            while len(self._grants) > self._capacity:
                self._grants.popitem(last=False)
        return token

    def spend(self, token: str, action: str) -> Grant | None:
        """Spend ``token`` for ``action``. Return its grant, or None when it allows nothing."""
        with self._lock:
            grant = self._grants.pop(token, None)
        if grant is None or grant.action != action or grant.expires < time.monotonic():
            return None
        return grant

    def __len__(self) -> int:
        with self._lock:
            return len(self._grants)

"""Grant files a host keeps current for relay (HS4, HS4a).

A host such as CLIO writes one file per provider, private to the person, and
replaces it atomically before its tokens expire. Relay reads it through its own
setting, ``--grant-file <provider>=<path>`` or ``CLIO_RELAY_GRANT_FILE_<PROVIDER>``,
and reads it again whenever the file changes. The file may be absent: it exists
only while the host holds a grant.

Format (HS4)::

    {"version": 1, "provider": "globus", "written_at": "...",
     "tokens": [{"resource_server": "...", "access_token": "...",
                 "token_type": "Bearer", "expires_at": "...", "scopes": ["..."]}]}
"""

from __future__ import annotations

import logging
import os
import threading
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field, SecretStr, ValidationError

from clio_relay.identity.base import Token

logger = logging.getLogger(__name__)

GRANT_VERSION = 1


class GrantToken(BaseModel):
    """One token in a grant file."""

    model_config = ConfigDict(extra="ignore")

    resource_server: str | None = None
    access_token: SecretStr
    token_type: str = "Bearer"
    expires_at: datetime | None = None
    scopes: list[str] = Field(default_factory=list[str])

    def token(self) -> Token:
        """Return it as relay's token type."""
        return Token(
            resource_server=self.resource_server,
            access_token=self.access_token,
            token_type=self.token_type,
            expires_at=self.expires_at,
            scopes=list(self.scopes),
            source="grant",
        )


class Grant(BaseModel):
    """A whole grant file."""

    model_config = ConfigDict(extra="ignore")

    version: int
    provider: str
    written_at: datetime | None = None
    tokens: list[GrantToken] = Field(default_factory=list[GrantToken])

    def find(self, resource_server: str | None = None) -> Token | None:
        """Return the live token for ``resource_server``, or the first live one."""
        for item in self.tokens:
            token = item.token()
            if not token.live():
                continue
            if resource_server is None or item.resource_server == resource_server:
                return token
        return None


@dataclass
class _Seen:
    stamp: tuple[int, int, int] | None
    grant: Grant | None
    problem: str | None


def _stamp(path: Path) -> tuple[int, int, int] | None:
    try:
        info = path.stat()
    except OSError:
        return None
    return (info.st_mtime_ns, info.st_size, info.st_ino)


class GrantFiles:
    """The grant files relay was told about, read again whenever one changes."""

    def __init__(self, paths: Mapping[str, Path | str] | None = None) -> None:
        self.paths: dict[str, Path] = {
            name.lower(): Path(value) for name, value in (paths or {}).items()
        }
        self._seen: dict[str, _Seen] = {}
        self._lock = threading.Lock()

    def providers(self) -> list[str]:
        """Return the providers that have a grant file setting."""
        return sorted(self.paths)

    def _load(self, provider: str, path: Path) -> _Seen:
        stamp = _stamp(path)
        if stamp is None:
            return _Seen(stamp=None, grant=None, problem=None)
        try:
            grant = Grant.model_validate_json(path.read_bytes())
        except (OSError, ValidationError, ValueError) as exc:
            # A half-written file is replaced atomically by the host, so the next
            # change brings a good one. Say why without echoing the contents.
            kind = type(exc).__name__
            return _Seen(stamp=stamp, grant=None, problem=f"unreadable grant file ({kind})")
        if grant.version != GRANT_VERSION:
            return _Seen(
                stamp=stamp, grant=None, problem=f"grant file version {grant.version} unknown"
            )
        if grant.provider.lower() != provider:
            return _Seen(
                stamp=stamp,
                grant=None,
                problem=f"grant file is for {grant.provider!r}, not {provider!r}",
            )
        return _Seen(stamp=stamp, grant=grant, problem=None)

    def _current(self, provider: str) -> _Seen:
        path = self.paths.get(provider)
        if path is None:
            return _Seen(stamp=None, grant=None, problem=None)
        stamp = _stamp(path)
        with self._lock:
            seen = self._seen.get(provider)
            if seen is not None and seen.stamp == stamp:
                return seen
        loaded = self._load(provider, path)
        with self._lock:
            self._seen[provider] = loaded
        return loaded

    def read(self, provider: str) -> Grant | None:
        """Return the provider's grant, read again if the file changed."""
        return self._current(provider.lower()).grant

    def problem(self, provider: str) -> str | None:
        """Return why the provider's grant file could not be used, if it could not."""
        return self._current(provider.lower()).problem

    def token(self, provider: str, resource_server: str | None = None) -> Token | None:
        """Return a live token from the provider's grant file."""
        grant = self.read(provider)
        return None if grant is None else grant.find(resource_server)

    def changed(self) -> list[str]:
        """Return the providers whose file changed since the last look, and reread them."""
        changed: list[str] = []
        for provider, path in self.paths.items():
            stamp = _stamp(path)
            with self._lock:
                seen = self._seen.get(provider)
            if seen is not None and seen.stamp == stamp:
                continue
            loaded = self._load(provider, path)
            with self._lock:
                self._seen[provider] = loaded
            if seen is not None:
                changed.append(provider)
        return changed

    @classmethod
    def from_settings(
        cls, settings: Mapping[str, object], env: Mapping[str, str] | None = None
    ) -> GrantFiles:
        """Build from ``settings["grant_files"]``, then ``CLIO_RELAY_GRANT_FILE_<PROVIDER>``."""
        values = os.environ if env is None else env
        prefix = "CLIO_RELAY_GRANT_FILE_"
        paths: dict[str, Path | str] = {
            key[len(prefix) :].lower(): value
            for key, value in values.items()
            if key.startswith(prefix) and value
        }
        given = settings.get("grant_files")
        if isinstance(given, Mapping):
            for name, value in given.items():  # pyright: ignore[reportUnknownVariableType]
                if isinstance(name, str) and isinstance(value, (str, Path)):
                    paths[name.lower()] = value
        return cls(paths)


class GrantWatcher:
    """Looks at the grant files every few seconds and reports the ones that changed.

    This is change detection only: it never ends a sign-in or a wait.
    """

    def __init__(
        self,
        grants: GrantFiles,
        on_change: Callable[[str], None],
        *,
        interval_s: float = 5.0,
    ) -> None:
        self.grants = grants
        self.on_change = on_change
        self.interval_s = interval_s
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        """Start watching, once. A watcher with no grant files does nothing."""
        if self._thread is not None or not self.grants.paths:
            return
        self.grants.changed()
        self._thread = threading.Thread(target=self._loop, name="relay-grants", daemon=True)
        self._thread.start()

    def _loop(self) -> None:
        while not self._stop.wait(self.interval_s):
            for provider in self.grants.changed():
                try:
                    self.on_change(provider)
                except Exception:
                    logger.exception("reacting to the %s grant change failed", provider)

    def stop(self) -> None:
        """Stop watching."""
        self._stop.set()
        thread, self._thread = self._thread, None
        if thread is not None:
            thread.join(timeout=self.interval_s + 1)

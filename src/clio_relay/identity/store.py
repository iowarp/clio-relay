"""Relay's own secrets: the operating system's keyring, or a private file (RL28).

Each entry is one JSON document under the service ``clio-relay``. A sign-in
relay did itself keeps its tokens here. A cloud key the person typed on relay's
page is kept here too. A grant file from the host is never copied in.

Relay uses the keyring when ``keyring`` has a usable backend. When it has none
(a headless machine with no Secret Service, or ``keyring``'s ``fail`` or
``null`` backend), relay keeps the entries in ``credentials.json`` in its own
configuration directory, readable by its owner only (mode 0600). ``describe``
and ``clio-relay asks identities`` say which one is in use.

``CLIO_RELAY_KEYRING`` picks one outright: ``keyring``, ``file``, or ``memory``,
which keeps entries in this process only, for tests. Unset, relay chooses as
above. The keyring itself honours ``PYTHON_KEYRING_BACKEND``.
"""

from __future__ import annotations

import contextlib
import json
import logging
import os
import stat
import sys
import tempfile
import threading
from collections.abc import Generator
from pathlib import Path
from typing import Any, Literal, Protocol

from clio_relay.jobs.refusals import Refusal, RefusalReason

logger = logging.getLogger(__name__)

SERVICE = "clio-relay"
FILE_NAME = "credentials.json"
# keyring's backends that store nothing: relay falls back to its file instead.
_UNUSABLE = {"keyring.backends.fail.Keyring", "keyring.backends.null.Keyring"}

type StoreKind = Literal["keyring", "file", "memory"]


class _Backend(Protocol):
    def get(self, name: str) -> str | None: ...

    def set(self, name: str, value: str) -> None: ...

    def delete(self, name: str) -> None: ...


class _MemoryBackend:
    def __init__(self) -> None:
        self._values: dict[str, str] = {}
        self._lock = threading.Lock()

    @property
    def values(self) -> dict[str, str]:
        return self._values

    def get(self, name: str) -> str | None:
        with self._lock:
            return self._values.get(name)

    def set(self, name: str, value: str) -> None:
        with self._lock:
            self._values[name] = value

    def delete(self, name: str) -> None:
        with self._lock:
            self._values.pop(name, None)


def _keyring_module() -> Any:
    try:
        import keyring  # pyright: ignore[reportMissingImports]
    except ImportError:
        return None
    return keyring  # pyright: ignore[reportUnknownVariableType]


def keyring_usable(service: str = SERVICE) -> str | None:
    """Return why the keyring cannot hold relay's entries, or ``None`` when it can.

    It reads one entry that does not exist, so it writes nothing.
    """
    module = _keyring_module()
    if module is None:
        return "the keyring package is not installed"
    try:
        backend: object = module.get_keyring()
        name = f"{type(backend).__module__}.{type(backend).__qualname__}"
        if name in _UNUSABLE:
            return f"the keyring has no usable backend ({name})"
        module.get_password(service, "clio-relay-probe")
    except Exception as exc:
        return f"the keyring refused: {type(exc).__name__}"
    return None


class _KeyringBackend:
    def __init__(self, service: str) -> None:
        self.service = service

    def _call(self, method: str, *args: str) -> Any:
        module = _keyring_module()
        if module is None:
            raise Refusal(
                RefusalReason.ADAPTER_UNAVAILABLE,
                "relay keeps its own sign-ins in the keyring, and the keyring package "
                "is not installed",
            )
        try:
            return getattr(module, method)(self.service, *args)
        except Exception as exc:
            if type(exc).__name__ == "PasswordDeleteError":
                return None
            raise Refusal(
                RefusalReason.ADAPTER_UNAVAILABLE,
                f"the keyring refused: {type(exc).__name__}. Set CLIO_RELAY_KEYRING=file "
                "to keep relay's sign-ins in a private file instead",
            ) from exc

    def get(self, name: str) -> str | None:
        value = self._call("get_password", name)
        return value if isinstance(value, str) else None

    def set(self, name: str, value: str) -> None:
        self._call("set_password", name, value)

    def delete(self, name: str) -> None:
        self._call("delete_password", name)


_file_locks: dict[Path, threading.Lock] = {}
_file_locks_guard = threading.Lock()


class _FileBackend:
    """``<config_dir>/credentials.json``, mode 0600, replaced whole on each write."""

    def __init__(self, path: Path, service: str) -> None:
        self.path = path
        self.service = service
        with _file_locks_guard:
            self._lock = _file_locks.setdefault(path, threading.Lock())

    @contextlib.contextmanager
    def _locked(self) -> Generator[None]:
        """Hold the file against this process's threads and, on POSIX, other processes."""
        with self._lock:
            self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            if sys.platform == "win32":
                yield
                return
            import fcntl

            lock = self.path.with_name(f".{self.path.name}.lock")
            descriptor = os.open(lock, os.O_RDWR | os.O_CREAT, 0o600)
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX)
                yield
            finally:
                os.close(descriptor)

    def _read(self) -> dict[str, dict[str, str]]:
        try:
            text = self.path.read_text(encoding="utf-8")
        except FileNotFoundError:
            return {}
        except OSError as exc:
            raise self._refusal(f"cannot read it: {type(exc).__name__}") from exc
        self._tighten()
        try:
            data: object = json.loads(text) if text.strip() else {}
        except ValueError as exc:
            raise self._refusal("it is not valid JSON") from exc
        if not isinstance(data, dict):
            raise self._refusal("it is not a JSON object")
        found: dict[str, dict[str, str]] = {}
        for service, entries in data.items():  # pyright: ignore[reportUnknownVariableType]
            if isinstance(service, str) and isinstance(entries, dict):
                found[service] = {
                    str(key): value  # pyright: ignore[reportUnknownArgumentType]
                    for key, value in entries.items()  # pyright: ignore[reportUnknownVariableType]
                    if isinstance(value, str)
                }
        return found

    def _tighten(self) -> None:
        if sys.platform == "win32":
            return
        with contextlib.suppress(OSError):
            mode = stat.S_IMODE(self.path.stat().st_mode)
            if mode & 0o077:
                os.chmod(self.path, 0o600)
                logger.warning("clio-relay: %s was readable by others; set it to 0600", self.path)

    def _write(self, data: dict[str, dict[str, str]]) -> None:
        text = json.dumps(data, sort_keys=True, indent=2)
        # mkstemp makes the file 0600, so no other user can read it at any point.
        descriptor, temporary = tempfile.mkstemp(
            dir=self.path.parent, prefix=f".{self.path.name}.", suffix=".tmp"
        )
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
                handle.write(text)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, self.path)
        except OSError as exc:
            with contextlib.suppress(OSError):
                os.unlink(temporary)
            raise self._refusal(f"cannot write it: {type(exc).__name__}") from exc

    def _refusal(self, why: str) -> Refusal:
        return Refusal(
            RefusalReason.ADAPTER_UNAVAILABLE,
            f"relay keeps its sign-ins in {self.path}, and {why}",
            path=str(self.path),
        )

    def get(self, name: str) -> str | None:
        # A write replaces the file whole, so a read needs no lock and creates nothing.
        return self._read().get(self.service, {}).get(name)

    def set(self, name: str, value: str) -> None:
        with self._locked():
            data = self._read()
            data.setdefault(self.service, {})[name] = value
            self._write(data)

    def delete(self, name: str) -> None:
        with self._locked():
            data = self._read()
            entries = data.get(self.service, {})
            if name not in entries:
                return
            del entries[name]
            if not entries:
                data.pop(self.service, None)
            self._write(data)


_shared_memory = _MemoryBackend()


def _default_config_dir() -> Path:
    from clio_relay.surface.settings import RelayPaths

    return RelayPaths.from_env().config_dir


class TokenStore:
    """JSON entries in the keyring or relay's private file. Nothing here is ever logged.

    ``backend`` is ``keyring``, ``file`` or ``memory``. ``None`` reads
    ``CLIO_RELAY_KEYRING`` and, when that is unset too, uses the keyring if it
    has a usable backend and the file otherwise. The file lives in
    ``config_dir``, which defaults to relay's configuration directory.
    """

    def __init__(
        self,
        backend: str | None = None,
        *,
        service: str = SERVICE,
        config_dir: Path | str | None = None,
    ) -> None:
        self.service = service
        self._choice = (backend or os.environ.get("CLIO_RELAY_KEYRING") or "auto").lower()
        self._config_dir = None if config_dir is None else Path(config_dir)
        self._resolved: tuple[StoreKind, _Backend] | None = None
        self._lock = threading.Lock()
        self.fallback_reason: str | None = None

    @property
    def path(self) -> Path:
        """Where the file is, whether or not relay uses it."""
        return (self._config_dir or _default_config_dir()) / FILE_NAME

    def _resolve(self) -> tuple[StoreKind, _Backend]:
        with self._lock:
            if self._resolved is None:
                self._resolved = self._choose()
            return self._resolved

    def _choose(self) -> tuple[StoreKind, _Backend]:
        if self._choice == "memory":
            return "memory", _shared_memory
        if self._choice == "file":
            return "file", _FileBackend(self.path, self.service)
        if self._choice == "keyring":
            return "keyring", _KeyringBackend(self.service)
        why = keyring_usable(self.service)
        if why is None:
            return "keyring", _KeyringBackend(self.service)
        self.fallback_reason = why
        logger.info("clio-relay: %s; keeping sign-ins in %s", why, self.path)
        return "file", _FileBackend(self.path, self.service)

    @property
    def kind(self) -> StoreKind:
        """``keyring``, ``file`` or ``memory``: where entries are kept."""
        return self._resolve()[0]

    @property
    def source(self) -> Literal["keyring", "file"]:
        """The token source a status reports for relay's own sign-ins."""
        return "file" if self.kind == "file" else "keyring"

    def location(self) -> dict[str, str | None]:
        """Where relay keeps credentials, for ``describe``. No entry is read."""
        kind = self.kind
        return {
            "stored_in": kind,
            "path": str(self.path) if kind == "file" else None,
            "why": self.fallback_reason if kind == "file" else None,
        }

    @property
    def values(self) -> dict[str, str]:
        """This process's memory entries, for tests. Empty unless ``kind`` is ``memory``."""
        kind, backend = self._resolve()
        return backend.values if kind == "memory" and isinstance(backend, _MemoryBackend) else {}

    def get(self, name: str) -> dict[str, Any] | None:
        """Return an entry, or ``None``. A store that cannot be read reads as empty."""
        try:
            text = self._resolve()[1].get(name)
        except Refusal as refusal:
            logger.warning("clio-relay: could not read a sign-in: %s", refusal.message)
            return None
        if not text:
            return None
        try:
            value: object = json.loads(text)
        except ValueError:
            return None
        if isinstance(value, dict):
            return value  # pyright: ignore[reportUnknownVariableType]
        return None

    def set(self, name: str, value: dict[str, Any]) -> None:
        """Write an entry. Refuses with ``adapter_unavailable`` when the store cannot."""
        self._resolve()[1].set(name, json.dumps(value, sort_keys=True, default=str))

    def delete(self, name: str) -> None:
        """Remove an entry. A missing one is fine."""
        self._resolve()[1].delete(name)

    def secret(self, name: str) -> str | None:
        """Return a secret the person typed on relay's page, such as a cloud key."""
        entry = self.get(f"secret:{name}")
        value = None if entry is None else entry.get("value")
        return value if isinstance(value, str) else None

    def set_secret(self, name: str, value: str) -> None:
        """Keep a secret the person typed."""
        self.set(f"secret:{name}", {"value": value})

    def delete_secret(self, name: str) -> None:
        """Forget a secret."""
        self.delete(f"secret:{name}")

"""Relay's own secrets, in the operating system's keyring under relay's name (RL28).

Each entry is one JSON document under the service ``clio-relay``. A sign-in
relay did itself keeps its tokens here. A cloud key the person typed on relay's
page is kept here too. A grant file from the host is never copied in.

``CLIO_RELAY_KEYRING=memory`` keeps entries in this process only, for tests. Any
other value leaves the choice to ``keyring``, which honours
``PYTHON_KEYRING_BACKEND``.
"""

from __future__ import annotations

import json
import os
import threading
from typing import Any, Protocol

from clio_relay.jobs.refusals import Refusal, RefusalReason

SERVICE = "clio-relay"


class _Backend(Protocol):
    def get(self, name: str) -> str | None: ...

    def set(self, name: str, value: str) -> None: ...

    def delete(self, name: str) -> None: ...


class _MemoryBackend:
    def __init__(self) -> None:
        self._values: dict[str, str] = {}
        self._lock = threading.Lock()

    def get(self, name: str) -> str | None:
        with self._lock:
            return self._values.get(name)

    def set(self, name: str, value: str) -> None:
        with self._lock:
            self._values[name] = value

    def delete(self, name: str) -> None:
        with self._lock:
            self._values.pop(name, None)


class _KeyringBackend:
    def __init__(self, service: str) -> None:
        self.service = service

    def _module(self) -> Any:
        try:
            import keyring  # pyright: ignore[reportMissingImports]
        except ImportError as exc:
            raise Refusal(
                RefusalReason.ADAPTER_UNAVAILABLE,
                "relay keeps its own sign-ins in the keyring, and the keyring package "
                "is not installed",
            ) from exc
        return keyring  # pyright: ignore[reportUnknownVariableType]

    def _call(self, method: str, *args: str) -> Any:
        module = self._module()
        try:
            return getattr(module, method)(self.service, *args)
        except Exception as exc:
            if type(exc).__name__ == "PasswordDeleteError":
                return None
            raise Refusal(
                RefusalReason.ADAPTER_UNAVAILABLE,
                f"the keyring refused: {type(exc).__name__}. On a machine with no "
                "keyring service, set PYTHON_KEYRING_BACKEND to a file backend",
            ) from exc

    def get(self, name: str) -> str | None:
        value = self._call("get_password", name)
        return value if isinstance(value, str) else None

    def set(self, name: str, value: str) -> None:
        self._call("set_password", name, value)

    def delete(self, name: str) -> None:
        self._call("delete_password", name)


_shared_memory = _MemoryBackend()


class TokenStore:
    """JSON entries in the keyring. Nothing here is ever logged."""

    def __init__(self, backend: str | None = None, *, service: str = SERVICE) -> None:
        choice = backend or os.environ.get("CLIO_RELAY_KEYRING") or "keyring"
        self.kind = "memory" if choice == "memory" else "keyring"
        self._backend: _Backend = (
            _shared_memory if self.kind == "memory" else _KeyringBackend(service)
        )

    def get(self, name: str) -> dict[str, Any] | None:
        """Return an entry, or ``None``."""
        text = self._backend.get(name)
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
        """Write an entry."""
        self._backend.set(name, json.dumps(value, sort_keys=True, default=str))

    def delete(self, name: str) -> None:
        """Remove an entry. A missing one is fine."""
        self._backend.delete(name)

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

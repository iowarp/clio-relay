"""The identity provider table (RL30).

One sign-in unlocks several sites. Each provider is registered by name with a
factory that takes an :class:`IdentityContext`. Relay ships ``globus`` and
``github``; ``tacc`` lives in ``identity/tacc.py`` (W10) and is skipped while
that module is absent. Another distribution adds a provider through the entry
point group ``clio_relay.identities``, or in process with :func:`register`.

A site names its identity in ``sites.yaml`` (``identity: globus``). After a
sign-in or a grant change, relay probes the sites that name that identity.
"""

from __future__ import annotations

import importlib
import logging
import threading
from collections.abc import Callable
from importlib.metadata import entry_points

from clio_relay.identity.base import (
    IdentityContext,
    IdentityProvider,
    IdentityStatus,
    SignInContext,
    SignInResult,
    SignInStep,
    Token,
)
from clio_relay.identity.grants import Grant, GrantFiles, GrantWatcher
from clio_relay.identity.store import TokenStore
from clio_relay.jobs.refusals import Refusal, RefusalReason

__all__ = [
    "IDENTITY_GROUP",
    "Grant",
    "GrantFiles",
    "GrantWatcher",
    "IdentityContext",
    "IdentityFactory",
    "IdentityProvider",
    "IdentityStatus",
    "Identities",
    "SignInContext",
    "SignInResult",
    "SignInStep",
    "Token",
    "TokenStore",
    "get",
    "names",
    "register",
    "release",
    "shared",
    "use",
]

logger = logging.getLogger(__name__)

IDENTITY_GROUP = "clio_relay.identities"

type IdentityFactory = Callable[[IdentityContext], IdentityProvider]

# Built in, loaded on first use. A module that is not installed is skipped.
_BUILTIN: dict[str, str] = {
    "globus": "clio_relay.identity.globus:create",
    "github": "clio_relay.identity.github:create",
    "tacc": "clio_relay.identity.tacc:TaccIdentity",
}

_registered: dict[str, IdentityFactory] = {}
_lock = threading.Lock()


def register(name: str, factory: IdentityFactory | None) -> None:
    """Register a provider in this process. ``None`` removes it."""
    with _lock:
        if factory is None:
            _registered.pop(name.lower(), None)
        else:
            _registered[name.lower()] = factory


def _load(target: str, name: str) -> IdentityFactory | None:
    module_name, _, attribute = target.partition(":")
    try:
        module = importlib.import_module(module_name)
    except ModuleNotFoundError as exc:
        if exc.name is not None and (
            module_name == exc.name or module_name.startswith(f"{exc.name}.")
        ):
            return None
        raise
    loaded: object = getattr(module, attribute, None)
    if not callable(loaded):
        raise Refusal(
            RefusalReason.INVALID_CONFIG,
            f"identity provider {name!r} points at {target!r}, which is not callable",
        )
    return loaded  # pyright: ignore[reportReturnType]


def _entry_points() -> dict[str, str]:
    return {point.name.lower(): point.value for point in entry_points(group=IDENTITY_GROUP)}


def get(name: str) -> IdentityFactory | None:
    """Return the factory for ``name``: registered, then an entry point, then built in."""
    key = name.lower()
    with _lock:
        if key in _registered:
            return _registered[key]
    target = _entry_points().get(key) or _BUILTIN.get(key)
    if target is None:
        return None
    return _load(target, key)


def names() -> list[str]:
    """Return every provider that can be built here."""
    candidates = set(_BUILTIN) | set(_entry_points())
    with _lock:
        candidates |= set(_registered)
    return sorted(name for name in candidates if get(name) is not None)


class Identities:
    """The providers relay has built, sharing one store and one set of grant files."""

    def __init__(self, context: IdentityContext) -> None:
        self.context = context
        self._built: dict[str, IdentityProvider] = {}
        self._lock = threading.Lock()

    def get(self, name: str) -> IdentityProvider | None:
        """Return the provider, built once, or ``None`` if there is no such provider."""
        key = name.lower()
        with self._lock:
            if key in self._built:
                return self._built[key]
        factory = get(key)
        if factory is None:
            return None
        provider = factory(self.context)
        with self._lock:
            return self._built.setdefault(key, provider)

    def names(self) -> list[str]:
        """Return every provider that can be built here."""
        return names()

    async def statuses(self) -> list[IdentityStatus]:
        """Return every provider's status."""
        found: list[IdentityStatus] = []
        for name in self.names():
            provider = self.get(name)
            if provider is None:
                continue
            try:
                found.append(await provider.status())
            except Refusal as refusal:
                found.append(
                    IdentityStatus(
                        provider=name,
                        title=getattr(provider, "title", name),
                        state="signed_out",
                        detail=refusal.message,
                    )
                )
        return found


_shared: Identities | None = None


def use(identities: Identities | None) -> None:
    """Make ``identities`` the one :func:`shared` returns. Relay's ask provider calls this."""
    global _shared
    with _lock:
        _shared = identities


def release(identities: Identities) -> None:
    """Stop :func:`shared` returning ``identities``, if it still does."""
    global _shared
    with _lock:
        if _shared is identities:
            _shared = None


def shared() -> Identities:
    """Return the identities relay is serving with, for an adapter that needs a token.

    Inside a serving relay this is the ask provider's, so it sees ``--grant-file``.
    Elsewhere it is built once from ``CLIO_RELAY_GRANT_FILE_<PROVIDER>`` and the
    keyring.
    """
    global _shared
    with _lock:
        if _shared is not None:
            return _shared
    from clio_relay.surface.settings import RelayPaths

    grants = GrantFiles.from_settings({"grant_files": dict(RelayPaths.from_env().grant_files)})
    built = Identities(IdentityContext(store=TokenStore(), grants=grants))
    with _lock:
        if _shared is None:
            _shared = built
        return _shared

"""Globus credentials for the adapters that use them (``globus`` and ``iri``).

Relay's identity layer, ``clio_relay.identity``, owns sign-in. ``get("globus")``
returns a provider whose ``token(resource_server=...)`` returns a token or
``None``, reading the grant file first and then the keyring, and refreshing as
needed. Adapters never start a sign-in. With no token they refuse with
``needs_sign_in`` and relay's sign-in flow takes over.

Until the identity layer is installed, :class:`GrantFileProvider` stands in. It
reads the grant file a host keeps current (``CLIO_RELAY_GRANT_FILE_GLOBUS``),
whose format is ``{version, provider, written_at, tokens: [{resource_server,
access_token, token_type, expires_at, scopes}]}``.
"""

from __future__ import annotations

import importlib
import inspect
import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol, cast

from clio_relay.jobs.refusals import Refusal, RefusalReason

COMPUTE_RESOURCE_SERVER = "funcx_service"
COMPUTE_SCOPE = "https://auth.globus.org/scopes/facd7ccc-c5f4-42aa-916b-a0e270e2c2a9/all"
TRANSFER_RESOURCE_SERVER = "transfer.api.globus.org"
TRANSFER_SCOPE = "urn:globus:auth:scope:transfer.api.globus.org:all"


@dataclass(frozen=True)
class Token:
    """An access token for one resource server."""

    resource_server: str
    access_token: str
    token_type: str = "Bearer"
    expires_at: datetime | None = None
    scopes: tuple[str, ...] = field(default_factory=tuple[str, ...])

    def expired(self, now: datetime | None = None) -> bool:
        """Return whether the token is past its expiry."""
        if self.expires_at is None:
            return False
        return (now or datetime.now(UTC)) >= self.expires_at


class TokenProvider(Protocol):
    """The shape of an identity provider, as relay's identity layer publishes it."""

    async def token(self, *, resource_server: str) -> object | None:
        """Return a token for ``resource_server``, or ``None`` when there is none."""
        ...

    def status(self) -> object:
        """Return ``signed_in``, ``signed_out`` or ``expired``."""
        ...


def _expiry(value: object) -> datetime | None:
    if isinstance(value, int | float):
        return datetime.fromtimestamp(float(value), UTC)
    if isinstance(value, str) and value:
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
    return None


class GrantFileProvider:
    """Reads a host's grant file. Stands in until relay's identity layer is installed."""

    def __init__(self, provider: str, path: Path | None = None) -> None:
        self.provider = provider
        self.path = path

    def _path(self) -> Path | None:
        if self.path is not None:
            return self.path
        from clio_relay.surface.settings import RelayPaths

        return RelayPaths.from_env().grant_files.get(self.provider)

    def _tokens(self) -> list[Token]:
        path = self._path()
        if path is None or not path.is_file():
            return []
        try:
            loaded: Any = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return []
        if not isinstance(loaded, dict):
            return []
        entries: Any = cast(dict[str, Any], loaded).get("tokens", [])
        tokens: list[Token] = []
        for raw in cast(list[Any], entries if isinstance(entries, list) else []):
            if not isinstance(raw, dict):
                continue
            item = cast(dict[str, Any], raw)
            server, access = item.get("resource_server"), item.get("access_token")
            if not isinstance(server, str) or not isinstance(access, str) or not access:
                continue
            scopes: Any = item.get("scopes") or []
            tokens.append(
                Token(
                    resource_server=server,
                    access_token=access,
                    token_type=str(item.get("token_type") or "Bearer"),
                    expires_at=_expiry(item.get("expires_at")),
                    scopes=tuple(str(scope) for scope in cast(list[Any], scopes)),
                )
            )
        return tokens

    async def token(self, *, resource_server: str) -> Token | None:
        for token in self._tokens():
            if token.resource_server == resource_server and not token.expired():
                return token
        return None

    def status(self) -> str:
        tokens = self._tokens()
        if not tokens:
            return "signed_out"
        if all(token.expired() for token in tokens):
            return "expired"
        return "signed_in"


def provider(name: str = "globus") -> TokenProvider:
    """Return relay's identity provider ``name``, or the grant-file stand-in."""
    try:
        module = importlib.import_module("clio_relay.identity")
    except ImportError:
        return GrantFileProvider(name)
    getter = getattr(module, "get", None)
    if not callable(getter):
        return GrantFileProvider(name)
    return cast(TokenProvider, getter(name))


def access_token(token: object | None) -> str | None:
    """Return the access token string from whatever the provider returned."""
    if token is None:
        return None
    if isinstance(token, str):
        return token or None
    value = getattr(token, "access_token", None)
    return value if isinstance(value, str) and value else None


async def status_of(source: TokenProvider) -> str:
    """Return the provider's status as a plain string."""
    value: object = source.status()
    if inspect.isawaitable(value):
        value = await cast(Any, value)
    text = getattr(value, "value", value)
    return str(text)


async def bearer(
    source: TokenProvider, resource_server: str, *, what: str, provider: str = "globus"
) -> str:
    """Return an access token for ``resource_server`` or refuse with ``needs_sign_in``."""
    token = access_token(await source.token(resource_server=resource_server))
    if token is None:
        raise Refusal(
            RefusalReason.NEEDS_SIGN_IN,
            f"sign in to {provider.capitalize()} so relay can reach {what}",
            provider=provider,
            resource_server=resource_server,
        )
    return token

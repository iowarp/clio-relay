"""The TACC identity (RL48): the Tapis device code grant, kept in the keyring.

The person signs in on TACC's own ``https`` page, never on relay's. Relay asks
the Tapis authenticator for a device code (``authenticator.generate_device_code``),
shows the person the verification address and the short user code, and polls
``authenticator.create_token`` until the tokens arrive. The device code and the
tokens are kept in the operating system's keyring under relay's name and are
never shown, logged or returned to the model. The user code is meant to be
read by the person (RFC 8628) and is not a secret.

Relay refreshes the access token on its own with the refresh grant, which needs
the OAuth client's key. When the refresh is refused, the identity is signed
out and the next call asks again.

This module implements the provider interface W6 publishes in
``clio_relay.identity``. Until that package is merged, the types it uses are
mirrored here with the same names and fields.
"""

from __future__ import annotations

import base64
import json
import os
import threading
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Literal, Protocol, cast, runtime_checkable

from pydantic import BaseModel, ConfigDict, Field, SecretStr

from clio_relay.jobs.refusals import Refusal, RefusalReason

PROVIDER = "tacc"
TITLE = "TACC"
KEYRING_SERVICE = "clio-relay"
DEFAULT_BASE_URL = "https://tacc.tapis.io"
DEFAULT_TENANT = "tacc"
# Relay's OAuth client in the tacc tenant (owner blocker O4). Until it exists the
# id and key come from the settings or from CLIO_RELAY_TACC_CLIENT_ID and _KEY.
SHIPPED_CLIENT_ID: str | None = None
SHIPPED_CLIENT_KEY: str | None = None
# RFC 8628 default. The Tapis authenticator sends no interval.
DEFAULT_POLL_S = 5.0
# The authenticator keeps a device code for 600 s (DeviceCode.CODE_TTL).
DEFAULT_DEVICE_CODE_S = 600.0

# The provider interface, mirrored field for field from clio_relay.identity.base
# and clio_relay.identity.store (W6). When W6's package is merged this block is
# replaced by imports from there.

IdentityState = Literal["signed_in", "signed_out", "expired"]
TokenSource = Literal["grant", "keyring"]
SignInState = Literal["pending", "done", "failed", "slow_down"]


class Token(BaseModel):
    """One access token. ``access_token`` never prints."""

    model_config = ConfigDict(extra="ignore")

    resource_server: str | None = None
    access_token: SecretStr
    token_type: str = "Bearer"
    expires_at: datetime | None = None
    scopes: list[str] = Field(default_factory=list[str])
    source: TokenSource = "keyring"


class IdentityStatus(BaseModel):
    provider: str
    title: str
    state: IdentityState
    source: TokenSource | None = None
    account: str | None = None
    expires_at: datetime | None = None
    scopes: list[str] = Field(default_factory=list[str])
    detail: str | None = None


class SignInStep(BaseModel):
    """Where the person goes. ``display`` is shown only on relay's own page."""

    url: str
    relay_page: bool = False
    display: dict[str, str] = Field(default_factory=dict[str, str])
    poll_interval_s: float | None = None
    accepts_code: bool = False
    data: dict[str, Any] = Field(default_factory=dict[str, Any], exclude=True, repr=False)


class SignInResult(BaseModel):
    state: SignInState
    message: str | None = None


@dataclass
class SignInContext:
    store: Any = None
    http: Any = None
    redirect_uri: str | None = None
    step: SignInStep | None = None


@dataclass
class IdentityContext:
    store: Any = None
    grants: Any = None
    settings: dict[str, Any] = field(default_factory=dict[str, Any])


@runtime_checkable
class SecretStore(Protocol):
    """JSON entries by name, as W6's ``TokenStore``. Production is the OS keyring."""

    def get(self, name: str) -> dict[str, Any] | None: ...

    def set(self, name: str, value: dict[str, Any]) -> None: ...

    def delete(self, name: str) -> None: ...


class MemoryStore:
    """Entries in this process only, for tests (``CLIO_RELAY_KEYRING=memory``)."""

    def __init__(self) -> None:
        self.values: dict[str, str] = {}
        self._lock = threading.Lock()

    def get(self, name: str) -> dict[str, Any] | None:
        with self._lock:
            text = self.values.get(name)
        return _entry(text)

    def set(self, name: str, value: dict[str, Any]) -> None:
        with self._lock:
            self.values[name] = json.dumps(value, sort_keys=True)

    def delete(self, name: str) -> None:
        with self._lock:
            self.values.pop(name, None)


_shared_memory = MemoryStore()


class KeyringStore:
    """The operating system's keyring under relay's name, one JSON entry per name."""

    def __init__(self, service: str = KEYRING_SERVICE) -> None:
        self.service = service

    def get(self, name: str) -> dict[str, Any] | None:
        import keyring

        return _entry(keyring.get_password(self.service, name))

    def set(self, name: str, value: dict[str, Any]) -> None:
        import keyring

        keyring.set_password(self.service, name, json.dumps(value, sort_keys=True))

    def delete(self, name: str) -> None:
        import keyring
        from keyring.errors import PasswordDeleteError

        try:
            keyring.delete_password(self.service, name)
        except PasswordDeleteError:
            pass


def default_store() -> SecretStore:
    """The keyring, or this process's memory under ``CLIO_RELAY_KEYRING=memory``."""
    if os.environ.get("CLIO_RELAY_KEYRING") == "memory":
        return _shared_memory
    return KeyringStore()


def _entry(text: str | None) -> dict[str, Any] | None:
    if not text:
        return None
    try:
        value: object = json.loads(text)
    except ValueError:
        return None
    return cast(dict[str, Any], value) if isinstance(value, dict) else None


# Settings and the stored record.


class TaccSettings(BaseModel):
    """Which tenant to sign in to, and relay's OAuth client there."""

    base_url: str = DEFAULT_BASE_URL
    tenant_id: str = DEFAULT_TENANT
    client_id: str | None = None
    client_key: SecretStr | None = None
    refresh_margin_s: float = Field(default=300.0, ge=0)

    @classmethod
    def resolve(cls, settings: Any = None, **overrides: Any) -> TaccSettings:
        """Read the ``tacc`` section of relay's settings, then the environment."""
        values: dict[str, Any] = {}
        section = _section(settings)
        values.update({key: value for key, value in section.items() if value is not None})
        env = {
            "base_url": os.environ.get("CLIO_RELAY_TACC_BASE_URL"),
            "tenant_id": os.environ.get("CLIO_RELAY_TACC_TENANT_ID"),
            "client_id": os.environ.get("CLIO_RELAY_TACC_CLIENT_ID"),
            "client_key": os.environ.get("CLIO_RELAY_TACC_CLIENT_KEY"),
        }
        values.update({key: value for key, value in env.items() if value})
        values.setdefault("client_id", SHIPPED_CLIENT_ID)
        values.setdefault("client_key", SHIPPED_CLIENT_KEY)
        values.update({key: value for key, value in overrides.items() if value is not None})
        return cls.model_validate(values)


def _section(settings: Any) -> dict[str, Any]:
    if settings is None:
        return {}
    for path in (("tacc",), ("identities", "tacc")):
        node: Any = settings
        for part in path:
            if isinstance(node, Mapping):
                node = cast(Mapping[str, Any], node).get(part)
            else:
                node = getattr(node, part, None)
            if node is None:
                break
        if isinstance(node, BaseModel):
            return node.model_dump()
        if isinstance(node, Mapping):
            return dict(cast(Mapping[str, Any], node))
    return {}


class PendingCode(BaseModel):
    """A device code the person has not finished with yet."""

    device_code: str
    user_code: str
    verification_uri: str
    expires_at: float
    interval_s: float = DEFAULT_POLL_S
    next_poll_at: float = 0.0


class TaccRecord(BaseModel):
    access_token: str | None = None
    access_expires_at: float | None = None
    refresh_token: str | None = None
    refresh_expires_at: float | None = None
    account: str | None = None
    pending: PendingCode | None = None


def jwt_claims(token: str) -> dict[str, Any]:
    """Read a JWT's claims without checking its signature. Only for ``exp`` and the user."""
    try:
        payload = token.split(".")[1]
        payload += "=" * (-len(payload) % 4)
        claims: object = json.loads(base64.urlsafe_b64decode(payload))
    except (IndexError, ValueError):
        return {}
    return cast(dict[str, Any], claims) if isinstance(claims, dict) else {}


def _seconds(value: Any, default: float) -> float:
    """Tapis sends ``expires_in`` as seconds, or for a device code as a timestamp."""
    if isinstance(value, int | float):
        return float(value)
    if isinstance(value, str) and value.strip().isdigit():
        return float(value)
    return default


ClientFactory = Callable[..., Any]


class TaccIdentity:
    """The ``tacc`` identity provider."""

    name = PROVIDER
    title = TITLE

    def __init__(
        self,
        context: IdentityContext | None = None,
        *,
        settings: TaccSettings | None = None,
        store: SecretStore | None = None,
        clock: Callable[[], float] = time.time,
        client_factory: ClientFactory | None = None,
    ) -> None:
        context_store: Any = getattr(context, "store", None)
        self.settings = settings or TaccSettings.resolve(getattr(context, "settings", None))
        if store is not None:
            self.store: SecretStore = store
        elif isinstance(context_store, SecretStore):
            self.store = context_store
        else:
            self.store = default_store()
        self.clock = clock
        self._client_factory = client_factory
        self.key = f"{PROVIDER}:{self.settings.base_url.rstrip('/')}"

    # the stored record

    def _load(self) -> TaccRecord:
        raw = self.store.get(self.key)
        if not raw:
            return TaccRecord()
        try:
            return TaccRecord.model_validate(raw)
        except ValueError:
            return TaccRecord()

    def _save(self, record: TaccRecord) -> None:
        if record == TaccRecord():
            self.store.delete(self.key)
        else:
            self.store.set(self.key, record.model_dump(mode="json"))

    def _client(self, access_token: str | None = None) -> Any:
        from clio_relay.adapters.tapis.client import make_client

        factory = self._client_factory or make_client
        return factory(self.settings.base_url, self.settings.tenant_id, access_token)

    def _client_id(self) -> str:
        if not self.settings.client_id:
            raise Refusal(
                RefusalReason.INVALID_CONFIG,
                "relay has no OAuth client for the TACC tenant yet; set "
                "CLIO_RELAY_TACC_CLIENT_ID (and CLIO_RELAY_TACC_CLIENT_KEY for refresh)",
                provider=PROVIDER,
            )
        return self.settings.client_id

    def _store_tokens(self, record: TaccRecord, result: Any) -> TaccRecord:
        now = self.clock()
        data = cast(dict[str, Any], result or {})
        access = cast(dict[str, Any], data.get("access_token") or {})
        refresh = cast(dict[str, Any], data.get("refresh_token") or {})
        token = str(access.get("access_token") or "")
        claims = jwt_claims(token)
        expires = claims.get("exp")
        record.access_token = token
        record.access_expires_at = (
            float(expires)
            if isinstance(expires, int | float)
            else now + _seconds(access.get("expires_in"), 4 * 3600)
        )
        record.account = str(claims.get("tapis/username") or record.account or "") or None
        if refresh.get("refresh_token"):
            record.refresh_token = str(refresh["refresh_token"])
            refresh_in = refresh.get("expires_in")
            record.refresh_expires_at = (
                now + _seconds(refresh_in, 0) if refresh_in is not None else None
            )
        record.pending = None
        return record

    # the provider interface

    async def status(self) -> IdentityStatus:
        record = self._load()
        now = self.clock()
        if record.access_token is None:
            return IdentityStatus(provider=PROVIDER, title=TITLE, state="signed_out")
        expires = record.access_expires_at
        live = expires is not None and expires > now
        state: IdentityState = "signed_in" if live or self._can_refresh(record) else "expired"
        return IdentityStatus(
            provider=PROVIDER,
            title=TITLE,
            state=state,
            source="keyring",
            account=record.account,
            expires_at=None if expires is None else datetime.fromtimestamp(expires, UTC),
        )

    def _can_refresh(self, record: TaccRecord) -> bool:
        if not record.refresh_token or self.settings.client_key is None:
            return False
        return record.refresh_expires_at is None or record.refresh_expires_at > self.clock()

    async def token(self, resource_server: str | None = None) -> Token | None:
        """Return a live access token, refreshing it first when it is close to expiry."""
        record = self._load()
        if record.access_token is None:
            return None
        now = self.clock()
        expires = record.access_expires_at or 0.0
        if expires - now <= self.settings.refresh_margin_s:
            if self._can_refresh(record):
                record = await self._refresh(record)
                if record.access_token is None:
                    return None
            elif expires <= now:
                return None
        assert record.access_token is not None
        return Token(
            resource_server=resource_server,
            access_token=SecretStr(record.access_token),
            expires_at=(
                None
                if record.access_expires_at is None
                else datetime.fromtimestamp(record.access_expires_at, UTC)
            ),
        )

    async def _refresh(self, record: TaccRecord) -> TaccRecord:
        from clio_relay.adapters.tapis.client import TapisCallError, acall

        assert self.settings.client_key is not None
        try:
            result = await acall(
                self._client(),
                "authenticator",
                "create_token",
                grant_type="refresh_token",
                refresh_token=record.refresh_token,
                client_id=self._client_id(),
                client_key=self.settings.client_key.get_secret_value(),
            )
        except TapisCallError as exc:
            if exc.status is None or exc.status >= 500:
                raise exc.refusal(RefusalReason.UNREACHABLE, provider=PROVIDER) from None
            # The refresh token was refused: the identity is signed out.
            self._save(TaccRecord(pending=record.pending))
            return TaccRecord()
        fresh = self._store_tokens(record, result)
        self._save(fresh)
        return fresh

    async def access_token(self) -> str | None:
        """Return the access token text, for relay's own Tapis client only."""
        found = await self.token()
        return None if found is None else found.access_token.get_secret_value()

    def pending(self) -> PendingCode | None:
        pending = self._load().pending
        if pending is not None and pending.expires_at <= self.clock():
            return None
        return pending

    async def begin(self, ctx: SignInContext | None = None) -> SignInStep:
        """Ask the authenticator for a device code, or reuse the one still open."""
        from clio_relay.adapters.tapis.client import TapisCallError, acall

        pending = self.pending()
        if pending is None:
            client_id = self._client_id()
            try:
                result = await acall(
                    self._client(), "authenticator", "generate_device_code", client_id=client_id
                )
            except TapisCallError as exc:
                raise exc.refusal(RefusalReason.UNREACHABLE, provider=PROVIDER) from None
            data = cast(dict[str, Any], result or {})
            now = self.clock()
            pending = PendingCode(
                device_code=str(data["device_code"]),
                user_code=str(data["user_code"]),
                verification_uri=str(data["verification_uri"]),
                expires_at=now + _seconds(data.get("expires_in"), DEFAULT_DEVICE_CODE_S),
                interval_s=DEFAULT_POLL_S,
                next_poll_at=now + DEFAULT_POLL_S,
            )
            record = self._load()
            record.pending = pending
            self._save(record)
        return self.step(pending)

    def step(self, pending: PendingCode) -> SignInStep:
        return SignInStep(
            url=pending.verification_uri,
            relay_page=False,
            display={"user_code": pending.user_code},
            poll_interval_s=pending.interval_s,
        )

    @staticmethod
    def message(step: SignInStep) -> str:
        """The ask's text. The user code is for the person to type (RFC 8628), not a secret."""
        code = step.display.get("user_code", "")
        return (
            f"Sign in to {TITLE}. Open the TACC page, enter the code {code} "
            "and sign in with your TACC account."
        )

    async def poll(
        self,
        ctx: SignInContext | None = None,
        step: SignInStep | None = None,
        *,
        force: bool = False,
    ) -> SignInResult:
        """Ask once whether the person finished.

        It never asks sooner than the interval, unless ``force``: the person
        has just said they finished, which happens once per ask.
        """
        from clio_relay.adapters.tapis.client import TapisCallError, acall

        record = self._load()
        pending = record.pending
        now = self.clock()
        if pending is None:
            if record.access_token is not None:
                return SignInResult(state="done", message="signed in")
            return SignInResult(state="failed", message="no sign-in is in progress")
        if pending.expires_at <= now:
            record.pending = None
            self._save(record)
            return SignInResult(state="failed", message="the TACC code expired; ask again")
        if now < pending.next_poll_at and not force:
            return SignInResult(state="pending", message="waiting for the TACC sign-in")
        pending.next_poll_at = now + pending.interval_s
        self._save(record)
        try:
            result = await acall(
                self._client(),
                "authenticator",
                "create_token",
                grant_type="device_code",
                device_code=pending.device_code,
                client_id=self._client_id(),
            )
        except TapisCallError as exc:
            text = exc.message.lower()
            if exc.status is None or exc.status >= 500:
                return SignInResult(state="pending", message="TACC did not answer; still waiting")
            if "slow_down" in text or "slow down" in text:
                pending.interval_s += 5.0
                pending.next_poll_at = now + pending.interval_s
                self._save(record)
                return SignInResult(state="slow_down", message="waiting for the TACC sign-in")
            if "not ready" in text or "authorization_pending" in text:
                return SignInResult(state="pending", message="waiting for the TACC sign-in")
            record.pending = None
            self._save(record)
            return SignInResult(state="failed", message=f"TACC refused the sign-in: {exc.message}")
        self._save(self._store_tokens(record, result))
        return SignInResult(state="done", message="signed in to TACC")

    async def callback(self, ctx: SignInContext | None, params: Mapping[str, str]) -> SignInResult:
        return SignInResult(state="failed", message="the TACC sign-in uses a device code")

    async def sign_out(self) -> None:
        """Revoke the tokens where Tapis allows it, then forget them."""
        from clio_relay.adapters.tapis.client import TapisCallError, acall

        record = self._load()
        for token in (record.access_token, record.refresh_token):
            if not token:
                continue
            try:
                await acall(self._client(), "authenticator", "revoke_token", token=token)
            except TapisCallError:
                pass
        self.store.delete(self.key)


def create(context: IdentityContext | None = None) -> TaccIdentity:
    """The factory W6's provider table calls."""
    return TaccIdentity(context)


__all__ = [
    "IdentityContext",
    "IdentityStatus",
    "KeyringStore",
    "MemoryStore",
    "PendingCode",
    "SecretStore",
    "SignInContext",
    "SignInResult",
    "SignInStep",
    "TaccIdentity",
    "TaccSettings",
    "Token",
    "create",
    "default_store",
    "jwt_claims",
]

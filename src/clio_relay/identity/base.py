"""What an identity provider is, and the values it trades in (RL30).

A provider signs a person in once and hands out tokens. It reads the host's
grant file first (HS4a) and falls back to its own sign-in, whose tokens live in
the operating system's keyring under relay's name (RL28).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, Literal, Protocol, runtime_checkable

from pydantic import BaseModel, ConfigDict, Field, SecretStr

from clio_relay.jobs.handles import utc_now

if TYPE_CHECKING:
    import httpx

    from clio_relay.identity.grants import GrantFiles
    from clio_relay.identity.store import TokenStore

IdentityState = Literal["signed_in", "signed_out", "expired"]
TokenSource = Literal["grant", "keyring"]
SignInState = Literal["pending", "done", "failed", "slow_down"]

# A token this close to expiry counts as expired, so a caller never gets one
# that dies in flight.
EXPIRY_MARGIN = timedelta(seconds=60)


class Token(BaseModel):
    """One access token. ``access_token`` never prints."""

    model_config = ConfigDict(extra="ignore")

    resource_server: str | None = None
    access_token: SecretStr
    token_type: str = "Bearer"
    expires_at: datetime | None = None
    scopes: list[str] = Field(default_factory=list[str])
    source: TokenSource = "keyring"

    def live(self, now: datetime | None = None) -> bool:
        """Return whether the token is still good for a while."""
        if self.expires_at is None:
            return True
        return self.expires_at - EXPIRY_MARGIN > (now or utc_now())


class IdentityStatus(BaseModel):
    """What a person sees about one identity."""

    provider: str
    title: str
    state: IdentityState
    source: TokenSource | None = None
    account: str | None = None
    expires_at: datetime | None = None
    scopes: list[str] = Field(default_factory=list[str])
    detail: str | None = None


class SignInStep(BaseModel):
    """Where the person goes next.

    ``relay_page`` false means ``url`` is the provider's own ``https`` page and
    the ask points there. True means relay's page shows ``display`` (a device
    code, a link) and the ask points at relay's page, so nothing in ``display``
    reaches the transcript. ``data`` is the provider's private state (a PKCE
    verifier, a device code) and is never shown or written down.
    """

    url: str
    relay_page: bool = False
    display: dict[str, str] = Field(default_factory=dict[str, str])
    poll_interval_s: float | None = None
    accepts_code: bool = Field(
        default=False, description="Relay's page offers a box to paste a code into"
    )
    data: dict[str, Any] = Field(default_factory=dict[str, Any], exclude=True, repr=False)


class SignInResult(BaseModel):
    """How a sign-in step went."""

    state: SignInState
    message: str | None = None


@dataclass
class SignInContext:
    """What relay gives a provider while it signs a person in."""

    store: TokenStore
    http: httpx.AsyncClient
    redirect_uri: str | None = None
    step: SignInStep | None = None


@dataclass
class IdentityContext:
    """What relay gives an identity factory."""

    store: TokenStore
    grants: GrantFiles
    settings: dict[str, Any] = field(default_factory=dict[str, Any])


@runtime_checkable
class IdentityProvider(Protocol):
    """One sign-in that unlocks sites (RL30)."""

    name: str
    title: str

    async def status(self) -> IdentityStatus:
        """Return whether the person is signed in, and through what."""
        ...

    async def token(self, resource_server: str | None = None) -> Token | None:
        """Return a live token: the grant file's first, then the keyring's."""
        ...

    async def begin(self, ctx: SignInContext) -> SignInStep:
        """Start a sign-in and say where the person goes."""
        ...

    async def poll(self, ctx: SignInContext, step: SignInStep) -> SignInResult:
        """Check a device-style sign-in. Others return ``pending``."""
        ...

    async def callback(self, ctx: SignInContext, params: dict[str, str]) -> SignInResult:
        """Finish a redirect or pasted-code sign-in. ``ctx.step`` is the begun step."""
        ...

    async def sign_out(self) -> None:
        """Forget relay's own tokens. A host's grant file stays the host's."""
        ...

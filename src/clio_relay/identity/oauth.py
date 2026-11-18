"""What relay's own OAuth sign-ins share: PKCE, token responses and the keyring entry."""

from __future__ import annotations

import base64
import hashlib
import json
import os
import secrets
from datetime import timedelta
from typing import Any

import httpx

from clio_relay.identity.base import (
    IdentityContext,
    IdentityStatus,
    SignInContext,
    SignInResult,
    SignInStep,
    Token,
)
from clio_relay.jobs.handles import utc_now
from clio_relay.jobs.refusals import Refusal, RefusalReason


def pkce_pair() -> tuple[str, str]:
    """Return a PKCE verifier and its S256 challenge."""
    verifier = secrets.token_urlsafe(48)
    digest = hashlib.sha256(verifier.encode("ascii")).digest()
    challenge = base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")
    return verifier, challenge


def jwt_claims(token: str) -> dict[str, Any]:
    """Read an id token's claims for display only. Nothing is trusted from them."""
    parts = token.split(".")
    if len(parts) < 2:
        return {}
    padded = parts[1] + "=" * (-len(parts[1]) % 4)
    try:
        value: object = json.loads(base64.urlsafe_b64decode(padded))
    except (ValueError, TypeError):
        return {}
    return value if isinstance(value, dict) else {}  # pyright: ignore[reportUnknownVariableType]


def token_entry(document: dict[str, Any]) -> dict[str, Any]:
    """Turn one OAuth token response into a stored token entry."""
    expires_in = document.get("expires_in")
    expires_at = None
    if isinstance(expires_in, (int, float)):
        expires_at = (utc_now() + timedelta(seconds=float(expires_in))).isoformat()
    scope = document.get("scope")
    scopes: list[str] = []
    if isinstance(scope, str):
        scopes = [item for item in scope.replace(",", " ").split() if item]
    return {
        "resource_server": document.get("resource_server"),
        "access_token": str(document.get("access_token", "")),
        "token_type": str(document.get("token_type") or "Bearer"),
        "expires_at": expires_at,
        "scopes": scopes,
    }


class OAuthIdentity:
    """Status, tokens and sign-out for a provider whose own sign-in is OAuth."""

    name: str = ""
    title: str = ""

    def __init__(self, context: IdentityContext) -> None:
        self.context = context

    @property
    def entry_name(self) -> str:
        """Return the keyring entry's name."""
        return f"identity:{self.name}"

    def setting(self, key: str, default: str | None = None) -> str | None:
        """Return ``settings[<name>][key]``, then ``CLIO_RELAY_<NAME>_<KEY>``."""
        section = self.context.settings.get(self.name)
        if isinstance(section, dict):
            value = section.get(key)  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            if isinstance(value, str) and value:
                return value
        env = os.environ.get(f"CLIO_RELAY_{self.name.upper()}_{key.upper()}")
        return env or default

    def stored(self) -> dict[str, Any] | None:
        """Return relay's own sign-in, from the keyring."""
        return self.context.store.get(self.entry_name)

    def _stored_tokens(self) -> list[Token]:
        entry = self.stored() or {}
        found: list[Token] = []
        for item in entry.get("tokens") or []:
            if isinstance(item, dict) and item.get("access_token"):  # pyright: ignore[reportUnknownMemberType]
                found.append(Token.model_validate(item))
        return found

    async def refresh(self) -> bool:
        """Refresh relay's own tokens. Providers with refresh tokens override this."""
        return False

    async def status(self) -> IdentityStatus:
        grant = self.context.grants.read(self.name)
        if grant is not None:
            live = [item.token() for item in grant.tokens if item.token().live()]
            if live:
                expiry = [token.expires_at for token in live if token.expires_at is not None]
                return IdentityStatus(
                    provider=self.name,
                    title=self.title,
                    state="signed_in",
                    source="grant",
                    expires_at=min(expiry) if expiry else None,
                    scopes=sorted({scope for token in live for scope in token.scopes}),
                    detail="from the host's grant file",
                )
        tokens = self._stored_tokens()
        entry = self.stored() or {}
        account = entry.get("account") if isinstance(entry.get("account"), str) else None
        if not tokens:
            problem = self.context.grants.problem(self.name)
            return IdentityStatus(
                provider=self.name, title=self.title, state="signed_out", detail=problem
            )
        live = [token for token in tokens if token.live()]
        if not live and entry.get("refresh_token"):
            if await self.refresh():
                live = [token for token in self._stored_tokens() if token.live()]
        expiry = [token.expires_at for token in live if token.expires_at is not None]
        return IdentityStatus(
            provider=self.name,
            title=self.title,
            state="signed_in" if live else "expired",
            source="keyring",
            account=account,
            expires_at=min(expiry) if expiry else None,
            scopes=sorted({scope for token in (live or tokens) for scope in token.scopes}),
        )

    async def token(self, resource_server: str | None = None) -> Token | None:
        found = self.context.grants.token(self.name, resource_server)
        if found is not None:
            return found
        for attempt in range(2):
            for token in self._stored_tokens():
                if resource_server is not None and token.resource_server != resource_server:
                    continue
                if token.live():
                    return token
            if attempt == 0 and not await self.refresh():
                break
        return None

    async def poll(self, ctx: SignInContext, step: SignInStep) -> SignInResult:
        return SignInResult(state="pending")

    async def callback(self, ctx: SignInContext, params: dict[str, str]) -> SignInResult:
        return SignInResult(state="failed", message="this sign-in takes no callback")

    async def sign_out(self) -> None:
        self.context.store.delete(self.entry_name)


async def post_form(
    http: httpx.AsyncClient,
    url: str,
    data: dict[str, str],
    *,
    auth: tuple[str, str] | None = None,
) -> tuple[int, dict[str, Any]]:
    """POST a form and return the status and JSON body. The body is never logged."""
    try:
        response = await http.post(
            url, data=data, auth=auth, headers={"Accept": "application/json"}
        )
    except httpx.HTTPError as exc:
        raise Refusal(
            RefusalReason.UNREACHABLE, f"could not reach {httpx.URL(url).host}: {type(exc).__name__}"
        ) from exc
    try:
        body: object = response.json()
    except ValueError:
        body = {}
    return response.status_code, body if isinstance(body, dict) else {}  # pyright: ignore[reportUnknownVariableType]

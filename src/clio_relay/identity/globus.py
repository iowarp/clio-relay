"""Relay's own Globus sign-in, for a host that hands relay no grant (RL30).

Relay signs in as a Globus native app: the authorization code flow with PKCE
and no client secret. The ask points at Globus Auth's own ``https`` page, so
Globus takes the password and any second factor and relay receives only tokens.

Two ways back to relay:

* ``redirect = relay``: Globus sends the browser to relay's callback, on
  loopback over stdio or relay's own origin over HTTP. The client must list
  that address as a redirect.
* ``redirect = paste``: Globus shows the code on its own page and the person
  pastes it into relay's page.

Settings: ``settings["globus"]`` or ``CLIO_RELAY_GLOBUS_<KEY>``, with keys
``client_id``, ``auth_url`` (default ``https://auth.globus.org``), ``scopes``
(space separated) and ``redirect``. No client id is registered yet (owner
blocker O10); without one, sign-in refuses and says so.
"""

from __future__ import annotations

import secrets
from typing import Any, cast
from urllib.parse import urlencode

import httpx

from clio_relay.identity.base import (
    IdentityContext,
    SignInContext,
    SignInResult,
    SignInStep,
)
from clio_relay.identity.oauth import (
    OAuthIdentity,
    jwt_claims,
    pkce_pair,
    post_form,
    token_entry,
)
from clio_relay.jobs.refusals import Refusal, RefusalReason

DEFAULT_AUTH_URL = "https://auth.globus.org"
DEFAULT_SCOPES = (
    "openid profile email offline_access urn:globus:auth:scope:transfer.api.globus.org:all"
)


class GlobusIdentity(OAuthIdentity):
    """Globus, through the host's grant file or relay's own native-app sign-in."""

    name = "globus"
    title = "Globus"

    def __init__(self, context: IdentityContext) -> None:
        super().__init__(context)

    @property
    def auth_url(self) -> str:
        return (self.setting("auth_url", DEFAULT_AUTH_URL) or DEFAULT_AUTH_URL).rstrip("/")

    def _client_id(self) -> str:
        client_id = self.setting("client_id")
        if not client_id:
            raise Refusal(
                RefusalReason.INVALID_CONFIG,
                "relay has no Globus native-app client to sign in with. Set "
                "CLIO_RELAY_GLOBUS_CLIENT_ID, or let the host hand relay a Globus grant file",
                provider=self.name,
            )
        return client_id

    async def begin(self, ctx: SignInContext) -> SignInStep:
        client_id = self._client_id()
        mode = self.setting("redirect") or ("relay" if ctx.redirect_uri else "paste")
        if mode == "relay" and ctx.redirect_uri:
            redirect_uri = ctx.redirect_uri
        else:
            mode = "paste"
            redirect_uri = f"{self.auth_url}/v2/web/auth-code"
        verifier, challenge = pkce_pair()
        state = secrets.token_urlsafe(24)
        query = urlencode(
            {
                "client_id": client_id,
                "redirect_uri": redirect_uri,
                "scope": self.setting("scopes", DEFAULT_SCOPES) or DEFAULT_SCOPES,
                "state": state,
                "response_type": "code",
                "code_challenge": challenge,
                "code_challenge_method": "S256",
                "access_type": "offline",
                "prompt": "login",
            }
        )
        url = f"{self.auth_url}/v2/oauth2/authorize?{query}"
        data = {"verifier": verifier, "state": state, "redirect_uri": redirect_uri}
        if mode == "relay":
            return SignInStep(url=url, relay_page=False, data=data)
        return SignInStep(
            url=url,
            relay_page=True,
            accepts_code=True,
            display={
                "link": url,
                "instructions": "Sign in to Globus at the link, then paste the code it shows.",
            },
            data=data,
        )

    async def callback(self, ctx: SignInContext, params: dict[str, str]) -> SignInResult:
        step = ctx.step
        if step is None:
            return SignInResult(state="failed", message="no Globus sign-in is in progress")
        if params.get("error"):
            return SignInResult(state="failed", message=f"Globus said {params.get('error')}")
        if "state" in params and not secrets.compare_digest(
            params["state"], str(step.data.get("state", ""))
        ):
            return SignInResult(state="failed", message="the sign-in reply was for another sign-in")
        code = (params.get("code") or "").strip()
        if not code:
            return SignInResult(state="failed", message="no code came back")
        status, body = await post_form(
            ctx.http,
            f"{self.auth_url}/v2/oauth2/token",
            {
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": str(step.data.get("redirect_uri", "")),
                "client_id": self._client_id(),
                "code_verifier": str(step.data.get("verifier", "")),
            },
        )
        if status != 200 or not body.get("access_token"):
            reason = body.get("error") or f"HTTP {status}"
            return SignInResult(state="failed", message=f"Globus refused the code: {reason}")
        self._save(body)
        return SignInResult(state="done", message="Signed in to Globus.")

    def _save(self, body: dict[str, Any]) -> None:
        documents: list[dict[str, Any]] = [body]
        others = body.get("other_tokens")
        if isinstance(others, list):
            for item in cast(list[Any], others):
                if isinstance(item, dict):
                    documents.append(cast(dict[str, Any], item))
        tokens: list[dict[str, Any]] = []
        for document in documents:
            entry = token_entry(document)
            refresh = document.get("refresh_token")
            if isinstance(refresh, str) and refresh:
                entry["refresh_token"] = refresh
            tokens.append(entry)
        account = None
        id_token = body.get("id_token")
        if isinstance(id_token, str):
            claims = jwt_claims(id_token)
            found = claims.get("preferred_username") or claims.get("email")
            account = found if isinstance(found, str) else None
        previous = self.stored() or {}
        self.context.store.set(
            self.entry_name,
            {
                "account": account or previous.get("account"),
                "refresh_token": any("refresh_token" in item for item in tokens),
                "tokens": tokens,
            },
        )

    async def refresh(self) -> bool:
        entry = self.stored()
        if not entry:
            return False
        client_id = self.setting("client_id")
        if not client_id:
            return False
        changed = False
        tokens: list[dict[str, Any]] = []
        async with httpx.AsyncClient(timeout=30) as http:
            for item in cast(list[Any], entry.get("tokens") or []):
                if not isinstance(item, dict):
                    continue
                current: dict[str, Any] = dict(cast(dict[str, Any], item))
                refresh = current.get("refresh_token")
                if isinstance(refresh, str) and refresh:
                    status, body = await post_form(
                        http,
                        f"{self.auth_url}/v2/oauth2/token",
                        {
                            "grant_type": "refresh_token",
                            "refresh_token": refresh,
                            "client_id": client_id,
                        },
                    )
                    if status == 200 and body.get("access_token"):
                        fresh = token_entry(body)
                        fresh["resource_server"] = fresh.get("resource_server") or current.get(
                            "resource_server"
                        )
                        fresh["refresh_token"] = body.get("refresh_token") or refresh
                        current = fresh
                        changed = True
                tokens.append(current)
        if changed:
            entry["tokens"] = tokens
            self.context.store.set(self.entry_name, entry)
        return changed


def create(context: IdentityContext) -> GlobusIdentity:
    """Identity factory for the provider table."""
    return GlobusIdentity(context)

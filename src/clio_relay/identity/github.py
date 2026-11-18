"""Relay's own GitHub sign-in, for a host that hands relay no grant (RL30).

The OAuth device flow (RFC 8628). Relay's page shows the user code and the
link to GitHub, so the code never reaches the transcript. Relay polls GitHub in
the background at the interval GitHub asks for, slowing down when told to.

Settings: ``settings["github"]`` or ``CLIO_RELAY_GITHUB_<KEY>``, with keys
``client_id``, ``url`` (default ``https://github.com``) and ``scopes``
(space separated, default ``repo read:org``).
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from clio_relay.identity.base import (
    IdentityContext,
    SignInContext,
    SignInResult,
    SignInStep,
)
from clio_relay.identity.oauth import OAuthIdentity, post_form, token_entry
from clio_relay.jobs.refusals import Refusal, RefusalReason

DEFAULT_URL = "https://github.com"
DEFAULT_SCOPES = "repo read:org"
DEVICE_GRANT = "urn:ietf:params:oauth:grant-type:device_code"
SLOW_DOWN_STEP_S = 5.0


class GitHubIdentity(OAuthIdentity):
    """GitHub, through the host's grant file or relay's own device-flow sign-in."""

    name = "github"
    title = "GitHub"

    def __init__(self, context: IdentityContext) -> None:
        super().__init__(context)

    @property
    def base_url(self) -> str:
        return (self.setting("url", DEFAULT_URL) or DEFAULT_URL).rstrip("/")

    def _client_id(self) -> str:
        client_id = self.setting("client_id")
        if not client_id:
            raise Refusal(
                RefusalReason.INVALID_CONFIG,
                "relay has no GitHub OAuth app to sign in with. Set "
                "CLIO_RELAY_GITHUB_CLIENT_ID, or let the host hand relay a GitHub grant file",
                provider=self.name,
            )
        return client_id

    async def begin(self, ctx: SignInContext) -> SignInStep:
        status, body = await post_form(
            ctx.http,
            f"{self.base_url}/login/device/code",
            {
                "client_id": self._client_id(),
                "scope": self.setting("scopes", DEFAULT_SCOPES) or DEFAULT_SCOPES,
            },
        )
        device_code = body.get("device_code")
        user_code = body.get("user_code")
        verification = body.get("verification_uri")
        if status != 200 or not (isinstance(device_code, str) and isinstance(user_code, str)):
            reason = body.get("error") or f"HTTP {status}"
            raise Refusal(
                RefusalReason.UNREACHABLE,
                f"GitHub did not start a sign-in: {reason}",
                provider=self.name,
            )
        link = verification if isinstance(verification, str) else f"{self.base_url}/login/device"
        interval = body.get("interval")
        expires_in = body.get("expires_in")
        expires_at = (
            datetime.now(UTC) + timedelta(seconds=float(expires_in))
            if isinstance(expires_in, (int, float)) and expires_in > 0
            else None
        )
        return SignInStep(
            url=link,
            relay_page=True,
            display={
                "link": link,
                "code": user_code,
                "instructions": "Open the link, sign in to GitHub and enter the code.",
            },
            poll_interval_s=float(interval) if isinstance(interval, (int, float)) else 5.0,
            expires_at=expires_at,
            data={"device_code": device_code},
        )

    async def poll(self, ctx: SignInContext, step: SignInStep) -> SignInResult:
        status, body = await post_form(
            ctx.http,
            f"{self.base_url}/login/oauth/access_token",
            {
                "client_id": self._client_id(),
                "device_code": str(step.data.get("device_code", "")),
                "grant_type": DEVICE_GRANT,
            },
        )
        error = body.get("error")
        if error == "authorization_pending":
            return SignInResult(state="pending")
        if error == "slow_down":
            return SignInResult(state="slow_down")
        if error == "expired_token":
            return SignInResult(state="expired", message="GitHub's code expired.")
        if error == "access_denied":
            return SignInResult(state="failed", message="The sign-in was declined on GitHub.")
        if status != 200 or not body.get("access_token"):
            return SignInResult(state="failed", message=f"GitHub said {error or status}")
        self._save(body)
        return SignInResult(state="done", message="Signed in to GitHub.")

    def _save(self, body: dict[str, Any]) -> None:
        entry = token_entry(body)
        entry["resource_server"] = "api.github.com"
        stored: dict[str, Any] = {"account": None, "tokens": [entry]}
        refresh = body.get("refresh_token")
        if isinstance(refresh, str) and refresh:
            entry["refresh_token"] = refresh
            stored["refresh_token"] = True
        self.context.store.set(self.entry_name, stored)


def create(context: IdentityContext) -> GitHubIdentity:
    """Identity factory for the provider table."""
    return GitHubIdentity(context)

"""The bearer token relay's HTTP transport accepts."""

from __future__ import annotations

import hmac

from fastmcp.server.auth import AccessToken, TokenVerifier


class RelayBearer(TokenVerifier):
    """Accept one bearer token, compared in constant time."""

    def __init__(self, token: str, *, base_url: str) -> None:
        super().__init__(base_url=base_url, resource_base_url=base_url)
        self._token = token

    async def verify_token(self, token: str) -> AccessToken | None:
        if not hmac.compare_digest(token, self._token):
            return None
        return AccessToken(token=token, client_id="clio-relay", scopes=[], subject="clio-relay")

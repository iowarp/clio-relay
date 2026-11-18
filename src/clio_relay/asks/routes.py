"""Relay's page under relay's own origin, for relay over Streamable HTTP.

The routes sit beside the MCP endpoint at ``/relay/asks/<ask_id>`` and
``/relay/asks/callback``. They take the operator credential as HTTP Basic auth
(any user name, the operator token as the password), which is separate from the
MCP bearer token, so a client holding only the bearer cannot approve anything.
Nothing is logged; run uvicorn with its access log off.
"""

from __future__ import annotations

import base64
import binascii
import hmac
import os
import secrets
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import urlsplit

from starlette.requests import Request
from starlette.responses import Response

from clio_relay.asks.loopback import CALLBACK, MAX_BODY, first_values
from clio_relay.asks.pages import SECURITY_HEADERS, PageResponse

if TYPE_CHECKING:
    from clio_relay.asks.provider import RelayAskProvider

OPERATOR_ENV = "CLIO_RELAY_OPERATOR_TOKEN"
REALM = 'Basic realm="clio-relay asks", charset="UTF-8"'


def operator_token_file(state_dir: Path) -> Path:
    """Return where the operator token is kept."""
    return state_dir / "operator-token"


def operator_token(state_dir: Path) -> str:
    """Return the operator token: ``CLIO_RELAY_OPERATOR_TOKEN``, else a private file."""
    given = os.environ.get(OPERATOR_ENV)
    if given:
        return given
    path = operator_token_file(state_dir)
    try:
        return path.read_text(encoding="ascii").strip()
    except FileNotFoundError:
        pass
    path.parent.mkdir(parents=True, exist_ok=True)
    token = secrets.token_urlsafe(24)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w", encoding="ascii") as handle:
        handle.write(token)
    return token


def authorized(header: str | None, token: str) -> bool:
    """Return whether an ``Authorization`` header carries the operator token."""
    if not header or not header.lower().startswith("basic "):
        return False
    try:
        decoded = base64.b64decode(header[6:].strip(), validate=True).decode("utf-8")
    except (binascii.Error, UnicodeDecodeError):
        return False
    _, _, password = decoded.partition(":")
    return hmac.compare_digest(password.encode(), token.encode())


def _respond(page: PageResponse) -> Response:
    return Response(page.body, status_code=page.status, headers=page.headers)


def _challenge() -> Response:
    headers = dict(SECURITY_HEADERS)
    headers["WWW-Authenticate"] = REALM
    return Response(
        "<!doctype html><title>Sign in</title><p>Relay's operator credential is needed.</p>",
        status_code=401,
        headers=headers,
    )


def mount(server: Any, owner: RelayAskProvider, public_base: str) -> None:
    """Add relay's page routes to a FastMCP server."""
    base_parts = urlsplit(public_base)
    public_origin = f"{base_parts.scheme}://{base_parts.netloc}"

    def token() -> str:
        return operator_token(owner.context.state_dir)

    # Made now, so the operator can read it before the first request.
    token()

    async def callback(request: Request) -> Response:
        if not authorized(request.headers.get("authorization"), token()):
            return _challenge()
        params = dict(request.query_params)
        return _respond(await owner.runner.call(owner.core.callback(params)))

    async def page(request: Request) -> Response:
        if not authorized(request.headers.get("authorization"), token()):
            return _challenge()
        ask_id = str(request.path_params.get("ask_id", ""))
        if request.method == "GET":
            return _respond(
                await owner.runner.call(owner.core.get(ask_id, request.query_params.get("t", "")))
            )
        length = request.headers.get("content-length")
        if length is None or not length.isdigit() or int(length) > MAX_BODY:
            return Response("Bad request", status_code=400)
        body = await request.body()
        if len(body) > MAX_BODY:
            return Response("Bad request", status_code=400)
        fields = first_values(body.decode("utf-8", errors="replace"))
        headers = {key.lower(): value for key, value in request.headers.items()}
        here = f"{request.url.scheme}://{request.url.netloc}"
        origins = {public_origin, here}
        return _respond(await owner.runner.call(owner.core.post(ask_id, fields, headers, origins)))

    server.custom_route(
        CALLBACK, methods=["GET"], name="relay-asks-callback", include_in_schema=False
    )(callback)
    server.custom_route(
        "/relay/asks/{ask_id}",
        methods=["GET", "POST"],
        name="relay-asks-page",
        include_in_schema=False,
    )(page)

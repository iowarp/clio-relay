"""Bearer-token / owner-session-header FastAPI dependency factories.

split/http-api-w3 (iowarp/clio-relay#231): moved verbatim out of
``http_api.py``. Every function here already took ``settings``/its header
values as a plain argument (none referenced a ``create_app()`` local
closure), so this is an unmodified, atomic move.
"""

from __future__ import annotations

import secrets
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Annotated

from fastapi import Header, WebSocket

from clio_relay import door_error_adapters, door_errors
from clio_relay.config import RelaySettings
from clio_relay.job_identity import (
    OWNER_SESSION_ID_HEADER,
    SESSION_GENERATION_ID_HEADER,
    JobOwnerSessionIdentity,
    OwnerSessionIdentityError,
    parse_job_owner_session_identity,
)
from clio_relay.pagination import validate_response_page_limit

if TYPE_CHECKING:
    from clio_relay.http_api_context import RelayApiContext


def _require_api_token(ctx: RelayApiContext) -> Callable[..., Awaitable[None]]:
    """Build the shared bearer-token/session-binding dependency.

    Also the owned-session client-liveness lease's ONE renewal chokepoint
    (iowarp/clio-relay#277): every authenticated request to an owned-session
    API -- attach's status cross-check, ``GET /queue`` polls, job/gateway
    submissions -- depends on this exact function, so renewing the lease here
    covers all of them without asking the client for anything new. Renewal
    happens only after both checks below already passed; an unauthenticated
    or misbound request never touches the lease.
    """
    settings = ctx.resolved

    async def dependency(
        authorization: Annotated[str | None, Header()] = None,
        x_clio_relay_token: Annotated[str | None, Header()] = None,
        x_clio_relay_owner_session_id: Annotated[
            str | None,
            Header(alias=OWNER_SESSION_ID_HEADER),
        ] = None,
        x_clio_relay_session_generation_id: Annotated[
            str | None,
            Header(alias=SESSION_GENERATION_ID_HEADER),
        ] = None,
    ) -> None:
        if settings.api_token is not None:
            supplied = _extract_token(authorization, x_clio_relay_token)
            if supplied is None or not secrets.compare_digest(supplied, settings.api_token):
                raise door_errors.http_problem(
                    "authentication_required", "missing or invalid relay API token"
                )
        expected_session_id = settings.owner_session_id
        expected_generation_id = settings.owner_session_generation_id
        if expected_session_id is None:
            return
        if x_clio_relay_owner_session_id is None or x_clio_relay_session_generation_id is None:
            raise door_errors.http_problem(
                "session_binding_headers_required",
                "exact owner session and generation headers are required",
            )
        if expected_generation_id is None or not (
            secrets.compare_digest(x_clio_relay_owner_session_id, expected_session_id)
            and secrets.compare_digest(
                x_clio_relay_session_generation_id,
                expected_generation_id,
            )
        ):
            raise door_errors.http_problem(
                "session_binding_identity_mismatch",
                "owner session or generation does not match this API process",
            )
        if ctx.owner_session_cluster is not None:
            ctx.queue.touch_owner_session_lease(
                expected_session_id,
                session_generation_id=expected_generation_id,
                cluster=ctx.owner_session_cluster,
                ttl_seconds=settings.owner_session_lease_ttl_seconds,
            )

    return dependency


def _job_owner_session_identity() -> Callable[..., Awaitable[JobOwnerSessionIdentity | None]]:
    """Parse optional attribution headers independently from bearer admission."""

    async def dependency(
        x_clio_relay_owner_session_id: Annotated[
            str | None,
            Header(alias=OWNER_SESSION_ID_HEADER),
        ] = None,
        x_clio_relay_session_generation_id: Annotated[
            str | None,
            Header(alias=SESSION_GENERATION_ID_HEADER),
        ] = None,
    ) -> JobOwnerSessionIdentity | None:
        try:
            return parse_job_owner_session_identity(
                x_clio_relay_owner_session_id,
                x_clio_relay_session_generation_id,
            )
        except OwnerSessionIdentityError as exc:
            raise door_errors.http_problem("owner_session_identity_refused", exc=exc) from exc

    return dependency


def _require_session_submission_binding(
    settings: RelaySettings,
) -> Callable[..., Awaitable[None]]:
    """Require exact client intent before a session-scoped API stamps job ownership."""

    async def dependency(
        x_clio_relay_owner_session_id: Annotated[
            str | None,
            Header(alias=OWNER_SESSION_ID_HEADER),
        ] = None,
        x_clio_relay_session_generation_id: Annotated[
            str | None,
            Header(alias=SESSION_GENERATION_ID_HEADER),
        ] = None,
    ) -> None:
        expected_session_id = settings.owner_session_id
        expected_generation_id = settings.owner_session_generation_id
        if expected_session_id is None:
            if (
                x_clio_relay_owner_session_id is not None
                or x_clio_relay_session_generation_id is not None
            ):
                raise door_errors.http_problem(
                    "unbound_session_api", "relay API is not bound to an owner session"
                )
            return
        if settings.api_token is None:
            raise door_errors.http_problem(
                "session_authentication_unavailable",
                "owned relay session submissions require API token authentication",
            )
        if x_clio_relay_owner_session_id is None or x_clio_relay_session_generation_id is None:
            raise door_errors.http_problem(
                "session_binding_headers_required",
                "exact owner session and generation headers are required",
            )
        if expected_generation_id is None or not (
            secrets.compare_digest(x_clio_relay_owner_session_id, expected_session_id)
            and secrets.compare_digest(
                x_clio_relay_session_generation_id,
                expected_generation_id,
            )
        ):
            raise door_errors.http_problem(
                "session_binding_identity_mismatch",
                "owner session or generation does not match this API process",
            )

    return dependency


def _require_websocket_page_limit(limit: object) -> None:
    try:
        validate_response_page_limit(limit)
    except ValueError as exc:
        raise door_error_adapters.websocket_refusal("websocket_page_limit_invalid") from exc


def _require_websocket_token(settings: RelaySettings, websocket: WebSocket) -> None:
    if settings.api_token is None:
        return
    supplied = websocket.query_params.get("token")
    if supplied is None:
        supplied = _extract_token(websocket.headers.get("authorization"), None)
    if supplied is None or not secrets.compare_digest(supplied, settings.api_token):
        raise door_error_adapters.websocket_refusal("websocket_authentication_failed")
    if settings.owner_session_id is None:
        return
    session_id = websocket.headers.get(OWNER_SESSION_ID_HEADER)
    generation_id = websocket.headers.get(SESSION_GENERATION_ID_HEADER)
    if (
        session_id is None
        or generation_id is None
        or settings.owner_session_generation_id is None
        or not secrets.compare_digest(session_id, settings.owner_session_id)
        or not secrets.compare_digest(generation_id, settings.owner_session_generation_id)
    ):
        raise door_error_adapters.websocket_refusal("websocket_session_binding_failed")


def _extract_token(authorization: str | None, header_token: str | None) -> str | None:
    if header_token:
        return header_token
    if authorization is None:
        return None
    scheme, _, token = authorization.partition(" ")
    if scheme.lower() != "bearer" or token == "":
        return None
    return token

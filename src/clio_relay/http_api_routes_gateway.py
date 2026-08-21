"""Scheduler-backed gateway-session CRUD routes.

split/http-api-w3 (iowarp/clio-relay#231): moved verbatim out of
``create_app()`` in ``http_api.py``. Every reference to a
``create_app()``-local closure (``resolved``, ``queue``,
``owner_session_cluster``, ``ensure_intake_open``, ``require_owned_gateway``)
is rewritten to the equivalent ``ctx.<name>`` attribute/method on the shared
``RelayApiContext`` (see ``http_api_context.py``'s own docstring) -- the same
mechanical bare-name -> qualified-name rewrite this codebase's other AST-
driven extractions already use; no other line changes.
"""

# pyright: reportUnusedFunction=false

from __future__ import annotations

from typing import Annotated

from fastapi import FastAPI, Query

from clio_relay import door_errors
from clio_relay.errors import NotFoundError, QueueConflictError
from clio_relay.http_api_context import RelayApiContext
from clio_relay.http_api_models import (
    GatewaySessionCreateRequest,
    GatewaySessionUpdateRequest,
    _has_relay_managed_gateway_state,
)
from clio_relay.http_api_redaction import _public_payload, _public_record
from clio_relay.identifiers import DurableRecordId
from clio_relay.models import GatewaySession
from clio_relay.pagination import DEFAULT_RESPONSE_PAGE_RECORDS, MAX_RESPONSE_PAGE_RECORDS


def register_gateway_routes(
    app: FastAPI,
    ctx: RelayApiContext,
    *,
    auth_dependency: object,
    session_submission_dependency: object,
) -> None:
    """Register the gateway-session create/list/get/update/close routes."""

    @app.post(
        "/gateway-sessions",
        response_model=GatewaySession,
        dependencies=[auth_dependency, session_submission_dependency],
    )
    def create_gateway_session(request: GatewaySessionCreateRequest) -> GatewaySession:
        ctx.ensure_intake_open()
        if ctx.owner_session_cluster is not None and request.cluster != ctx.owner_session_cluster:
            raise door_errors.http_problem(
                "gateway_cluster_mismatch",
                "gateway cluster does not match this owned relay session",
            )
        metadata = dict(request.metadata)
        if ctx.resolved.owner_session_id is not None:
            metadata.update(
                {
                    "owner": "clio-relay",
                    "owner_session_id": ctx.resolved.owner_session_id,
                }
            )
            if ctx.resolved.owner_session_generation_id is not None:
                metadata["owner_session_generation_id"] = ctx.resolved.owner_session_generation_id
        try:
            return _public_record(
                ctx.queue.create_gateway_session(
                    GatewaySession(
                        cluster=request.cluster,
                        name=request.name,
                        state=request.state,
                        queue_state=request.queue_state,
                        node=request.node,
                        requested_resources=request.requested_resources,
                        stdout_uri=request.stdout_uri,
                        stderr_uri=request.stderr_uri,
                        log_uris=request.log_uris,
                        gateway=request.gateway,
                        metadata=metadata,
                    )
                )
            )
        except QueueConflictError as exc:
            raise door_errors.http_problem(
                "gateway_conflict", exc=door_errors.public_message_error(exc)
            ) from exc

    @app.get(
        "/gateway-sessions",
        dependencies=[auth_dependency],
    )
    def list_gateway_sessions(
        cluster: str | None = None,
        cursor: Annotated[int, Query(ge=1)] = 1,
        limit: Annotated[int, Query(ge=1, le=MAX_RESPONSE_PAGE_RECORDS)] = (
            DEFAULT_RESPONSE_PAGE_RECORDS
        ),
    ) -> dict[str, object]:
        sessions, next_cursor, total = ctx.queue.list_gateway_sessions_page(
            cursor=cursor,
            limit=limit,
            cluster=cluster,
        )
        if ctx.resolved.owner_session_id is not None:
            sessions = [
                session
                for session in sessions
                if session.metadata.get("owner") == "clio-relay"
                and session.metadata.get("owner_session_id") == ctx.resolved.owner_session_id
                and session.metadata.get("owner_session_generation_id")
                == ctx.resolved.owner_session_generation_id
            ]
        return _public_payload(
            {
                "gateway_sessions": [session.model_dump(mode="json") for session in sessions],
                "source_cursor": cursor,
                "source_limit": limit,
                "source_next_cursor": next_cursor,
                "source_total": total,
                "source_total_semantics": "global_gateway_sequence_high_water",
                "filters_apply_within_source_window": True,
                "visibility_filter": (
                    "owner_session_within_source_window"
                    if ctx.resolved.owner_session_id is not None
                    else None
                ),
            }
        )

    @app.get(
        "/gateway-sessions/{session_id}",
        response_model=GatewaySession,
        dependencies=[auth_dependency],
    )
    def get_gateway_session(session_id: DurableRecordId) -> GatewaySession:
        try:
            return _public_record(ctx.require_owned_gateway(session_id))
        except NotFoundError as exc:
            raise door_errors.http_problem("gateway_not_found", exc=exc) from exc

    @app.patch(
        "/gateway-sessions/{session_id}",
        response_model=GatewaySession,
        dependencies=[auth_dependency],
    )
    def update_gateway_session(
        session_id: DurableRecordId,
        request: GatewaySessionUpdateRequest,
    ) -> GatewaySession:
        try:
            existing = ctx.require_owned_gateway(session_id)
        except NotFoundError as exc:
            raise door_errors.http_problem("gateway_not_found", exc=exc) from exc
        if request.gateway is not None and _has_relay_managed_gateway_state(existing.gateway):
            raise door_errors.http_problem(
                "gateway_conflict",
                message=(
                    "relay-managed runtime gateway state can only be changed by the "
                    "runtime supervisor"
                ),
            )
        updates = request.model_dump(exclude={"state", "metadata"}, exclude_none=True)
        metadata = dict(request.metadata)
        if ctx.resolved.owner_session_id is not None:
            metadata.update(
                {
                    "owner": "clio-relay",
                    "owner_session_id": ctx.resolved.owner_session_id,
                }
            )
            if ctx.resolved.owner_session_generation_id is not None:
                metadata["owner_session_generation_id"] = ctx.resolved.owner_session_generation_id
        try:
            return _public_record(
                ctx.queue.update_gateway_session(
                    session_id,
                    state=request.state,
                    metadata=metadata,
                    reject_relay_managed_fields=True,
                    **updates,
                )
            )
        except QueueConflictError as exc:
            raise door_errors.http_problem(
                "gateway_conflict", exc=door_errors.public_message_error(exc)
            ) from exc
        except NotFoundError as exc:
            raise door_errors.http_problem("gateway_not_found", exc=exc) from exc

    @app.post(
        "/gateway-sessions/{session_id}/close",
        response_model=GatewaySession,
        dependencies=[auth_dependency],
    )
    def close_gateway_session(session_id: DurableRecordId) -> GatewaySession:
        try:
            ctx.require_owned_gateway(session_id)
            return _public_record(ctx.queue.close_gateway_session(session_id))
        except QueueConflictError as exc:
            raise door_errors.http_problem(
                "gateway_conflict", exc=door_errors.public_message_error(exc)
            ) from exc
        except NotFoundError as exc:
            raise door_errors.http_problem("gateway_not_found", exc=exc) from exc

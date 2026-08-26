"""Owner-session self-admission routes (iowarp/clio-relay#179 dial burn-down).

Two owned-session control-plane operations that previously had no HTTP
surface and so fell to a per-operation ``ssh ... clio-relay session
quiesce-intake`` / ``... session admission-status`` dial even when the
held channel was already live: quiescing this session's OWN intake for
teardown, and reading this session's OWN admission status. Both are scoped
to ``ctx.resolved.owner_session_id``/``owner_session_generation_id`` -- the
identity the held channel is already authenticated as -- exactly like
``GET /session-status`` (``http_api_routes_session.py``) and ``GET /queue``
(``http_api_routes_queue.py``) auto-scope, never a caller-supplied session
id. ``cli_owned_relay_jobs.py`` consumes both over
:meth:`~clio_relay.remote_connection.RemoteConnection.request_json` when a
live matching channel exists (:mod:`clio_relay.remote_channel_dispatch`),
falling back to the pre-existing ssh CLI subcommands with a typed,
recorded reason otherwise.
"""

# pyright: reportUnusedFunction=false

from __future__ import annotations

from fastapi import FastAPI
from fastapi.params import Depends

from clio_relay import door_errors
from clio_relay.http_api_context import RelayApiContext
from clio_relay.http_api_models import OwnerSessionQuiesceIntakeRequest

OWNER_SESSION_INTAKE_QUIESCED_SCHEMA = "clio-relay.owner-session-intake-quiesced.v1"


def register_owner_session_admin_routes(
    app: FastAPI,
    ctx: RelayApiContext,
    *,
    auth_dependency: Depends,
) -> None:
    """Register the owner-session self-quiesce/admission-status routes."""

    @app.post("/session/quiesce-intake", dependencies=[auth_dependency])
    def quiesce_owner_session_intake(
        request: OwnerSessionQuiesceIntakeRequest,
    ) -> dict[str, object]:
        """Quiesce this owned session's own intake under one cleanup operation id."""
        session_id = ctx.resolved.owner_session_id
        generation_id = ctx.resolved.owner_session_generation_id
        if session_id is None or generation_id is None:
            raise door_errors.http_problem(
                "session_intake_quiescence_unavailable",
                "owner-session intake quiescence requires an owned session",
            )
        intent = ctx.queue.set_owner_session_closing(
            session_id,
            session_generation_id=generation_id,
            operation_id=request.cleanup_operation_id,
            stop_worker=request.stop_worker,
            cancel_jobs=request.cancel_jobs,
            cancel_scheduler_jobs=request.cancel_scheduler_jobs,
        )
        return {
            "schema_version": OWNER_SESSION_INTAKE_QUIESCED_SCHEMA,
            "session_id": session_id,
            "session_generation_id": generation_id,
            "intake": "quiesced",
            "cleanup_intent": intent,
        }

    @app.get("/session/admission-status", dependencies=[auth_dependency])
    def owner_session_admission_status_route() -> dict[str, object]:
        """Read this owned session's own admission/intake status."""
        session_id = ctx.resolved.owner_session_id
        generation_id = ctx.resolved.owner_session_generation_id
        if session_id is None or generation_id is None:
            raise door_errors.http_problem(
                "session_admission_status_unavailable",
                "owner-session admission status requires an owned session",
            )
        return ctx.queue.owner_session_generation_status(
            session_id,
            session_generation_id=generation_id,
        )


__all__ = ["OWNER_SESSION_INTAKE_QUIESCED_SCHEMA", "register_owner_session_admin_routes"]

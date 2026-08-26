"""HTTP API for desktop-facing relay operations.

split/http-api-w3 (iowarp/clio-relay#231): this module is now a thin facade.
``create_app()`` still assembles the one FastAPI application (exception
handlers, the input-artifact body-limit middleware, the auth dependencies,
and the one ``RelayApiContext`` every route shares) but delegates the actual
route bodies to owner modules:

* ``http_api_context.py`` -- the shared ``RelayApiContext`` (the former
  ``create_app()``-local ``queue``/``resolved``/``owner_session_cluster``
  closures, now methods) and the owned-session cluster-authority binder.
* ``http_api_middleware.py`` -- ``InputArtifactBodyLimitMiddleware``.
* ``http_api_redaction.py`` -- the ``_public_record``/``_public_payload``/
  ``_public_model_page`` capability redaction helpers.
* ``http_api_queue_paging.py`` -- owner-session-scoped ``/queue`` paging.
* ``http_api_models.py`` -- every HTTP request Pydantic model.
* ``http_api_error_handlers.py`` -- the four global FastAPI exception
  handlers.
* ``http_api_auth.py`` -- the bearer-token / owner-session-header dependency
  factories.
* ``http_api_streaming.py`` -- the SSE/WebSocket payload generators.
* ``http_api_routes_session.py`` / ``_jobs.py`` / ``_events.py`` /
  ``_artifacts.py`` / ``_gateway.py`` / ``_queue.py`` -- the route bodies
  themselves, one ``register_*_routes(app, ctx, ...)`` call per module,
  invoked below in the same order the original file declared them in.

Every name external code or tests reached through ``clio_relay.http_api``
(``create_app``, the module-level ``app``, ``InputArtifactBodyLimitMiddleware``,
``JarvisMcpCallSubmitRequest``, ``OWNER_SESSION_ID_HEADER``,
``SESSION_GENERATION_ID_HEADER``, and the ``door_errors`` module reference)
stays importable from here under its original name.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import cast

from fastapi import Depends, FastAPI
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException

from clio_relay import door_errors
from clio_relay.config import RelaySettings
from clio_relay.errors import ConfigurationError
from clio_relay.http_api_auth import (
    _job_owner_session_identity,
    _require_api_token,
    _require_session_submission_binding,
)
from clio_relay.http_api_context import RelayApiContext, _bound_owner_session_cluster_definition
from clio_relay.http_api_error_handlers import (
    _relay_framework_http_handler,
    _relay_http_problem_handler,
    _relay_request_validation_handler,
    _relay_unhandled_exception_handler,
)
from clio_relay.http_api_middleware import InputArtifactBodyLimitMiddleware
from clio_relay.http_api_models import JarvisMcpCallSubmitRequest as JarvisMcpCallSubmitRequest
from clio_relay.http_api_routes_artifacts import register_artifact_routes
from clio_relay.http_api_routes_events import register_event_routes
from clio_relay.http_api_routes_gateway import register_gateway_routes
from clio_relay.http_api_routes_jobs import register_job_routes
from clio_relay.http_api_routes_owner_session_admin import register_owner_session_admin_routes
from clio_relay.http_api_routes_queue import register_queue_routes
from clio_relay.http_api_routes_scheduler import register_scheduler_routes
from clio_relay.http_api_routes_session import register_session_routes
from clio_relay.http_api_routes_worker_probe import register_worker_probe_routes
from clio_relay.job_identity import OWNER_SESSION_ID_HEADER as OWNER_SESSION_ID_HEADER
from clio_relay.job_identity import SESSION_GENERATION_ID_HEADER as SESSION_GENERATION_ID_HEADER
from clio_relay.job_identity import JobOwnerSessionIdentity
from clio_relay.storage_runtime import storage_managed_queue

INPUT_ARTIFACT_REQUEST_JSON_OVERHEAD_BYTES = 16 * 1024


def create_app(settings: RelaySettings | None = None) -> FastAPI:
    """Create the FastAPI relay surface."""
    resolved = settings or RelaySettings.from_env()
    owner_session_cluster = resolved.resolved_owner_session_cluster()
    if resolved.owner_session_id is not None:
        if not owner_session_cluster:
            raise ConfigurationError(
                "owned relay session API requires CLIO_RELAY_OWNER_SESSION_CLUSTER"
            )
        if not resolved.session_owner_token:
            raise ConfigurationError(
                "owned relay session API requires CLIO_RELAY_SESSION_OWNER_TOKEN"
            )
        if len(resolved.session_owner_token.encode("utf-8")) < 32:
            raise ConfigurationError(
                "owned relay session API requires a session owner token of at least 32 bytes"
            )
        if not (resolved.api_token or resolved.allow_unauthenticated_owned_session):
            raise ConfigurationError("owned relay session API requires CLIO_RELAY_API_TOKEN")
    owner_session_cluster_definition = _bound_owner_session_cluster_definition(
        owner_session_id=resolved.owner_session_id,
        owner_session_cluster=owner_session_cluster,
    )
    queue = storage_managed_queue(resolved)
    queue.initialize()

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncGenerator[None]:
        """Retain shared core ownership for the API process lifetime."""
        if queue.closed:
            raise RuntimeError("clio-relay API application cannot restart after shutdown")
        try:
            yield
        finally:
            queue.close()

    app = FastAPI(title="clio-relay", lifespan=lifespan)
    app.add_exception_handler(door_errors.HTTPProblemError, _relay_http_problem_handler)
    app.add_exception_handler(RequestValidationError, _relay_request_validation_handler)
    app.add_exception_handler(StarletteHTTPException, _relay_framework_http_handler)
    app.add_exception_handler(Exception, _relay_unhandled_exception_handler)
    app.add_middleware(
        InputArtifactBodyLimitMiddleware,
        max_body_bytes=(
            4 * ((resolved.input_file_max_bytes + 2) // 3)
            + INPUT_ARTIFACT_REQUEST_JSON_OVERHEAD_BYTES
        ),
        api_token=resolved.api_token,
        owner_session_id=resolved.owner_session_id,
        session_generation_id=resolved.owner_session_generation_id,
    )
    ctx = RelayApiContext(
        queue=queue,
        resolved=resolved,
        owner_session_cluster=owner_session_cluster,
        owner_session_cluster_definition=owner_session_cluster_definition,
    )

    # auth_dependency is built from ctx (not bare `resolved`) because it is
    # also the owned-session client-liveness lease's renewal chokepoint
    # (iowarp/clio-relay#277) -- see http_api_auth._require_api_token.
    auth_dependency = Depends(_require_api_token(ctx))
    session_submission_dependency = Depends(_require_session_submission_binding(resolved))
    job_identity_parameter = cast(
        JobOwnerSessionIdentity | None,
        Depends(_job_owner_session_identity()),
    )

    register_session_routes(
        app,
        ctx,
        auth_dependency=auth_dependency,
        session_submission_dependency=session_submission_dependency,
    )
    register_job_routes(
        app,
        ctx,
        auth_dependency=auth_dependency,
        job_identity_parameter=job_identity_parameter,
    )
    register_event_routes(app, ctx, auth_dependency=auth_dependency)
    register_artifact_routes(app, ctx, auth_dependency=auth_dependency)
    register_gateway_routes(
        app,
        ctx,
        auth_dependency=auth_dependency,
        session_submission_dependency=session_submission_dependency,
    )
    register_queue_routes(app, ctx, auth_dependency=auth_dependency)
    register_owner_session_admin_routes(app, ctx, auth_dependency=auth_dependency)
    if resolved.owner_session_id is not None:
        # clio-relay#179 review S1(a)/S2: scheduler status/status-batch/
        # cancel and worker-info/target-info are never registered on the
        # global app, where auth is a no-op once --require-token False lets
        # api_token stay None (proven: unauthenticated cancel). Both only
        # make sense scoped to one owned session anyway -- unlike
        # register_owner_session_admin_routes above, which already
        # self-gates on this exact same condition per request (its own
        # ctx.resolved.owner_session_id check), these carry no such guard
        # internally, so the registration itself is the gate.
        register_scheduler_routes(app, ctx, auth_dependency=auth_dependency)
        register_worker_probe_routes(app, ctx, auth_dependency=auth_dependency)

    return app


app = create_app()

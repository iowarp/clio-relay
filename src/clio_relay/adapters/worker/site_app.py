"""The worker's HTTP API on the site.

It answers four things over the held link: the identity challenge (no
credential), the session status, a shutdown request, and the adapter verbs
(:mod:`clio_relay.adapters.worker.site_routes`). Everything but the identity
challenge needs the bearer token and the session headers that placement fixed.
"""

from __future__ import annotations

import hmac
import os
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Annotated, Final

from fastapi import Depends, FastAPI, Header, HTTPException, Query

from clio_relay.adapters.worker.link import WORKER_PROTOCOL
from clio_relay.adapters.worker.site_routes import SiteContext, register_adapter_routes
from clio_relay.session_api import (
    OWNER_SESSION_ID_HEADER,
    SESSION_GENERATION_ID_HEADER,
    session_identity_document,
)

OWNED_SESSION_STATUS_SCHEMA: Final = "clio-relay.owned-session-status.v1"


@dataclass(frozen=True)
class SiteSettings:
    """What placement fixed for this worker (``run/settings.json``)."""

    cluster: str
    session_id: str
    generation_id: str
    api_token: str
    session_owner_token: str
    state_dir: Path
    port: int
    # Asks site-serve to exit; jobs run detached from it, so they keep running.
    on_shutdown: Callable[[], None] | None = field(default=None, compare=False)


def site_app(settings: SiteSettings) -> FastAPI:
    """Build the worker's API for one placed session."""
    app = FastAPI(title="clio-relay worker", docs_url=None, redoc_url=None, openapi_url=None)

    def authorized(
        authorization: Annotated[str | None, Header()] = None,
        session_id: Annotated[str | None, Header(alias=OWNER_SESSION_ID_HEADER)] = None,
        generation_id: Annotated[str | None, Header(alias=SESSION_GENERATION_ID_HEADER)] = None,
    ) -> None:
        scheme, _, token = (authorization or "").partition(" ")
        if scheme.lower() != "bearer" or not hmac.compare_digest(
            token.encode("utf-8"), settings.api_token.encode("utf-8")
        ):
            raise HTTPException(status_code=401, detail="not authorized")
        if session_id != settings.session_id or generation_id != settings.generation_id:
            raise HTTPException(status_code=409, detail="not this worker's session")

    auth = Depends(authorized)

    # The link's bring-up polls this, before it has proved whose worker answers.
    @app.get("/healthz")
    def healthz() -> dict[str, bool]:  # pyright: ignore[reportUnusedFunction]
        return {"ok": True}

    @app.get("/session-identity")
    def session_identity(  # pyright: ignore[reportUnusedFunction]
        nonce: Annotated[str, Query(pattern=r"^[0-9a-f]{64}$")],
    ) -> dict[str, str]:
        return session_identity_document(
            owner_token=settings.session_owner_token,
            cluster=settings.cluster,
            session_id=settings.session_id,
            generation_id=settings.generation_id,
            nonce=nonce,
        )

    @app.get("/session-status", dependencies=[auth])
    def session_status() -> dict[str, object]:  # pyright: ignore[reportUnusedFunction]
        return {
            "schema_version": OWNED_SESSION_STATUS_SCHEMA,
            "owner": "clio-relay",
            "cluster": settings.cluster,
            "session_id": settings.session_id,
            "session_generation_id": settings.generation_id,
            "remote_api_port": settings.port,
            "running": True,
            "evidence": "live_api_self_report",
            "worker_protocol": WORKER_PROTOCOL,
        }

    @app.post("/shutdown", dependencies=[auth])
    def shutdown() -> dict[str, object]:  # pyright: ignore[reportUnusedFunction]
        if settings.on_shutdown is None:
            raise HTTPException(status_code=501, detail="this worker cannot be stopped this way")
        settings.on_shutdown()
        return {"stopping": True, "pid": os.getpid()}

    register_adapter_routes(
        app,
        SiteContext(cluster=settings.cluster, state_dir=settings.state_dir),
        auth_dependency=auth,
    )
    return app

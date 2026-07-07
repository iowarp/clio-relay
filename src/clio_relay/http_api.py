"""HTTP API for desktop-facing relay operations."""

# pyright: reportUnusedFunction=false

from __future__ import annotations

import secrets
from collections.abc import Awaitable, Callable
from typing import Annotated

from fastapi import Depends, FastAPI, Header, HTTPException, status

from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.errors import NotFoundError
from clio_relay.models import ArtifactRef, Cursor, MonitorRule, RelayEvent, RelayJob
from clio_relay.relay_ops import (
    cancel_job as request_cancel_job,
)
from clio_relay.relay_ops import (
    evaluate_monitor_rules,
    monitor_job,
    read_artifact_bytes,
    read_job_log,
    wait_for_terminal,
)


def create_app(settings: RelaySettings | None = None) -> FastAPI:
    """Create the FastAPI relay surface."""
    resolved = settings or RelaySettings.from_env()
    queue = ClioCoreQueue(resolved.core_dir)
    queue.initialize()
    app = FastAPI(title="clio-relay")
    auth_dependency = Depends(_require_api_token(resolved))

    @app.get("/healthz")
    def healthz() -> dict[str, object]:
        return {"ok": True, "auth": resolved.api_token is not None}

    @app.post("/jobs", response_model=RelayJob, dependencies=[auth_dependency])
    def submit_job(job: RelayJob) -> RelayJob:
        return queue.submit_job(job)

    @app.get("/jobs/{job_id}", response_model=RelayJob, dependencies=[auth_dependency])
    def get_job(job_id: str) -> RelayJob:
        try:
            return queue.get_job(job_id)
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.get(
        "/jobs/{job_id}/events",
        response_model=list[RelayEvent],
        dependencies=[auth_dependency],
    )
    def get_events(job_id: str, cursor: int = 1, limit: int = 100) -> list[RelayEvent]:
        events, _ = queue.drain_events(Cursor(job_id=job_id, next_seq=cursor), limit=limit)
        return events

    @app.get("/jobs/{job_id}/monitor", dependencies=[auth_dependency])
    def monitor(job_id: str, cursor: int = 1, limit: int = 100) -> dict[str, object]:
        try:
            return monitor_job(queue, job_id, cursor=cursor, limit=limit)
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.post("/jobs/{job_id}/wait", response_model=RelayJob, dependencies=[auth_dependency])
    def wait(job_id: str, timeout_seconds: float = 600, poll_seconds: float = 2) -> RelayJob:
        try:
            return wait_for_terminal(
                queue,
                job_id,
                timeout_seconds=timeout_seconds,
                poll_seconds=poll_seconds,
            )
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except TimeoutError as exc:
            raise HTTPException(status_code=408, detail=str(exc)) from exc

    @app.get("/jobs/{job_id}/logs/{stream_name}", dependencies=[auth_dependency])
    def get_log(
        job_id: str,
        stream_name: str,
        offset: int = 0,
        limit: int = 65536,
    ) -> dict[str, object]:
        try:
            if stream_name not in {"stdout", "stderr"}:
                raise HTTPException(status_code=400, detail="stream must be stdout or stderr")
            return read_job_log(
                resolved,
                queue.get_job(job_id),
                stream_name="stdout" if stream_name == "stdout" else "stderr",
                offset=offset,
                limit=limit,
            )
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.get(
        "/jobs/{job_id}/artifacts",
        response_model=list[ArtifactRef],
        dependencies=[auth_dependency],
    )
    def get_artifacts(job_id: str) -> list[ArtifactRef]:
        return queue.list_artifacts(job_id)

    @app.get("/artifacts/{artifact_id}/content", dependencies=[auth_dependency])
    def get_artifact_content(artifact_id: str) -> dict[str, object]:
        try:
            return read_artifact_bytes(queue, artifact_id)
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.post("/jobs/{job_id}/cancel", response_model=RelayJob, dependencies=[auth_dependency])
    def cancel_job(job_id: str) -> RelayJob:
        return request_cancel_job(queue, job_id)

    @app.post("/monitor/rules", response_model=MonitorRule, dependencies=[auth_dependency])
    def create_monitor_rule(rule: MonitorRule) -> MonitorRule:
        try:
            return queue.append_monitor_rule(rule)
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.get("/monitor/rules", response_model=list[MonitorRule], dependencies=[auth_dependency])
    def list_monitor_rules(job_id: str | None = None) -> list[MonitorRule]:
        return queue.list_monitor_rules(job_id)

    @app.post("/monitor/run-once", dependencies=[auth_dependency])
    def run_monitor_once(limit: int = 100) -> list[dict[str, object]]:
        return evaluate_monitor_rules(queue, limit=limit)

    return app


def _require_api_token(settings: RelaySettings) -> Callable[..., Awaitable[None]]:
    async def dependency(
        authorization: Annotated[str | None, Header()] = None,
        x_clio_relay_token: Annotated[str | None, Header()] = None,
    ) -> None:
        if settings.api_token is None:
            return
        supplied = _extract_token(authorization, x_clio_relay_token)
        if supplied is None or not secrets.compare_digest(supplied, settings.api_token):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="missing or invalid relay API token",
            )

    return dependency


def _extract_token(authorization: str | None, header_token: str | None) -> str | None:
    if header_token:
        return header_token
    if authorization is None:
        return None
    scheme, _, token = authorization.partition(" ")
    if scheme.lower() != "bearer" or token == "":
        return None
    return token


app = create_app()

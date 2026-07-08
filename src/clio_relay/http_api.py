"""HTTP API for desktop-facing relay operations."""

# pyright: reportUnusedFunction=false

from __future__ import annotations

import asyncio
import json
import secrets
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Annotated

from fastapi import (
    Depends,
    FastAPI,
    Header,
    HTTPException,
    Query,
    WebSocket,
    WebSocketDisconnect,
    WebSocketException,
    status,
)
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, ConfigDict, Field

from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.errors import NotFoundError, QueueConflictError
from clio_relay.models import (
    ArtifactRef,
    Cursor,
    GatewaySession,
    GatewaySessionState,
    JarvisRunSpec,
    JobKind,
    McpCallSpec,
    MonitorRule,
    ProgressRecord,
    RelayEvent,
    RelayJob,
    RelayTask,
    RemoteAgentTaskSpec,
    TaskEventStatus,
    TaskTimelineEvent,
)
from clio_relay.progress_provenance import external_progress_metadata
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
from clio_relay.relay_ops import (
    job_status as get_job_status_operation,
)


class JarvisSubmitRequest(BaseModel):
    """HTTP request to submit a JARVIS pipeline YAML document."""

    model_config = ConfigDict(extra="forbid")

    cluster: str
    pipeline_yaml: str
    idempotency_key: str


class RemoteAgentSubmitRequest(BaseModel):
    """HTTP request to submit a remote-agent task."""

    model_config = ConfigDict(extra="forbid")

    cluster: str
    prompt_path: str
    mcp_config_path: str | None = None
    model: str | None = None
    workdir: str | None = None
    timeout_seconds: int | None = Field(default=None, gt=0)
    idempotency_key: str


class McpCallSubmitRequest(BaseModel):
    """HTTP request to submit a remote MCP tool call."""

    model_config = ConfigDict(extra="forbid")

    cluster: str
    server: str
    tool: str
    arguments: dict[str, object] = Field(default_factory=dict)
    timeout_seconds: int | None = Field(default=None, gt=0)
    idempotency_key: str


class ProgressUpdateRequest(BaseModel):
    """HTTP request to record a job progress observation."""

    model_config = ConfigDict(extra="forbid")

    label: str = "progress"
    current: float | None = None
    total: float | None = Field(default=None, gt=0)
    unit: str | None = None
    message: str | None = None
    source_event_seq: int | None = Field(default=None, ge=1)
    metadata: dict[str, object] = Field(default_factory=dict)


class TaskTimelineEventRequest(BaseModel):
    """HTTP request to append a structured task timeline event."""

    model_config = ConfigDict(extra="forbid")

    event_type: str
    label: str
    status: TaskEventStatus = TaskEventStatus.RUNNING
    summary: str
    detail: str | None = None
    artifact_refs: list[str] = Field(default_factory=list)
    path_refs: list[str] = Field(default_factory=list)
    metadata: dict[str, object] = Field(default_factory=dict)


class GatewaySessionCreateRequest(BaseModel):
    """HTTP request to create a scheduler-backed gateway session."""

    model_config = ConfigDict(extra="forbid")

    cluster: str
    name: str
    state: GatewaySessionState = GatewaySessionState.CREATED
    scheduler: str = "slurm"
    scheduler_job_id: str | None = None
    queue_state: str | None = None
    node: str | None = None
    requested_resources: dict[str, object] = Field(default_factory=dict)
    stdout_uri: str | None = None
    stderr_uri: str | None = None
    log_uris: list[str] = Field(default_factory=list)
    gateway: dict[str, object] = Field(default_factory=dict)
    metadata: dict[str, object] = Field(default_factory=dict)


class GatewaySessionUpdateRequest(BaseModel):
    """HTTP request to update scheduler-backed gateway session state."""

    model_config = ConfigDict(extra="forbid")

    state: GatewaySessionState | None = None
    scheduler_job_id: str | None = None
    queue_state: str | None = None
    node: str | None = None
    requested_resources: dict[str, object] | None = None
    stdout_uri: str | None = None
    stderr_uri: str | None = None
    log_uris: list[str] | None = None
    gateway: dict[str, object] | None = None
    artifacts: list[str] | None = None
    metadata: dict[str, object] = Field(default_factory=dict)


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

    @app.post("/jobs/jarvis", response_model=RelayJob, dependencies=[auth_dependency])
    def submit_jarvis(request: JarvisSubmitRequest) -> RelayJob:
        return queue.submit_job(
            RelayJob(
                cluster=request.cluster,
                kind=JobKind.JARVIS,
                spec=JarvisRunSpec(pipeline_yaml=request.pipeline_yaml),
                idempotency_key=request.idempotency_key,
            )
        )

    @app.post("/jobs/remote-agent", response_model=RelayJob, dependencies=[auth_dependency])
    def submit_remote_agent(request: RemoteAgentSubmitRequest) -> RelayJob:
        return queue.submit_job(
            RelayJob(
                cluster=request.cluster,
                kind=JobKind.REMOTE_AGENT,
                spec=RemoteAgentTaskSpec(
                    prompt_path=request.prompt_path,
                    mcp_config_path=request.mcp_config_path,
                    model=request.model,
                    workdir=request.workdir,
                    timeout_seconds=request.timeout_seconds,
                ),
                idempotency_key=request.idempotency_key,
            )
        )

    @app.post("/jobs/mcp-call", response_model=RelayJob, dependencies=[auth_dependency])
    def submit_mcp_call(request: McpCallSubmitRequest) -> RelayJob:
        return queue.submit_job(
            RelayJob(
                cluster=request.cluster,
                kind=JobKind.MCP_CALL,
                spec=McpCallSpec(
                    server=request.server,
                    tool=request.tool,
                    arguments=request.arguments,
                    timeout_seconds=request.timeout_seconds,
                ),
                idempotency_key=request.idempotency_key,
            )
        )

    @app.get("/jobs/{job_id}", response_model=RelayJob, dependencies=[auth_dependency])
    def get_job(job_id: str) -> RelayJob:
        try:
            return queue.get_job(job_id)
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.get("/jobs/{job_id}/status", dependencies=[auth_dependency])
    def get_job_status(job_id: str) -> dict[str, object]:
        try:
            return get_job_status_operation(queue, job_id)
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

    @app.get(
        "/jobs/{job_id}/tasks",
        response_model=list[RelayTask],
        dependencies=[auth_dependency],
    )
    def get_tasks(job_id: str) -> list[RelayTask]:
        return queue.list_tasks(job_id)

    @app.get(
        "/tasks/{task_id}/events",
        response_model=list[TaskTimelineEvent],
        dependencies=[auth_dependency],
    )
    def get_task_events(
        task_id: str,
        cursor: Annotated[int, Query(ge=1)] = 1,
        limit: Annotated[int, Query(ge=1)] = 100,
    ) -> list[TaskTimelineEvent]:
        try:
            events, _ = queue.drain_task_events(task_id, cursor=cursor, limit=limit)
            return events
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.post(
        "/tasks/{task_id}/events",
        response_model=TaskTimelineEvent,
        dependencies=[auth_dependency],
    )
    def append_task_event(
        task_id: str,
        request: TaskTimelineEventRequest,
    ) -> TaskTimelineEvent:
        try:
            return queue.append_task_event(
                TaskTimelineEvent(
                    task_id=task_id,
                    event_type=request.event_type,
                    label=request.label,
                    status=request.status,
                    summary=request.summary,
                    detail=request.detail,
                    artifact_refs=request.artifact_refs,
                    path_refs=request.path_refs,
                    metadata=request.metadata,
                )
            )
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.get("/tasks/{task_id}/events/sse", dependencies=[auth_dependency])
    def task_events_sse(
        task_id: str,
        cursor: Annotated[int, Query(ge=1)] = 1,
        limit: Annotated[int, Query(ge=1)] = 100,
        poll_seconds: float = 1.0,
        stop_after_replay: bool = False,
    ) -> StreamingResponse:
        """Stream task timeline events as Server-Sent Events."""
        if poll_seconds <= 0:
            raise HTTPException(status_code=400, detail="poll_seconds must be positive")
        try:
            queue.get_task(task_id)
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return StreamingResponse(
            _task_sse_events(
                queue,
                task_id,
                cursor=cursor,
                limit=limit,
                poll_seconds=poll_seconds,
                stop_after_replay=stop_after_replay,
            ),
            media_type="text/event-stream",
        )

    @app.websocket("/tasks/{task_id}/events/ws")
    async def task_events_ws(
        websocket: WebSocket,
        task_id: str,
        cursor: int = 1,
        limit: int = 100,
        poll_seconds: float = 1.0,
    ) -> None:
        """Stream task timeline events over a WebSocket."""
        _require_websocket_token(resolved, websocket)
        if poll_seconds <= 0 or cursor < 1 or limit < 1:
            raise WebSocketException(code=status.WS_1008_POLICY_VIOLATION)
        try:
            queue.get_task(task_id)
        except NotFoundError as exc:
            raise WebSocketException(code=status.WS_1008_POLICY_VIOLATION) from exc
        await websocket.accept()
        try:
            async for payload in _task_stream_payloads(
                queue,
                task_id,
                cursor=cursor,
                limit=limit,
                poll_seconds=poll_seconds,
            ):
                await websocket.send_json(payload)
        except WebSocketDisconnect:
            return

    @app.get("/jobs/{job_id}/monitor", dependencies=[auth_dependency])
    def monitor(job_id: str, cursor: int = 1, limit: int = 100) -> dict[str, object]:
        try:
            return monitor_job(queue, job_id, cursor=cursor, limit=limit)
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.get("/jobs/{job_id}/monitor/sse", dependencies=[auth_dependency])
    def monitor_sse(
        job_id: str,
        cursor: int = 1,
        limit: int = 100,
        poll_seconds: float = 1.0,
        stop_on_terminal: bool = True,
    ) -> StreamingResponse:
        """Stream job monitor updates as Server-Sent Events."""
        if poll_seconds <= 0:
            raise HTTPException(status_code=400, detail="poll_seconds must be positive")
        try:
            queue.get_job(job_id)
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return StreamingResponse(
            _monitor_sse_events(
                queue,
                job_id,
                cursor=cursor,
                limit=limit,
                poll_seconds=poll_seconds,
                stop_on_terminal=stop_on_terminal,
            ),
            media_type="text/event-stream",
        )

    @app.websocket("/jobs/{job_id}/monitor/ws")
    async def monitor_ws(
        websocket: WebSocket,
        job_id: str,
        cursor: int = 1,
        limit: int = 100,
        poll_seconds: float = 1.0,
        stop_on_terminal: bool = True,
    ) -> None:
        """Stream job monitor updates over a WebSocket."""
        _require_websocket_token(resolved, websocket)
        if poll_seconds <= 0:
            raise WebSocketException(code=status.WS_1008_POLICY_VIOLATION)
        try:
            queue.get_job(job_id)
        except NotFoundError as exc:
            raise WebSocketException(code=status.WS_1008_POLICY_VIOLATION) from exc
        await websocket.accept()
        try:
            async for payload in _monitor_stream_payloads(
                queue,
                job_id,
                cursor=cursor,
                limit=limit,
                poll_seconds=poll_seconds,
                stop_on_terminal=stop_on_terminal,
            ):
                await websocket.send_json(payload)
                if payload["event"] == "terminal":
                    await websocket.close()
                    return
        except WebSocketDisconnect:
            return

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

    @app.get(
        "/jobs/{job_id}/progress",
        response_model=list[ProgressRecord],
        dependencies=[auth_dependency],
    )
    def get_progress(job_id: str) -> list[ProgressRecord]:
        return queue.list_progress(job_id)

    @app.post(
        "/gateway-sessions",
        response_model=GatewaySession,
        dependencies=[auth_dependency],
    )
    def create_gateway_session(request: GatewaySessionCreateRequest) -> GatewaySession:
        return queue.create_gateway_session(
            GatewaySession(
                cluster=request.cluster,
                name=request.name,
                state=request.state,
                scheduler=request.scheduler,
                scheduler_job_id=request.scheduler_job_id,
                queue_state=request.queue_state,
                node=request.node,
                requested_resources=request.requested_resources,
                stdout_uri=request.stdout_uri,
                stderr_uri=request.stderr_uri,
                log_uris=request.log_uris,
                gateway=request.gateway,
                metadata=request.metadata,
            )
        )

    @app.get(
        "/gateway-sessions",
        response_model=list[GatewaySession],
        dependencies=[auth_dependency],
    )
    def list_gateway_sessions(cluster: str | None = None) -> list[GatewaySession]:
        return queue.list_gateway_sessions(cluster=cluster)

    @app.get(
        "/gateway-sessions/{session_id}",
        response_model=GatewaySession,
        dependencies=[auth_dependency],
    )
    def get_gateway_session(session_id: str) -> GatewaySession:
        try:
            return queue.get_gateway_session(session_id)
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.patch(
        "/gateway-sessions/{session_id}",
        response_model=GatewaySession,
        dependencies=[auth_dependency],
    )
    def update_gateway_session(
        session_id: str,
        request: GatewaySessionUpdateRequest,
    ) -> GatewaySession:
        updates = request.model_dump(exclude={"state", "metadata"}, exclude_none=True)
        try:
            return queue.update_gateway_session(
                session_id,
                state=request.state,
                metadata=request.metadata,
                **updates,
            )
        except QueueConflictError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.post(
        "/gateway-sessions/{session_id}/close",
        response_model=GatewaySession,
        dependencies=[auth_dependency],
    )
    def close_gateway_session(session_id: str) -> GatewaySession:
        try:
            return queue.close_gateway_session(session_id)
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.post(
        "/jobs/{job_id}/progress",
        response_model=ProgressRecord,
        dependencies=[auth_dependency],
    )
    def record_progress(job_id: str, request: ProgressUpdateRequest) -> ProgressRecord:
        try:
            metadata = external_progress_metadata("external_http", dict(request.metadata))
            return queue.append_progress(
                ProgressRecord(
                    job_id=job_id,
                    label=request.label,
                    current=request.current,
                    total=request.total,
                    unit=request.unit,
                    message=request.message,
                    source_event_seq=request.source_event_seq,
                    metadata=metadata,
                )
            )
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

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


async def _monitor_sse_events(
    queue: ClioCoreQueue,
    job_id: str,
    *,
    cursor: int,
    limit: int,
    poll_seconds: float,
    stop_on_terminal: bool,
) -> AsyncIterator[str]:
    async for payload in _monitor_stream_payloads(
        queue,
        job_id,
        cursor=cursor,
        limit=limit,
        poll_seconds=poll_seconds,
        stop_on_terminal=stop_on_terminal,
    ):
        yield f"event: {payload['event']}\ndata: {json.dumps(payload['data'], default=str)}\n\n"


async def _task_sse_events(
    queue: ClioCoreQueue,
    task_id: str,
    *,
    cursor: int,
    limit: int,
    poll_seconds: float,
    stop_after_replay: bool,
) -> AsyncIterator[str]:
    async for payload in _task_stream_payloads(
        queue,
        task_id,
        cursor=cursor,
        limit=limit,
        poll_seconds=poll_seconds,
        stop_after_replay=stop_after_replay,
    ):
        yield f"event: {payload['event']}\ndata: {json.dumps(payload['data'], default=str)}\n\n"


async def _task_stream_payloads(
    queue: ClioCoreQueue,
    task_id: str,
    *,
    cursor: int,
    limit: int,
    poll_seconds: float,
    stop_after_replay: bool = False,
) -> AsyncIterator[dict[str, object]]:
    next_cursor = cursor
    while True:
        events, next_cursor = queue.drain_task_events(
            task_id,
            cursor=next_cursor,
            limit=limit,
        )
        if events:
            yield {
                "event": "task_events",
                "data": {
                    "task_id": task_id,
                    "events": [event.model_dump(mode="json") for event in events],
                    "next_cursor": next_cursor,
                },
            }
            if stop_after_replay:
                return
        elif stop_after_replay:
            return
        await asyncio.sleep(poll_seconds)


async def _monitor_stream_payloads(
    queue: ClioCoreQueue,
    job_id: str,
    *,
    cursor: int,
    limit: int,
    poll_seconds: float,
    stop_on_terminal: bool,
) -> AsyncIterator[dict[str, object]]:
    next_cursor = cursor
    while True:
        payload = monitor_job(queue, job_id, cursor=next_cursor, limit=limit)
        raw_next_cursor = payload["next_cursor"]
        if not isinstance(raw_next_cursor, int):
            raise TypeError("monitor payload next_cursor was not an integer")
        next_cursor = raw_next_cursor
        yield {"event": "monitor", "data": payload}
        job = queue.get_job(job_id)
        if stop_on_terminal and job.state.value in {"succeeded", "failed", "canceled"}:
            yield {"event": "terminal", "data": {"job_id": job_id, "state": job.state.value}}
            return
        await asyncio.sleep(poll_seconds)


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


def _require_websocket_token(settings: RelaySettings, websocket: WebSocket) -> None:
    if settings.api_token is None:
        return
    supplied = websocket.query_params.get("token")
    if supplied is None:
        supplied = _extract_token(websocket.headers.get("authorization"), None)
    if supplied is None or not secrets.compare_digest(supplied, settings.api_token):
        raise WebSocketException(code=status.WS_1008_POLICY_VIOLATION)


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

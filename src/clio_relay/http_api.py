"""HTTP API for desktop-facing relay operations."""

# pyright: reportUnusedFunction=false

from __future__ import annotations

import asyncio
import json
import secrets
from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import datetime
from typing import Annotated, TypeVar, cast

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
from pydantic import BaseModel, ConfigDict, Field, field_validator

from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.errors import ConfigurationError, NotFoundError, QueueConflictError
from clio_relay.jarvis_mcp import jarvis_mcp_server, jarvis_mcp_server_args
from clio_relay.models import (
    ArtifactRef,
    Cursor,
    GatewaySession,
    GatewaySessionState,
    JarvisRunSpec,
    JobKind,
    JobState,
    McpCallSpec,
    MonitorRule,
    ProgressRecord,
    RelayEvent,
    RelayJob,
    RelayTask,
    RemoteAgentTaskSpec,
    TaskEventStatus,
    TaskTimelineEvent,
    validate_mcp_env_from,
)
from clio_relay.pagination import (
    DEFAULT_RESPONSE_PAGE_RECORDS,
    MAX_RESPONSE_PAGE_RECORDS,
    validate_response_page_limit,
)
from clio_relay.progress_provenance import external_progress_metadata
from clio_relay.queue_management import (
    cancel_queue_job,
    cleanup_stale_jobs,
    diagnose_job,
    diagnose_queue,
    discover_stale_jobs,
    list_queue_jobs,
    worker_status,
)
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
from clio_relay.retention import TerminalRetentionCoordinator
from clio_relay.storage_runtime import StorageAdmissionError, storage_managed_queue
from clio_relay.validation_report import redact_sensitive_values

ModelRecord = TypeVar("ModelRecord", bound=BaseModel)


def _public_record(record: ModelRecord) -> ModelRecord:  # noqa: UP047
    """Return a response copy with nested capability values redacted."""
    original = record.model_dump(mode="json")
    payload = _restore_environment_references(original, redact_sensitive_values(original))
    return type(record).model_validate(payload)


def _public_payload(payload: dict[str, object]) -> dict[str, object]:
    """Redact nested capability values from a free-form HTTP payload."""
    redacted = _restore_environment_references(payload, redact_sensitive_values(payload))
    return cast(dict[str, object], redacted)


def _public_model_page(  # noqa: UP047
    record_key: str,
    records: list[ModelRecord],
    *,
    cursor: int,
    limit: int,
    next_cursor: int | None,
    total: int,
) -> dict[str, object]:
    """Return a redacted, stable one-based model collection page."""
    return {
        record_key: [record.model_dump(mode="json") for record in records],
        "cursor": cursor,
        "limit": limit,
        "next_cursor": next_cursor,
        "total": total,
    }


def _restore_environment_references(original: object, redacted: object) -> object:
    """Keep non-secret env_from variable names valid after capability redaction."""
    if isinstance(original, dict) and isinstance(redacted, dict):
        original_mapping = cast(dict[object, object], original)
        redacted_mapping = cast(dict[object, object], redacted)
        restored: dict[object, object] = {}
        for key, value in redacted_mapping.items():
            original_value = original_mapping.get(key)
            restored[key] = (
                original_value
                if key == "env_from" and isinstance(original_value, dict)
                else _restore_environment_references(original_value, value)
            )
        return restored
    if isinstance(original, list) and isinstance(redacted, list):
        original_values = cast(list[object], original)
        redacted_values = cast(list[object], redacted)
        return [
            _restore_environment_references(original_value, redacted_value)
            for original_value, redacted_value in zip(
                original_values,
                redacted_values,
                strict=False,
            )
        ]
    return redacted


class JarvisSubmitRequest(BaseModel):
    """HTTP request to submit a JARVIS pipeline YAML document."""

    model_config = ConfigDict(extra="forbid")

    cluster: str
    pipeline_yaml: str
    idempotency_key: str


class JarvisPipelineSubmitRequest(BaseModel):
    """HTTP request to submit an existing JARVIS pipeline by name."""

    model_config = ConfigDict(extra="forbid")

    cluster: str
    pipeline_name: str
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
    server_args: list[str] = Field(default_factory=list)
    env_from: dict[str, str] = Field(default_factory=dict)
    tool: str
    arguments: dict[str, object] = Field(default_factory=dict)
    timeout_seconds: int | None = Field(default=None, gt=0)
    idempotency_key: str

    @field_validator("env_from")
    @classmethod
    def validate_environment_references(cls, value: dict[str, str]) -> dict[str, str]:
        """Reject invalid names and relay-owned credential references."""
        return validate_mcp_env_from(value)


class JarvisMcpCallSubmitRequest(BaseModel):
    """HTTP request to submit a remote JARVIS MCP tool call."""

    model_config = ConfigDict(extra="forbid")

    cluster: str
    tool: str
    arguments: dict[str, object] = Field(default_factory=dict)
    timeout_seconds: int | None = Field(default=None, gt=0)
    idempotency_key: str


class QueueCancelRequest(BaseModel):
    """HTTP request to cancel a relay job with explicit scheduler policy."""

    model_config = ConfigDict(extra="forbid")

    cluster: str | None = None
    cancel_scheduler_job: bool = False


class RetentionCollectRequest(BaseModel):
    """HTTP request to preview or advance bounded terminal retention."""

    model_config = ConfigDict(extra="forbid")

    execute: bool = False
    batch_size: int = Field(default=100, ge=1, le=100)
    expected_updated_at: datetime | None = None


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
    scheduler: str = "external"
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
    queue = storage_managed_queue(resolved)
    queue.initialize()
    app = FastAPI(title="clio-relay")
    auth_dependency = Depends(_require_api_token(resolved))

    def ensure_intake_open() -> None:
        if resolved.owner_session_id is not None and queue.owner_session_is_closing(
            resolved.owner_session_id
        ):
            raise HTTPException(
                status_code=409,
                detail="relay session is closing and no longer accepts new work",
            )

    def owns_job(job: RelayJob) -> bool:
        return resolved.owner_session_id is None or (
            job.metadata.get("owner") == "clio-relay"
            and job.metadata.get("owner_session_id") == resolved.owner_session_id
            and job.metadata.get("owner_session_generation_id")
            == resolved.owner_session_generation_id
        )

    def require_owned_job(job_id: str) -> RelayJob:
        job = queue.get_job(job_id)
        if not owns_job(job):
            raise HTTPException(status_code=403, detail="job is not owned by this relay session")
        return job

    def require_owned_task(task_id: str) -> RelayTask:
        task = queue.get_task(task_id)
        require_owned_job(task.job_id)
        return task

    def require_owned_artifact(artifact_id: str) -> ArtifactRef:
        artifact = queue.get_artifact(artifact_id)
        require_owned_job(artifact.job_id)
        return artifact

    def submit_owned(job: RelayJob) -> RelayJob:
        ensure_intake_open()
        metadata = dict(job.metadata)
        if resolved.owner_session_id is not None:
            metadata.update(
                {
                    "owner": "clio-relay",
                    "owner_session_id": resolved.owner_session_id,
                }
            )
            if resolved.owner_session_generation_id is not None:
                metadata["owner_session_generation_id"] = resolved.owner_session_generation_id
        try:
            return _public_record(queue.submit_job(job.model_copy(update={"metadata": metadata})))
        except StorageAdmissionError as exc:
            raise HTTPException(status_code=507, detail=exc.decision.to_dict()) from exc

    def require_owned_gateway(session_id: str) -> GatewaySession:
        session = queue.get_gateway_session(session_id)
        if resolved.owner_session_id is None:
            return session
        if (
            session.metadata.get("owner") != "clio-relay"
            or session.metadata.get("owner_session_id") != resolved.owner_session_id
            or session.metadata.get("owner_session_generation_id")
            != resolved.owner_session_generation_id
        ):
            raise HTTPException(
                status_code=403,
                detail="gateway session is not owned by this relay session",
            )
        return session

    @app.get("/healthz")
    def healthz() -> dict[str, object]:
        return {"ok": True, "auth": resolved.api_token is not None}

    @app.get("/storage/status", dependencies=[auth_dependency])
    def storage_status() -> dict[str, object]:
        """Return the machine-readable queue admission and storage decision."""
        return _public_payload(queue.storage_runtime.status())

    @app.post("/jobs", response_model=RelayJob, dependencies=[auth_dependency])
    def submit_job(job: RelayJob) -> RelayJob:
        return submit_owned(job)

    @app.post("/jobs/jarvis", response_model=RelayJob, dependencies=[auth_dependency])
    def submit_jarvis(request: JarvisSubmitRequest) -> RelayJob:
        return submit_owned(
            RelayJob(
                cluster=request.cluster,
                kind=JobKind.JARVIS,
                spec=JarvisRunSpec(pipeline_yaml=request.pipeline_yaml),
                idempotency_key=request.idempotency_key,
            )
        )

    @app.post("/jobs/jarvis-pipeline", response_model=RelayJob, dependencies=[auth_dependency])
    def submit_jarvis_pipeline(request: JarvisPipelineSubmitRequest) -> RelayJob:
        return submit_owned(
            RelayJob(
                cluster=request.cluster,
                kind=JobKind.JARVIS,
                spec=JarvisRunSpec(pipeline_name=request.pipeline_name),
                idempotency_key=request.idempotency_key,
            )
        )

    @app.post("/jobs/remote-agent", response_model=RelayJob, dependencies=[auth_dependency])
    def submit_remote_agent(request: RemoteAgentSubmitRequest) -> RelayJob:
        return submit_owned(
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
        return submit_owned(
            RelayJob(
                cluster=request.cluster,
                kind=JobKind.MCP_CALL,
                spec=McpCallSpec(
                    server=request.server,
                    server_args=request.server_args,
                    env_from=request.env_from,
                    tool=request.tool,
                    arguments=request.arguments,
                    timeout_seconds=request.timeout_seconds,
                ),
                idempotency_key=request.idempotency_key,
            )
        )

    @app.post("/jobs/jarvis-mcp-call", response_model=RelayJob, dependencies=[auth_dependency])
    def submit_jarvis_mcp_call(request: JarvisMcpCallSubmitRequest) -> RelayJob:
        return submit_owned(
            RelayJob(
                cluster=request.cluster,
                kind=JobKind.MCP_CALL,
                spec=McpCallSpec(
                    server=jarvis_mcp_server(),
                    server_args=jarvis_mcp_server_args(),
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
            return _public_record(require_owned_job(job_id))
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.get("/jobs/{job_id}/status", dependencies=[auth_dependency])
    def get_job_status(job_id: str) -> dict[str, object]:
        try:
            require_owned_job(job_id)
            return _public_payload(get_job_status_operation(queue, job_id))
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.get(
        "/jobs/{job_id}/events",
        response_model=list[RelayEvent],
        dependencies=[auth_dependency],
    )
    def get_events(
        job_id: str,
        cursor: Annotated[int, Query(ge=1)] = 1,
        limit: Annotated[int, Query(ge=1, le=MAX_RESPONSE_PAGE_RECORDS)] = (
            DEFAULT_RESPONSE_PAGE_RECORDS
        ),
    ) -> list[RelayEvent]:
        require_owned_job(job_id)
        events, _ = queue.drain_events(Cursor(job_id=job_id, next_seq=cursor), limit=limit)
        return [_public_record(event) for event in events]

    @app.get(
        "/jobs/{job_id}/tasks",
        dependencies=[auth_dependency],
    )
    def get_tasks(
        job_id: str,
        cursor: Annotated[int, Query(ge=1)] = 1,
        limit: Annotated[int, Query(ge=1, le=MAX_RESPONSE_PAGE_RECORDS)] = (
            DEFAULT_RESPONSE_PAGE_RECORDS
        ),
    ) -> dict[str, object]:
        require_owned_job(job_id)
        tasks, next_cursor, total = queue.list_tasks_page(
            job_id,
            cursor=cursor,
            limit=limit,
        )
        return _public_payload(
            _public_model_page(
                "tasks",
                tasks,
                cursor=cursor,
                limit=limit,
                next_cursor=next_cursor,
                total=total,
            )
        )

    @app.get(
        "/tasks/{task_id}/events",
        response_model=list[TaskTimelineEvent],
        dependencies=[auth_dependency],
    )
    def get_task_events(
        task_id: str,
        cursor: Annotated[int, Query(ge=1)] = 1,
        limit: Annotated[int, Query(ge=1, le=MAX_RESPONSE_PAGE_RECORDS)] = (
            DEFAULT_RESPONSE_PAGE_RECORDS
        ),
    ) -> list[TaskTimelineEvent]:
        try:
            require_owned_task(task_id)
            events, _ = queue.drain_task_events(task_id, cursor=cursor, limit=limit)
            return [_public_record(event) for event in events]
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
            require_owned_task(task_id)
            return _public_record(
                queue.append_task_event(
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
            )
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.get("/tasks/{task_id}/events/sse", dependencies=[auth_dependency])
    def task_events_sse(
        task_id: str,
        cursor: Annotated[int, Query(ge=1)] = 1,
        limit: Annotated[int, Query(ge=1, le=MAX_RESPONSE_PAGE_RECORDS)] = (
            DEFAULT_RESPONSE_PAGE_RECORDS
        ),
        poll_seconds: float = 1.0,
        stop_after_replay: bool = False,
    ) -> StreamingResponse:
        """Stream task timeline events as Server-Sent Events."""
        if poll_seconds <= 0:
            raise HTTPException(status_code=400, detail="poll_seconds must be positive")
        try:
            require_owned_task(task_id)
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
        limit: int = DEFAULT_RESPONSE_PAGE_RECORDS,
        poll_seconds: float = 1.0,
    ) -> None:
        """Stream task timeline events over a WebSocket."""
        _require_websocket_token(resolved, websocket)
        if poll_seconds <= 0 or cursor < 1:
            raise WebSocketException(code=status.WS_1008_POLICY_VIOLATION)
        _require_websocket_page_limit(limit)
        try:
            require_owned_task(task_id)
        except (NotFoundError, HTTPException) as exc:
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
    def monitor(
        job_id: str,
        cursor: Annotated[int, Query(ge=1)] = 1,
        limit: Annotated[int, Query(ge=1, le=MAX_RESPONSE_PAGE_RECORDS)] = (
            DEFAULT_RESPONSE_PAGE_RECORDS
        ),
    ) -> dict[str, object]:
        try:
            require_owned_job(job_id)
            return _public_payload(monitor_job(queue, job_id, cursor=cursor, limit=limit))
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.get("/jobs/{job_id}/monitor/sse", dependencies=[auth_dependency])
    def monitor_sse(
        job_id: str,
        cursor: Annotated[int, Query(ge=1)] = 1,
        limit: Annotated[int, Query(ge=1, le=MAX_RESPONSE_PAGE_RECORDS)] = (
            DEFAULT_RESPONSE_PAGE_RECORDS
        ),
        poll_seconds: float = 1.0,
        stop_on_terminal: bool = True,
    ) -> StreamingResponse:
        """Stream job monitor updates as Server-Sent Events."""
        if poll_seconds <= 0:
            raise HTTPException(status_code=400, detail="poll_seconds must be positive")
        try:
            require_owned_job(job_id)
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
        limit: int = DEFAULT_RESPONSE_PAGE_RECORDS,
        poll_seconds: float = 1.0,
        stop_on_terminal: bool = True,
    ) -> None:
        """Stream job monitor updates over a WebSocket."""
        _require_websocket_token(resolved, websocket)
        if poll_seconds <= 0 or cursor < 1:
            raise WebSocketException(code=status.WS_1008_POLICY_VIOLATION)
        _require_websocket_page_limit(limit)
        try:
            require_owned_job(job_id)
        except (NotFoundError, HTTPException) as exc:
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
            require_owned_job(job_id)
            return _public_record(
                wait_for_terminal(
                    queue,
                    job_id,
                    timeout_seconds=timeout_seconds,
                    poll_seconds=poll_seconds,
                )
            )
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except TimeoutError as exc:
            raise HTTPException(status_code=408, detail=str(exc)) from exc

    @app.get("/jobs/{job_id}/logs/{stream_name}", dependencies=[auth_dependency])
    def get_log(
        job_id: str,
        stream_name: str,
        offset: Annotated[int, Query(ge=0)] = 0,
        limit: Annotated[int, Query(ge=1, le=1_048_576)] = 65_536,
    ) -> dict[str, object]:
        try:
            if stream_name not in {"stdout", "stderr"}:
                raise HTTPException(status_code=400, detail="stream must be stdout or stderr")
            return _public_payload(
                read_job_log(
                    resolved,
                    require_owned_job(job_id),
                    stream_name="stdout" if stream_name == "stdout" else "stderr",
                    offset=offset,
                    limit=limit,
                )
            )
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.get(
        "/jobs/{job_id}/artifacts",
        dependencies=[auth_dependency],
    )
    def get_artifacts(
        job_id: str,
        cursor: Annotated[int, Query(ge=1)] = 1,
        limit: Annotated[int, Query(ge=1, le=MAX_RESPONSE_PAGE_RECORDS)] = (
            DEFAULT_RESPONSE_PAGE_RECORDS
        ),
    ) -> dict[str, object]:
        require_owned_job(job_id)
        artifacts, next_cursor, total = queue.list_artifacts_page(
            job_id,
            cursor=cursor,
            limit=limit,
        )
        return _public_payload(
            _public_model_page(
                "artifacts",
                artifacts,
                cursor=cursor,
                limit=limit,
                next_cursor=next_cursor,
                total=total,
            )
        )

    @app.get(
        "/jobs/{job_id}/progress",
        dependencies=[auth_dependency],
    )
    def get_progress(
        job_id: str,
        cursor: Annotated[int, Query(ge=1)] = 1,
        limit: Annotated[int, Query(ge=1, le=MAX_RESPONSE_PAGE_RECORDS)] = (
            DEFAULT_RESPONSE_PAGE_RECORDS
        ),
    ) -> dict[str, object]:
        require_owned_job(job_id)
        progress, next_cursor, total = queue.list_progress_page(
            job_id,
            cursor=cursor,
            limit=limit,
        )
        return _public_payload(
            _public_model_page(
                "progress",
                progress,
                cursor=cursor,
                limit=limit,
                next_cursor=next_cursor,
                total=total,
            )
        )

    @app.post(
        "/gateway-sessions",
        response_model=GatewaySession,
        dependencies=[auth_dependency],
    )
    def create_gateway_session(request: GatewaySessionCreateRequest) -> GatewaySession:
        ensure_intake_open()
        metadata = dict(request.metadata)
        if resolved.owner_session_id is not None:
            metadata.update(
                {
                    "owner": "clio-relay",
                    "owner_session_id": resolved.owner_session_id,
                }
            )
            if resolved.owner_session_generation_id is not None:
                metadata["owner_session_generation_id"] = resolved.owner_session_generation_id
        return _public_record(
            queue.create_gateway_session(
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
                    metadata=metadata,
                )
            )
        )

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
        sessions, next_cursor, total = queue.list_gateway_sessions_page(
            cursor=cursor,
            limit=limit,
            cluster=cluster,
        )
        if resolved.owner_session_id is not None:
            sessions = [
                session
                for session in sessions
                if session.metadata.get("owner") == "clio-relay"
                and session.metadata.get("owner_session_id") == resolved.owner_session_id
                and session.metadata.get("owner_session_generation_id")
                == resolved.owner_session_generation_id
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
                    if resolved.owner_session_id is not None
                    else None
                ),
            }
        )

    @app.get(
        "/gateway-sessions/{session_id}",
        response_model=GatewaySession,
        dependencies=[auth_dependency],
    )
    def get_gateway_session(session_id: str) -> GatewaySession:
        try:
            return _public_record(require_owned_gateway(session_id))
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
        try:
            require_owned_gateway(session_id)
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        updates = request.model_dump(exclude={"state", "metadata"}, exclude_none=True)
        metadata = dict(request.metadata)
        if resolved.owner_session_id is not None:
            metadata.update(
                {
                    "owner": "clio-relay",
                    "owner_session_id": resolved.owner_session_id,
                }
            )
            if resolved.owner_session_generation_id is not None:
                metadata["owner_session_generation_id"] = resolved.owner_session_generation_id
        try:
            return _public_record(
                queue.update_gateway_session(
                    session_id,
                    state=request.state,
                    metadata=metadata,
                    **updates,
                )
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
            require_owned_gateway(session_id)
            return _public_record(queue.close_gateway_session(session_id))
        except QueueConflictError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.post(
        "/jobs/{job_id}/progress",
        response_model=ProgressRecord,
        dependencies=[auth_dependency],
    )
    def record_progress(job_id: str, request: ProgressUpdateRequest) -> ProgressRecord:
        try:
            require_owned_job(job_id)
            metadata = external_progress_metadata("external_http", dict(request.metadata))
            return _public_record(
                queue.append_progress(
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
            )
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.get("/artifacts/{artifact_id}/content", dependencies=[auth_dependency])
    def get_artifact_content(artifact_id: str) -> dict[str, object]:
        try:
            require_owned_artifact(artifact_id)
            return _public_payload(read_artifact_bytes(queue, artifact_id))
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.post("/jobs/{job_id}/cancel", response_model=RelayJob, dependencies=[auth_dependency])
    def cancel_job(job_id: str, request: QueueCancelRequest | None = None) -> RelayJob:
        job = require_owned_job(job_id)
        if request is not None and request.cluster is not None and request.cluster != job.cluster:
            raise HTTPException(
                status_code=409,
                detail=(
                    f"job {job_id} belongs to cluster {job.cluster}, "
                    f"not requested cluster {request.cluster}"
                ),
            )
        cancel_scheduler = False if request is None else request.cancel_scheduler_job
        return _public_record(request_cancel_job(queue, job_id, cancel_scheduler=cancel_scheduler))

    @app.post("/queue/jobs/{job_id}/cancel", dependencies=[auth_dependency])
    def cancel_queue_job_route(
        job_id: str,
        request: QueueCancelRequest | None = None,
    ) -> dict[str, object]:
        cancel_scheduler = False if request is None else request.cancel_scheduler_job
        try:
            require_owned_job(job_id)
            return _public_payload(
                cancel_queue_job(
                    queue,
                    job_id,
                    cluster=None if request is None else request.cluster,
                    scheduler_policy="request-scheduler" if cancel_scheduler else "relay-only",
                )
            )
        except ConfigurationError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.get("/queue", dependencies=[auth_dependency])
    def list_queue(
        cluster: str | None = None,
        state: str | None = None,
        kind: JobKind | None = None,
        include_terminal: bool = False,
        cursor: Annotated[int, Query(ge=1)] = 1,
        limit: Annotated[int, Query(ge=1, le=500)] = 100,
        scan_limit: Annotated[int, Query(ge=1, le=10_000)] = 1_000,
    ) -> dict[str, object]:
        job_state = None
        if state is not None:
            try:
                job_state = JobState(state)
            except ValueError as exc:
                raise HTTPException(status_code=422, detail=f"unknown job state: {state}") from exc
        try:
            payload = list_queue_jobs(
                queue,
                cluster=cluster,
                state=job_state,
                kind=kind,
                include_terminal=include_terminal,
                cursor=cursor,
                limit=limit,
                scan_limit=scan_limit,
            )
        except ConfigurationError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        if resolved.owner_session_id is not None:
            raw_jobs = payload.get("jobs")
            jobs: list[dict[str, object]] = []
            raw_job_items = cast(list[object], raw_jobs) if isinstance(raw_jobs, list) else []
            for raw_item in raw_job_items:
                if not isinstance(raw_item, dict):
                    continue
                item = cast(dict[str, object], raw_item)
                raw_job = item.get("job")
                if not isinstance(raw_job, dict):
                    continue
                job_payload = cast(dict[str, object], raw_job)
                if owns_job(RelayJob.model_validate(job_payload)):
                    jobs.append(item)
            payload["jobs"] = jobs
            payload["count"] = len(jobs)
            payload["visibility_filter"] = "owner_session_within_source_window"
        return _public_payload(payload)

    @app.get("/queue/jobs/{job_id}/diagnose", dependencies=[auth_dependency])
    def diagnose_queue_job_route(
        job_id: str,
        cluster: str | None = None,
        older_than_seconds: Annotated[int, Query(ge=1)] = 7_200,
        scan_limit: Annotated[int, Query(ge=1, le=10_000)] = 1_000,
    ) -> dict[str, object]:
        try:
            require_owned_job(job_id)
            return _public_payload(
                diagnose_job(
                    queue,
                    job_id,
                    cluster=cluster,
                    stale_after_seconds=older_than_seconds,
                    scan_limit=scan_limit,
                )
            )
        except ConfigurationError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.get("/retention/jobs/{job_id}/plan", dependencies=[auth_dependency])
    def retention_plan(
        job_id: str,
        expected_updated_at: datetime | None = None,
    ) -> dict[str, object]:
        """Build a read-only terminal-retention plan."""
        if resolved.owner_session_id is not None:
            raise HTTPException(
                status_code=403,
                detail="session-scoped APIs cannot inspect global retention state",
            )
        try:
            plan = TerminalRetentionCoordinator(queue, resolved.spool_dir).plan(
                job_id,
                expected_updated_at=expected_updated_at,
            )
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except QueueConflictError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return _public_payload(
            {
                "plan": plan.model_dump(mode="json"),
                "scheduler_cancel_requested": False,
            }
        )

    @app.get("/retention/jobs/{job_id}/status", dependencies=[auth_dependency])
    def retention_status(job_id: str) -> dict[str, object]:
        """Read the current crash-resumable retention phase without mutation."""
        if resolved.owner_session_id is not None:
            raise HTTPException(
                status_code=403,
                detail="session-scoped APIs cannot inspect global retention state",
            )
        try:
            plan = TerminalRetentionCoordinator(queue, resolved.spool_dir).plan(job_id)
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except QueueConflictError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return {
            "job_id": job_id,
            "receipt_id": plan.receipt_id,
            "phase": None if plan.receipt_phase is None else plan.receipt_phase.value,
            "complete": plan.receipt_phase is not None and plan.receipt_phase.value == "complete",
            "eligible": plan.eligible,
            "protections": plan.protections,
            "scheduler_cancel_requested": False,
        }

    @app.post("/retention/jobs/{job_id}/collect", dependencies=[auth_dependency])
    def retention_collect(
        job_id: str,
        request: RetentionCollectRequest | None = None,
    ) -> dict[str, object]:
        """Dry-run by default or advance bounded retention without scheduler cancellation."""
        if resolved.owner_session_id is not None:
            raise HTTPException(
                status_code=403,
                detail="session-scoped APIs cannot mutate global retention state",
            )
        options = request or RetentionCollectRequest()
        try:
            result = TerminalRetentionCoordinator(queue, resolved.spool_dir).collect(
                job_id,
                execute=options.execute,
                batch_size=options.batch_size,
                expected_updated_at=options.expected_updated_at,
            )
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except QueueConflictError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return _public_payload(result.model_dump(mode="json"))

    @app.get("/queue/stale", dependencies=[auth_dependency])
    def discover_stale_queue_route(
        cluster: str,
        older_than_seconds: Annotated[int, Query(ge=1)] = 7_200,
        job_id: str | None = None,
        kind: JobKind | None = None,
        limit: Annotated[int, Query(ge=1, le=500)] = 100,
        scan_limit: Annotated[int, Query(ge=1, le=10_000)] = 1_000,
    ) -> dict[str, object]:
        if resolved.owner_session_id is not None:
            raise HTTPException(
                status_code=403,
                detail="session-scoped APIs cannot inspect global stale-job state",
            )
        try:
            return _public_payload(
                discover_stale_jobs(
                    queue,
                    cluster=cluster,
                    older_than_seconds=older_than_seconds,
                    job_id=job_id,
                    kind=kind,
                    limit=limit,
                    scan_limit=scan_limit,
                )
            )
        except ConfigurationError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    @app.get("/queue/diagnostics", dependencies=[auth_dependency])
    def diagnose_queue_route(cluster: str | None = None) -> dict[str, object]:
        if resolved.owner_session_id is not None:
            raise HTTPException(
                status_code=403,
                detail="session-scoped APIs cannot inspect global queue diagnostics",
            )
        return _public_payload(diagnose_queue(queue, cluster=cluster))

    @app.post("/queue/cleanup-stale", dependencies=[auth_dependency])
    def cleanup_stale_queue_route(
        cluster: str,
        older_than_seconds: Annotated[int, Query(ge=1)] = 7_200,
        job_id: str | None = None,
        kind: JobKind | None = None,
        max_attempts: Annotated[int, Query(ge=1)] = 3,
        dry_run: bool = True,
        cancel_queued: bool = False,
        limit: Annotated[int, Query(ge=1, le=500)] = 100,
        scan_limit: Annotated[int, Query(ge=1, le=10_000)] = 1_000,
    ) -> dict[str, object]:
        if resolved.owner_session_id is not None:
            raise HTTPException(
                status_code=403,
                detail="session-scoped APIs cannot mutate global stale-job state",
            )
        try:
            return _public_payload(
                cleanup_stale_jobs(
                    queue,
                    cluster=cluster,
                    older_than_seconds=older_than_seconds,
                    job_id=job_id,
                    kind=kind,
                    max_attempts=max_attempts,
                    dry_run=dry_run,
                    cancel_queued=cancel_queued,
                    limit=limit,
                    scan_limit=scan_limit,
                )
            )
        except ConfigurationError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    @app.get("/workers", dependencies=[auth_dependency])
    def worker_status_route(cluster: str | None = None) -> dict[str, object]:
        if resolved.owner_session_id is not None:
            raise HTTPException(
                status_code=403,
                detail="session-scoped APIs cannot inspect global worker state",
            )
        return _public_payload(worker_status(queue, cluster=cluster))

    @app.post("/monitor/rules", response_model=MonitorRule, dependencies=[auth_dependency])
    def create_monitor_rule(rule: MonitorRule) -> MonitorRule:
        try:
            ensure_intake_open()
            require_owned_job(rule.job_id)
            return _public_record(queue.append_monitor_rule(rule))
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.get("/monitor/rules", dependencies=[auth_dependency])
    def list_monitor_rules(
        job_id: str | None = None,
        cursor: Annotated[int, Query(ge=1)] = 1,
        limit: Annotated[int, Query(ge=1, le=MAX_RESPONSE_PAGE_RECORDS)] = (
            DEFAULT_RESPONSE_PAGE_RECORDS
        ),
    ) -> dict[str, object]:
        if job_id is not None:
            require_owned_job(job_id)
        rules, next_cursor, total = queue.list_monitor_rules_page(
            cursor=cursor,
            limit=limit,
            job_id=job_id,
        )
        if resolved.owner_session_id is not None:
            rules = [rule for rule in rules if owns_job(queue.get_job(rule.job_id))]
        return _public_payload(
            {
                "rules": [rule.model_dump(mode="json") for rule in rules],
                "source_cursor": cursor,
                "source_limit": limit,
                "source_next_cursor": next_cursor,
                "source_total": total,
                "source_total_semantics": "global_monitor_rule_sequence_high_water",
                "filters_apply_within_source_window": True,
                "visibility_filter": (
                    "owner_session_within_source_window"
                    if resolved.owner_session_id is not None
                    else None
                ),
            }
        )

    @app.post("/monitor/run-once", dependencies=[auth_dependency])
    def run_monitor_once(
        limit: Annotated[int, Query(ge=1, le=MAX_RESPONSE_PAGE_RECORDS)] = (
            DEFAULT_RESPONSE_PAGE_RECORDS
        ),
    ) -> list[dict[str, object]]:
        if resolved.owner_session_id is not None:
            raise HTTPException(
                status_code=403,
                detail="session-scoped APIs cannot evaluate global monitor rules",
            )
        return [_public_payload(item) for item in evaluate_monitor_rules(queue, limit=limit)]

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
    limit = validate_response_page_limit(limit)
    next_cursor = cursor
    while True:
        events, next_cursor = queue.drain_task_events(
            task_id,
            cursor=next_cursor,
            limit=limit,
        )
        if events:
            yield _public_payload(
                {
                    "event": "task_events",
                    "data": {
                        "task_id": task_id,
                        "events": [event.model_dump(mode="json") for event in events],
                        "next_cursor": next_cursor,
                    },
                }
            )
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
    limit = validate_response_page_limit(limit)
    next_cursor = cursor
    while True:
        payload = monitor_job(queue, job_id, cursor=next_cursor, limit=limit)
        raw_next_cursor = payload["next_cursor"]
        if not isinstance(raw_next_cursor, int):
            raise TypeError("monitor payload next_cursor was not an integer")
        next_cursor = raw_next_cursor
        yield _public_payload({"event": "monitor", "data": payload})
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


def _require_websocket_page_limit(limit: object) -> None:
    try:
        validate_response_page_limit(limit)
    except ValueError as exc:
        raise WebSocketException(code=status.WS_1008_POLICY_VIOLATION) from exc


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

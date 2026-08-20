"""Job-event/task-event/monitor read and streaming (SSE + WebSocket) routes.

split/http-api-w3 (iowarp/clio-relay#231): moved verbatim out of
``create_app()`` in ``http_api.py``. Every reference to a
``create_app()``-local closure (``resolved``, ``queue``, ``require_owned_job``,
``require_owned_task``) is rewritten to the equivalent ``ctx.<name>``
attribute/method on the shared ``RelayApiContext`` (see
``http_api_context.py``'s own docstring) -- the same mechanical bare-name ->
qualified-name rewrite this codebase's other AST-driven extractions already
use; no other line changes.

``observe_until_terminal`` is imported here (not re-exported from
``http_api.py``) because ``wait`` is the only caller that moved with it;
``tests/test_http_api.py``'s
``monkeypatch.setattr(http_api_module, "observe_until_terminal", ...)`` site
re-points to ``clio_relay.http_api_routes_events.observe_until_terminal`` to
follow the move.
"""

# pyright: reportUnusedFunction=false

from __future__ import annotations

import math
from typing import Annotated

from fastapi import FastAPI, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import StreamingResponse

from clio_relay import door_error_adapters, door_errors
from clio_relay.errors import NotFoundError
from clio_relay.http_api_auth import _require_websocket_page_limit, _require_websocket_token
from clio_relay.http_api_context import RelayApiContext
from clio_relay.http_api_models import TaskTimelineEventRequest
from clio_relay.http_api_redaction import _public_model_page, _public_payload, _public_record
from clio_relay.http_api_streaming import (
    _monitor_sse_events,
    _monitor_stream_payloads,
    _task_sse_events,
    _task_stream_payloads,
)
from clio_relay.identifiers import DurableRecordId
from clio_relay.models import Cursor, JobWaitResult, RelayEvent, TaskTimelineEvent
from clio_relay.pagination import DEFAULT_RESPONSE_PAGE_RECORDS, MAX_RESPONSE_PAGE_RECORDS
from clio_relay.relay_ops import monitor_job, observe_until_terminal


def register_event_routes(
    app: FastAPI,
    ctx: RelayApiContext,
    *,
    auth_dependency: object,
) -> None:
    """Register the job/task event, monitor, and streaming routes."""

    @app.get(
        "/jobs/{job_id}/events",
        response_model=list[RelayEvent],
        dependencies=[auth_dependency],
    )
    def get_events(
        job_id: DurableRecordId,
        cursor: Annotated[int, Query(ge=1)] = 1,
        limit: Annotated[int, Query(ge=1, le=MAX_RESPONSE_PAGE_RECORDS)] = (
            DEFAULT_RESPONSE_PAGE_RECORDS
        ),
    ) -> list[RelayEvent]:
        ctx.require_owned_job(job_id)
        events, _ = ctx.queue.drain_events(Cursor(job_id=job_id, next_seq=cursor), limit=limit)
        return [_public_record(event) for event in events]

    @app.get(
        "/jobs/{job_id}/tasks",
        dependencies=[auth_dependency],
    )
    def get_tasks(
        job_id: DurableRecordId,
        cursor: Annotated[int, Query(ge=1)] = 1,
        limit: Annotated[int, Query(ge=1, le=MAX_RESPONSE_PAGE_RECORDS)] = (
            DEFAULT_RESPONSE_PAGE_RECORDS
        ),
    ) -> dict[str, object]:
        ctx.require_owned_job(job_id)
        tasks, next_cursor, total = ctx.queue.list_tasks_page(
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
        task_id: DurableRecordId,
        cursor: Annotated[int, Query(ge=1)] = 1,
        limit: Annotated[int, Query(ge=1, le=MAX_RESPONSE_PAGE_RECORDS)] = (
            DEFAULT_RESPONSE_PAGE_RECORDS
        ),
    ) -> list[TaskTimelineEvent]:
        try:
            ctx.require_owned_task(task_id)
            events, _ = ctx.queue.drain_task_events(task_id, cursor=cursor, limit=limit)
            return [_public_record(event) for event in events]
        except NotFoundError as exc:
            raise door_errors.http_problem("task_not_found", exc=exc) from exc

    @app.post(
        "/tasks/{task_id}/events",
        response_model=TaskTimelineEvent,
        dependencies=[auth_dependency],
    )
    def append_task_event(
        task_id: DurableRecordId,
        request: TaskTimelineEventRequest,
    ) -> TaskTimelineEvent:
        try:
            ctx.require_owned_task(task_id)
            return _public_record(
                ctx.queue.append_task_event(
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
            raise door_errors.http_problem("task_not_found", exc=exc) from exc

    @app.get("/tasks/{task_id}/events/sse", dependencies=[auth_dependency])
    def task_events_sse(
        task_id: DurableRecordId,
        cursor: Annotated[int, Query(ge=1)] = 1,
        limit: Annotated[int, Query(ge=1, le=MAX_RESPONSE_PAGE_RECORDS)] = (
            DEFAULT_RESPONSE_PAGE_RECORDS
        ),
        poll_seconds: float = 1.0,
        stop_after_replay: bool = False,
    ) -> StreamingResponse:
        """Stream task timeline events as Server-Sent Events."""
        if poll_seconds <= 0:
            raise door_errors.http_problem("poll_interval_invalid", "poll_seconds must be positive")
        try:
            ctx.require_owned_task(task_id)
        except NotFoundError as exc:
            raise door_errors.http_problem("task_not_found", exc=exc) from exc
        return StreamingResponse(
            _task_sse_events(
                ctx.queue,
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
        task_id: DurableRecordId,
        cursor: int = 1,
        limit: int = DEFAULT_RESPONSE_PAGE_RECORDS,
        poll_seconds: float = 1.0,
    ) -> None:
        """Stream task timeline events over a WebSocket."""
        _require_websocket_token(ctx.resolved, websocket)
        if poll_seconds <= 0:
            raise door_error_adapters.websocket_refusal("websocket_poll_interval_invalid")
        if cursor < 1:
            raise door_error_adapters.websocket_refusal("websocket_cursor_invalid")
        _require_websocket_page_limit(limit)
        try:
            ctx.require_owned_task(task_id)
        except NotFoundError as exc:
            raise door_error_adapters.websocket_refusal("websocket_resource_not_found") from exc
        except door_errors.HTTPProblemError as exc:
            raise door_error_adapters.websocket_refusal(
                "websocket_resource_ownership_refused"
            ) from exc
        await websocket.accept()
        try:
            async for payload in _task_stream_payloads(
                ctx.queue,
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
        job_id: DurableRecordId,
        cursor: Annotated[int, Query(ge=1)] = 1,
        limit: Annotated[int, Query(ge=1, le=MAX_RESPONSE_PAGE_RECORDS)] = (
            DEFAULT_RESPONSE_PAGE_RECORDS
        ),
    ) -> dict[str, object]:
        try:
            ctx.require_owned_job(job_id)
            return _public_payload(monitor_job(ctx.queue, job_id, cursor=cursor, limit=limit))
        except NotFoundError as exc:
            raise door_errors.http_problem("job_not_found", exc=exc) from exc

    @app.get("/jobs/{job_id}/monitor/sse", dependencies=[auth_dependency])
    def monitor_sse(
        job_id: DurableRecordId,
        cursor: Annotated[int, Query(ge=1)] = 1,
        limit: Annotated[int, Query(ge=1, le=MAX_RESPONSE_PAGE_RECORDS)] = (
            DEFAULT_RESPONSE_PAGE_RECORDS
        ),
        poll_seconds: float = 1.0,
        stop_on_terminal: bool = True,
    ) -> StreamingResponse:
        """Stream job monitor updates as Server-Sent Events."""
        if poll_seconds <= 0:
            raise door_errors.http_problem("poll_interval_invalid", "poll_seconds must be positive")
        try:
            ctx.require_owned_job(job_id)
        except NotFoundError as exc:
            raise door_errors.http_problem("job_not_found", exc=exc) from exc
        return StreamingResponse(
            _monitor_sse_events(
                ctx.queue,
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
        job_id: DurableRecordId,
        cursor: int = 1,
        limit: int = DEFAULT_RESPONSE_PAGE_RECORDS,
        poll_seconds: float = 1.0,
        stop_on_terminal: bool = True,
    ) -> None:
        """Stream job monitor updates over a WebSocket."""
        _require_websocket_token(ctx.resolved, websocket)
        if poll_seconds <= 0:
            raise door_error_adapters.websocket_refusal("websocket_poll_interval_invalid")
        if cursor < 1:
            raise door_error_adapters.websocket_refusal("websocket_cursor_invalid")
        _require_websocket_page_limit(limit)
        try:
            ctx.require_owned_job(job_id)
        except NotFoundError as exc:
            raise door_error_adapters.websocket_refusal("websocket_resource_not_found") from exc
        except door_errors.HTTPProblemError as exc:
            raise door_error_adapters.websocket_refusal(
                "websocket_resource_ownership_refused"
            ) from exc
        await websocket.accept()
        try:
            async for payload in _monitor_stream_payloads(
                ctx.queue,
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

    @app.post(
        "/jobs/{job_id}/wait",
        response_model=JobWaitResult,
        dependencies=[auth_dependency],
    )
    def wait(
        job_id: DurableRecordId,
        timeout_seconds: float = 600,
        poll_seconds: float = 2,
    ) -> JobWaitResult:
        if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
            raise door_errors.http_problem(
                "wait_parameters_invalid", "timeout_seconds must be positive and finite"
            )
        if not math.isfinite(poll_seconds) or poll_seconds <= 0:
            raise door_errors.http_problem(
                "wait_parameters_invalid", "poll_seconds must be positive and finite"
            )
        try:
            ctx.require_owned_job(job_id)
            return _public_record(
                observe_until_terminal(
                    ctx.queue,
                    job_id,
                    timeout_seconds=timeout_seconds,
                    poll_seconds=poll_seconds,
                )
            )
        except NotFoundError as exc:
            raise door_errors.http_problem("job_not_found", exc=exc) from exc

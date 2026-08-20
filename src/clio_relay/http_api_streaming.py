"""SSE/WebSocket payload generators for job-monitor and task-event streams.

split/http-api-w3 (iowarp/clio-relay#231): moved verbatim out of
``http_api.py``. Every function here already took ``queue`` as a plain
argument (not a ``create_app()`` closure capture), so this is an unmodified,
atomic move.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from typing import cast

from clio_relay.core_queue import ClioCoreQueue
from clio_relay.http_api_redaction import _public_payload
from clio_relay.pagination import validate_response_page_limit
from clio_relay.relay_ops import monitor_job


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
        raw_job = payload.get("job")
        if not isinstance(raw_job, dict):
            raise TypeError("monitor payload job was not an object")
        raw_state = cast(dict[str, object], raw_job).get("state")
        if not isinstance(raw_state, str):
            raise TypeError("monitor payload job state was not a string")
        if stop_on_terminal and payload.get("terminal") is True:
            yield {"event": "terminal", "data": {"job_id": job_id, "state": raw_state}}
            return
        await asyncio.sleep(poll_seconds)

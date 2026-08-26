"""SSE/WebSocket payload generators for job-monitor and task-event streams.

split/http-api-w3 (iowarp/clio-relay#231): moved verbatim out of
``http_api.py``. Every function here already took ``queue`` as a plain
argument (not a ``create_app()`` closure capture), so this is an unmodified,
atomic move.

clio-relay#221/#259: :func:`_log_tail_sse_events` adds the live-console
lane's server half beside the pre-existing monitor/task generators above --
same ``StreamingResponse``/``text/event-stream`` shape, same "sync read
inside an async generator" style the other two already use (no executor
offload here either; consistent with the established pattern, not a new
one). It follows a job's log spool from a byte offset, pushing each
appended chunk within one :data:`LOG_SSE_FOLLOW_POLL_SECONDS` tick instead
of the 2.0s tailer floor plus a client's own polling on top of that.
"""

# Every generator here (`_monitor_sse_events`/`_task_sse_events`/
# `_log_tail_sse_events`) is called only from the `http_api_routes_*.py`
# modules that register the routes wrapping them in `StreamingResponse` --
# never from within this file -- so pyright's cross-module private-name
# usage flags both ends, the same shape `http_api_routes_events.py`'s own
# `# pyright: reportUnusedFunction=false` already covers for its decorator-
# registered-only route handlers.
# pyright: reportUnusedFunction=false

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from typing import cast

from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.http_api_redaction import _public_payload
from clio_relay.models import TERMINAL_STATES
from clio_relay.pagination import validate_response_page_limit
from clio_relay.relay_ops import monitor_job, read_job_log
from clio_relay.spool import MAX_LOG_READ_BYTES, LogStreamName


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


#: clio-relay#221/#259: the SSE log-tail follow interval. The 2.0s
#: ``CONSOLE_TAIL_MIN_POLL_INTERVAL_SECONDS`` floor
#: (:mod:`clio_relay.console_stream`) stays exactly as it is for the
#: capture-side tailer this generator reads from -- this is a SEPARATE,
#: much tighter interval for how often an already-held SSE response checks
#: that spool for newly appended bytes. Matches the poll interval
#: :data:`clio_relay.observation.PATTERN_OBSERVATION_POLL_SECONDS` already
#: uses for the same class of problem (near-real-time visibility into a
#: growing job log) rather than inventing a second constant for an
#: identical need.
LOG_SSE_FOLLOW_POLL_SECONDS = 0.15


async def _log_tail_sse_events(
    settings: RelaySettings,
    queue: ClioCoreQueue,
    job_id: str,
    *,
    stream_name: LogStreamName,
    offset: int,
    poll_seconds: float,
) -> AsyncIterator[str]:
    """Follow one job log stream as Server-Sent Events from a byte offset.

    clio-relay#221/#259: pushes an appended chunk within one follow-poll
    interval of the write landing in the spool -- instead of today's
    double-polling pipeline (the capture-side tailer's own 2.0s floor, plus
    a client polling the byte-range route on top of that). Reuses the exact
    spool read primitive (:func:`clio_relay.relay_ops.read_job_log`) the
    byte-range ``GET /jobs/{job_id}/logs/{stream_name}`` route already
    serves, so the offset/limit/eof envelope a client already understands
    from that route means the same thing here.

    Each ``log_chunk`` event's ``id:`` line carries the chunk's own
    ``next_offset`` so a client can resume with ``Last-Event-ID`` (or by
    reading ``data.offset``) after a drop, without re-deriving it from the
    byte-range route. While there is unread data, the next read is issued
    immediately (no sleep) so a backlog drains as fast as the spool can be
    read; the poll interval only applies while caught up and waiting for
    more to be written.

    Closes with a typed ``end`` event once the owning job has reached a
    terminal state AND the stream has been drained to EOF -- never on EOF
    alone, since a still-running job may append more at any moment.
    """
    current_offset = offset
    while True:
        job = queue.get_job(job_id)
        page = read_job_log(
            settings,
            job,
            stream_name=stream_name,
            offset=current_offset,
            limit=MAX_LOG_READ_BYTES,
        )
        text = page["text"]
        next_offset = page["next_offset"]
        eof = page["eof"]
        if not isinstance(text, str) or not isinstance(next_offset, int):
            raise TypeError("job log reader returned an invalid SSE chunk")
        if text:
            current_offset = next_offset
            chunk_data = {
                "job_id": job_id,
                "stream": stream_name,
                "chunk": text,
                "offset": current_offset,
            }
            yield f"id: {current_offset}\nevent: log_chunk\ndata: {json.dumps(chunk_data)}\n\n"
            # More may already be waiting past what this one read drained --
            # keep draining without sleeping until a read comes back empty.
            continue
        if job.state in TERMINAL_STATES and eof:
            end_data = {
                "job_id": job_id,
                "stream": stream_name,
                "state": job.state,
                "offset": current_offset,
            }
            yield f"event: end\ndata: {json.dumps(end_data)}\n\n"
            return
        await asyncio.sleep(poll_seconds)

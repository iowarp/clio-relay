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
import codecs
import json
import time
from collections.abc import AsyncIterator
from typing import cast

from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.errors import NotFoundError
from clio_relay.http_api_redaction import _public_payload
from clio_relay.models import TERMINAL_STATES
from clio_relay.pagination import validate_response_page_limit
from clio_relay.relay_ops import monitor_job
from clio_relay.spool import MAX_LOG_READ_BYTES, JobSpool, LogStreamName


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


#: clio-relay#221/#259: the SSE log-tail follow interval. The (now 0.25s,
#: adversarial-review-tightened) ``CONSOLE_TAIL_MIN_POLL_INTERVAL_SECONDS``
#: floor (:mod:`clio_relay.console_stream`) stays a SEPARATE constant for
#: the capture-side tailer this generator reads from -- this is how often
#: an already-held SSE response checks that spool for newly appended
#: bytes. Matches :data:`clio_relay.observation.PATTERN_OBSERVATION_
#: POLL_SECONDS`'s own value for the same class of problem.
LOG_SSE_FOLLOW_POLL_SECONDS = 0.15

#: clio-relay#221/#259 adversarial review (D6): the caller-tunable upper
#: bound on ``poll_seconds``. Kept comfortably under
#: :data:`LOG_SSE_KEEPALIVE_INTERVAL_SECONDS` so the idle-check loop always
#: wakes up often enough that a keepalive is never overdue by more than one
#: poll interval.
LOG_SSE_MAX_POLL_SECONDS = 5.0

#: clio-relay#221/#259 adversarial review (D1): a job quiet for the
#: duration of a typical client socket read timeout (proven live at 30s)
#: previously tripped the client into a false ChannelDropped -> a 2FA
#: reconnect prompt, over a channel that was never actually gone. An SSE
#: comment (``: keepalive``, never a field, never dispatched as a frame)
#: emitted at least this often keeps the wire genuinely busy while a job is
#: merely quiet (compiling, queued, ...) -- well under the 30s window that
#: trips a socket timeout.
LOG_SSE_KEEPALIVE_INTERVAL_SECONDS = 10.0


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
    double-polling pipeline. Reads raw bytes through
    :meth:`~clio_relay.spool.JobSpool.read_log_bytes` (never
    :func:`~clio_relay.relay_ops.read_job_log`'s per-call lossy decode:
    D9 of the adversarial review proved a multi-byte UTF-8 character
    straddling two 1 MiB read boundaries gets mangled into two replacement
    characters instead of one correct one) through one incremental decoder
    held for the whole connection, so a boundary-straddling character
    decodes correctly once its remaining bytes arrive on the next read.
    The blocking file read itself runs via :func:`asyncio.to_thread` (D2:
    proven live to otherwise block the WHOLE door event loop for the
    duration of a large backlog drain -- 32x1 MiB drained in 219ms with
    zero other tasks scheduled, no healthz, no disconnect detection).

    Each ``log_chunk`` event's ``id:`` line carries the chunk's own
    ``next_offset``; its JSON ``data`` carries both ``offset`` (where this
    read started, matching the byte-range sibling route's own envelope
    field, D4/D11) and ``next_offset`` (D4) -- run through the same
    :func:`_public_payload` pass that sibling applies (D11). While there is
    unread data, the next read is issued immediately after yielding control
    once (``await asyncio.sleep(0)``, D2) so a backlog drains fast without
    starving the event loop; the poll interval only applies once caught up.
    An idle period emits a ``: keepalive`` SSE comment at least every
    :data:`LOG_SSE_KEEPALIVE_INTERVAL_SECONDS` (D1) so a client's socket
    read never times out against a merely-quiet job.

    Closes with a typed ``end`` event once the owning job has reached a
    terminal state AND the stream has been drained to EOF -- never on EOF
    alone. A job record that vanishes mid-stream (D7) closes the same way
    with ``state: "gone"`` instead of leaking an unhandled 500 after
    headers were already sent (which a client would see as a truncated 200,
    misread as a channel drop).
    """
    current_offset = offset
    decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
    last_activity = time.monotonic()
    while True:
        try:
            job = queue.get_job(job_id)
        except NotFoundError:
            end_data = _public_payload(
                {
                    "job_id": job_id,
                    "stream": stream_name,
                    "state": "gone",
                    "offset": current_offset,
                    "next_offset": current_offset,
                }
            )
            yield f"event: end\ndata: {json.dumps(end_data)}\n\n"
            return
        spool = JobSpool(settings.spool_dir, job)
        chunk_bytes, next_offset, eof = await asyncio.to_thread(
            spool.read_log_bytes,
            stream_name,
            offset=current_offset,
            limit=MAX_LOG_READ_BYTES,
        )
        if chunk_bytes:
            read_offset = current_offset
            current_offset = next_offset
            last_activity = time.monotonic()
            text = decoder.decode(chunk_bytes)
            if text:
                chunk_data = _public_payload(
                    {
                        "job_id": job_id,
                        "stream": stream_name,
                        "chunk": text,
                        "offset": read_offset,
                        "next_offset": current_offset,
                    }
                )
                yield f"id: {current_offset}\nevent: log_chunk\ndata: {json.dumps(chunk_data)}\n\n"
            # More may already be waiting past what this one read drained --
            # keep draining, but yield control once first (D2) so a long
            # backlog can never starve the rest of the event loop.
            await asyncio.sleep(0)
            continue
        if job.state in TERMINAL_STATES and eof:
            tail_text = decoder.decode(b"", final=True)
            if tail_text:
                chunk_data = _public_payload(
                    {
                        "job_id": job_id,
                        "stream": stream_name,
                        "chunk": tail_text,
                        "offset": current_offset,
                        "next_offset": current_offset,
                    }
                )
                yield (
                    f"id: {current_offset}\nevent: log_chunk\ndata: {json.dumps(chunk_data)}\n\n"
                )
            end_data = _public_payload(
                {
                    "job_id": job_id,
                    "stream": stream_name,
                    "state": job.state,
                    "offset": current_offset,
                    "next_offset": current_offset,
                }
            )
            yield f"event: end\ndata: {json.dumps(end_data)}\n\n"
            return
        now = time.monotonic()
        if now - last_activity >= LOG_SSE_KEEPALIVE_INTERVAL_SECONDS:
            last_activity = now
            yield ": keepalive\n\n"
        await asyncio.sleep(poll_seconds)

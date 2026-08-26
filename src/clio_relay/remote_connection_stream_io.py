"""Raw wire mechanics for one identity-bound HTTP stream to a remote relay.

:class:`~clio_relay.remote_connection.RemoteConnection` composes these
primitives; nothing here owns the *channel* the stream rides (see
:mod:`clio_relay.control_channel`) or which stream is pooled/reused (see
:mod:`clio_relay.remote_connection`). This module owns exactly the three
steps every owned-session HTTP exchange goes through: proving a freshly
opened stream against the connection's bring-up identity before any
credential is sent (:func:`_open_identity_bound_stream`,
:func:`verify_session_identity`), issuing one non-reconnecting JSON request
over an already-proven stream (:func:`_request_json_on_stream`), and reading
one bounded JSON response off the wire (:func:`read_json_response`).

clio-relay#213: :func:`_is_stale_stream_error` is the narrow, OS-observed
dead-pooled-stream signature ``RemoteConnection.request_json`` retries
exactly once -- deliberately narrow so an HTTP-status failure or a
slow-but-live server (``ObservationTimeoutError``) never retries under a
second identity-bound stream.

clio-relay#221/#259: :func:`_stream_sse_frames_on_stream` adds the live-
console lane's client half -- a fourth step, sibling to
:func:`_request_json_on_stream`/:func:`read_json_response`, for the one
owned-session exchange whose response is not one bounded JSON document but
an unbounded ``text/event-stream`` body. It reuses the exact same
identity-bound, never-reconnecting ``http.client.HTTPConnection`` those two
already require -- rides the SAME held channel, opens no new dial -- but
reads the body incrementally as :class:`SseFrame`\\ s instead of all at once.
Deliberately never retried the way :func:`_request_json_on_stream` is:
once frames have started reaching the caller, a transport failure can only
be reported forward as :class:`~clio_relay.control_channel.ChannelDropped`,
never silently replayed under a second stream and re-delivered as if
nothing happened -- resuming (a fresh call with a later offset) is always
the caller's own explicit choice.
"""

# _is_stale_stream_error/_open_identity_bound_stream/_request_json_on_stream/
# _stream_sse_frames_on_stream are leaf primitives called only from
# remote_connection.py's RemoteConnection class (whose methods stay resident
# there after this split), or from tests -- never from within this file --
# matching storage_ledger_codec.py's own leaf-primitive precedent.
# pyright: reportUnusedFunction=false

from __future__ import annotations

import hmac
import http.client
import json
import logging
import urllib.parse
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Final, cast

from clio_relay.bounded_payload import describe_delivery_refusal, is_delivery_refusal
from clio_relay.control_channel import ChannelDropped, ChannelEndpoint
from clio_relay.errors import ObservationTimeoutError, RelayError
from clio_relay.job_identity import (
    OWNER_SESSION_ID_HEADER,
    SESSION_GENERATION_ID_HEADER,
)

logger = logging.getLogger(__name__)

MAX_SESSION_API_RESPONSE_BYTES: Final = 8 * 1024 * 1024

#: clio-relay#221/#259 adversarial review (D1): a job quiet for as long as
#: the connection's ordinary bring-up/request timeout (proven live at 30s)
#: previously tripped the pooled stream's socket timeout mid-SSE-read,
#: surfacing as a false ChannelDropped -> a 2FA reconnect prompt over a
#: channel that was never actually gone. The server side now emits a
#: keepalive comment at least every 10s
#: (``http_api_streaming.LOG_SSE_KEEPALIVE_INTERVAL_SECONDS``); this is
#: widened well past that -- generous margin for network jitter -- but
#: still bounded (never an unbounded/None timeout), so a GENUINELY dead
#: connection is still caught, just not mistaken for one during an
#: ordinary quiet stretch.
LOG_SSE_STREAM_TIMEOUT_SECONDS: Final = 60.0

# clio-relay#213: the closed set of exceptions `_request_json_on_stream` wraps and
# chains that mean the *stream* died at the OS level (idle-closed, reset, a bad
# status line) rather than the request being genuinely rejected. Deliberately
# narrow: an HTTP-status failure or a slow-but-live server (ObservationTimeoutError)
# must never retry under a second identity-bound stream.
_STALE_STREAM_ERROR_TYPES: Final = (ConnectionError, http.client.BadStatusLine)


def _is_stale_stream_error(exc: BaseException) -> bool:
    """Return True only for the narrow, OS-observed dead-pooled-stream signature."""
    return isinstance(exc.__cause__, _STALE_STREAM_ERROR_TYPES)


_IDENTITY_FIELDS: Final = (
    "schema_version",
    "cluster",
    "session_id",
    "session_generation_id",
    "nonce",
)


def _open_identity_bound_stream(
    *,
    endpoint: ChannelEndpoint,
    nonce: str,
    expected_identity: dict[str, object],
    timeout_seconds: float,
) -> http.client.HTTPConnection:
    """Prove one non-reconnecting TCP stream before any credential is sent."""
    stream = http.client.HTTPConnection(endpoint.host, endpoint.port, timeout=timeout_seconds)
    try:
        stream.connect()
        stream.auto_open = 0
        stream.request(
            "GET",
            "/session-identity?" + urllib.parse.urlencode({"nonce": nonce}),
            headers={"Accept": "application/json", "Connection": "keep-alive"},
        )
        proof_response = stream.getresponse()
        proof_document = read_json_response(proof_response, label="session identity challenge")
        if proof_response.status != 200 or not isinstance(proof_document, dict):
            raise RelayError("owned session API did not return a valid server identity challenge")
        verify_session_identity(
            cast(dict[str, object], proof_document),
            expected=expected_identity,
        )
        if proof_response.will_close or stream.sock is None:
            raise RelayError(
                "owned session API closed the identity-proven connection before authentication"
            )
        return stream
    except (OSError, http.client.HTTPException) as exc:
        stream.close()
        raise RelayError("owned session API identity challenge failed") from exc
    except BaseException:
        stream.close()
        raise


def _request_json_on_stream(
    *,
    stream: http.client.HTTPConnection,
    method: str,
    path: str,
    query: dict[str, object] | None,
    body: dict[str, object] | None,
    api_token: str,
    session_id: str,
    generation_id: str,
    response_timeout_seconds: float | None,
) -> object:
    """Issue one request without permitting HTTPConnection to reconnect."""
    encoded_query = "" if query is None else "?" + urllib.parse.urlencode(query)
    encoded_body = None if body is None else json.dumps(body).encode("utf-8")
    proven_socket = stream.sock
    prior_connection_timeout = stream.timeout
    prior_socket_timeout: float | None = None
    response_timeout_applied = False
    try:
        if response_timeout_seconds is not None:
            if proven_socket is None:
                raise RelayError("owned session API identity-proven connection is not open")
            prior_socket_timeout = proven_socket.gettimeout()
            stream.timeout = response_timeout_seconds
            proven_socket.settimeout(response_timeout_seconds)
            response_timeout_applied = True
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {api_token}",
            OWNER_SESSION_ID_HEADER: session_id,
            SESSION_GENERATION_ID_HEADER: generation_id,
        }
        if encoded_body is not None:
            headers["Content-Type"] = "application/json"
        stream.request(
            method,
            path + encoded_query,
            body=encoded_body,
            headers=headers,
        )
        response = stream.getresponse()
        document = read_json_response(response, label=f"{method} {path}")
        if not 200 <= response.status < 300:
            # A1 (#231 R6 review): door_errors.as_http_problem spreads the
            # original T2 refusal document's fields (doc §6.4) into the 413
            # problem body (fault.data, F4) -- recognized here so its own
            # typed code/message surfaces instead of the generic "HTTP
            # {status}: {raw json blob}" that discards the structure.
            typed_document = (
                cast(dict[str, object], document) if isinstance(document, dict) else None
            )
            if typed_document is not None and is_delivery_refusal(typed_document):
                code = cast(dict[str, object], typed_document.get("delivery", {})).get("code")
                raise RelayError(
                    f"owned session API request refused delivery ({code}): "
                    f"{describe_delivery_refusal(typed_document)}"
                )
            detail = json.dumps(document, ensure_ascii=False)[:2_000]
            raise RelayError(
                f"owned session API request failed: {method} {path}: "
                f"HTTP {response.status}: {detail}"
            )
        return document
    except (OSError, http.client.HTTPException) as exc:
        if response_timeout_applied and isinstance(exc, TimeoutError):
            raise ObservationTimeoutError(
                f"owned session API response observation timed out for {method} {path}"
            ) from exc
        raise RelayError(
            f"owned session API identity-bound request failed for {method} {path}: {exc}"
        ) from exc
    finally:
        if response_timeout_seconds is not None:
            stream.timeout = prior_connection_timeout
            if (
                response_timeout_applied
                and stream.sock is proven_socket
                and proven_socket is not None
            ):
                try:
                    proven_socket.settimeout(prior_socket_timeout)
                except OSError:
                    # The response may have closed the proven socket.  Never let
                    # HTTPConnection reconnect it under the authenticated client.
                    stream.close()


def read_json_response(response: http.client.HTTPResponse, *, label: str) -> object:
    """Read one bounded JSON document from a channel response."""
    payload = response.read(MAX_SESSION_API_RESPONSE_BYTES + 1)
    if len(payload) > MAX_SESSION_API_RESPONSE_BYTES:
        raise RelayError(f"owned session API {label} response exceeded its byte limit")
    try:
        return json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RelayError(f"owned session API {label} response was not UTF-8 JSON") from exc


def verify_session_identity(
    observed: dict[str, object],
    *,
    expected: dict[str, object],
) -> None:
    """Require the reached listener to be the bring-up-proven session.

    ``expected`` is proven under whatever identity anchor this connection's
    mode declares (§8.3) -- the ssh-authenticated bootstrap act in
    ``ssh_forward`` mode, the weaker ``preshared_link_secret`` anchor in
    modes (a)/(b) -- never assumed to be ssh-specific here.
    """
    if any(observed.get(field) != expected.get(field) for field in _IDENTITY_FIELDS):
        raise RelayError(
            "owned session API server identity did not match the bring-up-proven session"
        )
    observed_signature = observed.get("hmac_sha256")
    expected_signature = expected.get("hmac_sha256")
    if (
        not isinstance(observed_signature, str)
        or not isinstance(expected_signature, str)
        or len(observed_signature) != 64
        or len(expected_signature) != 64
        or not hmac.compare_digest(observed_signature, expected_signature)
    ):
        raise RelayError("owned session API server identity HMAC did not verify")


@dataclass(frozen=True, slots=True)
class SseFrame:
    """One parsed Server-Sent Event frame off an identity-bound stream."""

    event: str
    data: str
    id: str | None


def _sse_field_value(line: str, *, prefix: str) -> str:
    """Return one SSE field's value, dropping at most one leading space."""
    value = line[len(prefix) :]
    return value[1:] if value.startswith(" ") else value


def _stream_sse_frames_on_stream(
    *,
    stream: http.client.HTTPConnection,
    method: str,
    path: str,
    query: dict[str, object] | None,
    api_token: str,
    session_id: str,
    generation_id: str,
) -> Iterator[SseFrame]:
    """Issue one streaming GET and yield parsed Server-Sent Event frames.

    Same identity-bound, non-reconnecting stream discipline as
    :func:`_request_json_on_stream` (never lets ``HTTPConnection`` silently
    redial), but the response body is read line-by-line as SSE frames land
    instead of being read whole. A frame dispatches on its blank-line
    terminator, matching the SSE spec's own framing; only frames carrying a
    ``data:`` field are dispatched (a bare ``id:``/comment-only frame is
    swallowed, exactly as a browser ``EventSource`` would -- this is also
    how the server's own ``: keepalive`` comments (D1) are tolerated: they
    never reach ``data:``, so they never dispatch a frame at all).

    clio-relay#221/#259 adversarial review (D1): widens the stream's socket
    timeout to :data:`LOG_SSE_STREAM_TIMEOUT_SECONDS` for the duration of
    this read, using the identical save/restore-in-``finally`` discipline
    :func:`_request_json_on_stream` already uses for its own
    ``response_timeout_seconds`` -- the connection's ordinary bring-up
    timeout is far too tight for a job that is merely quiet.

    clio-relay#221/#259 adversarial review (D8): each line is read with
    :data:`MAX_SESSION_API_RESPONSE_BYTES` as a hard cap
    (``response.readline(limit)``); a line that reaches the cap without a
    terminator is a typed :class:`RelayError`, never silently truncated or
    read forever. The bytes accumulated across one frame's ``data:`` lines
    are capped the same way, mirroring the same byte-budget discipline
    :func:`read_json_response` already applies to a whole JSON response.

    Raises:
        RelayError: The request could not be sent, the initial response was
            not HTTP 200 (the same two conditions
            :func:`_request_json_on_stream` itself would raise for), a line
            exceeded its byte cap without a terminator, or one frame's
            accumulated ``data:`` bytes exceeded their cap.
        ChannelDropped: The stream failed -- or the peer closed it -- after
            the response began, before an ``event: end`` frame was seen. A
            clean finish is the caller's own ``end`` frame; anything else
            ending the body is a drop, not a completion, and is never
            retried here (contrast clio-relay#213's narrow pre-first-byte
            stale-pooled-stream retry in ``RemoteConnection.request_json``).
    """
    encoded_query = "" if query is None else "?" + urllib.parse.urlencode(query)
    headers = {
        "Accept": "text/event-stream",
        "Authorization": f"Bearer {api_token}",
        OWNER_SESSION_ID_HEADER: session_id,
        SESSION_GENERATION_ID_HEADER: generation_id,
    }
    proven_socket = stream.sock
    prior_connection_timeout = stream.timeout
    prior_socket_timeout: float | None = None
    timeout_widened = False
    try:
        if proven_socket is not None:
            prior_socket_timeout = proven_socket.gettimeout()
            stream.timeout = LOG_SSE_STREAM_TIMEOUT_SECONDS
            proven_socket.settimeout(LOG_SSE_STREAM_TIMEOUT_SECONDS)
            timeout_widened = True
        try:
            stream.request(method, path + encoded_query, headers=headers)
            response = stream.getresponse()
        except (OSError, http.client.HTTPException) as exc:
            raise RelayError(
                f"owned session API stream request failed for {method} {path}: {exc}"
            ) from exc
        if response.status != 200:
            document = read_json_response(response, label=f"{method} {path}")
            detail = json.dumps(document, ensure_ascii=False)[:2_000]
            raise RelayError(
                f"owned session API stream request failed: {method} {path}: "
                f"HTTP {response.status}: {detail}"
            )
        event_type = "message"
        data_lines: list[str] = []
        data_bytes = 0
        event_id: str | None = None
        try:
            while True:
                raw_line = response.readline(MAX_SESSION_API_RESPONSE_BYTES)
                if not raw_line:
                    # The peer closed the body before an explicit `end`
                    # frame -- a drop, not a clean finish. A clean finish
                    # returns from this generator normally, right after
                    # yielding that frame below; this is the ONLY other way
                    # the loop ends.
                    raise ChannelDropped(
                        f"owned session API stream closed unexpectedly: {method} {path}"
                    )
                if not raw_line.endswith(b"\n"):
                    if len(raw_line) < MAX_SESSION_API_RESPONSE_BYTES:
                        # A short read with no terminator is the body ending
                        # MID-LINE -- a drop, not a size problem; the size
                        # message would send an operator hunting a cap that
                        # was never hit (clio-relay#221 review residual 4).
                        raise ChannelDropped(
                            f"owned session API stream closed mid-line: {method} {path}"
                        )
                    raise RelayError(
                        f"owned session API stream line for {method} {path} exceeded "
                        f"{MAX_SESSION_API_RESPONSE_BYTES} bytes without a terminator"
                    )
                line = raw_line.decode("utf-8", errors="replace")
                line = line[:-1]
                line = line[:-1] if line.endswith("\r") else line
                if line == "":
                    if data_lines:
                        yield SseFrame(event=event_type, data="\n".join(data_lines), id=event_id)
                        if event_type == "end":
                            return
                    event_type = "message"
                    data_lines = []
                    data_bytes = 0
                    event_id = None
                    continue
                if line.startswith(":"):
                    continue  # SSE comment/keepalive line -- never a field.
                if line.startswith("data:"):
                    value = _sse_field_value(line, prefix="data:")
                    data_bytes += len(value.encode("utf-8"))
                    if data_bytes > MAX_SESSION_API_RESPONSE_BYTES:
                        raise RelayError(
                            f"owned session API stream frame for {method} {path} exceeded "
                            f"{MAX_SESSION_API_RESPONSE_BYTES} accumulated data bytes"
                        )
                    data_lines.append(value)
                elif line.startswith("event:"):
                    event_type = _sse_field_value(line, prefix="event:")
                elif line.startswith("id:"):
                    event_id = _sse_field_value(line, prefix="id:")
        except (OSError, http.client.HTTPException) as exc:
            raise ChannelDropped(
                f"owned session API stream read failed for {method} {path}: {exc}"
            ) from exc
    finally:
        if timeout_widened:
            stream.timeout = prior_connection_timeout
            if stream.sock is proven_socket and proven_socket is not None:
                try:
                    proven_socket.settimeout(prior_socket_timeout)
                except OSError:
                    # The response may have closed the proven socket. Never
                    # let HTTPConnection reconnect it under the
                    # authenticated client.
                    stream.close()


@dataclass(frozen=True, slots=True)
class LogStreamChunk:
    """One decoded increment of a followed job log stream (clio-relay#221/#259)."""

    job_id: str
    stream: str
    text: str
    offset: int
    end: bool
    state: str | None = None


def _log_stream_chunk_from_frame(
    frame: SseFrame, *, job_id: str, stream_name: str
) -> LogStreamChunk | None:
    """Decode one SSE frame into a typed chunk, or ``None`` to skip an unknown one.

    clio-relay#221/#259 adversarial review (D10): an event type outside
    ``{"log_chunk", "end"}`` is logged and SKIPPED, never a hard failure --
    a future server addition to the SSE vocabulary must not retroactively
    break every already-deployed client reading this same route. Its
    payload shape is not assumed at all in that case (an unrecognized type
    could carry anything), so no field validation runs for it.
    """
    if frame.event not in {"log_chunk", "end"}:
        logger.warning(
            "clio-relay: owned session log stream sent an unrecognized SSE event "
            "type %r for job %s stream %s -- skipped",
            frame.event,
            job_id,
            stream_name,
        )
        return None
    try:
        payload: object = json.loads(frame.data) if frame.data else {}
    except json.JSONDecodeError as exc:
        raise RelayError(
            f"owned session API log stream returned a non-JSON {frame.event} frame"
        ) from exc
    if not isinstance(payload, dict):
        raise RelayError(f"owned session API log stream {frame.event} frame was not a JSON object")
    typed_payload = cast(dict[str, object], payload)
    if typed_payload.get("job_id") != job_id or typed_payload.get("stream") != stream_name:
        raise RelayError(
            "owned session API log stream frame did not match the requested job/stream"
        )
    # clio-relay#221/#259 adversarial review (D4): the wire now carries BOTH
    # `offset` (where this read started) and `next_offset` (the resumable
    # position, matching the frame's own `id:` line) -- `LogStreamChunk.
    # offset` reads `next_offset`, preserving its original "where to
    # resume from" meaning now that the two are no longer the same field.
    offset_value = typed_payload.get("next_offset")
    if not isinstance(offset_value, int):
        raise RelayError("owned session API log stream frame carried an invalid next_offset")
    if frame.event == "end":
        state = typed_payload.get("state")
        return LogStreamChunk(
            job_id=job_id,
            stream=stream_name,
            text="",
            offset=offset_value,
            end=True,
            state=state if isinstance(state, str) else None,
        )
    chunk_text = typed_payload.get("chunk")
    if not isinstance(chunk_text, str):
        raise RelayError("owned session API log stream frame carried no chunk text")
    return LogStreamChunk(
        job_id=job_id,
        stream=stream_name,
        text=chunk_text,
        offset=offset_value,
        end=False,
    )


def _stream_log_chunks_over_stream(
    *,
    stream: http.client.HTTPConnection,
    job_id: str,
    stream_name: str,
    offset: int,
    poll_seconds: float | None,
    api_token: str,
    session_id: str,
    generation_id: str,
) -> Iterator[LogStreamChunk]:
    """Compose the SSE frame reader and chunk decoder for one log-tail follow.

    The one seam :meth:`~clio_relay.remote_connection.RemoteConnection.
    stream_log_chunks` calls -- it owns only acquiring/discarding the pooled
    stream around this generator, keeping the SSE wire mechanics
    (:func:`_stream_sse_frames_on_stream`) and decoding
    (:func:`_log_stream_chunk_from_frame`) resident here, next to the frame
    parser they both depend on.
    """
    path = f"/jobs/{job_id}/logs/{stream_name}/sse"
    query: dict[str, object] = {"offset": offset}
    if poll_seconds is not None:
        query["poll_seconds"] = poll_seconds
    for frame in _stream_sse_frames_on_stream(
        stream=stream,
        method="GET",
        path=path,
        query=query,
        api_token=api_token,
        session_id=session_id,
        generation_id=generation_id,
    ):
        chunk = _log_stream_chunk_from_frame(frame, job_id=job_id, stream_name=stream_name)
        if chunk is not None:
            yield chunk

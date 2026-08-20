"""Direct loopback HTTP/SSE transport for the sandbox-browser secure runtime probe.

Extracted from ``live_acceptance.py`` (#231 rework): the pure bytes-level
plumbing underneath every browser-capability request -- one deadline-bound
direct socket connection (never a redirect- or proxy-aware client), bounded
body reading with an SSE early-stop, strict finite-JSON decoding, and media
-type/SSE-frame parsing. Nothing here decides acceptance policy or reaches
outside this one HTTP round trip; :mod:`clio_relay.live_acceptance_browser_
evidence` is the layer that turns these bytes into evidence.
"""

from __future__ import annotations

import http.client
import json
import math
import socket
import threading
import time
import urllib.parse
from contextlib import suppress
from typing import Any, Literal, cast

from clio_relay.errors import RelayError
from clio_relay.live_acceptance_models import _BrowserHttpRequestError, _BrowserHttpResponse
from clio_relay.mcp_stdio_validation import decode_strict_json


def _direct_browser_http_request(
    url: str,
    *,
    method: Literal["GET", "POST"],
    headers: dict[str, str],
    body: bytes | None,
    timeout_seconds: float,
    maximum_bytes: int,
    stop_after_sse_event: bool,
) -> _BrowserHttpResponse:
    """Issue one direct loopback request with an absolute wall-clock deadline."""
    if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
        raise RelayError("secure runtime browser timeout must be positive and finite")
    target, port = _direct_browser_http_target(url)
    deadline = time.monotonic() + timeout_seconds
    expired = threading.Event()
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=timeout_seconds)

    def abort_at_deadline() -> None:
        expired.set()
        active_socket = connection.sock
        if active_socket is not None:
            with suppress(OSError):
                active_socket.shutdown(socket.SHUT_RDWR)
        connection.close()

    timer = threading.Timer(timeout_seconds, abort_at_deadline)
    timer.daemon = True
    timer.start()
    try:
        connection.request(
            method,
            target,
            body=body,
            headers={**headers, "Connection": "close"},
        )
        response = connection.getresponse()
        if response.status < 200 or response.status > 299:
            raise _BrowserHttpRequestError(
                f"secure runtime browser request returned HTTP {response.status}",
                kind=f"http_{response.status}",
            )
        content_type_values = response.headers.get_all("Content-Type", failobj=[])
        if len(content_type_values) != 1:
            raise _BrowserHttpRequestError(
                "secure runtime browser response requires one Content-Type header",
                kind="protocol",
            )
        content_length_values = response.headers.get_all("Content-Length", failobj=[])
        if len(content_length_values) > 1:
            raise _BrowserHttpRequestError(
                "secure runtime browser response repeated Content-Length",
                kind="protocol",
            )
        transfer_encoding_values = response.headers.get_all("Transfer-Encoding", failobj=[])
        if (
            len(transfer_encoding_values) > 1
            or (transfer_encoding_values and transfer_encoding_values[0].casefold() != "chunked")
            or (transfer_encoding_values and content_length_values)
        ):
            raise _BrowserHttpRequestError(
                "secure runtime browser response had ambiguous transfer framing",
                kind="protocol",
            )
        if content_length_values:
            try:
                content_length = int(content_length_values[0])
            except ValueError as exc:
                raise _BrowserHttpRequestError(
                    "secure runtime browser response had invalid Content-Length",
                    kind="protocol",
                ) from exc
            if content_length < 0 or content_length > maximum_bytes:
                raise _BrowserHttpRequestError(
                    "secure runtime browser response exceeded its byte limit",
                    kind="flood",
                )
        payload = _read_browser_http_body(
            connection,
            response,
            deadline=deadline,
            maximum_bytes=maximum_bytes,
            stop_after_sse_event=stop_after_sse_event,
        )
        return _BrowserHttpResponse(
            status_code=int(response.status),
            content_type=str(content_type_values[0]),
            payload=payload,
        )
    except _BrowserHttpRequestError:
        raise
    except TimeoutError as exc:
        raise _BrowserHttpRequestError(
            "secure runtime browser request exceeded its absolute deadline",
            kind="deadline",
        ) from exc
    except (ConnectionRefusedError, ConnectionResetError, BrokenPipeError) as exc:
        kind = (
            "connection_refused" if isinstance(exc, ConnectionRefusedError) else "connection_reset"
        )
        raise _BrowserHttpRequestError(
            "secure runtime browser loopback proxy was unavailable",
            kind=kind,
        ) from exc
    except (OSError, http.client.HTTPException) as exc:
        if expired.is_set() or time.monotonic() >= deadline:
            raise _BrowserHttpRequestError(
                "secure runtime browser request exceeded its absolute deadline",
                kind="deadline",
            ) from exc
        error_number = getattr(exc, "winerror", None) or getattr(exc, "errno", None)
        if error_number in {61, 104, 111, 10054, 10061}:
            kind = "connection_refused" if error_number in {61, 111, 10061} else "connection_reset"
            raise _BrowserHttpRequestError(
                "secure runtime browser loopback proxy was unavailable",
                kind=kind,
            ) from exc
        raise _BrowserHttpRequestError(
            "secure runtime browser request failed at its direct loopback transport",
            kind="transport",
        ) from exc
    finally:
        timer.cancel()
        connection.close()


def _direct_browser_http_target(url: str) -> tuple[str, int]:
    """Return a direct HTTP request target without consulting redirect or proxy settings."""
    parsed = urllib.parse.urlsplit(url)
    try:
        port = parsed.port
    except ValueError as exc:
        raise RelayError("secure runtime browser URL had an invalid port") from exc
    if (
        parsed.scheme != "http"
        or parsed.hostname != "127.0.0.1"
        or port is None
        or parsed.username is not None
        or parsed.password is not None
        or parsed.fragment
        or any(character in url for character in "\r\n\x00")
    ):
        raise RelayError("secure runtime browser request requires one clean loopback HTTP URL")
    path = parsed.path or "/"
    return path + (f"?{parsed.query}" if parsed.query else ""), port


def _read_browser_http_body(
    connection: http.client.HTTPConnection,
    response: http.client.HTTPResponse,
    *,
    deadline: float,
    maximum_bytes: int,
    stop_after_sse_event: bool,
) -> bytes:
    """Read bounded decoded HTTP bytes while recomputing the absolute deadline."""
    payload = bytearray()
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise _BrowserHttpRequestError(
                "secure runtime browser request exceeded its absolute deadline",
                kind="deadline",
            )
        if connection.sock is not None:
            connection.sock.settimeout(remaining)
        chunk = response.read1(min(8192, maximum_bytes + 1 - len(payload)))
        if not chunk:
            if time.monotonic() >= deadline:
                raise _BrowserHttpRequestError(
                    "secure runtime browser request exceeded its absolute deadline",
                    kind="deadline",
                )
            break
        payload.extend(chunk)
        if len(payload) > maximum_bytes:
            raise _BrowserHttpRequestError(
                "secure runtime browser response exceeded its byte limit",
                kind="flood",
            )
        if stop_after_sse_event:
            frame_end = _first_sse_frame_end(payload)
            if frame_end is not None:
                return bytes(payload[:frame_end])
    if stop_after_sse_event:
        raise _BrowserHttpRequestError(
            "secure runtime browser events response omitted a complete event",
            kind="protocol",
        )
    if not payload:
        raise _BrowserHttpRequestError(
            "secure runtime browser response body was empty",
            kind="protocol",
        )
    return bytes(payload)


def _first_sse_frame_end(payload: bytes | bytearray) -> int | None:
    endings = [
        index + len(marker)
        for marker in (b"\n\n", b"\r\n\r\n")
        if (index := payload.find(marker)) >= 0
    ]
    return min(endings) if endings else None


def _require_media_type(content_type: str, *, expected: str) -> None:
    """Require one exact media type with at most a UTF-8 charset parameter."""
    parts = [part.strip() for part in content_type.split(";")]
    if not parts or parts[0].casefold() != expected:
        raise RelayError(f"secure runtime browser response was not {expected}")
    parameters: dict[str, str] = {}
    for raw in parts[1:]:
        name, separator, value = raw.partition("=")
        normalized_name = name.strip().casefold()
        normalized_value = value.strip().strip('"').casefold()
        if (
            separator != "="
            or not normalized_name
            or normalized_name in parameters
            or normalized_name != "charset"
            or normalized_value != "utf-8"
        ):
            raise RelayError("secure runtime browser response had invalid media-type parameters")
        parameters[normalized_name] = normalized_value


def _canonical_finite_json_bytes(value: object) -> bytes:
    try:
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise RelayError("secure runtime browser request body was not finite JSON") from exc


def _strict_finite_json(payload: bytes, *, label: str) -> object:
    """Decode UTF-8 JSON while rejecting duplicate keys and non-finite numbers."""
    try:
        return decode_strict_json(payload, label=f"secure runtime {label}")
    except RelayError:
        raise RelayError(f"secure runtime {label} was not strict finite JSON") from None


def _strict_sse_data_document(
    frame: bytes,
    *,
    expected_event_name: str,
) -> dict[str, Any]:
    """Require one complete SSE frame whose data field is a strict JSON object."""
    try:
        text = frame.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise RelayError("secure runtime browser SSE frame was not UTF-8") from exc
    normalized = text.replace("\r\n", "\n")
    if not normalized.endswith("\n\n"):
        raise RelayError("secure runtime browser SSE frame was incomplete")
    lines = normalized[:-2].split("\n")
    event_lines = [line[6:].lstrip(" ") for line in lines if line.startswith("event:")]
    if event_lines != [expected_event_name]:
        raise RelayError("secure runtime browser SSE event name did not match its adapter")
    data_lines = [line[5:].lstrip(" ") for line in lines if line.startswith("data:")]
    if not data_lines:
        raise RelayError("secure runtime browser SSE frame omitted data")
    decoded = _strict_finite_json("\n".join(data_lines).encode("utf-8"), label="SSE data")
    if not isinstance(decoded, dict):
        raise RelayError("secure runtime browser SSE data was not an object")
    return {str(key): value for key, value in cast(dict[object, object], decoded).items()}

"""Bounded-deadline HTTP readiness reads and browser-attachment support helpers.

Extracted from ``service_runtime.py`` (#231 rework slice): the
absolute-deadline bounded HTTP response reader used to poll JARVIS/local/
browser health endpoints without an ambient-proxy-discovering client
(``_read_bounded_http_response``, ``_new_readiness_http_client``,
``_sleep_before_deadline``), loopback-port selection/validation for desktop
binds (``_available_loopback_port``, ``_validated_available_loopback_port``),
and the browser-attachment capability-URL/revocation-marker helpers
(``_browser_attachment_grant``, ``_utc_timestamp``,
``_owned_browser_runtime_path``, ``_write_browser_revocation_marker``).

Depends on ``service_runtime_types`` (``_BoundedHttpResponse``,
``_BoundedHttpReadState``) -- never on the supervisor class, which imports
these names back qualified through this module instead.
"""

from __future__ import annotations

import json
import os
import secrets
import socket
import threading
import time
import urllib.parse
from collections.abc import Callable
from contextlib import suppress
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

import httpx

from clio_relay import service_runtime_types as _types
from clio_relay.browser_gateway import BrowserAttachmentGrant, BrowserAttachmentRecord
from clio_relay.config import RelaySettings
from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.models import ServiceRuntimeSpec, utc_now

# #231 service-runtime split, slice 11: shared by the start/jarvis-bind/browser
# mixins' local-health waits -- moved here (rather than duplicated three times)
# since every caller already imports this module for _read_bounded_http_response.
_RUNTIME_HEALTH_OBSERVATION_TIMEOUT_SECONDS = 5.0


def _read_bounded_http_response(
    url: str,
    *,
    headers: dict[str, str] | None,
    maximum_bytes: int | None,
    deadline: float | None = None,
) -> _types._BoundedHttpResponse:
    """Read headers and, when requested, a bounded body by one absolute deadline."""

    if maximum_bytes is not None and maximum_bytes <= 0:
        raise ValueError("HTTP response byte limit must be positive")
    effective_deadline = time.monotonic() + 5.0 if deadline is None else deadline
    remaining = effective_deadline - time.monotonic()
    if remaining <= 0:
        raise httpx.TimeoutException("HTTP response total deadline expired before connection")

    state = _types._BoundedHttpReadState()
    cancelled = threading.Event()
    completed = threading.Event()
    client = _new_readiness_http_client(remaining)

    def read_response() -> None:
        try:
            with client.stream("GET", url, headers=headers) as response:
                if maximum_bytes is None:
                    state.result = _types._BoundedHttpResponse(
                        status_code=response.status_code,
                        headers=httpx.Headers(response.headers),
                        content=b"",
                    )
                    return
                raw_length = response.headers.get("content-length")
                if raw_length is not None:
                    try:
                        content_length = int(raw_length)
                    except ValueError as exc:
                        raise ValueError("HTTP response Content-Length is invalid") from exc
                    if content_length < 0 or content_length > maximum_bytes:
                        raise ValueError(
                            f"HTTP response exceeds the {maximum_bytes}-byte decompressed limit"
                        )
                content = bytearray()
                for chunk in response.iter_bytes(chunk_size=64 * 1024):
                    if cancelled.is_set():
                        return
                    if len(content) + len(chunk) > maximum_bytes:
                        raise ValueError(
                            f"HTTP response exceeds the {maximum_bytes}-byte decompressed limit"
                        )
                    content.extend(chunk)
                if cancelled.is_set():
                    return
                state.result = _types._BoundedHttpResponse(
                    status_code=response.status_code,
                    headers=httpx.Headers(response.headers),
                    content=bytes(content),
                )
        except BaseException as exc:
            state.error = exc
        finally:
            with suppress(Exception):
                client.close()
            completed.set()

    reader = threading.Thread(
        target=read_response,
        name="clio-relay-readiness-http",
        daemon=True,
    )
    reader.start()
    completed_before_deadline = completed.wait(max(0.0, effective_deadline - time.monotonic()))
    if not completed_before_deadline or time.monotonic() > effective_deadline:
        cancelled.set()
        raise httpx.TimeoutException("HTTP response exceeded its total monotonic deadline")
    if state.error is not None:
        raise state.error
    if state.result is None:
        raise RuntimeError("HTTP response reader completed without a result or error")
    return state.result


def _new_readiness_http_client(timeout_seconds: float) -> httpx.Client:
    """Create one operation-owned client without ambient proxy discovery."""

    return httpx.Client(timeout=timeout_seconds, trust_env=False)


def _sleep_before_deadline(
    sleep: Callable[[float], None],
    poll_seconds: float,
    deadline: float,
) -> None:
    """Sleep for at most the remaining monotonic readiness budget."""

    remaining = deadline - time.monotonic()
    if remaining > 0:
        sleep(min(poll_seconds, remaining))


def _available_loopback_port(*, exclude: set[int] | None = None) -> int:
    """Select one currently free loopback TCP port outside an explicit exclusion set."""
    excluded = exclude or set()
    for _ in range(20):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
            listener.bind(("127.0.0.1", 0))
            port = cast(int, listener.getsockname()[1])
        if port not in excluded:
            return port
    raise RelayError("could not select a distinct loopback port")


def _validated_available_loopback_port(port: object) -> int:
    """Validate and availability-test an explicit operator-selected loopback port."""
    if isinstance(port, bool) or not isinstance(port, int):
        raise ConfigurationError("desktop bind port must be an integer")
    if port < 1 or port > 65_535:
        raise ConfigurationError("desktop bind port must be between 1 and 65535")
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
            listener.bind(("127.0.0.1", port))
    except OSError as exc:
        raise ConfigurationError(f"desktop bind port is already occupied: {port}") from exc
    return port


def _browser_attachment_grant(
    *,
    record: BrowserAttachmentRecord,
    capability: str,
    spec: ServiceRuntimeSpec,
) -> BrowserAttachmentGrant:
    """Build the one-time capability URLs without copying them into gateway state."""
    if spec.command_path is None or spec.stream_path is None or spec.event_stream_path is None:
        raise ConfigurationError("browser attachment requires stream, events, and command paths")
    if spec.state_path is None:
        raise ConfigurationError("browser attachment requires a state path")
    base = f"http://{record.bind_addr}:{record.bind_port}"

    def capability_url(path: str) -> str:
        encoded = urllib.parse.urlencode({"capability": capability})
        return f"{base}{path}?{encoded}"

    return BrowserAttachmentGrant(
        attachment_id=record.attachment_id,
        expires_at=record.expires_at,
        connect_url=capability_url("/"),
        health_url=capability_url(spec.health_path),
        stream_url=capability_url(spec.stream_path),
        events_url=capability_url(spec.event_stream_path),
        state_url=capability_url(spec.state_path),
        command_url=capability_url(spec.command_path),
    )


def _utc_timestamp(value: str) -> datetime:
    """Parse one explicitly UTC persisted timestamp."""
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise RelayError("browser attachment timestamp is invalid") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != UTC.utcoffset(parsed):
        raise RelayError("browser attachment timestamp is not UTC")
    return parsed


def _owned_browser_runtime_path(
    settings: RelaySettings,
    session_id: str,
    raw_path: str,
) -> Path:
    """Resolve a browser attachment path only inside its owned runtime directory."""
    expected = (settings.core_dir.parent / "runtime-sessions" / session_id).resolve()
    path = Path(raw_path).resolve()
    if path.parent != expected:
        raise RelayError("browser attachment revocation path escaped its runtime directory")
    return path


def _write_browser_revocation_marker(path: Path, attachment_id: str) -> None:
    """Durably revoke a browser capability before process cleanup begins."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{secrets.token_hex(8)}.tmp")
    try:
        with temporary.open("w", encoding="utf-8") as handle:
            handle.write(
                json.dumps(
                    {
                        "schema_version": "clio-relay.browser-capability-revocation.v1",
                        "attachment_id": attachment_id,
                        "revoked_at": utc_now().isoformat(),
                    },
                    sort_keys=True,
                )
                + "\n"
            )
            handle.flush()
            os.fsync(handle.fileno())
        temporary.chmod(0o600)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)

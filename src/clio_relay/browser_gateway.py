"""Capability-authenticated loopback proxy for sandboxed scientific viewers."""

from __future__ import annotations

import argparse
import hashlib
import hmac
import http.client
import json
import os
import socket
import sys
import threading
import time
import urllib.parse
from contextlib import suppress
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, BinaryIO, ClassVar, Literal, cast

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

CAPABILITY_ENV = "CLIO_RELAY_BROWSER_CAPABILITY"
UPSTREAM_AUTHORIZATION_ENV = "CLIO_RELAY_BROWSER_UPSTREAM_AUTHORIZATION"
BROWSER_GATEWAY_CONFIG_SCHEMA = "clio-relay.browser-gateway-config.v1"
BROWSER_GATEWAY_BOOTSTRAP_SCHEMA = "clio-relay.browser-gateway-bootstrap.v1"
BROWSER_ATTACHMENT_SCHEMA = "clio-relay.browser-attachment.v1"
BROWSER_ATTACHMENT_RECORD_SCHEMA = "clio-relay.browser-attachment-record.v1"
BROWSER_DETACHMENT_SCHEMA = "clio-relay.browser-detachment.v1"
MAX_REQUEST_BODY_BYTES = 4 * 1024 * 1024
MAX_BROWSER_BOOTSTRAP_BYTES = 4096
MAX_ACTIVE_BROWSER_REQUESTS = 32
MAX_BROWSER_OVERLOAD_RESPONDERS = 4
BROWSER_REQUEST_BACKLOG = 32
BROWSER_CLIENT_IO_TIMEOUT_SECONDS = 15.0
BROWSER_CLIENT_REQUEST_DEADLINE_SECONDS = 15.0
BROWSER_OVERLOAD_IO_TIMEOUT_SECONDS = 1.0
BROWSER_OVERLOAD_REQUEST_DEADLINE_SECONDS = 1.0
UPSTREAM_CONNECT_TIMEOUT_SECONDS = 5.0
UPSTREAM_COMMAND_RESPONSE_TIMEOUT_SECONDS = 900.0
UPSTREAM_IDLE_TIMEOUT_SECONDS = 60.0
_CAPABILITY_QUERY_KEY = "capability"
_ALLOWED_REQUEST_HEADERS = frozenset({"accept", "cache-control", "content-type", "last-event-id"})
_HOP_BY_HOP_HEADERS = frozenset(
    {
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
    }
)
_STRIPPED_RESPONSE_HEADERS = _HOP_BY_HOP_HEADERS | frozenset(
    {
        "access-control-allow-credentials",
        "access-control-allow-headers",
        "access-control-allow-methods",
        "access-control-allow-origin",
        "access-control-expose-headers",
        "access-control-max-age",
    }
)


class BrowserGatewayConfig(BaseModel):
    """Non-secret process configuration for one browser attachment proxy."""

    model_config = ConfigDict(extra="forbid", strict=True)

    schema_version: Literal["clio-relay.browser-gateway-config.v1"] = BROWSER_GATEWAY_CONFIG_SCHEMA
    attachment_id: str = Field(min_length=1, max_length=256)
    token_sha256: str
    bind_addr: Literal["127.0.0.1"] = "127.0.0.1"
    bind_port: int = Field(gt=0, le=65_535)
    upstream_protocol: Literal["http", "https"]
    upstream_addr: Literal["127.0.0.1"] = "127.0.0.1"
    upstream_port: int = Field(gt=0, le=65_535)
    allowed_paths: list[str] = Field(min_length=1, max_length=16)
    command_path: str
    expires_at: str
    revocation_path: str

    @field_validator("token_sha256")
    @classmethod
    def validate_token_digest(cls, value: str) -> str:
        """Require a canonical capability digest without persisting the capability."""
        if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
            raise ValueError("token_sha256 must be a canonical SHA-256")
        return value

    @field_validator("allowed_paths")
    @classmethod
    def validate_allowed_paths(cls, value: list[str]) -> list[str]:
        """Restrict forwarding to a small exact set of normalized HTTP paths."""
        if len(value) != len(set(value)):
            raise ValueError("allowed_paths must be unique")
        for path in value:
            parsed = urllib.parse.urlsplit(path)
            if (
                not path.startswith("/")
                or parsed.path != path
                or parsed.query
                or parsed.fragment
                or "//" in path
                or "\\" in path
            ):
                raise ValueError("allowed_paths must contain normalized absolute paths")
        return value

    @model_validator(mode="after")
    def validate_expiry_and_revocation_path(self) -> BrowserGatewayConfig:
        """Require a future UTC expiry and one absolute revocation-marker path."""
        expiry = parse_utc_timestamp(self.expires_at, "expires_at")
        if expiry.timestamp() <= 0:
            raise ValueError("expires_at must be after the Unix epoch")
        if not Path(self.revocation_path).is_absolute():
            raise ValueError("revocation_path must be absolute")
        if self.command_path not in self.allowed_paths:
            raise ValueError("command_path must appear in allowed_paths")
        return self


class BrowserGatewayBootstrap(BaseModel):
    """One bounded stdin-only secret handoff for a browser gateway process."""

    model_config = ConfigDict(extra="forbid", strict=True)

    schema_version: Literal["clio-relay.browser-gateway-bootstrap.v1"] = (
        BROWSER_GATEWAY_BOOTSTRAP_SCHEMA
    )
    capability: str = Field(min_length=43, max_length=512)
    upstream_authorization: str | None = Field(default=None, max_length=96)

    @field_validator("upstream_authorization")
    @classmethod
    def validate_upstream_authorization(cls, value: str | None) -> str | None:
        """Accept only the exact execution-owned 256-bit bearer shape."""
        if value is None:
            return None
        scheme, separator, token = value.partition(" ")
        if (
            scheme != "Bearer"
            or separator != " "
            or len(token) != 64
            or any(character not in "0123456789abcdef" for character in token)
        ):
            raise ValueError("upstream authorization must contain one 256-bit bearer token")
        return value


class BrowserAttachmentGrant(BaseModel):
    """One-time browser capability returned only by explicit attachment."""

    model_config = ConfigDict(extra="forbid", strict=True)

    schema_version: Literal["clio-relay.browser-attachment.v1"] = BROWSER_ATTACHMENT_SCHEMA
    attachment_id: str = Field(min_length=1, max_length=256)
    expires_at: str
    connect_url: str
    health_url: str
    stream_url: str
    events_url: str
    state_url: str
    command_url: str


class BrowserAttachmentRecord(BaseModel):
    """Safe, revocable browser attachment metadata stored in a gateway record."""

    model_config = ConfigDict(extra="forbid", strict=True)

    schema_version: Literal["clio-relay.browser-attachment-record.v1"] = (
        BROWSER_ATTACHMENT_RECORD_SCHEMA
    )
    attachment_id: str = Field(min_length=1, max_length=256)
    state: Literal["starting", "active", "revoking", "revoked", "failed"]
    issued_at: str
    expires_at: str
    revoked_at: str | None = None
    token_sha256: str
    bind_addr: Literal["127.0.0.1"] = "127.0.0.1"
    bind_port: int = Field(gt=0, le=65_535)
    proxy_process_id: int | None = Field(default=None, gt=0)
    revocation_path: str

    @field_validator("token_sha256")
    @classmethod
    def validate_token_digest(cls, value: str) -> str:
        """Require a canonical persisted capability digest."""
        if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
            raise ValueError("token_sha256 must be a canonical SHA-256")
        return value

    @model_validator(mode="after")
    def validate_lifecycle(self) -> BrowserAttachmentRecord:
        """Keep revocation timestamps coherent with the attachment state."""
        parse_utc_timestamp(self.issued_at, "issued_at")
        parse_utc_timestamp(self.expires_at, "expires_at")
        if self.state == "revoked":
            if self.revoked_at is None:
                raise ValueError("revoked browser attachments require revoked_at")
            parse_utc_timestamp(self.revoked_at, "revoked_at")
        elif self.revoked_at is not None:
            raise ValueError("only revoked browser attachments may contain revoked_at")
        return self


class BrowserDetachmentResult(BaseModel):
    """Exact revocation result returned by an explicit browser detach."""

    model_config = ConfigDict(extra="forbid", strict=True)

    schema_version: Literal["clio-relay.browser-detachment.v1"] = BROWSER_DETACHMENT_SCHEMA
    attachment_id: str = Field(min_length=1, max_length=256)
    state: Literal["revoked"] = "revoked"
    revoked_at: str
    already_revoked: bool
    proxy_process_id: int | None = Field(default=None, gt=0)
    proxy_stopped: bool
    capability_revoked: Literal[True] = True

    @field_validator("revoked_at")
    @classmethod
    def validate_revoked_at(cls, value: str) -> str:
        """Require an explicitly UTC revocation timestamp."""
        parse_utc_timestamp(value, "revoked_at")
        return value


class _AbsoluteSocketDeadline:
    """Close one request socket when an absolute input deadline expires."""

    def __init__(self, request_socket: socket.socket, timeout_seconds: float) -> None:
        if timeout_seconds <= 0:
            raise ValueError("browser request deadline must be positive")
        self._request_socket = request_socket
        self._lock = threading.Lock()
        self._finished = False
        self._expired = False
        self._timer = threading.Timer(timeout_seconds, self._expire)
        self._timer.daemon = True

    def start(self) -> None:
        """Start the one-shot absolute deadline."""
        self._timer.start()

    def complete(self) -> None:
        """Disarm the deadline once headers, authorization, and body are complete."""
        with self._lock:
            if self._finished:
                return
            self._finished = True
        self._timer.cancel()

    @property
    def expired(self) -> bool:
        """Return whether the deadline, rather than normal completion, won the race."""
        with self._lock:
            return self._expired

    def _expire(self) -> None:
        with self._lock:
            if self._finished:
                return
            self._finished = True
            self._expired = True
        with suppress(OSError):
            self._request_socket.shutdown(socket.SHUT_RDWR)


class _OverloadedRequestHandler(BaseHTTPRequestHandler):
    """Parse one rejected request before returning a deterministic HTTP 503."""

    protocol_version = "HTTP/1.1"
    server_version = "clio-relay-browser-gateway/1"
    sys_version = ""

    def do_GET(self) -> None:  # noqa: N802
        """Reject an overloaded GET with a complete close-delimited response."""
        self._reject()

    def do_POST(self) -> None:  # noqa: N802
        """Reject an overloaded POST with a complete close-delimited response."""
        self._reject()

    def do_OPTIONS(self) -> None:  # noqa: N802
        """Reject an overloaded preflight with a complete HTTP response."""
        self._reject()

    def do_HEAD(self) -> None:  # noqa: N802
        """Reject an overloaded HEAD without writing a response body."""
        self._reject()

    def _reject(self) -> None:
        payload = b'{"error":"browser attachment request capacity exhausted"}'
        self.close_connection = True
        self.send_response(503)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("Access-Control-Allow-Origin", "null")
        self.send_header("Vary", "Origin")
        self.send_header("Retry-After", "1")
        self.send_header("Connection", "close")
        self.end_headers()
        if self.command != "HEAD":
            with suppress(BrokenPipeError, ConnectionResetError, OSError):
                self.wfile.write(payload)
                self.wfile.flush()

    def log_message(self, format: str, *args: object) -> None:
        """Avoid attacker-controlled request text in overload logs."""
        del format, args


class CapabilityProxyServer(ThreadingHTTPServer):
    """Threaded loopback server with immutable attachment configuration."""

    daemon_threads = True
    allow_reuse_address = False
    request_queue_size = BROWSER_REQUEST_BACKLOG

    def __init__(
        self,
        config: BrowserGatewayConfig,
        capability: str,
        upstream_authorization: str | None = None,
    ) -> None:
        observed = hashlib.sha256(capability.encode("utf-8")).hexdigest()
        if not hmac.compare_digest(observed, config.token_sha256):
            raise ValueError("browser capability does not match its configured digest")
        if upstream_authorization is not None:
            scheme, separator, token = upstream_authorization.partition(" ")
            if (
                scheme != "Bearer"
                or separator != " "
                or len(token) != 64
                or any(character not in "0123456789abcdef" for character in token)
            ):
                raise ValueError("upstream authorization must contain one 256-bit bearer token")
        self.config = config
        self.capability = capability
        self.upstream_authorization = upstream_authorization
        self._maximum_active_requests = MAX_ACTIVE_BROWSER_REQUESTS
        self._active_request_count = 0
        self._active_request_condition = threading.Condition()
        self._overload_slots = threading.BoundedSemaphore(MAX_BROWSER_OVERLOAD_RESPONDERS)
        super().__init__((config.bind_addr, config.bind_port), CapabilityProxyHandler)

    def process_request(self, request: Any, client_address: Any) -> None:
        """Start a handler only when the bounded attachment budget has capacity."""

        request_socket = cast(socket.socket, request)
        if not self._admit_request():
            self._start_overload_response(request_socket, client_address)
            return
        try:
            super().process_request(request_socket, client_address)
        except BaseException:
            self._release_request()
            raise

    def process_request_thread(
        self,
        request: Any,
        client_address: Any,
    ) -> None:
        """Release one request slot after every success, error, or disconnect."""

        request_socket = cast(socket.socket, request)
        try:
            request_socket.settimeout(BROWSER_CLIENT_IO_TIMEOUT_SECONDS)
            super().process_request_thread(request_socket, client_address)
        finally:
            self._release_request()

    @property
    def active_request_count(self) -> int:
        """Return the exact number of admitted live handler threads."""
        with self._active_request_condition:
            return self._active_request_count

    def wait_for_active_request_count(self, expected: int, timeout: float) -> bool:
        """Wait for an exact admitted-handler count for diagnostics and tests."""
        if not 0 <= expected <= self._maximum_active_requests:
            raise ValueError("expected active request count is outside the server bound")
        if timeout < 0:
            raise ValueError("active request wait timeout cannot be negative")
        with self._active_request_condition:
            return self._active_request_condition.wait_for(
                lambda: self._active_request_count == expected,
                timeout=timeout,
            )

    def _admit_request(self) -> bool:
        """Atomically reserve one bounded handler slot."""
        with self._active_request_condition:
            if self._active_request_count >= self._maximum_active_requests:
                return False
            self._active_request_count += 1
            self._active_request_condition.notify_all()
            return True

    def _release_request(self) -> None:
        """Release one previously admitted handler slot exactly once."""
        with self._active_request_condition:
            if self._active_request_count < 1:
                raise RuntimeError("browser request admission accounting underflow")
            self._active_request_count -= 1
            self._active_request_condition.notify_all()

    def _start_overload_response(
        self,
        request_socket: socket.socket,
        client_address: Any,
    ) -> None:
        """Use a separately bounded parser so Windows closes after a valid response."""
        if not self._overload_slots.acquire(blocking=False):
            self.shutdown_request(request_socket)
            return
        worker = threading.Thread(
            target=self._process_overloaded_request,
            args=(request_socket, client_address),
            name=f"browser-gateway-overload-{self.config.attachment_id}",
            daemon=True,
        )
        try:
            worker.start()
        except BaseException:
            self._overload_slots.release()
            self.shutdown_request(request_socket)
            raise

    def _process_overloaded_request(
        self,
        request_socket: socket.socket,
        client_address: Any,
    ) -> None:
        """Parse request headers, flush HTTP 503, and close without a Windows RST."""
        deadline = _AbsoluteSocketDeadline(
            request_socket,
            BROWSER_OVERLOAD_REQUEST_DEADLINE_SECONDS,
        )
        deadline.start()
        try:
            request_socket.settimeout(BROWSER_OVERLOAD_IO_TIMEOUT_SECONDS)
            with suppress(BrokenPipeError, ConnectionResetError, TimeoutError, OSError):
                _OverloadedRequestHandler(request_socket, client_address, self)
        finally:
            deadline.complete()
            try:
                self.shutdown_request(request_socket)
            finally:
                self._overload_slots.release()


class CapabilityProxyHandler(BaseHTTPRequestHandler):
    """Authorize and proxy one narrowly scoped browser request."""

    protocol_version = "HTTP/1.1"
    server_version = "clio-relay-browser-gateway/1"
    sys_version = ""
    _SUPPORTED_METHODS: ClassVar[frozenset[str]] = frozenset({"GET", "POST", "OPTIONS"})
    _request_input_deadline: _AbsoluteSocketDeadline | None = None

    def handle_one_request(self) -> None:
        """Bound every keep-alive request until authentication and body completion."""
        deadline = _AbsoluteSocketDeadline(
            cast(socket.socket, self.connection),
            BROWSER_CLIENT_REQUEST_DEADLINE_SECONDS,
        )
        self._request_input_deadline = deadline
        deadline.start()
        try:
            super().handle_one_request()
        except OSError:
            if deadline.expired:
                self.close_connection = True
                return
            raise
        finally:
            deadline.complete()
            self._request_input_deadline = None

    @property
    def capability_server(self) -> CapabilityProxyServer:
        """Return the precisely typed immutable server configuration."""
        return cast(CapabilityProxyServer, self.server)

    def do_GET(self) -> None:  # noqa: N802
        """Proxy an authenticated GET, including bounded SSE streaming."""
        self._proxy_request()

    def do_POST(self) -> None:  # noqa: N802
        """Proxy an authenticated bounded command request."""
        self._proxy_request()

    def do_OPTIONS(self) -> None:  # noqa: N802
        """Answer only valid capability-bearing sandbox preflight requests."""
        target = self._authorize()
        if target is None:
            return
        try:
            self._request_body()
        except ValueError as exc:
            self._error(413, str(exc))
            return
        self._mark_request_input_complete()
        requested_method = self.headers.get("Access-Control-Request-Method")
        if requested_method not in {"GET", "POST"}:
            self._error(403, "requested method is not allowed")
            return
        if requested_method == "POST" and urllib.parse.urlsplit(target).path != (
            self.capability_server.config.command_path
        ):
            self._error(403, "POST is allowed only for the command endpoint")
            return
        requested_headers = {
            item.strip().casefold()
            for item in self.headers.get("Access-Control-Request-Headers", "").split(",")
            if item.strip()
        }
        if not requested_headers.issubset(_ALLOWED_REQUEST_HEADERS):
            self._error(403, "requested headers are not allowed")
            return
        self.send_response(204)
        self._cors_headers()
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header(
            "Access-Control-Allow-Headers",
            "Accept, Cache-Control, Content-Type, Last-Event-ID",
        )
        self.send_header("Access-Control-Max-Age", "300")
        self.send_header("Content-Length", "0")
        self.end_headers()

    def _authorize(self) -> str | None:
        config = self.capability_server.config
        parsed = urllib.parse.urlsplit(self.path)
        if parsed.path not in config.allowed_paths:
            self._error(404, "attachment path is not available")
            return None
        if self.headers.get_all("Origin", failobj=[]) != ["null"]:
            self._error(403, "browser attachment requires Origin: null")
            return None
        query = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
        supplied = query.pop(_CAPABILITY_QUERY_KEY, [])
        if len(supplied) != 1 or not hmac.compare_digest(
            supplied[0], self.capability_server.capability
        ):
            self._error(401, "browser capability is invalid")
            return None
        if Path(config.revocation_path).exists():
            self._error(401, "browser capability is revoked")
            return None
        if time.time() >= parse_utc_timestamp(config.expires_at, "expires_at").timestamp():
            self._error(401, "browser capability is expired")
            return None
        encoded_query = urllib.parse.urlencode(query, doseq=True)
        return parsed.path + (f"?{encoded_query}" if encoded_query else "")

    def _proxy_request(self) -> None:
        target = self._authorize()
        if target is None:
            return
        if self.command not in self._SUPPORTED_METHODS - {"OPTIONS"}:
            self._error(405, "method is not allowed")
            return
        if self.command == "POST" and urllib.parse.urlsplit(target).path != (
            self.capability_server.config.command_path
        ):
            self._error(405, "POST is allowed only for the command endpoint")
            return
        try:
            body = self._request_body()
        except ValueError as exc:
            self._error(413, str(exc))
            return
        self._mark_request_input_complete()
        config = self.capability_server.config
        connection_type: type[http.client.HTTPConnection] = (
            http.client.HTTPSConnection
            if config.upstream_protocol == "https"
            else http.client.HTTPConnection
        )
        connection = connection_type(
            config.upstream_addr,
            config.upstream_port,
            timeout=UPSTREAM_CONNECT_TIMEOUT_SECONDS,
        )
        request_headers = {
            name: value
            for name, value in self.headers.items()
            if name.casefold() in _ALLOWED_REQUEST_HEADERS
        }
        request_headers["Host"] = f"{config.upstream_addr}:{config.upstream_port}"
        if self.capability_server.upstream_authorization is not None:
            request_headers["Authorization"] = self.capability_server.upstream_authorization
        response_started = False
        try:
            # Keep connection establishment tightly bounded, then allow an
            # authenticated command to perform real remote work before it emits
            # response headers.  HTTPConnection otherwise reuses its connect
            # timeout while getresponse() waits for those headers.
            connection.connect()
            if (
                connection.sock is not None
                and self.command == "POST"
                and urllib.parse.urlsplit(target).path == config.command_path
            ):
                connection.sock.settimeout(UPSTREAM_COMMAND_RESPONSE_TIMEOUT_SECONDS)
            connection.request(self.command, target, body=body, headers=request_headers)
            response = connection.getresponse()
            if connection.sock is not None:
                connection.sock.settimeout(UPSTREAM_IDLE_TIMEOUT_SECONDS)
            media_type = response.getheader("Content-Type", "").partition(";")[0].strip()
            close_downstream = (
                media_type.casefold() == "text/event-stream"
                or response.getheader("Content-Length") is None
            )
            self.send_response(response.status, response.reason)
            for name, value in response.getheaders():
                if name.casefold() not in _STRIPPED_RESPONSE_HEADERS:
                    self.send_header(name, value)
            self._cors_headers()
            if close_downstream:
                self.send_header("Connection", "close")
                self.close_connection = True
            self.end_headers()
            response_started = True
            while True:
                chunk = response.read1(64 * 1024)
                if not chunk:
                    break
                self.wfile.write(chunk)
                self.wfile.flush()
            if response.length not in {None, 0}:
                self.close_connection = True
        except (BrokenPipeError, ConnectionResetError):
            self.close_connection = True
            return
        except (TimeoutError, OSError, http.client.HTTPException):
            if response_started:
                self.close_connection = True
            elif not self.wfile.closed:
                with suppress(BrokenPipeError, ConnectionResetError, OSError):
                    self._error(502, "upstream service is unavailable")
        finally:
            connection.close()

    def _request_body(self) -> bytes | None:
        raw_length = self.headers.get("Content-Length")
        if self.headers.get("Transfer-Encoding") is not None:
            raise ValueError("chunked request bodies are not accepted")
        if raw_length is None:
            return None
        try:
            length = int(raw_length)
        except ValueError as exc:
            raise ValueError("request Content-Length is invalid") from exc
        if length < 0 or length > MAX_REQUEST_BODY_BYTES:
            raise ValueError("request body exceeds the browser gateway limit")
        body = self.rfile.read(length)
        if len(body) != length:
            deadline = self._request_input_deadline
            if deadline is not None and deadline.expired:
                raise TimeoutError("browser request input exceeded its absolute deadline")
            raise ValueError("request body ended before its declared Content-Length")
        return body

    def _mark_request_input_complete(self) -> None:
        """Disarm only after the current request is authenticated and fully consumed."""
        deadline = self._request_input_deadline
        if deadline is not None:
            deadline.complete()

    def _cors_headers(self) -> None:
        self.send_header("Access-Control-Allow-Origin", "null")
        self.send_header("Vary", "Origin")
        self.send_header("Cache-Control", "no-store")

    def _error(self, status: int, message: str) -> None:
        payload = json.dumps({"error": message}, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        if self.command != "HEAD":
            with suppress(BrokenPipeError, ConnectionResetError, OSError):
                self.wfile.write(payload)

    def log_message(self, format: str, *args: object) -> None:
        """Emit bounded process-log entries without logging capability-bearing URLs."""
        del format
        command = getattr(self, "command", None)
        method = command if command in self._SUPPORTED_METHODS else "unparsed"
        status_value = args[1] if len(args) > 1 else None
        status = (
            str(status_value)
            if isinstance(status_value, int)
            or (
                isinstance(status_value, str)
                and len(status_value) == 3
                and status_value.isascii()
                and status_value.isdigit()
            )
            else "unknown"
        )
        sys.stderr.write(f"browser-gateway method={method} status={status}\n")


def parse_utc_timestamp(value: str, field: str) -> datetime:
    """Parse an explicitly UTC ISO-8601 timestamp."""
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{field} must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != UTC.utcoffset(parsed):
        raise ValueError(f"{field} must be UTC")
    return parsed


def load_browser_gateway_config(path: Path) -> BrowserGatewayConfig:
    """Load and validate one non-secret proxy configuration file."""
    try:
        return BrowserGatewayConfig.model_validate_json(path.read_text(encoding="utf-8-sig"))
    except OSError as exc:
        raise ValueError(f"cannot read browser gateway configuration: {exc}") from exc


def read_browser_gateway_bootstrap(stream: BinaryIO) -> BrowserGatewayBootstrap:
    """Read exactly one bounded duplicate-key-free bootstrap object from stdin."""
    content = stream.read(MAX_BROWSER_BOOTSTRAP_BYTES + 1)
    if len(content) > MAX_BROWSER_BOOTSTRAP_BYTES:
        raise ValueError("browser gateway bootstrap exceeds its byte limit")
    if not content:
        raise ValueError("browser gateway bootstrap is missing")

    def unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"browser gateway bootstrap contains duplicate key: {key}")
            result[key] = value
        return result

    def reject_constant(value: str) -> object:
        raise ValueError(f"browser gateway bootstrap contains non-finite constant: {value}")

    try:
        decoded = json.loads(
            content.decode("utf-8-sig"),
            object_pairs_hook=unique_object,
            parse_constant=reject_constant,
        )
        return BrowserGatewayBootstrap.model_validate(decoded)
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError) as exc:
        raise ValueError("browser gateway bootstrap is not valid UTF-8 JSON") from exc


def serve_browser_gateway(
    config: BrowserGatewayConfig,
    capability: str,
    upstream_authorization: str | None = None,
) -> None:
    """Serve one attachment until revocation, expiry, or an owned process stop."""
    expires_at = parse_utc_timestamp(config.expires_at, "expires_at").timestamp()
    if Path(config.revocation_path).exists() or time.time() >= expires_at:
        return
    server = CapabilityProxyServer(config, capability, upstream_authorization)
    watchdog_stop = threading.Event()

    def stop_when_authority_ends() -> None:
        while not watchdog_stop.wait(0.1):
            if Path(config.revocation_path).exists() or time.time() >= expires_at:
                server.shutdown()
                return

    watchdog = threading.Thread(
        target=stop_when_authority_ends,
        name=f"browser-gateway-watchdog-{config.attachment_id}",
        daemon=True,
    )
    watchdog.start()
    try:
        server.serve_forever(poll_interval=0.25)
    finally:
        watchdog_stop.set()
        server.server_close()
        watchdog.join(timeout=2)


def main(argv: list[str] | None = None) -> int:
    """Run the internal browser gateway process."""
    parser = argparse.ArgumentParser(prog="clio-relay-browser-gateway")
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--process-label", choices=["clio-relay-browser-frpc-proxy"])
    arguments = parser.parse_args(argv)
    os.environ.pop(CAPABILITY_ENV, None)
    os.environ.pop(UPSTREAM_AUTHORIZATION_ENV, None)
    try:
        bootstrap = read_browser_gateway_bootstrap(sys.stdin.buffer)
    except ValueError:
        parser.error("browser gateway bootstrap on stdin is invalid")
    config = load_browser_gateway_config(arguments.config.resolve())
    serve_browser_gateway(
        config,
        bootstrap.capability,
        bootstrap.upstream_authorization,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

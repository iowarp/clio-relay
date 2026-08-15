"""Door-owned error rendering for the loopback browser gateway."""

from __future__ import annotations

import json
from contextlib import suppress
from http.server import BaseHTTPRequestHandler


def browser_gateway_error(reason: str, message: str) -> tuple[int, dict[str, object]]:
    """Render a gateway refusal without creating the gateway/core import cycle."""
    from clio_relay import door_errors

    fault = door_errors.fault_for_reason(reason, message)
    return door_errors.as_browser_gateway_error(fault)


class OverloadedRequestHandler(BaseHTTPRequestHandler):
    """Return a complete typed 503 while the bounded gateway is saturated."""

    protocol_version = "HTTP/1.1"
    server_version = "clio-relay-browser-gateway/1"
    sys_version = ""

    def do_GET(self) -> None:  # noqa: N802
        """Reject an overloaded GET."""
        self._reject()

    def do_POST(self) -> None:  # noqa: N802
        """Reject an overloaded POST."""
        self._reject()

    def do_OPTIONS(self) -> None:  # noqa: N802
        """Reject an overloaded preflight."""
        self._reject()

    def do_HEAD(self) -> None:  # noqa: N802
        """Reject an overloaded HEAD without a body."""
        self._reject()

    def _reject(self) -> None:
        status_code, document = browser_gateway_error(
            "browser_gateway_overloaded",
            "browser attachment request capacity exhausted",
        )
        payload = json.dumps(document, separators=(",", ":")).encode("utf-8")
        self.close_connection = True
        self.send_response(status_code)
        self.send_header("Content-Type", "application/problem+json")
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

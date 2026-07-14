"""Bounded HTTP service for live gateway-runtime acceptance."""

from __future__ import annotations

import argparse
import re
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class HealthHandler(BaseHTTPRequestHandler):
    """Serve a small machine-readable health response."""

    health_body: bytes = b""

    def do_GET(self) -> None:  # noqa: N802
        """Return the exact runtime nonce only on the configured health path."""
        if self.path != "/healthz":
            self.send_error(404)
            return
        payload = self.health_body
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, format: str, *args: object) -> None:
        """Emit standard request logs for operator diagnostics."""
        super().log_message(format, *args)


def main() -> None:
    """Run the service until its bounded acceptance lifetime expires."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--health-nonce", required=True)
    parser.add_argument("--lifetime-seconds", type=int, default=900)
    args = parser.parse_args()
    if not 1 <= args.port <= 65535:
        raise SystemExit("port must be between 1 and 65535")
    if re.fullmatch(r"[0-9a-f]{64}", args.health_nonce) is None:
        raise SystemExit("health nonce must be 64 lowercase hexadecimal characters")
    if not 30 <= args.lifetime_seconds <= 1800:
        raise SystemExit("lifetime must be between 30 and 1800 seconds")
    HealthHandler.health_body = args.health_nonce.encode("ascii")
    deadline = time.monotonic() + args.lifetime_seconds
    with ThreadingHTTPServer(("0.0.0.0", args.port), HealthHandler) as server:
        server.timeout = 1.0
        while time.monotonic() < deadline:
            server.handle_request()


if __name__ == "__main__":
    main()

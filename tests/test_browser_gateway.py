from __future__ import annotations

import hashlib
import json
import socket
import threading
from collections.abc import Generator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import httpx

from clio_relay.browser_gateway import (
    BrowserGatewayConfig,
    CapabilityProxyServer,
    serve_browser_gateway,
)


def test_browser_gateway_requires_capability_and_exact_null_origin(tmp_path: Path) -> None:
    with _backend_server() as (backend_port, requests):
        capability = "x" * 43
        revocation_path = tmp_path / "revoked"
        with _capability_proxy(
            backend_port=backend_port,
            capability=capability,
            revocation_path=revocation_path,
        ) as proxy_port:
            base = f"http://127.0.0.1:{proxy_port}/healthz"
            missing = httpx.get(base, headers={"Origin": "null"})
            wrong_origin = httpx.get(
                f"{base}?{urlencode({'capability': capability})}",
                headers={"Origin": "https://example.invalid"},
            )
            valid = httpx.get(
                f"{base}?{urlencode({'capability': capability, 'probe': '1'})}",
                headers={"Origin": "null"},
            )

            assert missing.status_code == 401
            assert "access-control-allow-origin" not in missing.headers
            assert wrong_origin.status_code == 403
            assert "access-control-allow-origin" not in wrong_origin.headers
            assert valid.status_code == 200
            assert valid.headers["access-control-allow-origin"] == "null"
            assert "*" not in valid.headers["access-control-allow-origin"]
            assert requests == [("GET", "/healthz?probe=1", b"")]


def test_browser_gateway_preflight_methods_stream_and_command_are_narrow(
    tmp_path: Path,
) -> None:
    with _backend_server() as (backend_port, requests):
        capability = "y" * 43
        with _capability_proxy(
            backend_port=backend_port,
            capability=capability,
            revocation_path=tmp_path / "revoked",
        ) as proxy_port:
            query = urlencode({"capability": capability})
            command_url = f"http://127.0.0.1:{proxy_port}/commands?{query}"
            state_url = f"http://127.0.0.1:{proxy_port}/state?{query}"
            events_url = f"http://127.0.0.1:{proxy_port}/events?{query}"
            preflight = httpx.options(
                command_url,
                headers={
                    "Origin": "null",
                    "Access-Control-Request-Method": "POST",
                    "Access-Control-Request-Headers": "Content-Type, Accept",
                },
            )
            forbidden_preflight = httpx.options(
                command_url,
                headers={
                    "Origin": "null",
                    "Access-Control-Request-Method": "DELETE",
                },
            )
            wrong_post = httpx.post(state_url, headers={"Origin": "null"}, content=b"{}")
            command = httpx.post(
                command_url,
                headers={"Origin": "null", "Content-Type": "application/json"},
                content=b'{"operation":"render"}',
            )
            events = httpx.get(events_url, headers={"Origin": "null"})

            assert preflight.status_code == 204
            assert preflight.headers["access-control-allow-origin"] == "null"
            assert preflight.headers["access-control-allow-methods"] == "GET, POST, OPTIONS"
            assert forbidden_preflight.status_code == 403
            assert wrong_post.status_code == 405
            assert command.status_code == 200
            assert command.json() == {"accepted": True}
            assert events.text == "data: ready\n\n"
            assert requests == [
                ("POST", "/commands", b'{"operation":"render"}'),
                ("GET", "/events", b""),
            ]


def test_browser_gateway_revocation_and_expiry_fail_closed(tmp_path: Path) -> None:
    with _backend_server() as (backend_port, requests):
        capability = "z" * 43
        revocation_path = tmp_path / "revoked"
        with _capability_proxy(
            backend_port=backend_port,
            capability=capability,
            revocation_path=revocation_path,
        ) as proxy_port:
            url = f"http://127.0.0.1:{proxy_port}/healthz?{urlencode({'capability': capability})}"
            assert httpx.get(url, headers={"Origin": "null"}).status_code == 200
            revocation_path.write_text("revoked\n", encoding="utf-8")
            revoked = httpx.get(url, headers={"Origin": "null"})
            assert revoked.status_code == 401
            assert "access-control-allow-origin" not in revoked.headers
        with _capability_proxy(
            backend_port=backend_port,
            capability=capability,
            revocation_path=tmp_path / "other-revoked",
            expires_at=datetime(2000, 1, 1, tzinfo=UTC),
        ) as proxy_port:
            expired_url = (
                f"http://127.0.0.1:{proxy_port}/healthz?{urlencode({'capability': capability})}"
            )
            expired = httpx.get(expired_url, headers={"Origin": "null"})
            assert expired.status_code == 401
        assert requests == [("GET", "/healthz", b"")]


def test_browser_gateway_process_exits_on_expiry_and_revocation(tmp_path: Path) -> None:
    capability = "w" * 43
    for trigger in ("expiry", "revocation"):
        proxy_port = _free_port()
        revocation_path = tmp_path / f"{trigger}.revoked"
        config = BrowserGatewayConfig(
            attachment_id=f"browser-{trigger}",
            token_sha256=hashlib.sha256(capability.encode("utf-8")).hexdigest(),
            bind_port=proxy_port,
            upstream_protocol="http",
            upstream_port=_free_port(),
            allowed_paths=["/", "/healthz", "/events", "/state", "/commands"],
            command_path="/commands",
            expires_at=(
                datetime.now(UTC)
                + (timedelta(milliseconds=350) if trigger == "expiry" else timedelta(minutes=5))
            ).isoformat(),
            revocation_path=str(revocation_path.resolve()),
        )
        thread = threading.Thread(
            target=serve_browser_gateway,
            args=(config, capability),
            daemon=True,
        )
        thread.start()
        _wait_for_listener(proxy_port)
        if trigger == "revocation":
            revocation_path.write_text("revoked\n", encoding="utf-8")
        thread.join(timeout=3)
        assert not thread.is_alive(), f"browser proxy remained alive after {trigger}"


@contextmanager
def _backend_server() -> Generator[tuple[int, list[tuple[str, str, bytes]]]]:
    requests: list[tuple[str, str, bytes]] = []

    class BackendHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            requests.append(("GET", self.path, b""))
            if self.path.startswith("/events"):
                payload = b"data: ready\n\n"
                content_type = "text/event-stream"
            else:
                payload = json.dumps(
                    {
                        "schema_version": "jarvis.paraview.health.v1",
                        "status": "ready",
                        "service_instance_id": "service-1",
                        "revision": 1,
                    }
                ).encode("utf-8")
                content_type = "application/json"
            self.send_response(200)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def do_POST(self) -> None:  # noqa: N802
            length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(length)
            requests.append(("POST", self.path, body))
            payload = b'{"accepted":true}'
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, format: str, *args: Any) -> None:
            del format, args

    server = ThreadingHTTPServer(("127.0.0.1", 0), BackendHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield int(server.server_address[1]), requests
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


@contextmanager
def _capability_proxy(
    *,
    backend_port: int,
    capability: str,
    revocation_path: Path,
    expires_at: datetime | None = None,
) -> Generator[int]:
    proxy_port = _free_port()
    config = BrowserGatewayConfig(
        attachment_id="browser-test",
        token_sha256=hashlib.sha256(capability.encode("utf-8")).hexdigest(),
        bind_port=proxy_port,
        upstream_protocol="http",
        upstream_port=backend_port,
        allowed_paths=["/", "/healthz", "/events", "/state", "/commands", "/live-data"],
        command_path="/commands",
        expires_at=(expires_at or datetime.now(UTC) + timedelta(minutes=5)).isoformat(),
        revocation_path=str(revocation_path.resolve()),
    )
    server = CapabilityProxyServer(config, capability)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield proxy_port
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def _wait_for_listener(port: int) -> None:
    deadline = datetime.now(UTC) + timedelta(seconds=2)
    while datetime.now(UTC) < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.1):
                return
        except OSError:
            threading.Event().wait(0.02)
    raise AssertionError(f"browser gateway did not listen on port {port}")

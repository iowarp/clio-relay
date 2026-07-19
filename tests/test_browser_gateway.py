from __future__ import annotations

import hashlib
import http.client
import json
import socket
import threading
import time
from collections.abc import Generator
from contextlib import contextmanager, suppress
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from io import BytesIO
from pathlib import Path
from typing import Any, BinaryIO
from urllib.parse import urlencode

import httpx
import pytest

import clio_relay.browser_gateway as browser_gateway_module
from clio_relay.browser_gateway import (
    BrowserGatewayBootstrap,
    BrowserGatewayConfig,
    CapabilityProxyServer,
    read_browser_gateway_bootstrap,
    serve_browser_gateway,
)


def test_browser_gateway_bootstrap_is_bounded_and_duplicate_key_free() -> None:
    capability = "x" * 43
    authorization = f"Bearer {'a' * 64}"
    payload = json.dumps(
        {
            "schema_version": "clio-relay.browser-gateway-bootstrap.v1",
            "capability": capability,
            "upstream_authorization": authorization,
        },
        separators=(",", ":"),
    ).encode("utf-8")

    bootstrap = read_browser_gateway_bootstrap(BytesIO(payload))

    assert bootstrap.capability == capability
    assert bootstrap.upstream_authorization == authorization
    with pytest.raises(ValueError, match="duplicate key: capability"):
        read_browser_gateway_bootstrap(
            BytesIO(
                (
                    '{"schema_version":"clio-relay.browser-gateway-bootstrap.v1",'
                    f'"capability":"{capability}","capability":"{"y" * 43}"}}'
                ).encode()
            )
        )
    with pytest.raises(ValueError, match="byte limit"):
        read_browser_gateway_bootstrap(
            BytesIO(b"{" + b" " * browser_gateway_module.MAX_BROWSER_BOOTSTRAP_BYTES + b"}")
        )


def test_browser_gateway_main_erases_legacy_secret_environment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    capability = "z" * 43
    authorization = f"Bearer {'c' * 64}"
    config_path = tmp_path / "browser.json"
    config_path.write_text("{}", encoding="utf-8")
    monkeypatch.setenv(browser_gateway_module.CAPABILITY_ENV, "legacy-capability")
    monkeypatch.setenv(
        browser_gateway_module.UPSTREAM_AUTHORIZATION_ENV,
        f"Bearer {'d' * 64}",
    )

    def bootstrap_reader(_stream: BinaryIO) -> BrowserGatewayBootstrap:
        return BrowserGatewayBootstrap(
            capability=capability,
            upstream_authorization=authorization,
        )

    monkeypatch.setattr(browser_gateway_module, "read_browser_gateway_bootstrap", bootstrap_reader)
    config = BrowserGatewayConfig(
        attachment_id="environment-erasure",
        token_sha256=hashlib.sha256(capability.encode("utf-8")).hexdigest(),
        bind_port=_free_port(),
        upstream_protocol="http",
        upstream_port=_free_port(),
        allowed_paths=["/healthz", "/commands"],
        command_path="/commands",
        expires_at=(datetime.now(UTC) + timedelta(minutes=5)).isoformat(),
        revocation_path=str((tmp_path / "revoked").resolve()),
    )

    def config_loader(_path: Path) -> BrowserGatewayConfig:
        return config

    monkeypatch.setattr(browser_gateway_module, "load_browser_gateway_config", config_loader)

    def observe_bootstrap(
        _config: BrowserGatewayConfig,
        observed_capability: str,
        observed_authorization: str | None,
    ) -> None:
        assert observed_capability == capability
        assert observed_authorization == authorization
        assert browser_gateway_module.CAPABILITY_ENV not in browser_gateway_module.os.environ
        assert (
            browser_gateway_module.UPSTREAM_AUTHORIZATION_ENV
            not in browser_gateway_module.os.environ
        )

    monkeypatch.setattr(browser_gateway_module, "serve_browser_gateway", observe_bootstrap)

    assert browser_gateway_module.main(["--config", str(config_path)]) == 0


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


def test_browser_gateway_injects_owned_upstream_bearer_without_exposing_it(
    tmp_path: Path,
) -> None:
    """The browser capability never substitutes for cluster-service authorization."""
    token = "a" * 64
    capability = "q" * 43
    with _authorization_backend() as (backend_port, observed):
        proxy_port = _free_port()
        config = BrowserGatewayConfig(
            attachment_id="browser-authenticated-upstream",
            token_sha256=hashlib.sha256(capability.encode("utf-8")).hexdigest(),
            bind_port=proxy_port,
            upstream_protocol="http",
            upstream_port=backend_port,
            allowed_paths=["/healthz", "/commands"],
            command_path="/commands",
            expires_at=(datetime.now(UTC) + timedelta(minutes=5)).isoformat(),
            revocation_path=str((tmp_path / "revoked-auth").resolve()),
        )
        server = CapabilityProxyServer(config, capability, f"Bearer {token}")
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            url = f"http://127.0.0.1:{proxy_port}/healthz?{urlencode({'capability': capability})}"
            response = httpx.get(
                url,
                headers={"Origin": "null", "Authorization": "Bearer " + "b" * 64},
            )
            assert response.status_code == 200
            assert "authorization" not in response.headers
            assert observed == [f"Bearer {token}"]
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)


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


def test_browser_gateway_extends_response_header_timeout_only_for_commands(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Long commands outlive connect timeout while ordinary reads remain bounded."""
    monkeypatch.setattr(browser_gateway_module, "UPSTREAM_CONNECT_TIMEOUT_SECONDS", 0.1)
    monkeypatch.setattr(
        browser_gateway_module,
        "UPSTREAM_COMMAND_RESPONSE_TIMEOUT_SECONDS",
        0.75,
        raising=False,
    )
    with _delayed_response_backend(delay_seconds=0.25) as (backend_port, requests):
        capability = "d" * 43
        with _capability_proxy(
            backend_port=backend_port,
            capability=capability,
            revocation_path=tmp_path / "revoked-delayed-command",
        ) as proxy_port:
            query = urlencode({"capability": capability})
            command = httpx.post(
                f"http://127.0.0.1:{proxy_port}/commands?{query}",
                headers={"Origin": "null", "Content-Type": "application/json"},
                content=b'{"operation":"measure-field"}',
                timeout=2.0,
            )
            assert command.status_code == 200
            assert command.json() == {"accepted": True}

            state = httpx.get(
                f"http://127.0.0.1:{proxy_port}/state?{query}",
                headers={"Origin": "null"},
                timeout=2.0,
            )
            assert state.status_code == 502
            assert state.json() == {"error": "upstream service is unavailable"}

        assert requests == [
            ("POST", "/commands", b'{"operation":"measure-field"}'),
            ("GET", "/state", b""),
        ]


def test_browser_gateway_closes_ended_sse_and_reconnects_to_latest_revision(
    tmp_path: Path,
) -> None:
    with _revision_sse_backend(keep_open_for_command=False) as (backend_port, requests):
        capability = "r" * 43
        with _capability_proxy(
            backend_port=backend_port,
            capability=capability,
            revocation_path=tmp_path / "revoked",
        ) as proxy_port:
            query = urlencode({"capability": capability})
            events_url = f"http://127.0.0.1:{proxy_port}/events?{query}"
            command_url = f"http://127.0.0.1:{proxy_port}/commands?{query}"
            with httpx.Client(timeout=2.0) as client:
                with client.stream("GET", events_url, headers={"Origin": "null"}) as first:
                    assert first.headers["connection"] == "close"
                    assert b"".join(first.iter_bytes()) == b'data: {"revision":1}\n\n'

                command = client.post(
                    command_url,
                    headers={"Origin": "null", "Content-Type": "application/json"},
                    content=b'{"operation":"next-timestep"}',
                )
                assert command.status_code == 200

                with client.stream("GET", events_url, headers={"Origin": "null"}) as latest:
                    assert latest.headers["connection"] == "close"
                    assert b"".join(latest.iter_bytes()) == b'data: {"revision":2}\n\n'

            assert requests == [
                ("GET", "/events", b""),
                ("POST", "/commands", b'{"operation":"next-timestep"}'),
                ("GET", "/events", b""),
            ]


def test_browser_gateway_streams_post_command_revision_before_sse_closes(
    tmp_path: Path,
) -> None:
    with _revision_sse_backend(keep_open_for_command=True) as (backend_port, requests):
        capability = "s" * 43
        with _capability_proxy(
            backend_port=backend_port,
            capability=capability,
            revocation_path=tmp_path / "revoked",
        ) as proxy_port:
            query = urlencode({"capability": capability})
            events_url = f"http://127.0.0.1:{proxy_port}/events?{query}"
            command_url = f"http://127.0.0.1:{proxy_port}/commands?{query}"
            with (
                httpx.Client(timeout=3.0) as stream_client,
                stream_client.stream("GET", events_url, headers={"Origin": "null"}) as response,
            ):
                lines = response.iter_lines()
                assert response.headers["connection"] == "close"
                assert next(line for line in lines if line.startswith("data:")) == (
                    'data: {"revision":1}'
                )
                command = httpx.post(
                    command_url,
                    headers={"Origin": "null", "Content-Type": "application/json"},
                    content=b'{"operation":"next-timestep"}',
                    timeout=2.0,
                )
                assert command.status_code == 200
                assert next(line for line in lines if line.startswith("data:")) == (
                    'data: {"revision":2}'
                )
                assert all(not line for line in lines)

            assert requests == [
                ("GET", "/events", b""),
                ("POST", "/commands", b'{"operation":"next-timestep"}'),
            ]


def test_browser_gateway_bounds_long_lived_requests_and_recovers_slots(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One retained SSE consumes one slot, excess receives 503, and closure recovers it."""
    monkeypatch.setattr(browser_gateway_module, "MAX_ACTIVE_BROWSER_REQUESTS", 1)
    with _revision_sse_backend(keep_open_for_command=True) as (backend_port, _requests):
        capability = "b" * 43
        with _capability_proxy(
            backend_port=backend_port,
            capability=capability,
            revocation_path=tmp_path / "revoked-bounded",
        ) as proxy_port:
            query = urlencode({"capability": capability})
            events_url = f"http://127.0.0.1:{proxy_port}/events?{query}"
            health_url = f"http://127.0.0.1:{proxy_port}/healthz?{query}"
            with (
                httpx.Client(timeout=4.0) as stream_client,
                stream_client.stream(
                    "GET",
                    events_url,
                    headers={"Origin": "null"},
                ) as response,
            ):
                lines = response.iter_lines()
                assert next(line for line in lines if line.startswith("data:")) == (
                    'data: {"revision":1}'
                )
                for _attempt in range(5):
                    overloaded = httpx.get(
                        health_url,
                        headers={"Origin": "null"},
                        timeout=2.0,
                    )
                    assert overloaded.status_code == 503
                    assert overloaded.headers["connection"] == "close"
                    assert overloaded.json() == {
                        "error": "browser attachment request capacity exhausted"
                    }
                released = httpx.post(
                    f"http://127.0.0.1:{backend_port}/commands",
                    headers={"Content-Type": "application/json"},
                    content=b'{"operation":"release-capacity-test"}',
                    timeout=2.0,
                )
                assert released.status_code == 200
                assert next(line for line in lines if line.startswith("data:")) == (
                    'data: {"revision":2}'
                )

            deadline = datetime.now(UTC) + timedelta(seconds=4)
            recovered: httpx.Response | None = None
            while datetime.now(UTC) < deadline:
                try:
                    candidate = httpx.get(
                        health_url,
                        headers={"Origin": "null"},
                        timeout=2.0,
                    )
                except httpx.TransportError:
                    threading.Event().wait(0.05)
                    continue
                if candidate.status_code == 200:
                    recovered = candidate
                    break
                assert candidate.status_code == 503
                threading.Event().wait(0.05)
            assert recovered is not None


def test_browser_gateway_reclaims_idle_pre_auth_connection_slot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An unauthenticated socket cannot retain the sole handler slot indefinitely."""
    monkeypatch.setattr(browser_gateway_module, "MAX_ACTIVE_BROWSER_REQUESTS", 1)
    monkeypatch.setattr(browser_gateway_module, "BROWSER_CLIENT_IO_TIMEOUT_SECONDS", 1.0)
    with _backend_server() as (backend_port, _requests):
        capability = "i" * 43
        with _capability_proxy_server(
            backend_port=backend_port,
            capability=capability,
            revocation_path=tmp_path / "revoked-idle",
        ) as server:
            proxy_port = int(server.server_address[1])
            idle = socket.create_connection(("127.0.0.1", proxy_port), timeout=1.0)
            try:
                assert server.wait_for_active_request_count(1, timeout=1.0)
                url = (
                    f"http://127.0.0.1:{proxy_port}/healthz?{urlencode({'capability': capability})}"
                )
                overloaded = httpx.get(url, headers={"Origin": "null"}, timeout=2.0)
                assert overloaded.status_code == 503
                assert overloaded.json() == {
                    "error": "browser attachment request capacity exhausted"
                }
                assert server.wait_for_active_request_count(0, timeout=2.0)
                recovered = httpx.get(url, headers={"Origin": "null"}, timeout=2.0)
                assert recovered.status_code == 200
            finally:
                idle.close()


@pytest.mark.parametrize("slow_input", ["headers", "body"])
def test_browser_gateway_reclaims_trickled_request_at_absolute_deadline(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    slow_input: str,
) -> None:
    """Header and authenticated-body trickles cannot reset the sole slot's deadline."""
    monkeypatch.setattr(browser_gateway_module, "MAX_ACTIVE_BROWSER_REQUESTS", 1)
    monkeypatch.setattr(browser_gateway_module, "BROWSER_CLIENT_IO_TIMEOUT_SECONDS", 0.5)
    monkeypatch.setattr(
        browser_gateway_module,
        "BROWSER_CLIENT_REQUEST_DEADLINE_SECONDS",
        0.2,
    )
    with _backend_server() as (backend_port, _requests):
        capability = "d" * 43
        with _capability_proxy_server(
            backend_port=backend_port,
            capability=capability,
            revocation_path=tmp_path / f"revoked-trickle-{slow_input}",
        ) as server:
            proxy_port = int(server.server_address[1])
            query = urlencode({"capability": capability})
            slow = socket.create_connection(("127.0.0.1", proxy_port), timeout=1.0)
            trickle_stop = threading.Event()
            if slow_input == "headers":
                slow.sendall(b"GET /healthz?")
            else:
                slow.sendall(
                    (
                        f"POST /commands?{query} HTTP/1.1\r\n"
                        "Host: 127.0.0.1\r\n"
                        "Origin: null\r\n"
                        "Content-Type: application/json\r\n"
                        "Content-Length: 1024\r\n\r\n"
                    ).encode("ascii")
                    + b"{"
                )

            def trickle() -> None:
                while not trickle_stop.wait(0.03):
                    try:
                        slow.sendall(b"x")
                    except OSError:
                        return

            trickle_thread = threading.Thread(target=trickle, daemon=True)
            trickle_thread.start()
            started_at = time.monotonic()
            try:
                assert server.wait_for_active_request_count(1, timeout=1.0)
                health_url = f"http://127.0.0.1:{proxy_port}/healthz?{query}"
                assert server.wait_for_active_request_count(0, timeout=1.0)
                elapsed = time.monotonic() - started_at
                assert 0.1 <= elapsed < 0.8
                recovered = httpx.get(
                    health_url,
                    headers={"Origin": "null"},
                    timeout=2.0,
                )
                assert recovered.status_code == 200
            finally:
                trickle_stop.set()
                slow.close()
                trickle_thread.join(timeout=1.0)


def test_browser_gateway_timeout_logging_is_safe_before_request_parsing(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A pre-request socket timeout has no command attribute or unsafe request text."""
    handler = object.__new__(browser_gateway_module.CapabilityProxyHandler)

    handler.log_message("Request timed out: %r", TimeoutError("timed out"))
    handler.command = "GET"
    handler.log_message('"%s" %s %s', "GET /secret HTTP/1.1", "200", "12")

    assert capsys.readouterr().err == (
        "browser-gateway method=unparsed status=unknown\nbrowser-gateway method=GET status=200\n"
    )


def test_browser_gateway_keeps_finite_content_length_responses_alive(tmp_path: Path) -> None:
    with _backend_server() as (backend_port, requests):
        capability = "k" * 43
        with _capability_proxy(
            backend_port=backend_port,
            capability=capability,
            revocation_path=tmp_path / "revoked",
        ) as proxy_port:
            query = urlencode({"capability": capability})
            connection = http.client.HTTPConnection("127.0.0.1", proxy_port, timeout=2.0)
            try:
                connection.request("GET", f"/healthz?{query}", headers={"Origin": "null"})
                first = connection.getresponse()
                assert first.getheader("Connection") is None
                assert json.loads(first.read()) == {
                    "schema_version": "jarvis.paraview.health.v1",
                    "status": "ready",
                    "service_instance_id": "service-1",
                    "revision": 1,
                }
                downstream_socket = connection.sock
                assert downstream_socket is not None

                connection.request("GET", f"/state?{query}", headers={"Origin": "null"})
                second = connection.getresponse()
                assert second.getheader("Connection") is None
                second.read()
                assert connection.sock is downstream_socket
            finally:
                connection.close()

            assert requests == [
                ("GET", "/healthz", b""),
                ("GET", "/state", b""),
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
def _delayed_response_backend(
    *, delay_seconds: float
) -> Generator[tuple[int, list[tuple[str, str, bytes]]]]:
    requests: list[tuple[str, str, bytes]] = []

    class BackendHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            requests.append(("GET", self.path, b""))
            self._delayed_response(b'{"revision":1}')

        def do_POST(self) -> None:  # noqa: N802
            length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(length)
            requests.append(("POST", self.path, body))
            self._delayed_response(b'{"accepted":true}')

        def _delayed_response(self, payload: bytes) -> None:
            threading.Event().wait(delay_seconds)
            with suppress(BrokenPipeError, ConnectionResetError, OSError):
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
def _revision_sse_backend(
    *, keep_open_for_command: bool
) -> Generator[tuple[int, list[tuple[str, str, bytes]]]]:
    requests: list[tuple[str, str, bytes]] = []
    condition = threading.Condition()
    revision = 1

    class BackendHandler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_GET(self) -> None:  # noqa: N802
            nonlocal revision
            requests.append(("GET", self.path, b""))
            if not self.path.startswith("/events"):
                payload = json.dumps(
                    {
                        "schema_version": "jarvis.paraview.health.v1",
                        "status": "ready",
                        "service_instance_id": "service-1",
                        "revision": revision,
                    }
                ).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
                return
            with condition:
                initial_revision = revision
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream; charset=utf-8")
            if not keep_open_for_command:
                self.send_header("Connection", "close")
                self.close_connection = True
            self.end_headers()
            self.wfile.write(f'data: {{"revision":{initial_revision}}}\n\n'.encode())
            self.wfile.flush()
            if not keep_open_for_command:
                return
            with condition:
                changed = condition.wait_for(lambda: revision > initial_revision, timeout=10.0)
                latest_revision = revision
            if changed:
                self.wfile.write(f'data: {{"revision":{latest_revision}}}\n\n'.encode())
                self.wfile.flush()
            self.close_connection = True

        def do_POST(self) -> None:  # noqa: N802
            nonlocal revision
            length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(length)
            requests.append(("POST", self.path, body))
            with condition:
                revision += 1
                current_revision = revision
                condition.notify_all()
            payload = json.dumps({"accepted": True, "revision": current_revision}).encode()
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
def _authorization_backend() -> Generator[tuple[int, list[str | None]]]:
    observed: list[str | None] = []

    class BackendHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            observed.append(self.headers.get("Authorization"))
            payload = b'{"status":"ready"}'
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
        yield int(server.server_address[1]), observed
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
    with _capability_proxy_server(
        backend_port=backend_port,
        capability=capability,
        revocation_path=revocation_path,
        expires_at=expires_at,
    ) as server:
        yield int(server.server_address[1])


@contextmanager
def _capability_proxy_server(
    *,
    backend_port: int,
    capability: str,
    revocation_path: Path,
    expires_at: datetime | None = None,
) -> Generator[CapabilityProxyServer]:
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
        yield server
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

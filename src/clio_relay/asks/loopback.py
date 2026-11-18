"""Relay's page on loopback, for relay over stdio.

Binds ``127.0.0.1`` on a free port (or ``CLIO_RELAY_PAGE_PORT``), so only the
person's own machine can reach it. Nothing is logged.
"""

from __future__ import annotations

import threading
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import TYPE_CHECKING, cast
from urllib.parse import parse_qs, unquote, urlsplit

from clio_relay.asks.pages import PageResponse, response

if TYPE_CHECKING:
    from clio_relay.asks.provider import RelayAskProvider

MAX_BODY = 32 * 1024
PREFIX = "/relay/asks/"
CALLBACK = "/relay/asks/callback"


def first_values(raw: str) -> dict[str, str]:
    """Parse a query or form body, keeping the first value of each field."""
    return {key: values[0] for key, values in parse_qs(raw, keep_blank_values=True).items()}


class LoopbackServer:
    """Serves relay's page on loopback, once started."""

    def __init__(self, owner: RelayAskProvider, host: str = "127.0.0.1", port: int = 0) -> None:
        self.owner = owner
        self.host = host
        self.wanted_port = port
        self._server: _Server | None = None
        self._lock = threading.Lock()

    def start(self) -> int:
        """Start serving, once, and return the port."""
        with self._lock:
            if self._server is None:
                server = _Server((self.host, self.wanted_port), _Handler, owner=self.owner)
                threading.Thread(
                    target=server.serve_forever, name="relay-page", daemon=True
                ).start()
                self._server = server
            return int(self._server.server_address[1])

    @property
    def base(self) -> str:
        """Return the page's base address."""
        return f"http://{self.host}:{self.start()}"

    def close(self) -> None:
        """Stop serving."""
        with self._lock:
            server, self._server = self._server, None
        if server is not None:
            server.shutdown()
            server.server_close()


class _Server(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(
        self,
        address: tuple[str, int],
        handler: type[BaseHTTPRequestHandler],
        *,
        owner: RelayAskProvider,
    ) -> None:
        self.owner = owner
        super().__init__(address, handler)


class _Handler(BaseHTTPRequestHandler):
    server_version = "clio-relay"
    sys_version = ""

    @property
    def owner(self) -> RelayAskProvider:
        return cast(_Server, self.server).owner

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002
        return

    def _send(self, page: PageResponse) -> None:
        data = page.body.encode("utf-8")
        self.send_response(page.status)
        for key, value in page.headers.items():
            self.send_header(key, value)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _origins(self) -> set[str]:
        port = cast(_Server, self.server).server_address[1]
        return {f"http://127.0.0.1:{port}", f"http://localhost:{port}"}

    def _headers(self) -> dict[str, str]:
        return {key.lower(): value for key, value in self.headers.items()}

    def _ask_id(self, path: str) -> str | None:
        if not path.startswith(PREFIX):
            return None
        rest = unquote(path[len(PREFIX) :])
        return rest if rest and "/" not in rest else None

    def do_GET(self) -> None:  # noqa: N802
        parts = urlsplit(self.path)
        query = first_values(parts.query)
        runner = self.owner.runner
        if parts.path == CALLBACK:
            self._send(runner.submit(self.owner.core.callback(query)).result())
            return
        ask_id = self._ask_id(parts.path)
        if ask_id is None:
            self._send(response(404, "Not found", "<p>Nothing is here.</p>"))
            return
        self._send(runner.submit(self.owner.core.get(ask_id, query.get("t", ""))).result())

    def do_POST(self) -> None:  # noqa: N802
        parts = urlsplit(self.path)
        ask_id = self._ask_id(parts.path)
        try:
            length = int(self.headers.get("Content-Length") or 0)
        except ValueError:
            length = -1
        if ask_id is None or length < 0 or length > MAX_BODY:
            self._send(response(int(HTTPStatus.BAD_REQUEST), "Bad request", "<p>Bad request.</p>"))
            return
        fields = first_values(self.rfile.read(length).decode("utf-8", errors="replace"))
        page = self.owner.core.post(ask_id, fields, self._headers(), self._origins())
        self._send(self.owner.runner.submit(page).result())

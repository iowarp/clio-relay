"""Relay's own single-use pages on loopback, for the stub ask provider.

A ``GET`` shows the ask and changes nothing. A ``POST`` with the page's token
and its form token answers it once. The server binds ``127.0.0.1`` on a free
port, so only the person's own machine can reach it.
"""

from __future__ import annotations

import html
import secrets
import threading
from dataclasses import dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import cast
from urllib.parse import parse_qs, urlsplit

from clio_relay.adapters.hooks import Ask, schema_options
from clio_relay.jobs.handles import utc_now
from clio_relay.jobs.records import RecordStore

_MAX_BODY = 16 * 1024


@dataclass
class _PageTokens:
    page: str
    form: str


class LoopbackPages:
    """A small page server for asks. Start it once; it serves every ask."""

    def __init__(self, asks: RecordStore[Ask], host: str = "127.0.0.1") -> None:
        self.asks = asks
        self.host = host
        self._tokens: dict[str, _PageTokens] = {}
        self._lock = threading.Lock()
        self._server: _PagesServer | None = None
        self._thread: threading.Thread | None = None

    @property
    def port(self) -> int:
        """Return the bound port, starting the server if needed."""
        return self.start().server_address[1]

    def start(self) -> _PagesServer:
        """Start serving, once."""
        with self._lock:
            if self._server is None:
                self._server = _PagesServer((self.host, 0), _Handler, owner=self)
                self._server.daemon_threads = True
                self._thread = threading.Thread(
                    target=self._server.serve_forever, name="relay-pages", daemon=True
                )
                self._thread.start()
            return self._server

    def address(self, ask: Ask) -> str:
        """Return a fresh single-use address for ``ask``."""
        server = self.start()
        tokens = _PageTokens(page=secrets.token_urlsafe(24), form=secrets.token_urlsafe(24))
        with self._lock:
            self._tokens[ask.ask_id] = tokens
        port = server.server_address[1]
        return f"http://{self.host}:{port}/asks/{ask.ask_id}?t={tokens.page}"

    def check(self, ask_id: str, page: str, form: str | None) -> _PageTokens | None:
        with self._lock:
            tokens = self._tokens.get(ask_id)
        if tokens is None or not secrets.compare_digest(tokens.page, page):
            return None
        if form is not None and not secrets.compare_digest(tokens.form, form):
            return None
        return tokens

    def spend(self, ask_id: str) -> None:
        with self._lock:
            self._tokens.pop(ask_id, None)

    def answer(self, ask_id: str, action: str, value: str | None) -> Ask | None:
        """Record an answer from the page. Returns the ask, or ``None`` if gone."""
        with self.asks.locked(ask_id):
            ask = self.asks.get(ask_id)
            if ask is None:
                return None
            if ask.state == "pending":
                if action == "accept":
                    answer: dict[str, object] = {"action": "accept"}
                    if ask.kind == "choose" and value is not None:
                        answer["content"] = {_field(ask): value}
                    if ask.kind == "approve":
                        answer["content"] = {"approve": True}
                    ask.resolve("accepted", answer, by="page")
                else:
                    ask.resolve("declined", {"action": "decline"}, by="page")
            if ask.kind == "url" and action == "accept" and ask.completed_at is None:
                ask.completed_at = utc_now()
            self.asks.put(ask_id, ask)
            return ask

    def close(self) -> None:
        """Stop serving."""
        with self._lock:
            server, self._server = self._server, None
            self._tokens.clear()
        if server is not None:
            server.shutdown()
            server.server_close()


class _PagesServer(ThreadingHTTPServer):
    def __init__(
        self,
        address: tuple[str, int],
        handler: type[BaseHTTPRequestHandler],
        *,
        owner: LoopbackPages,
    ) -> None:
        self.owner = owner
        super().__init__(address, handler)


class _Handler(BaseHTTPRequestHandler):
    server_version = "clio-relay-pages"

    @property
    def owner(self) -> LoopbackPages:
        return cast("_PagesServer", self.server).owner

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002
        return

    def _send(self, status: HTTPStatus, body: str) -> None:
        data = body.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Frame-Options", "DENY")
        self.send_header("Content-Security-Policy", "default-src 'none'; form-action 'self'")
        self.send_header("Referrer-Policy", "no-referrer")
        self.end_headers()
        self.wfile.write(data)

    def _ask_id(self) -> str | None:
        parts = urlsplit(self.path).path.strip("/").split("/")
        if len(parts) == 2 and parts[0] == "asks":
            return parts[1]
        return None

    def do_GET(self) -> None:  # noqa: N802
        ask_id = self._ask_id()
        query = parse_qs(urlsplit(self.path).query)
        page = (query.get("t") or [""])[0]
        tokens = self.owner.check(ask_id, page, None) if ask_id else None
        ask = self.owner.asks.get(ask_id) if ask_id and tokens else None
        if ask is None or tokens is None:
            self._send(HTTPStatus.NOT_FOUND, _page("Not found", "<p>This page has expired.</p>"))
            return
        self._send(HTTPStatus.OK, _form(ask, page, tokens.form))

    def do_POST(self) -> None:  # noqa: N802
        ask_id = self._ask_id()
        length = int(self.headers.get("Content-Length") or 0)
        if ask_id is None or length > _MAX_BODY:
            self._send(HTTPStatus.BAD_REQUEST, _page("Bad request", "<p>Bad request.</p>"))
            return
        fields = parse_qs(self.rfile.read(length).decode("utf-8", errors="replace"))
        page = (fields.get("t") or [""])[0]
        form = (fields.get("csrf") or [""])[0]
        if self.owner.check(ask_id, page, form) is None:
            self._send(HTTPStatus.FORBIDDEN, _page("Expired", "<p>This page has expired.</p>"))
            return
        action = (fields.get("action") or ["decline"])[0]
        value = (fields.get("value") or [None])[0]
        ask = self.owner.answer(ask_id, action, value)
        self.owner.spend(ask_id)
        if ask is None:
            self._send(HTTPStatus.NOT_FOUND, _page("Not found", "<p>This ask is gone.</p>"))
            return
        done = "Done. You can close this page." if action == "accept" else "Declined."
        self._send(HTTPStatus.OK, _page("Thanks", f"<p>{html.escape(done)}</p>"))


def _page(title: str, body: str) -> str:
    return (
        "<!doctype html><html><head><meta charset='utf-8'>"
        f"<title>{html.escape(title)}</title></head><body>"
        f"<h1>{html.escape(title)}</h1>{body}</body></html>"
    )


def _form(ask: Ask, page: str, form: str) -> str:
    hidden = (
        f"<input type='hidden' name='t' value='{html.escape(page)}'>"
        f"<input type='hidden' name='csrf' value='{html.escape(form)}'>"
    )
    body = f"<p>{html.escape(ask.message)}</p>"
    if ask.state != "pending" and not (ask.kind == "url" and ask.completed_at is None):
        return _page("Already answered", body + f"<p>Answer: {html.escape(ask.state)}</p>")
    choice = ""
    if ask.kind == "choose":
        typed = schema_options(ask.requested_schema)
        picked = next((item.value for item in typed if item.recommended), None)
        choice = "".join(
            f"<label><input type='radio' name='value' value='{html.escape(item.value)}'"
            f"{' checked' if (item.value == picked if picked else index == 0) else ''}> "
            f"{html.escape(item.label)}{' (recommended)' if item.recommended else ''}"
            + (f"<br><small>{html.escape(item.description)}</small>" if item.description else "")
            + "</label><br>"
            for index, item in enumerate(typed)
        )
    if ask.binding:
        rows = "".join(
            f"<tr><th>{html.escape(str(key))}</th><td>{html.escape(str(value))}</td></tr>"
            for key, value in sorted(ask.binding.items())
        )
        body += f"<table>{rows}</table>"
    accept = "Finish sign-in" if ask.kind == "url" else "Approve"
    buttons = (
        f"<button name='action' value='accept'>{accept}</button> "
        "<button name='action' value='decline'>Decline</button>"
    )
    return _page(
        "Relay needs you",
        body + f"<form method='post'>{hidden}{choice}{buttons}</form>",
    )


def _field(ask: Ask) -> str:
    properties = (ask.requested_schema or {}).get("properties")
    if isinstance(properties, dict) and properties:
        return str(next(iter(properties)))  # pyright: ignore[reportUnknownArgumentType]
    return "value"

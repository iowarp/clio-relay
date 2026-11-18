"""``SSH_ASKPASS`` for relay: ssh's prompts go to the person, never to the agent (RL28).

An adapter that runs ``ssh`` or ``scp`` asks :func:`environment` for the
variables to run it with, sets ``BatchMode=no`` and closes stdin. ssh then runs
``clio-relay-askpass`` once per prompt with the prompt as ``argv[1]``. The helper
hands the prompt to the relay process that issued the variables, over a loopback
socket and a per-session token, and waits for the person's answer on relay's
page. It prints the answer, or exits 1 if the person declines.

``SSH_ASKPASS_PROMPT=confirm`` (a yes or no question) prints ``yes`` when the
person agrees. ``SSH_ASKPASS_PROMPT=none`` (a notice, such as "touch your
key") is shown until ssh ends the helper.

Answers stay in this process's memory and are never written down or logged.
Nothing here limits how long the person takes; a prompt whose ssh has gone away
is dropped.
"""

from __future__ import annotations

import contextlib
import json
import logging
import os
import secrets
import select
import shutil
import socket
import socketserver
import sys
import threading
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal, cast

logger = logging.getLogger(__name__)

ENV_ADDR = "CLIO_RELAY_ASKPASS_ADDR"
ENV_TOKEN = "CLIO_RELAY_ASKPASS_TOKEN"
ENV_SITE = "CLIO_RELAY_ASKPASS_SITE"
ENV_OPERATION = "CLIO_RELAY_ASKPASS_OPERATION"
PROGRAM = "clio-relay-askpass"
MAX_LINE = 64 * 1024
GONE_CHECK_S = 1.0

PromptKind = Literal["secret", "text", "confirm", "notice"]
Starter = Callable[[dict[str, str]], Awaitable[None]]


def classify(prompt: str) -> PromptKind:
    """Guess whether a prompt wants a secret or a visible answer."""
    lowered = prompt.lower()
    if "yes/no" in lowered or "(yes/no" in lowered:
        return "text"
    return "secret"


@dataclass
class Prompt:
    """One question ssh is waiting on."""

    prompt_id: str
    text: str
    kind: PromptKind
    created: float = field(default_factory=time.time)
    answered: threading.Event = field(default_factory=threading.Event, repr=False)
    answer: str | None = field(default=None, repr=False)
    declined: bool = False


class AskpassSession:
    """The prompts of one site's ssh calls, tied to one token."""

    def __init__(self, site: str, operation_id: str | None, title: str | None) -> None:
        self.token = secrets.token_urlsafe(24)
        self.site = site
        self.operation_id = operation_id
        self.title = title
        self.ask_id: str | None = None
        self.answered_count = 0
        self.closed = False
        self._prompts: dict[str, Prompt] = {}
        self._lock = threading.Lock()
        self.changed = threading.Condition(self._lock)

    def open_prompts(self) -> list[Prompt]:
        """Return the prompts still waiting, oldest first."""
        with self._lock:
            return sorted(self._prompts.values(), key=lambda item: item.created)

    def _add(self, prompt: Prompt) -> None:
        with self._lock:
            self._prompts[prompt.prompt_id] = prompt
            self.changed.notify_all()

    def _drop(self, prompt_id: str) -> None:
        with self._lock:
            self._prompts.pop(prompt_id, None)
            self.changed.notify_all()

    def answer(self, prompt_id: str, value: str) -> bool:
        """Answer one prompt. Returns ``False`` if it is no longer waiting."""
        with self._lock:
            prompt = self._prompts.pop(prompt_id, None)
            if prompt is None:
                return False
            prompt.answer = value
            self.answered_count += 1
            prompt.answered.set()
            self.changed.notify_all()
            return True

    def decline(self, prompt_id: str) -> bool:
        """Refuse one prompt; ssh sees the helper fail."""
        with self._lock:
            prompt = self._prompts.pop(prompt_id, None)
            if prompt is None:
                return False
            prompt.declined = True
            prompt.answered.set()
            self.changed.notify_all()
            return True

    def close(self) -> None:
        """Refuse everything still waiting and accept no more prompts."""
        with self._lock:
            self.closed = True
            prompts = list(self._prompts.values())
            self._prompts.clear()
            for prompt in prompts:
                prompt.declined = True
                prompt.answered.set()
            self.changed.notify_all()


Listener = Callable[[AskpassSession, Prompt], None]


def _gone(conn: socket.socket) -> bool:
    """Return whether the helper on the other end has closed its socket."""
    try:
        readable, _, _ = select.select([conn], [], [], 0)
        if not readable:
            return False
        return conn.recv(1, socket.MSG_PEEK) == b""
    except OSError:
        return True


def _read_line(conn: socket.socket) -> bytes:
    data = b""
    while b"\n" not in data:
        chunk = conn.recv(4096)
        if not chunk:
            break
        data += chunk
        if len(data) > MAX_LINE:
            raise ValueError("askpass request too long")
    return data.split(b"\n", 1)[0]


class _Handler(socketserver.BaseRequestHandler):
    def handle(self) -> None:
        broker = cast("_Server", self.server).broker
        conn = cast(socket.socket, self.request)
        try:
            broker.serve_one(conn)
        except (OSError, ValueError) as exc:
            logger.debug("askpass connection ended: %s", type(exc).__name__)


class _Server(socketserver.ThreadingTCPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, broker: Broker) -> None:
        self.broker = broker
        super().__init__(("127.0.0.1", 0), _Handler)


class Broker:
    """Hands ssh's prompts to whoever listens, and the answers back to ssh."""

    def __init__(self) -> None:
        self._sessions: dict[str, AskpassSession] = {}
        self._by_key: dict[tuple[str, str | None], str] = {}
        self._listeners: list[Listener] = []
        self._lock = threading.Lock()
        self._server: _Server | None = None

    def start(self) -> int:
        """Start listening, once, and return the port."""
        with self._lock:
            if self._server is None:
                self._server = _Server(self)
                threading.Thread(
                    target=self._server.serve_forever, name="relay-askpass", daemon=True
                ).start()
            return int(self._server.server_address[1])

    def stop(self) -> None:
        """Stop listening and refuse every open prompt."""
        with self._lock:
            server, self._server = self._server, None
            sessions = list(self._sessions.values())
            self._sessions.clear()
            self._by_key.clear()
        for session in sessions:
            session.close()
        if server is not None:
            server.shutdown()
            server.server_close()

    def session_for(
        self, site: str, operation_id: str | None = None, title: str | None = None
    ) -> AskpassSession:
        """Return the open session for a site and operation, making one if needed."""
        key = (site, operation_id)
        with self._lock:
            token = self._by_key.get(key)
            session = self._sessions.get(token) if token else None
            if session is None or session.closed:
                session = AskpassSession(site, operation_id, title)
                self._sessions[session.token] = session
                self._by_key[key] = session.token
            return session

    def new_session(
        self, site: str, operation_id: str | None = None, title: str | None = None
    ) -> AskpassSession:
        """Return a fresh session that no other caller shares."""
        session = AskpassSession(site, operation_id, title)
        with self._lock:
            self._sessions[session.token] = session
        return session

    def session(self, token: str) -> AskpassSession | None:
        """Return a session by its token."""
        with self._lock:
            return self._sessions.get(token)

    def sessions(self) -> list[AskpassSession]:
        """Return every open session."""
        with self._lock:
            return [item for item in self._sessions.values() if not item.closed]

    def forget(self, session: AskpassSession) -> None:
        """Close a session and drop it."""
        session.close()
        with self._lock:
            self._sessions.pop(session.token, None)
            for key, token in list(self._by_key.items()):
                if token == session.token:
                    del self._by_key[key]

    def add_listener(self, listener: Listener) -> None:
        """Be told about every new prompt."""
        with self._lock:
            self._listeners.append(listener)

    def remove_listener(self, listener: Listener) -> None:
        """Stop being told."""
        with self._lock:
            if listener in self._listeners:
                self._listeners.remove(listener)

    def _reply(self, conn: socket.socket, payload: dict[str, Any]) -> None:
        conn.sendall(json.dumps(payload).encode("utf-8") + b"\n")

    def serve_one(self, conn: socket.socket) -> None:
        """Serve one helper: read its prompt, wait for the person, answer."""
        line = _read_line(conn)
        request: object = json.loads(line or b"{}")
        if not isinstance(request, dict):
            raise ValueError("askpass request is not an object")
        values = cast(dict[str, Any], request)
        session = self.session(str(values.get("token", "")))
        if session is None or session.closed:
            self._reply(conn, {"decline": True, "message": "relay does not know this ssh call"})
            return
        kind_value = values.get("kind")
        text = str(values.get("prompt", ""))
        kind: PromptKind = (
            cast(PromptKind, kind_value)
            if kind_value in ("secret", "text", "confirm", "notice")
            else classify(text)
        )
        prompt = Prompt(prompt_id=secrets.token_hex(8), text=text, kind=kind)
        with self._lock:
            listeners = list(self._listeners)
        if not listeners:
            self._reply(
                conn,
                {"decline": True, "message": "relay has no page to ask the person on"},
            )
            return
        session._add(prompt)  # pyright: ignore[reportPrivateUsage]
        for listener in listeners:
            try:
                listener(session, prompt)
            except Exception:
                logger.exception("an askpass listener failed")
        try:
            if kind == "notice":
                while not _gone(conn) and not prompt.answered.wait(GONE_CHECK_S):
                    pass
                return
            while not prompt.answered.wait(GONE_CHECK_S):
                if _gone(conn):
                    return
            if prompt.declined:
                self._reply(conn, {"decline": True})
            else:
                self._reply(conn, {"answer": prompt.answer or ""})
        finally:
            session._drop(prompt.prompt_id)  # pyright: ignore[reportPrivateUsage]


_broker = Broker()
_starters: dict[str, Starter] = {}
_starters_lock = threading.Lock()


def broker() -> Broker:
    """Return this process's broker."""
    return _broker


def _state_dir(state_dir: Path | None) -> Path:
    if state_dir is not None:
        return state_dir
    from clio_relay.surface.settings import RelayPaths

    return RelayPaths.from_env().state_dir


def program(state_dir: Path | None = None) -> str:
    """Return the helper to set as ``SSH_ASKPASS``.

    The installed ``clio-relay-askpass`` next to this Python, then on ``PATH``,
    then a small wrapper written under ``<state>/askpass/``.
    """
    names = [f"{PROGRAM}.exe", PROGRAM] if sys.platform == "win32" else [PROGRAM]
    for name in names:
        beside = Path(sys.executable).parent / name
        if beside.is_file():
            return str(beside)
    found = shutil.which(PROGRAM)
    if found:
        return found
    folder = _state_dir(state_dir) / "askpass"
    folder.mkdir(parents=True, exist_ok=True)
    if sys.platform == "win32":
        wrapper = folder / f"{PROGRAM}.cmd"
        text = f'@"{sys.executable}" -m clio_relay.asks.askpass %*\r\n'
    else:
        wrapper = folder / PROGRAM
        text = f'#!/bin/sh\nexec "{sys.executable}" -m clio_relay.asks.askpass "$@"\n'
    if not wrapper.is_file() or wrapper.read_text(encoding="utf-8") != text:
        wrapper.write_text(text, encoding="utf-8")
        wrapper.chmod(0o755)
    return str(wrapper)


def environment_for(session: AskpassSession, state_dir: Path | None = None) -> dict[str, str]:
    """Return the variables that send one session's ssh prompts to relay."""
    port = _broker.start()
    env = {
        "SSH_ASKPASS": program(state_dir),
        "SSH_ASKPASS_REQUIRE": "force",
        "DISPLAY": os.environ.get("DISPLAY") or ":0",
        ENV_ADDR: f"127.0.0.1:{port}",
        ENV_TOKEN: session.token,
        ENV_SITE: session.site,
    }
    if session.operation_id:
        env[ENV_OPERATION] = session.operation_id
    return env


def environment(
    *,
    site: str,
    operation_id: str | None = None,
    title: str | None = None,
    state_dir: Path | None = None,
) -> dict[str, str]:
    """Return the variables to run ``ssh`` or ``scp`` with, for one site.

    Set them on the child, with ``BatchMode=no`` and stdin closed. Calls for the
    same site and operation share a session, so their prompts show together.
    """
    return environment_for(_broker.session_for(site, operation_id, title), state_dir)


def register_sign_in(site_id: str, starter: Starter | None) -> None:
    """Register how to sign in to a site (``ssh -MNf`` for the ssh adapter).

    The page's "Start sign-in" runs ``starter(env)`` with :func:`environment`
    variables for that ask, and shows each prompt. The starter returns once the
    sign-in holds, or raises a ``Refusal`` saying why it failed. ``None``
    removes it.
    """
    with _starters_lock:
        if starter is None:
            _starters.pop(site_id, None)
        else:
            _starters[site_id] = starter


def starter(site_id: str) -> Starter | None:
    """Return the sign-in registered for a site."""
    with _starters_lock:
        return _starters.get(site_id)


def ask(prompt: str, *, addr: str, token: str, kind: PromptKind) -> tuple[int, str | None]:
    """Ask relay one question. Returns the exit status and the answer."""
    host, _, port = addr.rpartition(":")
    with socket.create_connection((host or "127.0.0.1", int(port))) as conn:
        request = {"token": token, "prompt": prompt, "kind": kind}
        conn.sendall(json.dumps(request).encode("utf-8") + b"\n")
        if kind == "notice":
            with contextlib.suppress(OSError):
                while conn.recv(4096):
                    pass
            return 0, None
        line = _read_line(conn)
    reply: object = json.loads(line or b"{}")
    if not isinstance(reply, dict):
        return 1, None
    values = cast(dict[str, Any], reply)
    if values.get("decline") or "answer" not in values:
        message = values.get("message")
        if isinstance(message, str):
            sys.stderr.write(f"{PROGRAM}: {message}\n")
        return 1, None
    return 0, str(values["answer"])


def main(argv: list[str] | None = None) -> int:
    """The ``clio-relay-askpass`` helper ssh runs."""
    args = sys.argv[1:] if argv is None else argv
    prompt = args[0] if args else ""
    addr = os.environ.get(ENV_ADDR, "")
    token = os.environ.get(ENV_TOKEN, "")
    if not addr or not token:
        sys.stderr.write(f"{PROGRAM}: no relay to ask ({ENV_ADDR} is not set)\n")
        return 1
    mode = os.environ.get("SSH_ASKPASS_PROMPT", "").lower()
    kind: PromptKind = (
        "confirm" if mode == "confirm" else "notice" if mode == "none" else classify(prompt)
    )
    try:
        status, answer = ask(prompt, addr=addr, token=token, kind=kind)
    except (OSError, ValueError) as exc:
        sys.stderr.write(f"{PROGRAM}: could not reach relay: {type(exc).__name__}\n")
        return 1
    if status != 0:
        return status
    if kind == "confirm":
        sys.stdout.write("yes\n")
    elif answer is not None:
        sys.stdout.write(answer + "\n")
    sys.stdout.flush()
    return 0


if __name__ == "__main__":
    sys.exit(main())

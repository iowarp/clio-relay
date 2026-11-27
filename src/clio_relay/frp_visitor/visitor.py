"""Log in to ``frps``, hold the visitors' loopback ports, and carry each connection.

The process logs in, binds every visitor's port once it first logs in, and then
keeps those ports for its whole life. When the rendezvous host drops it logs in
again, with backoff, and a connection that arrives meanwhile waits a few seconds
for it before it is closed. Holding the ports across a drop keeps one owner per
port: a relay that starts while the host is away adopts this process instead of
starting a second one.
"""

from __future__ import annotations

import asyncio
import contextlib
import functools
import platform
import socket
import ssl
import sys
import time
from datetime import datetime
from typing import Any

from clio_relay.frp_visitor import PROTOCOL_VERSION, START_MARKER
from clio_relay.frp_visitor.config import Config, Visitor
from clio_relay.frp_visitor.dial import dial
from clio_relay.frp_visitor.nathole import HoleError
from clio_relay.frp_visitor.pipe import (
    NETWORK_ERRORS,
    BufferedPipe,
    Pipe,
    ProtocolError,
    TcpPipe,
    join,
)
from clio_relay.frp_visitor.quic import OPEN_TUNNEL_S, DirectPath
from clio_relay.frp_visitor.wire import (
    LOGIN,
    LOGIN_RESP,
    NAT_HOLE_RESP,
    NAT_HOLE_VISITOR,
    NEW_VISITOR_CONN,
    NEW_VISITOR_CONN_RESP,
    PING,
    PONG,
    CryptoPipe,
    auth_key,
    pack,
    read_message,
)
from clio_relay.frp_visitor.yamux import Session

ANSWER_S = 10.0
"""How long frpc waits for frps to answer a login or a visitor connection."""

EXCHANGE_S = 5.0
"""How long frpc waits for frps to answer a NAT hole message."""

HOLD_S = 10.0
"""How long a connection waits for the rendezvous host to come back."""

FIRST_BACKOFF_MAX_S = 10.0
BACKOFF_MAX_S = 20.0


class LoginRefused(ConnectionError):
    """frps answered the login with an error."""


class BindFailed(OSError):
    """A visitor's port could not be bound."""


def log(level: str, message: str) -> None:
    """Write one line in frpc's format, so relay reads it as it reads frpc."""
    stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    sys.stdout.write(f"{stamp} [{level}] [relay/visitor] {message}\n")
    sys.stdout.flush()


def _goos() -> str:
    return {"win32": "windows"}.get(sys.platform, sys.platform)


def _goarch() -> str:
    machine = platform.machine().lower()
    return {"x86_64": "amd64", "amd64": "amd64", "aarch64": "arm64", "arm64": "arm64"}.get(
        machine, machine
    )


def _describe(exc: BaseException) -> str:
    if isinstance(exc, asyncio.IncompleteReadError | ssl.SSLEOFError | ssl.SSLZeroReturnError):
        return "the rendezvous host closed the connection"
    if isinstance(exc, TimeoutError):
        return "the rendezvous host did not answer in time"
    return str(exc) or type(exc).__name__


class Connector:
    """Connections to frps: streams of one yamux session, or a new dial each when not muxed."""

    def __init__(self, config: Config) -> None:
        self._server = config.server
        self.session: Session | None = None

    async def open(self) -> None:
        """Open the shared session, when the server is muxed."""
        if self._server.tcp_mux:
            self.session = Session(await dial(self._server), self._server.mux_keepalive_s)
            self.session.start()

    async def connect(self) -> Pipe:
        """Return a new connection to frps."""
        if self.session is not None:
            return await self.session.open_stream()
        return await dial(self._server)

    async def close(self) -> None:
        """Close the session and every stream on it."""
        if self.session is not None:
            await self.session.close()


class VisitorProcess:
    """One visitor file, running."""

    def __init__(self, config: Config) -> None:
        self.config = config
        self.run_id = ""
        self._connector: Connector | None = None
        self._control_pipe: BufferedPipe | None = None
        self._up = asyncio.Event()
        self._servers: list[asyncio.Server] = []
        self._connections: set[asyncio.Task[None]] = set()
        self._direct: list[DirectPath] = []
        self._pending: dict[str, asyncio.Future[dict[str, Any]]] = {}

    async def run(self) -> int:
        """Log in, hold the ports and serve, logging in again after each drop."""
        server = self.config.server
        first = True
        backoff = 1.0
        try:
            while True:
                connector = Connector(self.config)
                log("I", "try to connect to server...")
                try:
                    await connector.open()
                    control = await self._login(connector)
                except NETWORK_ERRORS as exc:
                    await connector.close()
                    reason = _describe(exc)
                    if first and server.login_fail_exit:
                        log("E", f"login to the server failed: {reason}")
                        return 1
                    log("W", f"connect to server error: {reason}")
                    await asyncio.sleep(backoff)
                    limit = FIRST_BACKOFF_MAX_S if first else BACKOFF_MAX_S
                    backoff = min(backoff * 2, limit)
                    continue
                log("I", f"login to server success, get run id [{self.run_id}]")
                backoff = 1.0
                if first:
                    first = False
                    await self._bind()
                self._connector = connector
                self._up.set()
                reason = await self._control(control)
                if connector.session is not None and connector.session.reason:
                    reason = connector.session.reason
                self._up.clear()
                self._connector = None
                await connector.close()
                log("W", f"lost the rendezvous host: {reason}; logging in again")
                await asyncio.sleep(1.0)
        except BindFailed:
            return 1
        finally:
            await self._stop()

    async def _login(self, connector: Connector) -> BufferedPipe:
        server = self.config.server
        conn = BufferedPipe(await connector.connect())
        now = int(time.time())
        login = {
            "version": PROTOCOL_VERSION,
            "hostname": socket.gethostname(),
            "os": _goos(),
            "arch": _goarch(),
            "user": server.user,
            "privilege_key": auth_key(server.token, now),
            "timestamp": now,
            "run_id": self.run_id,
        }
        try:
            await conn.write(pack(LOGIN, login))
            kind, answer = await asyncio.wait_for(read_message(conn), ANSWER_S)
        except BaseException:
            with contextlib.suppress(*NETWORK_ERRORS):
                await conn.close()
            raise
        if kind != LOGIN_RESP:
            await conn.close()
            raise ProtocolError(f"frps answered the login with message type {kind!r}")
        if answer.get("error"):
            await conn.close()
            raise LoginRefused(str(answer["error"]))
        self.run_id = str(answer.get("run_id") or "")
        return BufferedPipe(CryptoPipe(conn, server.token))

    async def _control(self, control: BufferedPipe) -> str:
        """Hold the control connection until it drops, and return why it did."""
        server = self.config.server
        last_pong = time.monotonic()
        interval = server.heartbeat_s if server.heartbeat_s > 0 else 30

        async def heartbeat() -> str:
            while True:
                await asyncio.sleep(interval)
                timeout = server.heartbeat_timeout_s
                if timeout > 0 and time.monotonic() - last_pong > timeout:
                    return "heartbeat timeout"
                now = int(time.time())
                ping = {"privilege_key": auth_key(server.token, now), "timestamp": now}
                await control.write(pack(PING, ping))

        async def receive() -> str:
            nonlocal last_pong
            while True:
                kind, body = await read_message(control)
                if kind == PONG:
                    if body.get("error"):
                        return f"frps refused the heartbeat: {body['error']}"
                    last_pong = time.monotonic()
                elif kind == NAT_HOLE_RESP:
                    waiter = self._pending.get(str(body.get("transaction_id", "")))
                    if waiter is not None and not waiter.done():
                        waiter.set_result(body)
                elif self.config.debug:
                    log("D", f"ignored control message type {kind!r}")

        self._control_pipe = control
        tasks = [asyncio.create_task(heartbeat()), asyncio.create_task(receive())]
        try:
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        finally:
            self._control_pipe = None
            for waiter in self._pending.values():
                if not waiter.done():
                    waiter.set_exception(ConnectionResetError("the control connection closed"))
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
        task = done.pop()
        exc = task.exception()
        if exc is None:
            return task.result()
        if isinstance(exc, NETWORK_ERRORS):
            return _describe(exc)
        raise exc

    async def transact(self, body: dict[str, Any]) -> dict[str, Any]:
        """Send a NatHoleVisitor message and return frps' NatHoleResp to it, as frp's transporter.

        The whole exchange, including waiting for the rendezvous host to be
        connected, takes at most ``EXCHANGE_S``.
        """
        transaction = str(body["transaction_id"])
        waiter: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()
        self._pending[transaction] = waiter
        try:
            async with asyncio.timeout(EXCHANGE_S):
                await self._up.wait()
                control = self._control_pipe
                if control is None:
                    raise ConnectionResetError("the rendezvous host is not connected")
                await control.write(pack(NAT_HOLE_VISITOR, body))
                return await waiter
        except TimeoutError as exc:
            raise HoleError("get natHoleRespMsg error: context deadline exceeded") from exc
        except NETWORK_ERRORS as exc:
            raise HoleError(f"get natHoleRespMsg error: {_describe(exc)}") from exc
        finally:
            self._pending.pop(transaction, None)
            if not waiter.done():
                waiter.cancel()

    async def _bind(self) -> None:
        server = self.config.server
        for visitor in self.config.bound():
            direct: DirectPath | None = None
            if visitor.kind == "xtcp":
                direct = DirectPath(
                    visitor,
                    proxy_name=visitor.proxy_name(server.user),
                    stun_server=server.stun_server,
                    rendezvous=f"{server.address}:{server.port}",
                    transact=self.transact,
                    log=log,
                )
            handler = functools.partial(self._accept, visitor, direct)
            try:
                listener = await asyncio.start_server(handler, visitor.bind_addr, visitor.bind_port)
            except OSError as exc:
                log("E", f"[{visitor.name}] cannot listen to port {visitor.bind_port}: {exc}")
                raise BindFailed(str(exc)) from exc
            self._servers.append(listener)
            address = f"{visitor.bind_addr}:{visitor.bind_port}"
            log("I", f"[{visitor.name}] start visitor success on {address}")
            if direct is not None:
                self._direct.append(direct)
                direct.start()

    async def _accept(
        self,
        visitor: Visitor,
        direct: DirectPath | None,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        task = asyncio.current_task()
        if task is not None:
            self._connections.add(task)
        user = TcpPipe(reader, writer)
        try:
            remote = await self._route(visitor, direct)
            if remote is not None:
                await join(user, remote)
        finally:
            with contextlib.suppress(*NETWORK_ERRORS):
                await user.close()
            if task is not None:
                self._connections.discard(task)

    async def _route(self, visitor: Visitor, direct: DirectPath | None) -> Pipe | None:
        """Pick the path for one connection: the direct tunnel, else the STCP fallback."""
        route: Visitor | None = visitor
        if direct is not None:
            route = self.config.fallback(visitor)
            wait_s = visitor.fallback_timeout_ms / 1000 if route is not None else OPEN_TUNNEL_S
            try:
                stream = await direct.open(wait_s)
            except TimeoutError as exc:
                log("E", f"[{visitor.name}] open tunnel error: {exc}")
            else:
                if not visitor.use_encryption:
                    return stream
                return CryptoPipe(BufferedPipe(stream), visitor.secret_key)
            if route is None:
                return None
            if self.config.debug:
                log("D", f"[{visitor.name}] try to transfer connection to visitor: {route.name}")
        try:
            await asyncio.wait_for(self._up.wait(), HOLD_S)
        except TimeoutError:
            log("W", f"[{visitor.name}] the rendezvous host is not connected; closed a connection")
            return None
        try:
            return await self._open(route)
        except NETWORK_ERRORS as exc:
            log("W", f"[{visitor.name}] start new visitor connection error: {_describe(exc)}")
            return None

    async def _open(self, route: Visitor) -> Pipe:
        connector = self._connector
        if connector is None:
            raise ConnectionResetError("the rendezvous host is not connected")
        conn = BufferedPipe(await connector.connect())
        now = int(time.time())
        request = {
            "run_id": self.run_id,
            "proxy_name": route.proxy_name(self.config.server.user),
            "sign_key": auth_key(route.secret_key, now),
            "timestamp": now,
            "use_encryption": route.use_encryption,
        }
        try:
            await conn.write(pack(NEW_VISITOR_CONN, request))
            kind, answer = await asyncio.wait_for(read_message(conn), ANSWER_S)
            if kind != NEW_VISITOR_CONN_RESP:
                raise ProtocolError(f"frps answered the visitor connection with type {kind!r}")
            if answer.get("error"):
                raise ConnectionRefusedError(str(answer["error"]))
        except BaseException:
            with contextlib.suppress(*NETWORK_ERRORS):
                await conn.close()
            raise
        return CryptoPipe(conn, route.secret_key) if route.use_encryption else conn

    async def _stop(self) -> None:
        for listener in self._servers:
            listener.close()
        for task in list(self._connections):
            task.cancel()
        await asyncio.gather(*self._connections, return_exceptions=True)
        for direct in self._direct:
            await direct.close()
        if self._connector is not None:
            await self._connector.close()


async def run(config: Config, path: str) -> int:
    """Run one visitor file until the process is stopped."""
    log("I", f"{START_MARKER} for config file [{path}]")
    return await VisitorProcess(config).run()

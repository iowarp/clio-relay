"""The direct path: one QUIC connection over the punched UDP socket, and its upkeep.

frpc's xtcp proxy listens with quic-go on its end of the hole. This is the
client end, as frp 0.69.1's ``QUICTunnelSession`` dials it: ALPN ``frp``, no
certificate check (the peer's certificate is self-signed and new each time; the
punch itself proved the peer knows the secret key, and ``useEncryption`` adds
the stream crypto on top), a 30 s idle timeout and a 10 s keepalive. Each
connection the visitor carries is one bidirectional stream, with nothing sent
on it before the user's own bytes.

Two things differ from a plain aioquic client, both for durability:

- The tunnel reads the socket itself instead of through an asyncio datagram
  transport. On Windows a UDP socket reports an earlier datagram's "port
  unreachable" on its next read, and asyncio's transport stops reading after
  one, which would freeze the tunnel. Here that read is skipped.
- A write waits while its stream has more than ``SEND_WINDOW`` bytes the peer
  has not acknowledged, since aioquic never pauses a writer.

``DirectPath`` runs the punches (``nathole``) and keeps the tunnel, as frp's
``XTCPVisitor`` does; the visitor only asks it for a stream and, when there is
none in time, sends the connection to its STCP fallback.
"""

from __future__ import annotations

import asyncio
import contextlib
import itertools
import socket
import ssl
import time
from collections.abc import Buffer, Callable
from typing import Any, cast

from aioquic.asyncio.protocol import QuicConnectionProtocol
from aioquic.quic.configuration import QuicConfiguration
from aioquic.quic.connection import QuicConnection
from aioquic.quic.events import ConnectionTerminated, QuicEvent

from clio_relay.frp_visitor import nathole
from clio_relay.frp_visitor.config import Visitor
from clio_relay.frp_visitor.nathole import Address, HoleError, Transact, receive
from clio_relay.frp_visitor.pipe import CHUNK, NETWORK_ERRORS, Pipe, ProtocolError

ALPN = "frp"
HANDSHAKE_S = 10.0
IDLE_S = 30.0
KEEPALIVE_S = 10.0
SEND_WINDOW = 1 << 20
"""Unacknowledged bytes one stream may hold before its writer waits."""

PUNCH_SPACING_S = 10.0
"""frp starts punches at least this far apart."""

OPEN_TUNNEL_S = 20.0
RETRY_S = 0.5
NO_TUNNEL = "no tunnel session"

Log = Callable[[str, str], None]


class _Datagrams(asyncio.DatagramTransport):
    """What aioquic sends through: the punched socket, written directly."""

    def __init__(self, sock: socket.socket) -> None:
        super().__init__()
        self._sock = sock

    def sendto(self, data: Buffer, addr: Any = None) -> None:
        # A full send buffer or a moment without a route drops the datagram, which
        # QUIC sends again, as it would after a loss on the network.
        with contextlib.suppress(OSError):
            self._sock.sendto(data, addr)

    def close(self) -> None:
        self._sock.close()

    def is_closing(self) -> bool:
        return self._sock.fileno() == -1


class _Protocol(QuicConnectionProtocol):
    """aioquic's protocol, telling writers when acknowledgements may have arrived."""

    def __init__(self, quic: QuicConnection) -> None:
        super().__init__(quic)
        self._progress = asyncio.Event()

    def datagram_received(self, data: bytes | str, addr: Any) -> None:
        super().datagram_received(data, addr)
        self._wake()

    def quic_event_received(self, event: QuicEvent) -> None:
        super().quic_event_received(event)
        if isinstance(event, ConnectionTerminated):
            self._wake()

    def _wake(self) -> None:
        self._progress.set()
        self._progress.clear()

    async def progress(self) -> None:
        """Wait for the next datagram or the end of the connection."""
        await self._progress.wait()

    def unacknowledged(self, stream_id: int) -> int:
        """Return the bytes written to ``stream_id`` that the peer has not acknowledged."""
        streams = cast(dict[int, object], getattr(self._quic, "_streams", {}))
        sender = getattr(streams.get(stream_id), "sender", None)
        return len(cast(bytes, getattr(sender, "_buffer", b"")))


class QuicPipe(Pipe):
    """One stream of the tunnel."""

    def __init__(
        self, protocol: _Protocol, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        self._protocol = protocol
        self._reader = reader
        self._writer = writer
        self._id = int(writer.get_extra_info("stream_id"))

    async def read(self, n: int = CHUNK) -> bytes:
        return await self._reader.read(n)

    async def write(self, data: bytes) -> None:
        self._writer.write(data)
        await self._writer.drain()
        while self._protocol.unacknowledged(self._id) > SEND_WINDOW:
            await self._protocol.progress()
            await self._writer.drain()

    async def close(self) -> None:
        if self._writer.is_closing():
            return
        with contextlib.suppress(RuntimeError, *NETWORK_ERRORS):
            self._writer.write_eof()


class QuicTunnel:
    """The QUIC connection to the peer's frpc, on the socket the punch opened."""

    def __init__(self, sock: socket.socket, peer: Address) -> None:
        self.peer = peer
        self._sock = sock
        configuration = QuicConfiguration(
            is_client=True,
            alpn_protocols=[ALPN],
            verify_mode=ssl.CERT_NONE,
            idle_timeout=IDLE_S,
        )
        self._protocol = _Protocol(QuicConnection(configuration=configuration))
        self._protocol.connection_made(_Datagrams(sock))
        self._pings = itertools.count(1)
        self._tasks: list[asyncio.Task[None]] = []
        self._broken = False

    @classmethod
    async def open(cls, sock: socket.socket, peer: Address) -> QuicTunnel:
        """Dial the peer over ``sock``; the tunnel owns the socket from here on."""
        tunnel = cls(sock, peer)
        try:
            tunnel._tasks.append(asyncio.create_task(tunnel._read()))
            tunnel._protocol.connect(peer)
            await asyncio.wait_for(tunnel._protocol.wait_connected(), HANDSHAKE_S)
        except BaseException:
            await tunnel.close()
            raise
        tunnel._tasks.append(asyncio.create_task(tunnel._keepalive()))
        tunnel._tasks.append(asyncio.create_task(tunnel._reap()))
        return tunnel

    @property
    def alive(self) -> bool:
        """Return whether the connection can still carry a new stream."""
        return not self._broken and not self._protocol.quic_connection.is_closing()

    async def open_stream(self) -> Pipe:
        """Open one stream for one connection."""
        if not self.alive:
            raise ConnectionResetError("the direct path's tunnel is closed")
        reader, writer = await self._protocol.create_stream()
        return QuicPipe(self._protocol, reader, writer)

    async def check(self, timeout: float = 5.0) -> None:
        """Ping the peer and wait for its answer, raising if the tunnel is gone."""
        if not self.alive:
            raise ConnectionResetError("the direct path's tunnel is closed")
        await asyncio.wait_for(self._protocol.ping(), timeout)

    async def close(self) -> None:
        """Close the connection, its streams and the socket."""
        if not self._protocol.quic_connection.is_closing():
            self._protocol.close()
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._sock.close()

    async def _read(self) -> None:
        while True:
            try:
                data, address = await receive(self._sock)
            except OSError:
                # The socket itself failed; QUIC can no longer hear the peer.
                self._broken = True
                self._protocol.close()
                return
            self._protocol.datagram_received(data, address)

    async def _keepalive(self) -> None:
        while True:
            await asyncio.sleep(KEEPALIVE_S)
            if not self.alive:
                return
            self._protocol.quic_connection.send_ping(next(self._pings))
            self._protocol.transmit()

    async def _reap(self) -> None:
        """Release the socket once QUIC has ended the connection (an idle timeout, say)."""
        await self._protocol.wait_closed()
        self._broken = True
        others = [task for task in self._tasks if task is not asyncio.current_task()]
        for task in others:
            task.cancel()
        await asyncio.gather(*others, return_exceptions=True)
        self._sock.close()


class _Bucket:
    """frp's retry limiter: ``per_hour`` tokens, refilled evenly over an hour, full at start."""

    def __init__(self, per_hour: int) -> None:
        self._rate = per_hour / 3600
        self._size = float(per_hour)
        self._tokens = float(per_hour)
        self._at = time.monotonic()

    async def take(self) -> None:
        now = time.monotonic()
        self._tokens = min(self._size, self._tokens + (now - self._at) * self._rate)
        self._at = now
        if self._tokens < 1:
            await asyncio.sleep((1 - self._tokens) / self._rate)
            self._tokens, self._at = 1.0, time.monotonic()
        self._tokens -= 1


def _go_list(items: list[str]) -> str:
    return "[" + " ".join(items) + "]"


def _go_behavior(behavior: dict[str, Any]) -> str:
    ports = cast(list[dict[str, Any]], behavior.get("candidate_ports") or [])
    spans = " ".join(f"{{From:{p.get('from', 0)} To:{p.get('to', 0)}}}" for p in ports)
    return (
        f"{{Role:{behavior.get('role', '')} Mode:{behavior.get('mode', 0)} "
        f"TTL:{behavior.get('ttl', 0)} SendDelayMs:{behavior.get('send_delay_ms', 0)} "
        f"ReadTimeoutMs:{behavior.get('read_timeout', 0)} CandidatePorts:[{spans}] "
        f"SendRandomPorts:{behavior.get('send_random_ports', 0)} "
        f"ListenRandomPorts:{behavior.get('listen_random_ports', 0)}}}"
    )


class DirectPath:
    """One xtcp visitor's direct path: its punches, its tunnel and, if asked, its upkeep.

    As frp's ``XTCPVisitor``: a connection that finds no tunnel asks for a punch
    and retries every half second; punches run one at a time, at least
    ``PUNCH_SPACING_S`` apart, and a request during one is dropped. With
    ``keepTunnelOpen`` the path punches at start and checks the tunnel every
    ``minRetryInterval``, punching again (at most ``maxRetriesAnHour``) when it is
    gone. The check is a QUIC ping, where frp opens and closes a stream: a stream
    makes the peer frpc dial the service, and a ping proves the same tunnel.
    """

    def __init__(
        self,
        visitor: Visitor,
        *,
        proxy_name: str,
        stun_server: str,
        rendezvous: str,
        transact: Transact,
        log: Log,
    ) -> None:
        self.visitor = visitor
        self.tunnel: QuicTunnel | None = None
        self._proxy = proxy_name
        self._stun = stun_server
        self._rendezvous = rendezvous
        self._transact = transact
        self._log = log
        self._wanted = asyncio.Event()
        self._idle = True
        self._tasks: list[asyncio.Task[None]] = []

    def start(self) -> None:
        """Start the punch runner and, with ``keepTunnelOpen``, the upkeep."""
        self._tasks.append(asyncio.create_task(self._runner()))
        if self.visitor.keep_tunnel_open:
            self._wanted.set()
            self._tasks.append(asyncio.create_task(self._keep_open()))

    def request(self) -> None:
        """Ask for a punch, unless one is running or spacing out."""
        if self._idle:
            self._wanted.set()

    async def open(self, timeout: float) -> Pipe:
        """Return a stream of the tunnel, waiting up to ``timeout`` (20 s at most) for one."""
        loop = asyncio.get_running_loop()
        end = loop.time() + min(timeout, OPEN_TUNNEL_S)
        while True:
            try:
                return await self._stream()
            except LookupError:
                pass
            except NETWORK_ERRORS as exc:
                self._say("W", f"get tunnel connection error: {exc}")
            remaining = end - loop.time()
            if remaining <= 0:
                break
            await asyncio.sleep(min(RETRY_S, remaining))
            if loop.time() >= end:
                break
        raise TimeoutError("open tunnel timeout")

    async def close(self) -> None:
        """Stop punching and close the tunnel."""
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        if self.tunnel is not None:
            await self._drop(self.tunnel)

    async def _stream(self) -> Pipe:
        tunnel = self.tunnel
        if tunnel is None:
            self.request()
            raise LookupError(NO_TUNNEL)
        try:
            return await tunnel.open_stream()
        except NETWORK_ERRORS:
            await self._drop(tunnel)
            self.request()
            raise

    async def _drop(self, tunnel: QuicTunnel) -> None:
        if self.tunnel is tunnel:
            self.tunnel = None
        await tunnel.close()

    def _say(self, level: str, message: str) -> None:
        self._log(level, f"[{self.visitor.name}] {message}")

    async def _runner(self) -> None:
        loop = asyncio.get_running_loop()
        while True:
            self._idle = True
            await self._wanted.wait()
            self._wanted.clear()
            self._idle = False
            began = loop.time()
            try:
                await self._punch()
            except Exception as exc:  # noqa: BLE001 - one punch must never end the path
                self._say("W", f"make hole error: {type(exc).__name__}: {exc}")
            spent = loop.time() - began
            if spent < PUNCH_SPACING_S:
                await asyncio.sleep(PUNCH_SPACING_S - spent)

    async def _punch(self) -> None:
        visitor = self.visitor
        failures = (HoleError, OSError, ValueError, ProtocolError)
        try:
            await nathole.precheck(self._transact, self._proxy)
        except failures as exc:
            self._say("W", f"nathole precheck error: {exc}")
            return
        try:
            prepared = await nathole.prepare(
                self._stun, self._rendezvous, disable_assisted=visitor.disable_assisted_addrs
            )
        except failures as exc:
            self._say("W", f"nathole prepare error: {exc}")
            return
        self._say(
            "I",
            f"nathole prepare success, nat type: {prepared.nat_type}, behavior: "
            f"{prepared.behavior}, addresses: {_go_list(prepared.addrs)}, "
            f"assistedAddresses: {_go_list(prepared.assisted)}",
        )
        try:
            hole = await nathole.exchange(
                self._transact, prepared, self._proxy, visitor.secret_key, visitor.protocol
            )
        except BaseException as exc:
            prepared.sock.close()
            if not isinstance(exc, failures):
                raise
            self._say("W", f"nathole exchange info error: {exc}")
            return
        self._say(
            "I",
            f"get natHoleRespMsg, sid [{hole.sid}], protocol [{hole.protocol}], candidate "
            f"address {_go_list(hole.candidates)}, assisted address "
            f"{_go_list(hole.assisted)}, detectBehavior: {_go_behavior(hole.behavior)}",
        )
        try:
            sock, peer = await nathole.make_hole(prepared.sock, hole, visitor.secret_key)
        except failures as exc:
            self._say("W", f"make hole error: {exc}")
            return
        self._say(
            "I",
            f"establishing nat hole connection successful, sid [{hole.sid}], "
            f"remoteAddr [{peer[0]}:{peer[1]}]",
        )
        try:
            tunnel = await QuicTunnel.open(sock, peer)
        except (*NETWORK_ERRORS, ConnectionError) as exc:
            reason = "timeout: no recent network activity" if isinstance(exc, TimeoutError) else exc
            self._say("W", f"init tunnel session error: dial quic error: {reason}")
            return
        old, self.tunnel = self.tunnel, tunnel
        if old is not None:
            await old.close()

    async def _keep_open(self) -> None:
        bucket = _Bucket(self.visitor.max_retries_an_hour)
        while True:
            await asyncio.sleep(self.visitor.min_retry_interval_s)
            tunnel = self.tunnel
            try:
                if tunnel is None:
                    raise LookupError(NO_TUNNEL)
                await tunnel.check()
            except (LookupError, *NETWORK_ERRORS) as exc:
                reason = (
                    "the tunnel did not answer a ping" if isinstance(exc, TimeoutError) else exc
                )
                self._say("W", f"keepTunnelOpenWorker get tunnel connection error: {reason}")
                if tunnel is not None:
                    await self._drop(tunnel)
                self.request()
                await bucket.take()

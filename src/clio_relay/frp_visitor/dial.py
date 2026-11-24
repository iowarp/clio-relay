"""Dial ``frps`` the way frpc 0.69 does, for each transport relay uses.

- ``tcp``: TLS straight away when ``transport.tls.enable`` (frp's default),
  else plain.
- ``websocket``: a websocket to ``/~!frp`` first, then TLS inside it when enabled.
- ``wss``: TLS first, always, then the websocket. A front proxy terminates it.

Like frpc, TLS checks the server's certificate only when a CA file is given;
relay always gives one for ``wss``.
"""

from __future__ import annotations

import asyncio
import contextlib
import ssl

from websockets.client import ClientProtocol
from websockets.frames import Frame, Opcode
from websockets.http11 import Response
from websockets.protocol import State
from websockets.typing import Origin
from websockets.uri import parse_uri

from clio_relay.frp_visitor.config import Server
from clio_relay.frp_visitor.pipe import CHUNK, NETWORK_ERRORS, Pipe, ProtocolError, TcpPipe

WEBSOCKET_PATH = "/~!frp"
_FRP_TLS_BYTE = b"\x17"


class TlsPipe(Pipe):
    """TLS over any pipe, through memory BIOs, so it can sit inside a websocket."""

    def __init__(self, inner: Pipe, context: ssl.SSLContext, server_name: str) -> None:
        self._inner = inner
        self._incoming = ssl.MemoryBIO()
        self._outgoing = ssl.MemoryBIO()
        self._tls = context.wrap_bio(self._incoming, self._outgoing, server_hostname=server_name)
        self._send_lock = asyncio.Lock()
        self._recv_lock = asyncio.Lock()

    async def _flush(self) -> None:
        async with self._send_lock:
            data = self._outgoing.read()
            if data:
                await self._inner.write(data)

    async def _fill(self) -> None:
        data = await self._inner.read()
        if data:
            self._incoming.write(data)
        else:
            self._incoming.write_eof()

    async def handshake(self) -> None:
        """Run the TLS handshake."""
        while True:
            try:
                self._tls.do_handshake()
                break
            except ssl.SSLWantReadError:
                await self._flush()
                await self._fill()
        await self._flush()

    async def read(self, n: int = CHUNK) -> bytes:
        async with self._recv_lock:
            while True:
                try:
                    return self._tls.read(n)
                except ssl.SSLWantReadError:
                    await self._flush()
                    await self._fill()
                except (ssl.SSLZeroReturnError, ssl.SSLEOFError):
                    return b""

    async def write(self, data: bytes) -> None:
        self._tls.write(data)
        await self._flush()

    async def close(self) -> None:
        with contextlib.suppress(ssl.SSLError):
            self._tls.unwrap()
        with contextlib.suppress(*NETWORK_ERRORS):
            await self._flush()
        await self._inner.close()


class WebsocketPipe(Pipe):
    """A websocket client over any pipe, carrying the stream in binary frames."""

    def __init__(self, inner: Pipe, host: str) -> None:
        self._inner = inner
        self._protocol = ClientProtocol(
            parse_uri(f"ws://{host}{WEBSOCKET_PATH}"),
            origin=Origin(f"http://{host}"),
            max_size=None,
        )
        self._pending: bytearray = bytearray()
        self._send_lock = asyncio.Lock()
        self._recv_lock = asyncio.Lock()

    def _take(self, event: object) -> None:
        if isinstance(event, Frame) and event.opcode in (Opcode.TEXT, Opcode.BINARY, Opcode.CONT):
            self._pending += event.data

    async def _flush(self) -> None:
        async with self._send_lock:
            data = b"".join(self._protocol.data_to_send())
            if data:
                await self._inner.write(data)

    async def _receive(self) -> bool:
        data = await self._inner.read()
        if data:
            self._protocol.receive_data(data)
        else:
            self._protocol.receive_eof()
        await self._flush()
        return bool(data)

    async def handshake(self) -> None:
        """Send the upgrade request and check the answer."""
        self._protocol.send_request(self._protocol.connect())
        await self._flush()
        while True:
            open_ = await self._receive()
            upgraded = False
            for event in self._protocol.events_received():
                if isinstance(event, Response):
                    if self._protocol.handshake_exc is not None:
                        raise ProtocolError(
                            f"the websocket upgrade to {WEBSOCKET_PATH} was refused: "
                            f"{self._protocol.handshake_exc}"
                        )
                    upgraded = True
                else:
                    self._take(event)
            if upgraded:
                return
            if not open_:
                raise ProtocolError("the server closed the connection during the websocket upgrade")

    async def read(self, n: int = CHUNK) -> bytes:
        async with self._recv_lock:
            while not self._pending:
                if self._protocol.state is not State.OPEN:
                    return b""
                open_ = await self._receive()
                for event in self._protocol.events_received():
                    if not isinstance(event, Response):
                        self._take(event)
                if not open_ and not self._pending:
                    return b""
            data = bytes(self._pending[:n])
            del self._pending[:n]
            return data

    async def write(self, data: bytes) -> None:
        if self._protocol.state is not State.OPEN:
            raise ConnectionResetError("the websocket to the rendezvous host is closed")
        self._protocol.send_binary(data)
        await self._flush()

    async def close(self) -> None:
        if self._protocol.state is State.OPEN:
            self._protocol.send_close()
            with contextlib.suppress(*NETWORK_ERRORS):
                await self._flush()
        await self._inner.close()


def tls_context(server: Server) -> ssl.SSLContext:
    """Return frpc's TLS settings: verify against ``ca_file`` when given, else do not."""
    if server.ca_file:
        return ssl.create_default_context(cafile=server.ca_file)
    context = ssl.create_default_context()
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    return context


async def _tls(inner: Pipe, server: Server) -> Pipe:
    pipe = TlsPipe(inner, tls_context(server), server.server_name)
    await pipe.handshake()
    return pipe


async def _websocket(inner: Pipe, host: str) -> Pipe:
    pipe = WebsocketPipe(inner, host)
    await pipe.handshake()
    return pipe


def _host_port(address: str, port: int) -> str:
    return f"[{address}]:{port}" if ":" in address else f"{address}:{port}"


async def dial(server: Server) -> Pipe:
    """Open one connection to ``frps`` over the configured transport."""
    timeout = server.dial_timeout_s if server.dial_timeout_s > 0 else None
    reader, writer = await asyncio.wait_for(
        asyncio.open_connection(server.address, server.port), timeout
    )
    pipe: Pipe = TcpPipe(reader, writer)
    try:
        async with asyncio.timeout(timeout):
            if server.protocol == "wss":
                pipe = await _tls(pipe, server)
                pipe = await _websocket(pipe, server.server_name)
            else:
                if server.protocol == "websocket":
                    pipe = await _websocket(pipe, _host_port(server.address, server.port))
                if server.uses_tls:
                    if server.tls_first_byte:
                        await pipe.write(_FRP_TLS_BYTE)
                    pipe = await _tls(pipe, server)
    except BaseException:
        with contextlib.suppress(*NETWORK_ERRORS):
            await pipe.close()
        raise
    return pipe

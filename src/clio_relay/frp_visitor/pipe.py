"""Byte pipes the visitor layers: TCP, then TLS or a websocket, then yamux and crypto.

Every layer is a ``Pipe``: ``read`` returns up to ``n`` bytes, or ``b""`` at the
end; ``write`` sends all of ``data``; ``close`` ends it. Layering pipes keeps each
transport a few lines and lets the frp code above them ignore which one it is.
"""

from __future__ import annotations

import asyncio
import contextlib
import socket

CHUNK = 64 * 1024


class ProtocolError(ConnectionError):
    """The other end sent something the protocol does not allow."""


NETWORK_ERRORS: tuple[type[BaseException], ...] = (OSError, EOFError)
"""What a dropped or refused connection raises (``ssl.SSLError`` and timeouts are ``OSError``)."""


class Pipe:
    """A bidirectional byte stream."""

    async def read(self, n: int = CHUNK) -> bytes:
        """Return up to ``n`` bytes, or ``b""`` when the other end is done."""
        raise NotImplementedError

    async def write(self, data: bytes) -> None:
        """Send all of ``data``."""
        raise NotImplementedError

    async def close(self) -> None:
        """End the pipe, and every pipe under it."""
        raise NotImplementedError


class TcpPipe(Pipe):
    """A TCP connection."""

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self._reader = reader
        self._writer = writer
        sock = writer.get_extra_info("socket")
        if sock is not None:
            with contextlib.suppress(OSError):
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)

    async def read(self, n: int = CHUNK) -> bytes:
        return await self._reader.read(n)

    async def write(self, data: bytes) -> None:
        self._writer.write(data)
        await self._writer.drain()

    async def close(self) -> None:
        if self._writer.is_closing():
            return
        self._writer.close()
        with contextlib.suppress(*NETWORK_ERRORS):
            await self._writer.wait_closed()


class BufferedPipe(Pipe):
    """A pipe that can read an exact count or up to a delimiter, keeping what is left."""

    def __init__(self, inner: Pipe) -> None:
        self.inner = inner
        self._buffer = bytearray()

    async def _more(self, want: int) -> None:
        data = await self.inner.read(max(CHUNK, want))
        if not data:
            raise asyncio.IncompleteReadError(bytes(self._buffer), None)
        self._buffer += data

    async def readexactly(self, n: int) -> bytes:
        """Return exactly ``n`` bytes, or raise ``IncompleteReadError`` at the end."""
        while len(self._buffer) < n:
            await self._more(n - len(self._buffer))
        data = bytes(self._buffer[:n])
        del self._buffer[:n]
        return data

    async def read(self, n: int = CHUNK) -> bytes:
        if self._buffer:
            data = bytes(self._buffer[:n])
            del self._buffer[:n]
            return data
        return await self.inner.read(n)

    async def write(self, data: bytes) -> None:
        await self.inner.write(data)

    async def close(self) -> None:
        await self.inner.close()


async def join(one: Pipe, other: Pipe) -> None:
    """Copy both ways until either side ends, then close both, as frpc does."""

    async def copy(source: Pipe, sink: Pipe) -> None:
        with contextlib.suppress(*NETWORK_ERRORS):
            while data := await source.read():
                await sink.write(data)

    tasks = [asyncio.create_task(copy(one, other)), asyncio.create_task(copy(other, one))]
    try:
        await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        for pipe in (one, other):
            with contextlib.suppress(*NETWORK_ERRORS):
                await pipe.close()

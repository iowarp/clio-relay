"""A yamux client session, as frp multiplexes streams over one connection (``tcpMux``).

frps uses hashicorp's yamux. A frame is a 12-byte header (version 0, type,
flags, stream id, length), then ``length`` bytes for data frames. The client
opens odd stream ids, each with a 256 KiB window both ways that window-update
frames grow. Pings keep the session alive both ways.
"""

from __future__ import annotations

import asyncio
import contextlib
import struct

from clio_relay.frp_visitor.pipe import CHUNK, NETWORK_ERRORS, BufferedPipe, Pipe, ProtocolError

DATA, WINDOW, PING, GO_AWAY = 0, 1, 2, 3
SYN, ACK, FIN, RST = 1, 2, 4, 8
INITIAL_WINDOW = 256 * 1024
RECEIVE_WINDOW = 2 * 1024 * 1024
MAX_FRAME = 32 * 1024
_HEADER = struct.Struct(">BBHII")


class Stream(Pipe):
    """One yamux stream."""

    def __init__(self, session: Session, stream_id: int) -> None:
        self._session = session
        self.id = stream_id
        self._buffer = bytearray()
        self._readable = asyncio.Event()
        self._writable = asyncio.Event()
        self._writable.set()
        self._send_window = INITIAL_WINDOW
        self._consumed = 0
        self._remote_done = False
        self._local_done = False
        self._reset = False

    def on_data(self, data: bytes) -> None:
        if not (self._remote_done or self._local_done or self._reset):
            self._buffer += data
            self._readable.set()

    def on_window(self, delta: int) -> None:
        self._send_window += delta
        if self._send_window > 0:
            self._writable.set()

    def on_flags(self, flags: int) -> None:
        if flags & FIN:
            self._remote_done = True
        if flags & RST:
            self._reset = True
        if flags & (FIN | RST):
            self._readable.set()
            self._writable.set()
            self._forget_when_done()

    def ended(self) -> None:
        self._reset = True
        self._readable.set()
        self._writable.set()

    def _forget_when_done(self) -> None:
        if self._reset or (self._remote_done and self._local_done):
            self._session.forget(self.id)

    async def read(self, n: int = CHUNK) -> bytes:
        while not self._buffer:
            if self._reset:
                raise ConnectionResetError("the rendezvous host reset the stream")
            if self._remote_done:
                return b""
            self._readable.clear()
            await self._readable.wait()
        data = bytes(self._buffer[:n])
        del self._buffer[:n]
        self._consumed += len(data)
        if self._consumed >= RECEIVE_WINDOW // 2 and not self._remote_done:
            delta, self._consumed = self._consumed, 0
            await self._session.send(WINDOW, 0, self.id, delta)
        return data

    async def write(self, data: bytes) -> None:
        view = memoryview(data)
        while view:
            if self._reset:
                raise ConnectionResetError("the rendezvous host reset the stream")
            if self._local_done:
                raise ConnectionResetError("the stream is closed")
            if self._send_window <= 0:
                self._writable.clear()
                await self._writable.wait()
                continue
            size = min(len(view), self._send_window, MAX_FRAME)
            self._send_window -= size
            await self._session.send(DATA, 0, self.id, size, bytes(view[:size]))
            view = view[size:]

    async def close(self) -> None:
        if self._local_done:
            return
        self._local_done = True
        self._buffer.clear()
        if not self._reset:
            with contextlib.suppress(*NETWORK_ERRORS):
                await self._session.send(WINDOW, FIN, self.id, 0)
        self._forget_when_done()


class Session:
    """The client end of one yamux session over a pipe."""

    def __init__(self, pipe: Pipe, keepalive_s: float) -> None:
        self._pipe = BufferedPipe(pipe)
        self._keepalive_s = keepalive_s
        self._streams: dict[int, Stream] = {}
        self._next_id = 1
        self._send_lock = asyncio.Lock()
        self._pings: dict[int, asyncio.Future[None]] = {}
        self._ping_id = 0
        self._tasks: list[asyncio.Task[None]] = []
        self.closed = False
        self.reason = ""

    def start(self) -> None:
        """Start reading frames and sending keepalives."""
        self._tasks = [asyncio.create_task(self._receive())]
        if self._keepalive_s > 0:
            self._tasks.append(asyncio.create_task(self._keepalive()))

    def forget(self, stream_id: int) -> None:
        """Drop a stream both ends are done with."""
        self._streams.pop(stream_id, None)

    async def send(
        self, kind: int, flags: int, stream_id: int, length: int, body: bytes = b""
    ) -> None:
        """Send one frame."""
        if self.closed:
            raise ConnectionResetError(self.reason or "the session to the rendezvous host closed")
        async with self._send_lock:
            await self._pipe.write(_HEADER.pack(0, kind, flags, stream_id, length) + body)

    async def open_stream(self) -> Stream:
        """Open a stream, granting the server a larger window than the default."""
        stream_id = self._next_id
        self._next_id += 2
        stream = Stream(self, stream_id)
        self._streams[stream_id] = stream
        try:
            await self.send(WINDOW, SYN, stream_id, RECEIVE_WINDOW - INITIAL_WINDOW)
        except BaseException:
            self.forget(stream_id)
            raise
        return stream

    async def _receive(self) -> None:
        try:
            while True:
                version, kind, flags, stream_id, length = _HEADER.unpack(
                    await self._pipe.readexactly(_HEADER.size)
                )
                if version != 0:
                    raise ProtocolError(f"the rendezvous host sent yamux version {version}")
                if kind in (DATA, WINDOW):
                    body = await self._pipe.readexactly(length) if kind == DATA and length else b""
                    stream = self._streams.get(stream_id)
                    if stream is None:
                        if flags & SYN:
                            await self.send(WINDOW, RST, stream_id, 0)
                        continue
                    if kind == DATA:
                        stream.on_data(body)
                    else:
                        stream.on_window(length)
                    stream.on_flags(flags)
                elif kind == PING:
                    if flags & SYN:
                        await self.send(PING, ACK, 0, length)
                    elif (waiter := self._pings.pop(length, None)) and not waiter.done():
                        waiter.set_result(None)
                elif kind == GO_AWAY:
                    self.reason = "the rendezvous host ended the session"
                    return
                else:
                    raise ProtocolError(f"the rendezvous host sent yamux frame type {kind}")
        except NETWORK_ERRORS as exc:
            self.reason = self.reason or _describe(exc)
        finally:
            await self.close()

    async def _keepalive(self) -> None:
        loop = asyncio.get_running_loop()
        while not self.closed:
            await asyncio.sleep(self._keepalive_s)
            self._ping_id = (self._ping_id + 1) & 0xFFFFFFFF
            waiter: asyncio.Future[None] = loop.create_future()
            self._pings[self._ping_id] = waiter
            try:
                await self.send(PING, SYN, 0, self._ping_id)
                await asyncio.wait_for(waiter, self._keepalive_s)
            except TimeoutError:
                self.reason = "the rendezvous host stopped answering keepalives"
                await self.close()
                return
            except NETWORK_ERRORS:
                return

    async def wait_closed(self) -> None:
        """Return once the session has ended."""
        await asyncio.gather(*self._tasks, return_exceptions=True)

    async def close(self) -> None:
        """End the session and every stream on it."""
        if self.closed:
            return
        self.closed = True
        for stream in list(self._streams.values()):
            stream.ended()
        self._streams.clear()
        for waiter in self._pings.values():
            if not waiter.done():
                waiter.cancel()
        self._pings.clear()
        current = asyncio.current_task()
        for task in self._tasks:
            if task is not current:
                task.cancel()
        with contextlib.suppress(*NETWORK_ERRORS):
            await self._pipe.close()


def _describe(exc: BaseException) -> str:
    if isinstance(exc, asyncio.IncompleteReadError):
        return "the rendezvous host closed the connection"
    return str(exc) or type(exc).__name__

"""A small HTTP and WebSocket service that checks a secret on every request.

Standard library only, so it runs on any site with Python 3.10 or newer.

Routes:

- ``GET /health`` needs no secret and answers ``ok``. It is the health path.
- ``GET /`` needs the secret and answers a JSON description of the service.
- ``GET /ws`` needs the secret and upgrades to a WebSocket that echoes each message.

The secret is read from an environment variable, never from the command line, and is
accepted as ``Authorization: Bearer <secret>`` or as ``?token=<secret>`` (a browser
cannot set headers on a WebSocket). On SIGTERM or SIGINT the service writes the
optional final artifact and exits 0, so a test can see a service's termination output.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import contextlib
import hashlib
import hmac
import json
import os
import signal
import socket
import struct
import sys
import time
from dataclasses import dataclass, field
from urllib.parse import parse_qs, urlsplit

WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
MAX_HEADER_BYTES = 16 * 1024
MAX_FRAME_BYTES = 1024 * 1024
OP_CONTINUATION = 0x0
OP_TEXT = 0x1
OP_BINARY = 0x2
OP_CLOSE = 0x8
OP_PING = 0x9
OP_PONG = 0xA


@dataclass
class Request:
    """One parsed HTTP request head."""

    method: str
    path: str
    query: dict[str, list[str]]
    headers: dict[str, str]


@dataclass
class ServiceState:
    """Counters the service reports, and its secret."""

    secret: str
    started_at: float = field(default_factory=time.time)
    requests: int = 0
    refused: int = 0
    websocket_sessions: int = 0
    websocket_messages: int = 0


class ProtocolError(Exception):
    """A peer broke the HTTP or WebSocket framing."""


async def read_request(reader: asyncio.StreamReader) -> Request:
    """Read and parse one request head."""
    try:
        head = await reader.readuntil(b"\r\n\r\n")
    except asyncio.LimitOverrunError as exc:
        raise ProtocolError("request head too large") from exc
    if len(head) > MAX_HEADER_BYTES:
        raise ProtocolError("request head too large")
    lines = head.decode("latin-1").split("\r\n")
    parts = lines[0].split(" ")
    if len(parts) != 3:
        raise ProtocolError("bad request line")
    method, target, _version = parts
    headers: dict[str, str] = {}
    for line in lines[1:]:
        if not line:
            continue
        name, sep, value = line.partition(":")
        if not sep:
            raise ProtocolError("bad header line")
        headers[name.strip().lower()] = value.strip()
    split = urlsplit(target)
    return Request(method, split.path or "/", parse_qs(split.query), headers)


def presented_secret(request: Request) -> str | None:
    """Return the secret the request presents, if any."""
    auth = request.headers.get("authorization", "")
    scheme, _, value = auth.partition(" ")
    if scheme.lower() == "bearer" and value:
        return value.strip()
    tokens = request.query.get("token")
    if tokens:
        return tokens[0]
    return None


def authorized(request: Request, state: ServiceState) -> bool:
    """Compare the presented secret in constant time."""
    presented = presented_secret(request)
    if presented is None:
        return False
    return hmac.compare_digest(presented.encode(), state.secret.encode())


async def respond(
    writer: asyncio.StreamWriter,
    status: str,
    body: bytes,
    content_type: str,
    extra_headers: dict[str, str] | None = None,
) -> None:
    """Write one complete HTTP response and close."""
    headers = {
        "Content-Type": content_type,
        "Content-Length": str(len(body)),
        "Cache-Control": "no-store",
        "Connection": "close",
    }
    headers.update(extra_headers or {})
    head = f"HTTP/1.1 {status}\r\n" + "".join(f"{k}: {v}\r\n" for k, v in headers.items())
    writer.write(head.encode("latin-1") + b"\r\n" + body)
    await writer.drain()


async def read_frame(reader: asyncio.StreamReader) -> tuple[bool, int, bytes]:
    """Read one client frame. Client frames must be masked."""
    first, second = await reader.readexactly(2)
    fin = bool(first & 0x80)
    opcode = first & 0x0F
    if not second & 0x80:
        raise ProtocolError("client frame not masked")
    length = second & 0x7F
    if length == 126:
        (length,) = struct.unpack("!H", await reader.readexactly(2))
    elif length == 127:
        (length,) = struct.unpack("!Q", await reader.readexactly(8))
    if length > MAX_FRAME_BYTES:
        raise ProtocolError("frame too large")
    mask = await reader.readexactly(4)
    payload = bytearray(await reader.readexactly(length))
    for index in range(length):
        payload[index] ^= mask[index % 4]
    return fin, opcode, bytes(payload)


def encode_frame(opcode: int, payload: bytes) -> bytes:
    """Encode one unmasked server frame."""
    length = len(payload)
    if length < 126:
        header = struct.pack("!BB", 0x80 | opcode, length)
    elif length < 1 << 16:
        header = struct.pack("!BBH", 0x80 | opcode, 126, length)
    else:
        header = struct.pack("!BBQ", 0x80 | opcode, 127, length)
    return header + payload


async def serve_websocket(
    request: Request,
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    state: ServiceState,
) -> None:
    """Complete the upgrade and echo messages until the client closes."""
    key = request.headers.get("sec-websocket-key")
    upgrade = request.headers.get("upgrade", "").lower()
    if upgrade != "websocket" or not key:
        await respond(writer, "400 Bad Request", b"websocket upgrade required\n", "text/plain")
        return
    accept = base64.b64encode(hashlib.sha1((key + WS_GUID).encode()).digest()).decode()
    writer.write(
        (
            "HTTP/1.1 101 Switching Protocols\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            f"Sec-WebSocket-Accept: {accept}\r\n\r\n"
        ).encode("latin-1")
    )
    await writer.drain()
    state.websocket_sessions += 1
    message_opcode = OP_TEXT
    fragments: list[bytes] = []
    while True:
        fin, opcode, payload = await read_frame(reader)
        if opcode == OP_CLOSE:
            writer.write(encode_frame(OP_CLOSE, payload[:2]))
            await writer.drain()
            return
        if opcode == OP_PING:
            writer.write(encode_frame(OP_PONG, payload))
            await writer.drain()
            continue
        if opcode == OP_PONG:
            continue
        if opcode in (OP_TEXT, OP_BINARY):
            message_opcode = opcode
            fragments = [payload]
        elif opcode == OP_CONTINUATION:
            fragments.append(payload)
        else:
            raise ProtocolError(f"unknown opcode {opcode}")
        if fin:
            state.websocket_messages += 1
            writer.write(encode_frame(message_opcode, b"".join(fragments)))
            await writer.drain()
            fragments = []


def describe(state: ServiceState) -> bytes:
    """Return the JSON body of ``GET /``."""
    return json.dumps(
        {
            "service": "clio-relay-fixture-secret-service",
            "pid": os.getpid(),
            "hostname": socket.gethostname(),
            "uptime_seconds": round(time.time() - state.started_at, 3),
            "requests": state.requests,
            "refused": state.refused,
            "websocket_sessions": state.websocket_sessions,
            "websocket_messages": state.websocket_messages,
        }
    ).encode()


async def handle(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter, state: ServiceState
) -> None:
    """Serve one connection."""
    try:
        request = await read_request(reader)
        state.requests += 1
        if request.method != "GET":
            await respond(writer, "405 Method Not Allowed", b"GET only\n", "text/plain")
        elif request.path == "/health":
            await respond(writer, "200 OK", b"ok\n", "text/plain")
        elif not authorized(request, state):
            state.refused += 1
            await respond(
                writer,
                "401 Unauthorized",
                b"secret required\n",
                "text/plain",
                {"WWW-Authenticate": 'Bearer realm="clio-relay-fixture"'},
            )
        elif request.path == "/":
            await respond(writer, "200 OK", describe(state), "application/json")
        elif request.path == "/ws":
            await serve_websocket(request, reader, writer, state)
        else:
            await respond(writer, "404 Not Found", b"not found\n", "text/plain")
    except (asyncio.IncompleteReadError, ConnectionError, ProtocolError):
        pass
    finally:
        writer.close()
        with contextlib.suppress(ConnectionError):
            await writer.wait_closed()


def write_final_artifact(path: str, state: ServiceState) -> None:
    """Write the termination record a test looks for."""
    record = {
        "service": "clio-relay-fixture-secret-service",
        "stopped_at": time.time(),
        "requests": state.requests,
        "websocket_messages": state.websocket_messages,
    }
    with open(path, "w", encoding="utf-8") as handle_out:
        json.dump(record, handle_out)
        handle_out.write("\n")


async def main_async(args: argparse.Namespace) -> int:
    """Run until a termination signal."""
    secret = os.environ.get(args.secret_env, "")
    if not secret:
        print(f"environment variable {args.secret_env} must hold the secret", file=sys.stderr)
        return 2
    state = ServiceState(secret=secret)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(signum, stop.set)

    async def on_connect(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await handle(reader, writer, state)

    server = await asyncio.start_server(
        on_connect, args.host, args.port, limit=MAX_HEADER_BYTES + 1024
    )
    bound = server.sockets[0].getsockname()
    print(f"listening on {bound[0]}:{bound[1]}", flush=True)
    async with server:
        await stop.wait()
    if args.final_artifact:
        write_final_artifact(args.final_artifact, state)
        print(f"wrote {args.final_artifact}", flush=True)
    return 0


def parse_args(argv: list[str]) -> argparse.Namespace:
    """Parse the command line."""
    parser = argparse.ArgumentParser(
        description="HTTP and WebSocket fixture service that checks a secret."
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--secret-env", default="FIXTURE_SECRET")
    parser.add_argument("--final-artifact", default="")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Entry point."""
    return asyncio.run(main_async(parse_args(sys.argv[1:] if argv is None else argv)))


if __name__ == "__main__":
    raise SystemExit(main())

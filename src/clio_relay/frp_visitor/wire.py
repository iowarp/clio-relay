"""frp's v1 message codec and its AES-128-CFB streams.

A v1 message is a type byte, a big-endian int64 length and a JSON body whose
empty fields are left out, as Go's ``omitempty`` does. After login the control
stream is encrypted with the token, and a visitor connection with
``useEncryption`` with its secret key: AES-128-CFB, with the key from PBKDF2
(SHA-1, salt ``frp``, 64 rounds) and a random IV each way, sent first.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import struct
from typing import Any, cast

from cryptography.hazmat.decrepit.ciphers import modes
from cryptography.hazmat.primitives.ciphers import Cipher, CipherContext, algorithms

from clio_relay.frp_visitor.pipe import CHUNK, BufferedPipe, Pipe, ProtocolError

LOGIN = b"o"
LOGIN_RESP = b"1"
NEW_VISITOR_CONN = b"v"
NEW_VISITOR_CONN_RESP = b"3"
PING = b"h"
PONG = b"4"
KNOWN_TYPES = frozenset(b"o1p2cwrsv3h4uinm56")
"""Every v1 type byte frp 0.69 defines; one outside these is a protocol error."""

MAX_MESSAGE = 10240
_LENGTH = struct.Struct(">q")
_SALT = b"frp"


def auth_key(secret: str, timestamp: int) -> str:
    """Return frp's signature for ``secret`` at ``timestamp``: md5 of both, in hex."""
    return hashlib.md5(f"{secret}{timestamp}".encode()).hexdigest()


def pack(kind: bytes, body: dict[str, Any]) -> bytes:
    """Encode one message, leaving out empty fields as frp's ``omitempty`` does."""
    kept = {key: value for key, value in body.items() if value not in (None, "", 0, {}, [])}
    data = json.dumps(kept, separators=(",", ":")).encode()
    return kind + _LENGTH.pack(len(data)) + data


async def read_message(pipe: BufferedPipe) -> tuple[bytes, dict[str, Any]]:
    """Read one message and return its type byte and body."""
    head = await pipe.readexactly(1 + _LENGTH.size)
    kind = head[:1]
    if kind[0] not in KNOWN_TYPES:
        raise ProtocolError(f"frps sent message type {kind!r}, which frp does not define")
    (length,) = _LENGTH.unpack(head[1:])
    if length < 0 or length > MAX_MESSAGE:
        raise ProtocolError(f"frps sent a message of {length} bytes")
    data = await pipe.readexactly(length) if length else b""
    try:
        body: object = json.loads(data) if data else {}
    except ValueError as exc:
        raise ProtocolError(f"frps sent a message that is not JSON: {exc}") from exc
    if not isinstance(body, dict):
        raise ProtocolError("frps sent a message that is not a JSON object")
    return kind, cast(dict[str, Any], body)


def _cipher(secret: str, iv: bytes) -> Cipher[modes.CFB]:
    key = hashlib.pbkdf2_hmac("sha1", secret.encode(), _SALT, 64, 16)
    return Cipher(algorithms.AES(key), modes.CFB(iv))


class CryptoPipe(Pipe):
    """frp's AES-128-CFB stream over a pipe, keyed by a shared secret."""

    def __init__(self, inner: BufferedPipe, secret: str) -> None:
        self._inner = inner
        self._secret = secret
        self._iv = os.urandom(16)
        self._encryptor: CipherContext = _cipher(secret, self._iv).encryptor()
        self._decryptor: CipherContext | None = None
        self._lock = asyncio.Lock()

    async def read(self, n: int = CHUNK) -> bytes:
        if self._decryptor is None:
            try:
                iv = await self._inner.readexactly(16)
            except asyncio.IncompleteReadError:
                return b""
            self._decryptor = _cipher(self._secret, iv).decryptor()
        data = await self._inner.read(n)
        return self._decryptor.update(data) if data else b""

    async def write(self, data: bytes) -> None:
        async with self._lock:
            out = self._encryptor.update(data)
            if self._iv:
                out, self._iv = self._iv + out, b""
            await self._inner.write(out)

    async def close(self) -> None:
        await self._inner.close()

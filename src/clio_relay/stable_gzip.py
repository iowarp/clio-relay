"""A gzip whose bytes depend only on its content, on any Python.

``gzip.compress`` gives different bytes from different zlib builds (CPython 3.14
on Windows links zlib-ng, older builds zlib), so a content hash of its output
changes with the interpreter. Relay hashes its package archives, so it writes
them with stored deflate blocks instead: RFC 1951 and RFC 1952 fix every byte of
that stream, and every gzip reader (``tar -xzf``, ``tarfile`` ``r:gz``) reads it.
"""

from __future__ import annotations

import struct
import zlib

_BLOCK = 0xFFFF
_HEADER = b"\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\xff"
"""Magic, deflate, no flags, mtime 0, no extra flags, OS unknown."""


def stable_gzip(data: bytes) -> bytes:
    """Return ``data`` as a gzip member of stored blocks: the same bytes everywhere."""
    out = bytearray(_HEADER)
    offset = 0
    while True:
        chunk = data[offset : offset + _BLOCK]
        offset += len(chunk)
        final = offset >= len(data)
        out += bytes((1 if final else 0,))
        out += struct.pack("<HH", len(chunk), len(chunk) ^ 0xFFFF)
        out += chunk
        if final:
            break
    out += struct.pack("<II", zlib.crc32(data) & 0xFFFFFFFF, len(data) & 0xFFFFFFFF)
    return bytes(out)

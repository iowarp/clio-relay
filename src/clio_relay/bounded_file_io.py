"""Bounded, TOCTOU-safe file reads and digest primitives shared across mcp_call.

Split from ``runner.py`` (iowarp/clio-relay#231/#775 decomposition wave 3). Pure
leaf helpers with no dependency on any other mcp_call module --
:mod:`clio_relay.clio_kit_runtime_identity`,
:mod:`clio_relay.python_console_distribution`, and
:mod:`clio_relay.python_external_distribution` all import from here
rather than from each other, keeping that trio acyclic.
"""

from __future__ import annotations

import base64
import hashlib
import os
import stat
from pathlib import Path

from clio_relay.constants import FILE_HASH_CHUNK_BYTES


def is_sha256_text(value: object) -> bool:
    """Return whether a value is one lowercase hexadecimal SHA-256 digest."""
    return (
        isinstance(value, str)
        and len(value) == 64
        and value == value.lower()
        and all(character in "0123456789abcdef" for character in value)
    )


def bounded_regular_file_bytes(path: Path, *, max_bytes: int) -> bytes | None:
    """Read one stable non-link regular file under an explicit byte limit."""
    snapshot = bounded_regular_file_snapshot(path, max_bytes=max_bytes)
    return snapshot[0] if snapshot is not None else None


def bounded_regular_file_snapshot(
    path: Path,
    *,
    max_bytes: int,
) -> tuple[bytes, tuple[int, int, int, int]] | None:
    """Read one stable regular file and return the descriptor identity read."""
    try:
        before = path.lstat()
    except OSError:
        return None
    if (
        stat.S_ISLNK(before.st_mode)
        or not stat.S_ISREG(before.st_mode)
        or before.st_size < 0
        or before.st_size > max_bytes
    ):
        return None
    identity = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
    try:
        with path.open("rb") as stream:
            opened = os.fstat(stream.fileno())
            if (
                opened.st_dev,
                opened.st_ino,
                opened.st_size,
                opened.st_mtime_ns,
            ) != identity:
                return None
            payload = stream.read(max_bytes + 1)
    except OSError:
        return None
    if len(payload) != before.st_size:
        return None
    try:
        after = path.lstat()
    except OSError:
        return None
    if (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns) != identity:
        return None
    return payload, identity


def record_bound_sha256(path: Path, *, expected_size: int) -> str | None:
    """Hash one non-link regular distribution file and reject path replacement races."""
    try:
        before = path.lstat()
    except OSError:
        return None
    attributes = getattr(before, "st_file_attributes", 0)
    reparse = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
    if (
        stat.S_ISLNK(before.st_mode)
        or not stat.S_ISREG(before.st_mode)
        or (reparse and attributes & reparse)
        or before.st_size != expected_size
    ):
        return None
    identity = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
    digest = hashlib.sha256()
    try:
        with path.open("rb") as stream:
            opened = os.fstat(stream.fileno())
            if (
                opened.st_dev,
                opened.st_ino,
                opened.st_size,
                opened.st_mtime_ns,
            ) != identity or not stat.S_ISREG(opened.st_mode):
                return None
            while chunk := stream.read(FILE_HASH_CHUNK_BYTES):
                digest.update(chunk)
    except OSError:
        return None
    try:
        after = path.lstat()
    except OSError:
        return None
    if (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns) != identity:
        return None
    return digest.hexdigest()


def urlsafe_sha256_digest(value: str) -> str | None:
    """Decode an unpadded wheel RECORD SHA-256 value to lowercase hex."""
    try:
        decoded = base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))
    except (ValueError, TypeError):
        return None
    return decoded.hex() if len(decoded) == hashlib.sha256().digest_size else None

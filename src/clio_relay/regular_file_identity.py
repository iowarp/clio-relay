"""Snapshot-verified reads of one regular, non-symlink file (#231).

Extracted from :mod:`clio_relay.validation_report`
(``docs/design/relay-architecture-2026-08.md``, #231). Install-source
detection (the uv-tool receipt and the process-ancestor walk that identifies
the launching ``uv`` executable) needs to hash or read small trusted files
-- a launcher receipt, a ``RECORD`` file, an executable -- without a
TOCTOU gap: the file must still be the exact regular file its identity
snapshot named by the time the read completes. :func:`regular_file_identity`
takes that snapshot (device/inode/size/mtime, never following a symlink or
reparse point); :func:`hash_open_regular_file` and
:func:`read_open_regular_file` open the path, confirm the *opened* handle's
identity still matches, and only then hash or return its bytes -- re-checking
the identity once more afterward closes the window between the read and the
final confirmation.
"""

from __future__ import annotations

import hashlib
import os
import stat
from pathlib import Path


def regular_file_identity(path: Path) -> tuple[int, int, int, int] | None:
    """Return a stable identity only for a non-link, non-reparse regular file."""
    try:
        details = path.lstat()
    except OSError:
        return None
    file_attributes = getattr(details, "st_file_attributes", 0)
    reparse_attribute = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
    if stat.S_ISLNK(details.st_mode) or not stat.S_ISREG(details.st_mode):
        return None
    if reparse_attribute and file_attributes & reparse_attribute:
        return None
    return (details.st_dev, details.st_ino, details.st_size, details.st_mtime_ns)


def hash_open_regular_file(
    path: Path,
    expected_identity: tuple[int, int, int, int] | None,
) -> str | None:
    """Hash a regular file while confirming the opened handle matches its path snapshot."""
    if expected_identity is None:
        return None
    digest = hashlib.sha256()
    try:
        with path.open("rb") as stream:
            opened = os.fstat(stream.fileno())
            opened_identity = (
                opened.st_dev,
                opened.st_ino,
                opened.st_size,
                opened.st_mtime_ns,
            )
            if opened_identity != expected_identity or not stat.S_ISREG(opened.st_mode):
                return None
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError:
        return None
    if regular_file_identity(path) != expected_identity:
        return None
    return digest.hexdigest()


def read_open_regular_file(
    path: Path,
    expected_identity: tuple[int, int, int, int],
    *,
    maximum_bytes: int,
) -> bytes | None:
    """Read one path-anchored regular file with a strict byte ceiling."""
    try:
        with path.open("rb") as stream:
            opened = os.fstat(stream.fileno())
            opened_identity = (
                opened.st_dev,
                opened.st_ino,
                opened.st_size,
                opened.st_mtime_ns,
            )
            if opened_identity != expected_identity or not stat.S_ISREG(opened.st_mode):
                return None
            content = stream.read(maximum_bytes + 1)
    except OSError:
        return None
    if len(content) > maximum_bytes or regular_file_identity(path) != expected_identity:
        return None
    return content

"""Identity resolution and re-verification for the packaged relay executable.

Extracted from :mod:`clio_relay.mcp_stdio_validation` (file-size
decomposition; see ``scripts/check_file_size.py``). This module owns the
one concern of pinning down, hashing, and re-checking the EXACT installed
``clio-relay`` binary that packaged stdio validation is about to exec --
independent of the subprocess lifecycle that runs it (``mcp_stdio_validation_
process.py``) and the MCP transcript that binary is expected to speak
(``mcp_stdio_validation_contract.py``). Every name here is a private helper
with no external callers (confirmed by grep across ``src/`` and ``tests/``
before the move); :mod:`clio_relay.mcp_stdio_validation` imports them
directly rather than re-exporting.
"""

from __future__ import annotations

import hashlib
import os
import shutil
import stat
from dataclasses import dataclass
from pathlib import Path

from clio_relay.errors import RelayError

_VALIDATION_EXECUTABLE_ENV = "CLIO_RELAY_VALIDATION_TOOL_EXECUTABLE"
_MAX_EXECUTABLE_BYTES = 128 * 1024 * 1024


@dataclass(frozen=True)
class _ExecutableIdentity:
    """Stable identity for the exact launcher selected before process creation."""

    configured_path: Path
    canonical_path: Path
    configured_lstat: tuple[int, int, int, int]
    target_stat: tuple[int, int, int, int]
    sha256: str


def _resolve_packaged_executable() -> _ExecutableIdentity:
    configured = os.environ.get(_VALIDATION_EXECUTABLE_ENV)
    selected = configured if configured is not None else shutil.which("clio-relay")
    if selected is None:
        raise RelayError(
            "packaged clio-relay executable is unavailable; install the exact wheel as a "
            "persistent uv tool before running validation"
        )
    configured_path = Path(selected).expanduser()
    if configured is not None and not configured_path.is_absolute():
        raise RelayError(f"{_VALIDATION_EXECUTABLE_ENV} must name an absolute executable path")
    configured_path = Path(os.path.abspath(configured_path))
    try:
        configured_lstat = configured_path.lstat()
        canonical_path = configured_path.resolve(strict=True)
        target_stat = canonical_path.stat()
    except OSError as exc:
        raise RelayError("configured packaged clio-relay executable could not be verified") from exc
    if not stat.S_ISREG(target_stat.st_mode):
        raise RelayError("configured packaged clio-relay executable is not a regular file")
    if os.name != "nt" and not os.access(canonical_path, os.X_OK):
        raise RelayError("configured packaged clio-relay executable is not executable")
    if target_stat.st_size > _MAX_EXECUTABLE_BYTES:
        raise RelayError("configured packaged clio-relay executable exceeded its byte limit")
    return _ExecutableIdentity(
        configured_path=configured_path,
        canonical_path=canonical_path,
        configured_lstat=_stat_identity(configured_lstat),
        target_stat=_stat_identity(target_stat),
        sha256=_hash_regular_file(canonical_path, expected=_stat_identity(target_stat)),
    )


def _verify_executable_unchanged(executable: _ExecutableIdentity) -> None:
    try:
        configured_lstat = _stat_identity(executable.configured_path.lstat())
        canonical_path = executable.configured_path.resolve(strict=True)
        target_stat = _stat_identity(canonical_path.stat())
    except OSError as exc:
        raise RelayError("packaged clio-relay executable changed during validation") from exc
    if (
        configured_lstat != executable.configured_lstat
        or canonical_path != executable.canonical_path
        or target_stat != executable.target_stat
        or _hash_regular_file(canonical_path, expected=target_stat) != executable.sha256
    ):
        raise RelayError("packaged clio-relay executable changed during validation")


def _hash_regular_file(path: Path, *, expected: tuple[int, int, int, int]) -> str:
    digest = hashlib.sha256()
    remaining = expected[2]
    try:
        with path.open("rb") as stream:
            opened = os.fstat(stream.fileno())
            if _stat_identity(opened) != expected or not stat.S_ISREG(opened.st_mode):
                raise RelayError("configured packaged clio-relay executable identity changed")
            while remaining > 0:
                chunk = stream.read(min(1024 * 1024, remaining))
                if not chunk:
                    raise RelayError("configured packaged clio-relay executable identity changed")
                digest.update(chunk)
                remaining -= len(chunk)
            if stream.read(1):
                raise RelayError("configured packaged clio-relay executable identity changed")
        final_identity = _stat_identity(path.stat())
    except OSError as exc:
        raise RelayError("configured packaged clio-relay executable could not be read") from exc
    if final_identity != expected:
        raise RelayError("configured packaged clio-relay executable identity changed")
    return digest.hexdigest()


def _stat_identity(value: os.stat_result) -> tuple[int, int, int, int]:
    return (int(value.st_dev), int(value.st_ino), int(value.st_size), int(value.st_mtime_ns))

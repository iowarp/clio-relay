"""Zero-dependency filesystem/identity primitives for bootstrap reconciliation.

Content hashing, bounded reads, atomic writes, and stat-identity comparison
used across every other ``bootstrap_reconcile_*`` owner module. This module
depends only on :mod:`clio_relay.bootstrap_reconcile_constants` -- never on
the pydantic models -- so it stays the acyclic base of the split's import
graph (iowarp/clio-relay#255).
"""

from __future__ import annotations

import ctypes
import hashlib
import json
import os
import stat
import sys
from contextlib import suppress
from pathlib import Path
from typing import cast

import yaml

from clio_relay.bootstrap_reconcile_constants import (
    _AT_FDCWD,
    _O_BINARY,
    _O_NOFOLLOW,
    _RENAME_EXCHANGE,
)
from clio_relay.errors import ConfigurationError


def canonical_json_sha256(value: object) -> str:
    """Hash one JSON value using the deployment contract's canonical form."""
    encoded = json.dumps(
        value,
        ensure_ascii=True,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _is_sha256(value: object) -> bool:
    return bool(
        isinstance(value, str)
        and len(value) == 64
        and value == value.lower()
        and all(character in "0123456789abcdef" for character in value)
    )


def _path_is_directory_alias(path: Path) -> bool:
    """Return whether a directory path is a symbolic-link or junction alias."""
    if path.is_symlink():
        return True
    is_junction = getattr(path, "is_junction", None)
    return bool(is_junction()) if callable(is_junction) else False


def _expand_home(value: str, home: Path) -> Path:
    if value == "~":
        return home
    if value.startswith("~/"):
        return home / value[2:]
    path = Path(value)
    if not path.is_absolute():
        raise ConfigurationError(f"bootstrap state path is not absolute: {value}")
    return path


def _canonical_path_preserving_final(path: Path) -> Path:
    """Canonicalize ancestor aliases without following the final path component."""
    lexical = Path(os.path.abspath(path.expanduser()))
    if any(character in str(lexical) for character in "\x00\r\n"):
        raise ConfigurationError("managed path contains unsafe characters")
    try:
        parent = lexical.parent.resolve(strict=True)
    except (OSError, RuntimeError, ValueError) as exc:
        raise ConfigurationError("managed path parent is unavailable") from exc
    if not parent.is_dir():  # pragma: no cover - resolve(strict=True) normally proves this
        raise ConfigurationError("managed path parent is not a directory")
    return parent / lexical.name


def _yaml_mapping(raw: bytes, *, label: str) -> dict[str, object]:
    try:
        value = cast(object, yaml.safe_load(raw.decode("utf-8")))
    except (UnicodeError, yaml.YAMLError) as exc:
        raise ConfigurationError(f"{label} is invalid") from exc
    typed_value = cast(dict[object, object], value) if isinstance(value, dict) else {}
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in typed_value):
        raise ConfigurationError(f"{label} must contain one string-keyed mapping")
    return cast(dict[str, object], value)


def _read_regular_bounded(path: Path, *, maximum: int) -> bytes:
    raw, _identity = _read_regular_bounded_with_identity(path, maximum=maximum)
    return raw


def _read_regular_bounded_with_identity(
    path: Path,
    *,
    maximum: int,
) -> tuple[bytes, tuple[int, int, int, int, int, int]]:
    """Read one bounded regular file and retain its stable filesystem identity."""
    descriptor: int | None = None
    try:
        flags = os.O_RDONLY | _O_BINARY | _O_NOFOLLOW
        descriptor = os.open(path, flags)
        before = os.fstat(descriptor)
        linked = path.lstat()
        if not stat.S_ISREG(before.st_mode) or before.st_size < 1 or before.st_size > maximum:
            raise ConfigurationError(f"state file is not one bounded regular file: {path}")
        if _cross_handle_stat_identity(before) != _cross_handle_stat_identity(linked):
            raise ConfigurationError(f"state file path changed while it was opened: {path}")
        chunks: list[bytes] = []
        remaining = maximum + 1
        while remaining > 0:
            chunk = os.read(descriptor, min(1024 * 1024, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        raw = b"".join(chunks)
        after = os.fstat(descriptor)
        linked_after = path.lstat()
    except OSError as exc:
        raise ConfigurationError(f"could not read state file: {path}") from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)
    if (
        len(raw) != before.st_size
        or len(raw) > maximum
        or _stat_identity(before) != _stat_identity(after)
        or _cross_handle_stat_identity(before) != _cross_handle_stat_identity(linked_after)
    ):
        raise ConfigurationError(f"state file changed while it was inspected: {path}")
    return raw, _stat_identity(before)


def _read_bounded(path: Path, *, maximum: int) -> str:
    return _read_regular_bounded(path, maximum=maximum).decode("utf-8")


def _stat_identity(value: os.stat_result) -> tuple[int, int, int, int, int, int]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


def _cross_handle_stat_identity(value: os.stat_result) -> tuple[int, ...]:
    """Return fields stable across descriptor/path stat handles on this platform."""
    if os.name == "nt":
        # Windows may report ctime_ns with different rounding and synthesize
        # execute permission bits from a path's extension only for lstat.
        # Device/inode and file type still bind the file object; size and mtime
        # retain change detection across descriptor and path handles.
        return (
            value.st_dev,
            value.st_ino,
            stat.S_IFMT(value.st_mode),
            value.st_size,
            value.st_mtime_ns,
        )
    return _stat_identity(value)


def _atomic_exchange_paths(left: Path, right: Path) -> None:
    """Atomically exchange two existing pathnames without dropping either object."""
    if sys.platform == "linux":
        library = ctypes.CDLL(None, use_errno=True)
        renameat2 = library.renameat2
        renameat2.argtypes = (
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_uint,
        )
        renameat2.restype = ctypes.c_int
        result = renameat2(
            _AT_FDCWD,
            os.fsencode(left),
            _AT_FDCWD,
            os.fsencode(right),
            _RENAME_EXCHANGE,
        )
        if result != 0:
            error = ctypes.get_errno()
            raise OSError(error, os.strerror(error), f"{left} <-> {right}")
        return
    if os.name != "nt":
        raise ConfigurationError("atomic bootstrap path exchange requires Linux")
    # The staged bootstrap runs on Linux. This fallback keeps the path contract
    # testable on Windows without weakening the supported cluster operation.
    holding = right.with_name(f".{right.name}.{os.getpid()}.exchange")
    if holding.exists() or holding.is_symlink():
        raise ConfigurationError(f"atomic exchange holding path already exists: {holding}")
    os.replace(right, holding)
    try:
        os.replace(left, right)
    except BaseException:
        os.replace(holding, right)
        raise
    os.replace(holding, left)


def _identity_matches_after_rename(
    before: tuple[int, int, int, int, int, int],
    after: tuple[int, int, int, int, int, int],
) -> bool:
    """Compare file identity while excluding ctime, which rename changes on Linux."""
    return before[:5] == after[:5]


def verify_atomic_exchange_support(
    directories: tuple[Path, ...],
    *,
    identity: str,
) -> dict[str, object]:
    """Exercise and restore atomic exchange on every staged-mutation filesystem."""
    try:
        _require_sha256(identity, field="exchange_preflight_identity")
    except ValueError as exc:
        raise ConfigurationError(str(exc)) from exc
    verified: list[str] = []
    seen: set[Path] = set()
    for raw_directory in directories:
        directory = Path(os.path.abspath(raw_directory.expanduser()))
        try:
            details = directory.lstat()
            resolved = directory.resolve(strict=True)
        except (OSError, RuntimeError, ValueError) as exc:
            raise ConfigurationError(
                f"atomic exchange preflight directory is unavailable: {directory}"
            ) from exc
        if directory.is_symlink() or not stat.S_ISDIR(details.st_mode):
            raise ConfigurationError(
                f"atomic exchange preflight path is not one directory: {directory}"
            )
        if resolved in seen:
            continue
        seen.add(resolved)
        left = directory / f".clio-relay-exchange-{identity}.left"
        right = directory / f".clio-relay-exchange-{identity}.right"
        left_payload = f"left:{identity}\n".encode("ascii")
        right_payload = f"right:{identity}\n".encode("ascii")
        if left.exists() or left.is_symlink() or right.exists() or right.is_symlink():
            try:
                observed = {
                    _read_regular_bounded(left, maximum=256),
                    _read_regular_bounded(right, maximum=256),
                }
            except ConfigurationError as exc:
                raise ConfigurationError(
                    f"atomic exchange preflight recovery is unproven: {directory}"
                ) from exc
            if observed != {left_payload, right_payload}:
                raise ConfigurationError(
                    f"atomic exchange preflight recovery is unproven: {directory}"
                )
            left.unlink()
            right.unlink()
            _fsync_directory(directory)
        try:
            for path, payload in ((left, left_payload), (right, right_payload)):
                with path.open("xb") as stream:
                    os.chmod(path, 0o600)
                    stream.write(payload)
                    stream.flush()
                    os.fsync(stream.fileno())
            _fsync_directory(directory)
            _atomic_exchange_paths(left, right)
            if (
                _read_regular_bounded(left, maximum=256) != right_payload
                or _read_regular_bounded(right, maximum=256) != left_payload
            ):
                raise ConfigurationError(
                    f"atomic exchange preflight produced invalid state: {directory}"
                )
            _atomic_exchange_paths(left, right)
            if (
                _read_regular_bounded(left, maximum=256) != left_payload
                or _read_regular_bounded(right, maximum=256) != right_payload
            ):
                raise ConfigurationError(
                    f"atomic exchange preflight did not restore state: {directory}"
                )
            left.unlink()
            right.unlink()
            _fsync_directory(directory)
        except BaseException:
            with suppress(OSError):
                left.unlink(missing_ok=True)
            with suppress(OSError):
                right.unlink(missing_ok=True)
            _fsync_directory(directory)
            raise
        verified.append(str(directory))
    return {
        "schema_version": "clio-relay.atomic-exchange-preflight.v1",
        "identity": identity,
        "directories": verified,
    }


def _atomic_json(path: Path, value: object) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    payload = json.dumps(value, indent=2, sort_keys=True, default=str) + "\n"
    try:
        with temporary.open("x", encoding="utf-8", newline="\n") as stream:
            os.chmod(temporary, 0o600)
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    except BaseException:
        with suppress(OSError):
            temporary.unlink(missing_ok=True)
        raise


def _fsync_directory(path: Path) -> None:
    if os.name == "nt":
        return
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _require_sha256(value: object, *, field: str) -> None:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or value != value.lower()
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ValueError(f"{field} must contain one lowercase SHA-256 digest")

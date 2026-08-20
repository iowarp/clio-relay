"""Filesystem-safety primitives for the storage policy's state root.

Two closely related layers live here. The identity layer (``_is_link_or_
reparse``, ``_safe_lstat``, ``_require_positive_bound``, ``_volume_id``) is
the small, unpatched building block the bounded tree scan and the volume-
health assembly both depend on for a link-rejecting stat and per-OS volume
grouping. The durable-I/O layer built on top of it -- bounded, ownership-
verified reads of the private ledger/lock files, the lock-file bootstrap,
the state-root directory-chain hardening (owner-private, no links, no
writable-by-others-without-sticky ancestor), and the non-overlapping-roots
guard -- is the other half of ``storage_ledger_codec.py``'s content rules:
together they are the full durable half of the storage policy. The atomic
``os.replace`` swap itself (``_replace_file``) stays in ``storage_policy.py``
because it is a directly monkeypatched test seam (see that module's
docstring) called from ``StoragePolicy._write_ledger``, which stays there
with it.
"""

# Every function here is a leaf primitive called only from the other owner
# modules composing StoragePolicy (storage_policy.py, storage_snapshot.py,
# storage_reservation_ledger.py) or from tests -- never from within this
# file -- matching http_api.py's own decorator-only-caller precedent.
# pyright: reportUnusedFunction=false

from __future__ import annotations

import os
import stat
from contextlib import suppress
from pathlib import Path
from typing import Final

from clio_relay.filesystem_paths import internal_filesystem_path, logical_filesystem_path
from clio_relay.storage_policy_types import StoragePolicyError, StorageReason

_FILE_ATTRIBUTE_REPARSE_POINT: Final = 0x400


def _is_link_or_reparse(result: os.stat_result) -> bool:
    attributes = int(getattr(result, "st_file_attributes", 0) or 0)
    return stat.S_ISLNK(result.st_mode) or bool(attributes & _FILE_ATTRIBUTE_REPARSE_POINT)


def _safe_lstat(path: Path, *, root: bool) -> os.stat_result:
    logical_path = logical_filesystem_path(path)
    path = internal_filesystem_path(path, force_extended=True)
    try:
        result = os.lstat(path)
    except OSError as exc:
        raise StoragePolicyError(
            StorageReason.SCAN_ROOT_INVALID if root else StorageReason.SCAN_CHANGED,
            "storage path could not be inspected",
            details={"root": str(logical_path), "error": type(exc).__name__},
        ) from exc
    if _is_link_or_reparse(result):
        raise StoragePolicyError(
            StorageReason.SCAN_ROOT_INVALID if root else StorageReason.SCAN_UNSAFE_ENTRY,
            "storage path is a link or reparse point",
            details={"root": str(logical_path)},
        )
    return result


def _require_positive_bound(name: str, value: int) -> None:
    if type(value) is not int or value <= 0:
        raise ValueError(f"{name} must be a positive integer")


def _volume_id(path: Path, result: os.stat_result) -> str:
    if os.name == "nt":
        drive = os.path.splitdrive(str(logical_filesystem_path(path)))[0].casefold()
        return f"volume:{drive}:{int(result.st_dev)}"
    return f"device:{int(result.st_dev)}"


def _bounded_read_regular_file(path: Path, max_bytes: int) -> bytes:
    path = internal_filesystem_path(path, force_extended=True)
    before = _validate_private_regular_file(path, allow_empty=False)
    if before.st_size > max_bytes:
        raise StoragePolicyError(
            StorageReason.LEDGER_OVERSIZED,
            "storage reservation ledger exceeds its configured byte limit",
            details={"max_ledger_bytes": max_bytes},
        )
    flags = os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise StoragePolicyError(
            StorageReason.LEDGER_UNSAFE,
            "storage reservation ledger could not be opened safely",
            details={"error": type(exc).__name__},
        ) from exc
    try:
        opened = os.fstat(descriptor)
        if (
            not stat.S_ISREG(opened.st_mode)
            or opened.st_nlink != 1
            or int(opened.st_dev) != int(before.st_dev)
            or int(opened.st_ino) != int(before.st_ino)
        ):
            raise StoragePolicyError(
                StorageReason.LEDGER_UNSAFE,
                "storage reservation ledger identity changed while opening",
            )
        chunks: list[bytes] = []
        remaining = max_bytes + 1
        while remaining > 0:
            chunk = os.read(descriptor, min(64 * 1024, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        raw = b"".join(chunks)
        after = os.fstat(descriptor)
        if (
            int(after.st_dev) != int(opened.st_dev)
            or int(after.st_ino) != int(opened.st_ino)
            or int(after.st_size) != len(raw)
        ):
            raise StoragePolicyError(
                StorageReason.LEDGER_UNSAFE,
                "storage reservation ledger changed while reading",
            )
        if len(raw) > max_bytes:
            raise StoragePolicyError(
                StorageReason.LEDGER_OVERSIZED,
                "storage reservation ledger exceeds its configured byte limit",
                details={"max_ledger_bytes": max_bytes},
            )
        return raw
    finally:
        os.close(descriptor)


def _validate_private_regular_file(path: Path, *, allow_empty: bool) -> os.stat_result:
    path = internal_filesystem_path(path, force_extended=True)
    try:
        result = os.lstat(path)
    except OSError as exc:
        raise StoragePolicyError(
            StorageReason.LEDGER_UNSAFE,
            "storage policy state file could not be inspected",
            details={"error": type(exc).__name__},
        ) from exc
    if _is_link_or_reparse(result) or not stat.S_ISREG(result.st_mode) or result.st_nlink != 1:
        raise StoragePolicyError(
            StorageReason.LEDGER_UNSAFE,
            "storage policy state file is not a private regular file",
        )
    if os.name != "nt" and (result.st_uid != os.geteuid() or stat.S_IMODE(result.st_mode) & 0o077):
        raise StoragePolicyError(
            StorageReason.LEDGER_UNSAFE,
            "storage policy state file is not owner-private",
        )
    if not allow_empty and result.st_size == 0:
        raise StoragePolicyError(
            StorageReason.LEDGER_MALFORMED,
            "storage reservation ledger is empty",
        )
    return result


def _prepare_lock_file(path: Path) -> None:
    path = internal_filesystem_path(path, force_extended=True)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_BINARY", 0)
    try:
        descriptor = os.open(path, flags, 0o600)
    except FileExistsError:
        _validate_private_regular_file(path, allow_empty=True)
        return
    except OSError as exc:
        raise StoragePolicyError(
            StorageReason.LEDGER_UNSAFE,
            "storage reservation lock file could not be created safely",
            details={"error": type(exc).__name__},
        ) from exc
    os.close(descriptor)
    with suppress(OSError):
        # Windows ACLs do not implement POSIX mode bits; file identity remains checked.
        os.chmod(path, 0o600)
    _validate_private_regular_file(path, allow_empty=True)


def _fsync_directory(path: Path) -> None:
    path = internal_filesystem_path(path, force_extended=True)
    if os.name == "nt":
        return
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError:
        return
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _reject_overlapping_roots(first: Path, second: Path) -> None:
    first_key = os.path.normcase(os.path.abspath(first))
    second_key = os.path.normcase(os.path.abspath(second))
    try:
        common = os.path.commonpath((first_key, second_key))
    except ValueError:
        return
    if common in {first_key, second_key}:
        raise ValueError("core_root and spool_root must be distinct, non-nested trees")


def _ensure_directory_no_links(path: Path) -> None:
    path = internal_filesystem_path(path, force_extended=True)
    missing: list[Path] = []
    cursor = path
    while not cursor.exists():
        missing.append(cursor)
        parent = cursor.parent
        if parent == cursor:
            break
        cursor = parent
    if not cursor.exists():
        raise StoragePolicyError(
            StorageReason.LEDGER_UNSAFE,
            "storage policy state root has no existing filesystem ancestor",
        )
    existing = _safe_lstat(cursor, root=True)
    if not stat.S_ISDIR(existing.st_mode):
        raise StoragePolicyError(
            StorageReason.LEDGER_UNSAFE,
            "storage policy state root ancestor is not a directory",
        )
    _validate_state_parent_security(cursor)
    for component in reversed(missing):
        with suppress(FileExistsError):
            component.mkdir(mode=0o700)
        result = _safe_lstat(component, root=True)
        if not stat.S_ISDIR(result.st_mode):
            raise StoragePolicyError(
                StorageReason.LEDGER_UNSAFE,
                "storage policy state path is not a directory",
            )
        if os.name != "nt":
            try:
                os.chmod(component, 0o700)
            except OSError as exc:
                raise StoragePolicyError(
                    StorageReason.LEDGER_UNSAFE,
                    "storage policy state directory could not be made owner-private",
                ) from exc
    final = _safe_lstat(path, root=True)
    if not stat.S_ISDIR(final.st_mode):
        raise StoragePolicyError(
            StorageReason.LEDGER_UNSAFE,
            "storage policy state root is not a directory",
        )
    if os.name != "nt":
        try:
            os.chmod(path, 0o700)
        except OSError as exc:
            raise StoragePolicyError(
                StorageReason.LEDGER_UNSAFE,
                "storage policy state root could not be made owner-private",
            ) from exc
        final = _safe_lstat(path, root=True)
        if final.st_uid != os.geteuid() or stat.S_IMODE(final.st_mode) != 0o700:
            raise StoragePolicyError(
                StorageReason.LEDGER_UNSAFE,
                "storage policy state root is not owner-private",
            )
        _validate_state_parent_security(path.parent)


def _validate_state_parent_security(path: Path) -> None:
    if os.name == "nt":
        return
    result = _safe_lstat(path, root=True)
    if not stat.S_ISDIR(result.st_mode):
        raise StoragePolicyError(
            StorageReason.LEDGER_UNSAFE,
            "storage policy state parent is not a directory",
        )
    mode = stat.S_IMODE(result.st_mode)
    writable_by_others = bool(mode & 0o022)
    sticky = bool(mode & stat.S_ISVTX)
    if writable_by_others and not sticky:
        raise StoragePolicyError(
            StorageReason.LEDGER_UNSAFE,
            "storage policy state parent permits unprotected replacement",
        )

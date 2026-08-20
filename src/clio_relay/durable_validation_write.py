"""Durably write a validation report through a pinned directory ancestry (#231).

Extracted from :mod:`clio_relay.validation_report`
(``docs/design/relay-architecture-2026-08.md``, #231). This is the top-level
orchestration for a durable validation write:
:func:`durably_ensure_validation_directory` walks from the requested path up
to its nearest existing, verified-real ancestor, then creates every missing
directory back down through :mod:`clio_relay.validation_directory_windows`
(Windows) or :func:`create_posix_validation_directory_child` (POSIX,
``dir_fd``-anchored so every step stays below the parent that was just
proven real), re-verifying identity at every step so nothing downstream ever
trusts an unproven path component. :func:`atomic_write_text` is the public
entry point a report/lock-result write calls: it takes
:mod:`clio_relay.validation_writer_lock`'s parent-wide lock, prunes stale
crash-recovery ``.pending`` files, writes the new content to a fresh
``.pending`` file (or reuses an exact byte-identical one already there), and
atomically replaces the target -- re-reading the replaced file's bytes and
identity afterward so a caller never returns believing a write succeeded
when the filesystem silently did something else.
"""

from __future__ import annotations

import ctypes
import hashlib
import os
import stat
from collections.abc import Callable
from contextlib import suppress
from pathlib import Path
from typing import Any, cast

from clio_relay.cluster_config import (
    open_private_atomic_file,
    open_private_configuration_windows_descriptor,
)
from clio_relay.filesystem_paths import internal_filesystem_path, logical_filesystem_path
from clio_relay.validation_directory_windows import (
    WindowsValidationDirectoryAnchor,
    close_windows_validation_directory,
    create_windows_validation_directory_child,
    open_windows_validation_directory,
    verify_windows_validation_directory,
)
from clio_relay.validation_limits import MAX_VALIDATION_REPORT_WRITE_BYTES
from clio_relay.validation_writer_lock import (
    ValidationWriterLock,
    acquire_validation_writer_lock,
    prune_stale_validation_pending_files,
    release_validation_writer_lock,
    verify_validation_writer_lock_parent,
)


def verify_posix_validation_directory(directory_fd: int, path: Path) -> None:
    opened = os.fstat(directory_fd)
    linked = os.stat(path, follow_symlinks=False)
    resolved = path.resolve(strict=True)
    if not (
        stat.S_ISDIR(opened.st_mode)
        and stat.S_ISDIR(linked.st_mode)
        and not stat.S_ISLNK(linked.st_mode)
        and os.path.samestat(opened, linked)
        and os.path.normcase(str(resolved)) == os.path.normcase(str(path))
    ):
        raise OSError(f"validation report directory path changed: {path}")


def create_posix_validation_directory_child(
    parent_fd: int,
    child_name: str,
) -> int:
    """Create and pin one owner-private child through a retained POSIX dirfd."""
    if Path(child_name).name != child_name or child_name in {".", ".."}:
        raise OSError(f"unsafe validation report directory component: {child_name}")
    child_fd: int | None = None
    created = False
    platform_os = cast(Any, os)
    fchmod = cast(Callable[[int, int], None], platform_os.fchmod)
    geteuid = cast(Callable[[], int], platform_os.geteuid)
    try:
        os.mkdir(child_name, 0o700, dir_fd=parent_fd)
        created = True
        child_fd = os.open(
            child_name,
            os.O_RDONLY
            | getattr(os, "O_DIRECTORY", 0)
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_CLOEXEC", 0),
            dir_fd=parent_fd,
        )
        fchmod(child_fd, 0o700)
        opened = os.fstat(child_fd)
        linked = os.stat(child_name, dir_fd=parent_fd, follow_symlinks=False)
        if not (
            stat.S_ISDIR(opened.st_mode)
            and stat.S_ISDIR(linked.st_mode)
            and opened.st_uid == geteuid()
            and linked.st_uid == geteuid()
            and stat.S_IMODE(opened.st_mode) == 0o700
            and stat.S_IMODE(linked.st_mode) == 0o700
            and os.path.samestat(opened, linked)
        ):
            raise OSError(f"validation report directory child is not owner-private: {child_name}")
        os.fsync(child_fd)
        os.fsync(parent_fd)
        result = child_fd
        child_fd = None
        return result
    except BaseException:
        if child_fd is not None:
            os.close(child_fd)
        if created:
            with suppress(OSError):
                os.rmdir(child_name, dir_fd=parent_fd)
                os.fsync(parent_fd)
        raise


def durably_ensure_validation_directory(path: Path) -> None:
    """Create missing ancestry relative to pinned parents and persist every entry."""
    requested = Path(os.path.abspath(path))
    missing: list[Path] = []
    cursor = requested
    while True:
        try:
            existing = os.stat(cursor, follow_symlinks=False)
        except FileNotFoundError:
            missing.append(cursor)
            parent = cursor.parent
            if parent == cursor:  # pragma: no cover - filesystem root must exist
                raise OSError(
                    f"validation report directory root is unavailable: {cursor}"
                ) from None
            cursor = parent
            continue
        if (
            not stat.S_ISDIR(existing.st_mode)
            or stat.S_ISLNK(existing.st_mode)
            or getattr(existing, "st_file_attributes", 0) & 0x00000400
        ):
            raise OSError(f"validation report ancestor is not a real directory: {cursor}")
        resolved = cursor.resolve(strict=True)
        if os.path.normcase(str(resolved)) != os.path.normcase(str(cursor)):
            raise OSError(
                f"validation report ancestor traverses a symlink or reparse point: {cursor}"
            )
        break

    if os.name == "nt":
        anchor = open_windows_validation_directory(cursor, expected_status=existing)
        try:
            for directory in reversed(missing):
                child_anchor = create_windows_validation_directory_child(
                    anchor,
                    directory.name,
                )
                try:
                    verify_windows_validation_directory(anchor)
                    verify_windows_validation_directory(child_anchor)
                    resolved = directory.resolve(strict=True)
                    if os.path.normcase(str(resolved)) != os.path.normcase(str(directory)):
                        raise OSError(
                            "validation report directory traverses a Windows reparse point: "
                            f"{directory}"
                        )
                except BaseException:
                    close_windows_validation_directory(child_anchor)
                    raise
                close_windows_validation_directory(anchor)
                anchor = child_anchor
            verify_windows_validation_directory(anchor)
        finally:
            close_windows_validation_directory(anchor)
        return

    directory_fd = os.open(
        cursor,
        os.O_RDONLY
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_CLOEXEC", 0),
    )
    try:
        if not os.path.samestat(existing, os.fstat(directory_fd)):
            raise OSError(f"validation report ancestor changed while pinning: {cursor}")
        verify_posix_validation_directory(directory_fd, cursor)
        for directory in reversed(missing):
            child_fd = create_posix_validation_directory_child(
                directory_fd,
                directory.name,
            )
            try:
                verify_posix_validation_directory(directory_fd, cursor)
                verify_posix_validation_directory(child_fd, directory)
            except BaseException:
                os.close(child_fd)
                raise
            os.close(directory_fd)
            directory_fd = child_fd
            cursor = directory
        verify_posix_validation_directory(directory_fd, requested)
    finally:
        os.close(directory_fd)


def atomic_write_text(path: Path, text: str) -> None:
    """Serialize and durably replace one validation text file."""
    logical_path = logical_filesystem_path(path)
    requested_parent = logical_path.parent.absolute()
    durably_ensure_validation_directory(requested_parent)
    resolved_parent = requested_parent.resolve(strict=True)
    if os.path.normcase(str(resolved_parent)) != os.path.normcase(str(requested_parent)):
        raise OSError(
            "validation report parent cannot traverse a symlink or reparse point: "
            f"{requested_parent}"
        )
    writer_lock = acquire_validation_writer_lock(resolved_parent)
    try:
        atomic_write_text_locked(path, text, writer_lock=writer_lock)
    finally:
        release_validation_writer_lock(writer_lock)


def atomic_write_text_locked(
    path: Path,
    text: str,
    *,
    writer_lock: ValidationWriterLock,
) -> None:
    """Durably replace one text file through a pinned, revalidated parent."""
    logical_path = logical_filesystem_path(path)
    requested_parent = logical_path.parent.absolute()
    parent_status = verify_validation_writer_lock_parent(writer_lock, requested_parent)
    resolved_parent = writer_lock.path.parent
    logical_path = resolved_parent / logical_path.name
    storage_path = internal_filesystem_path(logical_path, force_extended=True)
    payload = text.encode("utf-8")
    if len(payload) > MAX_VALIDATION_REPORT_WRITE_BYTES:
        raise OSError(f"validation report exceeds {MAX_VALIDATION_REPORT_WRITE_BYTES} bytes")
    if not stat.S_ISDIR(parent_status.st_mode) or stat.S_ISLNK(parent_status.st_mode):
        raise OSError(f"validation report parent is not a real directory: {storage_path.parent}")
    pending_identity = hashlib.sha256(storage_path.name.encode("utf-8")).hexdigest()[:32]
    temporary_name = f".clio-validation-{pending_identity}.pending"
    prune_stale_validation_pending_files(
        resolved_parent,
        current_name=temporary_name,
        writer_lock=writer_lock,
    )

    if os.name == "posix":
        if writer_lock.parent_fd is None:  # pragma: no cover - platform invariant
            raise OSError("validation writer lock omitted its POSIX parent descriptor")
        directory_fd = os.dup(writer_lock.parent_fd)
        output_fd: int | None = None
        try:
            if not os.path.samestat(
                os.fstat(writer_lock.parent_fd),
                os.fstat(directory_fd),
            ):
                raise OSError("validation report parent differs from its writer lock")
            pending_exact = False
            pending_fd: int | None = None
            with suppress(FileNotFoundError):
                pending_fd = os.open(
                    temporary_name,
                    os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0),
                    dir_fd=directory_fd,
                )
            if pending_fd is not None:
                try:
                    pending_opened = os.fstat(pending_fd)
                    pending_linked = os.stat(
                        temporary_name,
                        dir_fd=directory_fd,
                        follow_symlinks=False,
                    )
                    if not (
                        stat.S_ISREG(pending_opened.st_mode)
                        and stat.S_ISREG(pending_linked.st_mode)
                        and pending_opened.st_nlink == 1
                        and pending_linked.st_nlink == 1
                        and pending_opened.st_uid == os.geteuid()
                        and pending_linked.st_uid == os.geteuid()
                        and stat.S_IMODE(pending_opened.st_mode) == 0o600
                        and stat.S_IMODE(pending_linked.st_mode) == 0o600
                        and os.path.samestat(pending_opened, pending_linked)
                    ):
                        raise OSError("validation report pending file is unsafe")
                    pending_value = os.read(pending_fd, len(payload) + 1)
                    pending_final = os.fstat(pending_fd)
                    pending_exact = bool(
                        pending_value == payload
                        and (
                            pending_opened.st_dev,
                            pending_opened.st_ino,
                            pending_opened.st_size,
                            pending_opened.st_mtime_ns,
                            pending_opened.st_ctime_ns,
                        )
                        == (
                            pending_final.st_dev,
                            pending_final.st_ino,
                            pending_final.st_size,
                            pending_final.st_mtime_ns,
                            pending_final.st_ctime_ns,
                        )
                    )
                finally:
                    os.close(pending_fd)
                if not pending_exact:
                    os.unlink(temporary_name, dir_fd=directory_fd)
                    os.fsync(directory_fd)
            if not pending_exact:
                output_fd = os.open(
                    temporary_name,
                    os.O_WRONLY
                    | os.O_CREAT
                    | os.O_EXCL
                    | getattr(os, "O_NOFOLLOW", 0)
                    | getattr(os, "O_CLOEXEC", 0),
                    0o600,
                    dir_fd=directory_fd,
                )
                os.fchmod(output_fd, 0o600)
                view = memoryview(payload)
                while view:
                    written = os.write(output_fd, view)
                    if written <= 0:
                        raise OSError("validation report write made no progress")
                    view = view[written:]
                os.fsync(output_fd)
                os.close(output_fd)
                output_fd = None
            os.replace(
                temporary_name,
                storage_path.name,
                src_dir_fd=directory_fd,
                dst_dir_fd=directory_fd,
            )
            os.fsync(directory_fd)
            final_fd = os.open(
                storage_path.name,
                os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0),
                dir_fd=directory_fd,
            )
            try:
                opened = os.fstat(final_fd)
                linked = os.stat(
                    storage_path.name,
                    dir_fd=directory_fd,
                    follow_symlinks=False,
                )
                reread = bytearray()
                while len(reread) <= len(payload):
                    chunk = os.read(final_fd, min(64 * 1024, len(payload) + 1 - len(reread)))
                    if not chunk:
                        break
                    reread.extend(chunk)
                final_opened = os.fstat(final_fd)
                final_linked = os.stat(
                    storage_path.name,
                    dir_fd=directory_fd,
                    follow_symlinks=False,
                )
                if not (
                    bytes(reread) == payload
                    and stat.S_ISREG(opened.st_mode)
                    and opened.st_nlink == 1
                    and linked.st_nlink == 1
                    and opened.st_uid == os.geteuid()
                    and linked.st_uid == os.geteuid()
                    and stat.S_IMODE(opened.st_mode) == 0o600
                    and stat.S_IMODE(linked.st_mode) == 0o600
                    and (opened.st_dev, opened.st_ino) == (linked.st_dev, linked.st_ino)
                    and (
                        opened.st_dev,
                        opened.st_ino,
                        opened.st_size,
                        opened.st_mtime_ns,
                        opened.st_ctime_ns,
                    )
                    == (
                        final_opened.st_dev,
                        final_opened.st_ino,
                        final_opened.st_size,
                        final_opened.st_mtime_ns,
                        final_opened.st_ctime_ns,
                    )
                    and (final_linked.st_dev, final_linked.st_ino)
                    == (final_opened.st_dev, final_opened.st_ino)
                    and os.path.samestat(
                        parent_status,
                        os.stat(storage_path.parent, follow_symlinks=False),
                    )
                ):
                    raise OSError("validation report changed during durable replacement")
            finally:
                os.close(final_fd)
        finally:
            if output_fd is not None:
                os.close(output_fd)
            os.close(directory_fd)
        return

    temporary = storage_path.with_name(temporary_name)
    parent_anchor: WindowsValidationDirectoryAnchor | None = None
    try:
        if os.name == "nt":
            kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
            parent_anchor = open_windows_validation_directory(
                resolved_parent,
                expected_status=parent_status,
            )
            if (
                writer_lock.windows_parent is None
                or parent_anchor.identity != writer_lock.windows_parent.identity
            ):
                raise OSError("validation report parent differs from its writer lock")
        if os.name == "nt":
            pending_exact = False
            try:
                pending_status = os.lstat(temporary)
            except FileNotFoundError:
                pending_status = None
            if pending_status is not None:
                if not (
                    stat.S_ISREG(pending_status.st_mode)
                    and not stat.S_ISLNK(pending_status.st_mode)
                    and pending_status.st_nlink == 1
                ):
                    raise OSError("validation report pending file is unsafe")
                pending_descriptor = open_private_configuration_windows_descriptor(temporary)
                with os.fdopen(pending_descriptor, "rb") as stream:
                    pending_opened = os.fstat(stream.fileno())
                    pending_linked = os.lstat(temporary)
                    if not (
                        stat.S_ISREG(pending_opened.st_mode)
                        and stat.S_ISREG(pending_linked.st_mode)
                        and pending_opened.st_nlink == 1
                        and pending_linked.st_nlink == 1
                        and os.path.samestat(pending_status, pending_opened)
                        and os.path.samestat(pending_opened, pending_linked)
                    ):
                        raise OSError("validation report pending file changed before recovery")
                    secured_pending = os.fstat(stream.fileno())
                    secured_linked = os.lstat(temporary)
                    if not (
                        secured_pending.st_nlink == 1
                        and secured_linked.st_nlink == 1
                        and os.path.samestat(pending_opened, secured_pending)
                        and os.path.samestat(secured_pending, secured_linked)
                    ):
                        raise OSError(
                            "validation report pending file changed while securing its ACL"
                        )
                    pending_value = stream.read(len(payload) + 1)
                    pending_final = os.fstat(stream.fileno())
                    if (
                        secured_pending.st_dev,
                        secured_pending.st_ino,
                        secured_pending.st_size,
                        secured_pending.st_mtime_ns,
                        secured_pending.st_ctime_ns,
                    ) != (
                        pending_final.st_dev,
                        pending_final.st_ino,
                        pending_final.st_size,
                        pending_final.st_mtime_ns,
                        pending_final.st_ctime_ns,
                    ):
                        raise OSError("validation report pending file changed during recovery")
                pending_exact = pending_value == payload
                if not pending_exact:
                    temporary.unlink()
            if not pending_exact:
                with open_private_atomic_file(temporary) as stream:
                    view = memoryview(payload)
                    while view:
                        written = stream.write(view)
                        if written <= 0:
                            raise OSError("validation report write made no progress")
                        view = view[written:]
                    stream.flush()
                    os.fsync(stream.fileno())
        else:
            with temporary.open("x", encoding="utf-8", newline="\n") as stream:
                stream.write(text)
                stream.flush()
                os.fsync(stream.fileno())
        if os.name == "nt":
            move_file_ex = kernel32.MoveFileExW
            move_file_ex.argtypes = [ctypes.c_wchar_p, ctypes.c_wchar_p, ctypes.c_uint32]
            move_file_ex.restype = ctypes.c_int
            if not move_file_ex(str(temporary), str(storage_path), 0x00000001 | 0x00000008):
                error_number = ctypes.get_last_error()
                raise OSError(error_number, ctypes.FormatError(error_number), str(storage_path))
        else:
            os.replace(temporary, storage_path)
        final_descriptor = (
            open_private_configuration_windows_descriptor(storage_path)
            if os.name == "nt"
            else os.open(storage_path, os.O_RDONLY | getattr(os, "O_CLOEXEC", 0))
        )
        with os.fdopen(final_descriptor, "rb") as stream:
            opened = os.fstat(stream.fileno())
            linked = os.stat(storage_path, follow_symlinks=False)
            if not (
                stat.S_ISREG(opened.st_mode)
                and stat.S_ISREG(linked.st_mode)
                and opened.st_nlink == 1
                and linked.st_nlink == 1
                and os.path.samestat(opened, linked)
            ):
                raise OSError("validation report replacement is not one exact regular file")
            reread = stream.read(len(payload) + 1)
            final_opened = os.fstat(stream.fileno())
        linked = os.stat(storage_path, follow_symlinks=False)
        if not (
            reread == payload
            and stat.S_ISREG(opened.st_mode)
            and stat.S_ISREG(final_opened.st_mode)
            and stat.S_ISREG(linked.st_mode)
            and opened.st_nlink == 1
            and final_opened.st_nlink == 1
            and linked.st_nlink == 1
            and (opened.st_dev, opened.st_ino, opened.st_size)
            == (final_opened.st_dev, final_opened.st_ino, final_opened.st_size)
            == (linked.st_dev, linked.st_ino, linked.st_size)
            and os.path.samestat(
                parent_status,
                os.stat(storage_path.parent, follow_symlinks=False),
            )
        ):
            raise OSError("validation report changed during durable replacement")
    finally:
        close_windows_validation_directory(parent_anchor)

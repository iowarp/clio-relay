"""Serialize validation-report replacement under one parent-wide lock (#231).

Extracted from :mod:`clio_relay.validation_report`
(``docs/design/relay-architecture-2026-08.md``, #231). Two concurrent
processes durably writing a report into the same directory must not race:
one open, owner-private lock file per parent
(``.clio-validation-writer-v1.lock``) serializes both the write itself and
the crash-recovery sweep that removes stale ``.pending`` staging files left
by an interrupted prior write. :func:`acquire_validation_writer_lock` opens
or creates that lock file, takes an exclusive advisory lock on POSIX
(``fcntl.flock``) or an exclusive descriptor on Windows (no ``flock``
equivalent; the open handle itself is the lock), and returns a
:class:`ValidationWriterLock` a caller must eventually pass to
:func:`release_validation_writer_lock`. :func:`verify_validation_writer_lock_parent`
re-proves the lock's parent directory has not been swapped since acquisition
without granting any new access. :func:`prune_stale_validation_pending_files`
bounds crash recovery to a small, exactly-verified set of owner-private
regular files under the held lock, refusing to proceed if pruning cannot
fully succeed.
"""

from __future__ import annotations

import ctypes
import os
import stat
from dataclasses import dataclass
from importlib import import_module
from pathlib import Path

from clio_relay.cluster_config import (
    acquire_private_configuration_windows_parent_guard,
    open_private_atomic_file,
    open_private_configuration_windows_descriptor,
    release_private_configuration_windows_parent_guard,
)
from clio_relay.errors import ConfigurationError
from clio_relay.filesystem_paths import internal_filesystem_path
from clio_relay.validation_directory_windows import (
    WindowsValidationDirectoryAnchor,
    close_windows_validation_directory,
    open_windows_validation_directory,
    verify_windows_validation_directory,
)
from clio_relay.validation_limits import (
    MAX_VALIDATION_PENDING_FILES,
    MAX_VALIDATION_REPORT_WRITE_BYTES,
    VALIDATION_PENDING_PATTERN,
)


@dataclass(frozen=True, slots=True)
class ValidationWriterLock:
    """Parent-wide lock bounding deterministic validation staging files."""

    path: Path
    descriptor: int
    parent_fd: int | None = None
    windows_parent: WindowsValidationDirectoryAnchor | None = None


def acquire_validation_writer_lock(parent: Path) -> ValidationWriterLock:
    """Serialize validation replacement and stale-pending recovery in one parent."""
    parent_status = os.lstat(parent)
    lock_path = parent / ".clio-validation-writer-v1.lock"
    if os.name == "posix":
        parent_fd = os.open(
            parent,
            os.O_RDONLY
            | getattr(os, "O_DIRECTORY", 0)
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_CLOEXEC", 0),
        )
        descriptor: int | None = None
        try:
            if not os.path.samestat(parent_status, os.fstat(parent_fd)):
                raise OSError("validation writer lock parent changed while opening")
            try:
                descriptor = os.open(
                    lock_path.name,
                    os.O_RDWR
                    | os.O_CREAT
                    | os.O_EXCL
                    | getattr(os, "O_NOFOLLOW", 0)
                    | getattr(os, "O_CLOEXEC", 0),
                    0o600,
                    dir_fd=parent_fd,
                )
                os.fchmod(descriptor, 0o600)
                os.fsync(descriptor)
                os.fsync(parent_fd)
            except FileExistsError:
                descriptor = os.open(
                    lock_path.name,
                    os.O_RDWR | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0),
                    dir_fd=parent_fd,
                )
            opened = os.fstat(descriptor)
            linked = os.stat(lock_path.name, dir_fd=parent_fd, follow_symlinks=False)
            if not (
                stat.S_ISREG(opened.st_mode)
                and stat.S_ISREG(linked.st_mode)
                and opened.st_nlink == 1
                and linked.st_nlink == 1
                and opened.st_uid == os.geteuid()
                and linked.st_uid == os.geteuid()
                and stat.S_IMODE(opened.st_mode) == 0o600
                and stat.S_IMODE(linked.st_mode) == 0o600
                and os.path.samestat(opened, linked)
            ):
                raise OSError("validation writer lock is not one owner-private regular file")
            try:
                import_module("fcntl").flock(descriptor, 2 | 4)
            except BlockingIOError:
                raise OSError("another validation writer owns this directory") from None
            confirmed = os.stat(lock_path.name, dir_fd=parent_fd, follow_symlinks=False)
            if not os.path.samestat(opened, confirmed):
                raise OSError("validation writer lock changed during acquisition")
            return ValidationWriterLock(
                path=lock_path,
                descriptor=descriptor,
                parent_fd=parent_fd,
            )
        except BaseException:
            if descriptor is not None:
                os.close(descriptor)
            os.close(parent_fd)
            raise

    windows_parent: WindowsValidationDirectoryAnchor | None = None
    windows_parent_guard: tuple[Path, ctypes.c_void_p] | None = None
    descriptor: int | None = None
    try:
        windows_parent_guard = acquire_private_configuration_windows_parent_guard(parent)
        windows_parent = open_windows_validation_directory(
            parent,
            expected_status=parent_status,
        )
        storage_lock_path = internal_filesystem_path(lock_path, force_extended=True)
        try:
            os.lstat(storage_lock_path)
        except FileNotFoundError:
            try:
                with open_private_atomic_file(storage_lock_path) as stream:
                    stream.flush()
                    os.fsync(stream.fileno())
            except FileExistsError:
                pass
        try:
            descriptor = open_private_configuration_windows_descriptor(
                storage_lock_path,
                exclusive=True,
            )
        except ConfigurationError as exc:
            raise OSError("validation writer lock could not be acquired") from exc
        verify_windows_validation_directory(windows_parent)
        result = ValidationWriterLock(
            path=lock_path,
            descriptor=descriptor,
            windows_parent=windows_parent,
        )
        acquired_parent_guard = windows_parent_guard
        windows_parent_guard = None
        release_private_configuration_windows_parent_guard(acquired_parent_guard)
        return result
    except BaseException:
        if descriptor is not None:
            os.close(descriptor)
        close_windows_validation_directory(windows_parent)
        release_private_configuration_windows_parent_guard(windows_parent_guard)
        raise


def release_validation_writer_lock(lock: ValidationWriterLock) -> None:
    """Release one validation writer lock, preserving its single stable inode."""
    release_error: BaseException | None = None
    if lock.parent_fd is not None:
        try:
            import_module("fcntl").flock(lock.descriptor, 8)
        except BaseException as exc:  # pragma: no cover - OS release failure
            release_error = exc
    try:
        os.close(lock.descriptor)
    except BaseException as exc:  # pragma: no cover - OS release failure
        release_error = release_error or exc
    if lock.parent_fd is not None:
        try:
            os.close(lock.parent_fd)
        except BaseException as exc:  # pragma: no cover - OS release failure
            release_error = release_error or exc
    try:
        close_windows_validation_directory(lock.windows_parent)
    except BaseException as exc:  # pragma: no cover - OS release failure
        release_error = release_error or exc
    if release_error is not None:
        raise OSError(f"validation writer lock could not be released: {release_error}")


def verify_validation_writer_lock_parent(
    lock: ValidationWriterLock,
    parent: Path,
) -> os.stat_result:
    """Verify the named parent against the retained writer lock without mutation."""
    requested_parent = parent.absolute()
    if os.path.normcase(str(requested_parent)) != os.path.normcase(str(lock.path.parent)):
        raise OSError("validation report parent differs from its writer lock")
    if lock.parent_fd is not None:
        try:
            linked_parent = os.stat(requested_parent, follow_symlinks=False)
            linked_lock = os.stat(
                lock.path.name,
                dir_fd=lock.parent_fd,
                follow_symlinks=False,
            )
        except FileNotFoundError:
            raise OSError("validation report parent disappeared after writer lock") from None
        opened_parent = os.fstat(lock.parent_fd)
        opened_lock = os.fstat(lock.descriptor)
        if not (
            stat.S_ISDIR(linked_parent.st_mode)
            and not stat.S_ISLNK(linked_parent.st_mode)
            and os.path.samestat(opened_parent, linked_parent)
            and stat.S_ISREG(opened_lock.st_mode)
            and stat.S_ISREG(linked_lock.st_mode)
            and opened_lock.st_nlink == 1
            and linked_lock.st_nlink == 1
            and os.path.samestat(opened_lock, linked_lock)
        ):
            raise OSError("validation report parent differs from its writer lock")
        return opened_parent
    if lock.windows_parent is None:
        raise OSError("validation writer lock omitted its parent ownership handle")
    verify_windows_validation_directory(lock.windows_parent)
    linked_parent = os.lstat(internal_filesystem_path(requested_parent, force_extended=True))
    if not os.path.samestat(lock.windows_parent.status, linked_parent):
        raise OSError("validation report parent differs from its writer lock")
    return linked_parent


def prune_stale_validation_pending_files(
    parent: Path,
    *,
    current_name: str,
    writer_lock: ValidationWriterLock,
) -> None:
    """Bound crash-recovery staging to the current target under the parent-wide lock."""
    candidates: list[tuple[str, os.stat_result]] = []
    scan_target: int | Path = (
        writer_lock.parent_fd
        if writer_lock.parent_fd is not None
        else internal_filesystem_path(parent, force_extended=True)
    )
    with os.scandir(scan_target) as entries:
        for entry in entries:
            if VALIDATION_PENDING_PATTERN.fullmatch(entry.name) is None:
                continue
            if len(candidates) >= MAX_VALIDATION_PENDING_FILES:
                raise OSError("validation report pending-file retention limit was exceeded")
            observed = (
                os.stat(
                    entry.name,
                    dir_fd=writer_lock.parent_fd,
                    follow_symlinks=False,
                )
                if writer_lock.parent_fd is not None
                else os.lstat(internal_filesystem_path(parent / entry.name, force_extended=True))
            )
            candidates.append((entry.name, observed))
    for name, observed in candidates:
        if name == current_name:
            continue
        if not (
            stat.S_ISREG(observed.st_mode)
            and not stat.S_ISLNK(observed.st_mode)
            and observed.st_nlink == 1
            and 0 <= observed.st_size <= MAX_VALIDATION_REPORT_WRITE_BYTES
            and not getattr(observed, "st_file_attributes", 0) & 0x00000400
        ):
            raise OSError("stale validation report pending file is unsafe")
        if os.name == "posix" and not (
            observed.st_uid == os.geteuid() and stat.S_IMODE(observed.st_mode) == 0o600
        ):
            raise OSError("stale validation report pending file is not owner-private")
        if os.name == "nt":
            descriptor = open_private_configuration_windows_descriptor(
                internal_filesystem_path(parent / name, force_extended=True)
            )
            os.close(descriptor)
            confirmed = os.lstat(internal_filesystem_path(parent / name, force_extended=True))
        else:
            confirmed = os.stat(
                name,
                dir_fd=writer_lock.parent_fd,
                follow_symlinks=False,
            )
        if not (
            confirmed.st_nlink == 1
            and (confirmed.st_dev, confirmed.st_ino, confirmed.st_size)
            == (observed.st_dev, observed.st_ino, observed.st_size)
        ):
            raise OSError("stale validation report pending file changed before deletion")
        if writer_lock.parent_fd is not None:
            os.unlink(name, dir_fd=writer_lock.parent_fd)
            os.fsync(writer_lock.parent_fd)
        else:
            os.unlink(internal_filesystem_path(parent / name, force_extended=True))

    remaining: list[str] = []
    with os.scandir(scan_target) as entries:
        for entry in entries:
            if (
                VALIDATION_PENDING_PATTERN.fullmatch(entry.name) is not None
                and entry.name != current_name
            ):
                remaining.append(entry.name)
    if remaining:
        raise OSError("stale validation report pending-file pruning was incomplete")

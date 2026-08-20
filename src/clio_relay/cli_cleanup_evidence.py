"""Local cleanup-evidence retention primitives extracted off ``cli.py``
(iowarp/clio-relay#231 continuation): the crash-released process lock
guarding local cleanup evidence, and the Windows directory-pinning
helpers that back it. Used by both ``cli_session_start.py`` (checking a
prior finalized report before a fresh start) and
``cli_session_teardown.py``/``cli_owned_report_artifact.py`` (writing
and checkpointing evidence during teardown)."""

from __future__ import annotations

import ctypes
import os
import stat
from dataclasses import dataclass
from importlib import import_module
from pathlib import Path

import clio_relay.cluster_config as cluster_config
from clio_relay.cluster_config import (
    ensure_private_configuration_windows_handle,
    release_private_configuration_windows_parent_guard,
)
from clio_relay.config import RelaySettings
from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.filesystem_paths import internal_filesystem_path
from clio_relay.installation import (
    INSTALL_RECEIPT_PATH_ENV,
    default_install_receipt_path,
)
from clio_relay.session_lifecycle import (
    OwnedSessionInputPolicy,
)
from clio_relay.validation_report import (
    durably_ensure_validation_directory,
)


def _owned_session_input_policy(settings: RelaySettings) -> OwnedSessionInputPolicy:
    """Project the coordinator's validated input limits into one session plan."""

    return OwnedSessionInputPolicy(
        file_max_bytes=settings.input_file_max_bytes,
        total_max_bytes=settings.input_total_max_bytes,
        file_max_count=settings.input_file_max_count,
    )


def _cleanup_evidence_state_parent() -> Path:
    """Return the one user-scoped parent for all local cleanup evidence."""
    receipt_path = default_install_receipt_path().expanduser()
    if not receipt_path.is_absolute():
        raise ConfigurationError(
            f"{INSTALL_RECEIPT_PATH_ENV} must be an absolute path when cleanup evidence "
            "is persisted"
        )
    return receipt_path.parent


@dataclass(frozen=True, slots=True)
class _LocalCleanupReportChunk:
    """One immutable bounded chunk of a locally retained cleanup report."""

    path: Path
    size: int
    sha256: str


@dataclass(frozen=True, slots=True)
class _LocalCleanupReportArtifact:
    """Manifest and chunks retaining one exact coordinator cleanup report."""

    manifest_path: Path
    manifest_sha256: str
    report_sha256: str
    report_size: int
    chunks: tuple[_LocalCleanupReportChunk, ...]


@dataclass(frozen=True, slots=True)
class _WindowsPinnedDirectory:
    """Windows directory handle held without delete sharing to block path swaps."""

    path: Path
    status: os.stat_result
    handle: ctypes.c_void_p


class _WindowsCleanupFileTime(ctypes.Structure):
    _fields_ = [("low", ctypes.c_uint32), ("high", ctypes.c_uint32)]


class _WindowsCleanupFileInformation(ctypes.Structure):
    _fields_ = [
        ("attributes", ctypes.c_uint32),
        ("creation_time", _WindowsCleanupFileTime),
        ("last_access_time", _WindowsCleanupFileTime),
        ("last_write_time", _WindowsCleanupFileTime),
        ("volume_serial_number", ctypes.c_uint32),
        ("file_size_high", ctypes.c_uint32),
        ("file_size_low", ctypes.c_uint32),
        ("number_of_links", ctypes.c_uint32),
        ("file_index_high", ctypes.c_uint32),
        ("file_index_low", ctypes.c_uint32),
    ]


@dataclass(frozen=True, slots=True)
class _CleanupEvidenceLock:
    """One process-owned lock serializing a local cleanup evidence store."""

    path: Path
    parent_fd: int | None = None
    descriptor: int | None = None
    windows_handle: ctypes.c_void_p | None = None
    windows_parent: _WindowsPinnedDirectory | None = None


def _optional_runtime_descriptor(descriptor: int | None) -> int | None:
    """Preserve an OS-selected descriptor as optional across nested helpers."""
    return descriptor


def _windows_parent_guard_names(
    guard: tuple[Path, ctypes.c_void_p] | None,
) -> frozenset[str]:
    """Return the one exact internal guard name ignored by bounded enumeration."""
    return frozenset() if guard is None else frozenset({guard[0].name})


def _open_windows_pinned_directory(
    path: Path,
    *,
    expected: os.stat_result,
    acl_write: bool = False,
) -> _WindowsPinnedDirectory:
    """Open and verify one non-reparse Windows directory without delete sharing."""
    if os.name != "nt":  # pragma: no cover - platform contract
        raise RelayError("Windows directory pinning is unavailable")
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    get_attributes = kernel32.GetFileAttributesW
    get_attributes.argtypes = [ctypes.c_wchar_p]
    get_attributes.restype = ctypes.c_uint32
    invalid_attributes = 0xFFFFFFFF
    file_attribute_directory = 0x00000010
    file_attribute_reparse_point = 0x00000400
    storage_path = internal_filesystem_path(path, force_extended=True)
    attributes = int(get_attributes(str(storage_path)))
    if (
        attributes == invalid_attributes
        or not attributes & file_attribute_directory
        or attributes & file_attribute_reparse_point
    ):
        raise RelayError("local cleanup report artifact directory is a Windows reparse point")
    create_file = kernel32.CreateFileW
    create_file.argtypes = [
        ctypes.c_wchar_p,
        ctypes.c_uint32,
        ctypes.c_uint32,
        ctypes.c_void_p,
        ctypes.c_uint32,
        ctypes.c_uint32,
        ctypes.c_void_p,
    ]
    create_file.restype = ctypes.c_void_p
    raw_handle = create_file(
        str(storage_path),
        0x00000080 | (0x00020000 | 0x00040000 | 0x00080000 if acl_write else 0),
        0x00000001 | 0x00000002,
        None,
        3,
        0x02000000 | 0x00200000,
        None,
    )
    if raw_handle in (None, ctypes.c_void_p(-1).value):
        error_number = ctypes.get_last_error()
        raise RelayError(
            "local cleanup report artifact directory cannot be pinned: "
            f"{ctypes.FormatError(error_number)}"
        )
    handle = ctypes.c_void_p(raw_handle)
    try:
        observed = os.stat(storage_path, follow_symlinks=False)
        attributes = int(get_attributes(str(storage_path)))
        if not (
            os.path.samestat(expected, observed)
            and stat.S_ISDIR(observed.st_mode)
            and attributes != invalid_attributes
            and attributes & file_attribute_directory
            and not attributes & file_attribute_reparse_point
        ):
            raise RelayError("local cleanup report artifact directory changed while pinning")
        return _WindowsPinnedDirectory(path=path, status=observed, handle=handle)
    except BaseException:
        close_handle = kernel32.CloseHandle
        close_handle.argtypes = [ctypes.c_void_p]
        close_handle.restype = ctypes.c_int
        close_handle(handle)
        raise


def _verify_windows_pinned_directory(anchor: _WindowsPinnedDirectory) -> None:
    """Revalidate the named directory while its no-delete-share handle remains open."""
    if os.name != "nt":  # pragma: no cover - platform contract
        raise RelayError("Windows directory verification is unavailable")
    storage_path = internal_filesystem_path(anchor.path, force_extended=True)
    observed = os.stat(storage_path, follow_symlinks=False)
    if not os.path.samestat(anchor.status, observed):
        raise RelayError("local cleanup report artifact directory identity changed")
    get_attributes = ctypes.WinDLL("kernel32", use_last_error=True).GetFileAttributesW
    get_attributes.argtypes = [ctypes.c_wchar_p]
    get_attributes.restype = ctypes.c_uint32
    attributes = int(get_attributes(str(storage_path)))
    if attributes == 0xFFFFFFFF or attributes & 0x00000400:
        raise RelayError("local cleanup report artifact directory became a reparse point")


def _close_windows_pinned_directory(anchor: _WindowsPinnedDirectory | None) -> None:
    """Close one Windows directory anchor."""
    if anchor is None:
        return
    if os.name != "nt":  # pragma: no cover - platform contract
        raise RelayError("Windows directory handles cannot be closed on this platform")
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    close_handle = kernel32.CloseHandle
    close_handle.argtypes = [ctypes.c_void_p]
    close_handle.restype = ctypes.c_int
    if not close_handle(anchor.handle):
        error_number = ctypes.get_last_error()
        raise RelayError(
            "local cleanup report artifact directory handle could not be closed: "
            f"{ctypes.FormatError(error_number)}"
        )


def _windows_cleanup_file_information(
    handle: ctypes.c_void_p,
    *,
    path: Path,
) -> _WindowsCleanupFileInformation:
    if os.name != "nt":  # pragma: no cover - platform contract
        raise RelayError("Windows cleanup handles cannot be inspected on this platform")
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    get_information = kernel32.GetFileInformationByHandle
    get_information.argtypes = [
        ctypes.c_void_p,
        ctypes.POINTER(_WindowsCleanupFileInformation),
    ]
    get_information.restype = ctypes.c_int
    information = _WindowsCleanupFileInformation()
    if not get_information(handle, ctypes.byref(information)):
        error_number = ctypes.get_last_error()
        raise RelayError(
            f"cleanup evidence lock cannot be inspected: {ctypes.FormatError(error_number)}"
        )
    if (
        information.attributes & 0x00000010
        or information.attributes & 0x00000400
        or information.number_of_links != 1
    ):
        raise RelayError(f"cleanup evidence lock is not one private regular file: {path}")
    return information


def _acquire_cleanup_evidence_lock() -> _CleanupEvidenceLock:
    """Acquire the crash-released lock shared by cleanup artifacts and validation output."""
    requested_parent = _cleanup_evidence_state_parent()
    durably_ensure_validation_directory(requested_parent)
    parent_directory = requested_parent.resolve(strict=True)
    if os.path.normcase(str(parent_directory)) != os.path.normcase(str(requested_parent)):
        raise RelayError("cleanup evidence lock parent traverses a symlink or reparse point")
    parent_status = os.lstat(parent_directory)
    if not stat.S_ISDIR(parent_status.st_mode) or stat.S_ISLNK(parent_status.st_mode):
        raise RelayError("cleanup evidence lock parent is not a real directory")
    lock_path = parent_directory / ".clio-cleanup-evidence-v1.lock"
    if os.name == "posix":
        parent_fd = os.open(
            parent_directory,
            os.O_RDONLY
            | getattr(os, "O_DIRECTORY", 0)
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_CLOEXEC", 0),
        )
        descriptor: int | None = None
        try:
            if not os.path.samestat(parent_status, os.fstat(parent_fd)):
                raise RelayError("cleanup evidence lock parent changed while opening")
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
                raise RelayError("cleanup evidence lock is not one owner-private regular file")
            flock = import_module("fcntl").flock
            try:
                flock(descriptor, 2 | 4)
            except BlockingIOError:
                raise RelayError("another cleanup is writing evidence in this directory") from None
            confirmed = os.stat(lock_path.name, dir_fd=parent_fd, follow_symlinks=False)
            if not os.path.samestat(opened, confirmed):
                raise RelayError("cleanup evidence lock changed during acquisition")
            return _CleanupEvidenceLock(
                path=lock_path,
                parent_fd=parent_fd,
                descriptor=descriptor,
            )
        except BaseException:
            if descriptor is not None:
                os.close(descriptor)
            os.close(parent_fd)
            raise

    windows_parent: _WindowsPinnedDirectory | None = None
    windows_handle: ctypes.c_void_p | None = None
    windows_parent_guard: tuple[Path, ctypes.c_void_p] | None = None
    try:
        windows_parent_guard = cluster_config.acquire_private_configuration_windows_parent_guard(
            parent_directory
        )
        windows_parent = _open_windows_pinned_directory(
            parent_directory,
            expected=parent_status,
        )
        storage_lock_path = internal_filesystem_path(lock_path, force_extended=True)
        try:
            lock_status = os.lstat(storage_lock_path)
        except FileNotFoundError:
            try:
                with cluster_config.open_private_atomic_file(storage_lock_path) as stream:
                    stream.flush()
                    os.fsync(stream.fileno())
            except FileExistsError:
                pass
            lock_status = os.lstat(storage_lock_path)
        if not (
            stat.S_ISREG(lock_status.st_mode)
            and not stat.S_ISLNK(lock_status.st_mode)
            and lock_status.st_nlink == 1
            and not getattr(lock_status, "st_file_attributes", 0) & 0x00000400
        ):
            raise RelayError("cleanup evidence lock is not one private regular file")
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        create_file = kernel32.CreateFileW
        create_file.argtypes = [
            ctypes.c_wchar_p,
            ctypes.c_uint32,
            ctypes.c_uint32,
            ctypes.c_void_p,
            ctypes.c_uint32,
            ctypes.c_uint32,
            ctypes.c_void_p,
        ]
        create_file.restype = ctypes.c_void_p
        raw_handle = create_file(
            str(storage_lock_path),
            0x80000000 | 0x00020000 | 0x00040000 | 0x00080000,
            0,
            None,
            3,
            0x00200000,
            None,
        )
        if raw_handle in (None, ctypes.c_void_p(-1).value):
            error_number = ctypes.get_last_error()
            if error_number in {5, 32, 33}:
                raise RelayError("another cleanup is writing evidence in this directory") from None
            raise RelayError(
                f"cleanup evidence lock cannot be opened: {ctypes.FormatError(error_number)}"
            )
        windows_handle = ctypes.c_void_p(raw_handle)
        information = _windows_cleanup_file_information(
            windows_handle,
            path=lock_path,
        )
        observed = os.lstat(storage_lock_path)
        file_index = (int(information.file_index_high) << 32) | int(information.file_index_low)
        if not (
            os.path.samestat(lock_status, observed)
            and observed.st_nlink == 1
            and observed.st_ino == file_index
        ):
            raise RelayError("cleanup evidence lock changed during acquisition")
        ensure_private_configuration_windows_handle(
            storage_lock_path,
            handle=windows_handle,
            directory=False,
        )
        _verify_windows_pinned_directory(windows_parent)
        result = _CleanupEvidenceLock(
            path=lock_path,
            windows_handle=windows_handle,
            windows_parent=windows_parent,
        )
        acquired_parent_guard = windows_parent_guard
        windows_parent_guard = None
        release_private_configuration_windows_parent_guard(acquired_parent_guard)
        return result
    except BaseException:
        if windows_handle is not None:
            close_handle = ctypes.WinDLL("kernel32", use_last_error=True).CloseHandle
            close_handle.argtypes = [ctypes.c_void_p]
            close_handle.restype = ctypes.c_int
            close_handle(windows_handle)
        _close_windows_pinned_directory(windows_parent)
        release_private_configuration_windows_parent_guard(windows_parent_guard)
        raise


def _release_cleanup_evidence_lock(lock: _CleanupEvidenceLock | None) -> None:
    """Release one cleanup evidence lock without removing its private stable inode."""
    if lock is None:
        return
    release_error: BaseException | None = None
    if lock.descriptor is not None:
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
    if lock.windows_handle is not None:
        if os.name != "nt":  # pragma: no cover - corrupt cross-platform state
            raise RelayError("Windows cleanup evidence handle exists on a non-Windows platform")
        close_handle = ctypes.WinDLL("kernel32", use_last_error=True).CloseHandle
        close_handle.argtypes = [ctypes.c_void_p]
        close_handle.restype = ctypes.c_int
        if not close_handle(lock.windows_handle):
            error_number = ctypes.get_last_error()
            release_error = release_error or OSError(
                error_number,
                ctypes.FormatError(error_number),
                str(lock.path),
            )
    try:
        _close_windows_pinned_directory(lock.windows_parent)
    except BaseException as exc:  # pragma: no cover - OS release failure
        release_error = release_error or exc
    if release_error is not None:
        raise RelayError(f"cleanup evidence lock could not be released: {release_error}")


def _verify_cleanup_evidence_lock(
    lock: _CleanupEvidenceLock,
    *,
    expected_parent: Path,
) -> None:
    """Verify that the retained cleanup lock still pins the named evidence parent."""
    resolved_parent = expected_parent.absolute().resolve(strict=True)
    if os.path.normcase(str(resolved_parent)) != os.path.normcase(str(lock.path.parent)):
        raise RelayError("cleanup evidence lock does not cover the validation parent")
    if lock.parent_fd is not None and lock.descriptor is not None:
        parent_linked = os.lstat(resolved_parent)
        lock_linked = os.stat(
            lock.path.name,
            dir_fd=lock.parent_fd,
            follow_symlinks=False,
        )
        if not (
            os.path.samestat(os.fstat(lock.parent_fd), parent_linked)
            and os.path.samestat(os.fstat(lock.descriptor), lock_linked)
            and lock_linked.st_nlink == 1
        ):
            raise RelayError("cleanup evidence lock identity changed")
        return
    if lock.windows_parent is None or lock.windows_handle is None:
        raise RelayError("cleanup evidence lock has no platform ownership handle")
    _verify_windows_pinned_directory(lock.windows_parent)
    information = _windows_cleanup_file_information(lock.windows_handle, path=lock.path)
    lock_linked = os.lstat(internal_filesystem_path(lock.path, force_extended=True))
    file_index = (int(information.file_index_high) << 32) | int(information.file_index_low)
    if lock_linked.st_ino != file_index or lock_linked.st_nlink != 1:
        raise RelayError("cleanup evidence lock identity changed")

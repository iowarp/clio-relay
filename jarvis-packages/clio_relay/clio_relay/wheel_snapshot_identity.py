"""POSIX/Windows identity and cleanup primitives for one held private wheel snapshot.

Split from ``runner.py`` (iowarp/clio-relay#231/#775 decomposition wave 3). Pure
leaf, platform-specific descriptor/handle primitives -- no facade reach-back
needed. :mod:`clio_relay.wheel_private_launch` is the sole consumer of
this module's public surface (the orchestration that actually stages, launches
through, and tears down one private snapshot).
"""

from __future__ import annotations

import hashlib
import hmac
import os
import stat
from collections.abc import Callable
from pathlib import Path
from typing import Any, cast

from clio_relay.constants import FILE_HASH_CHUNK_BYTES


def _file_descriptor_identity(value: os.stat_result) -> tuple[int, int, int, int]:
    """Return the stable fields used to bind an open regular artifact."""
    return (value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns)


def _verified_stream_identity(
    stream: Any,
    *,
    expected_sha256: str,
    expected_size: int,
    label: str,
) -> tuple[int, int, int, int]:
    """Verify one held regular stream against its expected exact bytes."""
    opened = os.fstat(stream.fileno())
    if not stat.S_ISREG(opened.st_mode) or opened.st_size != expected_size:
        raise ValueError(f"{label} size or type did not match release provenance")
    identity = _file_descriptor_identity(opened)
    digest = hashlib.sha256()
    stream.seek(0)
    while chunk := stream.read(FILE_HASH_CHUNK_BYTES):
        digest.update(chunk)
    after = os.fstat(stream.fileno())
    if _file_descriptor_identity(after) != identity or not hmac.compare_digest(
        digest.hexdigest(),
        expected_sha256,
    ):
        raise ValueError(f"{label} bytes changed during verification")
    stream.seek(0)
    return identity


def _stream_still_matches(
    stream: Any,
    *,
    identity: tuple[int, int, int, int],
    expected_sha256: str,
    expected_size: int,
) -> bool:
    """Revalidate a held stream after the nested child exits."""
    try:
        return (
            _verified_stream_identity(
                stream,
                expected_sha256=expected_sha256,
                expected_size=expected_size,
                label="held MCP wheel",
            )
            == identity
        )
    except (OSError, ValueError):
        return False


def _path_matches_identity(path: Path, identity: tuple[int, int, int, int]) -> bool:
    """Return whether a path still names the held regular artifact."""
    try:
        observed = path.lstat()
    except OSError:
        return False
    return stat.S_ISREG(observed.st_mode) and _file_descriptor_identity(observed) == identity


def _private_snapshot_permissions_safe(stream: Any, path: Path) -> bool:
    """Return whether the held snapshot remains a private single-link regular file."""
    try:
        opened = os.fstat(stream.fileno())
        observed = path.lstat()
    except OSError:
        return False
    if (
        not stat.S_ISREG(opened.st_mode)
        or not stat.S_ISREG(observed.st_mode)
        or opened.st_nlink != 1
        or observed.st_nlink != 1
    ):
        return False
    return os.name == "nt" or (
        opened.st_uid == os.getuid()
        and observed.st_uid == os.getuid()
        and stat.S_IMODE(opened.st_mode) == 0o400
        and stat.S_IMODE(observed.st_mode) == 0o400
    )


def _private_directory_identity(
    path: Path,
    *,
    writable: bool,
) -> tuple[int, int, int, int]:
    """Validate one private real snapshot directory and return its identity."""
    observed = path.lstat()
    expected_mode = 0o700 if writable else 0o500
    if not stat.S_ISDIR(observed.st_mode) or path.is_symlink():
        raise ValueError("private MCP wheel directory is not a real directory")
    if os.name != "nt" and (
        observed.st_uid != os.getuid() or stat.S_IMODE(observed.st_mode) != expected_mode
    ):
        raise ValueError("private MCP wheel directory ownership or mode is unsafe")
    return _file_descriptor_identity(observed)


def _private_directory_still_matches(
    path: Path,
    identity: tuple[int, int, int, int],
) -> bool:
    """Revalidate the private snapshot directory after execution."""
    try:
        observed = _private_directory_identity(path, writable=False)
    except (OSError, ValueError):
        return False
    return observed[:2] == identity[:2]


def _open_posix_snapshot_cleanup_descriptors(path: Path) -> tuple[int, int]:
    """Hold the snapshot parent and exact directory without following links."""
    directory_flags = (
        os.O_RDONLY
        | int(getattr(os, "O_DIRECTORY", 0))
        | int(getattr(os, "O_CLOEXEC", 0))
        | int(getattr(os, "O_NOFOLLOW", 0))
    )
    parent_descriptor = os.open(path.parent, directory_flags)
    try:
        directory_descriptor = os.open(
            path.name,
            directory_flags,
            dir_fd=parent_descriptor,
        )
    except BaseException:
        os.close(parent_descriptor)
        raise
    try:
        opened = os.fstat(directory_descriptor)
        observed = path.lstat()
        if (
            not stat.S_ISDIR(opened.st_mode)
            or not stat.S_ISDIR(observed.st_mode)
            or (opened.st_dev, opened.st_ino) != (observed.st_dev, observed.st_ino)
        ):
            raise ValueError("private MCP wheel directory changed while opening cleanup handles")
    except BaseException:
        os.close(directory_descriptor)
        os.close(parent_descriptor)
        raise
    return parent_descriptor, directory_descriptor


_WINDOWS_DELETE = 0x00010000
_WINDOWS_FILE_LIST_DIRECTORY = 0x00000001
_WINDOWS_FILE_READ_ATTRIBUTES = 0x00000080
_WINDOWS_FILE_SHARE_READ = 0x00000001
_WINDOWS_FILE_SHARE_WRITE = 0x00000002
_WINDOWS_OPEN_EXISTING = 3
_WINDOWS_FILE_ATTRIBUTE_DIRECTORY = 0x00000010
_WINDOWS_FILE_ATTRIBUTE_REPARSE_POINT = 0x00000400
_WINDOWS_FILE_FLAG_OPEN_REPARSE_POINT = 0x00200000
_WINDOWS_FILE_FLAG_BACKUP_SEMANTICS = 0x02000000
_WINDOWS_FILE_DISPOSITION_INFO = 4


def _open_windows_snapshot_cleanup_handle(
    path: Path,
    *,
    expected_inode: int,
    directory: bool,
) -> int:
    """Open one exact Windows cleanup entry without permitting substitution."""
    if os.name != "nt":
        raise RuntimeError("Windows snapshot cleanup handles require Windows")
    import ctypes
    from ctypes import wintypes

    desired_access = _WINDOWS_DELETE | _WINDOWS_FILE_READ_ATTRIBUTES
    flags = _WINDOWS_FILE_FLAG_OPEN_REPARSE_POINT
    if directory:
        desired_access |= _WINDOWS_FILE_LIST_DIRECTORY
        flags |= _WINDOWS_FILE_FLAG_BACKUP_SEMANTICS
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.CreateFileW.argtypes = [
        wintypes.LPCWSTR,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.LPVOID,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.HANDLE,
    ]
    kernel32.CreateFileW.restype = wintypes.HANDLE
    raw_handle = kernel32.CreateFileW(
        str(path),
        desired_access,
        _WINDOWS_FILE_SHARE_READ | _WINDOWS_FILE_SHARE_WRITE,
        None,
        _WINDOWS_OPEN_EXISTING,
        flags,
        None,
    )
    invalid_handle = ctypes.c_void_p(-1).value
    if raw_handle == invalid_handle:
        error = ctypes.get_last_error()
        raise OSError(error, os.strerror(error), path)
    handle = int(raw_handle)
    try:
        attributes, inode, links = _windows_snapshot_handle_information(handle, path)
        is_directory = bool(attributes & _WINDOWS_FILE_ATTRIBUTE_DIRECTORY)
        is_reparse = bool(attributes & _WINDOWS_FILE_ATTRIBUTE_REPARSE_POINT)
        if (
            expected_inode <= 0
            or inode != expected_inode
            or is_directory != directory
            or is_reparse
            or (not directory and links != 1)
        ):
            raise ValueError(f"Windows snapshot cleanup entry changed while opening: {path}")
        return handle
    except BaseException:
        _close_windows_snapshot_cleanup_handle(handle)
        raise


def _windows_snapshot_handle_information(
    handle: int,
    path: Path,
) -> tuple[int, int, int]:
    """Return attributes, stable identity, and links for a Windows handle."""
    if os.name != "nt":
        raise RuntimeError("Windows snapshot handle inspection requires Windows")
    import ctypes
    from ctypes import wintypes

    class _ByHandleFileInformation(ctypes.Structure):
        _fields_ = [
            ("file_attributes", wintypes.DWORD),
            ("creation_time", wintypes.FILETIME),
            ("last_access_time", wintypes.FILETIME),
            ("last_write_time", wintypes.FILETIME),
            ("volume_serial_number", wintypes.DWORD),
            ("file_size_high", wintypes.DWORD),
            ("file_size_low", wintypes.DWORD),
            ("number_of_links", wintypes.DWORD),
            ("file_index_high", wintypes.DWORD),
            ("file_index_low", wintypes.DWORD),
        ]

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.GetFileInformationByHandle.argtypes = [
        wintypes.HANDLE,
        ctypes.POINTER(_ByHandleFileInformation),
    ]
    kernel32.GetFileInformationByHandle.restype = wintypes.BOOL
    information = _ByHandleFileInformation()
    if not kernel32.GetFileInformationByHandle(handle, ctypes.byref(information)):
        error = ctypes.get_last_error()
        raise OSError(error, os.strerror(error), path)
    inode = (int(information.file_index_high) << 32) | int(information.file_index_low)
    return (
        int(information.file_attributes),
        inode,
        int(information.number_of_links),
    )


def _mark_windows_snapshot_handle_for_delete(handle: int, path: Path) -> None:
    """Mark one exact Windows cleanup handle for deletion on close."""
    if os.name != "nt":
        raise RuntimeError("Windows snapshot handle deletion requires Windows")
    import ctypes
    from ctypes import wintypes

    class _FileDispositionInformation(ctypes.Structure):
        _fields_ = [("delete_file", wintypes.BOOL)]

    disposition = _FileDispositionInformation(delete_file=True)
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.SetFileInformationByHandle.argtypes = [
        wintypes.HANDLE,
        ctypes.c_int,
        wintypes.LPVOID,
        wintypes.DWORD,
    ]
    kernel32.SetFileInformationByHandle.restype = wintypes.BOOL
    if not kernel32.SetFileInformationByHandle(
        handle,
        _WINDOWS_FILE_DISPOSITION_INFO,
        ctypes.byref(disposition),
        ctypes.sizeof(disposition),
    ):
        error = ctypes.get_last_error()
        raise OSError(error, os.strerror(error), path)


def _close_windows_snapshot_cleanup_handle(handle: int) -> None:
    """Close a Windows cleanup handle."""
    if os.name != "nt":
        raise RuntimeError("Windows snapshot handle cleanup requires Windows")
    import ctypes
    from ctypes import wintypes

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel32.CloseHandle.restype = wintypes.BOOL
    kernel32.CloseHandle(handle)


def _remove_private_snapshot(
    path: Path,
    *,
    snapshot_path: Path,
    directory_identity: tuple[int, int, int, int] | None,
    snapshot_identity: tuple[int, int, int, int] | None,
    posix_parent_descriptor: int | None,
    posix_directory_descriptor: int | None,
    posix_snapshot_descriptor: int | None,
    windows_directory_handle: int | None,
    windows_snapshot_handle: int | None,
) -> str | None:
    """Delete the exact held snapshot file and directory without path recursion."""
    if os.name == "nt":
        return _remove_windows_private_snapshot(
            path,
            snapshot_path=snapshot_path,
            directory_identity=directory_identity,
            snapshot_identity=snapshot_identity,
            directory_handle=windows_directory_handle,
            snapshot_handle=windows_snapshot_handle,
        )
    return _remove_posix_private_snapshot(
        path,
        snapshot_path=snapshot_path,
        directory_identity=directory_identity,
        snapshot_identity=snapshot_identity,
        parent_descriptor=posix_parent_descriptor,
        directory_descriptor=posix_directory_descriptor,
        snapshot_descriptor=posix_snapshot_descriptor,
    )


def _remove_posix_private_snapshot(
    path: Path,
    *,
    snapshot_path: Path,
    directory_identity: tuple[int, int, int, int] | None,
    snapshot_identity: tuple[int, int, int, int] | None,
    parent_descriptor: int | None,
    directory_descriptor: int | None,
    snapshot_descriptor: int | None,
) -> str | None:
    """Delete a POSIX snapshot through held parent and directory descriptors."""
    if parent_descriptor is None or directory_descriptor is None or directory_identity is None:
        for descriptor in (directory_descriptor, parent_descriptor):
            if descriptor is not None:
                os.close(descriptor)
        return "private MCP wheel snapshot has no complete POSIX cleanup handles"
    try:
        held_directory = os.fstat(directory_descriptor)
        if (
            not stat.S_ISDIR(held_directory.st_mode)
            or _file_descriptor_identity(held_directory)[:2] != directory_identity[:2]
        ):
            return "private MCP wheel snapshot directory handle changed before cleanup"
        _posix_fchmod(directory_descriptor, 0o700)
        entries = set(os.listdir(directory_descriptor))
        expected_entries: set[str] = (
            {snapshot_path.name} if snapshot_identity is not None else set()
        )
        unexpected_entries = entries - expected_entries
        if snapshot_identity is not None:
            if snapshot_descriptor is None:
                return "private MCP wheel snapshot has no held POSIX file descriptor"
            held_snapshot = os.fstat(snapshot_descriptor)
            if (
                not stat.S_ISREG(held_snapshot.st_mode)
                or held_snapshot.st_nlink != 1
                or _file_descriptor_identity(held_snapshot)[:2] != snapshot_identity[:2]
            ):
                return "private MCP wheel snapshot held file changed before cleanup"
            if snapshot_path.name not in entries:
                return "private MCP wheel snapshot file disappeared before cleanup"
            observed_snapshot = os.stat(
                snapshot_path.name,
                dir_fd=directory_descriptor,
                follow_symlinks=False,
            )
            if (
                not stat.S_ISREG(observed_snapshot.st_mode)
                or _file_descriptor_identity(observed_snapshot)[:2] != snapshot_identity[:2]
            ):
                return "private MCP wheel snapshot file changed before cleanup"
            os.unlink(snapshot_path.name, dir_fd=directory_descriptor)
            os.fsync(directory_descriptor)
            unlinked_snapshot = os.fstat(snapshot_descriptor)
            if (
                _file_descriptor_identity(unlinked_snapshot)[:2] != snapshot_identity[:2]
                or unlinked_snapshot.st_nlink != 0
            ):
                return "private MCP wheel snapshot held file remained linked after cleanup"
        if unexpected_entries:
            return "private MCP wheel snapshot directory contains unexpected entries"
        if os.listdir(directory_descriptor):
            return "private MCP wheel snapshot directory was not empty after file cleanup"
        observed_path = os.stat(
            path.name,
            dir_fd=parent_descriptor,
            follow_symlinks=False,
        )
        if (
            not stat.S_ISDIR(observed_path.st_mode)
            or _file_descriptor_identity(observed_path)[:2] != directory_identity[:2]
        ):
            return "private MCP wheel snapshot directory path changed before cleanup"
        os.rmdir(path.name, dir_fd=parent_descriptor)
        os.fsync(parent_descriptor)
        if os.fstat(directory_descriptor).st_nlink != 0:
            return "private MCP wheel snapshot original directory remained after cleanup"
        return None
    except OSError as exc:
        return f"private MCP wheel snapshot cleanup failed: {exc}"
    finally:
        os.close(directory_descriptor)
        os.close(parent_descriptor)


def _posix_fchmod(descriptor: int, mode: int) -> None:
    """Call POSIX fchmod without exposing the platform-specific attribute to Pyright."""
    fchmod = cast(Callable[[int, int], None], getattr(os, "fchmod"))  # noqa: B009
    fchmod(descriptor, mode)


def _remove_windows_private_snapshot(
    path: Path,
    *,
    snapshot_path: Path,
    directory_identity: tuple[int, int, int, int] | None,
    snapshot_identity: tuple[int, int, int, int] | None,
    directory_handle: int | None,
    snapshot_handle: int | None,
) -> str | None:
    """Delete the exact Windows snapshot file and directory by retained handles."""
    if directory_handle is None or directory_identity is None:
        if snapshot_handle is not None:
            _close_windows_snapshot_cleanup_handle(snapshot_handle)
        if directory_handle is not None:
            _close_windows_snapshot_cleanup_handle(directory_handle)
        return "private MCP wheel snapshot has no complete Windows cleanup handles"
    active_directory_handle: int | None = directory_handle
    active_snapshot_handle: int | None = snapshot_handle
    try:
        directory_attributes, directory_inode, _ = _windows_snapshot_handle_information(
            directory_handle,
            path,
        )
        if (
            directory_inode != directory_identity[1]
            or not directory_attributes & _WINDOWS_FILE_ATTRIBUTE_DIRECTORY
            or directory_attributes & _WINDOWS_FILE_ATTRIBUTE_REPARSE_POINT
        ):
            return "private MCP wheel snapshot directory handle changed before cleanup"
        if snapshot_identity is not None:
            if snapshot_handle is None:
                snapshot_handle = _open_windows_snapshot_cleanup_handle(
                    snapshot_path,
                    expected_inode=snapshot_identity[1],
                    directory=False,
                )
                active_snapshot_handle = snapshot_handle
            snapshot_attributes, snapshot_inode, links = _windows_snapshot_handle_information(
                snapshot_handle,
                snapshot_path,
            )
            if (
                snapshot_inode != snapshot_identity[1]
                or snapshot_attributes & _WINDOWS_FILE_ATTRIBUTE_DIRECTORY
                or snapshot_attributes & _WINDOWS_FILE_ATTRIBUTE_REPARSE_POINT
                or links != 1
            ):
                return "private MCP wheel snapshot file handle changed before cleanup"
            _mark_windows_snapshot_handle_for_delete(snapshot_handle, snapshot_path)
            _close_windows_snapshot_cleanup_handle(snapshot_handle)
            active_snapshot_handle = None
            if snapshot_path.exists():
                return "private MCP wheel snapshot file remained after handle deletion"
        _mark_windows_snapshot_handle_for_delete(directory_handle, path)
        _close_windows_snapshot_cleanup_handle(directory_handle)
        active_directory_handle = None
        if path.exists():
            return "private MCP wheel snapshot directory remained after handle deletion"
        return None
    except OSError as exc:
        return f"private MCP wheel snapshot cleanup failed: {exc}"
    finally:
        if active_snapshot_handle is not None:
            _close_windows_snapshot_cleanup_handle(active_snapshot_handle)
        if active_directory_handle is not None:
            _close_windows_snapshot_cleanup_handle(active_directory_handle)

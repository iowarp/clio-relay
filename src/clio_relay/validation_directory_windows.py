"""Pin, verify, and create Windows validation-report directories (#231).

Extracted from :mod:`clio_relay.validation_report`
(``docs/design/relay-architecture-2026-08.md``, #231). A durable report
write must prove its target directory is a real, non-reparse, non-symlink
directory that has not been swapped out from under it between the identity
check and the write -- Windows has no ``O_NOFOLLOW``/``dir_fd`` combination
equivalent to POSIX's, so this module opens a raw, delete-share-restricted
``HANDLE`` with ``CreateFileW`` and re-derives the directory's identity
(volume serial + file index) from that same handle every time it needs
re-proving. :func:`open_windows_validation_directory` opens and pins one
directory as a :class:`WindowsValidationDirectoryAnchor`;
:func:`verify_windows_validation_directory` re-checks a held anchor is still
valid; :func:`create_windows_validation_directory_child` creates one private
child directory below a pinned parent by building it in a sibling
``.pending`` staging directory first and atomically renaming it into place
with ``MoveFileExW``, so a crash never leaves a partially-initialized child
visible under its final name.

The higher-level orchestration that walks a full ancestor chain
(:func:`~clio_relay.durable_validation_write.durably_ensure_validation_directory`)
and the cross-platform writer-lock lifecycle
(:mod:`clio_relay.validation_writer_lock`) both import this module's
primitives; this module has no dependency on either of them.
"""

from __future__ import annotations

import ctypes
import hashlib
import os
import stat
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path

from clio_relay.cluster_config import (
    create_private_configuration_directory,
    ensure_private_configuration_windows_handle,
)
from clio_relay.filesystem_paths import internal_filesystem_path


class _WindowsValidationFileTime(ctypes.Structure):
    _fields_ = [("low", ctypes.c_uint32), ("high", ctypes.c_uint32)]


class _WindowsValidationFileInformation(ctypes.Structure):
    _fields_ = [
        ("attributes", ctypes.c_uint32),
        ("creation_time", _WindowsValidationFileTime),
        ("last_access_time", _WindowsValidationFileTime),
        ("last_write_time", _WindowsValidationFileTime),
        ("volume_serial_number", ctypes.c_uint32),
        ("file_size_high", ctypes.c_uint32),
        ("file_size_low", ctypes.c_uint32),
        ("number_of_links", ctypes.c_uint32),
        ("file_index_high", ctypes.c_uint32),
        ("file_index_low", ctypes.c_uint32),
    ]


@dataclass(frozen=True, slots=True)
class WindowsValidationDirectoryAnchor:
    """One non-reparse Windows directory pinned without delete sharing."""

    path: Path
    status: os.stat_result
    handle: ctypes.c_void_p
    identity: tuple[int, int, int]


def windows_validation_directory_identity(
    handle: ctypes.c_void_p,
    *,
    path: Path,
) -> tuple[int, int, int]:
    if os.name != "nt":  # pragma: no cover - platform contract
        raise OSError("Windows validation handles cannot be inspected on this platform")
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    get_information = kernel32.GetFileInformationByHandle
    get_information.argtypes = [
        ctypes.c_void_p,
        ctypes.POINTER(_WindowsValidationFileInformation),
    ]
    get_information.restype = ctypes.c_int
    information = _WindowsValidationFileInformation()
    if not get_information(handle, ctypes.byref(information)):
        error_number = ctypes.get_last_error()
        raise OSError(error_number, ctypes.FormatError(error_number), str(path))
    if not information.attributes & 0x00000010 or information.attributes & 0x00000400:
        raise OSError(f"validation report directory is a Windows reparse point: {path}")
    return (
        int(information.volume_serial_number),
        int(information.file_index_high),
        int(information.file_index_low),
    )


def close_windows_validation_directory(
    anchor: WindowsValidationDirectoryAnchor | None,
) -> None:
    if anchor is None:
        return
    close_windows_validation_handle(anchor.handle, path=anchor.path)


def close_windows_validation_handle(handle: ctypes.c_void_p, *, path: Path) -> None:
    if os.name != "nt":  # pragma: no cover - platform contract
        raise OSError("Windows validation handles cannot be closed on this platform")
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    close_handle = kernel32.CloseHandle
    close_handle.argtypes = [ctypes.c_void_p]
    close_handle.restype = ctypes.c_int
    if not close_handle(handle):
        error_number = ctypes.get_last_error()
        raise OSError(error_number, ctypes.FormatError(error_number), str(path))


def open_windows_validation_handle(
    path: Path,
    *,
    allow_delete_share: bool,
    acl_write: bool,
) -> ctypes.c_void_p:
    if os.name != "nt":  # pragma: no cover - platform contract
        raise OSError("Windows validation handles cannot be opened on this platform")
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
    storage_path = internal_filesystem_path(path, force_extended=True)
    share = 0x00000001 | 0x00000002 | (0x00000004 if allow_delete_share else 0)
    raw_handle = create_file(
        str(storage_path),
        0x00000080 | (0x00020000 | 0x00040000 | 0x00080000 if acl_write else 0),
        share,
        None,
        3,
        0x02000000 | 0x00200000,
        None,
    )
    if raw_handle in (None, ctypes.c_void_p(-1).value):
        error_number = ctypes.get_last_error()
        raise OSError(error_number, ctypes.FormatError(error_number), str(path))
    return ctypes.c_void_p(raw_handle)


def open_windows_validation_directory(
    path: Path,
    *,
    expected_status: os.stat_result,
    expected_identity: tuple[int, int, int] | None = None,
    allow_delete_share: bool = False,
    acl_write: bool = False,
) -> WindowsValidationDirectoryAnchor:
    storage_path = internal_filesystem_path(path, force_extended=True)
    handle = open_windows_validation_handle(
        path,
        allow_delete_share=allow_delete_share,
        acl_write=acl_write,
    )
    try:
        identity = windows_validation_directory_identity(handle, path=path)
        observed = os.lstat(storage_path)
        if not (
            os.path.samestat(expected_status, observed)
            and stat.S_ISDIR(observed.st_mode)
            and not stat.S_ISLNK(observed.st_mode)
            and not getattr(observed, "st_file_attributes", 0) & 0x00000400
            and (expected_identity is None or identity == expected_identity)
        ):
            raise OSError(f"validation report directory changed while pinning: {path}")
        anchor = WindowsValidationDirectoryAnchor(
            path=path,
            status=observed,
            handle=handle,
            identity=identity,
        )
        verification_handle = open_windows_validation_handle(
            path,
            allow_delete_share=False,
            acl_write=False,
        )
        try:
            if windows_validation_directory_identity(verification_handle, path=path) != identity:
                raise OSError(f"validation report directory path changed: {path}")
        finally:
            close_windows_validation_handle(verification_handle, path=path)
        return anchor
    except BaseException:
        close_windows_validation_handle(handle, path=path)
        raise


def verify_windows_validation_directory(
    anchor: WindowsValidationDirectoryAnchor,
) -> None:
    if windows_validation_directory_identity(anchor.handle, path=anchor.path) != anchor.identity:
        raise OSError(f"validation report directory handle changed: {anchor.path}")
    storage_path = internal_filesystem_path(anchor.path, force_extended=True)
    observed = os.lstat(storage_path)
    if not (
        os.path.samestat(anchor.status, observed)
        and stat.S_ISDIR(observed.st_mode)
        and not stat.S_ISLNK(observed.st_mode)
        and not getattr(observed, "st_file_attributes", 0) & 0x00000400
    ):
        raise OSError(f"validation report directory path changed: {anchor.path}")
    verification_handle = open_windows_validation_handle(
        anchor.path,
        allow_delete_share=False,
        acl_write=False,
    )
    try:
        if (
            windows_validation_directory_identity(
                verification_handle,
                path=anchor.path,
            )
            != anchor.identity
        ):
            raise OSError(f"validation report directory path changed: {anchor.path}")
    finally:
        close_windows_validation_handle(verification_handle, path=anchor.path)


def create_windows_validation_directory_child(
    parent: WindowsValidationDirectoryAnchor,
    child_name: str,
) -> WindowsValidationDirectoryAnchor:
    """Create, durably name, and pin one private child below a pinned parent."""
    if os.name != "nt":  # pragma: no cover - platform contract
        raise OSError("Windows validation directories cannot be created on this platform")
    if Path(child_name).name != child_name or child_name in {".", ".."}:
        raise OSError(f"unsafe validation report directory component: {child_name}")
    verify_windows_validation_directory(parent)
    child_path = parent.path / child_name
    pending_identity = hashlib.sha256(child_name.encode("utf-8")).hexdigest()[:32]
    temporary_path = parent.path / f".clio-validation-dir-{pending_identity}.pending"
    temporary_anchor: WindowsValidationDirectoryAnchor | None = None
    child_anchor: WindowsValidationDirectoryAnchor | None = None
    try:
        temporary_storage_path = internal_filesystem_path(
            temporary_path,
            force_extended=True,
        )
        try:
            temporary_status = os.lstat(temporary_storage_path)
        except FileNotFoundError:
            create_private_configuration_directory(temporary_storage_path)
            temporary_status = os.lstat(temporary_storage_path)
        if not (
            stat.S_ISDIR(temporary_status.st_mode)
            and not stat.S_ISLNK(temporary_status.st_mode)
            and not getattr(temporary_status, "st_file_attributes", 0) & 0x00000400
        ):
            raise OSError(f"validation report pending directory is unsafe: {temporary_path}")
        temporary_anchor = open_windows_validation_directory(
            temporary_path,
            expected_status=temporary_status,
            allow_delete_share=True,
            acl_write=True,
        )
        ensure_private_configuration_windows_handle(
            temporary_storage_path,
            handle=temporary_anchor.handle,
            directory=True,
        )
        with os.scandir(temporary_storage_path) as entries:
            if next(entries, None) is not None:
                raise OSError(f"validation report pending directory is not empty: {temporary_path}")
        verify_windows_validation_directory(temporary_anchor)
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        move_file_ex = kernel32.MoveFileExW
        move_file_ex.argtypes = [ctypes.c_wchar_p, ctypes.c_wchar_p, ctypes.c_uint32]
        move_file_ex.restype = ctypes.c_int
        if not move_file_ex(
            str(temporary_storage_path),
            str(internal_filesystem_path(child_path, force_extended=True)),
            0x00000008,
        ):
            error_number = ctypes.get_last_error()
            raise OSError(error_number, ctypes.FormatError(error_number), str(child_path))
        child_status = os.lstat(internal_filesystem_path(child_path, force_extended=True))
        child_anchor = open_windows_validation_directory(
            child_path,
            expected_status=child_status,
            expected_identity=temporary_anchor.identity,
            acl_write=True,
        )
        ensure_private_configuration_windows_handle(
            internal_filesystem_path(child_path, force_extended=True),
            handle=child_anchor.handle,
            directory=True,
        )
        verify_windows_validation_directory(parent)
        verify_windows_validation_directory(child_anchor)
        result = child_anchor
        child_anchor = None
        return result
    finally:
        close_windows_validation_directory(child_anchor)
        close_windows_validation_directory(temporary_anchor)
        with suppress(OSError):
            os.rmdir(internal_filesystem_path(temporary_path, force_extended=True))

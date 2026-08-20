"""Windows ctypes handle primitives for execution-sidecar quarantine
(iowarp/clio-relay#231).

Owner module for the ``kernel32``-backed handle operations the execution-
sidecar cleanup path needs on Windows, where POSIX's directory-fd-scoped
``renameat2(RENAME_NOREPLACE)`` (``endpoint.py``'s ``_rename_noreplace_at``)
has no equivalent: opening a path without allowing delete-sharing or reparse
traversal (``_open_windows_cleanup_handle``), reading stable file identity
off an already-open handle (``_windows_handle_information``), applying a
no-replace ``FileRenameInfo`` to an open handle (``_mark_windows_handle_for_
rename``), closing a handle (``_close_windows_cleanup_handle``), validating a
handle against a pinned ``_RuntimeSidecarAnchor``
(``_validate_windows_sidecar_handle``), and the two orchestrators that chain
these to quarantine one sidecar or a whole batch by handle
(``_quarantine_windows_sidecar_by_handle``, ``_remove_execution_sidecars_
windows``).

Depends on ``endpoint_sidecar_types.py`` (the Windows kernel32 constants and
``_RuntimeSidecarAnchor``) and ``endpoint_runtime_sidecar_anchor.py``
(``_validate_runtime_sidecar_stat`` and ``_execution_sidecar_quarantine_
name`` -- the latter is why that primitive lives on the anchor module rather
than on the still-co-resident execution-sidecar cleanup orchestration in
``endpoint.py``: ``_remove_execution_sidecars_windows`` needs it, and putting
it there instead of here would recreate the exact cross-module cycle this
split is designed to avoid). Both are leaves, so this module stays acyclic;
``endpoint.py``'s still-co-resident ``_remove_execution_sidecars`` calls into
``_remove_execution_sidecars_windows`` here on the Windows branch of its own
cross-platform quarantine.

Near-duplicates the same ``kernel32`` handle-cleanup pattern in
``mcp_call/runner.py`` (``_open_windows_snapshot_cleanup_handle`` and
siblings) -- no shared import is possible there (separately wheel-packaged
subprocess entry point, per the architecture doc's own §4.5/§5 note), so
that duplication is accepted by design, not an oversight of this split.
"""

from __future__ import annotations

import ctypes
import os
from contextlib import suppress
from ctypes import wintypes
from pathlib import Path

from clio_relay.endpoint_runtime_sidecar_anchor import (
    _execution_sidecar_quarantine_name,
    _validate_runtime_sidecar_stat,
)
from clio_relay.endpoint_sidecar_types import (
    _WINDOWS_DELETE,
    _WINDOWS_ERROR_ALREADY_EXISTS,
    _WINDOWS_ERROR_FILE_EXISTS,
    _WINDOWS_ERROR_FILE_NOT_FOUND,
    _WINDOWS_ERROR_PATH_NOT_FOUND,
    _WINDOWS_FILE_ATTRIBUTE_DIRECTORY,
    _WINDOWS_FILE_ATTRIBUTE_REPARSE_POINT,
    _WINDOWS_FILE_FLAG_BACKUP_SEMANTICS,
    _WINDOWS_FILE_FLAG_OPEN_REPARSE_POINT,
    _WINDOWS_FILE_READ_ATTRIBUTES,
    _WINDOWS_FILE_RENAME_INFO,
    _WINDOWS_FILE_SHARE_READ,
    _WINDOWS_FILE_SHARE_WRITE,
    _WINDOWS_OPEN_EXISTING,
    _RuntimeSidecarAnchor,
)
from clio_relay.errors import ConfigurationError
from clio_relay.filesystem_paths import internal_filesystem_path


def _remove_execution_sidecars_windows(
    paths: list[Path],
    *,
    spool_path: Path,
    expected_spool_identity: tuple[int, int],
    expected_anchors: dict[Path, _RuntimeSidecarAnchor] | None = None,
    expected_quarantines: dict[Path, Path] | None = None,
) -> dict[Path, Path]:
    """Quarantine exact Windows file handles while the parent cannot be replaced."""
    anchors = expected_anchors or {}
    quarantines = expected_quarantines or {}
    directory_handle = _open_windows_cleanup_handle(
        spool_path,
        desired_access=_WINDOWS_DELETE | _WINDOWS_FILE_READ_ATTRIBUTES,
        share_mode=_WINDOWS_FILE_SHARE_READ | _WINDOWS_FILE_SHARE_WRITE,
        flags=_WINDOWS_FILE_FLAG_BACKUP_SEMANTICS | _WINDOWS_FILE_FLAG_OPEN_REPARSE_POINT,
        missing_ok=False,
    )
    if directory_handle is None:
        raise ConfigurationError(f"execution spool disappeared during cleanup: {spool_path}")
    try:
        attributes, file_id = _windows_handle_information(directory_handle, spool_path)
        if attributes & _WINDOWS_FILE_ATTRIBUTE_REPARSE_POINT:
            raise ConfigurationError(f"execution spool became a reparse point: {spool_path}")
        if not attributes & _WINDOWS_FILE_ATTRIBUTE_DIRECTORY:
            raise ConfigurationError(f"execution spool is not a directory: {spool_path}")
        expected_file_id = expected_spool_identity[1]
        if expected_file_id and file_id != expected_file_id:
            raise ConfigurationError(f"execution spool changed while opened: {spool_path}")
        result: dict[Path, Path] = {}
        for path in paths:
            anchor = anchors.get(path)
            if anchor is None:
                raise ConfigurationError(f"execution sidecar has no durable anchor: {path}")
            quarantine = quarantines.get(path)
            if quarantine is None:
                quarantine = spool_path / _execution_sidecar_quarantine_name(anchor)
            if quarantine.parent != spool_path or quarantine.name == path.name:
                raise ConfigurationError(
                    f"invalid execution sidecar quarantine target: {quarantine}"
                )
            _quarantine_windows_sidecar_by_handle(
                path,
                quarantine=quarantine,
                anchored_directory_handle=directory_handle,
                expected_anchor=anchor,
            )
            result[path] = quarantine
        return result
    finally:
        _close_windows_cleanup_handle(directory_handle)


def _quarantine_windows_sidecar_by_handle(
    path: Path,
    *,
    quarantine: Path,
    anchored_directory_handle: int,
    expected_anchor: _RuntimeSidecarAnchor,
) -> None:
    """Rename one exact open sidecar handle to a no-replace quarantine target."""
    _windows_handle_information(anchored_directory_handle, path.parent)
    existing_quarantine = _open_windows_cleanup_handle(
        quarantine,
        desired_access=_WINDOWS_FILE_READ_ATTRIBUTES,
        share_mode=_WINDOWS_FILE_SHARE_READ | _WINDOWS_FILE_SHARE_WRITE,
        flags=_WINDOWS_FILE_FLAG_BACKUP_SEMANTICS | _WINDOWS_FILE_FLAG_OPEN_REPARSE_POINT,
        missing_ok=True,
    )
    if existing_quarantine is not None:
        try:
            _validate_windows_sidecar_handle(
                existing_quarantine,
                quarantine,
                expected_anchor=expected_anchor,
            )
            if os.path.lexists(internal_filesystem_path(path)):
                raise ConfigurationError(
                    f"execution sidecar source was replaced after quarantine: {path}"
                )
            return
        finally:
            _close_windows_cleanup_handle(existing_quarantine)
    file_handle = _open_windows_cleanup_handle(
        path,
        desired_access=_WINDOWS_DELETE | _WINDOWS_FILE_READ_ATTRIBUTES,
        share_mode=_WINDOWS_FILE_SHARE_READ | _WINDOWS_FILE_SHARE_WRITE,
        flags=_WINDOWS_FILE_FLAG_BACKUP_SEMANTICS | _WINDOWS_FILE_FLAG_OPEN_REPARSE_POINT,
        missing_ok=True,
    )
    if file_handle is None:
        raise ConfigurationError(f"anchored execution sidecar and quarantine disappeared: {path}")
    try:
        _validate_windows_sidecar_handle(
            file_handle,
            path,
            expected_anchor=expected_anchor,
        )
        with suppress(FileExistsError):
            _mark_windows_handle_for_rename(file_handle, path, quarantine)
    finally:
        _close_windows_cleanup_handle(file_handle)
    if os.path.lexists(internal_filesystem_path(path)):
        raise ConfigurationError(f"execution sidecar source was replaced during quarantine: {path}")
    quarantine_handle = _open_windows_cleanup_handle(
        quarantine,
        desired_access=_WINDOWS_FILE_READ_ATTRIBUTES,
        share_mode=_WINDOWS_FILE_SHARE_READ | _WINDOWS_FILE_SHARE_WRITE,
        flags=_WINDOWS_FILE_FLAG_BACKUP_SEMANTICS | _WINDOWS_FILE_FLAG_OPEN_REPARSE_POINT,
        missing_ok=False,
    )
    if quarantine_handle is None:
        raise ConfigurationError(f"execution sidecar quarantine disappeared: {quarantine}")
    try:
        _validate_windows_sidecar_handle(
            quarantine_handle,
            quarantine,
            expected_anchor=expected_anchor,
        )
    finally:
        _close_windows_cleanup_handle(quarantine_handle)


def _validate_windows_sidecar_handle(
    handle: int,
    path: Path,
    *,
    expected_anchor: _RuntimeSidecarAnchor,
) -> None:
    """Validate a non-reparse Windows handle against its pre-release inode."""
    attributes, file_id = _windows_handle_information(handle, path)
    if attributes & _WINDOWS_FILE_ATTRIBUTE_REPARSE_POINT:
        raise ConfigurationError(f"execution sidecar became a reparse point: {path}")
    if attributes & _WINDOWS_FILE_ATTRIBUTE_DIRECTORY:
        raise ConfigurationError(f"execution sidecar became a directory: {path}")
    if file_id != expected_anchor.inode:
        raise ConfigurationError(f"execution sidecar file identity changed: {path}")
    try:
        file_stat = os.stat(internal_filesystem_path(path), follow_symlinks=False)
    except OSError as exc:
        raise ConfigurationError(f"could not inspect execution sidecar {path}: {exc}") from exc
    _validate_runtime_sidecar_stat(
        file_stat,
        expected=expected_anchor,
        label="execution sidecar",
    )


def _open_windows_cleanup_handle(
    path: Path,
    *,
    desired_access: int,
    share_mode: int,
    flags: int,
    missing_ok: bool,
) -> int | None:
    """Open a Windows path without allowing delete sharing or reparse traversal."""
    if os.name != "nt":
        raise RuntimeError("Windows cleanup handles require Windows")
    from ctypes import wintypes

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
        str(internal_filesystem_path(path)),
        desired_access,
        share_mode,
        None,
        _WINDOWS_OPEN_EXISTING,
        flags,
        None,
    )
    invalid_handle = ctypes.c_void_p(-1).value
    if raw_handle == invalid_handle:
        error = ctypes.get_last_error()
        if missing_ok and error in {
            _WINDOWS_ERROR_FILE_NOT_FOUND,
            _WINDOWS_ERROR_PATH_NOT_FOUND,
        }:
            return None
        raise ConfigurationError(
            f"could not open execution cleanup path {path}: Windows error {error}"
        )
    return int(raw_handle)


class _ByHandleFileInformation(ctypes.Structure):
    """``BY_HANDLE_FILE_INFORMATION`` layout (fixed; wintypes imports cross-platform)."""

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


class _FileRenameInformationLayout(ctypes.Structure):
    """``FILE_RENAME_INFO`` layout head; the flexible ``FileName`` tail is
    buffer-built from field offsets at the call site (fixed layout here)."""

    _fields_ = [
        ("replace_if_exists", wintypes.BOOLEAN),
        ("root_directory", wintypes.HANDLE),
        ("file_name_length", wintypes.DWORD),
        ("file_name", wintypes.WCHAR * 1),
    ]


def _windows_handle_information(handle: int, path: Path) -> tuple[int, int]:
    """Return attributes and stable file identity for an already-open Windows handle."""
    if os.name != "nt":
        raise RuntimeError("Windows cleanup handle inspection requires Windows")

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.GetFileInformationByHandle.argtypes = [
        wintypes.HANDLE,
        ctypes.POINTER(_ByHandleFileInformation),
    ]
    kernel32.GetFileInformationByHandle.restype = wintypes.BOOL
    information = _ByHandleFileInformation()
    if not kernel32.GetFileInformationByHandle(handle, ctypes.byref(information)):
        error = ctypes.get_last_error()
        raise ConfigurationError(
            f"could not inspect execution cleanup handle {path}: Windows error {error}"
        )
    file_id = (int(information.file_index_high) << 32) | int(information.file_index_low)
    return int(information.file_attributes), file_id


def _mark_windows_handle_for_rename(
    handle: int,
    source: Path,
    quarantine: Path,
) -> None:
    """Apply no-replace FileRenameInfo to an exact open sidecar handle."""
    if os.name != "nt":
        raise RuntimeError("Windows handle rename requires Windows")

    quarantine_text = str(internal_filesystem_path(quarantine))
    quarantine_bytes = quarantine_text.encode("utf-16-le")
    if not quarantine_bytes or "\x00" in quarantine_text:
        raise ConfigurationError(f"invalid execution sidecar quarantine name: {quarantine}")

    # FILE_RENAME_INFORMATION ends in a flexible WCHAR array. Windows accepts
    # FileNameLength as the exact non-NUL UTF-16 payload length, but the input
    # buffer must still include storage for the terminating WCHAR. Build the
    # buffer from the field offset so ctypes structure tail padding cannot be
    # interpreted as part of the destination name.
    file_name_offset = _FileRenameInformationLayout.file_name.offset
    buffer_size = file_name_offset + len(quarantine_bytes) + ctypes.sizeof(wintypes.WCHAR)
    rename_buffer = (ctypes.c_ubyte * buffer_size)()
    buffer_address = ctypes.addressof(rename_buffer)
    replace_if_exists = wintypes.BOOLEAN(False)
    root_directory = wintypes.HANDLE()
    file_name_length = wintypes.DWORD(len(quarantine_bytes))
    ctypes.memmove(
        buffer_address + _FileRenameInformationLayout.replace_if_exists.offset,
        ctypes.byref(replace_if_exists),
        ctypes.sizeof(replace_if_exists),
    )
    ctypes.memmove(
        buffer_address + _FileRenameInformationLayout.root_directory.offset,
        ctypes.byref(root_directory),
        ctypes.sizeof(root_directory),
    )
    ctypes.memmove(
        buffer_address + _FileRenameInformationLayout.file_name_length.offset,
        ctypes.byref(file_name_length),
        ctypes.sizeof(file_name_length),
    )
    ctypes.memmove(
        buffer_address + file_name_offset,
        quarantine_bytes,
        len(quarantine_bytes),
    )

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
        _WINDOWS_FILE_RENAME_INFO,
        rename_buffer,
        buffer_size,
    ):
        error = ctypes.get_last_error()
        if error in {_WINDOWS_ERROR_FILE_EXISTS, _WINDOWS_ERROR_ALREADY_EXISTS}:
            raise FileExistsError(error, f"execution sidecar quarantine exists: {quarantine}")
        raise ConfigurationError(
            f"could not quarantine execution sidecar {source}: Windows error {error}"
        )


def _close_windows_cleanup_handle(handle: int) -> None:
    """Close a Windows cleanup handle without masking an earlier cleanup failure."""
    if os.name != "nt":
        raise RuntimeError("Windows handle cleanup requires Windows")
    from ctypes import wintypes

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel32.CloseHandle.restype = wintypes.BOOL
    kernel32.CloseHandle(handle)

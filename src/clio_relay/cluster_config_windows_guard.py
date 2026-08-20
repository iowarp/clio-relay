"""Windows parent-rename guards and privately-ACL'd descriptor opening.

Part of the `cluster_config` Windows-security split (iowarp/clio-relay#231).
This owns the auto-deleting sibling that blocks a parent directory rename
during a durable multi-step write, and opening an *existing* file under an
enforced private ACL. Both depend on
`cluster_config_windows_primitives.py` and `cluster_config_windows_paths.py`
(imported as modules, so `tests/test_cluster_config.py`'s
`monkeypatch.setattr` against those modules' names reaches the call sites
here too).
"""

from __future__ import annotations

import ctypes
import os
import stat
import time
from pathlib import Path
from typing import cast
from uuid import uuid4

from clio_relay import cluster_config_windows_paths as _windows_paths
from clio_relay import cluster_config_windows_primitives as _windows_primitives
from clio_relay.cluster_config_io import (
    CONFIG_READ_RETRY_SECONDS,
    MAX_CONFIG_READ_ATTEMPTS,
    _is_reparse_stat,
)
from clio_relay.cluster_config_windows_primitives import (
    _WINDOWS_CREATE_NEW,
    _WINDOWS_DELETE,
    _WINDOWS_FILE_ATTRIBUTE_DIRECTORY,
    _WINDOWS_FILE_ATTRIBUTE_NORMAL,
    _WINDOWS_FILE_ATTRIBUTE_REPARSE_POINT,
    _WINDOWS_FILE_FLAG_DELETE_ON_CLOSE,
    _WINDOWS_FILE_FLAG_OPEN_REPARSE_POINT,
    _WINDOWS_FILE_SHARE_READ,
    _WINDOWS_GENERIC_READ,
    _WINDOWS_GENERIC_WRITE,
    _WINDOWS_OPEN_EXISTING,
    _WINDOWS_READ_CONTROL,
    _WINDOWS_WRITE_DAC,
    _WINDOWS_WRITE_OWNER,
    _WindowsFileInformation,
    _WindowsSecurityAttributes,
)
from clio_relay.errors import ConfigurationError
from clio_relay.filesystem_paths import internal_filesystem_path


def acquire_private_configuration_windows_parent_guard(
    parent: Path,
) -> tuple[Path, ctypes.c_void_p]:
    """Create an auto-deleting private child that prevents parent rename on Windows."""
    if os.name != "nt":  # pragma: no cover - explicit platform contract
        raise ConfigurationError("Windows parent guarding is unavailable")
    guard_path = parent / f".clio-parent-guard-{os.getpid()}-{uuid4().hex}.pending"
    storage_path = internal_filesystem_path(guard_path, force_extended=True)
    kernel32 = _windows_primitives._load_windows_library("kernel32")
    advapi32 = _windows_primitives._load_windows_library("advapi32")
    owner_sid = _windows_primitives._current_windows_user_sid(
        advapi32=advapi32,
        kernel32=kernel32,
        path=storage_path,
    )
    security_descriptor = _windows_paths._build_private_windows_security_descriptor(
        directory=False,
        advapi32=advapi32,
        owner_sid=owner_sid,
        path=storage_path,
    )
    security_attributes = _WindowsSecurityAttributes(
        length=ctypes.sizeof(_WindowsSecurityAttributes),
        security_descriptor=security_descriptor,
        inherit_handle=0,
    )
    create_file = kernel32.CreateFileW
    create_file.argtypes = [
        ctypes.c_wchar_p,
        ctypes.c_uint32,
        ctypes.c_uint32,
        ctypes.POINTER(_WindowsSecurityAttributes),
        ctypes.c_uint32,
        ctypes.c_uint32,
        ctypes.c_void_p,
    ]
    create_file.restype = ctypes.c_void_p
    raw_handle: int | None = None
    try:
        raw_value = create_file(
            str(storage_path),
            _WINDOWS_GENERIC_READ
            | _WINDOWS_GENERIC_WRITE
            | _WINDOWS_DELETE
            | _WINDOWS_READ_CONTROL
            | _WINDOWS_WRITE_DAC
            | _WINDOWS_WRITE_OWNER,
            0,
            ctypes.byref(security_attributes),
            _WINDOWS_CREATE_NEW,
            _WINDOWS_FILE_ATTRIBUTE_NORMAL
            | _WINDOWS_FILE_FLAG_OPEN_REPARSE_POINT
            | _WINDOWS_FILE_FLAG_DELETE_ON_CLOSE,
            None,
        )
        if raw_value in (None, ctypes.c_void_p(-1).value):
            raise ConfigurationError(
                "could not create private Windows parent guard "
                f"({_windows_primitives._windows_last_error()}): {parent}"
            )
        raw_handle = cast(int, raw_value)
        handle = ctypes.c_void_p(raw_handle)
        _windows_paths._validate_windows_configuration_handle(
            handle,
            directory=False,
            kernel32=kernel32,
            path=storage_path,
        )
        _windows_paths.ensure_private_configuration_windows_handle(
            storage_path,
            handle=handle,
            directory=False,
        )
        return guard_path, handle
    except BaseException:
        if raw_handle is not None:
            _windows_primitives._close_windows_handle(
                ctypes.c_void_p(raw_handle), kernel32=kernel32
            )
        raise
    finally:
        _windows_primitives._free_windows_local(security_descriptor, kernel32=kernel32)


def release_private_configuration_windows_parent_guard(
    guard: tuple[Path, ctypes.c_void_p] | None,
) -> None:
    """Close one auto-deleting Windows parent guard."""
    if guard is None:
        return
    if os.name != "nt":  # pragma: no cover - explicit platform contract
        raise ConfigurationError("Windows parent guarding is unavailable")
    _path, handle = guard
    _windows_primitives._close_windows_handle(
        handle, kernel32=_windows_primitives._load_windows_library("kernel32")
    )


def open_private_configuration_windows_descriptor(
    path: Path,
    *,
    exclusive: bool = False,
    expected_nlink: int = 1,
) -> int:
    """Open one exact Windows file and enforce its private ACL in place."""
    if os.name != "nt":  # pragma: no cover - explicit platform contract
        raise ConfigurationError("Windows private descriptor opening is unavailable")
    storage_path = internal_filesystem_path(path, force_extended=True)
    before = os.lstat(storage_path)
    if not (
        stat.S_ISREG(before.st_mode)
        and not _is_reparse_stat(before)
        and before.st_nlink == expected_nlink
    ):
        raise ConfigurationError(f"configuration path is not one regular owned file: {path}")
    kernel32 = _windows_primitives._load_windows_library("kernel32")
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
    full_access = (
        _WINDOWS_GENERIC_READ | _WINDOWS_READ_CONTROL | _WINDOWS_WRITE_DAC | _WINDOWS_WRITE_OWNER
    )
    owner_preserving_access = full_access & ~_WINDOWS_WRITE_OWNER
    raw_handle: int | None = None
    for attempt in range(MAX_CONFIG_READ_ATTEMPTS):
        candidate_handle = create_file(
            str(storage_path),
            full_access,
            0 if exclusive else _WINDOWS_FILE_SHARE_READ,
            None,
            _WINDOWS_OPEN_EXISTING,
            _WINDOWS_FILE_FLAG_OPEN_REPARSE_POINT,
            None,
        )
        if candidate_handle not in (None, ctypes.c_void_p(-1).value):
            raw_handle = cast(int, candidate_handle)
            break
        error = _windows_primitives._windows_last_error()
        if error == 5:
            candidate_handle = create_file(
                str(storage_path),
                owner_preserving_access,
                0 if exclusive else _WINDOWS_FILE_SHARE_READ,
                None,
                _WINDOWS_OPEN_EXISTING,
                _WINDOWS_FILE_FLAG_OPEN_REPARSE_POINT,
                None,
            )
            if candidate_handle not in (None, ctypes.c_void_p(-1).value):
                raw_handle = cast(int, candidate_handle)
                break
            error = _windows_primitives._windows_last_error()
        if error not in {5, 32, 33} or attempt + 1 >= MAX_CONFIG_READ_ATTEMPTS:
            raise ConfigurationError(f"could not open private Windows file ({error}): {path}")
        observed = os.lstat(storage_path)
        if not (
            stat.S_ISREG(observed.st_mode)
            and not _is_reparse_stat(observed)
            and observed.st_nlink == expected_nlink
            and os.path.samestat(before, observed)
        ):
            raise ConfigurationError(f"private Windows file changed while awaiting access: {path}")
        time.sleep(CONFIG_READ_RETRY_SECONDS)
    if raw_handle is None:  # pragma: no cover - loop exits by success or exception
        raise ConfigurationError(f"could not open private Windows file: {path}")
    handle = ctypes.c_void_p(raw_handle)
    try:
        get_information = kernel32.GetFileInformationByHandle
        get_information.argtypes = [
            ctypes.c_void_p,
            ctypes.POINTER(_WindowsFileInformation),
        ]
        get_information.restype = ctypes.c_int
        information = _WindowsFileInformation()
        if not get_information(handle, ctypes.byref(information)):
            error = _windows_primitives._windows_last_error()
            raise ConfigurationError(f"could not inspect private Windows file ({error}): {path}")
        file_index = (int(information.file_index_high) << 32) | int(information.file_index_low)
        after = os.lstat(storage_path)
        if not (
            not information.attributes & _WINDOWS_FILE_ATTRIBUTE_DIRECTORY
            and not information.attributes & _WINDOWS_FILE_ATTRIBUTE_REPARSE_POINT
            and information.number_of_links == expected_nlink
            and before.st_ino == file_index
            and os.path.samestat(before, after)
            and after.st_nlink == expected_nlink
        ):
            raise ConfigurationError(f"private Windows file changed while opening: {path}")
        _windows_paths.ensure_private_configuration_windows_handle(
            storage_path,
            handle=handle,
            directory=False,
        )
        confirmed = os.lstat(storage_path)
        if not os.path.samestat(after, confirmed) or confirmed.st_nlink != expected_nlink:
            raise ConfigurationError(f"private Windows file changed while securing: {path}")
        descriptor_flags = os.O_RDONLY | getattr(os, "O_BINARY", 0)
        descriptor = _windows_primitives._open_windows_os_file_handle(raw_handle, descriptor_flags)
        raw_handle = None
        return descriptor
    finally:
        if raw_handle not in (None, ctypes.c_void_p(-1).value):
            _windows_primitives._close_windows_handle(handle, kernel32=kernel32)

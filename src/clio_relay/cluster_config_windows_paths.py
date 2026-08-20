"""Create and privately ACL configuration files/directories on Windows.

Part of the `cluster_config` Windows-security split (iowarp/clio-relay#231).
This owns creating a new owner-private file/directory (the SDDL-built
security descriptor plus the `CreateFileW`/`CreateDirectoryW` calls),
applying/re-asserting the exact owner-private DACL on an existing path
(`_set_private_windows_acl`, which reads the ACL back through
`cluster_config_windows_acl.py`), and the public
`ensure_private_configuration_*`/`open_private_atomic_file` entry points
every other module in the codebase imports from the `cluster_config` facade.

At 490 lines this sits above the 150-500 sweet spot: `_set_private_windows_acl`
depends on `_build_private_windows_security_descriptor`,
`_open_windows_configuration_handle`, and `_validate_windows_configuration_handle`
(the create/open side), while those in turn have no need of
`_set_private_windows_acl` -- splitting it out to join
`cluster_config_windows_acl.py` would make that module depend back on this one,
recreating the very cycle this split exists to avoid. One owner, not a forced
cut, per the same precedent this codebase's own decomposition history already
documents for comparably-shaped clusters.

Depends on `cluster_config_windows_primitives.py` (imported as a module, so
`tests/test_cluster_config.py`'s `monkeypatch.setattr` against that module's
names reaches every call site here) and `cluster_config_windows_acl.py`
(likewise, for `_verify_private_windows_acl`).
"""

from __future__ import annotations

import ctypes
import os
import stat
from pathlib import Path
from typing import Any, BinaryIO, cast

from clio_relay import cluster_config_windows_acl as _windows_acl
from clio_relay import cluster_config_windows_primitives as _windows_primitives
from clio_relay.cluster_config_io import _is_reparse_stat
from clio_relay.cluster_config_windows_primitives import (
    _WINDOWS_CREATE_NEW,
    _WINDOWS_DACL_SECURITY_INFORMATION,
    _WINDOWS_ERROR_ALREADY_EXISTS,
    _WINDOWS_FILE_ATTRIBUTE_DIRECTORY,
    _WINDOWS_FILE_ATTRIBUTE_NORMAL,
    _WINDOWS_FILE_ATTRIBUTE_REPARSE_POINT,
    _WINDOWS_FILE_FLAG_BACKUP_SEMANTICS,
    _WINDOWS_FILE_FLAG_OPEN_REPARSE_POINT,
    _WINDOWS_FILE_SHARE_READ,
    _WINDOWS_FILE_SHARE_WRITE,
    _WINDOWS_GENERIC_READ,
    _WINDOWS_GENERIC_WRITE,
    _WINDOWS_OPEN_EXISTING,
    _WINDOWS_OWNER_SECURITY_INFORMATION,
    _WINDOWS_PROTECTED_DACL_SECURITY_INFORMATION,
    _WINDOWS_READ_CONTROL,
    _WINDOWS_SE_FILE_OBJECT,
    _WINDOWS_WRITE_DAC,
    _WINDOWS_WRITE_OWNER,
    _WindowsFileInformation,
    _WindowsSecurityAttributes,
)
from clio_relay.errors import ConfigurationError
from clio_relay.filesystem_paths import internal_filesystem_path


def _build_private_windows_security_descriptor(
    *,
    directory: bool,
    advapi32: Any,
    owner_sid: str,
    path: Path,
) -> ctypes.c_void_p:
    sddl = (
        f"O:{owner_sid}D:P(A;OICI;FA;;;OW)(A;OICI;FA;;;SY)(A;OICI;FA;;;BA)"
        if directory
        else f"O:{owner_sid}D:P(A;;FA;;;OW)(A;;FA;;;SY)(A;;FA;;;BA)"
    )
    descriptor = ctypes.c_void_p()
    convert = advapi32.ConvertStringSecurityDescriptorToSecurityDescriptorW
    convert.argtypes = [
        ctypes.c_wchar_p,
        ctypes.c_uint32,
        ctypes.POINTER(ctypes.c_void_p),
        ctypes.POINTER(ctypes.c_uint32),
    ]
    convert.restype = ctypes.c_int
    if not convert(sddl, 1, ctypes.byref(descriptor), None):
        error = _windows_primitives._windows_last_error()
        raise ConfigurationError(f"could not build private Windows ACL ({error}): {path}")
    return descriptor


def _create_private_windows_atomic_descriptor(path: Path) -> int:
    kernel32 = _windows_primitives._load_windows_library("kernel32")
    advapi32 = _windows_primitives._load_windows_library("advapi32")
    owner_sid = _windows_primitives._current_windows_user_sid(
        advapi32=advapi32,
        kernel32=kernel32,
        path=path,
    )
    security_descriptor = _build_private_windows_security_descriptor(
        directory=False,
        advapi32=advapi32,
        owner_sid=owner_sid,
        path=path,
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
    create_error = 0
    try:
        raw_handle = create_file(
            str(internal_filesystem_path(path, force_extended=True)),
            _WINDOWS_GENERIC_WRITE
            | _WINDOWS_READ_CONTROL
            | _WINDOWS_WRITE_DAC
            | _WINDOWS_WRITE_OWNER,
            0,
            ctypes.byref(security_attributes),
            _WINDOWS_CREATE_NEW,
            _WINDOWS_FILE_ATTRIBUTE_NORMAL,
            None,
        )
        if raw_handle in (None, ctypes.c_void_p(-1).value):
            create_error = _windows_primitives._windows_last_error()
    finally:
        _windows_primitives._free_windows_local(security_descriptor, kernel32=kernel32)
    if raw_handle in (None, ctypes.c_void_p(-1).value):
        raise _windows_primitives._windows_error(create_error)
    try:
        descriptor_flags = os.O_WRONLY | getattr(os, "O_BINARY", 0)
        return _windows_primitives._open_windows_os_file_handle(
            cast(int, raw_handle), descriptor_flags
        )
    except BaseException:
        _windows_primitives._close_windows_handle(ctypes.c_void_p(raw_handle), kernel32=kernel32)
        raise


def _create_private_windows_directory(path: Path, *, exist_ok: bool = True) -> None:
    kernel32 = _windows_primitives._load_windows_library("kernel32")
    advapi32 = _windows_primitives._load_windows_library("advapi32")
    owner_sid = _windows_primitives._current_windows_user_sid(
        advapi32=advapi32,
        kernel32=kernel32,
        path=path,
    )
    security_descriptor = _build_private_windows_security_descriptor(
        directory=True,
        advapi32=advapi32,
        owner_sid=owner_sid,
        path=path,
    )
    security_attributes = _WindowsSecurityAttributes(
        length=ctypes.sizeof(_WindowsSecurityAttributes),
        security_descriptor=security_descriptor,
        inherit_handle=0,
    )
    create_directory = kernel32.CreateDirectoryW
    create_directory.argtypes = [
        ctypes.c_wchar_p,
        ctypes.POINTER(_WindowsSecurityAttributes),
    ]
    create_directory.restype = ctypes.c_int
    try:
        storage_path = internal_filesystem_path(path, force_extended=True)
        created = create_directory(str(storage_path), ctypes.byref(security_attributes))
        if not created:
            error = _windows_primitives._windows_last_error()
            if error != _WINDOWS_ERROR_ALREADY_EXISTS or not exist_ok:
                raise ConfigurationError(
                    f"could not create private Windows configuration directory ({error}): {path}"
                )
    finally:
        _windows_primitives._free_windows_local(security_descriptor, kernel32=kernel32)


def _open_windows_configuration_handle(
    path: Path,
    *,
    directory: bool,
    kernel32: Any,
    write_owner: bool = False,
) -> ctypes.c_void_p:
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
    flags = _WINDOWS_FILE_FLAG_OPEN_REPARSE_POINT
    if directory:
        flags |= _WINDOWS_FILE_FLAG_BACKUP_SEMANTICS
    desired_access = _WINDOWS_GENERIC_READ | _WINDOWS_READ_CONTROL | _WINDOWS_WRITE_DAC
    if write_owner:
        desired_access |= _WINDOWS_WRITE_OWNER
    raw_handle = create_file(
        str(internal_filesystem_path(path, force_extended=True)),
        desired_access,
        _WINDOWS_FILE_SHARE_READ | _WINDOWS_FILE_SHARE_WRITE,
        None,
        _WINDOWS_OPEN_EXISTING,
        flags,
        None,
    )
    if raw_handle in (None, ctypes.c_void_p(-1).value):
        error = _windows_primitives._windows_last_error()
        raise ConfigurationError(f"could not open Windows configuration path ({error}): {path}")
    handle = ctypes.c_void_p(raw_handle)
    try:
        _validate_windows_configuration_handle(
            handle,
            directory=directory,
            kernel32=kernel32,
            path=path,
        )
    except BaseException:
        _windows_primitives._close_windows_handle(handle, kernel32=kernel32)
        raise
    return handle


def _validate_windows_configuration_handle(
    handle: ctypes.c_void_p,
    *,
    directory: bool,
    kernel32: Any,
    path: Path,
) -> None:
    get_information = kernel32.GetFileInformationByHandle
    get_information.argtypes = [ctypes.c_void_p, ctypes.POINTER(_WindowsFileInformation)]
    get_information.restype = ctypes.c_int
    information = _WindowsFileInformation()
    if not get_information(handle, ctypes.byref(information)):
        error = _windows_primitives._windows_last_error()
        raise ConfigurationError(f"could not inspect Windows configuration path ({error}): {path}")
    is_directory = bool(information.attributes & _WINDOWS_FILE_ATTRIBUTE_DIRECTORY)
    is_reparse_point = bool(information.attributes & _WINDOWS_FILE_ATTRIBUTE_REPARSE_POINT)
    if is_directory != directory or is_reparse_point:
        kind = "directory" if directory else "file"
        raise ConfigurationError(f"configuration path is not a regular {kind}: {path}")


def _set_private_windows_acl(
    path: Path,
    *,
    directory: bool,
    existing_handle: ctypes.c_void_p | None = None,
) -> None:
    advapi32 = _windows_primitives._load_windows_library("advapi32")
    kernel32 = _windows_primitives._load_windows_library("kernel32")
    user_sid = _windows_primitives._current_windows_user_sid(
        advapi32=advapi32,
        kernel32=kernel32,
        path=path,
    )
    default_owner_sid = _windows_primitives._current_windows_default_owner_sid(
        advapi32=advapi32,
        kernel32=kernel32,
        path=path,
    )
    descriptor = _build_private_windows_security_descriptor(
        directory=directory,
        advapi32=advapi32,
        owner_sid=user_sid,
        path=path,
    )
    handle = existing_handle
    owns_handle = existing_handle is None
    try:
        dacl = ctypes.c_void_p()
        dacl_present = ctypes.c_int()
        dacl_defaulted = ctypes.c_int()
        get_dacl = advapi32.GetSecurityDescriptorDacl
        get_dacl.argtypes = [
            ctypes.c_void_p,
            ctypes.POINTER(ctypes.c_int),
            ctypes.POINTER(ctypes.c_void_p),
            ctypes.POINTER(ctypes.c_int),
        ]
        get_dacl.restype = ctypes.c_int
        if not get_dacl(
            descriptor,
            ctypes.byref(dacl_present),
            ctypes.byref(dacl),
            ctypes.byref(dacl_defaulted),
        ):
            error = _windows_primitives._windows_last_error()
            raise ConfigurationError(f"could not read private Windows ACL ({error}): {path}")
        if not dacl_present.value or dacl.value is None:
            raise ConfigurationError(f"private Windows ACL has no DACL: {path}")
        if handle is None:
            handle = _open_windows_configuration_handle(
                path,
                directory=directory,
                kernel32=kernel32,
            )
        else:
            _validate_windows_configuration_handle(
                handle,
                directory=directory,
                kernel32=kernel32,
                path=path,
            )
        owner_sid = _windows_primitives._windows_object_owner_sid(
            handle,
            advapi32=advapi32,
            kernel32=kernel32,
            path=path,
        )
        _windows_primitives._require_current_windows_owner(
            owner_sid=owner_sid,
            user_sid=user_sid,
            default_owner_sid=default_owner_sid,
            path=path,
        )
        # Elevated tokens can assign their TokenOwner SID (commonly the local
        # Administrators group) to objects this process creates.  Accept only
        # that token-proven default, then normalize it to TokenUser below.
        descriptor_owner = ctypes.c_void_p()
        owner_defaulted = ctypes.c_int()
        get_owner = advapi32.GetSecurityDescriptorOwner
        get_owner.argtypes = [
            ctypes.c_void_p,
            ctypes.POINTER(ctypes.c_void_p),
            ctypes.POINTER(ctypes.c_int),
        ]
        get_owner.restype = ctypes.c_int
        if not get_owner(
            descriptor,
            ctypes.byref(descriptor_owner),
            ctypes.byref(owner_defaulted),
        ):
            error = _windows_primitives._windows_last_error()
            raise ConfigurationError(
                f"could not read private Windows configuration owner ({error}): {path}"
            )
        if descriptor_owner.value is None:
            raise ConfigurationError(f"private Windows configuration owner has no SID: {path}")
        normalize_owner = owner_sid != user_sid
        if normalize_owner and owns_handle:
            _windows_primitives._close_windows_handle(handle, kernel32=kernel32)
            handle = None
            handle = _open_windows_configuration_handle(
                path,
                directory=directory,
                kernel32=kernel32,
                write_owner=True,
            )
            owner_sid = _windows_primitives._windows_object_owner_sid(
                handle,
                advapi32=advapi32,
                kernel32=kernel32,
                path=path,
            )
            _windows_primitives._require_current_windows_owner(
                owner_sid=owner_sid,
                user_sid=user_sid,
                default_owner_sid=default_owner_sid,
                path=path,
            )
            normalize_owner = owner_sid != user_sid
        set_security = advapi32.SetSecurityInfo
        set_security.argtypes = [
            ctypes.c_void_p,
            ctypes.c_uint32,
            ctypes.c_uint32,
            ctypes.c_void_p,
            ctypes.c_void_p,
            ctypes.c_void_p,
            ctypes.c_void_p,
        ]
        set_security.restype = ctypes.c_uint32
        result = set_security(
            handle,
            _WINDOWS_SE_FILE_OBJECT,
            _WINDOWS_DACL_SECURITY_INFORMATION
            | _WINDOWS_PROTECTED_DACL_SECURITY_INFORMATION
            | (_WINDOWS_OWNER_SECURITY_INFORMATION if normalize_owner else 0),
            descriptor_owner if normalize_owner else None,
            None,
            dacl,
            None,
        )
        if result != 0:
            raise ConfigurationError(
                f"could not protect Windows configuration ACL ({result}): {path}"
            )
        _windows_acl._verify_private_windows_acl(
            handle,
            directory=directory,
            expected_owner_sid=user_sid,
            advapi32=advapi32,
            kernel32=kernel32,
            path=path,
        )
    finally:
        if owns_handle and handle is not None:
            _windows_primitives._close_windows_handle(handle, kernel32=kernel32)
        _windows_primitives._free_windows_local(descriptor, kernel32=kernel32)


def ensure_private_configuration_directory(path: Path) -> None:
    """Create a configuration directory privately, then verify its exact protections."""
    if os.name != "nt":
        path.mkdir(mode=0o700, parents=True, exist_ok=True)
        ensure_private_configuration_path(path, directory=True)
        return
    missing: list[Path] = []
    current = path
    while not current.exists():
        missing.append(current)
        parent = current.parent
        if parent == current:
            raise ConfigurationError(f"configuration directory has no existing parent: {path}")
        current = parent
    if not missing:
        ensure_private_configuration_path(path, directory=True)
        return

    kernel32 = _windows_primitives._load_windows_library("kernel32")
    held_handles: list[ctypes.c_void_p] = []
    try:
        for directory in reversed(missing):
            _create_private_windows_directory(directory)
            handle = _open_windows_configuration_handle(
                directory,
                directory=True,
                kernel32=kernel32,
                write_owner=True,
            )
            try:
                _set_private_windows_acl(
                    directory,
                    directory=True,
                    existing_handle=handle,
                )
            except BaseException:
                _windows_primitives._close_windows_handle(handle, kernel32=kernel32)
                raise
            held_handles.append(handle)
    finally:
        for handle in reversed(held_handles):
            _windows_primitives._close_windows_handle(handle, kernel32=kernel32)


def create_private_configuration_directory(path: Path) -> None:
    """Create exactly one owner-private directory without accepting an existing path."""
    if os.name != "nt":
        os.mkdir(path, 0o700)
        ensure_private_configuration_path(path, directory=True)
        return
    _create_private_windows_directory(path, exist_ok=False)
    ensure_private_configuration_path(path, directory=True)


def ensure_private_configuration_path(path: Path, *, directory: bool) -> None:
    """Enforce private ownership on a configuration file or state directory."""
    if os.name != "nt":
        value = os.lstat(path)
        expected_type = stat.S_ISDIR(value.st_mode) if directory else stat.S_ISREG(value.st_mode)
        if not expected_type or _is_reparse_stat(value):
            kind = "directory" if directory else "file"
            raise ConfigurationError(f"configuration path is not a regular {kind}: {path}")
        if hasattr(os, "getuid") and value.st_uid != os.getuid():
            raise ConfigurationError(f"configuration path is not owned by this user: {path}")
        if stat.S_IMODE(value.st_mode) & 0o022:
            raise ConfigurationError(
                f"configuration path is writable by group or other users: {path}"
            )
        return
    _set_private_windows_acl(path, directory=directory)


def ensure_private_configuration_windows_handle(
    path: Path,
    *,
    handle: ctypes.c_void_p,
    directory: bool,
) -> None:
    """Enforce and verify a private ACL through an exact open Windows handle."""
    if os.name != "nt":  # pragma: no cover - explicit platform contract
        raise ConfigurationError("Windows handle ACL enforcement is unavailable")
    _set_private_windows_acl(
        path,
        directory=directory,
        existing_handle=handle,
    )


def open_private_atomic_file(path: Path) -> BinaryIO:
    """Create a new private regular file for an eventual atomic replacement."""
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_BINARY", 0)
    flags |= getattr(os, "O_CLOEXEC", 0)
    descriptor = (
        _create_private_windows_atomic_descriptor(path)
        if os.name == "nt"
        else os.open(path, flags, 0o600)
    )
    try:
        if os.name == "nt":
            _set_private_windows_acl(
                path,
                directory=False,
                existing_handle=ctypes.c_void_p(
                    _windows_primitives._windows_os_file_handle(descriptor)
                ),
            )
        else:
            ensure_private_configuration_path(path, directory=False)
        return os.fdopen(descriptor, "wb")
    except BaseException:
        os.close(descriptor)
        raise

"""Low-level Win32 primitives for the private-configuration ACL machinery.

This is the leaf layer of the `cluster_config` Windows-security split
(iowarp/clio-relay#231): Win32 constants, the raw ctypes structure layouts,
library/error/handle primitives, and current-user/owner SID resolution. None
of these depend on anything else in the split -- every other Windows-facing
owner module (`cluster_config_windows_acl.py`, `cluster_config_windows_paths.py`,
`cluster_config_windows_guard.py`) imports this module and calls through the
module object (`_windows_primitives.<name>(...)`) rather than importing these
names by bare reference, so `tests/test_cluster_config.py`'s
`monkeypatch.setattr(cluster_config_windows_primitives, "<name>", ...)` calls
reach every call site regardless of which owner module it lives in.
"""

from __future__ import annotations

import ctypes
from collections.abc import Callable
from importlib import import_module
from pathlib import Path
from typing import Any, cast

from clio_relay.errors import ConfigurationError

_WINDOWS_READ_CONTROL = 0x00020000
_WINDOWS_WRITE_DAC = 0x00040000
_WINDOWS_WRITE_OWNER = 0x00080000
_WINDOWS_GENERIC_READ = 0x80000000
_WINDOWS_GENERIC_WRITE = 0x40000000
_WINDOWS_DELETE = 0x00010000
_WINDOWS_FILE_SHARE_READ = 0x00000001
_WINDOWS_FILE_SHARE_WRITE = 0x00000002
_WINDOWS_CREATE_NEW = 1
_WINDOWS_OPEN_EXISTING = 3
_WINDOWS_FILE_ATTRIBUTE_NORMAL = 0x00000080
_WINDOWS_FILE_ATTRIBUTE_DIRECTORY = 0x00000010
_WINDOWS_FILE_ATTRIBUTE_REPARSE_POINT = 0x00000400
_WINDOWS_FILE_FLAG_OPEN_REPARSE_POINT = 0x00200000
_WINDOWS_FILE_FLAG_BACKUP_SEMANTICS = 0x02000000
_WINDOWS_FILE_FLAG_DELETE_ON_CLOSE = 0x04000000
_WINDOWS_SE_FILE_OBJECT = 1
_WINDOWS_OWNER_SECURITY_INFORMATION = 0x00000001
_WINDOWS_DACL_SECURITY_INFORMATION = 0x00000004
_WINDOWS_PROTECTED_DACL_SECURITY_INFORMATION = 0x80000000
_WINDOWS_SE_DACL_PROTECTED = 0x1000
_WINDOWS_ACL_SIZE_INFORMATION = 2
_WINDOWS_ACCESS_ALLOWED_ACE_TYPE = 0
_WINDOWS_FILE_ALL_ACCESS = 0x001F01FF
_WINDOWS_OBJECT_INHERIT_ACE = 0x01
_WINDOWS_CONTAINER_INHERIT_ACE = 0x02
_WINDOWS_PRIVATE_SIDS = {"S-1-3-4", "S-1-5-18", "S-1-5-32-544"}
_WINDOWS_TOKEN_QUERY = 0x0008
_WINDOWS_TOKEN_USER = 1
_WINDOWS_TOKEN_OWNER = 4
_WINDOWS_ERROR_ALREADY_EXISTS = 183


class _WindowsFileTime(ctypes.Structure):
    _fields_ = [("low", ctypes.c_uint32), ("high", ctypes.c_uint32)]


class _WindowsFileInformation(ctypes.Structure):
    _fields_ = [
        ("attributes", ctypes.c_uint32),
        ("creation_time", _WindowsFileTime),
        ("last_access_time", _WindowsFileTime),
        ("last_write_time", _WindowsFileTime),
        ("volume_serial_number", ctypes.c_uint32),
        ("file_size_high", ctypes.c_uint32),
        ("file_size_low", ctypes.c_uint32),
        ("number_of_links", ctypes.c_uint32),
        ("file_index_high", ctypes.c_uint32),
        ("file_index_low", ctypes.c_uint32),
    ]


class _WindowsAclSizeInformation(ctypes.Structure):
    _fields_ = [
        ("ace_count", ctypes.c_uint32),
        ("acl_bytes_in_use", ctypes.c_uint32),
        ("acl_bytes_free", ctypes.c_uint32),
    ]


class _WindowsAceHeader(ctypes.Structure):
    _fields_ = [
        ("ace_type", ctypes.c_ubyte),
        ("ace_flags", ctypes.c_ubyte),
        ("ace_size", ctypes.c_uint16),
    ]


class _WindowsAccessAllowedAce(ctypes.Structure):
    _fields_ = [
        ("header", _WindowsAceHeader),
        ("mask", ctypes.c_uint32),
        ("sid_start", ctypes.c_uint32),
    ]


class _WindowsSidAndAttributes(ctypes.Structure):
    _fields_ = [("sid", ctypes.c_void_p), ("attributes", ctypes.c_uint32)]


class _WindowsTokenUser(ctypes.Structure):
    _fields_ = [("user", _WindowsSidAndAttributes)]


class _WindowsTokenOwner(ctypes.Structure):
    _fields_ = [("owner", ctypes.c_void_p)]


class _WindowsSecurityAttributes(ctypes.Structure):
    _fields_ = [
        ("length", ctypes.c_uint32),
        ("security_descriptor", ctypes.c_void_p),
        ("inherit_handle", ctypes.c_int),
    ]


def _load_windows_library(name: str) -> Any:
    """Load a Win32 library without exposing platform-specific ctypes stubs."""
    factory = cast(Callable[..., Any], vars(ctypes)["WinDLL"])
    return factory(name, use_last_error=True)


def _windows_last_error() -> int:
    """Return the calling thread's Win32 last-error value."""
    get_last_error = cast(Callable[[], int], vars(ctypes)["get_last_error"])
    return get_last_error()


def _windows_error(error: int) -> OSError:
    """Build the native Python exception for a Win32 error code."""
    factory = cast(Callable[[int], OSError], vars(ctypes)["WinError"])
    return factory(error)


def _windows_os_file_handle(descriptor: int) -> int:
    """Return the Win32 handle owned by a CRT file descriptor."""
    module = import_module("msvcrt")
    get_osfhandle = cast(Callable[[int], int], vars(module)["get_osfhandle"])
    return get_osfhandle(descriptor)


def _open_windows_os_file_handle(handle: int, flags: int) -> int:
    """Transfer ownership of a Win32 handle to a CRT file descriptor."""
    module = import_module("msvcrt")
    open_osfhandle = cast(Callable[[int, int], int], vars(module)["open_osfhandle"])
    return open_osfhandle(handle, flags)


def _close_windows_handle(handle: ctypes.c_void_p, *, kernel32: Any) -> None:
    close_handle = kernel32.CloseHandle
    close_handle.argtypes = [ctypes.c_void_p]
    close_handle.restype = ctypes.c_int
    close_handle(handle)


def _free_windows_local(pointer: ctypes.c_void_p, *, kernel32: Any) -> None:
    local_free = kernel32.LocalFree
    local_free.argtypes = [ctypes.c_void_p]
    local_free.restype = ctypes.c_void_p
    local_free(pointer)


def _windows_sid_text(
    sid_pointer: ctypes.c_void_p,
    *,
    advapi32: Any,
    kernel32: Any,
    path: Path,
    context: str,
) -> str:
    sid_to_text = advapi32.ConvertSidToStringSidW
    sid_to_text.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_void_p)]
    sid_to_text.restype = ctypes.c_int
    sid_text_pointer = ctypes.c_void_p()
    if not sid_to_text(sid_pointer, ctypes.byref(sid_text_pointer)):
        error = _windows_last_error()
        raise ConfigurationError(f"could not inspect Windows {context} SID ({error}): {path}")
    try:
        return ctypes.wstring_at(sid_text_pointer)
    finally:
        local_free = kernel32.LocalFree
        local_free.argtypes = [ctypes.c_void_p]
        local_free.restype = ctypes.c_void_p
        local_free(sid_text_pointer)


def _current_windows_token_sid(
    *,
    information_class: int,
    minimum_size: int,
    context: str,
    advapi32: Any,
    kernel32: Any,
    path: Path,
) -> str:
    get_current_process = kernel32.GetCurrentProcess
    get_current_process.argtypes = []
    get_current_process.restype = ctypes.c_void_p
    open_process_token = advapi32.OpenProcessToken
    open_process_token.argtypes = [
        ctypes.c_void_p,
        ctypes.c_uint32,
        ctypes.POINTER(ctypes.c_void_p),
    ]
    open_process_token.restype = ctypes.c_int
    token = ctypes.c_void_p()
    if not open_process_token(
        get_current_process(),
        _WINDOWS_TOKEN_QUERY,
        ctypes.byref(token),
    ):
        error = _windows_last_error()
        raise ConfigurationError(f"could not inspect current Windows user ({error}): {path}")
    try:
        get_token_information = advapi32.GetTokenInformation
        get_token_information.argtypes = [
            ctypes.c_void_p,
            ctypes.c_uint32,
            ctypes.c_void_p,
            ctypes.c_uint32,
            ctypes.POINTER(ctypes.c_uint32),
        ]
        get_token_information.restype = ctypes.c_int
        required = ctypes.c_uint32()
        get_token_information(
            token,
            information_class,
            None,
            0,
            ctypes.byref(required),
        )
        if required.value < minimum_size:
            error = _windows_last_error()
            raise ConfigurationError(
                f"could not size current Windows {context} identity ({error}): {path}"
            )
        buffer = ctypes.create_string_buffer(required.value)
        if not get_token_information(
            token,
            information_class,
            buffer,
            required.value,
            ctypes.byref(required),
        ):
            error = _windows_last_error()
            raise ConfigurationError(f"could not read current Windows {context} ({error}): {path}")
        sid = ctypes.cast(buffer, ctypes.POINTER(ctypes.c_void_p)).contents
        if sid.value is None:
            raise ConfigurationError(f"current Windows {context} has no SID: {path}")
        return _windows_sid_text(
            sid,
            advapi32=advapi32,
            kernel32=kernel32,
            path=path,
            context=f"current {context}",
        )
    finally:
        _close_windows_handle(token, kernel32=kernel32)


def _current_windows_user_sid(*, advapi32: Any, kernel32: Any, path: Path) -> str:
    return _current_windows_token_sid(
        information_class=_WINDOWS_TOKEN_USER,
        minimum_size=ctypes.sizeof(_WindowsTokenUser),
        context="user",
        advapi32=advapi32,
        kernel32=kernel32,
        path=path,
    )


def _current_windows_default_owner_sid(*, advapi32: Any, kernel32: Any, path: Path) -> str:
    return _current_windows_token_sid(
        information_class=_WINDOWS_TOKEN_OWNER,
        minimum_size=ctypes.sizeof(_WindowsTokenOwner),
        context="default owner",
        advapi32=advapi32,
        kernel32=kernel32,
        path=path,
    )


def _windows_object_owner_sid(
    handle: ctypes.c_void_p,
    *,
    advapi32: Any,
    kernel32: Any,
    path: Path,
) -> str:
    get_security = advapi32.GetSecurityInfo
    get_security.argtypes = [
        ctypes.c_void_p,
        ctypes.c_uint32,
        ctypes.c_uint32,
        ctypes.POINTER(ctypes.c_void_p),
        ctypes.c_void_p,
        ctypes.c_void_p,
        ctypes.c_void_p,
        ctypes.POINTER(ctypes.c_void_p),
    ]
    get_security.restype = ctypes.c_uint32
    owner = ctypes.c_void_p()
    descriptor = ctypes.c_void_p()
    result = get_security(
        handle,
        _WINDOWS_SE_FILE_OBJECT,
        _WINDOWS_OWNER_SECURITY_INFORMATION,
        ctypes.byref(owner),
        None,
        None,
        None,
        ctypes.byref(descriptor),
    )
    if result != 0:
        raise ConfigurationError(
            f"could not inspect Windows configuration owner ({result}): {path}"
        )
    try:
        if owner.value is None:
            raise ConfigurationError(f"Windows configuration path has no owner: {path}")
        return _windows_sid_text(
            owner,
            advapi32=advapi32,
            kernel32=kernel32,
            path=path,
            context="configuration owner",
        )
    finally:
        local_free = kernel32.LocalFree
        local_free.argtypes = [ctypes.c_void_p]
        local_free.restype = ctypes.c_void_p
        local_free(descriptor)


def _require_current_windows_owner(
    *,
    owner_sid: str,
    user_sid: str,
    default_owner_sid: str | None = None,
    path: Path,
) -> None:
    permitted_owner_sids = {user_sid}
    if default_owner_sid is not None:
        permitted_owner_sids.add(default_owner_sid)
    if owner_sid not in permitted_owner_sids:
        raise ConfigurationError(f"configuration path is not owned by this user: {path}")

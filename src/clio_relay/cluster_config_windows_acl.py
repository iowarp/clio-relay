"""Read/verify the private-configuration Windows ACL.

Part of the `cluster_config` Windows-security split (iowarp/clio-relay#231).
This module owns reading back a DACL's entries and verifying that a path's
ACL is the exact owner-private set the split's write side
(`cluster_config_windows_paths.py`) installs. It depends only on the leaf
primitives in `cluster_config_windows_primitives.py`, imported as a module so
`tests/test_cluster_config.py`'s `monkeypatch.setattr` calls against that
module's names reach these call sites too.
"""

from __future__ import annotations

import ctypes
from pathlib import Path
from typing import Any, cast

from clio_relay import cluster_config_windows_primitives as _windows_primitives
from clio_relay.cluster_config_windows_primitives import (
    _WINDOWS_ACCESS_ALLOWED_ACE_TYPE,
    _WINDOWS_ACL_SIZE_INFORMATION,
    _WINDOWS_CONTAINER_INHERIT_ACE,
    _WINDOWS_DACL_SECURITY_INFORMATION,
    _WINDOWS_FILE_ALL_ACCESS,
    _WINDOWS_OBJECT_INHERIT_ACE,
    _WINDOWS_OWNER_SECURITY_INFORMATION,
    _WINDOWS_PRIVATE_SIDS,
    _WINDOWS_SE_DACL_PROTECTED,
    _WINDOWS_SE_FILE_OBJECT,
    _WindowsAccessAllowedAce,
    _WindowsAclSizeInformation,
)
from clio_relay.errors import ConfigurationError


def _windows_acl_entries(
    dacl: ctypes.c_void_p,
    *,
    advapi32: Any,
    kernel32: Any,
    path: Path,
) -> list[tuple[str, int, int]]:
    get_acl_information = advapi32.GetAclInformation
    get_acl_information.argtypes = [
        ctypes.c_void_p,
        ctypes.POINTER(_WindowsAclSizeInformation),
        ctypes.c_uint32,
        ctypes.c_uint32,
    ]
    get_acl_information.restype = ctypes.c_int
    information = _WindowsAclSizeInformation()
    if not get_acl_information(
        dacl,
        ctypes.byref(information),
        ctypes.sizeof(information),
        _WINDOWS_ACL_SIZE_INFORMATION,
    ):
        error = _windows_primitives._windows_last_error()
        raise ConfigurationError(f"could not inspect Windows configuration ACL ({error}): {path}")
    get_ace = advapi32.GetAce
    get_ace.argtypes = [ctypes.c_void_p, ctypes.c_uint32, ctypes.POINTER(ctypes.c_void_p)]
    get_ace.restype = ctypes.c_int
    entries: list[tuple[str, int, int]] = []
    for index in range(information.ace_count):
        ace_pointer = ctypes.c_void_p()
        if not get_ace(dacl, index, ctypes.byref(ace_pointer)):
            error = _windows_primitives._windows_last_error()
            raise ConfigurationError(
                f"could not inspect Windows configuration ACE ({error}): {path}"
            )
        ace = ctypes.cast(ace_pointer, ctypes.POINTER(_WindowsAccessAllowedAce)).contents
        if (
            ace.header.ace_type != _WINDOWS_ACCESS_ALLOWED_ACE_TYPE
            or ace.header.ace_size < ctypes.sizeof(_WindowsAccessAllowedAce)
        ):
            raise ConfigurationError(f"Windows configuration ACL has an unexpected ACE: {path}")
        sid_address = cast(int, ace_pointer.value) + _WindowsAccessAllowedAce.sid_start.offset
        sid = _windows_primitives._windows_sid_text(
            ctypes.c_void_p(sid_address),
            advapi32=advapi32,
            kernel32=kernel32,
            path=path,
            context="configuration ACE",
        )
        entries.append((sid, ace.mask, ace.header.ace_flags))
    return entries


def _verify_private_windows_acl(
    handle: ctypes.c_void_p,
    *,
    directory: bool,
    expected_owner_sid: str,
    advapi32: Any,
    kernel32: Any,
    path: Path,
) -> None:
    get_security = advapi32.GetSecurityInfo
    get_security.argtypes = [
        ctypes.c_void_p,
        ctypes.c_uint32,
        ctypes.c_uint32,
        ctypes.POINTER(ctypes.c_void_p),
        ctypes.c_void_p,
        ctypes.POINTER(ctypes.c_void_p),
        ctypes.c_void_p,
        ctypes.POINTER(ctypes.c_void_p),
    ]
    get_security.restype = ctypes.c_uint32
    owner = ctypes.c_void_p()
    dacl = ctypes.c_void_p()
    descriptor = ctypes.c_void_p()
    result = get_security(
        handle,
        _WINDOWS_SE_FILE_OBJECT,
        _WINDOWS_OWNER_SECURITY_INFORMATION | _WINDOWS_DACL_SECURITY_INFORMATION,
        ctypes.byref(owner),
        None,
        ctypes.byref(dacl),
        None,
        ctypes.byref(descriptor),
    )
    if result != 0:
        raise ConfigurationError(f"could not read back private Windows ACL ({result}): {path}")
    try:
        if owner.value is None:
            raise ConfigurationError(f"Windows configuration path has no owner: {path}")
        owner_sid = _windows_primitives._windows_sid_text(
            owner,
            advapi32=advapi32,
            kernel32=kernel32,
            path=path,
            context="configuration owner",
        )
        _windows_primitives._require_current_windows_owner(
            owner_sid=owner_sid,
            user_sid=expected_owner_sid,
            path=path,
        )
        if dacl.value is None:
            raise ConfigurationError(f"Windows configuration path has no private DACL: {path}")
        get_control = advapi32.GetSecurityDescriptorControl
        get_control.argtypes = [
            ctypes.c_void_p,
            ctypes.POINTER(ctypes.c_uint16),
            ctypes.POINTER(ctypes.c_uint32),
        ]
        get_control.restype = ctypes.c_int
        control = ctypes.c_uint16()
        revision = ctypes.c_uint32()
        if not get_control(descriptor, ctypes.byref(control), ctypes.byref(revision)):
            error = _windows_primitives._windows_last_error()
            raise ConfigurationError(
                f"could not verify Windows configuration ACL control ({error}): {path}"
            )
        if not control.value & _WINDOWS_SE_DACL_PROTECTED:
            raise ConfigurationError(f"Windows configuration ACL remains inherited: {path}")
        expected_flags = (
            _WINDOWS_OBJECT_INHERIT_ACE | _WINDOWS_CONTAINER_INHERIT_ACE if directory else 0
        )
        entries = _windows_acl_entries(
            dacl,
            advapi32=advapi32,
            kernel32=kernel32,
            path=path,
        )
        if (
            len(entries) != len(_WINDOWS_PRIVATE_SIDS)
            or {sid for sid, _mask, _flags in entries} != _WINDOWS_PRIVATE_SIDS
        ):
            raise ConfigurationError(f"Windows configuration ACL is not owner-private: {path}")
        if any(
            mask != _WINDOWS_FILE_ALL_ACCESS or flags != expected_flags
            for _sid, mask, flags in entries
        ):
            raise ConfigurationError(f"Windows configuration ACL grants unexpected access: {path}")
    finally:
        local_free = kernel32.LocalFree
        local_free.argtypes = [ctypes.c_void_p]
        local_free.restype = ctypes.c_void_p
        local_free(descriptor)

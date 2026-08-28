"""Verify -- never re-apply -- the private Windows ACL on a read path.

Part of the `cluster_config` Windows-security split (iowarp/clio-relay#231).

clio-relay#289 security-facet fix (owner ruling, 2026-08-28): "you apply them
and then let the OS check on reads; re-applying them has no value."
Configuration READ paths must VERIFY the private ACL instead of
RE-APPLYING it. `cluster_config_windows_paths._set_private_windows_acl`
(the write side) builds a security descriptor and calls `SetSecurityInfo`
-- a WRITE -- whose SID-normalization work can stall on LSA network
resolution: measured 4.4-5.1s per call, 15/15 consistent, during a
flaky-network window on a Microsoft-account box. Calling it on every read
(the pre-fix shape: `read_bounded_configuration_bytes` called
`ensure_private_configuration_path`, which calls `_set_private_windows_acl`
unconditionally) wedged the door's tools/list path and the release-gate
suite.

This module owns the read-only counterpart used by genuine read paths:

  1. Open the existing path read-only
     (`cluster_config_windows_paths._open_windows_configuration_handle`,
     which already performs the regular-file/no-reparse check via
     `_validate_windows_configuration_handle`).
  2. Confirm the DACL/owner the write side already installed still holds
     (`cluster_config_windows_acl._verify_private_windows_acl` -- SID
     VALUE comparison only, via `ConvertSidToStringSidW`/`GetSecurityInfo`;
     no `LookupAccountSid`/LSA name resolution, and no `SetSecurityInfo`).

A mismatch is a typed `ConfigurationError` naming the path -- NEVER a
silent heal-by-overwrite. Re-applying the ACL on a read would silently
overwrite (and thereby mask) evidence of tampering: a permissive or
foreign-owned file would come back "fixed" instead of refused, which is
strictly weaker than refusing outright. Refusing is therefore the only
correct read-path response to drift; healing belongs exclusively to the
explicit write/create paths (`open_private_atomic_file`,
`create_private_configuration_directory`,
`ensure_private_configuration_directory`), which are unchanged by this
module and keep applying+verifying the ACL exactly as before.

Depends on `cluster_config_windows_primitives.py`, `cluster_config_windows_
paths.py`, and `cluster_config_windows_acl.py` (each imported as a module,
so `tests/test_cluster_config.py`'s `monkeypatch.setattr` against those
modules' names reaches the call sites here too). `cluster_config_windows_
paths.py` does not import this module -- it is a pure downstream consumer of
the write-side owner modules, so no import cycle is introduced.
"""

from __future__ import annotations

import os
from pathlib import Path

from clio_relay import cluster_config_windows_acl as _windows_acl
from clio_relay import cluster_config_windows_paths as _windows_paths
from clio_relay import cluster_config_windows_primitives as _windows_primitives
from clio_relay.errors import ConfigurationError


def verify_private_configuration_windows_path(path: Path, *, directory: bool) -> None:
    """Verify the exact owner-private ACL on an existing Windows path, read-only.

    Never mutates the ACL: this function never calls `SetSecurityInfo` (the
    write primitive `_set_private_windows_acl` uses, and the one whose SID
    work measured the multi-second stall) and never resolves a SID to a
    name (`ConvertSidToStringSidW`/`GetSecurityInfo` are pure local SID
    VALUE operations -- no `LookupAccountSid`, no LSA round trip). The
    handle open itself reuses `_open_windows_configuration_handle`, the
    same shared primitive the write side uses to validate a handle before
    hardening it, which requests `WRITE_DAC` as one of its desired-access
    flags without ever exercising it -- Windows access checks are
    evaluated locally against the caller's already-resolved token, so
    requesting (not using) that right adds no network-dependent work.
    Raises `ConfigurationError` naming the path if the owner or DACL has
    drifted from the exact owner-private set the write side installs, if
    the DACL is not protected (still inherited), or if the path is not a
    regular file/directory (a reparse point, or the wrong file/directory
    kind).

    Callers on a genuine read path -- an already-created configuration file
    or directory whose protections were applied at write/create time --
    should call this (or the platform-agnostic
    `cluster_config_io.verify_private_configuration_path`) instead of
    `cluster_config_windows_paths.ensure_private_configuration_path`, which
    re-applies (writes) the ACL on every call. That re-apply is correct and
    required on write/create paths; on a read path it is both wasted work
    and, under LSA flakiness, a multi-second stall for no security benefit
    (the OS already enforces the ACL on every access once applied).
    """
    if os.name != "nt":  # pragma: no cover - explicit platform contract
        raise ConfigurationError("Windows read-only ACL verification is unavailable")
    kernel32 = _windows_primitives._load_windows_library(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        "kernel32"
    )
    advapi32 = _windows_primitives._load_windows_library(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        "advapi32"
    )
    user_sid = _windows_primitives._current_windows_user_sid(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        advapi32=advapi32,
        kernel32=kernel32,
        path=path,
    )
    handle = _windows_paths._open_windows_configuration_handle(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        path,
        directory=directory,
        kernel32=kernel32,
    )
    try:
        _windows_acl._verify_private_windows_acl(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            handle,
            directory=directory,
            expected_owner_sid=user_sid,
            advapi32=advapi32,
            kernel32=kernel32,
            path=path,
        )
    finally:
        _windows_primitives._close_windows_handle(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            handle, kernel32=kernel32
        )

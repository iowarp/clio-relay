"""Verify the private Windows ACL on a read path; heal drift loudly, never silently.

Part of the `cluster_config` Windows-security split (iowarp/clio-relay#231).

clio-relay#289 security-facet fix, controller design ruling (2026-08-28,
resolving the FIX-FIRST adversarial review's D1/D2/D3 findings together):
the contract is VERIFY-on-read, HEAL-ONLY-ON-DRIFT, LOUDLY -- not
refuse-on-drift. Per the owner's own words: "you apply them and then let
the OS check on reads; re-applying them has no value." A prior version of
this module refused outright on any drift; that over-specified the owner's
actual contract and bricked real, legitimate states the write side itself
already tolerates or that other tooling produces:

  * A directory created by plain `Path.mkdir()`/`write_text()` (never
    passed through `ensure_private_configuration_path`) has an inherited,
    unprotected DACL -- not tampering, just "never hardened yet". Ten
    in-tree tests construct exactly this shape (`tests/test_service_
    runtime.py`, `tests/test_cli_jarvis_mcp_validate.py`); refusing it
    would have broken every Windows CI leg.
  * The docs instruct hand-editing `.clio-relay/clusters.json` directly;
    any atomic-save editor (temp-file-plus-rename, e.g. VS Code) produces a
    freshly-created file with a default, non-private ACL by construction.
  * iowarp/clio-relay#30's elevated-token migration case: a pre-existing
    config legitimately owned by the token's default owner (commonly
    BUILTIN\\Administrators) rather than the current user, never yet
    touched by a write.

None of these are attacks; they are ordinary states a read must tolerate.
Genuine tampering (a foreign owner unrelated to either accepted owner, a
widened DACL, a stripped inheritance-protection flag) is exactly the same
observable shape as "never hardened" from the read side's point of view --
the read path cannot and should not try to distinguish "malicious" from
"never touched"; it treats both as drift, heals them identically (via the
existing, trusted write path), and REPORTS the heal so nothing is silent.

Contract:

  1. Open the existing path read-only, requesting only `GENERIC_READ` and
     `READ_CONTROL` -- never `WRITE_DAC` (D6: a verifier that doesn't ask
     for a right it will never exercise doesn't fail opaquely against an
     ACL that specifically restricts that right, e.g. an OWNER RIGHTS ACE
     narrowed below WRITE_DAC; `_open_windows_configuration_handle`'s
     `request_write_dac=False` also still runs the regular-file/no-reparse
     structural check via `_validate_windows_configuration_handle` -- that
     check is NOT a drift target, it stays a hard refusal: a reparse point
     or a directory-where-a-file-was-expected is a structural violation an
     ACL heal cannot and must not paper over).
  2. Verify the DACL/owner the write side already installed still holds
     (`cluster_config_windows_acl._verify_private_windows_acl` -- SID VALUE
     comparison only, via `ConvertSidToStringSidW`/`GetSecurityInfo`; no
     `LookupAccountSid`/LSA name resolution, and -- on the clean path --
     no `SetSecurityInfo`). D5: passes BOTH `expected_owner_sid` (the
     current user) and `default_owner_sid` (the elevated token's default
     owner), mirroring exactly what the write side
     (`_set_private_windows_acl`) already accepts as a legitimate starting
     owner before it normalizes -- the read verifier must not flag a
     #30-migrated, Administrators-owned config as drift on every single
     read.
  3. On a clean pass: return. Zero `SetSecurityInfo` calls -- this is the
     latency fix clio-relay#289 exists to ship.
  4. On drift (the verify step raises `ConfigurationError`): heal it by
     calling the existing, trusted write path
     (`cluster_config_windows_paths.ensure_private_configuration_path`,
     unchanged by this module) and emit a structured, typed warning
     (`configuration_acl_healed_on_read`, `logging`'s `extra=` structured-
     fields idiom this codebase already uses -- see
     `endpoint_owner_session_sweep.py`) naming the path and what drifted.
     Never silent (the no-silent-fallback doctrine): a read that changes
     the file's protections without saying so is exactly the failure mode
     that doctrine forbids. If the heal itself cannot succeed (e.g. a
     truly foreign owner even the write side's normalization dance cannot
     resolve), ITS exception propagates unmodified -- that is a genuine,
     unrecoverable failure, not "refuse on drift".

Depends on `cluster_config_windows_primitives.py`, `cluster_config_windows_
paths.py`, and `cluster_config_windows_acl.py` (each imported as a module,
so `tests/test_cluster_config.py`'s `monkeypatch.setattr` against those
modules' names reaches the call sites here too). `cluster_config_windows_
paths.py` does not import this module -- it is a pure downstream consumer of
the write-side owner modules, so no import cycle is introduced.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

from clio_relay import cluster_config_windows_acl as _windows_acl
from clio_relay import cluster_config_windows_paths as _windows_paths
from clio_relay import cluster_config_windows_primitives as _windows_primitives
from clio_relay.errors import ConfigurationError

logger = logging.getLogger(__name__)


def verify_private_configuration_windows_path(path: Path, *, directory: bool) -> None:
    """Verify the owner-private ACL on an existing Windows path; heal drift loudly.

    See the module docstring for the full VERIFY-then-HEAL-LOUDLY contract.
    Structural violations (a reparse point, or the wrong file/directory
    kind) are never healed -- they propagate as a hard `ConfigurationError`
    exactly as before, since an ACL re-apply cannot and must not paper over
    "this is not the kind of object a configuration path may be".
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
    default_owner_sid = _windows_primitives._current_windows_default_owner_sid(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        advapi32=advapi32,
        kernel32=kernel32,
        path=path,
    )
    # request_write_dac=False (D6): this verifier never calls SetSecurityInfo,
    # so it never needs WRITE_DAC -- and not asking for it means a
    # wrong-mask ACL that denies WRITE_DAC (even to the owner, e.g. a
    # narrowed OWNER RIGHTS ACE) still opens successfully here, so the real
    # diagnosis (structural mismatch, or ACL drift below) runs instead of an
    # opaque CreateFileW ERROR_ACCESS_DENIED (5).
    handle = _windows_paths._open_windows_configuration_handle(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
        path,
        directory=directory,
        kernel32=kernel32,
        request_write_dac=False,
    )
    try:
        try:
            _windows_acl._verify_private_windows_acl(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
                handle,
                directory=directory,
                expected_owner_sid=user_sid,
                default_owner_sid=default_owner_sid,
                advapi32=advapi32,
                kernel32=kernel32,
                path=path,
            )
        except ConfigurationError as drift:
            _heal_and_warn_on_read_drift(path, directory=directory, drift=drift)
    finally:
        _windows_primitives._close_windows_handle(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            handle, kernel32=kernel32
        )


def _heal_and_warn_on_read_drift(
    path: Path,
    *,
    directory: bool,
    drift: ConfigurationError,
) -> None:
    """Re-apply the private ACL (the trusted write path) and report the heal.

    Never silent: emits a structured, typed warning naming the path and
    what drifted (the no-silent-fallback doctrine). If the write path
    itself cannot establish a private ACL (a genuinely unrecoverable
    state -- e.g. WRITE_DAC itself is denied even to the owner, so there is
    no access path left to fix the DACL at all), that failure is real and
    must not be swallowed -- but it is re-raised WITH the original drift
    diagnosis attached, so the caller sees what was wrong AND that a repair
    was attempted and failed, rather than only the heal's own possibly
    opaque failure (D6's diagnosability goal applies here too: a bare
    `could not open ... (5)` from the heal attempt alone would silently
    swallow the specific, already-diagnosed drift reason).
    """
    try:
        _windows_paths.ensure_private_configuration_path(path, directory=directory)
    except ConfigurationError as heal_failure:
        raise ConfigurationError(
            f"configuration ACL drifted and could not be healed on read: {path} "
            f"(drift: {drift}; heal failed: {heal_failure})"
        ) from heal_failure
    logger.warning(
        "configuration_acl_healed_on_read",
        extra={"path": str(path), "directory": directory, "drift": str(drift)},
    )

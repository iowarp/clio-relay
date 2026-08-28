"""Bounded, integrity-checked reads of local configuration files.

Split out of `cluster_config.py` (iowarp/clio-relay#231): reading a
configuration file that must not have changed identity mid-read, is capped at
a byte budget, and must be owned/private before its bytes are trusted.
Platform-agnostic -- the Windows-specific ACL enforcement itself lives in the
`cluster_config_windows_*` owner modules, which this module's
`_is_reparse_stat` is shared with (imported there directly since no test
patches it).

`verify_private_configuration_path` (clio-relay#289) is the platform-agnostic
read-path entry point: on Windows it VERIFIES existing private protections
first and only re-applies them on detected drift -- loudly, via a structured
warning, never silently -- dispatching to the read-only
`cluster_config_windows_read_verify` owner module (VERIFY-then-HEAL-LOUDLY;
see that module's docstring for the controller design ruling and why an
outright refusal was rejected). POSIX is unchanged: its `ensure_private_
configuration_path` branch is already a pure stat check with no mutation, so
it is called directly. Every genuine read path in this module calls
`verify_private_configuration_path`; write/create paths elsewhere keep
calling `ensure_private_configuration_path` directly and are unaffected.
"""

from __future__ import annotations

import os
import stat
import time
from pathlib import Path

from clio_relay.errors import ConfigurationError

MAX_CLUSTER_REGISTRY_BYTES = 4 * 1024 * 1024
MAX_CONFIG_READ_ATTEMPTS = 25
CONFIG_READ_RETRY_SECONDS = 0.02


class _ConfigurationChangedError(ConfigurationError):
    """Transient configuration identity/version change during a stable read."""


def read_bounded_configuration_bytes(path: Path, *, max_bytes: int) -> bytes:
    """Read one stable regular configuration file without following links."""
    if max_bytes < 1:
        raise ValueError("configuration byte limit must be positive")
    verify_private_configuration_path(path.parent, directory=True)
    initial = os.lstat(path)
    _require_safe_configuration_stat(path, initial, max_bytes=max_bytes)
    verify_private_configuration_path(path, directory=False)
    last_error: OSError | _ConfigurationChangedError | None = None
    for attempt in range(MAX_CONFIG_READ_ATTEMPTS):
        try:
            before = os.lstat(path)
            _require_safe_configuration_stat(path, before, max_bytes=max_bytes)
            flags = os.O_RDONLY | getattr(os, "O_BINARY", 0)
            flags |= getattr(os, "O_CLOEXEC", 0)
            flags |= getattr(os, "O_NOFOLLOW", 0)
            descriptor = os.open(path, flags)
            try:
                opened = os.fstat(descriptor)
                _require_safe_configuration_stat(path, opened, max_bytes=max_bytes)
                if _stat_version(before) != _stat_version(opened):
                    raise _ConfigurationChangedError(
                        f"configuration identity changed during open: {path}"
                    )
                with os.fdopen(descriptor, "rb", closefd=False) as stream:
                    payload = stream.read(max_bytes + 1)
                    stream.seek(0)
                    confirmed_payload = stream.read(max_bytes + 1)
                if len(payload) > max_bytes:
                    raise ConfigurationError(
                        f"configuration file exceeds {max_bytes} bytes: {path}"
                    )
                final = os.fstat(descriptor)
                after = os.lstat(path)
                if (
                    payload != confirmed_payload
                    or _stat_version(opened) != _stat_version(final)
                    or _stat_version(final) != _stat_version(after)
                    or final.st_size != len(payload)
                ):
                    raise _ConfigurationChangedError(f"configuration changed during read: {path}")
                return payload.removeprefix(b"\xef\xbb\xbf")
            finally:
                os.close(descriptor)
        except (OSError, _ConfigurationChangedError) as exc:
            last_error = exc
            if attempt + 1 >= MAX_CONFIG_READ_ATTEMPTS:
                break
            time.sleep(CONFIG_READ_RETRY_SECONDS)
    if last_error is not None:
        raise ConfigurationError(
            f"cannot read configuration file {path}: {last_error}"
        ) from last_error
    raise ConfigurationError(f"cannot read configuration file: {path}")


def verify_private_configuration_path(path: Path, *, directory: bool) -> None:
    """Verify private configuration protections on a read; heal drift loudly.

    The read-path counterpart to `cluster_config_windows_paths.
    ensure_private_configuration_path`. On POSIX, delegates to it directly:
    that function's non-Windows branch is already a pure stat/ownership/mode
    check with no mutation, so a read path can call it unchanged (mirrors
    the same shape rather than duplicating it) -- no POSIX regression
    motivated a change there. On Windows, calls the read-only verifier
    (`cluster_config_windows_read_verify.
    verify_private_configuration_windows_path`): a clean path costs zero
    `SetSecurityInfo` calls (the write side's `SetSecurityInfo` write is
    what stalled for seconds under LSA network-resolution flakiness); a
    DRIFTED path is healed by calling the existing, trusted write path and
    reporting the heal via a structured, typed warning -- never silently,
    and never a bare refusal. See `cluster_config_windows_read_verify`'s
    module docstring for the controller design ruling (clio-relay#289,
    2026-08-28) this implements and why an unconditional refusal on drift
    was rejected: it broke ten in-tree tests and the documented
    hand-edit-the-registry workflow, both of which produce a legitimately
    unhardened file/directory that is not tampering.

    Write/create paths (`open_private_atomic_file`,
    `create_private_configuration_directory`,
    `ensure_private_configuration_directory`) are unaffected and keep
    unconditionally applying+verifying the ACL exactly as before.
    """
    if os.name != "nt":
        # Function-scope import: cluster_config_windows_paths imports
        # _is_reparse_stat from this module at module scope, so a
        # module-scope import back here would be a load-order circular
        # import between the two halves of the original single-file module.
        # Deferring to call time (this module is fully loaded by then) is
        # the same proven idiom this codebase's own decomposition history
        # already uses for this exact shape of cross-owner dependency.
        from clio_relay.cluster_config_windows_paths import (
            ensure_private_configuration_path,
        )

        ensure_private_configuration_path(path, directory=directory)
        return
    # Same deferral reasoning: cluster_config_windows_read_verify imports
    # cluster_config_windows_paths (module scope), which imports this module
    # (module scope) for _is_reparse_stat -- a module-scope import here would
    # complete the cycle.
    from clio_relay.cluster_config_windows_read_verify import (
        verify_private_configuration_windows_path,
    )

    verify_private_configuration_windows_path(path, directory=directory)


def _require_safe_configuration_stat(path: Path, value: os.stat_result, *, max_bytes: int) -> None:
    if not stat.S_ISREG(value.st_mode) or _is_reparse_stat(value):
        raise ConfigurationError(f"configuration path is not a regular owned file: {path}")
    if os.name != "nt" and hasattr(os, "getuid"):
        if value.st_uid != os.getuid():
            raise ConfigurationError(f"configuration path is not owned by this user: {path}")
        if stat.S_IMODE(value.st_mode) & 0o022:
            raise ConfigurationError(
                f"configuration path is writable by group or other users: {path}"
            )
    if value.st_size > max_bytes:
        raise ConfigurationError(f"configuration file exceeds {max_bytes} bytes: {path}")


def _is_reparse_stat(value: os.stat_result) -> bool:
    attributes = getattr(value, "st_file_attributes", 0)
    return bool(attributes & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0))


def _stat_version(value: os.stat_result) -> tuple[int, int, int, int]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_size,
        value.st_mtime_ns,
    )


def _fsync_directory(path: Path) -> None:
    """Best-effort fsync of a directory after an atomic replacement."""
    if os.name == "nt":
        return
    directory_fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)

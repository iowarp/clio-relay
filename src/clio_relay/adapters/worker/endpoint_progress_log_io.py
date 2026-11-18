"""Package-progress log path/identity primitives (iowarp/clio-relay#231).

Owner module for the small, self-contained helpers that resolve and safely
open a child pipeline's stdout-adjacent progress log: normalizing a
provider-declared relative path against the child's own working directory,
validating a native-subprocess cwd against the legacy Windows path bound,
and opening the log file without following a symlink or racing a path swap.

These are leaf primitives -- they depend only on stdlib, ``filesystem_paths``,
and ``errors``, and never reach back into ``endpoint.py`` or any sibling
owner module extracted from it. ``endpoint_runtime_sidecar_anchor.py`` (the
runtime-sidecar anchor primitives) and ``EndpointWorker`` itself both import
``_progress_log_identity`` from here to compare filesystem identity across an
open/reopen boundary.
"""

from __future__ import annotations

import os
import stat as stat_module
from pathlib import Path
from typing import BinaryIO

from clio_relay.errors import ConfigurationError
from clio_relay.filesystem_paths import (
    WINDOWS_LEGACY_PATH_HEADROOM,
    internal_filesystem_path,
    logical_filesystem_path,
)


def _progress_log_identity(stat: os.stat_result) -> tuple[int, int]:
    return stat.st_dev, stat.st_ino


def _normalize_package_progress_log_path(child_cwd: Path, path: Path) -> Path:
    expanded = path.expanduser()
    candidate = expanded if expanded.is_absolute() else child_cwd.absolute() / expanded
    return Path(os.path.abspath(candidate))


def _validated_native_subprocess_cwd(cwd: Path) -> Path:
    """Return a logical cwd or reject unverified native Windows path forms."""
    logical_cwd = logical_filesystem_path(cwd)
    if os.name != "nt":
        return logical_cwd
    absolute_cwd = os.path.abspath(logical_cwd)
    if absolute_cwd.startswith("\\\\"):
        raise ConfigurationError(
            "native JARVIS working directories on Windows must not use UNC paths"
        )
    if len(absolute_cwd) >= WINDOWS_LEGACY_PATH_HEADROOM:
        raise ConfigurationError(
            "native JARVIS working directory exceeds the verified Windows path bound"
        )
    return logical_cwd


def _render_progress_log_identity(identity: tuple[int, int] | None) -> str | None:
    if identity is None:
        return None
    return f"{identity[0]}:{identity[1]}"


def _open_package_progress_log(path: Path) -> BinaryIO | None:
    """Open one regular provider log without following symlinks or path races."""
    storage_path = internal_filesystem_path(path)
    try:
        path_stat = os.stat(storage_path, follow_symlinks=False)
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise ConfigurationError(f"could not inspect package progress log {path}: {exc}") from exc
    if stat_module.S_ISLNK(path_stat.st_mode):
        raise ConfigurationError(f"package progress log symlinks are not allowed: {path}")
    if not stat_module.S_ISREG(path_stat.st_mode):
        raise ConfigurationError(f"package progress log is not a regular file: {path}")
    flags = (
        os.O_RDONLY
        | getattr(os, "O_BINARY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_NONBLOCK", 0)
    )
    try:
        descriptor = os.open(storage_path, flags)
    except OSError as exc:
        raise ConfigurationError(f"could not open package progress log {path}: {exc}") from exc
    try:
        opened_stat = os.fstat(descriptor)
        if not stat_module.S_ISREG(opened_stat.st_mode):
            raise ConfigurationError(f"package progress log is not a regular file: {path}")
        if _progress_log_identity(opened_stat) != _progress_log_identity(path_stat):
            raise ConfigurationError(f"package progress log changed while it was opened: {path}")
        return os.fdopen(descriptor, "rb")
    except Exception:
        os.close(descriptor)
        raise

"""Private bootstrap serialization lock and legacy cursor-permission repair.

``bootstrap_invocation_lock`` is the one private lock every bootstrap
inspection and mutation is serialized through; the permission-repair
compatibility operation runs while a candidate generation holds the
inherited exclusive writer-lifetime guard (iowarp/clio-relay#255).
"""

from __future__ import annotations

import os
import stat
from collections.abc import Generator
from contextlib import contextmanager, suppress
from pathlib import Path

from clio_relay.bootstrap_reconcile_constants import (
    _FCHMOD,
    _GETUID,
    BOOTSTRAP_LOCK_TIMEOUT_SECONDS,
)
from clio_relay.errors import ConfigurationError
from clio_relay.worker_lifetime_lock import (
    WorkerLifetimeLock,
    WorkerLifetimeLockUnavailable,
    exclusive_migration_lifetime,
)


def repair_legacy_cursor_permissions_for_upgrade(core_dir: Path) -> dict[str, object]:
    """Privatize the fixed legacy cursor directory through a pinned queue root.

    Forward recovery can execute a generation whose queue initializer predates
    cursor-directory repair. The current candidate calls this compatibility
    operation while holding the inherited exclusive writer-lifetime guard, so
    the old generation can finish its journal before the candidate replaces it.
    Missing cursors are a no-op; links, foreign ownership, and identity changes
    fail closed.
    """
    if os.name != "posix" or _FCHMOD is None or _GETUID is None:
        raise ConfigurationError("legacy cursor permission repair requires POSIX fchmod")
    with exclusive_migration_lifetime(core_dir) as locked_core:
        root_descriptor = locked_core.filesystem_root_descriptor
        if root_descriptor is None:
            raise ConfigurationError("legacy cursor permission repair has no pinned root")
        flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
        flags |= getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        try:
            descriptor = os.open("cursors", flags, dir_fd=root_descriptor)
        except FileNotFoundError:
            return {
                "schema_version": "clio-relay.bootstrap-legacy-cursor-repair.v1",
                "action": "absent",
            }
        except OSError as exc:
            raise ConfigurationError(
                "legacy cursor directory cannot be safely opened through the pinned root"
            ) from exc
        try:
            try:
                os.set_inheritable(descriptor, False)
                before = os.fstat(descriptor)
                if not stat.S_ISDIR(before.st_mode) or before.st_uid != _GETUID():
                    raise ConfigurationError(
                        "legacy cursor directory is not one owned real directory"
                    )
                action = "reused"
                if stat.S_IMODE(before.st_mode) != 0o700:
                    _FCHMOD(descriptor, 0o700)
                    action = "repaired"
                after = os.fstat(descriptor)
                if (
                    (after.st_dev, after.st_ino) != (before.st_dev, before.st_ino)
                    or not stat.S_ISDIR(after.st_mode)
                    or after.st_uid != _GETUID()
                    or stat.S_IMODE(after.st_mode) != 0o700
                ):
                    raise ConfigurationError(
                        "legacy cursor directory identity changed during permission repair"
                    )
                return {
                    "schema_version": "clio-relay.bootstrap-legacy-cursor-repair.v1",
                    "action": action,
                    "device": after.st_dev,
                    "inode": after.st_ino,
                }
            except OSError as exc:
                raise ConfigurationError(
                    "legacy cursor directory permissions could not be repaired"
                ) from exc
        finally:
            with suppress(OSError):
                os.close(descriptor)


@contextmanager
def bootstrap_invocation_lock(
    *,
    home: Path | None = None,
    timeout_seconds: float = BOOTSTRAP_LOCK_TIMEOUT_SECONDS,
) -> Generator[Path]:
    """Serialize bootstrap inspection and mutation through one private lock."""
    if timeout_seconds <= 0:
        raise ValueError("bootstrap lock timeout must be positive")
    resolved_home = (home or Path.home()).resolve()
    directory = resolved_home / ".local/share/clio-relay"
    lock = WorkerLifetimeLock(
        directory,
        mode="exclusive",
        timeout_seconds=timeout_seconds,
        lock_name="bootstrap.lock",
    )
    try:
        lock.acquire()
    except WorkerLifetimeLockUnavailable as exc:
        raise ConfigurationError("timed out acquiring the bootstrap lock") from exc
    except ConfigurationError as exc:
        raise ConfigurationError(f"private bootstrap lock is invalid: {exc}") from exc
    try:
        yield lock.path
    finally:
        lock.release()

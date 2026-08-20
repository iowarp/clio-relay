"""Atomic durable-record writes owned by the core queue store."""

from __future__ import annotations

import json
import os
import stat
import time
from contextlib import suppress
from pathlib import Path
from uuid import uuid4

from pydantic import BaseModel

from clio_relay import cluster_config, queue_layout
from clio_relay.errors import ConfigurationError, QueueConflictError
from clio_relay.filesystem_paths import internal_filesystem_path, logical_filesystem_path
from clio_relay.models import SchedulerCancelPending


def write_model(storage_root: Path, path: Path, record: BaseModel) -> None:
    """Serialize and atomically persist one typed durable record."""
    # Scheduler cancellation records may contain the contract maximum of
    # 1,000 dispositions. Claim fields were added after v1.0.7, so writing six
    # explicit nulls per legacy disposition would make a previously valid
    # record exceed its durable family limit. Missing optional fields retain
    # the same Pydantic defaults; an active claim is still serialized in full.
    exclude_none = isinstance(record, SchedulerCancelPending)
    write_text(
        storage_root,
        path,
        record.model_dump_json(indent=2, exclude_none=exclude_none),
    )


def write_json(storage_root: Path, path: Path, record: dict[str, object]) -> None:
    """Serialize and atomically persist one JSON object."""
    write_text(storage_root, path, json.dumps(record))


def require_safe_write_directory(storage_root: Path, directory: Path) -> os.stat_result:
    """Create and validate one owner-controlled directory below the queue root."""
    try:
        logical_directory = logical_filesystem_path(directory)
        internal_directory = internal_filesystem_path(
            logical_directory,
            force_extended=True,
        )
    except ValueError as error:
        raise QueueConflictError(f"write directory has an unsupported path: {directory}") from error
    try:
        relative = internal_directory.relative_to(storage_root)
    except ValueError as error:
        raise QueueConflictError(
            f"write directory escaped queue root: {logical_directory}"
        ) from error
    if any(part in {"", ".", ".."} for part in relative.parts):
        raise QueueConflictError(f"write directory has unsafe ancestry: {logical_directory}")
    current = storage_root
    for part in relative.parts:
        current /= part
        try:
            current_stat = os.lstat(current)
        except FileNotFoundError:
            with suppress(FileExistsError):
                current.mkdir(mode=0o700)
            current_stat = os.lstat(current)
        if not stat.S_ISDIR(current_stat.st_mode) or queue_layout.record_is_reparse(current_stat):
            raise QueueConflictError(
                f"write directory ancestry is unsafe: {logical_filesystem_path(current)}"
            )
        if os.name != "nt" and hasattr(os, "geteuid") and current_stat.st_uid != os.geteuid():
            raise QueueConflictError(
                f"write directory is not owned by this user: {logical_filesystem_path(current)}"
            )
    return os.lstat(internal_directory)


def require_private_write_staging(storage_root: Path) -> tuple[Path, os.stat_result]:
    """Return the private non-reparse staging directory used for atomic writes."""
    staging = storage_root / queue_layout.WRITE_STAGING_FAMILY
    try:
        if not os.path.lexists(staging):
            cluster_config.ensure_private_configuration_directory(staging)
        if os.name != "nt":
            os.chmod(staging, 0o700)
        cluster_config.ensure_private_configuration_path(staging, directory=True)
    except (ConfigurationError, OSError) as error:
        raise QueueConflictError(
            f"queue write staging is not owner-private: {logical_filesystem_path(staging)}"
        ) from error
    staging_stat = require_safe_write_directory(storage_root, staging)
    if not stat.S_ISDIR(staging_stat.st_mode) or queue_layout.record_is_reparse(staging_stat):
        raise QueueConflictError(
            f"queue write staging is not a safe directory: {logical_filesystem_path(staging)}"
        )
    return staging, staging_stat


def purge_write_staging(storage_root: Path) -> None:
    """Remove bounded crash leftovers while holding the cross-process queue lock."""
    staging, _ = require_private_write_staging(storage_root)
    leftovers: list[Path] = []
    try:
        with os.scandir(staging) as entries:
            for entry in entries:
                if len(leftovers) >= queue_layout.WRITE_STAGING_MAX_LEFTOVERS:
                    raise QueueConflictError(
                        f"queue write staging exceeds the bounded cleanup limit: {staging}"
                    )
                path = Path(entry.path)
                stem = entry.name.removesuffix(".tmp")
                entry_stat = os.lstat(path)
                if (
                    not entry.name.endswith(".tmp")
                    or len(stem) != 32
                    or any(character not in "0123456789abcdef" for character in stem)
                    or not stat.S_ISREG(entry_stat.st_mode)
                    or queue_layout.record_is_reparse(entry_stat)
                    or entry_stat.st_nlink != 1
                ):
                    raise QueueConflictError(
                        f"queue write staging contains an unsafe entry: {path}"
                    )
                leftovers.append(path)
    except QueueConflictError:
        raise
    except OSError as error:
        raise QueueConflictError(f"cannot scan queue write staging: {staging}") from error
    for path in leftovers:
        unlink_durable_path(path)
    if leftovers:
        fsync_write_directory(staging)


def write_text(storage_root: Path, path: Path, text: str) -> None:
    """Encode and atomically persist one durable text record."""
    write_bytes(
        storage_root,
        path,
        text.encode("utf-8"),
        max_bytes=queue_layout.record_max_bytes(path),
    )


def write_bytes(storage_root: Path, path: Path, payload: bytes, *, max_bytes: int) -> None:
    """Atomically write owner-private bytes below the queue root."""
    try:
        logical_path = logical_filesystem_path(path)
        internal_path = internal_filesystem_path(logical_path, force_extended=True)
    except ValueError as error:
        raise QueueConflictError(f"queue write path is unsupported: {path}") from error
    if max_bytes < 1 or len(payload) > max_bytes:
        raise QueueConflictError(
            f"{queue_layout.record_family(internal_path)} record exceeds the "
            f"{max_bytes}-byte limit: "
            f"{logical_path}"
        )
    target_parent_stat = require_safe_write_directory(storage_root, internal_path.parent)
    staging, staging_stat = require_private_write_staging(storage_root)
    if staging_stat.st_dev != target_parent_stat.st_dev:
        raise QueueConflictError(
            "atomic queue replacement crosses filesystems: "
            f"{logical_filesystem_path(staging)} -> {logical_path.parent}"
        )
    temporary = staging / f"{uuid4().hex}.tmp"
    try:
        try:
            with cluster_config.open_private_atomic_file(temporary) as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
        except (ConfigurationError, OSError) as error:
            raise QueueConflictError(
                f"cannot create private staged queue record: {logical_filesystem_path(temporary)}"
            ) from error
        observed_staging = os.lstat(staging)
        observed_parent = os.lstat(internal_path.parent)
        if not os.path.samestat(staging_stat, observed_staging):
            raise QueueConflictError(
                f"queue write staging changed before replace: {logical_filesystem_path(staging)}"
            )
        if not os.path.samestat(target_parent_stat, observed_parent):
            raise QueueConflictError(
                f"queue write target directory changed before replace: {logical_path.parent}"
            )
        for attempt in range(queue_layout.ATOMIC_REPLACE_ATTEMPTS):
            try:
                temporary.replace(internal_path)
                break
            except PermissionError:
                if attempt + 1 >= queue_layout.ATOMIC_REPLACE_ATTEMPTS:
                    raise
                time.sleep(queue_layout.ATOMIC_REPLACE_RETRY_SECONDS)
    finally:
        unlink_durable_path(temporary, missing_ok=True)
    fsync_write_directory(staging)
    fsync_write_directory(internal_path.parent)


def fsync_write_directory(path: Path) -> None:
    """Persist directory metadata where the platform exposes directory fsync."""
    try:
        directory_fd = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    except OSError:
        return
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def unlink_durable_path(path: Path, *, missing_ok: bool = False) -> None:
    """Delete one durable path after bounded Windows sharing-violation retries."""
    for attempt in range(queue_layout.ATOMIC_REPLACE_ATTEMPTS):
        try:
            path.unlink(missing_ok=missing_ok)
            return
        except OSError as error:
            if (
                getattr(error, "winerror", None) not in {5, 32, 33}
                or attempt + 1 >= queue_layout.ATOMIC_REPLACE_ATTEMPTS
            ):
                raise
            time.sleep(queue_layout.ATOMIC_REPLACE_RETRY_SECONDS)

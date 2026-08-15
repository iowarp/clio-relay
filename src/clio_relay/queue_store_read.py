"""Canonical, JSON, and bounded durable-record reads for the core queue store."""

from __future__ import annotations

import errno
import json
import logging
import os
import stat
import time
from collections.abc import Iterable
from pathlib import Path

from pydantic import BaseModel

from clio_relay import queue_layout
from clio_relay.errors import QueueConflictError, queue_conflict_from_cause

logger = logging.getLogger(__name__)


def read_canonical_record[Record: BaseModel](
    storage_root: Path,
    path: Path,
    model: type[Record],
) -> Record:
    """Read one canonical record and bind its content to its storage identity."""
    record = read_json_file(path, model)
    queue_layout.validate_canonical_access(storage_root, path, record)
    return record


def read_optional[Record: BaseModel](
    storage_root: Path,
    path: Path,
    model: type[Record],
) -> Record | None:
    """Read one optional canonical record."""
    if path_lstat(path) is None:
        return None
    try:
        return read_canonical_record(storage_root, path, model)
    except FileNotFoundError:
        return None


def _read_records[Record: BaseModel](
    paths: Iterable[Path],
    model: type[Record],
    *,
    identity_field: str,
) -> list[Record]:
    records: list[Record] = []
    for path in paths:
        try:
            record = read_json_file(path, model)
        except FileNotFoundError:
            continue
        if getattr(record, identity_field, None) != path.stem:
            raise QueueConflictError(
                f"canonical {identity_field} filename/content identity mismatch: {path}"
            )
        records.append(record)
    return records


def read_many[Record: BaseModel](
    directory: Path,
    model: type[Record],
    *,
    identity_field: str | None = None,
) -> Iterable[Record]:
    """Read one complete bounded family of canonical records."""
    resolved_identity = identity_field or queue_layout.record_identity_field(model)
    paths, truncated = scan_json_record_paths(
        directory,
        limit=queue_layout.MAX_BOUNDED_SCAN_RECORDS,
        label=f"canonical {resolved_identity} records",
    )
    if truncated:
        raise QueueConflictError(
            "canonical record family exceeds the bounded read limit of "
            f"{queue_layout.MAX_BOUNDED_SCAN_RECORDS}: {directory}"
        )
    return _read_records(paths, model, identity_field=resolved_identity)


def scan_many[Record: BaseModel](
    directory: Path,
    model: type[Record],
    *,
    limit: int,
    identity_field: str | None = None,
) -> tuple[list[Record], bool]:
    """Read at most ``limit`` canonical records and report truncation."""
    if limit < 1:
        raise ValueError("record scan limit must be at least 1")
    resolved_identity = identity_field or queue_layout.record_identity_field(model)
    paths, truncated = scan_json_record_paths(
        directory,
        limit=limit,
        label=f"canonical {resolved_identity} records",
    )
    return _read_records(paths, model, identity_field=resolved_identity), truncated


def scan_json_record_paths(
    directory: Path,
    *,
    limit: int,
    label: str,
) -> tuple[list[Path], bool]:
    """Scan regular JSON children without following a replaced directory or entry."""
    try:
        directory_stat = os.lstat(directory)
    except FileNotFoundError:
        return [], False
    except OSError as error:
        raise QueueConflictError(f"cannot inspect {label}: {directory}") from error
    if not stat.S_ISDIR(directory_stat.st_mode) or queue_layout.record_is_reparse(directory_stat):
        raise QueueConflictError(f"{label} is not a safe directory: {directory}")
    paths: list[Path] = []
    try:
        with os.scandir(directory) as entries:
            for entry in entries:
                try:
                    entry_stat = entry.stat(follow_symlinks=False)
                except FileNotFoundError:
                    continue
                path = Path(entry.path)
                if (
                    not entry.name.endswith(".json")
                    or not stat.S_ISREG(entry_stat.st_mode)
                    or queue_layout.record_is_reparse(entry_stat)
                ):
                    raise QueueConflictError(f"{label} contains an unsafe record: {path}")
                if len(paths) >= limit:
                    return paths, True
                paths.append(path)
    except QueueConflictError:
        raise
    except OSError as error:
        raise QueueConflictError(f"cannot scan {label}: {directory}") from error
    return paths, False


def bounded_json_record_paths(directory: Path, *, limit: int, label: str) -> list[Path]:
    """Return bounded regular JSON children or fail closed on ambiguous layout."""
    try:
        directory_stat = os.lstat(directory)
    except FileNotFoundError:
        return []
    if not stat.S_ISDIR(directory_stat.st_mode) or queue_layout.record_is_reparse(directory_stat):
        raise QueueConflictError(f"{label} is not a safe directory: {directory}")
    paths: list[Path] = []
    try:
        with os.scandir(directory) as entries:
            for entry in entries:
                if len(paths) >= limit:
                    raise QueueConflictError(
                        f"{label} exceeded its safety bound of {limit} records"
                    )
                try:
                    entry_stat = entry.stat(follow_symlinks=False)
                except FileNotFoundError:
                    continue
                path = Path(entry.path)
                if (
                    not entry.name.endswith(".json")
                    or not stat.S_ISREG(entry_stat.st_mode)
                    or queue_layout.record_is_reparse(entry_stat)
                ):
                    raise QueueConflictError(f"{label} contains an unsafe record: {path}")
                paths.append(path)
    except OSError as error:
        raise queue_conflict_from_cause(
            f"cannot scan {label}",
            cause=error,
            logger=logger,
        ) from error
    return paths


def read_json_file[Record: BaseModel](path: Path, model: type[Record]) -> Record:
    """Read and validate one bounded typed JSON record."""
    last_error: OSError | json.JSONDecodeError | QueueConflictError | None = None
    for _ in range(queue_layout.ATOMIC_REPLACE_ATTEMPTS):
        try:
            return model.model_validate_json(read_bounded_record_bytes(path))
        except (PermissionError, json.JSONDecodeError) as error:
            last_error = error
            time.sleep(queue_layout.ATOMIC_REPLACE_RETRY_SECONDS)
        except QueueConflictError as error:
            if not transient_record_access_conflict(error):
                raise
            last_error = error
            time.sleep(queue_layout.ATOMIC_REPLACE_RETRY_SECONDS)
    if last_error is not None:
        raise last_error
    return model.model_validate_json(read_bounded_record_bytes(path))


def read_json_document(path: Path) -> object:
    """Read one bounded JSON document."""
    try:
        return json.loads(read_bounded_record_bytes(path))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise queue_conflict_from_cause(
            f"invalid JSON record {path}",
            cause=error,
            logger=logger,
        ) from error


def read_bounded_record_bytes_once(path: Path, *, limit: int) -> bytes:
    """Read one stable record generation or identify a transient replacement."""
    before = os.lstat(path)
    queue_layout.validate_record_stat(before, path=path)
    flags = (
        os.O_RDONLY
        | getattr(os, "O_BINARY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_NONBLOCK", 0)
    )
    descriptor = -1
    try:
        try:
            descriptor = os.open(path, flags)
        except FileNotFoundError as error:
            raise queue_layout.TransientRecordReplacement(
                f"durable record disappeared while opening: {path}"
            ) from error
        opened = os.fstat(descriptor)
        queue_layout.validate_record_stat(opened, path=path)
        try:
            after_open = os.lstat(path)
        except FileNotFoundError as error:
            raise queue_layout.TransientRecordReplacement(
                f"durable record disappeared after opening: {path}"
            ) from error
        queue_layout.validate_record_stat(after_open, path=path)
        if (
            not queue_layout.record_stats_match(before, opened, compare_ctime=False)
            or not queue_layout.record_stats_match(opened, after_open, compare_ctime=False)
            or not queue_layout.record_stats_match(before, after_open, compare_ctime=True)
        ):
            raise queue_layout.TransientRecordReplacement(
                f"durable record changed while opening: {path}"
            )
        chunks: list[bytes] = []
        total = 0
        while total <= limit:
            before_chunk = os.fstat(descriptor)
            queue_layout.validate_record_stat(before_chunk, path=path)
            if not queue_layout.record_stats_match(opened, before_chunk, compare_ctime=True):
                raise queue_layout.TransientRecordReplacement(
                    f"durable record changed while reading: {path}"
                )
            chunk = os.read(descriptor, min(65_536, limit + 1 - total))
            after_chunk = os.fstat(descriptor)
            queue_layout.validate_record_stat(after_chunk, path=path)
            if not queue_layout.record_stats_match(opened, after_chunk, compare_ctime=True):
                raise queue_layout.TransientRecordReplacement(
                    f"durable record changed while reading: {path}"
                )
            if not chunk:
                break
            chunks.append(chunk)
            total += len(chunk)
        final = os.fstat(descriptor)
        queue_layout.validate_record_stat(final, path=path)
        try:
            after_read = os.lstat(path)
        except FileNotFoundError as error:
            raise queue_layout.TransientRecordReplacement(
                f"durable record disappeared after reading: {path}"
            ) from error
        queue_layout.validate_record_stat(after_read, path=path)
        if (
            not queue_layout.record_stats_match(opened, final, compare_ctime=True)
            or not queue_layout.record_stats_match(final, after_read, compare_ctime=False)
            or not queue_layout.record_stats_match(before, after_read, compare_ctime=True)
        ):
            raise queue_layout.TransientRecordReplacement(
                f"durable record changed while reading: {path}"
            )
        if total > limit:
            raise QueueConflictError(
                f"{queue_layout.record_family(path)} record exceeds the {limit}-byte limit: {path}"
            )
        if total != final.st_size:
            raise queue_layout.TransientRecordReplacement(
                f"durable record changed size while reading: {path}"
            )
        return b"".join(chunks)
    except (queue_layout.TransientRecordReplacement, QueueConflictError):
        raise
    except OSError as error:
        raise queue_conflict_from_cause(
            f"cannot read durable record {path}",
            cause=error,
            logger=logger,
        ) from error
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def read_bounded_record_bytes(path: Path) -> bytes:
    """Read one stable bounded record, retrying only atomic replacement races."""
    limit = queue_layout.record_max_bytes(path)
    last_replacement: queue_layout.TransientRecordReplacement | None = None
    for attempt in range(queue_layout.ATOMIC_REPLACE_ATTEMPTS):
        try:
            return read_bounded_record_bytes_once(path, limit=limit)
        except FileNotFoundError as error:
            if last_replacement is None:
                raise
            last_replacement = queue_layout.TransientRecordReplacement(
                f"durable record remained absent during atomic replacement: {path}"
            )
            last_replacement.__cause__ = error
        except queue_layout.TransientRecordReplacement as error:
            last_replacement = error
        if attempt + 1 < queue_layout.ATOMIC_REPLACE_ATTEMPTS:
            time.sleep(queue_layout.ATOMIC_REPLACE_RETRY_SECONDS)
    raise QueueConflictError(
        f"durable record did not stabilize after {queue_layout.ATOMIC_REPLACE_ATTEMPTS} "
        f"atomic replacement attempts: {path}"
    ) from last_replacement


def transient_record_access_conflict(error: QueueConflictError) -> bool:
    """Return whether a durable read failed only on a transient sharing denial."""
    cause = error.__cause__
    if not isinstance(cause, OSError):
        return False
    return (
        isinstance(cause, PermissionError)
        or cause.errno in {errno.EACCES, errno.EPERM}
        or getattr(cause, "winerror", None) in {5, 32, 33}
    )


def path_lstat(path: Path) -> os.stat_result | None:
    """Return one path's lstat or ``None`` when it is absent."""
    try:
        return os.lstat(path)
    except FileNotFoundError:
        return None

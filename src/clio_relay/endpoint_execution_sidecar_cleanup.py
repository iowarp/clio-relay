"""Cross-platform execution-sidecar quarantine orchestration
(iowarp/clio-relay#231).

Owner module for atomically quarantining a job's execution sidecars (never
deleting them -- retaining every inode under a deterministic name so a
crashed worker can always resume where it left off): building the durable
quarantine plan before release (``_execution_sidecar_cleanup_plan``),
restoring and validating exact quarantine targets from durable task state
(``_execution_cleanup_quarantine_paths``), building the acknowledgment
evidence written before a retry marker is removed
(``_execution_cleanup_ack_metadata``), the Linux ``renameat2(RENAME_
NOREPLACE)`` primitive (``_rename_noreplace_at``), the cross-platform
orchestrator that dispatches to it or to the Windows handle path
(``_remove_execution_sidecars``), and releasing anchor descriptors
(``_close_runtime_sidecar_anchors``).

Depends on ``endpoint_sidecar_types.py`` (schema constants),
``endpoint_progress_log_io.py`` (``_progress_log_identity``),
``endpoint_runtime_sidecar_anchor.py`` (``_execution_sidecar_quarantine_
name``, ``_runtime_sidecar_anchor_from_metadata``, ``_validate_runtime_
sidecar_stat``), and ``endpoint_windows_sidecar_handles.py``
(``_remove_execution_sidecars_windows``, the Windows branch of
``_remove_execution_sidecars``) -- all leaves relative to this module
(``endpoint_windows_sidecar_handles.py`` depends on ``endpoint_runtime_
sidecar_anchor.py`` for the exact same quarantine-name primitive, never on
this module, which is what keeps the pair acyclic), so this module stays
acyclic too. ``EndpointWorker`` (still resident in ``endpoint.py``) is this
module's main caller.
"""

from __future__ import annotations

import ctypes
import errno
import os
import stat as stat_module
import sys
from collections.abc import Callable
from contextlib import suppress
from pathlib import Path
from typing import cast

from clio_relay.endpoint_progress_log_io import _progress_log_identity
from clio_relay.endpoint_runtime_sidecar_anchor import (
    _execution_sidecar_quarantine_name,
    _runtime_sidecar_anchor_from_metadata,
    _validate_runtime_sidecar_stat,
)
from clio_relay.endpoint_sidecar_types import (
    EXECUTION_SIDECAR_CLEANUP_SCHEMA,
    EXECUTION_SIDECAR_QUARANTINE_SCHEMA,
    _RuntimeSidecarAnchor,
)
from clio_relay.endpoint_windows_sidecar_handles import _remove_execution_sidecars_windows
from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.filesystem_paths import internal_filesystem_path
from clio_relay.models import RelayTask, utc_now


def _execution_sidecar_cleanup_plan(
    path: Path,
    anchor: _RuntimeSidecarAnchor,
) -> dict[str, object]:
    """Build durable, deterministic quarantine state before execution release."""
    return {
        "schema_version": EXECUTION_SIDECAR_CLEANUP_SCHEMA,
        "quarantine_schema_version": EXECUTION_SIDECAR_QUARANTINE_SCHEMA,
        "source_name": path.name,
        "quarantine_name": _execution_sidecar_quarantine_name(anchor),
        "anchor": anchor.as_metadata(),
        "stage": "prepared",
    }


def _execution_cleanup_quarantine_paths(
    task: RelayTask,
    *,
    paths: list[Path],
    expected_anchors: dict[Path, _RuntimeSidecarAnchor],
) -> dict[Path, Path]:
    """Restore and validate exact quarantine targets from durable task state."""
    raw_cleanup = task.metadata.get("execution_cleanup")
    cleanup = cast(dict[str, object], raw_cleanup) if isinstance(raw_cleanup, dict) else {}
    raw_states = cleanup.get("sidecars")
    if not isinstance(raw_states, dict) or not raw_states:
        raise RelayError(f"execution cleanup has no staged sidecars for task {task.task_id}")
    states = cast(dict[str, object], raw_states)
    targets: dict[Path, Path] = {}
    for path in paths:
        matching_states = [
            cast(dict[str, object], value)
            for value in states.values()
            if isinstance(value, dict)
            and cast(dict[object, object], value).get("source_name") == path.name
        ]
        anchor = expected_anchors.get(path)
        if len(matching_states) != 1:
            raise RelayError(
                f"execution cleanup does not uniquely own sidecar for task {task.task_id}: "
                f"{path.name}"
            )
        state = matching_states[0]
        quarantine_name = state.get("quarantine_name")
        if (
            state.get("schema_version") != EXECUTION_SIDECAR_CLEANUP_SCHEMA
            or state.get("quarantine_schema_version") != EXECUTION_SIDECAR_QUARANTINE_SCHEMA
            or not isinstance(quarantine_name, str)
            or Path(quarantine_name).name != quarantine_name
        ):
            raise RelayError(f"execution cleanup sidecar state is invalid for task {task.task_id}")
        recorded_anchor = _runtime_sidecar_anchor_from_metadata(
            state.get("anchor"),
            task_id=task.task_id,
        )
        if anchor is None:
            raise RelayError(
                f"execution sidecar anchor is missing for task {task.task_id}: {path.name}"
            )
        if recorded_anchor != anchor:
            raise RelayError(f"execution cleanup sidecar anchor conflicts for task {task.task_id}")
        expected_name = _execution_sidecar_quarantine_name(anchor)
        if quarantine_name != expected_name:
            raise RelayError(
                f"execution cleanup quarantine identity conflicts for task {task.task_id}"
            )
        targets[path] = path.parent / _execution_sidecar_quarantine_name(anchor)
    return targets


def _execution_cleanup_ack_metadata(
    task: RelayTask,
    quarantined: dict[Path, Path],
) -> dict[str, object]:
    """Build canonical cleanup evidence written before the retry marker is removed."""
    now = utc_now().isoformat()
    raw_cleanup = task.metadata.get("execution_cleanup")
    cleanup = dict(cast(dict[str, object], raw_cleanup)) if isinstance(raw_cleanup, dict) else {}
    raw_states = cleanup.get("sidecars")
    states = dict(cast(dict[str, object], raw_states)) if isinstance(raw_states, dict) else {}
    for source, quarantine in quarantined.items():
        matching_roles = [
            role
            for role, value in states.items()
            if isinstance(value, dict)
            and cast(dict[object, object], value).get("source_name") == source.name
        ]
        if not matching_roles:
            continue
        if len(matching_roles) != 1:
            raise RelayError(
                f"execution cleanup contains duplicate sidecar state for task {task.task_id}"
            )
        role = matching_roles[0]
        state = cast(dict[str, object], states[role])
        if state.get("quarantine_name") != quarantine.name:
            raise RelayError(f"execution cleanup quarantine did not match for task {task.task_id}")
        states[role] = {
            **state,
            "stage": "quarantined",
            "quarantined_at": state.get("quarantined_at", now),
        }
    if cleanup:
        cleanup.update(
            {
                "acknowledgment_stage": "acknowledged",
                "acknowledged_at": now,
            }
        )
        if states:
            cleanup["sidecars"] = states
    evidence = {
        source.name: quarantine.name
        for source, quarantine in sorted(quarantined.items(), key=lambda item: item[0].name)
    }
    return {
        "execution_cleanup": cleanup,
        "execution_sidecars_quarantined": True,
        "execution_sidecars_quarantined_at": now,
        "execution_sidecar_quarantines": {
            "schema_version": EXECUTION_SIDECAR_QUARANTINE_SCHEMA,
            "entries": evidence,
        },
        # Compatibility for v0.9 readers: the active sidecar names are gone,
        # while exact quarantine evidence remains with the whole job spool.
        "execution_sidecars_removed": True,
        "execution_sidecars_removed_at": now,
    }


def _rename_noreplace_at(
    directory_fd: int,
    source_name: str,
    quarantine_name: str,
) -> None:
    """Atomically rename inside one Linux directory without replacing a target."""
    if not sys.platform.startswith("linux"):
        raise ConfigurationError(
            "secure execution sidecar quarantine requires Linux renameat2(RENAME_NOREPLACE)"
        )
    libc = ctypes.CDLL(None, use_errno=True)
    renameat2 = getattr(libc, "renameat2", None)
    if renameat2 is None:
        raise ConfigurationError(
            "secure execution sidecar quarantine requires renameat2(RENAME_NOREPLACE)"
        )
    renameat2.argtypes = [
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_uint,
    ]
    renameat2.restype = ctypes.c_int
    result = renameat2(
        directory_fd,
        os.fsencode(source_name),
        directory_fd,
        os.fsencode(quarantine_name),
        1,
    )
    if result == 0:
        return
    error = ctypes.get_errno()
    if error == errno.EEXIST:
        raise FileExistsError(error, os.strerror(error), quarantine_name)
    raise OSError(error, os.strerror(error), source_name)


def _remove_execution_sidecars(
    paths: list[Path],
    *,
    spool_path: Path,
    expected_anchors: dict[Path, _RuntimeSidecarAnchor] | None = None,
    expected_quarantines: dict[Path, Path] | None = None,
    on_quarantined: Callable[[Path, Path], None] | None = None,
) -> dict[Path, Path]:
    """Atomically quarantine exact sidecar inodes and retain durable evidence."""
    anchors = expected_anchors or {}
    quarantines = expected_quarantines or {}
    storage_spool_path = internal_filesystem_path(spool_path)
    try:
        if any(path.parent != spool_path for path in paths):
            raise ConfigurationError("execution sidecar path escaped its job spool")
        missing_anchors = [path for path in paths if path not in anchors]
        if missing_anchors:
            raise ConfigurationError(
                "execution sidecar cleanup requires durable anchors: "
                + ", ".join(path.name for path in missing_anchors)
            )
        if any(
            source not in paths or target.parent != spool_path or target == source
            for source, target in quarantines.items()
        ):
            raise ConfigurationError("execution sidecar quarantine path escaped its job spool")
        try:
            spool_stat = os.stat(storage_spool_path, follow_symlinks=False)
        except FileNotFoundError as exc:
            if anchors:
                raise ConfigurationError(
                    f"anchored execution spool disappeared before cleanup: {spool_path}"
                ) from exc
            return {}
        except OSError as exc:
            raise ConfigurationError(
                f"could not inspect execution spool {spool_path}: {exc}"
            ) from exc
        if not stat_module.S_ISDIR(spool_stat.st_mode) or stat_module.S_ISLNK(spool_stat.st_mode):
            raise ConfigurationError(f"execution spool is not an owned directory: {spool_path}")
        for anchor in anchors.values():
            if anchor.descriptor is not None:
                _validate_runtime_sidecar_stat(
                    os.fstat(anchor.descriptor),
                    expected=anchor,
                    label="execution sidecar",
                )
        if os.name == "nt":
            result = _remove_execution_sidecars_windows(
                paths,
                spool_path=spool_path,
                expected_spool_identity=_progress_log_identity(spool_stat),
                expected_anchors=anchors,
                expected_quarantines=quarantines,
            )
            for source, quarantine in result.items():
                if on_quarantined is not None:
                    on_quarantined(source, quarantine)
            return result
        flags = (
            os.O_RDONLY
            | getattr(os, "O_DIRECTORY", 0)
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0)
        )
        try:
            directory_fd = os.open(storage_spool_path, flags)
        except OSError as exc:
            raise ConfigurationError(
                f"could not anchor execution spool {spool_path}: {exc}"
            ) from exc
        try:
            opened_stat = os.fstat(directory_fd)
            if _progress_log_identity(opened_stat) != _progress_log_identity(spool_stat):
                raise ConfigurationError(f"execution spool changed while opened: {spool_path}")
            result: dict[Path, Path] = {}
            for path in paths:
                anchor = anchors.get(path)
                quarantine = quarantines.get(path)
                if quarantine is None and anchor is not None:
                    quarantine = spool_path / _execution_sidecar_quarantine_name(anchor)
                if quarantine is not None and quarantine.parent != spool_path:
                    raise ConfigurationError(
                        f"execution sidecar quarantine escaped its spool: {quarantine}"
                    )
                if quarantine is not None and quarantine.name == path.name:
                    raise ConfigurationError(
                        f"execution sidecar quarantine aliases its source: {path}"
                    )
                try:
                    quarantine_stat = (
                        None
                        if quarantine is None
                        else os.stat(
                            quarantine.name,
                            dir_fd=directory_fd,
                            follow_symlinks=False,
                        )
                    )
                except FileNotFoundError:
                    quarantine_stat = None
                except OSError as exc:
                    raise ConfigurationError(
                        f"could not inspect execution sidecar quarantine {quarantine}: {exc}"
                    ) from exc
                try:
                    entry_stat = os.stat(path.name, dir_fd=directory_fd, follow_symlinks=False)
                except FileNotFoundError:
                    entry_stat = None
                except OSError as exc:
                    raise ConfigurationError(
                        f"could not inspect execution sidecar {path}: {exc}"
                    ) from exc
                if quarantine_stat is not None:
                    if anchor is None:
                        raise ConfigurationError(
                            f"execution sidecar quarantine has no durable anchor: {quarantine}"
                        )
                    _validate_runtime_sidecar_stat(
                        quarantine_stat,
                        expected=anchor,
                        label="execution sidecar quarantine",
                    )
                    if entry_stat is not None:
                        raise ConfigurationError(
                            f"execution sidecar source was replaced after quarantine: {path}"
                        )
                    result[path] = cast(Path, quarantine)
                    if on_quarantined is not None:
                        on_quarantined(path, cast(Path, quarantine))
                    continue
                if entry_stat is None:
                    raise ConfigurationError(
                        f"anchored execution sidecar and quarantine disappeared: {path}"
                    )
                if stat_module.S_ISDIR(entry_stat.st_mode):
                    raise ConfigurationError(f"execution sidecar became a directory: {path}")
                if anchor is None:
                    raise ConfigurationError(f"execution sidecar has no durable anchor: {path}")
                _validate_runtime_sidecar_stat(
                    entry_stat,
                    expected=anchor,
                    label="execution sidecar",
                )
                if quarantine is None:
                    quarantine = spool_path / _execution_sidecar_quarantine_name(anchor)
                if quarantine.parent != spool_path or quarantine.name == path.name:
                    raise ConfigurationError(
                        f"invalid execution sidecar quarantine target: {quarantine}"
                    )
                with suppress(FileExistsError):
                    _rename_noreplace_at(directory_fd, path.name, quarantine.name)
                try:
                    quarantined_stat = os.stat(
                        quarantine.name,
                        dir_fd=directory_fd,
                        follow_symlinks=False,
                    )
                except OSError as exc:
                    raise ConfigurationError(
                        f"could not verify execution sidecar quarantine {quarantine}: {exc}"
                    ) from exc
                _validate_runtime_sidecar_stat(
                    quarantined_stat,
                    expected=anchor,
                    label="execution sidecar quarantine",
                )
                try:
                    replacement_stat = os.stat(
                        path.name,
                        dir_fd=directory_fd,
                        follow_symlinks=False,
                    )
                except FileNotFoundError:
                    replacement_stat = None
                if replacement_stat is not None:
                    raise ConfigurationError(
                        f"execution sidecar source was replaced during quarantine: {path}"
                    )
                os.fsync(directory_fd)
                result[path] = quarantine
                if on_quarantined is not None:
                    on_quarantined(path, quarantine)
            return result
        finally:
            os.close(directory_fd)
    finally:
        _close_runtime_sidecar_anchors(anchors)


def _close_runtime_sidecar_anchors(
    anchors: dict[Path, _RuntimeSidecarAnchor] | None,
) -> None:
    for anchor in (anchors or {}).values():
        if anchor.descriptor is None:
            continue
        with suppress(OSError):
            os.close(anchor.descriptor)

"""Runtime-sidecar filesystem-anchor lifecycle (iowarp/clio-relay#231).

Owner module for the private "runtime metadata sidecar" file identity: the
pinned filesystem anchor a worker precreates before dispatch and later
re-validates on every open, so a swapped/relinked file can never be read as
if it were the original private channel. Four related primitives:

- ``_runtime_sidecar_anchor`` / ``_runtime_sidecar_anchor_from_metadata``
  build (or restore from durable JSON) one ``_RuntimeSidecarAnchor``.
- ``_validate_runtime_sidecar_stat`` fails closed unless an observed stat
  still matches a pinned anchor (identity, single hard link, owner, mode).
- ``_precreate_runtime_sidecar`` / ``_open_owned_sidecar`` create and later
  reopen the sidecar file, each immediately re-validating what they got.

Also owns ``_execution_sidecar_quarantine_name`` -- a pure function of one
anchor's identity (no execution-cleanup state), and the reason
``endpoint_windows_sidecar_handles.py``'s ``_remove_execution_sidecars_windows``
can depend on this module without creating a cycle back to the still-
co-resident execution-sidecar cleanup orchestration in ``endpoint.py``.

Depends only on ``endpoint_sidecar_types.py`` (the ``_RuntimeSidecarAnchor``
dataclass) and ``endpoint_progress_log_io.py`` (``_progress_log_identity``,
used by ``_open_owned_sidecar`` to detect a swap between stat and open) --
both leaves themselves, so this module stays acyclic. The Windows-handle
primitives (``endpoint_windows_sidecar_handles.py``) and the execution-
sidecar cleanup primitives import ``_validate_runtime_sidecar_stat``,
``_runtime_sidecar_anchor_from_metadata``, and
``_execution_sidecar_quarantine_name`` from here in turn.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import stat as stat_module
from pathlib import Path
from typing import BinaryIO, cast

from clio_relay.endpoint_progress_log_io import _progress_log_identity
from clio_relay.endpoint_sidecar_types import _RuntimeSidecarAnchor
from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.filesystem_paths import internal_filesystem_path


def _execution_sidecar_quarantine_name(anchor: _RuntimeSidecarAnchor) -> str:
    """Return a bounded deterministic retention name for one exact sidecar inode."""
    digest = hashlib.sha256(
        json.dumps(
            anchor.as_metadata(),
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).digest()
    token = base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")
    # Keep the target no longer than the shortest generated runtime sidecar
    # name. This preserves all 256 identity bits while avoiding a rename that
    # crosses the legacy Windows MAX_PATH boundary after the source was
    # successfully created in the same spool directory.
    return f".q1-{token}"


def _runtime_sidecar_anchor(
    file_stat: os.stat_result,
    *,
    descriptor: int | None = None,
) -> _RuntimeSidecarAnchor:
    return _RuntimeSidecarAnchor(
        device=int(file_stat.st_dev),
        inode=int(file_stat.st_ino),
        owner=int(file_stat.st_uid),
        link_count=int(file_stat.st_nlink),
        mode=stat_module.S_IMODE(file_stat.st_mode),
        descriptor=descriptor,
    )


def _runtime_sidecar_anchor_from_metadata(
    value: object,
    *,
    task_id: str,
) -> _RuntimeSidecarAnchor:
    """Restore one durable runtime-sidecar anchor without coercing its identity."""
    if not isinstance(value, dict):
        raise RelayError(f"runtime sidecar anchor is missing for task {task_id}")
    typed = cast(dict[str, object], value)
    fields = {"device", "inode", "owner", "link_count", "mode"}
    if set(typed) != fields or any(
        isinstance(typed[field], bool) or not isinstance(typed[field], int) for field in fields
    ):
        raise RelayError(f"runtime sidecar anchor is invalid for task {task_id}")
    return _RuntimeSidecarAnchor(
        device=cast(int, typed["device"]),
        inode=cast(int, typed["inode"]),
        owner=cast(int, typed["owner"]),
        link_count=cast(int, typed["link_count"]),
        mode=cast(int, typed["mode"]),
    )


def _validate_runtime_sidecar_stat(
    file_stat: os.stat_result,
    *,
    expected: _RuntimeSidecarAnchor,
    label: str,
) -> None:
    if not stat_module.S_ISREG(file_stat.st_mode):
        raise ConfigurationError(f"{label} is not a regular file")
    observed = _runtime_sidecar_anchor(file_stat)
    if observed != expected:
        raise ConfigurationError(f"{label} filesystem identity or permissions changed")
    if observed.link_count != 1:
        raise ConfigurationError(f"{label} must have exactly one hard link")
    if os.name != "nt":
        if observed.owner != os.getuid():
            raise ConfigurationError(f"{label} is not owned by the worker user")
        if observed.mode != 0o600:
            raise ConfigurationError(f"{label} mode must remain 0600")


def _precreate_runtime_sidecar(path: Path) -> _RuntimeSidecarAnchor:
    """Create an empty private runtime sidecar and pin its filesystem identity."""
    storage_path = internal_filesystem_path(path)
    flags = (
        os.O_WRONLY
        | os.O_CREAT
        | os.O_EXCL
        | getattr(os, "O_BINARY", 0)
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    try:
        descriptor = os.open(storage_path, flags, 0o600)
    except OSError as exc:
        raise ConfigurationError(
            f"could not precreate runtime metadata sidecar {path}: {exc}"
        ) from exc
    keep_descriptor = False
    try:
        os.set_inheritable(descriptor, False)
        if os.name != "nt":
            os.fchmod(descriptor, 0o600)
        opened_stat = os.fstat(descriptor)
        anchor = _runtime_sidecar_anchor(
            opened_stat,
            descriptor=(descriptor if os.name != "nt" else None),
        )
        _validate_runtime_sidecar_stat(
            opened_stat,
            expected=anchor,
            label="runtime metadata sidecar",
        )
        keep_descriptor = os.name != "nt"
        return anchor
    finally:
        if not keep_descriptor:
            os.close(descriptor)


def _open_owned_sidecar(
    path: Path,
    *,
    label: str,
    expected_anchor: _RuntimeSidecarAnchor | None = None,
) -> BinaryIO | None:
    """Open a regular relay sidecar without following symlinks or path races."""
    storage_path = internal_filesystem_path(path)
    if expected_anchor is not None and expected_anchor.descriptor is not None:
        _validate_runtime_sidecar_stat(
            os.fstat(expected_anchor.descriptor),
            expected=expected_anchor,
            label=label,
        )
    try:
        path_stat = os.stat(storage_path, follow_symlinks=False)
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise ConfigurationError(f"could not inspect {label} {path}: {exc}") from exc
    if stat_module.S_ISLNK(path_stat.st_mode):
        raise ConfigurationError(f"{label} symlinks are not allowed: {path}")
    if not stat_module.S_ISREG(path_stat.st_mode):
        raise ConfigurationError(f"{label} is not a regular file: {path}")
    if expected_anchor is not None:
        _validate_runtime_sidecar_stat(path_stat, expected=expected_anchor, label=label)
    flags = (
        os.O_RDONLY
        | getattr(os, "O_BINARY", 0)
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_NONBLOCK", 0)
    )
    try:
        descriptor = os.open(storage_path, flags)
    except OSError as exc:
        raise ConfigurationError(f"could not open {label} {path}: {exc}") from exc
    try:
        os.set_inheritable(descriptor, False)
        opened_stat = os.fstat(descriptor)
        if not stat_module.S_ISREG(opened_stat.st_mode):
            raise ConfigurationError(f"{label} is not a regular file: {path}")
        if _progress_log_identity(opened_stat) != _progress_log_identity(path_stat):
            raise ConfigurationError(f"{label} changed while it was opened: {path}")
        if expected_anchor is not None:
            _validate_runtime_sidecar_stat(opened_stat, expected=expected_anchor, label=label)
        return os.fdopen(descriptor, "rb")
    except Exception:
        os.close(descriptor)
        raise

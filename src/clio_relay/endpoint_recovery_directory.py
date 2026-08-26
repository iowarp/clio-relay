"""Private JARVIS execution-recovery directory lifecycle (iowarp/clio-relay#231).

Owner module for the pinned, private, per-attempt directory a worker creates
before dispatching a trusted JARVIS execution recovery query, plus the
generic private-JSON-file primitive that directory's contents are written
through. Three related layers:

- Timestamp/process-identity validation: ``_recovery_timestamp`` (strict
  timezone-aware ISO parsing, no coercion), ``_recovery_query_process_is_
  valid`` (the exact process identity needed to prove a blocked recovery
  query survived a worker crash), ``_jarvis_execution_recovery_retry_due``.
- Directory-anchor lifecycle: ``_recovery_directory_anchor_metadata_is_
  valid``/``_recovery_directory_anchor_from_metadata``/``_recovery_
  directory_anchor_from_stat`` build or restore one
  ``_RecoveryDirectoryAnchor``; ``_validate_recovery_directory_stat`` fails
  closed unless an observed stat still matches a pinned anchor;
  ``_open_or_create_recovery_directory``/``_close_recovery_directory_anchor``
  create or reopen the directory (via the Windows handle primitives on that
  platform) and release its handle; ``_validate_recovery_directory_path``/
  ``_validate_recovery_process_cwd`` revalidate it later against a pinned
  anchor or a blocked process's inherited cwd.
- Bounded, race-free file I/O scoped to that directory:
  ``_read_owned_recovery_result``/``_remove_owned_recovery_output`` read or
  remove one regular result file; ``_private_json_payload``/``_write_
  private_json_file`` are the generic canonical-JSON serialize/atomic-write
  primitive the recovery-intent writers use.

Depends on the leaf modules ``endpoint_sidecar_types.py`` (the
``_RecoveryDirectoryAnchor`` dataclass and Windows constants),
``endpoint_progress_log_io.py`` (``_progress_log_identity``), and
``endpoint_windows_sidecar_handles.py`` (the Windows handle primitives used
on that platform's directory-open/close/validate path) -- all leaves
themselves relative to this module, so it stays acyclic. The still-
co-resident JARVIS execution-recovery orchestration in ``endpoint.py``
(``_durable_jarvis_execution_recovery`` and friends) and ``EndpointWorker``
both import forward from here.
"""

from __future__ import annotations

import json
import os
import secrets
import stat as stat_module
import sys
from contextlib import suppress
from datetime import datetime
from pathlib import Path
from typing import Any, cast

from clio_relay.endpoint_progress_log_io import _progress_log_identity
from clio_relay.endpoint_sidecar_types import (
    _WINDOWS_FILE_ATTRIBUTE_DIRECTORY,
    _WINDOWS_FILE_ATTRIBUTE_REPARSE_POINT,
    _WINDOWS_FILE_FLAG_BACKUP_SEMANTICS,
    _WINDOWS_FILE_FLAG_OPEN_REPARSE_POINT,
    _WINDOWS_FILE_READ_ATTRIBUTES,
    _WINDOWS_FILE_SHARE_READ,
    _WINDOWS_FILE_SHARE_WRITE,
    MCP_JARVIS_EXECUTION_RECOVERY_RESULT_MAX_BYTES,
    _RecoveryDirectoryAnchor,
)
from clio_relay.endpoint_windows_sidecar_handles import (
    _close_windows_cleanup_handle,
    _open_windows_cleanup_handle,
    _windows_handle_information,
)
from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.filesystem_paths import internal_filesystem_path
from clio_relay.models import utc_now


def _recovery_timestamp(value: str) -> datetime | None:
    """Parse one timezone-aware durable recovery timestamp without coercion."""
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else None


def _recovery_query_process_is_valid(
    value: dict[str, object],
    *,
    attempted_at: datetime,
) -> bool:
    """Validate the exact process identity needed after a recovery-query crash."""
    expected_fields = {
        "schema_version",
        "pid",
        "hostname",
        "process_start_identity",
        "process_group_id",
        "started_at",
        "endpoint_id",
        "containment",
    }
    process_id = value.get("pid")
    process_group_id = value.get("process_group_id")
    endpoint_id = value.get("endpoint_id")
    raw_started_at = value.get("started_at")
    started_at = _recovery_timestamp(raw_started_at) if isinstance(raw_started_at, str) else None
    return (
        set(value) == expected_fields
        and value.get("schema_version") == "clio-relay.execution-ownership.v1"
        and isinstance(process_id, int)
        and not isinstance(process_id, bool)
        and process_id > 0
        and isinstance(value.get("hostname"), str)
        and bool(value["hostname"])
        and isinstance(value.get("process_start_identity"), str)
        and bool(value["process_start_identity"])
        and (
            process_group_id is None
            or (
                isinstance(process_group_id, int)
                and not isinstance(process_group_id, bool)
                and process_group_id > 0
            )
        )
        and started_at is not None
        and started_at >= attempted_at
        and (endpoint_id is None or (isinstance(endpoint_id, str) and bool(endpoint_id)))
        and isinstance(value.get("containment"), dict)
    )


def _jarvis_execution_recovery_retry_due(
    intent: dict[str, Any],
    *,
    now: datetime | None = None,
) -> bool:
    """Return whether one pending recovery reached its durable retry time."""
    raw_retry_at = intent.get("next_retry_at")
    if raw_retry_at is None:
        return True
    if not isinstance(raw_retry_at, str):
        raise RelayError("JARVIS execution recovery retry timestamp is invalid")
    retry_at = _recovery_timestamp(raw_retry_at)
    if retry_at is None:
        raise RelayError("JARVIS execution recovery retry timestamp is invalid")
    return (now or utc_now()) >= retry_at


def _recovery_directory_anchor_metadata_is_valid(value: object) -> bool:
    """Return whether a durable private-directory identity is exact and bounded."""
    if not isinstance(value, dict):
        return False
    typed = cast(dict[str, object], value)
    fields = {"device", "inode", "owner", "mode"}
    return set(typed) == fields and all(
        not isinstance(typed[field], bool) and isinstance(typed[field], int) for field in fields
    )


def _recovery_directory_anchor_from_metadata(value: object) -> _RecoveryDirectoryAnchor:
    """Restore a durable private recovery-directory identity."""
    if not _recovery_directory_anchor_metadata_is_valid(value):
        raise ConfigurationError("JARVIS recovery directory identity is invalid")
    typed = cast(dict[str, int], value)
    return _RecoveryDirectoryAnchor(
        device=typed["device"],
        inode=typed["inode"],
        owner=typed["owner"],
        mode=typed["mode"],
    )


def _recovery_directory_anchor_from_stat(
    observed: os.stat_result,
    *,
    descriptor: int | None = None,
    windows_handle: int | None = None,
) -> _RecoveryDirectoryAnchor:
    """Build a private recovery-directory anchor from one open filesystem object."""
    return _RecoveryDirectoryAnchor(
        device=int(observed.st_dev),
        inode=int(observed.st_ino),
        owner=int(observed.st_uid),
        mode=stat_module.S_IMODE(observed.st_mode),
        descriptor=descriptor,
        windows_handle=windows_handle,
    )


def _validate_recovery_directory_stat(
    observed: os.stat_result,
    *,
    expected: _RecoveryDirectoryAnchor | None,
    path: Path,
) -> None:
    """Fail closed unless a recovery directory is real, private, and unchanged."""
    if stat_module.S_ISLNK(observed.st_mode) or not stat_module.S_ISDIR(observed.st_mode):
        raise ConfigurationError(f"JARVIS recovery path is not a real directory: {path}")
    attributes = int(getattr(observed, "st_file_attributes", 0))
    if attributes & _WINDOWS_FILE_ATTRIBUTE_REPARSE_POINT:
        raise ConfigurationError(f"JARVIS recovery directory is a reparse point: {path}")
    current = _recovery_directory_anchor_from_stat(observed)
    if expected is not None and current != expected:
        raise ConfigurationError(f"JARVIS recovery directory identity changed: {path}")
    if os.name != "nt":
        if current.owner != os.getuid():
            raise ConfigurationError("JARVIS recovery directory is not owned by the worker user")
        if current.mode != 0o700:
            raise ConfigurationError("JARVIS recovery directory mode must remain 0700")


def _open_or_create_recovery_directory(
    path: Path,
    *,
    expected_metadata: object,
) -> tuple[_RecoveryDirectoryAnchor, bool]:
    """Create or reopen one pinned private recovery directory without following links."""
    storage_path = internal_filesystem_path(path)
    expected = (
        None
        if expected_metadata is None
        else _recovery_directory_anchor_from_metadata(expected_metadata)
    )
    created = False
    if expected is None:
        try:
            os.mkdir(storage_path, 0o700)
        except FileExistsError as exc:
            raise ConfigurationError(
                f"unowned JARVIS recovery path already exists: {path}"
            ) from exc
        except OSError as exc:
            raise ConfigurationError(
                f"could not create private JARVIS recovery directory {path}: {exc}"
            ) from exc
        created = True
        if os.name != "nt":
            os.chmod(storage_path, 0o700, follow_symlinks=False)
    try:
        path_stat = os.stat(storage_path, follow_symlinks=False)
    except OSError as exc:
        raise ConfigurationError(
            f"could not inspect private JARVIS recovery directory {path}: {exc}"
        ) from exc
    _validate_recovery_directory_stat(path_stat, expected=expected, path=path)
    if os.name == "nt":
        handle = _open_windows_cleanup_handle(
            path,
            desired_access=_WINDOWS_FILE_READ_ATTRIBUTES,
            share_mode=_WINDOWS_FILE_SHARE_READ | _WINDOWS_FILE_SHARE_WRITE,
            flags=(_WINDOWS_FILE_FLAG_BACKUP_SEMANTICS | _WINDOWS_FILE_FLAG_OPEN_REPARSE_POINT),
            missing_ok=False,
        )
        assert handle is not None
        try:
            attributes, file_id = _windows_handle_information(handle, path)
            if attributes & _WINDOWS_FILE_ATTRIBUTE_REPARSE_POINT:
                raise ConfigurationError(f"JARVIS recovery directory is a reparse point: {path}")
            if not attributes & _WINDOWS_FILE_ATTRIBUTE_DIRECTORY:
                raise ConfigurationError(f"JARVIS recovery path is not a directory: {path}")
            if file_id != int(path_stat.st_ino):
                raise ConfigurationError(f"JARVIS recovery directory changed while opening: {path}")
            anchor = _recovery_directory_anchor_from_stat(
                path_stat,
                windows_handle=handle,
            )
            if expected is not None and anchor != expected:
                raise ConfigurationError(f"JARVIS recovery directory identity changed: {path}")
            return anchor, created
        except BaseException:
            _close_windows_cleanup_handle(handle)
            raise
    flags = (
        os.O_RDONLY
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    try:
        descriptor = os.open(storage_path, flags)
    except OSError as exc:
        raise ConfigurationError(
            f"could not pin private JARVIS recovery directory {path}: {exc}"
        ) from exc
    try:
        os.set_inheritable(descriptor, False)
        opened = os.fstat(descriptor)
        anchor = _recovery_directory_anchor_from_stat(opened, descriptor=descriptor)
        _validate_recovery_directory_stat(opened, expected=expected, path=path)
        if (anchor.device, anchor.inode) != (int(path_stat.st_dev), int(path_stat.st_ino)):
            raise ConfigurationError(f"JARVIS recovery directory changed while opening: {path}")
        return anchor, created
    except BaseException:
        os.close(descriptor)
        raise


def _close_recovery_directory_anchor(anchor: _RecoveryDirectoryAnchor) -> None:
    """Release the OS handle that pinned one recovery directory."""
    if anchor.descriptor is not None:
        with suppress(OSError):
            os.close(anchor.descriptor)
    if anchor.windows_handle is not None:
        _close_windows_cleanup_handle(anchor.windows_handle)


def _validate_recovery_directory_path(
    path: Path,
    anchor: _RecoveryDirectoryAnchor,
) -> None:
    """Verify the named directory still resolves to the pinned recovery object."""
    try:
        observed = os.stat(internal_filesystem_path(path), follow_symlinks=False)
    except OSError as exc:
        raise ConfigurationError(
            f"could not revalidate private JARVIS recovery directory {path}: {exc}"
        ) from exc
    _validate_recovery_directory_stat(observed, expected=anchor, path=path)
    if anchor.descriptor is not None:
        _validate_recovery_directory_stat(
            os.fstat(anchor.descriptor),
            expected=anchor,
            path=path,
        )
    if anchor.windows_handle is not None:
        attributes, file_id = _windows_handle_information(anchor.windows_handle, path)
        if (
            attributes & _WINDOWS_FILE_ATTRIBUTE_REPARSE_POINT
            or not attributes & _WINDOWS_FILE_ATTRIBUTE_DIRECTORY
            or file_id != anchor.inode
        ):
            raise ConfigurationError(f"JARVIS recovery directory handle changed: {path}")


def _validate_recovery_process_cwd(
    process_id: int,
    *,
    directory: Path,
    anchor: _RecoveryDirectoryAnchor,
) -> None:
    """Prove the blocked recovery process inherited the pinned working directory."""
    if os.name == "nt":
        _validate_recovery_directory_path(directory, anchor)
        return
    if not sys.platform.startswith("linux"):
        raise ConfigurationError(
            "JARVIS recovery process cwd verification requires Linux or Windows"
        )
    try:
        observed = os.stat(Path("/proc") / str(process_id) / "cwd")
    except OSError as exc:
        raise ConfigurationError(
            f"could not verify JARVIS recovery process cwd for {process_id}: {exc}"
        ) from exc
    if (int(observed.st_dev), int(observed.st_ino)) != (anchor.device, anchor.inode):
        raise ConfigurationError("JARVIS recovery process cwd escaped its pinned directory")


def _read_owned_recovery_result(
    path: Path,
    *,
    directory_anchor: _RecoveryDirectoryAnchor,
) -> bytes:
    """Read one bounded regular recovery result from the pinned directory object."""
    storage_path = internal_filesystem_path(path)
    name = path.name
    try:
        path_stat = (
            os.stat(name, dir_fd=directory_anchor.descriptor, follow_symlinks=False)
            if directory_anchor.descriptor is not None
            else os.stat(storage_path, follow_symlinks=False)
        )
    except OSError as exc:
        raise ConfigurationError(f"could not inspect JARVIS recovery result {path}: {exc}") from exc
    if stat_module.S_ISLNK(path_stat.st_mode) or not stat_module.S_ISREG(path_stat.st_mode):
        raise ConfigurationError(f"JARVIS recovery result is not a regular file: {path}")
    if int(path_stat.st_nlink) != 1:
        raise ConfigurationError("JARVIS recovery result must have exactly one hard link")
    if os.name != "nt" and int(path_stat.st_uid) != os.getuid():
        raise ConfigurationError("JARVIS recovery result is not owned by the worker user")
    flags = (
        os.O_RDONLY
        | getattr(os, "O_BINARY", 0)
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    try:
        descriptor = (
            os.open(name, flags, dir_fd=directory_anchor.descriptor)
            if directory_anchor.descriptor is not None
            else os.open(storage_path, flags)
        )
    except OSError as exc:
        raise ConfigurationError(f"could not open JARVIS recovery result {path}: {exc}") from exc
    try:
        os.set_inheritable(descriptor, False)
        opened = os.fstat(descriptor)
        if (
            not stat_module.S_ISREG(opened.st_mode)
            or _progress_log_identity(opened) != _progress_log_identity(path_stat)
            or int(opened.st_nlink) != 1
            or (os.name != "nt" and int(opened.st_uid) != os.getuid())
        ):
            raise ConfigurationError("JARVIS recovery result identity changed while opening")
        if int(opened.st_size) > MCP_JARVIS_EXECUTION_RECOVERY_RESULT_MAX_BYTES:
            raise ConfigurationError("JARVIS recovery result exceeds its byte limit")
        payload = os.read(descriptor, MCP_JARVIS_EXECUTION_RECOVERY_RESULT_MAX_BYTES + 1)
        while len(payload) <= MCP_JARVIS_EXECUTION_RECOVERY_RESULT_MAX_BYTES:
            chunk = os.read(
                descriptor,
                MCP_JARVIS_EXECUTION_RECOVERY_RESULT_MAX_BYTES + 1 - len(payload),
            )
            if not chunk:
                break
            payload += chunk
        if len(payload) > MCP_JARVIS_EXECUTION_RECOVERY_RESULT_MAX_BYTES:
            raise ConfigurationError("JARVIS recovery result exceeds its byte limit")
        final_stat = os.fstat(descriptor)
        if _progress_log_identity(final_stat) != _progress_log_identity(opened):
            raise ConfigurationError("JARVIS recovery result changed while reading")
        _validate_recovery_directory_path(path.parent, directory_anchor)
        return payload
    finally:
        os.close(descriptor)


def _remove_owned_recovery_output(
    path: Path,
    *,
    directory_anchor: _RecoveryDirectoryAnchor,
) -> None:
    """Remove only a regular single-link prior result from the pinned directory."""
    storage_path = internal_filesystem_path(path)
    name = path.name
    try:
        observed = (
            os.stat(name, dir_fd=directory_anchor.descriptor, follow_symlinks=False)
            if directory_anchor.descriptor is not None
            else os.stat(storage_path, follow_symlinks=False)
        )
    except FileNotFoundError:
        return
    except OSError as exc:
        raise ConfigurationError(f"could not inspect prior JARVIS recovery output: {exc}") from exc
    if (
        stat_module.S_ISLNK(observed.st_mode)
        or not stat_module.S_ISREG(observed.st_mode)
        or int(observed.st_nlink) != 1
        or (os.name != "nt" and int(observed.st_uid) != os.getuid())
    ):
        raise ConfigurationError("prior JARVIS recovery output has unsafe filesystem identity")
    try:
        if directory_anchor.descriptor is not None:
            os.unlink(name, dir_fd=directory_anchor.descriptor)
            os.fsync(directory_anchor.descriptor)
        else:
            storage_path.unlink()
    except OSError as exc:
        raise ConfigurationError(f"could not remove prior JARVIS recovery output: {exc}") from exc
    _validate_recovery_directory_path(path.parent, directory_anchor)


def _private_json_payload(value: object) -> bytes:
    """Serialize one bounded canonical private JSON document."""
    try:
        payload = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ConfigurationError(f"private JSON input is not canonical: {exc}") from exc
    if len(payload) > 1024 * 1024:
        raise ConfigurationError("private JSON input exceeds 1 MiB")
    return payload


def write_private_json_file(
    path: Path,
    value: object,
    *,
    directory_anchor: _RecoveryDirectoryAnchor | None = None,
) -> None:
    """Atomically write private JSON without following or truncating hostile links.

    Public (clio-relay#271 direction): every owner module across the
    endpoint decomposition imports this, so the leading underscore was
    pure reportPrivateUsage noise, not a real privacy boundary.
    """
    payload = _private_json_payload(value)
    storage_path = internal_filesystem_path(path)
    parent = internal_filesystem_path(path.parent)
    name = path.name
    directory_fd = None if directory_anchor is None else directory_anchor.descriptor
    if directory_anchor is not None:
        _validate_recovery_directory_path(path.parent, directory_anchor)
    try:
        existing = (
            os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
            if directory_fd is not None
            else os.stat(storage_path, follow_symlinks=False)
        )
    except FileNotFoundError:
        existing = None
    except OSError as exc:
        raise ConfigurationError(f"could not inspect private JSON input {path}: {exc}") from exc
    if existing is not None and (
        stat_module.S_ISLNK(existing.st_mode)
        or not stat_module.S_ISREG(existing.st_mode)
        or int(existing.st_nlink) != 1
        or (os.name != "nt" and int(existing.st_uid) != os.getuid())
    ):
        raise ConfigurationError(f"private JSON input has unsafe filesystem identity: {path}")
    temporary_name = f".{name}.{secrets.token_hex(16)}.tmp"
    temporary_path = parent / temporary_name
    flags = (
        os.O_WRONLY
        | os.O_CREAT
        | os.O_EXCL
        | getattr(os, "O_BINARY", 0)
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    try:
        descriptor = (
            os.open(temporary_name, flags, 0o600, dir_fd=directory_fd)
            if directory_fd is not None
            else os.open(temporary_path, flags, 0o600)
        )
    except OSError as exc:
        raise ConfigurationError(f"could not write private JSON input {path}: {exc}") from exc
    try:
        os.set_inheritable(descriptor, False)
        if os.name != "nt":
            os.fchmod(descriptor, 0o600)
        opened = os.fstat(descriptor)
        if not stat_module.S_ISREG(opened.st_mode):
            raise ConfigurationError(f"private JSON input is not a regular file: {path}")
        if int(opened.st_nlink) != 1:
            raise ConfigurationError(f"private JSON input must have one hard link: {path}")
        if os.name != "nt" and (
            int(opened.st_uid) != os.getuid() or stat_module.S_IMODE(opened.st_mode) != 0o600
        ):
            raise ConfigurationError(f"private JSON input ownership or mode is unsafe: {path}")
        view = memoryview(payload)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise ConfigurationError(f"private JSON input write made no progress: {path}")
            view = view[written:]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    try:
        if directory_fd is not None:
            os.replace(
                temporary_name,
                name,
                src_dir_fd=directory_fd,
                dst_dir_fd=directory_fd,
            )
            os.fsync(directory_fd)
        else:
            os.replace(temporary_path, storage_path)
        final_stat = (
            os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
            if directory_fd is not None
            else os.stat(storage_path, follow_symlinks=False)
        )
        if (
            not stat_module.S_ISREG(final_stat.st_mode)
            or int(final_stat.st_nlink) != 1
            or (os.name != "nt" and int(final_stat.st_uid) != os.getuid())
        ):
            raise ConfigurationError(f"private JSON input replacement was unsafe: {path}")
        if directory_anchor is not None:
            _validate_recovery_directory_path(path.parent, directory_anchor)
    except BaseException:
        with suppress(OSError):
            if directory_fd is not None:
                os.unlink(temporary_name, dir_fd=directory_fd)
            else:
                temporary_path.unlink(missing_ok=True)
        raise

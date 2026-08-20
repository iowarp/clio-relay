"""Launch an exact recorded MCP wheel only through a private verified byte snapshot.

Split from ``runner.py`` (iowarp/clio-relay#231/#775 decomposition wave 3). Pure
leaf orchestration over :mod:`clio_relay.wheel_snapshot_identity` -- no
facade reach-back needed.
"""

from __future__ import annotations

import os
import tempfile
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from typing import Any, cast

from clio_relay.constants import FILE_HASH_CHUNK_BYTES
from clio_relay.wheel_snapshot_identity import (
    _open_posix_snapshot_cleanup_descriptors,
    _open_windows_snapshot_cleanup_handle,
    _path_matches_identity,
    _private_directory_identity,
    _private_directory_still_matches,
    _private_snapshot_permissions_safe,
    _remove_private_snapshot,
    _stream_still_matches,
    _verified_stream_identity,
)


def _wheel_install_input_identity(
    server_artifact: dict[str, Any],
) -> dict[str, Any] | None:
    """Return the unique exact wheel input recorded in release provenance."""
    if server_artifact.get("install_source") != "wheel":
        return None
    install_spec = server_artifact.get("install_spec")
    raw_inputs = server_artifact.get("input_files")
    if not isinstance(install_spec, str) or not isinstance(raw_inputs, list):
        raise ValueError("exact MCP wheel provenance is incomplete")
    try:
        resolved = str(Path(install_spec).expanduser().resolve(strict=True))
    except OSError as exc:
        raise ValueError("exact MCP wheel disappeared before launch") from exc
    matches = [
        cast(dict[str, Any], item)
        for item in cast(list[object], raw_inputs)
        if isinstance(item, dict) and cast(dict[str, Any], item).get("path") == resolved
    ]
    if len(matches) != 1:
        raise ValueError("exact MCP wheel has no unique recorded input identity")
    identity = matches[0]
    if (
        not isinstance(identity.get("sha256"), str)
        or not isinstance(identity.get("size_bytes"), int)
        or identity.get("size_bytes", -1) < 0
    ):
        raise ValueError("exact MCP wheel input identity is incomplete")
    return identity


@contextmanager
def _prepared_mcp_launch(
    command: list[str],
    *,
    server_args: list[str],
    server_artifact: dict[str, Any],
) -> Generator[tuple[list[str], dict[str, Any] | None]]:
    """Launch an exact wheel only through a private verified byte snapshot."""
    wheel_identity = _wheel_install_input_identity(server_artifact)
    if wheel_identity is None:
        yield command, None
        return
    install_spec = server_artifact.get("install_spec")
    if not isinstance(install_spec, str):
        raise ValueError("exact MCP wheel install specification is unavailable")
    from_indexes = [
        index
        for index, argument in enumerate(server_args[:-1])
        if argument == "--from" and server_args[index + 1] == install_spec
    ]
    if len(from_indexes) != 1:
        raise ValueError("exact MCP wheel has no unique --from launch argument")
    source_path = Path(cast(str, wheel_identity["path"]))
    expected_sha256 = cast(str, wheel_identity["sha256"])
    expected_size = cast(int, wheel_identity["size_bytes"])
    private_root = Path(tempfile.mkdtemp(prefix="clio-relay-mcp-wheel-"))
    snapshot_path = private_root / source_path.name
    source_stream: Any = None
    snapshot_stream: Any = None
    source_identity: tuple[int, int, int, int] | None = None
    snapshot_identity: tuple[int, int, int, int] | None = None
    directory_identity: tuple[int, int, int, int] | None = None
    posix_parent_descriptor: int | None = None
    posix_directory_descriptor: int | None = None
    windows_directory_handle: int | None = None
    windows_snapshot_handle: int | None = None
    evidence: dict[str, Any] = {
        "schema_version": "clio-relay.mcp-execution-artifact.v1",
        "source_path": str(source_path),
        "source_sha256": expected_sha256,
        "source_size_bytes": expected_size,
        "private_snapshot": True,
        "snapshot_sha256": None,
        "snapshot_size_bytes": None,
        "snapshot_verified_before_launch": False,
        "snapshot_verified_after_launch": False,
        "source_verified_after_launch": False,
        "cleanup_verified": False,
    }
    body_failure: BaseException | None = None
    security_failures: list[str] = []
    try:
        directory_identity = _private_directory_identity(private_root, writable=True)
        if os.name == "nt":
            windows_directory_handle = _open_windows_snapshot_cleanup_handle(
                private_root,
                expected_inode=directory_identity[1],
                directory=True,
            )
        else:
            posix_parent_descriptor, posix_directory_descriptor = (
                _open_posix_snapshot_cleanup_descriptors(private_root)
            )
        source_stream = source_path.open("rb")
        source_identity = _verified_stream_identity(
            source_stream,
            expected_sha256=expected_sha256,
            expected_size=expected_size,
            label="source MCP wheel",
        )
        flags = (
            os.O_WRONLY
            | os.O_CREAT
            | os.O_EXCL
            | int(getattr(os, "O_BINARY", 0))
            | int(getattr(os, "O_CLOEXEC", 0))
            | int(getattr(os, "O_NOFOLLOW", 0))
        )
        descriptor = os.open(snapshot_path, flags, 0o600)
        try:
            with os.fdopen(descriptor, "wb", closefd=False) as destination:
                source_stream.seek(0)
                while chunk := source_stream.read(FILE_HASH_CHUNK_BYTES):
                    destination.write(chunk)
                destination.flush()
                os.fsync(destination.fileno())
        finally:
            os.close(descriptor)
        if os.name != "nt":
            os.chmod(snapshot_path, 0o400)
            os.chmod(private_root, 0o500)
        directory_identity = _private_directory_identity(private_root, writable=False)
        snapshot_stream = snapshot_path.open("rb")
        snapshot_identity = _verified_stream_identity(
            snapshot_stream,
            expected_sha256=expected_sha256,
            expected_size=expected_size,
            label="private MCP wheel snapshot",
        )
        if not _private_snapshot_permissions_safe(snapshot_stream, snapshot_path):
            raise ValueError("private MCP wheel snapshot permissions are unsafe")
        if not _path_matches_identity(snapshot_path, snapshot_identity):
            raise ValueError("private MCP wheel snapshot path changed before launch")
        evidence.update(
            {
                "snapshot_sha256": expected_sha256,
                "snapshot_size_bytes": expected_size,
                "snapshot_verified_before_launch": True,
            }
        )
        snapshot_args = list(server_args)
        snapshot_args[from_indexes[0] + 1] = str(snapshot_path)
        launch_command = [command[0], *snapshot_args]
        try:
            yield launch_command, evidence
        except BaseException as exc:
            body_failure = exc
        if not _stream_still_matches(
            source_stream,
            identity=source_identity,
            expected_sha256=expected_sha256,
            expected_size=expected_size,
        ):
            security_failures.append("source MCP wheel descriptor changed during launch")
        elif not _path_matches_identity(source_path, source_identity):
            security_failures.append("source MCP wheel path changed during launch")
        else:
            evidence["source_verified_after_launch"] = True
        if not _stream_still_matches(
            snapshot_stream,
            identity=snapshot_identity,
            expected_sha256=expected_sha256,
            expected_size=expected_size,
        ):
            security_failures.append("private MCP wheel snapshot changed during launch")
        elif not _private_snapshot_permissions_safe(snapshot_stream, snapshot_path):
            security_failures.append("private MCP wheel snapshot permissions changed")
        elif not _path_matches_identity(snapshot_path, snapshot_identity):
            security_failures.append("private MCP wheel snapshot path changed during launch")
        elif not _private_directory_still_matches(
            private_root,
            directory_identity,
        ):
            security_failures.append("private MCP wheel directory changed during launch")
        else:
            evidence["snapshot_verified_after_launch"] = True
    finally:
        posix_snapshot_descriptor = (
            snapshot_stream.fileno() if os.name != "nt" and snapshot_stream is not None else None
        )
        if os.name == "nt" and snapshot_stream is not None:
            snapshot_stream.close()
        if source_stream is not None:
            source_stream.close()
        try:
            cleanup_error = _remove_private_snapshot(
                private_root,
                snapshot_path=snapshot_path,
                directory_identity=directory_identity,
                snapshot_identity=snapshot_identity,
                posix_parent_descriptor=posix_parent_descriptor,
                posix_directory_descriptor=posix_directory_descriptor,
                posix_snapshot_descriptor=posix_snapshot_descriptor,
                windows_directory_handle=windows_directory_handle,
                windows_snapshot_handle=windows_snapshot_handle,
            )
        finally:
            if os.name != "nt" and snapshot_stream is not None:
                snapshot_stream.close()
        evidence["cleanup_verified"] = cleanup_error is None
        if cleanup_error is not None:
            security_failures.append(cleanup_error)
    if security_failures:
        raise ValueError("; ".join(security_failures)) from body_failure
    if body_failure is not None:
        raise body_failure

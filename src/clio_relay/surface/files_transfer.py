"""Moving a file by its path: ``files`` ops ``upload`` and ``download`` (docs/files.md).

The agent names a file on the relay host and a path on the site; relay moves
the bytes itself, in chunks through the adapter's own file access, and checks
the SHA-256 where the file landed. Only metadata comes back, so a large file
never passes through the agent's context as base64.

The local path is read or written by the relay process, with its permissions,
so a host that runs relay inside a sandbox must let that launch read the
workspace it names.
"""

from __future__ import annotations

import asyncio
import hashlib
import os
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any, BinaryIO, Protocol, runtime_checkable

from clio_relay.jobs.file_rules import CHUNK_BYTES, PART_SUFFIX
from clio_relay.jobs.handles import FileAccess
from clio_relay.jobs.refusals import Refusal, RefusalReason

Progress = Callable[[int, int | None], Awaitable[None]]
"""Called with the bytes moved so far and the total, when known."""


@runtime_checkable
class SiteChecksum(Protocol):
    """File access that hashes a file where it is, without moving its bytes."""

    async def sha256(self, path: str) -> str | None:
        """Return the file's SHA-256 hex digest, or ``None`` when the site cannot hash."""
        ...


def chunk_size(access: FileAccess) -> int:
    """How many bytes relay moves in one call of ``access``."""
    size = getattr(access, "transfer_chunk_bytes", CHUNK_BYTES)
    return size if isinstance(size, int) and size > 0 else CHUNK_BYTES


def local_path(value: str | None) -> Path:
    """Return the relay host's path for ``value``: absolute, or ``~`` for the home directory."""
    if not value:
        raise Refusal(RefusalReason.INVALID_ARGUMENTS, "this op needs local_path")
    path = Path(os.path.expanduser(value))
    if not path.is_absolute():
        raise Refusal(
            RefusalReason.INVALID_ARGUMENTS,
            "local_path is a path on the relay host: give it absolute, or under ~",
            local_path=value,
        )
    return path


def _unreadable(path: Path, error: OSError) -> Refusal:
    return Refusal(
        RefusalReason.POLICY_REFUSED,
        f"relay cannot open {path} ({error.strerror or error}). Relay reads and writes "
        "local files as its own process: when the host runs relay inside a sandbox, the "
        "sandbox must let it reach this path",
        local_path=str(path),
    )


def _open_source(path: Path) -> tuple[BinaryIO, int]:
    try:
        if not path.is_file():
            raise Refusal(
                RefusalReason.PATH_NOT_FOUND,
                f"{path} is not a file on the relay host",
                local_path=str(path),
            )
        handle = path.open("rb")
    except PermissionError as error:
        raise _unreadable(path, error) from error
    return handle, os.fstat(handle.fileno()).st_size


async def _site_sum(access: FileAccess, path: str, chunk: int) -> tuple[str | None, str]:
    """Hash the file on the site, or read it back into relay to hash it there."""
    if isinstance(access, SiteChecksum):
        found = await access.sha256(path)
        if found:
            return found.lower(), "site"
    digest = hashlib.sha256()
    offset = 0
    while True:
        data = await access.read(path, offset=offset, limit=chunk)
        digest.update(data)
        offset += len(data)
        if len(data) < chunk:
            return digest.hexdigest(), "read_back"


async def upload(
    access: FileAccess,
    source_path: str | None,
    path: str,
    *,
    overwrite: bool,
    progress: Progress | None = None,
) -> dict[str, Any]:
    """Copy a file on the relay host to ``path`` on the site and check it there."""
    source = local_path(source_path)
    chunk = chunk_size(access)
    handle, size = await asyncio.to_thread(_open_source, source)
    digest = hashlib.sha256()
    offset = 0
    try:
        while True:
            data = await asyncio.to_thread(handle.read, chunk)
            final = len(data) < chunk or offset + len(data) >= size
            digest.update(data)
            entry = await access.write(path, data, overwrite=overwrite, offset=offset, final=final)
            offset += len(data)
            if progress is not None:
                await progress(offset, size)
            if final:
                break
    finally:
        await asyncio.to_thread(handle.close)
    sent = digest.hexdigest()
    found, how = await _site_sum(access, path, chunk)
    if found != sent:
        raise Refusal(
            RefusalReason.UNREACHABLE,
            f"{path} on the site does not match {source}: SHA-256 {found} there, {sent} "
            "here. Upload it again with overwrite",
            path=path,
            local_path=str(source),
        )
    return {
        "path": entry.path,
        "local_path": str(source),
        "size": offset,
        "sha256": sent,
        "verified": how,
    }


def _check_target(target: Path, overwrite: bool) -> Path:
    if target.is_dir():
        raise Refusal(
            RefusalReason.INVALID_ARGUMENTS,
            f"{target} is a directory; give the file's own path",
            local_path=str(target),
        )
    if target.exists() and not overwrite:
        raise Refusal(
            RefusalReason.PATH_EXISTS,
            f"{target} exists on the relay host; pass overwrite to replace it",
            local_path=str(target),
        )
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        part = target.with_name(target.name + PART_SUFFIX)
        part.open("wb").close()
    except PermissionError as error:
        raise _unreadable(target, error) from error
    return part


async def download(
    access: FileAccess,
    path: str,
    target_path: str | None,
    *,
    overwrite: bool,
    progress: Progress | None = None,
) -> dict[str, Any]:
    """Copy ``path`` on the site to a file on the relay host, checked against the site's hash."""
    target = local_path(target_path)
    entry = await access.stat(path)
    if entry.kind != "file":
        raise Refusal(RefusalReason.INVALID_ARGUMENTS, f"{path} is not a file", path=path)
    part = await asyncio.to_thread(_check_target, target, overwrite)
    chunk = chunk_size(access)
    digest = hashlib.sha256()
    offset = 0
    try:
        with part.open("wb") as handle:
            while True:
                data = await access.read(path, offset=offset, limit=chunk)
                await asyncio.to_thread(handle.write, data)
                digest.update(data)
                offset += len(data)
                if progress is not None:
                    await progress(offset, entry.size)
                if len(data) < chunk:
                    break
        got = digest.hexdigest()
        found: str | None = None
        how = "size"
        if isinstance(access, SiteChecksum):
            found = await access.sha256(path)
            how = "site" if found else how
        if (found and found.lower() != got) or (not found and offset != entry.size):
            raise Refusal(
                RefusalReason.UNREACHABLE,
                f"the copy of {path} does not match the site's; download it again",
                path=path,
                local_path=str(target),
            )
        await asyncio.to_thread(os.replace, part, target)
    finally:
        part.unlink(missing_ok=True)
    return {
        "path": entry.path,
        "local_path": str(target),
        "size": offset,
        "sha256": got,
        "verified": how,
    }

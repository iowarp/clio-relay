"""File access on a Tapis system through Tapis Files (RL46).

Paths are the system's own paths. An absolute path is taken from the
system's root directory; a relative one is under relay's work directory.
Content addressed files go in ``<work dir>/content/<sha256>`` (RL16).
"""

from __future__ import annotations

import hashlib
import posixpath
import uuid
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Any, cast

from clio_relay.adapters.tapis.client import TapisCallError, acall, aread_range
from clio_relay.jobs.file_rules import (
    check_offset,
    not_empty_refusal,
    part_path,
    protected_refusal,
    resume_refusal,
)
from clio_relay.jobs.handles import FileEntry
from clio_relay.jobs.refusals import Refusal, RefusalReason

LIST_PAGE = 1000
"""Tapis ``listFiles`` returns at most this many entries a call by default; relay pages."""


class TapisFiles:
    """``FileAccess`` over one Tapis system."""

    def __init__(self, client: Callable[[], Awaitable[Any]], system_id: str, work_dir: str) -> None:
        self._client = client
        self.system_id = system_id
        self.work_dir = work_dir.rstrip("/") or "/"

    def resolve(self, path: str) -> str:
        """Return the Tapis path for a path the caller gave."""
        if path.startswith("/"):
            joined = posixpath.normpath(path)
        else:
            joined = posixpath.normpath(posixpath.join(self.work_dir, path))
            if not (joined + "/").startswith(self.work_dir.rstrip("/") + "/"):
                raise Refusal(
                    RefusalReason.INVALID_ARGUMENTS,
                    "a relative path must stay inside relay's work directory",
                    path=path,
                )
        return joined

    def content_path(self, digest: str) -> str:
        return f"{self.work_dir}/content/{digest}"

    async def _call(self, operation: str, /, **kwargs: Any) -> Any:
        client = await self._client()
        return await acall(client, "files", operation, systemId=self.system_id, **kwargs)

    @staticmethod
    def _missing(exc: TapisCallError, path: str) -> Refusal:
        if exc.not_found:
            return Refusal(RefusalReason.PATH_NOT_FOUND, f"{path} does not exist", path=path)
        return exc.refusal(path=path)

    async def list(self, path: str) -> list[FileEntry]:
        target = self.resolve(path)
        found: list[dict[str, Any]] = []
        while True:
            try:
                page = await self._call(
                    "listFiles", path=target.lstrip("/") or "/", limit=LIST_PAGE, offset=len(found)
                )
            except TapisCallError as exc:
                raise self._missing(exc, path) from None
            items = cast(list[dict[str, Any]], page or [])
            found.extend(items)
            if len(items) < LIST_PAGE:
                break
        entries: list[FileEntry] = []
        for item in found:
            kind = str(item.get("type") or "")
            entries.append(
                FileEntry(
                    path="/" + str(item.get("path") or "").lstrip("/"),
                    kind="dir" if kind == "dir" else "file" if kind == "file" else "other",
                    size=item.get("size") if kind == "file" else None,
                    modified=_when(item.get("lastModified")),
                )
            )
        return entries

    async def _stat_info(self, path: str, *, follow: bool) -> dict[str, Any]:
        target = self.resolve(path)
        try:
            found = await self._call("getStatInfo", path=target.lstrip("/"), followLinks=follow)
        except TapisCallError as exc:
            raise self._missing(exc, path) from None
        return cast(dict[str, Any], found)

    async def stat(self, path: str) -> FileEntry:
        target = self.resolve(path)
        info = await self._stat_info(path, follow=True)
        is_dir = bool(info.get("dir"))
        modified = info.get("modifyTime")
        return FileEntry(
            path=target,
            kind="dir" if is_dir else "file",
            size=None if is_dir else int(info.get("size") or 0),
            modified=(
                datetime.fromtimestamp(float(modified), UTC)
                if isinstance(modified, int | float)
                else _when(modified)
            ),
        )

    async def exists(self, path: str) -> bool:
        try:
            await self.stat(path)
        except Refusal as refusal:
            if refusal.reason == RefusalReason.PATH_NOT_FOUND:
                return False
            raise
        return True

    async def _size(self, path: str) -> int:
        """Return the size of ``path``, or 0 when it is not there."""
        try:
            return (await self.stat(path)).size or 0
        except Refusal as refusal:
            if refusal.reason == RefusalReason.PATH_NOT_FOUND:
                return 0
            raise

    async def read(self, path: str, *, offset: int = 0, limit: int | None = None) -> bytes:
        check_offset(path, offset)
        target = self.resolve(path)
        if limit is not None and limit <= 0:
            return b""
        if limit is None and offset:
            size = (await self.stat(path)).size or 0
            if offset >= size:
                return b""
            limit = size - offset
        try:
            if limit is None:
                data = await self._call("getContents", path=target.lstrip("/"))
                return data if isinstance(data, bytes) else str(data).encode("utf-8")
            client = await self._client()
            return await aread_range(client, self.system_id, target.lstrip("/"), offset, limit)
        except TapisCallError as exc:
            raise self._missing(exc, path) from None

    async def write(
        self,
        path: str,
        data: bytes,
        *,
        overwrite: bool = False,
        offset: int = 0,
        final: bool = True,
    ) -> FileEntry:
        """Write a whole file, or one chunk of a larger one (docs/files.md).

        Tapis Files has no append, so a chunk is added by reading the part back
        and inserting it again with the chunk on the end. Each chunk is at most
        4 MiB, so this costs the part's size once per chunk.
        """
        check_offset(path, offset)
        target = self.resolve(path)
        if offset == 0:
            await self._check_target(path, target, overwrite)
            if final:
                await self._insert(target, data)
                return await self.stat(target)
        part = part_path(target)
        if offset:
            held = await self._size(part)
            if held != offset:
                raise resume_refusal(path, held)
            data = await self.read(part) + data
        if final:
            await self._check_target(path, target, overwrite)
        await self._insert(part, data)
        if not final:
            return await self.stat(part)
        await self.move(part, target)
        return await self.stat(target)

    async def _check_target(self, path: str, target: str, overwrite: bool) -> None:
        """Refuse a write onto an existing path without ``overwrite``, or onto a directory.

        Tapis moves a part into a directory that stands at the target, so relay
        never lets the target be a directory.
        """
        try:
            entry = await self.stat(target)
        except Refusal as refusal:
            if refusal.reason == RefusalReason.PATH_NOT_FOUND:
                return
            raise
        if not overwrite:
            raise Refusal(
                RefusalReason.PATH_EXISTS, f"{path} exists; pass overwrite to replace it", path=path
            )
        if entry.kind == "dir":
            raise Refusal(
                RefusalReason.PATH_EXISTS, f"{path} is a directory; relay writes files", path=path
            )

    async def _insert(self, target: str, data: bytes) -> None:
        try:
            await self._call("insert", path=target.lstrip("/"), file=data)
        except TapisCallError as exc:
            raise exc.refusal(path=target) from None

    async def move(self, source: str, target: str) -> None:
        try:
            await self._call(
                "moveCopy",
                path=self.resolve(source).lstrip("/"),
                operation="MOVE",
                newPath=self.resolve(target).lstrip("/"),
            )
        except TapisCallError as exc:
            raise exc.refusal(path=source) from None

    async def delete(self, path: str, *, recursive: bool = False) -> None:
        """Delete a file, a symlink or a directory.

        Tapis always deletes a directory with everything in it, so relay checks
        ``recursive`` first. A symlink is looked at without following it, and
        Tapis removes the link itself. Tapis does not report the home directory,
        so relay refuses the work directory and every directory above it, which
        takes in home when the work directory is under it.
        """
        if not path:
            raise Refusal(RefusalReason.INVALID_ARGUMENTS, "give the path to delete")
        target = self.resolve(path)
        if (self.work_dir.rstrip("/") + "/").startswith(target.rstrip("/") + "/"):
            raise protected_refusal(path)
        info = await self._stat_info(path, follow=False)
        if info.get("dir") and not info.get("link") and not recursive and await self.list(path):
            raise not_empty_refusal(path)
        try:
            await self._call("delete", path=target.lstrip("/"))
        except TapisCallError as exc:
            raise self._missing(exc, path) from None

    async def stage(self, data: bytes) -> str:
        digest = hashlib.sha256(data).hexdigest()
        if await self.has(digest):
            return digest
        final = self.content_path(digest)
        partial = f"{final}.part-{uuid.uuid4().hex[:12]}"
        await self._insert(partial, data)
        await self.move(partial, final)
        return digest

    async def has(self, digest: str) -> bool:
        return await self.exists(self.content_path(digest.lower()))


def _when(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None

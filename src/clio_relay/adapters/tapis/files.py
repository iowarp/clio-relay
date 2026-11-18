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

from clio_relay.adapters.tapis.client import TapisCallError, acall
from clio_relay.jobs.handles import FileEntry
from clio_relay.jobs.refusals import Refusal, RefusalReason


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
        try:
            found = await self._call("listFiles", path=target.lstrip("/") or "/")
        except TapisCallError as exc:
            raise self._missing(exc, path) from None
        entries: list[FileEntry] = []
        for item in cast(list[dict[str, Any]], found or []):
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

    async def stat(self, path: str) -> FileEntry:
        target = self.resolve(path)
        try:
            info = cast(dict[str, Any], await self._call("getStatInfo", path=target.lstrip("/")))
        except TapisCallError as exc:
            raise self._missing(exc, path) from None
        is_dir = bool(info.get("dir"))
        modified = info.get("modifyTime")
        return FileEntry(
            path=target,
            kind="dir" if is_dir else "file",
            size=None if is_dir else int(info.get("size") or 0),
            modified=(
                datetime.fromtimestamp(float(modified), UTC)
                if isinstance(modified, int | float)
                else None
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

    async def read(self, path: str, *, offset: int = 0, limit: int | None = None) -> bytes:
        target = self.resolve(path)
        headers: dict[str, str] = {}
        if limit is not None:
            if limit <= 0:
                return b""
            # Tapis reads "first,last". Ask one byte past the end and trim, which is
            # right whether the service counts the last byte in or not.
            headers["range"] = f"{offset},{offset + limit}"
        elif offset:
            size = (await self.stat(path)).size or 0
            if offset >= size:
                return b""
            headers["range"] = f"{offset},{size}"
        try:
            data = await self._call("getContents", path=target.lstrip("/"), _tapis_headers=headers)
        except TapisCallError as exc:
            raise self._missing(exc, path) from None
        body = data if isinstance(data, bytes) else str(data).encode("utf-8")
        return body if limit is None else body[:limit]

    async def write(self, path: str, data: bytes, *, overwrite: bool = False) -> FileEntry:
        target = self.resolve(path)
        if not overwrite and await self.exists(target):
            raise Refusal(RefusalReason.PATH_EXISTS, f"{path} already exists", path=path)
        await self._insert(target, data)
        return await self.stat(target)

    async def _insert(self, target: str, data: bytes) -> None:
        try:
            await self._call("insert", path=target.lstrip("/"), file=data)
        except TapisCallError as exc:
            raise exc.refusal(path=target) from None

    async def mkdir(self, path: str) -> None:
        target = self.resolve(path)
        try:
            await self._call("mkdir", path=target.lstrip("/"))
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

    async def delete(self, path: str) -> None:
        try:
            await self._call("delete", path=self.resolve(path).lstrip("/"))
        except TapisCallError as exc:
            if not exc.not_found:
                raise exc.refusal(path=path) from None

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

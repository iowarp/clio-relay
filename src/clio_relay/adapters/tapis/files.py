"""File access on a Tapis system through Tapis Files (RL46).

Paths are the host's own paths, as JARVIS and the person see them: an absolute
one is taken as it is, a relative one is under relay's work directory. Only
the Tapis calls see Tapis paths, which are relative to the system's
``rootDir`` (:meth:`TapisFiles.tapis_path`); a host path outside the
``rootDir`` is refused. The rules in docs/files.md come from
:class:`~clio_relay.adapters.site_files.SiteFiles`; this module only moves
bytes with Tapis Files.

Tapis Files has no append, so a chunked upload collects in relay's local
staging directory and goes up once, with one ``insert``, on its final chunk.
"""

from __future__ import annotations

import posixpath
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from clio_relay.adapters.site_files import Kind, SiteFiles
from clio_relay.adapters.tapis.client import TapisCallError, acall, aread_range
from clio_relay.jobs.file_rules import protected_refusal
from clio_relay.jobs.handles import FileEntry
from clio_relay.jobs.refusals import Refusal, RefusalReason

LIST_PAGE = 1000
"""Tapis ``listFiles`` returns at most this many entries a call by default; relay pages."""

WORK_DIR_NAME = ".clio-relay/tapis"
"""Where relay's work directory sits under the person's home on a host relay registered."""


def home_for(work_dir: str) -> str:
    """Return the home directory relay can tell from ``work_dir``.

    Tapis does not report a home. On a host relay registered, the work
    directory is ``<home>/.clio-relay/tapis``; anywhere else ``~`` is the work
    directory itself.
    """
    work_dir = posixpath.normpath(work_dir)
    suffix = "/" + WORK_DIR_NAME
    if work_dir.endswith(suffix) and len(work_dir) > len(suffix):
        return work_dir.removesuffix(suffix)
    return work_dir


class TapisFiles(SiteFiles):
    """``FileAccess`` over one Tapis system, in host paths."""

    def __init__(
        self,
        client: Callable[[], Awaitable[Any]],
        system_id: str,
        work_dir: str,
        *,
        staging: Path,
        home: str | None = None,
        root_dir: str = "/",
    ) -> None:
        """``work_dir`` is a Tapis path; ``root_dir`` is the system's ``rootDir`` on the host."""
        self.root_dir = posixpath.normpath(root_dir)
        host_work_dir = self.host_path(work_dir)
        super().__init__(root=host_work_dir, home=home or home_for(host_work_dir), staging=staging)
        self._client = client
        self.system_id = system_id

    def host_path(self, path: str) -> str:
        """Return the host's path for a Tapis path."""
        return posixpath.normpath(f"{self.root_dir.rstrip('/')}/{path.lstrip('/')}")

    def tapis_path(self, path: str) -> str:
        """Return the Tapis path for a host path, refusing one outside the ``rootDir``."""
        path = posixpath.normpath(path)
        if self.root_dir == "/":
            return path
        if path == self.root_dir:
            return "/"
        if path.startswith(self.root_dir + "/"):
            return path[len(self.root_dir) :]
        raise Refusal(
            RefusalReason.PATH_NOT_FOUND,
            f"{path} is outside the Tapis system's rootDir {self.root_dir}, "
            "so Tapis cannot reach it",
            path=path,
            root_dir=self.root_dir,
        )

    async def _call(self, operation: str, path: str, /, **kwargs: Any) -> Any:
        client = await self._client()
        try:
            return await acall(
                client,
                "files",
                operation,
                systemId=self.system_id,
                path=_api(self.tapis_path(path)),
                **kwargs,
            )
        except TapisCallError as exc:
            if exc.not_found:
                raise self._missing(path) from None
            raise exc.refusal(path=path) from None

    async def kind(self, path: str) -> Kind:
        try:
            info = cast(dict[str, Any], await self._call("getStatInfo", path, followLinks=False))
        except Refusal as refusal:
            if refusal.reason == RefusalReason.PATH_NOT_FOUND:
                return "missing"
            raise
        if info.get("link"):
            return "link"
        return "dir" if info.get("dir") else "file"

    async def describe(self, path: str) -> FileEntry:
        info = cast(dict[str, Any], await self._call("getStatInfo", path, followLinks=True))
        is_dir = bool(info.get("dir"))
        return FileEntry(
            path=path,
            kind="dir" if is_dir else "file",
            size=None if is_dir else int(info.get("size") or 0),
            modified=_when(info.get("modifyTime")),
        )

    async def children(self, path: str) -> list[FileEntry]:
        found: list[dict[str, Any]] = []
        while True:
            page = await self._call("listFiles", path, limit=LIST_PAGE, offset=len(found))
            items = cast(list[dict[str, Any]], page or [])
            found.extend(items)
            if len(items) < LIST_PAGE:
                break
        entries: list[FileEntry] = []
        for item in found:
            kind = str(item.get("type") or "")
            entries.append(
                FileEntry(
                    path=self.host_path(str(item.get("path") or "")),
                    kind="dir" if kind == "dir" else "file" if kind == "file" else "other",
                    size=item.get("size") if kind == "file" else None,
                    modified=_when(item.get("lastModified")),
                )
            )
        return sorted(entries, key=lambda entry: posixpath.basename(entry.path))

    async def get(self, path: str, offset: int, limit: int | None) -> bytes:
        if limit == 0:
            return b""
        if limit is None and offset:
            size = (await self.describe(path)).size or 0
            if offset >= size:
                return b""
            limit = size - offset
        if limit is None:
            data = await self._call("getContents", path)
            return data if isinstance(data, bytes) else str(data).encode("utf-8")
        client = await self._client()
        try:
            return await aread_range(
                client, self.system_id, _api(self.tapis_path(path)), offset, limit
            )
        except TapisCallError as exc:
            if exc.not_found:
                raise self._missing(path) from None
            raise exc.refusal(path=path) from None

    async def put(self, path: str, data: bytes) -> None:
        """Insert ``data`` as ``path``, refusing a directory there: Tapis would write into it."""
        if await self.kind(path) == "dir":
            raise Refusal(
                RefusalReason.PATH_EXISTS, f"{path} is a directory; relay writes files", path=path
            )
        await self._call("insert", path, file=data)

    async def remove(self, path: str) -> None:
        await self._call("delete", path)

    async def remove_tree(self, path: str) -> None:
        """Delete a tree with one Tapis call, never one that holds relay's work directory.

        Tapis does not report the home directory, so relay refuses every
        directory at or above its work directory.
        """
        if (self.root.rstrip("/") + "/").startswith(path.rstrip("/") + "/"):
            raise protected_refusal(path)
        await self._call("delete", path)


def _api(path: str) -> str:
    """Return ``path`` as Tapis Files takes it: relative to the system's root."""
    return path.lstrip("/") or "/"


def _when(value: Any) -> datetime | None:
    if isinstance(value, int | float) and not isinstance(value, bool):
        return datetime.fromtimestamp(float(value), UTC)
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None

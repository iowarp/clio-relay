"""``files`` for the ``iri`` adapter, over the IRI ``/filesystem`` calls.

Every call is an asynchronous task the client polls. The API moves files of up
to 5 MiB, so larger reads and writes are refused with a pointer to Globus.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import posixpath
import stat as stat_module
from datetime import UTC, datetime
from typing import Any, Literal, cast

from clio_relay.adapters.iri.client import FILE_SIZE_LIMIT, IriClient
from clio_relay.jobs.file_rules import check_relative, no_delete, whole_writes_only
from clio_relay.jobs.handles import FileEntry
from clio_relay.jobs.refusals import Refusal, RefusalReason

_DIR_TYPES = ("d", "dir", "directory")
_FILE_TYPES = ("-", "f", "file", "regular file")


def _kind(value: object) -> Literal["file", "dir", "other"]:
    text = str(value).lower()
    if text in _DIR_TYPES:
        return "dir"
    if text in _FILE_TYPES:
        return "file"
    return "other"


def _time(value: object) -> datetime | None:
    if isinstance(value, int | float):
        return datetime.fromtimestamp(float(value), UTC)
    if isinstance(value, str) and value:
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
    return None


def _size(value: object) -> int | None:
    try:
        return int(cast(Any, value))
    except (TypeError, ValueError):
        return None


def decode_download(result: dict[str, Any]) -> bytes:
    """Return the bytes of a download task's result.

    The reference implementation leaves the encoding to each facility: some
    return the text, some a base64 field. Both are accepted.
    """
    output: Any = result.get("output")
    if isinstance(output, str):
        return output.encode("utf-8")
    if isinstance(output, dict):
        fields = cast(dict[str, Any], output)
        for key in ("content_base64", "base64", "data_base64"):
            value = fields.get(key)
            if isinstance(value, str):
                return base64.b64decode(value)
        for key in ("data", "content"):
            value = fields.get(key)
            if isinstance(value, str):
                try:
                    return base64.b64decode(value, validate=True)
                except (binascii.Error, ValueError):
                    return value.encode("utf-8")
    raise Refusal(RefusalReason.UNREACHABLE, "the IRI download returned no content")


class IriFileAccess:
    """File access on one IRI filesystem resource."""

    def __init__(
        self, client: IriClient, *, resource_id: str, root: str | None, content_dir: str
    ) -> None:
        self.client = client
        self.resource_id = resource_id
        self.root = root
        self.content_dir = content_dir

    def resolve(self, path: str) -> str:
        """Return the absolute path on the facility."""
        check_relative(path)
        if path.startswith("/"):
            return posixpath.normpath(path)
        if self.root is None:
            raise Refusal(
                RefusalReason.INVALID_CONFIG,
                "relative paths need a root; set root in the site's iri settings",
            )
        return posixpath.normpath(posixpath.join(self.root, path or "."))

    async def _task(self, command: str, args: dict[str, Any], **kw: Any) -> dict[str, Any]:
        return await self.client.file_task(command, self.resource_id, args, **kw)

    async def list(self, path: str) -> list[FileEntry]:
        target = self.resolve(path)
        result = await self._task(
            "ls",
            {
                "path": target,
                "showHidden": True,
                "numericUid": False,
                "recursive": False,
                "dereference": False,
            },
        )
        items: Any = result.get("output") or []
        entries: list[FileEntry] = []
        for raw in cast(list[Any], items if isinstance(items, list) else []):
            if not isinstance(raw, dict):
                continue
            item = cast(dict[str, Any], raw)
            name = str(item.get("name", ""))
            kind = _kind(item.get("type"))
            full = name if name.startswith("/") else posixpath.join(target, name)
            entries.append(
                FileEntry(
                    path=full,
                    kind=kind,
                    size=_size(item.get("size")) if kind == "file" else None,
                    modified=_time(item.get("last_modified") or item.get("lastModified")),
                )
            )
        return entries

    async def stat(self, path: str) -> FileEntry:
        target = self.resolve(path)
        result = await self._task("stat", {"path": target, "dereference": True})
        output: Any = result.get("output")
        if not isinstance(output, dict):
            raise Refusal(RefusalReason.PATH_NOT_FOUND, f"{path} does not exist", path=path)
        info = cast(dict[str, Any], output)
        mode = _size(info.get("mode")) or 0
        kind: Literal["file", "dir", "other"] = (
            "dir" if stat_module.S_ISDIR(mode) else "file" if stat_module.S_ISREG(mode) else "other"
        )
        return FileEntry(
            path=target,
            kind=kind,
            size=_size(info.get("size")) if kind == "file" else None,
            modified=_time(info.get("mtime")),
        )

    async def _exists(self, path: str) -> FileEntry | None:
        try:
            return await self.stat(path)
        except Refusal as refusal:
            if refusal.reason == RefusalReason.PATH_NOT_FOUND:
                return None
            raise

    async def mkdir(self, path: str) -> None:
        """Make a directory and its parents when it does not exist."""
        target = self.resolve(path)
        if await self._exists(target) is None:
            await self._task("mkdir", {"path": target, "parent": True})

    async def read(self, path: str, *, offset: int = 0, limit: int | None = None) -> bytes:
        entry = await self.stat(path)
        if entry.kind != "file":
            raise Refusal(RefusalReason.PATH_NOT_FOUND, f"{path} is not a file", path=path)
        if entry.size is not None and entry.size > FILE_SIZE_LIMIT:
            raise Refusal(
                RefusalReason.INVALID_ARGUMENTS,
                f"{path} is {entry.size} bytes; the IRI API moves files up to "
                f"{FILE_SIZE_LIMIT} bytes, so use the site's globus adapter for it",
            )
        data = decode_download(await self._task("download", {"path": entry.path}))
        start = max(0, offset)
        return data[start:] if limit is None else data[start : start + max(0, limit)]

    async def write(
        self,
        path: str,
        data: bytes,
        *,
        overwrite: bool = False,
        offset: int = 0,
        final: bool = True,
    ) -> FileEntry:
        whole_writes_only(path, offset, final)
        if len(data) > FILE_SIZE_LIMIT:
            raise Refusal(
                RefusalReason.INVALID_ARGUMENTS,
                f"the IRI API moves files up to {FILE_SIZE_LIMIT} bytes; use the site's "
                "globus adapter for larger files",
            )
        if not overwrite and await self._exists(path) is not None:
            raise Refusal(
                RefusalReason.PATH_EXISTS, f"{path} exists; pass overwrite to replace it", path=path
            )
        target = self.resolve(path)
        parent = posixpath.dirname(target)
        if parent:
            await self.mkdir(parent)
        await self._task("upload", {"path": target}, content=data)
        return await self.stat(target)

    async def delete(self, path: str, *, recursive: bool = False) -> None:
        raise no_delete(path)

    def _cas(self, digest: str) -> str:
        return posixpath.join(self.content_dir, digest)

    async def stage(self, data: bytes) -> str:
        digest = hashlib.sha256(data).hexdigest()
        if not await self.has(digest):
            await self.write(self._cas(digest), data, overwrite=True)
        return digest

    async def has(self, digest: str) -> bool:
        return await self._exists(self._cas(digest)) is not None

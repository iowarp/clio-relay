"""``files`` for the ``iri`` adapter, over the IRI ``/filesystem`` calls.

Every call is an asynchronous task the client polls. The API moves files of up
to 5 MiB, so larger reads and writes are refused with a pointer to Globus. The
rules of docs/files.md come from :class:`~clio_relay.adapters.site_files.SiteFiles`.
"""

from __future__ import annotations

import base64
import binascii
import posixpath
import stat as stat_module
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, ClassVar, Literal, cast

from clio_relay.adapters.iri.client import FILE_SIZE_LIMIT, IriClient
from clio_relay.adapters.site_files import Kind, SiteFiles
from clio_relay.jobs.handles import FileEntry
from clio_relay.jobs.refusals import Refusal, RefusalReason

GLOBUS_HINT = "Use the site's globus adapter for larger files."
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


class IriFiles(SiteFiles):
    """``files`` on one IRI filesystem resource, from the six primitives of :class:`SiteFiles`.

    ``root`` and ``home`` are optional settings, since the API reports neither.
    A path that needs one that is not set is refused with ``invalid_config``.
    """

    max_upload_bytes: ClassVar[int | None] = FILE_SIZE_LIMIT
    upload_hint: ClassVar[str] = GLOBUS_HINT

    def __init__(
        self,
        client: IriClient,
        *,
        resource_id: str,
        root: str | None,
        home: str | None,
        staging: Path,
    ) -> None:
        super().__init__(root=root or "/", home=home or "/", staging=staging)
        self.client = client
        self.resource_id = resource_id
        self.has_root = root is not None
        self.has_home = home is not None

    def resolve(self, path: str) -> str:
        """Return the absolute path on the facility, once its setting is there."""
        in_home = path == "~" or path.startswith("~/")
        if in_home and not self.has_home:
            raise Refusal(
                RefusalReason.INVALID_CONFIG,
                "paths under ~ need the home directory; set home in the site's iri settings",
            )
        if not path.startswith("/") and not in_home and not self.has_root:
            raise Refusal(
                RefusalReason.INVALID_CONFIG,
                "relative paths need a root; set root in the site's iri settings",
            )
        return super().resolve(path)

    def content_path(self, digest: str) -> str:
        if not self.has_root:
            raise Refusal(
                RefusalReason.INVALID_CONFIG,
                "staging content needs a root; set root in the site's iri settings",
            )
        return super().content_path(digest)

    async def _task(self, command: str, args: dict[str, Any], **kw: Any) -> dict[str, Any]:
        return await self.client.file_task(command, self.resource_id, args, **kw)

    async def _stat(self, path: str, *, dereference: bool) -> dict[str, Any]:
        result = await self._task("stat", {"path": path, "dereference": dereference})
        output: Any = result.get("output")
        if not isinstance(output, dict):
            raise Refusal(RefusalReason.PATH_NOT_FOUND, f"{path} does not exist", path=path)
        return cast(dict[str, Any], output)

    async def kind(self, path: str) -> Kind:
        try:
            info = await self._stat(path, dereference=False)
        except Refusal as refusal:
            if refusal.reason == RefusalReason.PATH_NOT_FOUND:
                return "missing"
            raise
        mode = _size(info.get("mode")) or 0
        if stat_module.S_ISLNK(mode):
            return "link"
        return "dir" if stat_module.S_ISDIR(mode) else "file"

    async def describe(self, path: str) -> FileEntry:
        info = await self._stat(path, dereference=True)
        mode = _size(info.get("mode")) or 0
        kind: Literal["file", "dir", "other"] = (
            "dir" if stat_module.S_ISDIR(mode) else "file" if stat_module.S_ISREG(mode) else "other"
        )
        return FileEntry(
            path=path,
            kind=kind,
            size=_size(info.get("size")) if kind == "file" else None,
            modified=_time(info.get("mtime")),
        )

    async def children(self, path: str) -> list[FileEntry]:
        result = await self._task(
            "ls",
            {
                "path": path,
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
            entries.append(
                FileEntry(
                    path=name if name.startswith("/") else posixpath.join(path, name),
                    kind=kind,
                    size=_size(item.get("size")) if kind == "file" else None,
                    modified=_time(item.get("last_modified") or item.get("lastModified")),
                )
            )
        return sorted(entries, key=lambda entry: entry.path)

    async def get(self, path: str, offset: int, limit: int | None) -> bytes:
        entry = await self.describe(path)
        if entry.size is not None and entry.size > FILE_SIZE_LIMIT:
            raise Refusal(
                RefusalReason.INVALID_ARGUMENTS,
                f"{path} is {entry.size} bytes; the IRI API moves files up to "
                f"{FILE_SIZE_LIMIT} bytes. {GLOBUS_HINT}",
                path=path,
            )
        data = decode_download(await self._task("download", {"path": path}))
        return data[offset:] if limit is None else data[offset : offset + limit]

    async def mkdir(self, path: str) -> None:
        """Make a directory and its parents when it does not exist."""
        target = self.resolve(path)
        if await self.kind(target) == "missing":
            await self._task("mkdir", {"path": target, "parent": True})

    async def put(self, path: str, data: bytes) -> None:
        parent = posixpath.dirname(path)
        if parent:
            await self.mkdir(parent)
        await self._task("upload", {"path": path}, content=data)

    async def remove(self, path: str) -> None:
        await self._task("rm", {"path": path})

    async def remove_tree(self, path: str) -> None:
        # The API's rm removes a directory with everything in it, in one task.
        await self._task("rm", {"path": path})

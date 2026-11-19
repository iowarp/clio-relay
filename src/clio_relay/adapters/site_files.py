"""``files`` for a remote site, from six small primitives (docs/files.md).

An adapter whose site is reached over a network (a REST API, SFTP, a transfer
service) subclasses :class:`SiteFiles` and implements the methods marked
abstract. Every rule in docs/files.md is applied here: relative paths,
``overwrite``, chunked uploads and their resume offsets, delete's refusals,
the protected directories, and the site's content store. So every adapter
built on it behaves the same and passes the conformance kit's files tests.

The primitives take absolute POSIX paths on the site, already resolved and
checked. They do not apply rules; they only move bytes. None of them appends
or removes a tree, because many site APIs cannot:

- A chunked upload is kept in a local staging directory and pushed once, with
  :meth:`SiteFiles.upload`, when its ``final`` chunk arrives. The resume point
  that ``stat("<path>.relay-part")`` reports comes from the staged part.
- A recursive delete walks the tree and removes one entry at a time, deepest
  first. An adapter whose site removes a tree in one call overrides
  :meth:`SiteFiles.remove_tree`.
- A site that caps one upload sets :attr:`SiteFiles.max_upload_bytes` and
  :attr:`SiteFiles.upload_hint`; a larger file refuses before any byte moves.
"""

from __future__ import annotations

import hashlib
import posixpath
from abc import ABC, abstractmethod
from pathlib import Path
from typing import ClassVar, Literal

from clio_relay.jobs.file_rules import (
    PART_SUFFIX,
    check_offset,
    check_relative,
    not_empty_refusal,
    protected_refusal,
    resume_refusal,
)
from clio_relay.jobs.handles import FileEntry
from clio_relay.jobs.refusals import Refusal, RefusalReason

type Kind = Literal["file", "dir", "link", "missing"]


class SiteFiles(ABC):
    """:class:`~clio_relay.jobs.handles.FileAccess` on a site, from primitives.

    ``root`` is relay's work directory on the site and ``home`` the site user's
    home, both absolute. ``staging`` is a local directory for chunked uploads
    in progress, normally ``context.state_dir / "parts"``. Staged content
    lives on the site under ``<root>/.relay-content``.
    """

    max_upload_bytes: ClassVar[int | None] = None
    """The most bytes the site takes in one upload, or None for no limit."""

    upload_hint: ClassVar[str] = ""
    """What to use instead when a file is over ``max_upload_bytes``."""

    def __init__(self, *, root: str, home: str, staging: Path) -> None:
        self.root = posixpath.normpath(root)
        self.home = posixpath.normpath(home)
        self.staging = staging

    # The primitives an adapter implements.

    @abstractmethod
    async def kind(self, path: str) -> Kind:
        """Say what ``path`` is, without following a symlink."""

    @abstractmethod
    async def describe(self, path: str) -> FileEntry:
        """Return the entry for an existing ``path``, following a symlink."""

    @abstractmethod
    async def children(self, path: str) -> list[FileEntry]:
        """List an existing directory, sorted by name, with absolute paths."""

    @abstractmethod
    async def get(self, path: str, offset: int, limit: int | None) -> bytes:
        """Read up to ``limit`` bytes of an existing file from ``offset`` (all when None)."""

    @abstractmethod
    async def put(self, path: str, data: bytes) -> None:
        """Write ``data`` as the whole of ``path``, creating parent directories."""

    @abstractmethod
    async def remove(self, path: str) -> None:
        """Remove a file, a symlink (never its target) or an empty directory."""

    # Optional: override when the site does better.

    async def upload(self, path: str, local: Path) -> None:
        """Write the local file ``local`` as ``path``. Override to stream it."""
        await self.put(path, local.read_bytes())

    async def remove_tree(self, path: str) -> None:
        """Remove a directory and everything under it, never following a symlink."""
        for child in await self.children(path):
            if await self.kind(child.path) == "dir":
                await self.remove_tree(child.path)
            else:
                await self.remove(child.path)
        await self.remove(path)

    # The rules. Adapters do not override these.

    def resolve(self, path: str) -> str:
        """Return the absolute site path for a ``files`` path."""
        check_relative(path)
        if not path:
            return self.root
        if path == "~" or path.startswith("~/"):
            return posixpath.normpath(self.home + path[1:])
        if path.startswith("/"):
            return posixpath.normpath(path)
        return posixpath.normpath(posixpath.join(self.root, path))

    def _part(self, target: str) -> Path:
        name = hashlib.sha256(target.encode()).hexdigest()[:32]
        return self.staging / f"{name}{PART_SUFFIX}"

    @staticmethod
    def _missing(path: str) -> Refusal:
        return Refusal(RefusalReason.PATH_NOT_FOUND, f"{path} does not exist", path=path)

    @staticmethod
    def _exists(path: str) -> Refusal:
        return Refusal(
            RefusalReason.PATH_EXISTS, f"{path} exists; pass overwrite to replace it", path=path
        )

    def _check_size(self, path: str, size: int) -> None:
        limit = self.max_upload_bytes
        if limit is not None and size > limit:
            hint = f" {self.upload_hint}" if self.upload_hint else ""
            raise Refusal(
                RefusalReason.INVALID_ARGUMENTS,
                f"{path} would be {size} bytes; this site takes at most {limit} in one upload."
                + hint,
                path=path,
                max_upload_bytes=limit,
            )

    async def list(self, path: str) -> list[FileEntry]:
        target = self.resolve(path)
        kind = await self.kind(target)
        if kind == "missing":
            raise self._missing(path)
        entry = await self.describe(target)
        return await self.children(target) if entry.kind == "dir" else [entry]

    async def stat(self, path: str) -> FileEntry:
        target = self.resolve(path)
        if target.endswith(PART_SUFFIX):
            part = self._part(target.removesuffix(PART_SUFFIX))
            if part.is_file():
                return FileEntry(path=target, kind="file", size=part.stat().st_size)
        if await self.kind(target) == "missing":
            raise self._missing(path)
        return await self.describe(target)

    async def read(self, path: str, *, offset: int = 0, limit: int | None = None) -> bytes:
        target = self.resolve(path)
        if await self.kind(target) == "missing" or (await self.describe(target)).kind != "file":
            raise self._missing(path)
        return await self.get(target, max(0, offset), None if limit is None else max(0, limit))

    async def write(
        self,
        path: str,
        data: bytes,
        *,
        overwrite: bool = False,
        offset: int = 0,
        final: bool = True,
    ) -> FileEntry:
        check_offset(path, offset)
        target = self.resolve(path)
        self._check_size(path, offset + len(data))
        if offset == 0 and not overwrite and await self.kind(target) != "missing":
            raise self._exists(path)
        if offset == 0 and final:
            await self.put(target, data)
            return await self.describe(target)
        part = self._part(target)
        held = part.stat().st_size if offset and part.is_file() else 0
        if held != offset:
            raise resume_refusal(path, held)
        # Refuse before the final chunk joins the part, so a resend with overwrite fits.
        if final and offset and not overwrite and await self.kind(target) != "missing":
            raise self._exists(path)
        part.parent.mkdir(parents=True, exist_ok=True)
        with part.open("ab" if offset else "wb") as handle:
            handle.write(data)
        if not final:
            return FileEntry(path=target + PART_SUFFIX, kind="file", size=offset + len(data))
        await self.upload(target, part)
        part.unlink()
        return await self.describe(target)

    async def delete(self, path: str, *, recursive: bool = False) -> None:
        if not path:
            raise Refusal(RefusalReason.INVALID_ARGUMENTS, "give the path to delete")
        target = self.resolve(path)
        # Before ``kind``: a facility may refuse to stat ``/`` at all.
        if target in {self.root, self.home, "/"}:
            raise protected_refusal(path)
        kind = await self.kind(target)
        if kind == "missing":
            raise self._missing(path)
        if kind != "dir":
            await self.remove(target)
        elif await self.children(target):
            if not recursive:
                raise not_empty_refusal(path)
            await self.remove_tree(target)
        else:
            await self.remove(target)

    def content_path(self, digest: str) -> str:
        """Where the site keeps staged bytes named ``digest``."""
        return posixpath.join(self.root, ".relay-content", digest)

    async def stage(self, data: bytes) -> str:
        digest = hashlib.sha256(data).hexdigest()
        target = self.content_path(digest)
        if await self.kind(target) == "missing":
            self._check_size(target, len(data))
            await self.put(target, data)
        return digest

    async def has(self, digest: str) -> bool:
        return await self.kind(self.content_path(digest.lower())) == "file"

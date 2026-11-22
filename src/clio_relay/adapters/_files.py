"""File access on the machine relay runs on. The ``local`` and fixture adapters use it."""

from __future__ import annotations

import asyncio
import hashlib
import os
import shutil
from datetime import UTC, datetime
from pathlib import Path

from clio_relay.jobs.content import ContentStore
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


def _entry(path: Path, shown: str) -> FileEntry:
    info = path.stat()
    kind = "dir" if path.is_dir() else "file" if path.is_file() else "other"
    return FileEntry(
        path=shown,
        kind=kind,
        size=info.st_size if kind == "file" else None,
        modified=datetime.fromtimestamp(info.st_mtime, UTC),
    )


class LocalFileAccess:
    """Paths are absolute, or relative to ``root``."""

    def __init__(self, root: Path, content: ContentStore) -> None:
        self.root = root
        self.content = content

    def resolve(self, path: str) -> Path:
        """Return the host path for ``path``."""
        check_relative(path)
        expanded = Path(os.path.expanduser(path)) if path else self.root
        return expanded if expanded.is_absolute() else self.root / expanded

    def _missing(self, path: str) -> Refusal:
        return Refusal(RefusalReason.PATH_NOT_FOUND, f"{path} does not exist", path=path)

    async def list(self, path: str) -> list[FileEntry]:
        target = self.resolve(path)

        def _list() -> list[FileEntry]:
            if not target.exists():
                raise self._missing(path)
            if not target.is_dir():
                return [_entry(target, str(target))]
            return [
                _entry(child, str(child))
                for child in sorted(target.iterdir(), key=lambda item: item.name)
            ]

        return await asyncio.to_thread(_list)

    async def stat(self, path: str) -> FileEntry:
        target = self.resolve(path)
        if not target.exists():
            raise self._missing(path)
        return await asyncio.to_thread(_entry, target, str(target))

    async def read(self, path: str, *, offset: int = 0, limit: int | None = None) -> bytes:
        target = self.resolve(path)

        def _read() -> bytes:
            if not target.is_file():
                raise self._missing(path)
            with target.open("rb") as handle:
                handle.seek(max(0, offset))
                return handle.read(-1 if limit is None else max(0, limit))

        return await asyncio.to_thread(_read)

    async def sha256(self, path: str) -> str:
        """Hash ``path`` where it is."""
        target = self.resolve(path)

        def _sum() -> str:
            if not target.is_file():
                raise self._missing(path)
            with target.open("rb") as handle:
                return hashlib.file_digest(handle, "sha256").hexdigest()

        return await asyncio.to_thread(_sum)

    def _exists_refusal(self, path: str) -> Refusal:
        return Refusal(
            RefusalReason.PATH_EXISTS, f"{path} exists; pass overwrite to replace it", path=path
        )

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
        part = target.with_name(target.name + PART_SUFFIX)

        def _write() -> FileEntry:
            if offset == 0 and target.exists() and not overwrite:
                raise self._exists_refusal(path)
            target.parent.mkdir(parents=True, exist_ok=True)
            if offset == 0 and final:
                tmp = target.with_name(f".{target.name}.relay-tmp")
                tmp.write_bytes(data)
                os.replace(tmp, target)
                return _entry(target, str(target))
            if offset == 0:
                part.write_bytes(data)
            else:
                held = part.stat().st_size if part.is_file() else 0
                if held != offset:
                    raise resume_refusal(path, held)
                with part.open("ab") as handle:
                    handle.write(data)
            if not final:
                return _entry(part, str(part))
            if target.exists() and not overwrite:
                raise self._exists_refusal(path)
            os.replace(part, target)
            return _entry(target, str(target))

        return await asyncio.to_thread(_write)

    def _protected(self, target: Path) -> bool:
        resolved = target.resolve()
        return resolved in {self.root.resolve(), Path.home().resolve(), Path(resolved.anchor)}

    async def delete(self, path: str, *, recursive: bool = False) -> None:
        if not path:
            raise Refusal(RefusalReason.INVALID_ARGUMENTS, "give the path to delete")
        target = self.resolve(path)

        def _delete() -> None:
            if target.is_symlink() or target.is_file():
                target.unlink()
                return
            if not target.exists():
                raise self._missing(path)
            if self._protected(target):
                raise protected_refusal(path)
            if not target.is_dir():
                target.unlink()
            elif recursive:
                shutil.rmtree(target)
            elif any(target.iterdir()):
                raise not_empty_refusal(path)
            else:
                target.rmdir()

        await asyncio.to_thread(_delete)

    async def stage(self, data: bytes) -> str:
        return await asyncio.to_thread(self.content.put, data)

    async def has(self, digest: str) -> bool:
        return await asyncio.to_thread(self.content.has, digest)

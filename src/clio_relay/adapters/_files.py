"""File access on the machine relay runs on. The ``local`` and fixture adapters use it."""

from __future__ import annotations

import asyncio
import os
from datetime import UTC, datetime
from pathlib import Path

from clio_relay.jobs.content import ContentStore
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

    async def write(self, path: str, data: bytes, *, overwrite: bool = False) -> FileEntry:
        target = self.resolve(path)

        def _write() -> FileEntry:
            if target.exists() and not overwrite:
                raise Refusal(
                    RefusalReason.PATH_EXISTS,
                    f"{path} exists; pass overwrite to replace it",
                    path=path,
                )
            target.parent.mkdir(parents=True, exist_ok=True)
            tmp = target.with_name(f".{target.name}.relay-tmp")
            tmp.write_bytes(data)
            os.replace(tmp, target)
            return _entry(target, str(target))

        return await asyncio.to_thread(_write)

    async def stage(self, data: bytes) -> str:
        return await asyncio.to_thread(self.content.put, data)

    async def has(self, digest: str) -> bool:
        return await asyncio.to_thread(self.content.has, digest)

"""SiteFiles over an in-memory site that cannot append or remove a tree."""

from __future__ import annotations

import posixpath
from pathlib import Path
from typing import ClassVar

import pytest

from clio_relay.adapters.site_files import Kind, SiteFiles
from clio_relay.jobs.handles import FileEntry
from clio_relay.jobs.refusals import Refusal, RefusalReason

pytestmark = pytest.mark.asyncio


class MemoryFiles(SiteFiles):
    max_upload_bytes: ClassVar[int | None] = 10
    upload_hint: ClassVar[str] = "Use the transfer service."

    def __init__(self, staging: Path) -> None:
        super().__init__(root="/work", home="/home/ada", staging=staging)
        self.files: dict[str, bytes] = {}
        self.dirs: set[str] = {"/", "/work", "/home", "/home/ada"}
        self.puts: list[str] = []

    async def kind(self, path: str) -> Kind:
        return "dir" if path in self.dirs else "file" if path in self.files else "missing"

    async def describe(self, path: str) -> FileEntry:
        if path in self.dirs:
            return FileEntry(path=path, kind="dir")
        return FileEntry(path=path, kind="file", size=len(self.files[path]))

    async def children(self, path: str) -> list[FileEntry]:
        names = [p for p in [*self.files, *self.dirs] if p != "/" and posixpath.dirname(p) == path]
        return [await self.describe(name) for name in sorted(names)]

    async def get(self, path: str, offset: int, limit: int | None) -> bytes:
        data = self.files[path][offset:]
        return data if limit is None else data[:limit]

    async def put(self, path: str, data: bytes) -> None:
        parent = posixpath.dirname(path)
        while parent not in self.dirs:
            self.dirs.add(parent)
            parent = posixpath.dirname(parent)
        self.files[path] = data
        self.puts.append(path)

    async def remove(self, path: str) -> None:
        if path in self.dirs:
            assert not await self.children(path), "the site cannot remove a tree"
            self.dirs.remove(path)
        else:
            del self.files[path]


async def test_chunks_stay_local_until_the_last(tmp_path: Path) -> None:
    files = MemoryFiles(tmp_path)
    await files.write("big", b"abcd", final=False)
    part = await files.stat("big.relay-part")
    assert part.size == 4
    assert files.puts == []
    with pytest.raises(Refusal) as wrong:
        await files.write("big", b"x", offset=2, final=False)
    assert wrong.value.data["resume_at"] == 4
    await files.write("big", b"efg", offset=4)
    assert files.files["/work/big"] == b"abcdefg"
    assert files.puts == ["/work/big"]
    assert list(tmp_path.iterdir()) == []


async def test_an_upload_over_the_cap_refuses_with_the_hint(tmp_path: Path) -> None:
    files = MemoryFiles(tmp_path)
    await files.write("big", b"12345678", final=False)
    with pytest.raises(Refusal) as refused:
        await files.write("big", b"901", offset=8)
    assert refused.value.reason == RefusalReason.INVALID_ARGUMENTS
    assert "transfer service" in refused.value.message
    assert files.puts == []


async def test_a_recursive_delete_walks_the_tree(tmp_path: Path) -> None:
    files = MemoryFiles(tmp_path)
    await files.write("tree/a/one", b"1")
    await files.write("tree/two", b"2")
    with pytest.raises(Refusal) as refused:
        await files.delete("tree")
    assert refused.value.reason == RefusalReason.INVALID_ARGUMENTS
    await files.delete("tree", recursive=True)
    assert await files.kind("/work/tree") == "missing"
    assert files.files == {}


async def test_a_refused_last_chunk_can_be_resent_with_overwrite(tmp_path: Path) -> None:
    files = MemoryFiles(tmp_path)
    await files.write("big", b"abcd", final=False)
    files.files["/work/big"] = b"old"
    with pytest.raises(Refusal) as refused:
        await files.write("big", b"ef", offset=4)
    assert refused.value.reason == RefusalReason.PATH_EXISTS
    await files.write("big", b"ef", offset=4, overwrite=True)
    assert files.files["/work/big"] == b"abcdef"


class NoRootStat(MemoryFiles):
    async def kind(self, path: str) -> Kind:
        if path == "/":
            raise Refusal(RefusalReason.UNREACHABLE, "403 on stat /")
        return await super().kind(path)


@pytest.mark.parametrize("path", ["/", "/work/", "~", "/home/ada"])
async def test_protected_paths_refuse_before_the_site_is_asked(tmp_path: Path, path: str) -> None:
    with pytest.raises(Refusal) as refused:
        await NoRootStat(tmp_path).delete(path, recursive=True)
    assert refused.value.reason == RefusalReason.INVALID_ARGUMENTS
    assert "relay does not delete" in refused.value.message

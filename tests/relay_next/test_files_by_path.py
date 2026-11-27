"""files upload and download: a file moves by its path, never through the agent."""

from __future__ import annotations

import asyncio
import hashlib
import os
from pathlib import Path
from typing import Any

import pytest
from fastmcp import Client

from clio_relay.adapters._files import LocalFileAccess
from clio_relay.jobs.content import ContentStore
from clio_relay.jobs.file_rules import CHUNK_BYTES
from clio_relay.jobs.handles import FileEntry
from clio_relay.jobs.refusals import Refusal, RefusalReason
from clio_relay.surface.files_transfer import download, upload
from clio_relay.surface.server import create_server
from clio_relay.surface.service import RelayService
from clio_relay.surface.settings import RelayPaths

from .conftest import add_site


def _structured(result: Any) -> dict[str, Any]:
    structured = result.structured_content
    assert isinstance(structured, dict), result
    return structured  # pyright: ignore[reportUnknownVariableType]


def _call(op: str, **args: object) -> dict[str, object]:
    return {"site": "fx", "op": op, **args}


def test_a_large_file_goes_up_and_down_by_path(relay_paths: RelayPaths, tmp_path: Path) -> None:
    """Over two chunks each way, checked on the site, with only metadata in the result."""
    add_site(relay_paths)
    data = os.urandom(2 * CHUNK_BYTES + 123)
    source = tmp_path / "local" / "part.stl"
    source.parent.mkdir()
    source.write_bytes(data)
    digest = hashlib.sha256(data).hexdigest()
    back = tmp_path / "local" / "out" / "back.stl"

    async def scenario() -> None:
        async with Client(create_server(relay_paths)) as client:
            sent = await client.call_tool(
                "files", _call("upload", local_path=str(source), path="in/part.stl")
            )
            moved = _structured(sent)
            assert moved["size"] == len(data)
            assert moved["sha256"] == digest
            assert moved["verified"] == "site"
            assert "content_base64" not in moved and "text" not in moved
            assert sum(len(getattr(item, "text", "")) for item in sent.content) < 400
            stat = await client.call_tool("files", _call("stat", path="in/part.stl"))
            assert _structured(stat)["size"] == len(data)
            again = await client.call_tool(
                "files",
                _call("upload", local_path=str(source), path="in/part.stl"),
                raise_on_error=False,
            )
            assert _structured(again)["refusal"]["reason"] == "path_exists"

            got = await client.call_tool(
                "files", _call("download", path="in/part.stl", local_path=str(back))
            )
            assert _structured(got)["sha256"] == digest
            assert back.read_bytes() == data
            assert not list(back.parent.glob("*.relay-part"))
            kept = await client.call_tool(
                "files",
                _call("download", path="in/part.stl", local_path=str(back)),
                raise_on_error=False,
            )
            assert _structured(kept)["refusal"]["reason"] == "path_exists"
            back.write_bytes(b"stale")
            await client.call_tool(
                "files",
                _call("download", path="in/part.stl", local_path=str(back), overwrite=True),
            )
            assert back.read_bytes() == data

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("op", "local", "reason"),
    [
        ("upload", "relative/part.stl", "invalid_arguments"),
        ("upload", None, "invalid_arguments"),
        ("upload", "MISSING", "path_not_found"),
        ("download", "relative/out.stl", "invalid_arguments"),
    ],
)
def test_the_local_path_is_a_real_absolute_path(
    relay_paths: RelayPaths, tmp_path: Path, op: str, local: str | None, reason: str
) -> None:
    add_site(relay_paths)
    if local == "MISSING":
        local = str(tmp_path / "nothing.stl")

    async def scenario() -> None:
        async with Client(create_server(relay_paths)) as client:
            await client.call_tool("files", _call("write", path="there.stl", content="x"))
            path = "there.stl" if op == "download" else "new.stl"
            args = {"path": path} | ({} if local is None else {"local_path": local})
            refused = await client.call_tool("files", _call(op, **args), raise_on_error=False)
            assert _structured(refused)["refusal"]["reason"] == reason

    asyncio.run(scenario())


def test_a_replacing_upload_on_a_guarded_site_asks_first(
    relay_paths: RelayPaths, tmp_path: Path
) -> None:
    add_site(relay_paths, policy="guarded")
    service = RelayService(relay_paths)
    source = tmp_path / "new.txt"
    source.write_bytes(b"new bytes")

    async def scenario() -> None:
        async with Client(create_server(relay_paths), mode="legacy") as client:
            fresh = await client.call_tool(
                "files", _call("upload", local_path=str(source), path="a.txt")
            )
            assert _structured(fresh)["size"] == 9
            replace = _call("upload", local_path=str(source), path="a.txt", overwrite=True)
            asked = await client.call_tool("files", replace, raise_on_error=False)
            refusal = _structured(asked)["refusal"]
            assert refusal["reason"] == "needs_approval"
            service.answer(refusal["data"]["ask_id"], "accept", {"approve": True}, by="test")
            done = await client.call_tool("files", replace)
            assert _structured(done)["sha256"] == hashlib.sha256(b"new bytes").hexdigest()

    asyncio.run(scenario())
    assert [ask.completed_at is not None for ask in service.asks.all()] == [True]


class _NoSum:
    """File access that cannot hash where the file is, with small chunks."""

    transfer_chunk_bytes = 5

    def __init__(self, inner: LocalFileAccess, *, corrupt: bool = False) -> None:
        self.inner = inner
        self.corrupt = corrupt
        self.writes: list[int] = []

    async def stat(self, path: str) -> FileEntry:
        return await self.inner.stat(path)

    async def read(self, path: str, *, offset: int = 0, limit: int | None = None) -> bytes:
        return await self.inner.read(path, offset=offset, limit=limit)

    async def write(
        self,
        path: str,
        data: bytes,
        *,
        overwrite: bool = False,
        offset: int = 0,
        final: bool = True,
    ) -> FileEntry:
        self.writes.append(len(data))
        if self.corrupt and final:
            data = data[:-1] + b"?"
        return await self.inner.write(path, data, overwrite=overwrite, offset=offset, final=final)


def test_without_a_site_hash_relay_reads_the_file_back(tmp_path: Path) -> None:
    store = ContentStore(tmp_path / "cas")
    access = _NoSum(LocalFileAccess(tmp_path / "site", store))
    source = tmp_path / "src.bin"
    source.write_bytes(b"0123456789abc")

    async def scenario() -> None:
        moved = await upload(access, str(source), "x.bin", overwrite=False)  # pyright: ignore[reportArgumentType]
        assert moved["verified"] == "read_back"
        assert access.writes == [5, 5, 3]
        target = tmp_path / "back.bin"
        got = await download(access, "x.bin", str(target), overwrite=False)  # pyright: ignore[reportArgumentType]
        assert got["verified"] == "size"
        assert target.read_bytes() == b"0123456789abc"
        exact = tmp_path / "exact.bin"
        exact.write_bytes(b"0123456789")
        whole = await upload(access, str(exact), "y.bin", overwrite=False)  # pyright: ignore[reportArgumentType]
        assert whole["size"] == 10

    asyncio.run(scenario())


def test_a_copy_that_differs_on_the_site_is_refused(tmp_path: Path) -> None:
    store = ContentStore(tmp_path / "cas")
    access = _NoSum(LocalFileAccess(tmp_path / "site", store), corrupt=True)
    source = tmp_path / "src.bin"
    source.write_bytes(b"0123456789abc")

    async def scenario() -> None:
        with pytest.raises(Refusal) as refused:
            await upload(access, str(source), "x.bin", overwrite=False)  # pyright: ignore[reportArgumentType]
        assert refused.value.reason == RefusalReason.UNREACHABLE

    asyncio.run(scenario())

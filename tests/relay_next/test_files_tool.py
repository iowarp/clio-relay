"""The files tool through a stock MCP client (docs/files.md)."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from fastmcp import Client

from clio_relay.surface.server import create_server
from clio_relay.surface.settings import RelayPaths

from .conftest import add_site

pytestmark = pytest.mark.release_platform("posix")


def _structured(result: Any) -> dict[str, Any]:
    structured = result.structured_content
    assert isinstance(structured, dict), result
    return structured  # pyright: ignore[reportUnknownVariableType]

def test_exec_and_files(relay_paths: RelayPaths) -> None:
    add_site(relay_paths)

    async def scenario() -> None:
        async with Client(create_server(relay_paths), mode="legacy") as client:
            hostname = {"site": "fx", "command": ["hostname"]}
            ran = _structured(await client.call_tool("exec", hostname))
            assert ran["exit_code"] == 0
            wrote = await client.call_tool(
                "files", {"site": "fx", "op": "write", "path": "a/b.txt", "content": "hi"}
            )
            assert _structured(wrote)["size"] == 2
            read = await client.call_tool("files", {"site": "fx", "op": "read", "path": "a/b.txt"})
            assert _structured(read)["text"] == "hi"
            listed = await client.call_tool("files", {"site": "fx", "op": "list", "path": "a"})
            assert [e["path"] for e in _structured(listed)["entries"]][0].endswith("b.txt")
            staged = _structured(
                await client.call_tool("files", {"site": "fx", "op": "stage", "content": "x"})
            )
            has = await client.call_tool(
                "files", {"site": "fx", "op": "has", "digest": staged["sha256"]}
            )
            assert _structured(has)["present"] is True
            again = await client.call_tool(
                "files",
                {"site": "fx", "op": "write", "path": "a/b.txt", "content": "x"},
                raise_on_error=False,
            )
            assert _structured(again)["refusal"]["reason"] == "path_exists"

    asyncio.run(scenario())


def test_files_chunks_and_delete(relay_paths: RelayPaths) -> None:
    """docs/files.md through the tool: a chunked upload resumes, then delete."""
    add_site(relay_paths)

    def call(op: str, **args: object) -> dict[str, object]:
        return {"site": "fx", "op": op, **args}

    async def scenario() -> None:
        async with Client(create_server(relay_paths), mode="legacy") as client:
            first = _structured(
                await client.call_tool(
                    "files", call("write", path="up/big.txt", content="hello ", final=False)
                )
            )
            assert first["resume_at"] == 6
            assert str(first["path"]).endswith("big.txt.relay-part")
            wrong = await client.call_tool(
                "files",
                call("write", path="up/big.txt", content="x", offset=2, final=False),
                raise_on_error=False,
            )
            refusal = _structured(wrong)["refusal"]
            assert refusal["reason"] == "invalid_arguments"
            assert refusal["data"]["resume_at"] == 6
            done = _structured(
                await client.call_tool(
                    "files", call("write", path="up/big.txt", content="world", offset=6)
                )
            )
            assert done["size"] == 11
            read = await client.call_tool("files", call("read", path="up/big.txt"))
            assert _structured(read)["text"] == "hello world"
            full = await client.call_tool("files", call("delete", path="up"), raise_on_error=False)
            assert _structured(full)["refusal"]["reason"] == "invalid_arguments"
            gone = await client.call_tool("files", call("delete", path="up", recursive=True))
            assert _structured(gone)["deleted"] is True
            missing = await client.call_tool("files", call("stat", path="up"), raise_on_error=False)
            assert _structured(missing)["refusal"]["reason"] == "path_not_found"
            escape = await client.call_tool(
                "files", call("read", path="../outside"), raise_on_error=False
            )
            assert _structured(escape)["refusal"]["reason"] == "invalid_arguments"

    asyncio.run(scenario())

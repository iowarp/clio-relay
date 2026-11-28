"""The read tools: annotations a host can honour per call, and reads that never ask."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from fastmcp import Client

from clio_relay.surface.a2ui import surfaces
from clio_relay.surface.server import create_server
from clio_relay.surface.service import RelayService
from clio_relay.surface.settings import RelayPaths

from .a2ui_host import basic_client
from .conftest import add_site

READS = {"describe", "exec_read", "files_read", "packages_read"}
PAGE_ONLY = {
    surfaces.OPEN_ADD,
    surfaces.EDIT_SITE,
    surfaces.CANCEL,
    surfaces.REMOVE_SITE,
    surfaces.OPEN_DIALOG,
    surfaces.PICK,
    surfaces.BACK,
    surfaces.CLOSE_DIALOG,
}
REPLACES = {
    surfaces.SAVE_SITE,
    surfaces.SWITCH_ADAPTER,
    surfaces.SET_VALUE,
    surfaces.CONFIRM_REMOVE,
    surfaces.UNDO,
}


def _structured(result: Any) -> dict[str, Any]:
    structured = result.structured_content
    assert isinstance(structured, dict), result
    return structured  # pyright: ignore[reportUnknownVariableType]


def _hints(tool: Any) -> dict[str, Any]:
    listed = tool.model_dump(by_alias=True, mode="json", exclude_none=True)
    return listed.get("annotations", {})


def test_every_tool_says_whether_it_only_reads(relay_paths: RelayPaths) -> None:
    async def scenario() -> None:
        async with Client(create_server(relay_paths), mode="legacy") as client:
            tools = {tool.name: _hints(tool) for tool in await client.list_tools()}
        assert set(tools) >= READS
        for name, hints in tools.items():
            assert hints["openWorldHint"] is False, name
            if name in READS:
                assert hints["readOnlyHint"] is True, name
                assert hints["destructiveHint"] is False, name
            else:
                assert hints.get("readOnlyHint") is not True, name

    asyncio.run(scenario())


def test_compute_page_actions_carry_their_own_hints(relay_paths: RelayPaths) -> None:
    async def scenario() -> None:
        async with basic_client(relay_paths) as client:
            tools = {tool.name: _hints(tool) for tool in await client.list_tools()}
        for name in surfaces.ACTIONS:
            hints = tools[name]
            assert hints["openWorldHint"] is False, name
            assert hints["readOnlyHint"] is (name in PAGE_ONLY), name
            assert hints["destructiveHint"] is (name in REPLACES), name

    asyncio.run(scenario())


@pytest.mark.parametrize("policy", ["open", "guarded"])
def test_exec_read_runs_reads_and_refuses_the_rest(relay_paths: RelayPaths, policy: str) -> None:
    add_site(relay_paths, policy=policy)

    async def scenario() -> None:
        async with Client(create_server(relay_paths), mode="legacy") as client:
            read = await client.call_tool("exec_read", {"site": "fx", "command": ["hostname"]})
            assert _structured(read)["stdout"] == "fixture-login-1\n"
            jarvis = await client.call_tool(
                "exec_read",
                {"site": "fx", "command": ["jarvis", "sched", "info", "+json"]},
                raise_on_error=False,
            )
            assert "refusal" not in _structured(jarvis), "a JARVIS read runs with no plan"
            for command in (["rm", "-rf", "/tmp/x"], ["jarvis", "rg", "facts", "answer", "i"]):
                refused = await client.call_tool(
                    "exec_read", {"site": "fx", "command": command}, raise_on_error=False
                )
                assert refused.is_error
                refusal = _structured(refused)["refusal"]
                assert refusal["reason"] == "policy_refused", command
                assert refusal["data"]["next_step"] == "exec"
                assert "exec_read runs only read-only commands" in refusal["message"]

    asyncio.run(scenario())
    assert RelayService(relay_paths).asks.all() == [], "a read never asks"


def test_files_read_and_describe_packages_only_read(relay_paths: RelayPaths) -> None:
    add_site(relay_paths, policy="guarded")

    async def scenario() -> None:
        async with Client(create_server(relay_paths), mode="legacy") as client:
            await client.call_tool(
                "files", {"site": "fx", "op": "write", "path": "a.txt", "content": "hi"}
            )
            listed = await client.call_tool("files_read", {"site": "fx", "op": "list", "path": "."})
            assert "a.txt" in str(_structured(listed))
            read = await client.call_tool(
                "files_read", {"site": "fx", "op": "read", "path": "a.txt"}
            )
            assert "hi" in str(_structured(read))
            wrong = await client.call_tool(
                "files_read",
                {"site": "fx", "op": "delete", "path": "a.txt"},
                raise_on_error=False,
            )
            assert wrong.is_error
            described = _structured(await client.call_tool("describe", {"site": "fx"}))
            # The fixture adapter has no JARVIS root to list, so describe shows the
            # last listing (none) with the reason, and packages_read refuses.
            assert described["packages"] == [] and "packages_note" in described
            catalog = await client.call_tool("packages_read", {"site": "fx"}, raise_on_error=False)
            assert catalog.is_error
            assert _structured(catalog)["refusal"]["reason"] == "verb_not_implemented"
            old = await client.call_tool(
                "packages", {"op": "list", "site": "fx", "path": "x"}, raise_on_error=False
            )
            assert old.is_error, "listing is packages_read"

    asyncio.run(scenario())
    assert RelayService(relay_paths).asks.all() == []

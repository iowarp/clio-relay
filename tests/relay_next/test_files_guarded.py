"""Guarded sites: files delete and replace wait for the person (docs/guarded-sites.md)."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from fastmcp import Client
from fastmcp.client.elicitation import ElicitResult
from fastmcp.client.transports import FastMCPTransport

from clio_relay.surface.server import create_server
from clio_relay.surface.service import RelayService
from clio_relay.surface.settings import RelayPaths

from .conftest import add_site

pytestmark = pytest.mark.release_platform("posix")


class NoTasksClient(Client[FastMCPTransport]):
    """A modern client that does not opt into the tasks extension."""

    _auto_internal_extensions = False


def _structured(result: Any) -> dict[str, Any]:
    structured = result.structured_content
    assert isinstance(structured, dict), result
    return structured  # pyright: ignore[reportUnknownVariableType]


def _call(op: str, **args: object) -> dict[str, object]:
    return {"site": "fx", "op": op, **args}


async def _approve(message: str, response_type: Any, params: Any, context: Any) -> Any:
    return {"approve": True}


async def _decline(message: str, response_type: Any, params: Any, context: Any) -> Any:
    return ElicitResult(action="decline", content=None)


def test_without_a_form_the_call_names_the_ask(relay_paths: RelayPaths) -> None:
    """No elicitation: needs_approval with the ask; after the yes, the same call
    goes through once, and the next delete asks again."""
    add_site(relay_paths, policy="guarded")
    service = RelayService(relay_paths)

    async def scenario() -> None:
        async with Client(create_server(relay_paths), mode="legacy") as client:
            for name in ("a.txt", "b.txt"):
                wrote = await client.call_tool("files", _call("write", path=name, content="1"))
                assert _structured(wrote)["size"] == 1
            replace = await client.call_tool(
                "files",
                _call("write", path="a.txt", content="22", overwrite=True),
                raise_on_error=False,
            )
            refusal = _structured(replace)["refusal"]
            assert refusal["reason"] == "needs_approval"
            ask_id = refusal["data"]["ask_id"]
            assert "Replace" in refusal["data"]["asks"][0]["message"]
            again = await client.call_tool(
                "files",
                _call("write", path="a.txt", content="22", overwrite=True),
                raise_on_error=False,
            )
            assert _structured(again)["refusal"]["data"]["ask_id"] == ask_id
            service.answer(ask_id, "accept", {"approve": True}, by="test")
            done = await client.call_tool(
                "files", _call("write", path="a.txt", content="22", overwrite=True)
            )
            assert _structured(done)["size"] == 2
            deleting = await client.call_tool(
                "files", _call("delete", path="a.txt"), raise_on_error=False
            )
            delete_ask = _structured(deleting)["refusal"]["data"]["ask_id"]
            assert delete_ask != ask_id
            service.answer(delete_ask, "accept", {"approve": True}, by="test")
            gone = await client.call_tool("files", _call("delete", path="a.txt"))
            assert _structured(gone)["deleted"] is True
            other = await client.call_tool(
                "files", _call("delete", path="b.txt"), raise_on_error=False
            )
            assert _structured(other)["refusal"]["reason"] == "needs_approval"

    asyncio.run(scenario())


def test_a_chunked_replace_asks_once(relay_paths: RelayPaths) -> None:
    add_site(relay_paths, policy="guarded")
    service = RelayService(relay_paths)

    async def scenario() -> None:
        async with Client(create_server(relay_paths), mode="legacy") as client:
            await client.call_tool("files", _call("write", path="big.txt", content="old"))
            first = _call("write", path="big.txt", content="hello ", overwrite=True, final=False)
            asked = await client.call_tool("files", first, raise_on_error=False)
            ask_id = _structured(asked)["refusal"]["data"]["ask_id"]
            service.answer(ask_id, "accept", {"approve": True}, by="test")
            assert _structured(await client.call_tool("files", first))["resume_at"] == 6
            last = _call("write", path="big.txt", content="world", overwrite=True, offset=6)
            assert _structured(await client.call_tool("files", last))["size"] == 11
            read = await client.call_tool("files", _call("read", path="big.txt"))
            assert _structured(read)["text"] == "hello world"

    asyncio.run(scenario())
    asks = RelayService(relay_paths).asks.all()
    assert len(asks) == 1 and asks[0].completed_at is not None


@pytest.mark.parametrize("mode", ["legacy", "modern"])
def test_a_form_approves_in_the_call(relay_paths: RelayPaths, mode: str) -> None:
    add_site(relay_paths, policy="guarded")

    async def scenario() -> None:
        server = create_server(relay_paths)
        client: Client[Any] = (
            Client(server, mode="legacy", elicitation_handler=_approve)
            if mode == "legacy"
            else NoTasksClient(server, elicitation_handler=_approve)
        )
        async with client:
            await client.call_tool("files", _call("write", path="d/x.txt", content="x"))
            gone = await client.call_tool("files", _call("delete", path="d", recursive=True))
            assert _structured(gone)["deleted"] is True

    asyncio.run(scenario())
    asks = RelayService(relay_paths).asks.all()
    expected = "elicitation" if mode == "legacy" else "input_required"
    assert [(ask.answered_by, ask.binding["recursive"]) for ask in asks] == [(expected, True)]


@pytest.mark.parametrize("mode", ["legacy", "modern"])
def test_a_no_keeps_the_file(relay_paths: RelayPaths, mode: str) -> None:
    add_site(relay_paths, policy="guarded")

    async def scenario() -> None:
        server = create_server(relay_paths)
        client: Client[Any] = (
            Client(server, mode="legacy", elicitation_handler=_decline)
            if mode == "legacy"
            else NoTasksClient(server, elicitation_handler=_decline)
        )
        async with client:
            await client.call_tool("files", _call("write", path="keep.txt", content="x"))
            refused = await client.call_tool(
                "files", _call("delete", path="keep.txt"), raise_on_error=False
            )
            assert not refused.is_error
            declined = _structured(refused)["declined"]
            assert declined["not_done"] == "Relay left keep.txt as it was."
            assert declined["question"].startswith("Delete keep.txt on ")
            kept = await client.call_tool("files", _call("read", path="keep.txt"))
            assert _structured(kept)["text"] == "x"

    asyncio.run(scenario())


def test_an_open_site_never_asks(relay_paths: RelayPaths) -> None:
    add_site(relay_paths, policy="open")

    async def scenario() -> None:
        async with Client(create_server(relay_paths), mode="legacy") as client:
            await client.call_tool("files", _call("write", path="o.txt", content="x"))
            await client.call_tool(
                "files", _call("write", path="o.txt", content="y", overwrite=True)
            )
            gone = await client.call_tool("files", _call("delete", path="o.txt"))
            assert _structured(gone)["deleted"] is True

    asyncio.run(scenario())
    assert RelayService(relay_paths).asks.all() == []

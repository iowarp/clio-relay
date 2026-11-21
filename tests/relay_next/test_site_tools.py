"""cancel and site_clean: tools of their own, so a host's question names what they do."""

from __future__ import annotations

import asyncio
from collections.abc import Iterator
from typing import Any, cast

import pytest
from fastmcp import Client
from fastmcp.client.elicitation import ElicitResult
from fastmcp.client.transports import FastMCPTransport

from clio_relay.adapters.base import CleansSite
from clio_relay.surface.server import create_server
from clio_relay.surface.service import RelayService
from clio_relay.surface.settings import RelayPaths
from clio_relay.testing.fixture_adapter import FixtureAdapter

from .conftest import add_site

SPACK = ["~/spack", "~/spack-config"]


class NoTasksClient(Client[FastMCPTransport]):
    """A modern client that does not opt into the tasks extension."""

    _auto_internal_extensions = False


def _structured(result: Any) -> dict[str, Any]:
    structured = result.structured_content
    assert isinstance(structured, dict), result
    return structured  # pyright: ignore[reportUnknownVariableType]


async def _approve(message: str, response_type: Any, params: Any, context: Any) -> Any:
    assert "keeping ~/spack, ~/spack-config" in message
    return {"approve": True}


async def _decline(message: str, response_type: Any, params: Any, context: Any) -> Any:
    return ElicitResult(action="decline", content=None)


@pytest.fixture
def cleaned(monkeypatch: pytest.MonkeyPatch) -> Iterator[list[tuple[list[str], list[str]]]]:
    """Give the fixture adapter a clean that records what it was told to remove and keep."""
    calls: list[tuple[list[str], list[str]]] = []

    def clean_targets(self: FixtureAdapter) -> tuple[list[str], list[str]]:
        return ["~/relay-worker"], list(SPACK)

    async def clean(self: FixtureAdapter, targets: list[str], keep: list[str]) -> dict[str, Any]:
        calls.append((targets, keep))
        return {"removed": targets, "kept": keep}

    monkeypatch.setattr(FixtureAdapter, "clean_targets", clean_targets, raising=False)
    monkeypatch.setattr(FixtureAdapter, "clean", clean, raising=False)
    yield calls
    monkeypatch.undo()
    # ABCMeta caches the positive isinstance, which would outlive the patch.
    cast(Any, CleansSite)._abc_caches_clear()


def test_cancel_and_site_clean_are_listed_as_changes(relay_paths: RelayPaths) -> None:
    async def scenario() -> None:
        async with Client(create_server(relay_paths), mode="legacy") as client:
            tools = {tool.name: tool for tool in await client.list_tools()}
        for name, title in (
            ("cancel", "Cancel a job or stop a service"),
            ("site_clean", "Remove relay's deployment from a site"),
        ):
            listed = tools[name].model_dump(by_alias=True, mode="json", exclude_none=True)
            assert listed["title"] == title
            hints = listed["annotations"]
            assert hints["readOnlyHint"] is False, name
            assert hints["destructiveHint"] is True, name
            assert hints["openWorldHint"] is False, name

    asyncio.run(scenario())


def test_cancel_ends_a_job(relay_paths: RelayPaths) -> None:
    add_site(relay_paths, config={"queue_s": 30, "run_s": 30})

    async def scenario() -> None:
        async with Client(create_server(relay_paths), mode="legacy") as client:
            spec = {"executable": "sleep", "arguments": ["300"]}
            started = await client.call_tool("run", {"site": "fx", "spec": spec, "wait_s": 0})
            view = _structured(started)
            assert view["state"] in ("queued", "running"), view
            ended = await client.call_tool("cancel", {"operation": view["operation_id"]})
            assert _structured(ended)["state"] == "cancelled"
            unknown = await client.call_tool(
                "cancel", {"operation": "op-none"}, raise_on_error=False
            )
            assert unknown.is_error

    asyncio.run(scenario())


@pytest.mark.parametrize("policy", ["open", "guarded"])
def test_site_clean_asks_then_keeps_the_spack_store(
    relay_paths: RelayPaths, cleaned: list[tuple[list[str], list[str]]], policy: str
) -> None:
    add_site(relay_paths, policy=policy)
    service = RelayService(relay_paths)

    async def scenario() -> None:
        async with Client(create_server(relay_paths), mode="legacy") as client:
            asked = await client.call_tool("site_clean", {"site": "fx"}, raise_on_error=False)
            refusal = _structured(asked)["refusal"]
            assert refusal["reason"] == "needs_approval"
            ask_id = refusal["data"]["ask_id"]
            assert "Clean fx" in refusal["data"]["asks"][0]["message"]
            again = await client.call_tool("site_clean", {"site": "fx"}, raise_on_error=False)
            assert _structured(again)["refusal"]["data"]["ask_id"] == ask_id
            assert cleaned == []
            service.answer(ask_id, "accept", {"approve": True}, by="test")
            done = _structured(await client.call_tool("site_clean", {"site": "fx"}))
            assert done["schema"] == "relay.site_clean.v1"
            assert done["kept"] == SPACK
            next_time = await client.call_tool("site_clean", {"site": "fx"}, raise_on_error=False)
            assert _structured(next_time)["refusal"]["data"]["ask_id"] != ask_id

    asyncio.run(scenario())
    assert len(cleaned) == 1
    targets, keep = cleaned[0]
    assert "~/relay-worker" in targets and keep == SPACK


@pytest.mark.parametrize("mode", ["legacy", "modern"])
def test_site_clean_takes_the_yes_in_the_call(
    relay_paths: RelayPaths, cleaned: list[tuple[list[str], list[str]]], mode: str
) -> None:
    add_site(relay_paths)

    async def scenario() -> None:
        server = create_server(relay_paths)
        client: Client[Any] = (
            Client(server, mode="legacy", elicitation_handler=_approve)
            if mode == "legacy"
            else NoTasksClient(server, elicitation_handler=_approve)
        )
        async with client:
            done = _structured(await client.call_tool("site_clean", {"site": "fx"}))
            assert done["site"] == "fx"

    asyncio.run(scenario())
    assert len(cleaned) == 1
    expected = "elicitation" if mode == "legacy" else "input_required"
    assert [ask.answered_by for ask in RelayService(relay_paths).asks.all()] == [expected]


def test_site_clean_does_nothing_on_a_no(
    relay_paths: RelayPaths, cleaned: list[tuple[list[str], list[str]]]
) -> None:
    add_site(relay_paths)

    async def scenario() -> None:
        server = create_server(relay_paths)
        async with Client(server, mode="legacy", elicitation_handler=_decline) as client:
            refused = await client.call_tool("site_clean", {"site": "fx"}, raise_on_error=False)
            assert _structured(refused)["refusal"]["reason"] == "policy_refused"

    asyncio.run(scenario())
    assert cleaned == []


def test_site_clean_refuses_a_site_no_adapter_can_clean(relay_paths: RelayPaths) -> None:
    add_site(relay_paths)

    async def scenario() -> None:
        async with Client(create_server(relay_paths), mode="legacy") as client:
            refused = await client.call_tool("site_clean", {"site": "fx"}, raise_on_error=False)
            assert _structured(refused)["refusal"]["reason"] == "verb_not_implemented"

    asyncio.run(scenario())
    assert RelayService(relay_paths).asks.all() == []

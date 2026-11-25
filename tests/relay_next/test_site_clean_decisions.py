"""site_clean keeps JARVIS's recorded install answers unless told to forget them.

The answers (which MPI, which compiler) are facts about the cluster the person
confirmed. Clean keeps them next to the Spack store by default; ``forget_decisions``
(``--forget-decisions`` on the CLI) deletes them, so the next install asks again.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Iterator
from typing import Any, cast

import pytest
from fastmcp import Client
from typer.testing import CliRunner

from clio_relay import cli, site_clean
from clio_relay.adapters.base import CleansSite
from clio_relay.site_prune import is_decisions, kept, site_keeps
from clio_relay.surface.server import create_server
from clio_relay.surface.service import RelayService
from clio_relay.surface.settings import RelayPaths
from clio_relay.testing.fixture_adapter import FixtureAdapter

from .conftest import add_site

HOME = "/home/me/.relay"
SPACK = [f"{HOME}/spack", f"{HOME}/spack-config", f"{HOME}/jarvis-cd/site/miniforge3"]
DECISIONS = f"{HOME}/jarvis-cd/site/decisions.yaml"


@pytest.fixture
def cleaned(monkeypatch: pytest.MonkeyPatch) -> Iterator[list[tuple[list[str], list[str]]]]:
    """Give the fixture adapter a site home whose clean records what it removes and keeps."""
    calls: list[tuple[list[str], list[str]]] = []

    def clean_targets(self: FixtureAdapter) -> tuple[list[str], list[str]]:
        return [HOME], site_keeps(HOME)

    async def clean(self: FixtureAdapter, targets: list[str], keep: list[str]) -> dict[str, Any]:
        calls.append((targets, keep))
        return {"removed": targets, "kept": keep}

    monkeypatch.setattr(FixtureAdapter, "clean_targets", clean_targets, raising=False)
    monkeypatch.setattr(FixtureAdapter, "clean", clean, raising=False)
    yield calls
    monkeypatch.undo()
    # ABCMeta caches the positive isinstance, which would outlive the patch.
    cast(Any, CleansSite)._abc_caches_clear()


def test_a_site_home_keeps_spack_and_the_answers() -> None:
    assert site_keeps(HOME + "/") == [*SPACK, DECISIONS]
    assert is_decisions(DECISIONS) and not any(is_decisions(path) for path in SPACK)
    assert kept([*SPACK, DECISIONS, DECISIONS]) == [*SPACK, DECISIONS]
    assert kept([*SPACK, DECISIONS], decisions=False) == SPACK
    assert kept([*SPACK, DECISIONS], spack=False) == [DECISIONS]
    assert kept([*SPACK, DECISIONS], spack=False, decisions=False) == []


@pytest.mark.parametrize(
    ("options", "keep"),
    [
        ({}, [*SPACK, DECISIONS]),
        ({"forget_decisions": True}, SPACK),
        ({"remove_spack": True}, [DECISIONS]),
        ({"remove_spack": True, "forget_decisions": True}, []),
    ],
)
def test_the_plan_keeps_what_was_not_asked_to_go(
    relay_paths: RelayPaths,
    cleaned: list[tuple[list[str], list[str]]],
    options: dict[str, bool],
    keep: list[str],
) -> None:
    add_site(relay_paths)

    async def scenario() -> dict[str, Any]:
        service = RelayService(relay_paths)
        try:
            return await site_clean.plan(service, "fx", **options)
        finally:
            await service.close()

    planned = asyncio.run(scenario())
    assert planned["keep"] == keep
    text = site_clean.describe_plan(planned)
    forgets = "next install asks them again" in text
    assert forgets == bool(keep and DECISIONS not in keep)
    if not keep:
        assert "the Spack store and JARVIS's answers go too" in text


def test_the_tool_forgets_the_answers_only_when_asked(
    relay_paths: RelayPaths, cleaned: list[tuple[list[str], list[str]]]
) -> None:
    add_site(relay_paths)
    service = RelayService(relay_paths)

    async def scenario() -> None:
        async with Client(create_server(relay_paths), mode="legacy") as client:
            tools = {tool.name: tool for tool in await client.list_tools()}
            option = tools["site_clean"].input_schema["properties"]["forget_decisions"]
            assert option["default"] is False and "asks them again" in option["description"]
            for forget in (False, True):
                arguments = {"site": "fx", "forget_decisions": forget}
                asked = await client.call_tool("site_clean", arguments, raise_on_error=False)
                refusal = cast(dict[str, Any], asked.structured_content)["refusal"]
                message = refusal["data"]["asks"][0]["detail"]
                assert ("next install asks them again" in message) is forget
                service.answer(refusal["data"]["ask_id"], "accept", {"approve": True}, by="test")
                await client.call_tool("site_clean", arguments)

    asyncio.run(scenario())
    assert [keep for _, keep in cleaned] == [[*SPACK, DECISIONS], SPACK]


def test_the_cli_forgets_the_answers_only_when_asked(
    relay_paths: RelayPaths,
    cleaned: list[tuple[list[str], list[str]]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    add_site(relay_paths)
    monkeypatch.setenv("CLIO_RELAY_CONFIG_DIR", str(relay_paths.config_dir))
    monkeypatch.setenv("CLIO_RELAY_STATE_DIR", str(relay_paths.state_dir))
    runner = CliRunner()
    for argv, keep in (
        (["site", "clean", "fx", "--yes"], [*SPACK, DECISIONS]),
        (["site", "clean", "fx", "--yes", "--forget-decisions"], SPACK),
    ):
        result = runner.invoke(cli.app, argv)
        assert result.exit_code == 0, result.output
        assert json.loads(result.stdout)["kept"] == keep

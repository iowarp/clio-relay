"""The server's instructions name the configured sites and the packages they hold."""

from __future__ import annotations

import asyncio
import json
import os
import sys
from types import SimpleNamespace
from typing import Any, cast

import pytest
from mcp import Client
from mcp.client.stdio import StdioServerParameters

from clio_relay.packages import PackageEntry
from clio_relay.sites.models import SiteEntry
from clio_relay.surface import instructions
from clio_relay.surface.service import RelayService
from clio_relay.surface.settings import RelayPaths

from .conftest import RELAY_ENTRY, add_site

SUMMARY = (
    "Xcompact3d (Incompact3d v5.0) incompressible flow simulation. Give it an STL file "
    "and it simulates flow past that body."
)


def _package(name: str = "clio_jarvis.xcompact3d", summary: str | None = SUMMARY) -> PackageEntry:
    return PackageEntry(name=name, version="0.2.1", digest="ab" * 32, summary=summary)


def _listed(paths: RelayPaths, site: str) -> None:
    """Record that the site's JARVIS root lists the package."""
    paths.packages_dir.mkdir(parents=True, exist_ok=True)
    found = [_package().model_dump(mode="json")]
    (paths.packages_dir / f"{site}.json").write_text(json.dumps(found), encoding="utf-8")


def _site(site_id: str, **fields: Any) -> SiteEntry:
    return SiteEntry.model_validate({"id": site_id, "adapter": "ssh", **fields})


def test_the_text_names_each_site_and_package() -> None:
    text = instructions.compose(
        [
            _site("ares", name="Ares cluster", config={"scheduler": "slurm"}),
            _site("lab", name="LAB", policy="open"),
        ],
        [_package(), _package("lab_kit.echo", summary=None)],
    )
    assert text.startswith(f"{instructions.LEAD} {instructions.GENERAL}")
    assert "only through relay's tools (describe, run, exec, files, serve" in text
    assert f"{instructions.SITES_LEAD}: ares (Ares cluster; ssh); lab (ssh)." in text
    assert (
        "Packages their JARVIS roots hold: clio_jarvis.xcompact3d 0.2.1 "
        "(Xcompact3d (Incompact3d v5.0) incompressible flow simulation); lab_kit.echo 0.2.1."
        in text
    )
    assert "use relay's describe and run tools" in text


def test_long_lists_and_summaries_are_cut() -> None:
    sites = [_site(f"s{index:02d}") for index in range(instructions.MAX_LISTED + 3)]
    text = instructions.compose(sites, [_package(summary="word " * 60)])
    assert "s11 (ssh); and 3 more (describe lists them all)." in text
    assert "s12" not in text
    described = text.split("clio_jarvis.xcompact3d 0.2.1 (", 1)[1].split(")", 1)[0]
    assert len(described) == instructions.SUMMARY_CHARS and described.endswith("...")


def test_no_sites_says_so() -> None:
    text = instructions.compose([], [_package()])
    assert text == f"{instructions.LEAD} {instructions.GENERAL} {instructions.NO_SITES}"
    assert "Compute page" in text


@pytest.mark.parametrize("sites", [[], [_site("ares")]])
def test_the_text_sends_the_agent_to_no_transport_of_its_own(sites: list[SiteEntry]) -> None:
    """A transport is how relay reaches a site: nothing points the agent at its files."""
    text = instructions.compose(sites, []).lower()
    assert "do not check its connection, host or credentials yourself" in text
    assert "describe(site=...) reaches the site" in text
    for pointer in ("ssh config", "ssh_config", "~/.ssh", "known_hosts", "ssh -g"):
        assert pointer not in text


def test_a_broken_sites_file_still_gives_instructions(relay_paths: RelayPaths) -> None:
    relay_paths.sites_file.parent.mkdir(parents=True, exist_ok=True)
    relay_paths.sites_file.write_text("- {id: [\n", encoding="utf-8")
    service = RelayService(relay_paths)
    assert instructions.current(service).endswith(instructions.NO_SITES)


@pytest.mark.asyncio
async def test_the_text_follows_the_sites_file(relay_paths: RelayPaths) -> None:
    service = RelayService(relay_paths)
    server = SimpleNamespace(instructions=instructions.current(service))
    refreshing = asyncio.create_task(
        instructions.keep_current(cast(Any, server), service, interval_s=0.01)
    )
    try:
        add_site(relay_paths, "later")
        _listed(relay_paths, "later")
        for _ in range(500):
            if "clio_jarvis.xcompact3d" in server.instructions:
                break
            await asyncio.sleep(0.01)
        assert "later (fixture)" in server.instructions
        assert "clio_jarvis.xcompact3d 0.2.1" in server.instructions
    finally:
        refreshing.cancel()
        await asyncio.gather(refreshing, return_exceptions=True)
        await service.close()


@pytest.mark.release_platform("posix")
@pytest.mark.parametrize("mode", ["legacy", "auto"])
def test_a_plain_client_reads_them_when_it_connects(relay_paths: RelayPaths, mode: str) -> None:
    add_site(relay_paths, "ares", adapter="fixture", policy="guarded")
    _listed(relay_paths, "ares")
    env = dict(os.environ)
    env["CLIO_RELAY_CONFIG_DIR"] = str(relay_paths.config_dir)
    env["CLIO_RELAY_STATE_DIR"] = str(relay_paths.state_dir)
    server = StdioServerParameters(
        command=sys.executable,
        args=["-c", RELAY_ENTRY, "serve"],
        env=env,
    )

    async def scenario() -> str | None:
        async with Client(server, mode=mode) as client:
            return client.instructions

    text = asyncio.run(scenario())
    assert text is not None
    assert f"{instructions.SITES_LEAD}: ares (fixture)." in text
    assert "clio_jarvis.xcompact3d 0.2.1 (Xcompact3d (Incompact3d v5.0)" in text

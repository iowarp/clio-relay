"""The server's instructions name the configured sites and the staged packages."""

from __future__ import annotations

import asyncio
import os
import sys
from types import SimpleNamespace
from typing import Any, cast

import pytest
from mcp import Client
from mcp.client.stdio import StdioServerParameters

from clio_relay.packages import PackageCatalog, PackageEntry
from clio_relay.sites.models import SiteEntry
from clio_relay.surface import instructions
from clio_relay.surface.service import RelayService
from clio_relay.surface.settings import RelayPaths

from .conftest import add_site

SUMMARY = (
    "Xcompact3d (Incompact3d v5.0) incompressible flow simulation. Give it an STL file "
    "and it simulates flow past that body."
)


def _package(name: str = "clio_jarvis.xcompact3d", summary: str | None = SUMMARY) -> PackageEntry:
    return PackageEntry(name=name, version="0.2.1", sha256="ab" * 32, summary=summary)


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
    assert text.startswith(instructions.GENERAL)
    assert (
        "Compute sites configured for relay: ares (Ares cluster; ssh, slurm, guarded); "
        "lab (ssh, open)." in text
    )
    assert (
        "Packages staged to run on them: clio_jarvis.xcompact3d 0.2.1 "
        "(Xcompact3d (Incompact3d v5.0) incompressible flow simulation); lab_kit.echo 0.2.1."
        in text
    )
    assert "use relay's describe and run tools" in text


def test_long_lists_and_summaries_are_cut() -> None:
    sites = [_site(f"s{index:02d}") for index in range(instructions.MAX_LISTED + 3)]
    text = instructions.compose(sites, [_package(summary="word " * 60)])
    assert "s11 (ssh, guarded); and 3 more (describe lists them all)." in text
    assert "s12" not in text
    described = text.split("clio_jarvis.xcompact3d 0.2.1 (", 1)[1].split(")", 1)[0]
    assert len(described) == instructions.SUMMARY_CHARS and described.endswith("...")


def test_no_sites_says_so() -> None:
    text = instructions.compose([], [_package()])
    assert text == f"{instructions.GENERAL} No compute sites are configured yet."


def test_a_broken_sites_file_still_gives_instructions(relay_paths: RelayPaths) -> None:
    relay_paths.sites_file.parent.mkdir(parents=True, exist_ok=True)
    relay_paths.sites_file.write_text("- {id: [\n", encoding="utf-8")
    service = RelayService(relay_paths)
    assert instructions.current(service).endswith("No compute sites are configured yet.")


@pytest.mark.asyncio
async def test_the_text_follows_the_sites_file(relay_paths: RelayPaths) -> None:
    service = RelayService(relay_paths)
    server = SimpleNamespace(instructions=instructions.current(service))
    refreshing = asyncio.create_task(
        instructions.keep_current(cast(Any, server), service, interval_s=0.01)
    )
    try:
        add_site(relay_paths, "later")
        PackageCatalog(relay_paths.packages_dir).put(_package())
        for _ in range(500):
            if "clio_jarvis.xcompact3d" in server.instructions:
                break
            await asyncio.sleep(0.01)
        assert "later (fixture, open)" in server.instructions
        assert "clio_jarvis.xcompact3d 0.2.1" in server.instructions
    finally:
        refreshing.cancel()
        await asyncio.gather(refreshing, return_exceptions=True)
        await service.close()


@pytest.mark.release_platform("posix")
@pytest.mark.parametrize("mode", ["legacy", "auto"])
def test_a_plain_client_reads_them_when_it_connects(relay_paths: RelayPaths, mode: str) -> None:
    add_site(relay_paths, "ares", adapter="fixture", policy="guarded")
    PackageCatalog(relay_paths.packages_dir).put(_package())
    env = dict(os.environ)
    env["CLIO_RELAY_CONFIG_DIR"] = str(relay_paths.config_dir)
    env["CLIO_RELAY_STATE_DIR"] = str(relay_paths.state_dir)
    server = StdioServerParameters(
        command=sys.executable,
        args=["-c", "from clio_relay.cli import main; main()", "serve"],
        env=env,
    )

    async def scenario() -> str | None:
        async with Client(server, mode=mode) as client:
            return client.instructions

    text = asyncio.run(scenario())
    assert text is not None
    assert "Compute sites configured for relay: ares (fixture, guarded)." in text
    assert "clio_jarvis.xcompact3d 0.2.1 (Xcompact3d (Incompact3d v5.0)" in text

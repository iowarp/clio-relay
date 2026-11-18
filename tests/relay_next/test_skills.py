"""Site skills served as MCP resources (RL34)."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest
from fastmcp import Client

from clio_relay import catalog
from clio_relay.sites.store import SiteStore
from clio_relay.surface.changes import ServerChanges
from clio_relay.surface.server import create_server
from clio_relay.surface.settings import RelayPaths
from clio_relay.surface.skills import (
    SkillResources,
    site_skill_uri,
    skill_link,
    skill_uri,
    skills,
)

from .conftest import add_site

pytestmark = pytest.mark.release_platform("posix")


def _link(paths: RelayPaths, site_id: str, skill: str | None) -> None:
    SiteStore(paths.sites_file).edit(site_id, {"skill": skill})


def _own_skill(paths: RelayPaths, name: str, text: str) -> Path:
    folder = paths.config_dir / "skills"
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / f"{name}.md"
    path.write_text(text, "utf-8")
    return path


def test_catalog_sites_link_shipped_skills() -> None:
    shipped = catalog.skills()
    assert shipped
    for entry in catalog.sites():
        if entry.skill is not None:
            assert entry.skill in shipped, entry.id


def test_own_skills_replace_shipped_ones(relay_paths: RelayPaths) -> None:
    name = next(iter(catalog.skills()))
    _own_skill(relay_paths, name, "# Mine\n")
    _own_skill(relay_paths, "extra", "# Extra\nnotes\n")
    found = skills(relay_paths.config_dir)
    assert found[name].text == "# Mine\n" and found[name].source == "person"
    assert found["extra"].title == "Extra"


def test_skill_resources_list_and_read(relay_paths: RelayPaths) -> None:
    add_site(relay_paths, "fx")
    add_site(relay_paths, "bare")
    add_site(relay_paths, "lost")
    _own_skill(relay_paths, "fx-notes", "# Using fx\nSubmit with run.\n")
    _link(relay_paths, "fx", "fx-notes")
    _link(relay_paths, "lost", "no-such-skill")

    async def scenario() -> None:
        async with Client(create_server(relay_paths), mode="legacy") as client:
            listed = {str(item.uri): item for item in await client.list_resources()}
            for name in catalog.skills():
                assert skill_uri(name) in listed
            assert skill_uri("fx-notes") in listed
            assert site_skill_uri("fx") in listed
            assert site_skill_uri("bare") not in listed
            assert site_skill_uri("lost") not in listed
            item = listed[site_skill_uri("fx")]
            assert item.mimeType == "text/markdown" and item.title == "Using fx"
            read = await client.read_resource(site_skill_uri("fx"))
            assert getattr(read[0], "text", None) == "# Using fx\nSubmit with run.\n"

            described: Any = (await client.call_tool("describe", {})).structured_content
            by_id = {site["id"]: site for site in described["sites"]}
            assert by_id["fx"]["skill"] == {"linked": "fx-notes", "uri": site_skill_uri("fx")}
            assert by_id["bare"]["skill"] is None
            assert by_id["lost"]["skill"]["uri"] is None
            one: Any = (await client.call_tool("describe", {"site": "fx"})).structured_content
            assert one["skill"]["uri"] == site_skill_uri("fx")

    asyncio.run(scenario())


def test_a_site_links_a_skill_file_by_path(relay_paths: RelayPaths, tmp_path: Path) -> None:
    add_site(relay_paths, "fx")
    outside = tmp_path / "elsewhere" / "fx.md"
    outside.parent.mkdir()
    outside.write_text("# Outside\n", "utf-8")
    _link(relay_paths, "fx", str(outside))
    entry = SiteStore(relay_paths.sites_file).get("fx")
    assert skill_link(entry, relay_paths.config_dir) == {
        "linked": str(outside),
        "uri": site_skill_uri("fx"),
    }
    (relay_paths.config_dir / "fx-rel.md").write_text("# Relative\n", "utf-8")
    _link(relay_paths, "fx", "fx-rel.md")
    provider = SkillResources(relay_paths.config_dir, SiteStore(relay_paths.sites_file))
    texts = {str(item.uri): item.text for item in provider.resources()}
    assert texts[site_skill_uri("fx")] == "# Relative\n"


@pytest.mark.asyncio
async def test_watch_announces_changes(relay_paths: RelayPaths) -> None:
    add_site(relay_paths, "fx")
    path = _own_skill(relay_paths, "fx-notes", "# One\n")
    changes = ServerChanges()
    events: list[tuple[str, str | None]] = []

    async def list_changed() -> None:
        events.append(("list", None))

    async def updated(uri: str) -> None:
        events.append(("updated", uri))

    changes.resources_list_changed = list_changed  # type: ignore[method-assign]
    changes.resource_updated = updated  # type: ignore[method-assign]
    provider = SkillResources(relay_paths.config_dir, SiteStore(relay_paths.sites_file))
    watching = asyncio.create_task(provider.watch(changes, interval_s=0.05))
    try:
        await asyncio.sleep(0.15)
        assert events == []
        _link(relay_paths, "fx", "fx-notes")
        await _until(lambda: ("list", None) in events)
        events.clear()
        path.write_text("# Two\n", "utf-8")
        await _until(lambda: ("updated", site_skill_uri("fx")) in events)
        assert ("updated", skill_uri("fx-notes")) in events
        assert ("list", None) not in events
    finally:
        watching.cancel()
        await asyncio.gather(watching, return_exceptions=True)


async def _until(check: Any, limit_s: float = 5.0) -> None:
    loop = asyncio.get_running_loop()
    end = loop.time() + limit_s
    while not check():
        assert loop.time() < end, "the change was not announced"
        await asyncio.sleep(0.02)

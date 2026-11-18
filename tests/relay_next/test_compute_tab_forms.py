"""The Compute tab's forms: a field's error shows on that field."""

from __future__ import annotations

import asyncio

from clio_relay.sites.store import SiteStore
from clio_relay.surface.a2ui.components import Tree
from clio_relay.surface.a2ui.forms import Field, field_components
from clio_relay.surface.settings import RelayPaths

from .test_compute_tab import Host, a2ui_client


def test_a_choice_keeps_its_error_as_a_caption() -> None:
    """A Basic ChoicePicker draws no check message, so its error stays visible as text."""
    item = Field("policy", "Policy", "choice", options=("guarded",))
    tree = Tree()
    ids = field_components(tree, "f", item, "/d/policy", "Pick one", "/d/valid/policy")
    by_id = {component["id"]: component for component in tree.components}
    assert "checks" not in by_id["f"]
    assert by_id["f-error"]["text"] == "Pick one"
    assert ids == ["f", "f-error"]
    text = Field("name", "Name", "text")
    tree = Tree()
    ids = field_components(tree, "t", text, "/d/name", "Too long", "/d/valid/name")
    assert ids == ["t"]
    assert tree.components[0]["checks"] == [
        {"condition": {"path": "/d/valid/name"}, "message": "Too long"}
    ]


def test_add_from_catalog(relay_paths: RelayPaths) -> None:
    async def scenario() -> None:
        async with a2ui_client(relay_paths) as client:
            host = Host(client)
            await host.open()
            options = host.surfaces["relay-add"]["components"]["add-catalog-site"]["options"]
            first = options[0]["value"]
            host.set("relay-add", "/catalog/id", [first])
            host.set("relay-add", "/catalog/site_id", "Bad Id")
            result = await host.press("relay-add", "add-catalog")
            assert result.structured_content["ok"] is False
            assert set(host.failing("relay-add")) == {"add-catalog-id"}
            assert host.get("relay-add", "/catalog/valid/catalog.site_id") is False
            host.set("relay-add", "/catalog/id", [])
            await host.press("relay-add", "add-catalog")
            assert host.failing("relay-add") == {}
            assert "Choose a site from the catalog" in host.text("relay-add")
            host.set("relay-add", "/catalog/id", [first])
            host.set("relay-add", "/catalog/site_id", "mine")
            result = await host.press("relay-add", "add-catalog")
            assert result.structured_content["ok"] is True, host.text("relay-add")
            assert host.get("relay-add", "/catalog/valid") == {}
            [site] = SiteStore(relay_paths.sites_file).list()
            assert site.id == "mine"
            assert "s-mine" in host.visible("relay-sites")

    asyncio.run(scenario())

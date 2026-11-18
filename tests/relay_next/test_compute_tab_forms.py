"""The Compute tab's forms: a field's error shows on that field."""

from __future__ import annotations

import asyncio

from clio_relay.sites.store import SiteStore
from clio_relay.surface.a2ui.components import Tree
from clio_relay.surface.a2ui.forms import Field, Kind, field_components
from clio_relay.surface.settings import RelayPaths

from .test_compute_tab import Host, a2ui_client


def test_every_field_carries_its_own_error_as_a_check() -> None:
    """A ChoicePicker, a CheckBox and a TextField each fail a check bound to the form's data."""
    kinds: tuple[tuple[Kind, tuple[str, ...]], ...] = (
        ("choice", ("guarded",)),
        ("bool", ()),
        ("text", ()),
    )
    for kind, options in kinds:
        item = Field("x", "X", kind, options=options)
        tree = Tree()
        ids = field_components(tree, "f", item, "/d/x", "Wrong", "/d/valid/x")
        assert ids == ["f"]
        assert tree.components[0]["checks"] == [
            {"condition": {"path": "/d/valid/x"}, "message": "Wrong"}
        ]
    tree = Tree()
    ids = field_components(tree, "c", Field("x", "X", "choice", options=("a",)), "/d/x", "Wrong")
    assert ids == ["c", "c-error"]
    assert "checks" not in tree.components[0]


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
            assert host.failing("relay-add") == {
                "add-catalog-site": "Choose a site from the catalog"
            }
            assert host.get("relay-add", "/catalog/valid/catalog.id") is False
            assert "Choose a site from the catalog" not in host.text("relay-add")
            host.set("relay-add", "/catalog/id", [first])
            host.set("relay-add", "/catalog/site_id", "mine")
            result = await host.press("relay-add", "add-catalog")
            assert result.structured_content["ok"] is True, host.text("relay-add")
            assert host.get("relay-add", "/catalog/valid") == {}
            [site] = SiteStore(relay_paths.sites_file).list()
            assert site.id == "mine"
            assert "s-mine" in host.visible("relay-sites")

    asyncio.run(scenario())

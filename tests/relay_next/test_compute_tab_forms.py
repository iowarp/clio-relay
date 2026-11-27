"""The Compute page's forms: a field's error shows on that field."""

from __future__ import annotations

import asyncio

from clio_relay.adapters.base import SiteInfo
from clio_relay.sites.store import SiteStore
from clio_relay.surface.a2ui.components import Tree
from clio_relay.surface.a2ui.forms import Field, Kind, draft_value, field_components, form
from clio_relay.surface.a2ui.newsite import ready_line
from clio_relay.surface.settings import RelayPaths

from .a2ui_host import Host, basic_client


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


def test_a_picker_list_sent_back_stays_one_list() -> None:
    """A ChoicePicker's draft comes back from the client as its list, empty or not."""
    item = Field("policy", "Security", "choice", options=("guarded", "open"))
    assert draft_value(item, []) == []
    assert draft_value(item, ["open"]) == ["open"]
    assert draft_value(item, "open") == ["open"]


def test_globus_names_its_compute_endpoint() -> None:
    """Globus's ``endpoint`` would read as the page's own word, so its label says which."""
    schema = {"properties": {"endpoint": {"type": "string"}}, "required": ["endpoint"]}
    assert form("globus", schema).fields[0].label == "Globus Compute endpoint ID"
    assert form("globus", schema).fields[0].key == "endpoint"
    assert form("other", schema).fields[0].label == "Endpoint"


def test_add_from_catalog(relay_paths: RelayPaths) -> None:
    """The Basic page's add form; relay's catalog gets the add dialog instead."""

    async def scenario() -> None:
        async with basic_client(relay_paths) as client:
            host = Host(client)
            await host.open()
            await host.add()
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
                "add-catalog-site": "Choose an endpoint from the catalog"
            }
            assert host.get("relay-add", "/catalog/valid/catalog.id") is False
            assert "Choose an endpoint from the catalog" not in host.text("relay-add")
            host.set("relay-add", "/catalog/id", [first])
            host.set("relay-add", "/catalog/site_id", "mine")
            result = await host.press("relay-add", "add-catalog")
            assert result.structured_content["ok"] is True, host.text("relay-add")
            assert host.get("relay-add", "/catalog/valid") == {}
            [site] = SiteStore(relay_paths.sites_file).list()
            assert site.id == "mine"
            assert f"s-{site.id}" in host.visible("relay-sites")
            assert host.visible("relay-add") == {"root"}, "a successful add closes the form"

    asyncio.run(scenario())


def test_no_scheduler_is_null_on_the_wire_whatever_the_adapter_wrote() -> None:
    for written in ("none", "None", " NONE ", "", None):
        info = SiteInfo(adapter="any", scheduler=written)
        assert info.scheduler is None
        assert info.model_dump(mode="json")["scheduler"] is None
    info = SiteInfo(adapter="any")
    info.scheduler = "SLURM"
    assert info.scheduler == "slurm"
    info.scheduler = "none"
    assert info.scheduler is None


def test_a_ready_line_names_the_scheduler_properly_and_leaves_out_none() -> None:
    local = SiteInfo(adapter="local", scheduler=None)
    assert (
        ready_line("This computer", "local", local, 0.1)
        == "Reached This computer over Local: 0.1 s"
    )
    for reported, shown in (
        ("slurm", "Slurm"),
        ("pbs", "PBS"),
        ("lsf", "LSF"),
        ("cobalt", "Cobalt"),
    ):
        info = SiteInfo(adapter="ssh", scheduler=reported)
        assert ready_line("Ares", "ssh", info, 1.14) == f"Reached Ares over SSH: {shown}, 1.1 s"
    assert ready_line("Ares", "ssh", None, 2.0) == "Reached Ares over SSH: 2.0 s"

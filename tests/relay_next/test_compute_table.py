"""The Compute page's endpoint table, for a client that renders relay's catalog.

The table follows CLIO's Tools settings: a defaults row, then one row per
endpoint with inline Protocol and Security pickers, its Status and a menu. Each
pick and menu item changes the same site store the CLI does.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from clio_relay.sites.store import SiteStore
from clio_relay.surface.a2ui.components import COMPONENTS, TABLE
from clio_relay.surface.service import RelayService
from clio_relay.surface.settings import RelayPaths

from .a2ui_host import Host, a2ui_client, basic_client, said
from .conftest import add_site


# What CLIO's choice-table renderer accepts: rows and the table are strict.
ROW_KEYS = {"id", "title", "subtitle", "caption", "icon", "values", "inherited", "error"}
ROW_KEYS |= {"context", "menu"}
TABLE_KEYS = {"id", "component", "label", "columns", "rows", "defaults", "onChoose", "rowMenu"}
TABLE_KEYS |= {"add", "empty", "accessibility", "weight"}


def _options(table: dict[str, Any], column: str) -> dict[str, dict[str, Any]]:
    [found] = [item for item in table["columns"] if item["id"] == column]
    return {option["value"]: option for option in found["options"]}


def test_the_table_has_a_defaults_row_and_a_row_per_endpoint(relay_paths: RelayPaths) -> None:
    add_site(relay_paths)

    async def scenario() -> None:
        async with a2ui_client(relay_paths) as client:
            host = Host(client)
            first = await host.open()
            assert await host.open() == first, "a reread with nothing changed is identical"
            table = host.table()
            assert [column["id"] for column in table["columns"]] == [
                "protocol",
                "security",
                "status",
            ]
            assert [column["title"] for column in table["columns"]] == [
                "Protocol",
                "Security",
                "Status",
            ]
            protocols = _options(table, "protocol")
            order = ["ssh", "worker", "tapis", "globus", "iri", "sky", "local"]
            assert list(protocols)[:7] == order
            assert protocols["sky"]["label"] == "SkyPilot"
            assert protocols["ssh"]["description"]
            security = _options(table, "security")
            assert {key: item["label"] for key, item in security.items()} == {
                "guarded": "Secure",
                "open": "Open",
            }
            assert security["guarded"]["description"] == "Runs only reviewed packages"
            status = [column for column in table["columns"] if column["id"] == "status"][0]
            assert status["editable"] is False
            assert all(option["icon"] and option["tone"] for option in status["options"])
            assert table["defaults"]["values"] == {"protocol": "ssh", "security": "guarded"}
            assert table["add"]["label"] == "Add endpoint"
            row = host.row("fx")
            assert row["values"] == {"protocol": "fixture", "security": "open", "status": "ready"}
            assert [item["id"] for item in row["menu"]] == ["probe", "edit", "remove"]
            assert row["menu"][-1]["destructive"] is True
            assert set(row) <= ROW_KEYS and set(table) <= TABLE_KEYS
            assert "s-fx-detail" not in host.visible("relay-sites")
            for surface in host.surfaces.values():
                for component in surface["components"].values():
                    assert component["component"] in COMPONENTS
            assert host.visible("relay-add") == {"root"}

    asyncio.run(scenario())


def test_a_pick_without_its_rows_token_is_refused(relay_paths: RelayPaths) -> None:
    add_site(relay_paths)
    store = SiteStore(relay_paths.sites_file)

    async def scenario() -> None:
        async with a2ui_client(relay_paths) as client:
            host = Host(client)
            await host.open()
            name, context = host.choice("fx", "security", "guarded")
            del context["token"]
            result = await client.call_tool(name, context, raise_on_error=False)
            assert result.is_error and "out of date" in said(result)
            assert store.get("fx").policy == "open"
            # A row's token names its endpoint, whatever row the call claims.
            add_site(relay_paths, "other")
            await host.open()
            name, context = host.choice("fx", "security", "guarded")
            result = await client.call_tool(name, context | {"row": "other"})
            assert not result.is_error
            assert store.get("fx").policy == "guarded"
            assert store.get("other").policy == "open"

    asyncio.run(scenario())


def test_picking_security_and_protocol_edits_the_endpoint(
    relay_paths: RelayPaths, monkeypatch: pytest.MonkeyPatch
) -> None:
    added: list[str] = []
    original = RelayService.site_added

    async def site_added(self: RelayService, site_id: str) -> Any:
        added.append(site_id)
        return await original(self, site_id)

    monkeypatch.setattr(RelayService, "site_added", site_added)
    add_site(relay_paths, "lab")
    store = SiteStore(relay_paths.sites_file)

    async def scenario() -> None:
        async with a2ui_client(relay_paths) as client:
            host = Host(client)
            await host.open()
            result = await host.choose("lab", "security", "guarded")
            assert result.structured_content["ok"] is True, said(result)
            assert store.get("lab").policy == "guarded"
            assert host.row("lab")["values"]["security"] == "guarded"
            assert "lab is now Secure." in said(result)
            assert added == ["lab"]

            # Local needs no settings, so picking it adds it and uses it at once.
            result = await host.choose("lab", "protocol", "local")
            assert result.structured_content["ok"] is True, said(result)
            site = store.get("lab")
            assert site.adapter == "local" and "fixture" in site.alternatives
            assert host.row("lab")["values"]["protocol"] == "local"
            assert "lab now uses Local." in said(result)
            # Picking one it already has switches back.
            await host.choose("lab", "protocol", "fixture")
            assert store.get("lab").adapter == "fixture"

            result = await host.choose("lab", "security", "nowhere")
            assert result.structured_content["ok"] is False
            assert host.row("lab")["error"] == "Choose Secure or Open"
            assert store.get("lab").policy == "guarded"

    asyncio.run(scenario())


def test_a_protocol_that_needs_settings_opens_them_under_its_row(relay_paths: RelayPaths) -> None:
    add_site(relay_paths, "lab", adapter="local", config={})
    store = SiteStore(relay_paths.sites_file)

    async def scenario() -> None:
        async with a2ui_client(relay_paths) as client:
            host = Host(client)
            await host.open()
            result = await host.choose("lab", "protocol", "ssh")
            assert result.structured_content["ok"] is True, said(result)
            assert "Fill in lab's SSH settings, then save to use SSH." in said(result)
            assert store.get("lab").adapter == "local", "nothing changes before the save"
            assert "s-lab-detail" in host.visible("relay-sites")
            assert "e-lab-save" in host.visible("relay-sites")
            assert host.get("relay-sites", "/drafts/lab/adapters/ssh/use") is True
            assert [item["id"] for item in host.row("lab")["menu"]] == ["probe", "remove"]

            # Saving without the profile shows the error on its field.
            result = await host.press("relay-sites", "e-lab-save")
            assert result.structured_content["ok"] is False
            assert set(host.failing("relay-sites")) == {"e-lab-a-ssh-f-ssh_profile"}

            host.set("relay-sites", "/drafts/lab/adapters/ssh/ssh_profile", "lab-login")
            result = await host.press("relay-sites", "e-lab-save")
            assert result.structured_content["ok"] is True, host.text("relay-sites")
            site = store.get("lab")
            assert site.adapter == "ssh" and site.config["ssh_profile"] == "lab-login"
            assert "local" in site.alternatives
            assert "It now uses SSH." in said(result)
            assert "s-lab-detail" not in host.visible("relay-sites")

            # Cancelling a picked protocol leaves the endpoint as it was.
            await host.choose("lab", "protocol", "iri")
            tabs = host.surfaces["relay-sites"]["components"]["e-lab-tabs"]["tabs"]
            assert tabs[0]["title"] == "IRI", "the protocol to fill in opens first"
            await host.press("relay-sites", "e-lab-cancel")
            assert store.get("lab").adapter == "ssh"
            assert "iri" not in store.get("lab").alternatives
            assert "s-lab-detail" not in host.visible("relay-sites")

    asyncio.run(scenario())


def test_defaults_shape_the_add_form(relay_paths: RelayPaths) -> None:
    store = SiteStore(relay_paths.sites_file)

    async def scenario() -> None:
        async with a2ui_client(relay_paths) as client:
            host = Host(client)
            await host.open()
            assert host.table()["rows"] == []
            assert host.table()["empty"] == "No endpoints yet."
            result = await host.choose("", "protocol", "local")
            assert result.structured_content["ok"] is True, said(result)
            await host.choose("", "security", "open")
            assert host.table()["defaults"]["values"] == {"protocol": "local", "security": "open"}
            result = await host.choose("", "protocol", "fixture")
            assert result.structured_content["ok"] is False, "a test-only protocol is not offered"

            await host.add()
            tabs = host.surfaces["relay-add"]["components"]["add-tabs"]["tabs"]
            assert tabs[0]["title"] == "Local"
            assert host.get("relay-add", "/drafts/add/site/policy") == ["open"]
            host.set("relay-add", "/drafts/add/site/id", "here")
            result = await host.press("relay-add", "add-a-local-add")
            assert result.structured_content["ok"] is True, host.text("relay-add")
            site = store.get("here")
            assert (site.adapter, site.policy) == ("local", "open")
            assert host.row("here")["values"]["protocol"] == "local"

        # A client with only the Basic catalog saves the same defaults from its form.
        async with basic_client(relay_paths) as client:
            host = Host(client)
            await host.open()
            assert host.get("relay-sites", "/defaults") == {
                "protocol": ["local"],
                "security": ["open"],
            }
            host.set("relay-sites", "/defaults/security", ["guarded"])
            result = await host.press("relay-sites", "defaults-save")
            assert result.structured_content["ok"] is True, said(result)
            assert "Saved the defaults for new endpoints." in said(result)
        async with a2ui_client(relay_paths) as client:
            host = Host(client)
            await host.open()
            assert host.table()["defaults"]["values"]["security"] == "guarded"

    asyncio.run(scenario())


def test_the_row_menu_probes_edits_and_removes(relay_paths: RelayPaths) -> None:
    add_site(relay_paths, "lab")
    store = SiteStore(relay_paths.sites_file)

    async def scenario() -> None:
        async with a2ui_client(relay_paths) as client:
            host = Host(client)
            await host.open()
            result = await host.menu("lab", "probe")
            assert result.structured_content["ok"] is True, said(result)
            assert "Probed lab." in said(result)
            assert "The last probe found it" in host.row("lab")["caption"]
            assert "·" not in host.row("lab")["caption"]

            await host.menu("lab", "edit")
            assert "e-lab-save" in host.visible("relay-sites")
            host.set("relay-sites", "/drafts/lab/site/name", "Lab cluster")
            result = await host.press("relay-sites", "e-lab-save")
            assert result.structured_content["ok"] is True, host.text("relay-sites")
            assert store.get("lab").name == "Lab cluster"
            assert host.row("lab")["title"] == "Lab cluster"

            await host.menu("lab", "remove")
            assert "s-lab-confirm-yes" in host.visible("relay-sites")
            assert store.list() != []
            await host.press("relay-sites", "s-lab-confirm-yes")
            assert store.list() == []
            assert host.table()["rows"] == []
            assert "Removed Lab cluster." in host.text("relay-sites")

    asyncio.run(scenario())


def test_connect_is_in_the_menu_of_an_endpoint_that_needs_a_sign_in(
    relay_paths: RelayPaths,
) -> None:
    add_site(relay_paths, "signs", config={"sign_in": True})

    async def scenario() -> None:
        async with a2ui_client(relay_paths) as client:
            host = Host(client)
            await host.open()
            row = host.row("signs")
            assert row["values"]["status"] == "needs_sign_in"
            assert row["menu"][0]["id"] == "connect"
            assert row["caption"].startswith("Needs a ")
            result = await host.menu("signs", "connect")
            assert result.structured_content["ok"] is True, said(result)
            assert "s-signs-detail" in host.visible("relay-sites")
            link = host.surfaces["relay-sites"]["components"]["s-signs-open"]
            assert link["action"]["functionCall"]["args"]["url"].startswith("http://127.0.0.1:")
            assert all(
                component["component"] != TABLE or component["id"] == "endpoints"
                for component in host.surfaces["relay-sites"]["components"].values()
            )

    asyncio.run(scenario())

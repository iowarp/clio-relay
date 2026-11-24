"""The Compute page's endpoint table, for a client that renders relay's catalog.

The table follows CLIO's Tools settings: column headers, then one row per
endpoint with inline Protocol, Security and Spending pickers, its Status and a
menu. The defaults for new endpoints sit behind a gear, in a dialog. Each pick
and menu item changes the same site store the CLI does.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest
from typer.testing import CliRunner

from clio_relay import cli
from clio_relay.sites.store import SiteStore
from clio_relay.surface.a2ui.components import COMPONENTS, TABLE
from clio_relay.surface.service import RelayService
from clio_relay.surface.settings import RelayPaths

from .a2ui_host import Host, a2ui_client, basic_client, said, toast
from .conftest import add_site

# What CLIO's choice-table renderer accepts: rows and the table are strict.
ROW_KEYS = frozenset(
    [
        "id",
        "title",
        "subtitle",
        "caption",
        "icon",
        "values",
        "options",
        "inherited",
        "error",
        "context",
        "menu",
    ]
)
TABLE_KEYS = frozenset(
    [
        "id",
        "component",
        "label",
        "columns",
        "rows",
        "onChoose",
        "rowMenu",
        "empty",
        "accessibility",
        "weight",
    ]
)


def _options(table: dict[str, Any], column: str) -> dict[str, dict[str, Any]]:
    [found] = [item for item in table["columns"] if item["id"] == column]
    return {option["value"]: option for option in found["options"]}


def test_the_table_has_headers_and_a_row_per_endpoint(relay_paths: RelayPaths) -> None:
    add_site(relay_paths)
    add_site(relay_paths, "plain", policy="guarded", spend="always")

    async def scenario() -> None:
        async with a2ui_client(relay_paths) as client:
            host = Host(client)
            first = await host.open()
            assert await host.open() == first, "a reread with nothing changed is identical"
            table = host.table()
            assert [column["id"] for column in table["columns"]] == [
                "protocol",
                "security",
                "spend",
                "status",
            ]
            assert [column["title"] for column in table["columns"]] == [
                "Protocol",
                "Security",
                "Spending",
                "Status",
            ]
            protocols = _options(table, "protocol")
            order = ["ssh", "worker", "tapis", "globus", "iri", "sky", "local"]
            assert list(protocols)[:7] == order
            assert protocols["sky"]["label"] == "SkyPilot"
            assert protocols["ssh"]["description"]
            assert all(protocols[name].get("icon") for name in order)
            security = _options(table, "security")
            assert {key: item["label"] for key, item in security.items()} == {
                "guarded": "Secure",
                "open": "Open",
            }
            assert security["guarded"]["description"] == "Runs only reviewed packages"
            status = [column for column in table["columns"] if column["id"] == "status"][0]
            assert status["editable"] is False
            assert all(option["icon"] and option["tone"] for option in status["options"])
            assert "defaults" not in table and "add" not in table
            # The gear and Add endpoint sit on the page's header line, each opening a dialog.
            assert {"page-head", "defaults-open", "add-open"} <= host.visible("relay-sites")
            assert not host.is_open("defaults-dialog") and not host.is_open("add-dialog")
            assert host.get("relay-sites", "/defaultsDraft/protocol") == ["ssh"]
            assert host.get("relay-sites", "/defaultsDraft/policy") == ["guarded"]
            assert host.get("relay-sites", "/defaultsDraft/spend") == ["always"]
            row = host.row("fx")
            assert row["values"] == {
                "protocol": "fixture",
                "security": "open",
                "spend": "never",
                "status": "ready",
            }
            assert [item["id"] for item in row["menu"]] == ["test", "edit", "remove"]
            assert row["menu"][-1]["destructive"] is True
            assert row["options"] == {"protocol": ["fixture"]}
            assert "inherited" not in row
            # A value the defaults gave shows as inherited, the way Tools marks its defaults.
            assert host.row("plain")["inherited"] == ["security", "spend"]
            assert set(row) <= ROW_KEYS and set(table) <= TABLE_KEYS
            assert "s-fx-detail" not in host.visible("relay-sites")
            for surface in host.surfaces.values():
                for component in surface["components"].values():
                    assert component["component"] in COMPONENTS
            assert set(host.surfaces) == {"relay-sites"}

    asyncio.run(scenario())


def test_rows_stay_within_the_tables_bounds(relay_paths: RelayPaths) -> None:
    add_site(relay_paths)
    SiteStore(relay_paths.sites_file).edit("fx", {"name": "N" * 300, "address": "a" * 300})

    async def scenario() -> None:
        async with a2ui_client(relay_paths) as client:
            host = Host(client)
            await host.open()
            row = host.row("fx")
            assert len(row["title"]) == 120 and row["title"].endswith("\u2026")
            assert len(row["subtitle"]) == 120
            table = host.table()
            declared = {column["id"] for column in table["columns"]}
            assert set(row["values"]) <= declared
            icons = [
                option["icon"]
                for column in table["columns"]
                for option in column["options"]
                if "icon" in option
            ]
            assert icons and all(icon.isalnum() and icon[0].islower() for icon in icons)

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


def test_a_protocol_that_needs_settings_opens_them_in_a_dialog(relay_paths: RelayPaths) -> None:
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
            assert host.is_open("edit-dialog")
            assert "e-lab-save" not in host.visible("relay-sites"), "the dialog holds the buttons"
            assert host.get("relay-sites", "/drafts/lab/adapters/ssh/use") is True

            # Saving without the profile shows the error on its field.
            result = await host.dialog("edit-dialog", "confirm")
            assert result.structured_content["ok"] is False
            assert set(host.failing("relay-sites")) == {"e-lab-a-ssh-f-ssh_profile"}

            host.set("relay-sites", "/drafts/lab/adapters/ssh/ssh_profile", "lab-login")
            result = await host.dialog("edit-dialog", "confirm")
            assert result.structured_content["ok"] is True, host.text("relay-sites")
            site = store.get("lab")
            assert site.adapter == "ssh" and site.config["ssh_profile"] == "lab-login"
            assert "local" in site.alternatives
            assert "It now uses SSH." in said(result)
            assert "edit-dialog" not in host.visible("relay-sites")

            # Cancelling a picked protocol leaves the endpoint as it was.
            await host.choose("lab", "protocol", "iri")
            tabs = host.surfaces["relay-sites"]["components"]["e-lab-tabs"]["tabs"]
            assert tabs[0]["title"] == "IRI", "the protocol to fill in opens first"
            await host.dialog("edit-dialog", "cancel")
            assert store.get("lab").adapter == "ssh"
            assert "iri" not in store.get("lab").alternatives
            assert "edit-dialog" not in host.visible("relay-sites")

    asyncio.run(scenario())


async def _save_defaults(host: Host, protocol: str, policy: str) -> Any:
    await host.press("relay-sites", "defaults-open")
    assert host.is_open("defaults-dialog")
    host.set("relay-sites", "/defaultsDraft/protocol", [protocol])
    host.set("relay-sites", "/defaultsDraft/policy", [policy])
    return await host.dialog("defaults-dialog", "confirm")


def test_the_gear_holds_the_defaults_for_new_endpoints(relay_paths: RelayPaths) -> None:
    store = SiteStore(relay_paths.sites_file)

    async def scenario() -> None:
        async with a2ui_client(relay_paths) as client:
            host = Host(client)
            await host.open()
            assert host.table()["rows"] == []
            assert host.table()["empty"] == "No endpoints yet"
            assert [column["title"] for column in host.table()["columns"]][0] == "Protocol"
            result = await _save_defaults(host, "fixture", "open")
            assert result.structured_content["ok"] is False, "a test-only protocol is not offered"
            assert host.is_open("defaults-dialog")
            assert host.component("defaults-dialog")["status"]["tone"] == "negative"

            result = await _save_defaults(host, "local", "open")
            assert result.structured_content["ok"] is True, said(result)
            assert toast(result) == {
                "message": "Saved the defaults for new endpoints",
                "tone": "positive",
            }
            assert not host.is_open("defaults-dialog")
            assert store.defaults().model_dump() == {
                "adapter": "local",
                "policy": "open",
                "spend": "always",
            }
            assert host.get("relay-sites", "/defaultsDraft/policy") == ["open"]

            # Closing the dialog without saving changes nothing.
            await host.press("relay-sites", "defaults-open")
            host.set("relay-sites", "/defaultsDraft/policy", ["guarded"])
            await host.dialog("defaults-dialog", "cancel")
            assert not host.is_open("defaults-dialog")
            assert store.defaults().policy == "open"

        # A client with only the Basic catalog saves the same defaults from its form.
        async with basic_client(relay_paths) as client:
            host = Host(client)
            await host.open()
            assert host.get("relay-sites", "/defaults") == {
                "protocol": ["local"],
                "security": ["open"],
                "spend": ["always"],
            }
            host.set("relay-sites", "/defaults/security", ["guarded"])
            result = await host.press("relay-sites", "defaults-save")
            assert result.structured_content["ok"] is True, said(result)
            assert "Saved the defaults for new endpoints." in said(result)
        async with a2ui_client(relay_paths) as client:
            host = Host(client)
            await host.open()
            assert host.get("relay-sites", "/defaultsDraft/policy") == ["guarded"]

    asyncio.run(scenario())


def test_the_page_and_the_cli_share_the_defaults(
    relay_paths: RelayPaths, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("CLIO_RELAY_CONFIG_DIR", str(relay_paths.config_dir))
    monkeypatch.setenv("CLIO_RELAY_STATE_DIR", str(relay_paths.state_dir))
    store = SiteStore(relay_paths.sites_file)
    runner = CliRunner()

    async def pick() -> None:
        async with a2ui_client(relay_paths) as client:
            result = await _save_defaults(Host(client), "local", "open")
            assert result.structured_content["ok"] is True, said(result)

    async def shown() -> dict[str, Any]:
        async with a2ui_client(relay_paths) as client:
            host = Host(client)
            await host.open()
            found = host.get("relay-sites", "/defaultsDraft")
            return {"protocol": found["protocol"], "policy": found["policy"]}

    asyncio.run(pick())
    assert store.defaults().model_dump() == {
        "adapter": "local",
        "policy": "open",
        "spend": "always",
    }
    assert store.path.with_name("site-defaults.yaml").is_file()

    # The CLI's site add leaves out --adapter and --policy and gets the page's defaults.
    added = runner.invoke(cli.app, ["site", "add", "here"])
    assert added.exit_code == 0, added.output
    site = store.get("here")
    assert (site.adapter, site.policy) == ("local", "open")

    refused = runner.invoke(cli.app, ["site", "defaults", "--adapter", "nope"])
    assert refused.exit_code != 0
    refused = runner.invoke(cli.app, ["site", "defaults", "--policy", "loose"])
    assert refused.exit_code != 0
    changed = runner.invoke(cli.app, ["site", "defaults", "--policy", "guarded"])
    assert changed.exit_code == 0, changed.output
    assert json.loads(changed.output) == {
        "adapter": "local",
        "policy": "guarded",
        "spend": "always",
    }
    assert asyncio.run(shown()) == {"protocol": ["local"], "policy": ["guarded"]}


def test_the_row_menu_tests_edits_and_removes_with_undo(relay_paths: RelayPaths) -> None:
    add_site(relay_paths, "lab")
    store = SiteStore(relay_paths.sites_file)

    async def scenario() -> None:
        async with a2ui_client(relay_paths) as client:
            host = Host(client)
            await host.open()
            result = await host.menu("lab", "test")
            assert result.structured_content["ok"] is True, said(result)
            assert "Probed lab." in said(result)
            assert "The last test found it" in host.row("lab")["caption"]
            assert "\u00b7" not in host.row("lab")["caption"]

            await host.menu("lab", "edit")
            assert host.is_open("edit-dialog")
            assert host.component("edit-dialog")["title"] == "Edit lab"
            host.set("relay-sites", "/drafts/lab/site/name", "Lab cluster")
            result = await host.dialog("edit-dialog", "confirm")
            assert result.structured_content["ok"] is True, host.text("relay-sites")
            assert store.get("lab").name == "Lab cluster"
            assert host.row("lab")["title"] == "Lab cluster"
            assert "edit-dialog" not in host.visible("relay-sites")

            # Remove acts at once and offers Undo, rather than asking first.
            result = await host.menu("lab", "remove")
            assert store.list() == []
            assert host.table()["rows"] == []
            shown = toast(result)
            assert shown is not None and shown["message"] == "Removed Lab cluster"
            assert shown["undo"]["label"] == "Undo"
            result = await host.undo(result)
            assert result.structured_content["ok"] is True, said(result)
            assert store.get("lab").name == "Lab cluster"
            back = toast(result)
            assert back is not None
            assert str(back["message"]).startswith("Put Lab cluster back")
            assert "undo" not in back, "putting it back offers no second Undo"

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
            assert row["caption"].startswith("Needs ")
            result = await host.menu("signs", "connect")
            assert result.structured_content["ok"] is True, said(result)
            assert host.is_open("signin-dialog")
            link = host.component("signin-open")
            assert link["action"]["functionCall"]["args"]["url"].startswith("http://127.0.0.1:")
            assert all(
                component["component"] != TABLE or component["id"] == "endpoints"
                for component in host.surfaces["relay-sites"]["components"].values()
            )
            await host.dialog("signin-dialog", "cancel")
            assert "signin-dialog" not in host.visible("relay-sites")

    asyncio.run(scenario())

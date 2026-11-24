"""The Compute page's Add endpoint dialog, for a client that renders relay's catalog.

The dialog has three tabs: Catalog (one row per site, with its protocols),
Saved (computers the person can already reach) and Your own. Each goes
site, then protocol, then Test, then Save, and Save gives a toast with Undo.
"""

from __future__ import annotations

import asyncio
import json
import re
from importlib.resources import files
from pathlib import Path
from typing import Any

import pytest

from clio_relay.sites.models import SiteEntry
from clio_relay.sites.store import SiteStore
from clio_relay.surface.a2ui import i18n, saved
from clio_relay.surface.a2ui.newsite import build, removed_record, saved_items
from clio_relay.surface.a2ui.surfaces import ADD_TABS
from clio_relay.surface.settings import RelayPaths

from .a2ui_host import Host, a2ui_client, said, toast

CATALOG_TAB, SAVED_TAB, OWN_TAB = ADD_TABS


def _locale(lang: str) -> dict[str, str]:
    text = files("clio_relay.surface.a2ui").joinpath("locales", f"{lang}.json").read_text("utf-8")
    return json.loads(text)


def _ssh_files(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Give the Saved tab a ~/.ssh/config with one host and CLIO a file with another."""
    home = tmp_path / "ssh"
    home.mkdir()
    (home / "config").write_text(
        "Host lab\n  HostName lab.example.org\n  User me\n"
        "Host *\n  User nobody\n"
        "Host mine\n  HostName elsewhere.example.org\n",
        encoding="utf-8",
    )
    kept = tmp_path / "clio-hosts"
    kept.write_text("Host mine\n  HostName mine.example.org\n", encoding="utf-8")
    monkeypatch.setattr(saved, "_ssh_dir", lambda: home)
    monkeypatch.setenv(saved.SAVED_ENV, str(kept))
    return kept


def test_the_add_dialog_opens_on_the_catalog_with_each_site_once(
    relay_paths: RelayPaths,
) -> None:
    async def scenario() -> None:
        async with a2ui_client(relay_paths) as client:
            host = Host(client)
            await host.open()
            assert not host.is_open("add-dialog")
            await host.add()
            assert host.is_open("add-dialog"), "Add endpoint opens a dialog, not the page"
            dialog = host.component("add-dialog")
            assert [tab["title"] for tab in dialog["tabs"]] == ["Catalog", "Saved", "Your own"]
            assert [tab["icon"] for tab in dialog["tabs"]] == ["book", "bookmark", "link"]
            assert host.get("relay-sites", "/add/tab") == CATALOG_TAB
            assert dialog["confirm"]["enabled"] is False
            assert dialog["rail"]["steps"] == ["Site", "Protocol", "Test", "Save"]

            sites = host.items("add-site")
            values = [item["value"] for item in sites]
            assert values[0] == "ares" and values[-1] == "local"
            assert len(values) == len(set(values)), "a site is listed once"
            assert "ares-tapis" not in values, "Ares through Tapis is a protocol of Ares"
            assert sites[0]["chips"] == ["SSH", "Tapis", "Worker"]
            assert sites[-1]["title"] == "This computer"

            # A site with several protocols offers them, the first one picked.
            result = await host.pick("add-site", "ares")
            assert result.structured_content["ok"] is True, said(result)
            assert host.get("relay-sites", "/add/protocol") == "ssh"
            ways = host.items("add-protocol")
            assert [item["value"] for item in ways] == ["ssh", "tapis", "worker"]
            assert host.component("add-dialog")["rail"]["step"] == 1

            # Closing the dialog forgets the draft.
            await host.dialog("add-dialog", "cancel")
            assert not host.is_open("add-dialog")
            await host.add()
            assert host.get("relay-sites", "/add/site") == ""

    asyncio.run(scenario())


def test_a_catalog_site_is_tested_then_saved_and_undo_removes_it(
    relay_paths: RelayPaths,
) -> None:
    store = SiteStore(relay_paths.sites_file)

    async def scenario() -> None:
        async with a2ui_client(relay_paths) as client:
            host = Host(client)
            await host.open()
            await host.add()
            await host.pick("add-site", "local")
            assert "add-protocol" not in host.visible("relay-sites"), "one protocol, no picker"

            # Save stays refused until a test passes on this draft.
            result = await host.dialog("add-dialog", "confirm")
            assert result.structured_content["ok"] is False
            status = host.component("add-dialog")["status"]
            assert status == {"tone": "negative", "text": "Test it again before you save it"}
            assert store.list() == []

            result = await host.dialog("add-dialog", "test")
            assert result.structured_content["ok"] is True, host.text("relay-sites")
            dialog = host.component("add-dialog")
            assert dialog["status"]["tone"] == "positive"
            assert dialog["confirm"]["enabled"] is True
            assert dialog["cancel"]["label"] == "Back"
            assert "tabs" not in dialog and "add-test" in host.visible("relay-sites")
            assert host.get("relay-sites", "/add/save/name") == "This computer"

            result = await host.dialog("add-dialog", "confirm")
            assert result.structured_content["ok"] is True, host.text("relay-sites")
            shown = toast(result)
            assert shown is not None
            assert str(shown["message"]).startswith("Added This computer")
            assert shown["undo"]["label"] == "Undo"
            assert not host.is_open("add-dialog")
            site = store.get("local")
            assert (site.adapter, site.name) == ("local", "This computer")
            assert host.row("local")["title"] == "This computer"

            result = await host.undo(result)
            assert result.structured_content["ok"] is True, said(result)
            assert store.list() == []
            assert toast(result) == {"message": "Removed This computer", "tone": "positive"}

    asyncio.run(scenario())


def test_a_change_after_the_test_asks_for_a_new_one(relay_paths: RelayPaths) -> None:
    async def scenario() -> None:
        async with a2ui_client(relay_paths) as client:
            host = Host(client)
            await host.open()
            await host.add()
            await host.pick("add-site", "local")
            await host.dialog("add-dialog", "test")
            assert host.component("add-dialog")["confirm"]["enabled"] is True

            # Back returns to the tabs with the draft kept; a new pick forgets the test.
            await host.dialog("add-dialog", "cancel")
            assert host.get("relay-sites", "/add/site") == "local"
            assert "tabs" in host.component("add-dialog")
            await host.pick("add-site", "ares")
            assert host.component("add-dialog")["confirm"]["enabled"] is False
            result = await host.dialog("add-dialog", "confirm")
            assert result.structured_content["ok"] is False

    asyncio.run(scenario())


def test_your_own_takes_a_name_and_a_protocol(relay_paths: RelayPaths) -> None:
    store = SiteStore(relay_paths.sites_file)

    async def scenario() -> None:
        async with a2ui_client(relay_paths) as client:
            host = Host(client)
            await host.open()
            await host.add()
            host.set("relay-sites", "/add/tab", OWN_TAB)
            result = await host.dialog("add-dialog", "test")
            assert result.structured_content["ok"] is False
            assert host.get("relay-sites", "/add/valid/own.name") is False
            assert "Enter a name" in host.failing("relay-sites").values()

            host.set("relay-sites", "/add/own/name", "Lab box")
            await host.pick("add-own-protocol", "local")
            assert host.get("relay-sites", "/add/tab") == OWN_TAB
            result = await host.dialog("add-dialog", "test")
            assert result.structured_content["ok"] is True, host.text("relay-sites")
            result = await host.dialog("add-dialog", "confirm")
            assert result.structured_content["ok"] is True, host.text("relay-sites")
            site = store.get("lab-box")
            assert (site.name, site.adapter) == ("Lab box", "local")

    asyncio.run(scenario())


def test_the_saved_tab_lists_known_hosts_and_removed_endpoints(
    relay_paths: RelayPaths, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _ssh_files(tmp_path, monkeypatch)
    store = SiteStore(relay_paths.sites_file)

    async def scenario() -> None:
        async with a2ui_client(relay_paths) as client:
            host = Host(client)
            await host.open()
            await host.add()
            items = host.items("add-saved-list")
            assert [item["value"] for item in items] == ["clio:mine", "host:lab"]
            assert [item["group"] for item in items] == ["Saved SSH hosts", "From ~/.ssh/config"]
            assert items[0]["subtitle"] == "mine.example.org"
            assert items[1]["subtitle"] == "me@lab.example.org"
            assert all(item["chips"] == ["SSH"] for item in items)

            result = await host.pick("add-saved-list", "host:lab")
            assert result.structured_content["ok"] is True, said(result)
            assert host.get("relay-sites", "/add/tab") == SAVED_TAB
            await host.dialog("add-dialog", "cancel")

            # A removed endpoint comes back under its own group, and saving it again
            # takes it off the list.
            await host.add()
            await host.pick("add-site", "local")
            await host.dialog("add-dialog", "test")
            await host.dialog("add-dialog", "confirm")
            await host.menu("local", "remove")
            assert store.list() == []
            await host.add()
            items = host.items("add-saved-list")
            assert items[-1]["value"] == "removed:0"
            assert items[-1]["group"] == "Removed from relay"
            assert items[-1]["subtitle"] == "Local, removed today"
            await host.pick("add-saved-list", "removed:0")
            result = await host.dialog("add-dialog", "test")
            assert result.structured_content["ok"] is True, host.text("relay-sites")
            result = await host.dialog("add-dialog", "confirm")
            assert result.structured_content["ok"] is True, host.text("relay-sites")
            assert store.get("local").adapter == "local"
            await host.add()
            assert [item["value"] for item in host.items("add-saved-list")] == [
                "clio:mine",
                "host:lab",
            ]

    asyncio.run(scenario())


def test_a_saved_host_builds_an_ssh_endpoint(
    relay_paths: RelayPaths, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    kept = _ssh_files(tmp_path, monkeypatch)
    store = SiteStore(relay_paths.sites_file)
    environ = {saved.SAVED_ENV: str(kept)}

    built = build({"tab": SAVED_TAB, "saved": "host:lab"}, store, {}, [], environ=environ)
    assert built.errors == {}
    assert built.entry is not None
    assert (built.entry.id, built.entry.adapter, built.entry.address) == (
        "lab",
        "ssh",
        "lab.example.org",
    )
    assert built.entry.config == {"ssh_profile": "lab"}

    # A host CLIO saved names CLIO's file, so ssh reads the same entry.
    built = build({"tab": SAVED_TAB, "saved": "clio:mine"}, store, {}, [], environ=environ)
    assert built.entry is not None
    assert built.entry.config == {"ssh_profile": "mine", "ssh_config": str(kept)}

    missing = build({"tab": SAVED_TAB, "saved": "host:gone"}, store, {}, [], environ=environ)
    assert missing.entry is None and missing.errors == {"add.saved": "Choose a computer"}

    entry = SiteEntry(id="old", name="Old one", adapter="local")
    record = removed_record(entry)
    found = saved_items([record], [], environ)
    assert [item.key for item in found] == ["clio:mine", "host:lab", "removed:0"]
    # An endpoint already reached by an alias, or already back, is left out.
    assert [item.key for item in saved_items([record], ["lab", "old"], environ)] == ["clio:mine"]
    built = build({"tab": SAVED_TAB, "saved": "removed:0"}, store, {}, [record], environ=environ)
    assert built.entry is not None and built.entry.id == "old"


CATALOG_FILE = (
    Path(__file__).resolve().parents[2]
    / "plugin"
    / "ai.iowarp.clio"
    / "a2ui"
    / "clio-relay-compute"
    / "catalog.json"
)
# ComponentCommon lives in A2UI's common_types, which relay doesn't vendor.
COMMON = {"id", "accessibility", "weight"}


def _declared(catalog: dict[str, Any], schema: dict[str, Any]) -> dict[str, Any]:
    """Return the properties a component schema declares, local refs followed."""
    found: dict[str, Any] = dict(schema.get("properties", {}))
    for part in schema.get("allOf", []):
        ref = part.get("$ref", "")
        if ref.startswith("#/$defs/"):
            found |= _declared(catalog, catalog["$defs"][ref.removeprefix("#/$defs/")])
        elif not ref:
            found |= _declared(catalog, part)
    return found


def test_everything_the_page_draws_fits_the_shipped_catalog(relay_paths: RelayPaths) -> None:
    """CLIO refuses a surface its catalog doesn't allow, so the page must fit it."""
    catalog = json.loads(CATALOG_FILE.read_text("utf-8"))
    SiteStore(relay_paths.sites_file).add(SiteEntry(id="here", name="Here", adapter="local"))
    drawn: list[dict[str, Any]] = []

    async def scenario() -> None:
        async with a2ui_client(relay_paths) as client:
            host = Host(client)
            await host.open()
            drawn.extend(host.surfaces["relay-sites"]["components"].values())
            await host.add()
            drawn.extend(host.surfaces["relay-sites"]["components"].values())

    asyncio.run(scenario())
    assert any(c["component"] == "clio.dialog.v1" for c in drawn)
    for component in drawn:
        declared = _declared(catalog, catalog["components"][component["component"]])
        extra = set(component) - set(declared) - COMMON
        assert not extra, f"{component['id']} uses {extra}, which the catalog lacks"
        for key, value in component.items():
            allowed = declared.get(key, {}).get("enum")
            if allowed is not None:
                assert value in allowed, f"{component['id']}.{key} is {value!r}, not in {allowed}"


def test_the_page_speaks_the_locale_the_request_names(relay_paths: RelayPaths) -> None:
    es = _locale("es")

    async def scenario() -> None:
        async with a2ui_client(relay_paths) as client:
            host = Host(client, locale="es-ES")
            await host.open()
            table = host.table()
            assert table["columns"][0]["title"] == es["Protocol"]
            assert table["empty"] == es["No endpoints yet"]
            await host.add()
            dialog = host.component("add-dialog")
            assert [tab["title"] for tab in dialog["tabs"]] == [
                es["Catalog"],
                es["Saved"],
                es["Your own"],
            ]
            assert host.items("add-site")[-1]["title"] == es["This computer"]
            await host.pick("add-site", "local")
            await host.dialog("add-dialog", "test")
            result = await host.dialog("add-dialog", "confirm")
            assert result.structured_content["ok"] is True, host.text("relay-sites")
            shown = toast(result)
            assert shown is not None
            assert str(shown["message"]).startswith(es["Added {name}"].split("{")[0])
            assert shown["undo"]["label"] == es["Undo"]

        # Another request without a locale is in English again.
        async with a2ui_client(relay_paths) as client:
            host = Host(client)
            await host.open()
            assert host.table()["columns"][0]["title"] == "Protocol"

    asyncio.run(scenario())


def test_a_locale_tag_picks_a_language() -> None:
    assert i18n.language("es-ES") == "es"
    assert i18n.language("ja_JP") == "ja"
    assert i18n.language("EL") == "el"
    assert i18n.language("fr-FR") == "en"
    assert i18n.language(None) == "en"
    token = i18n.use("ja")
    try:
        assert i18n._("Protocol") == _locale("ja")["Protocol"]
        assert i18n._("Not a known string") == "Not a known string"
        assert i18n._("Edit {name}", name="lab") == _locale("ja")["Edit {name}"].format(name="lab")
    finally:
        i18n.reset(token)
    assert i18n._("Protocol") == "Protocol"


@pytest.mark.parametrize("lang", [lang for lang in i18n.LANGUAGES if lang != "en"])
def test_every_language_has_every_string_and_its_placeholders(lang: str) -> None:
    english = list(_locale("es"))
    found = _locale(lang)
    assert list(found) == english, "every language has the same strings, in order"
    named = re.compile(r"\{[a-z_]+\}")
    for text, shown in found.items():
        assert sorted(named.findall(shown)) == sorted(named.findall(text)), text
        assert shown.strip(), text
        assert "\u00b7" not in shown and "\u0387" not in shown, text

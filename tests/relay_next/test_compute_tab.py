"""The Compute tab: listing rules, action tokens, and each action through the site store."""

from __future__ import annotations

import asyncio
import copy
import json
from typing import Any, cast

import mcp_types
import pytest
from fastmcp import Client
from mcp.client.extension import advertise

from clio_relay.adapters import registry
from clio_relay.sites.store import SiteStore
from clio_relay.surface.a2ui import (
    ACTIONS,
    BASIC_CATALOG_ID,
    EXTENSION_ID,
    RELAY_CATALOG_ID,
    TAB_URI,
)
from clio_relay.surface.a2ui.components import COMPONENTS
from clio_relay.surface.a2ui.forms import form
from clio_relay.surface.a2ui.protocol import MIME_TYPE
from clio_relay.surface.a2ui.tab import _sentence  # pyright: ignore[reportPrivateUsage]
from clio_relay.surface.server import create_server
from clio_relay.surface.service import RelayService
from clio_relay.surface.settings import RelayPaths
from clio_relay.testing.fixture_adapter import FixtureAdapter

from .conftest import add_site

VERBS = {"describe", "run", "exec", "files", "serve"}
AD = {
    "versions": ["v0.9.1"],
    "clientCapabilities": {"v0.9.1": {"supportedCatalogIds": [BASIC_CATALOG_ID, RELAY_CATALOG_ID]}},
}


def a2ui_client(paths: RelayPaths) -> Client[Any]:
    return Client(create_server(paths), extensions=[advertise(EXTENSION_ID, AD)])


def _body(contents: object) -> str:
    assert isinstance(contents, mcp_types.TextResourceContents), contents
    return contents.text


def _said(result: Any) -> str:
    first = result.content[0]
    assert isinstance(first, mcp_types.TextContent), first
    return first.text


def _unescape(part: str) -> str:
    return part.replace("~1", "/").replace("~0", "~")


def _get(data: Any, path: str) -> Any:
    for part in [p for p in path.strip("/").split("/") if p]:
        if not isinstance(data, dict):
            return None
        data = cast(dict[str, Any], data).get(_unescape(part))
    return data


def _set(data: dict[str, Any], path: str, value: Any) -> None:
    parts = [_unescape(p) for p in path.strip("/").split("/") if p]
    if not parts:
        data.clear()
        data.update(copy.deepcopy(value))
        return
    for part in parts[:-1]:
        data = data.setdefault(part, {})
    data[parts[-1]] = copy.deepcopy(value)


class Host:
    """Folds A2UI messages the way CLIO does and presses buttons for a test."""

    def __init__(self, client: Client[Any]) -> None:
        self.client = client
        self.surfaces: dict[str, dict[str, Any]] = {}

    def apply(self, messages: list[dict[str, Any]]) -> None:
        for message in messages:
            if "createSurface" in message:
                sid = message["createSurface"]["surfaceId"]
                self.surfaces[sid] = {"components": {}, "data": {}}
            elif "updateComponents" in message:
                body = message["updateComponents"]
                for component in body["components"]:
                    self.surfaces[body["surfaceId"]]["components"][component["id"]] = component
            elif "updateDataModel" in message:
                body = message["updateDataModel"]
                data = self.surfaces[body["surfaceId"]]["data"]
                _set(data, body.get("path", "/"), body.get("value"))

    async def open(self) -> list[dict[str, Any]]:
        contents = await self.client.read_resource(TAB_URI)
        assert len(contents) == 1 and contents[0].mime_type == MIME_TYPE
        messages = json.loads(_body(contents[0]))
        assert "createSurface" in messages[0]
        self.surfaces = {}
        self.apply(messages)
        return messages

    def visible(self, sid: str) -> set[str]:
        """Return the ids reachable from the surface's root."""
        components = self.surfaces[sid]["components"]
        seen: set[str] = set()
        stack = ["root"]
        while stack:
            cid = stack.pop()
            if cid in seen or cid not in components:
                continue
            seen.add(cid)
            component = components[cid]
            stack += component.get("children", [])
            if "child" in component:
                stack.append(component["child"])
            stack += [tab["child"] for tab in component.get("tabs", [])]
        return seen

    def text(self, sid: str) -> str:
        components = self.surfaces[sid]["components"]
        return "\n".join(
            str(components[cid].get("text"))
            for cid in sorted(self.visible(sid))
            if components[cid]["component"] == "Text"
        )

    def failing(self, sid: str) -> dict[str, str]:
        """Return each visible field's failing check message, judged as web_core does."""
        components = self.surfaces[sid]["components"]
        found: dict[str, str] = {}
        for cid in sorted(self.visible(sid)):
            for check in components[cid].get("checks", []):
                condition = check["condition"]
                assert set(condition) == {"path"}, "a literal condition reads as valid"
                if not _get(self.surfaces[sid]["data"], condition["path"]):
                    found[cid] = check["message"]
        return found

    def set(self, sid: str, path: str, value: Any) -> None:
        _set(self.surfaces[sid]["data"], path, value)

    def get(self, sid: str, path: str) -> Any:
        return _get(self.surfaces[sid]["data"], path)

    def action(self, sid: str, button: str) -> tuple[str, dict[str, Any]]:
        assert button in self.visible(sid), f"{button} is not on {sid}"
        event = self.surfaces[sid]["components"][button]["action"]["event"]
        context: dict[str, Any] = {}
        for key, value in cast(dict[str, Any], event["context"]).items():
            bound = cast(dict[str, Any], value) if isinstance(value, dict) else None
            if bound is not None and set(bound) == {"path"}:
                context[key] = _get(self.surfaces[sid]["data"], str(bound["path"]))
            else:
                context[key] = value
        return str(event["name"]), context

    async def press(self, sid: str, button: str) -> Any:
        name, context = self.action(sid, button)
        result = await self.client.call_tool(name, context, raise_on_error=False)
        assert result.content[0].type == "text"
        if not result.is_error:
            embedded = [
                item for item in result.content if isinstance(item, mcp_types.EmbeddedResource)
            ]
            assert len(embedded) == 1
            messages = json.loads(_body(embedded[0].resource))
            assert not any("createSurface" in message for message in messages)
            self.apply(messages)
        return result


def test_plain_clients_see_the_five_verbs_and_no_tab(relay_paths: RelayPaths) -> None:
    async def scenario() -> None:
        for mode in ("auto", "legacy"):
            async with Client(create_server(relay_paths), mode=mode) as client:
                assert {tool.name for tool in await client.list_tools()} == VERBS
                uris = {str(item.uri) for item in await client.list_resources()}
                assert TAB_URI not in uris
                contents = await client.read_resource(TAB_URI)
                assert contents[0].mime_type == "text/plain"
                result = await client.call_tool("compute_probe_site", {}, raise_on_error=False)
                assert result.is_error

    asyncio.run(scenario())


def test_a2ui_clients_see_app_only_tools_that_refuse_without_a_token(
    relay_paths: RelayPaths,
) -> None:
    add_site(relay_paths)

    async def scenario() -> None:
        async with a2ui_client(relay_paths) as client:
            tools = {tool.name: tool for tool in await client.list_tools()}
            assert set(tools) == VERBS | set(ACTIONS)
            for name in ACTIONS:
                assert (tools[name].meta or {}).get("ui", {}).get("visibility") == ["app"]
            for name in VERBS:
                assert "visibility" not in (tools[name].meta or {}).get("ui", {})
            uris = {str(item.uri) for item in await client.list_resources()}
            assert TAB_URI in uris
            for args in ({}, {"token": "made-up"}):
                result = await client.call_tool("compute_probe_site", args, raise_on_error=False)
                assert result.is_error
                assert "out of date" in _said(result)

            host = Host(client)
            await host.open()
            name, context = host.action("relay-sites", "s-fx-probe-button")
            result = await client.call_tool("compute_remove_site", context, raise_on_error=False)
            assert result.is_error, "a token is for one action only"
            again = await client.call_tool(name, context, raise_on_error=False)
            assert again.is_error, "a token misused once is spent"
            await host.open()
            name, context = host.action("relay-sites", "s-fx-probe-button")
            result = await client.call_tool(name, context, raise_on_error=False)
            assert not result.is_error
            again = await client.call_tool(name, context, raise_on_error=False)
            assert again.is_error, "a token is single use"

    asyncio.run(scenario())


def test_notices_keep_the_case_of_names() -> None:
    message = "relay has no OAuth client for the TACC tenant yet; set CLIO_RELAY_TACC_CLIENT_ID"
    assert _sentence(message) == "R" + message[1:]
    assert _sentence("") == ""


def test_a_reread_with_nothing_changed_is_identical(relay_paths: RelayPaths) -> None:
    """The host re-renders a surface whose content changed, which resets a half-filled form."""
    add_site(relay_paths)

    async def scenario() -> None:
        async with a2ui_client(relay_paths) as client:
            host = Host(client)
            first = await host.open()
            assert await host.open() == first
            name, context = host.action("relay-sites", "s-fx-probe-button")
            assert not (await client.call_tool(name, context, raise_on_error=False)).is_error
            await host.open()
            _, fresh = host.action("relay-sites", "s-fx-probe-button")
            assert fresh["token"] != context["token"], "a spent token is replaced"

    asyncio.run(scenario())


def test_a_token_rendered_by_one_relay_is_spent_by_another_once(
    relay_paths: RelayPaths,
) -> None:
    """A host reads the tab over one connection and calls actions over others."""
    add_site(relay_paths)

    async def scenario() -> None:
        async with (
            a2ui_client(relay_paths) as reader,
            a2ui_client(relay_paths) as first,
            a2ui_client(relay_paths) as second,
        ):
            host = Host(reader)
            await host.open()
            name, context = host.action("relay-sites", "s-fx-edit")
            result = await first.call_tool(name, context, raise_on_error=False)
            assert not result.is_error, _said(result)
            again = await second.call_tool(name, context, raise_on_error=False)
            assert again.is_error, "a token is single use across relays"
            # What one relay's action changed, the reader's next read shows.
            await host.open()
            assert "s-fx-edit-divider" in host.visible("relay-sites")
            assert "out of date" in host.text("relay-sites")

    asyncio.run(scenario())


def test_tab_uses_only_the_catalog_components(relay_paths: RelayPaths) -> None:
    add_site(relay_paths, adapter="fixture", config={"sign_in": True})

    async def scenario() -> None:
        async with a2ui_client(relay_paths) as client:
            host = Host(client)
            messages = await host.open()
            created = [m["createSurface"] for m in messages if "createSurface" in m]
            assert {item["surfaceId"] for item in created} == {"relay-sites", "relay-add"}
            assert {item["catalogId"] for item in created} == {RELAY_CATALOG_ID}
            await host.press("relay-sites", "s-fx-edit")
            for surface in host.surfaces.values():
                for component in surface["components"].values():
                    assert component["component"] in COMPONENTS
            # The add form has a tab per installed adapter, each with its config fields.
            # A test-only adapter is not offered, but a site already on one can edit it.
            visible = host.visible("relay-add")
            for name in registry.names():
                if not registry.load(name).listed:
                    assert f"add-a-{name}-add" not in visible
                    continue
                assert f"add-a-{name}-add" in visible
                schema = form(name, registry.config_schema(name))
                for field in schema.fields:
                    assert f"add-a-{name}-f-{field.key}" in visible
            assert host.get("relay-add", "/drafts/add/config/fixture") is None
            assert host.get("relay-sites", "/drafts/fx/adapters/fixture") is not None

    asyncio.run(scenario())


def test_add_edit_switch_probe_and_remove_go_through_the_store(
    relay_paths: RelayPaths, monkeypatch: pytest.MonkeyPatch
) -> None:
    added: list[str] = []
    original = RelayService.site_added

    async def site_added(self: RelayService, site_id: str) -> Any:
        added.append(site_id)
        return await original(self, site_id)

    monkeypatch.setattr(RelayService, "site_added", site_added)
    # The fixture is test-only, so this test lists it to add a site through the form.
    monkeypatch.setattr(FixtureAdapter, "listed", True)
    store = SiteStore(relay_paths.sites_file)

    async def scenario() -> None:
        async with a2ui_client(relay_paths) as client:
            host = Host(client)
            await host.open()
            assert "No sites yet" in host.text("relay-sites")

            # A bad value is shown on its field, not as a tool error or a caption.
            host.set("relay-add", "/drafts/add/site/id", "Bad Id")
            result = await host.press("relay-add", "add-a-fixture-add")
            assert not result.is_error
            assert result.structured_content["ok"] is False
            assert set(host.failing("relay-add")) == {"add-s-f-id"}
            assert host.surfaces["relay-add"]["components"]["add-s-f-id"]["checks"][0][
                "condition"
            ] == {"path": "/drafts/add/valid/site.id"}
            assert host.get("relay-add", "/drafts/add/valid/site.id") is False
            assert "add-s-f-id-error" not in host.visible("relay-add")
            assert store.list() == []

            host.set("relay-add", "/drafts/add/site/id", "")
            await host.press("relay-add", "add-a-fixture-add")
            assert host.failing("relay-add") == {"add-s-f-id": "Enter a site ID"}

            host.set("relay-add", "/drafts/add/site/id", "lab")
            host.set("relay-add", "/drafts/add/site/name", "Lab cluster")
            host.set("relay-add", "/drafts/add/config/fixture/run_s", "0.1")
            result = await host.press("relay-add", "add-a-fixture-add")
            assert result.structured_content["ok"] is True, host.text("relay-add")
            assert host.failing("relay-add") == {}
            assert "checks" not in host.surfaces["relay-add"]["components"]["add-s-f-id"]
            assert host.get("relay-add", "/drafts/add/valid") == {}
            [site] = store.list()
            assert (site.id, site.name, site.adapter) == ("lab", "Lab cluster", "fixture")
            assert site.config["run_s"] == 0.1
            assert site.policy == "guarded"
            assert added == ["lab"]
            assert host.get("relay-add", "/drafts/add/site/id") == ""
            assert host.get("relay-sites", "/switch/lab/adapter") == ["fixture"]
            assert "Added Lab cluster." in _said(result)

            # Adding the same id again is refused on the id field.
            host.set("relay-add", "/drafts/add/site/id", "lab")
            result = await host.press("relay-add", "add-a-fixture-add")
            assert result.structured_content["ok"] is False
            assert host.failing("relay-add") == {"add-s-f-id": "A site named lab exists"}

            await host.press("relay-sites", "s-lab-probe-button")
            assert "Probed Lab cluster." in host.text("relay-sites")
            assert "s-lab-probe" in host.visible("relay-sites")

            await host.press("relay-sites", "s-lab-edit")
            assert "e-lab-save" in host.visible("relay-sites")
            assert host.get("relay-sites", "/drafts/lab/site/name") == "Lab cluster"
            host.set("relay-sites", "/drafts/lab/adapters/fixture/run_s", "soon")
            result = await host.press("relay-sites", "e-lab-save")
            assert result.structured_content["ok"] is False
            assert set(host.failing("relay-sites")) == {"e-lab-a-fixture-f-run_s"}
            assert host.get("relay-sites", "/drafts/lab/valid/adapters.fixture.run_s") is False
            host.set("relay-sites", "/drafts/lab/adapters/fixture/run_s", "0.1")
            host.set("relay-sites", "/drafts/lab/site/queue", "debug")
            host.set("relay-sites", "/drafts/lab/adapters/fixture/queue_s", "0.3")
            host.set("relay-sites", "/drafts/lab/adapters/local/use", True)
            result = await host.press("relay-sites", "e-lab-save")
            assert result.structured_content["ok"] is True, host.text("relay-sites")
            site = store.get("lab")
            assert site.defaults.queue == "debug"
            assert site.config["queue_s"] == 0.3
            assert "local" in site.alternatives
            assert "e-lab-save" not in host.visible("relay-sites")
            assert added == ["lab", "lab"]

            host.set("relay-sites", "/switch/lab/adapter", ["local"])
            result = await host.press("relay-sites", "s-lab-switch")
            assert result.structured_content["ok"] is True, host.text("relay-sites")
            site = store.get("lab")
            assert site.adapter == "local" and "fixture" in site.alternatives
            assert added == ["lab", "lab", "lab"]

            await host.press("relay-sites", "s-lab-remove")
            assert "s-lab-confirm-yes" in host.visible("relay-sites")
            assert store.list() != []
            await host.press("relay-sites", "s-lab-confirm-yes")
            assert store.list() == []
            assert "Removed Lab cluster." in host.text("relay-sites")

            # A fresh read shows the store as the command line left it.
            await host.open()
            assert "No sites yet" in host.text("relay-sites")

    asyncio.run(scenario())


def test_a_site_that_cannot_be_read_leaves_the_others(
    relay_paths: RelayPaths, monkeypatch: pytest.MonkeyPatch
) -> None:
    from clio_relay.surface.a2ui import tab as tab_module

    add_site(relay_paths)
    add_site(relay_paths, "broken")
    real = tab_module.site_status

    async def status(service: RelayService, site: Any) -> dict[str, Any]:
        if site.id == "broken":
            raise RuntimeError("no keyring")
        return await real(service, site)

    async def probe(self: Any, site_id: str, values: dict[str, Any]) -> Any:
        raise RuntimeError("probe blew up")

    monkeypatch.setattr(tab_module, "site_status", status)
    monkeypatch.setattr(tab_module.ComputeTab, "probe_site", probe)

    async def scenario() -> None:
        async with a2ui_client(relay_paths) as client:
            host = Host(client)
            await host.open()
            assert {"s-fx", "s-broken"} <= host.visible("relay-sites")
            assert "Relay could not read this site's state (RuntimeError)" in host.text(
                "relay-sites"
            )
            result = await host.press("relay-sites", "s-fx-probe-button")
            assert not result.is_error
            assert result.structured_content["notice"] == (
                "That did not work: relay hit RuntimeError."
            )
            assert "That did not work: relay hit RuntimeError." in host.text("relay-sites")

    asyncio.run(scenario())


def test_a_removed_site_leaves_the_tab(relay_paths: RelayPaths) -> None:
    async def scenario() -> None:
        async with a2ui_client(relay_paths) as client:
            host = Host(client)
            await host.open()
            add_site(relay_paths, "cli")
            await host.open()
            assert "s-cli" in host.visible("relay-sites")
            SiteStore(relay_paths.sites_file).remove("cli")
            result = await host.press("relay-sites", "s-cli-edit")
            assert result.structured_content["ok"] is False
            assert "s-cli" not in host.visible("relay-sites")
            assert "No site is named 'cli'." in host.text("relay-sites")

    asyncio.run(scenario())


def test_connect_offers_relay_sign_in_page(relay_paths: RelayPaths) -> None:
    add_site(relay_paths, "signs", config={"sign_in": True})

    async def scenario() -> None:
        async with a2ui_client(relay_paths) as client:
            host = Host(client)
            await host.open()
            assert "s-signs-connect" in host.visible("relay-sites"), host.text("relay-sites")
            result = await host.press("relay-sites", "s-signs-connect")
            assert result.structured_content["ok"] is True, host.text("relay-sites")
            link = host.surfaces["relay-sites"]["components"]["s-signs-open"]
            url = link["action"]["functionCall"]["args"]["url"]
            assert url.startswith("http://127.0.0.1:")

    asyncio.run(scenario())

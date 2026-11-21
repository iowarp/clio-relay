"""A test host that folds A2UI messages and acts on them the way CLIO does."""

from __future__ import annotations

import copy
import json
from typing import Any, cast

import mcp_types
from fastmcp import Client
from mcp.client.extension import advertise

from clio_relay.surface.a2ui import BASIC_CATALOG_ID, EXTENSION_ID, RELAY_CATALOG_ID, TAB_URI
from clio_relay.surface.a2ui.components import TABLE
from clio_relay.surface.a2ui.protocol import MIME_TYPE
from clio_relay.surface.server import create_server
from clio_relay.surface.settings import RelayPaths

TOOLS = {
    "describe",
    "run",
    "exec",
    "exec_read",
    "files",
    "files_read",
    "serve",
    "packages",
    "shutdown",
    "cancel",
    "site_clean",
}
# A client that renders relay's catalog gets the endpoint table.
AD = {
    "versions": ["v0.9.1"],
    "clientCapabilities": {"v0.9.1": {"supportedCatalogIds": [BASIC_CATALOG_ID, RELAY_CATALOG_ID]}},
}
# A client that renders only the Basic catalog gets the endpoint cards.
BASIC_AD = {
    "versions": ["v0.9.1"],
    "clientCapabilities": {"v0.9.1": {"supportedCatalogIds": [BASIC_CATALOG_ID]}},
}


def a2ui_client(paths: RelayPaths, ad: dict[str, Any] | None = None) -> Client[Any]:
    return Client(create_server(paths), extensions=[advertise(EXTENSION_ID, ad or AD)])


def basic_client(paths: RelayPaths) -> Client[Any]:
    return a2ui_client(paths, BASIC_AD)


def body(contents: object) -> str:
    assert isinstance(contents, mcp_types.TextResourceContents), contents
    return contents.text


def said(result: Any) -> str:
    first = result.content[0]
    assert isinstance(first, mcp_types.TextContent), first
    return first.text


def _unescape(part: str) -> str:
    return part.replace("~1", "/").replace("~0", "~")


def get(data: Any, path: str) -> Any:
    for part in [p for p in path.strip("/").split("/") if p]:
        if not isinstance(data, dict):
            return None
        data = cast(dict[str, Any], data).get(_unescape(part))
    return data


def put(data: dict[str, Any], path: str, value: Any) -> None:
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
                found = message["updateComponents"]
                for component in found["components"]:
                    self.surfaces[found["surfaceId"]]["components"][component["id"]] = component
            elif "updateDataModel" in message:
                found = message["updateDataModel"]
                data = self.surfaces[found["surfaceId"]]["data"]
                put(data, found.get("path", "/"), found.get("value"))

    async def open(self) -> list[dict[str, Any]]:
        contents = await self.client.read_resource(TAB_URI)
        assert len(contents) == 1 and contents[0].mime_type == MIME_TYPE
        messages = json.loads(body(contents[0]))
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
                if not get(self.surfaces[sid]["data"], condition["path"]):
                    found[cid] = check["message"]
        return found

    def set(self, sid: str, path: str, value: Any) -> None:
        put(self.surfaces[sid]["data"], path, value)

    def get(self, sid: str, path: str) -> Any:
        return get(self.surfaces[sid]["data"], path)

    def _context(self, sid: str, event: dict[str, Any]) -> dict[str, Any]:
        context: dict[str, Any] = {}
        for key, value in cast(dict[str, Any], event.get("context", {})).items():
            bound = cast(dict[str, Any], value) if isinstance(value, dict) else None
            if bound is not None and set(bound) == {"path"}:
                context[key] = get(self.surfaces[sid]["data"], str(bound["path"]))
            else:
                context[key] = value
        return context

    def action(self, sid: str, button: str) -> tuple[str, dict[str, Any]]:
        assert button in self.visible(sid), f"{button} is not on {sid}"
        event = self.surfaces[sid]["components"][button]["action"]["event"]
        return str(event["name"]), self._context(sid, event)

    async def call(self, name: str, context: dict[str, Any]) -> Any:
        result = await self.client.call_tool(name, context, raise_on_error=False)
        assert result.content[0].type == "text"
        if not result.is_error:
            embedded = [
                item for item in result.content if isinstance(item, mcp_types.EmbeddedResource)
            ]
            assert len(embedded) == 1
            messages = json.loads(body(embedded[0].resource))
            assert not any("createSurface" in message for message in messages)
            self.apply(messages)
        return result

    async def press(self, sid: str, button: str) -> Any:
        return await self.call(*self.action(sid, button))

    # The endpoint table, as CLIO's choice-table renderer drives it

    def table(self, sid: str = "relay-sites", table: str = "endpoints") -> dict[str, Any]:
        assert table in self.visible(sid), f"{table} is not on {sid}"
        component = self.surfaces[sid]["components"][table]
        assert component["component"] == TABLE
        return component

    def row(self, row_id: str, sid: str = "relay-sites") -> dict[str, Any]:
        rows = {row["id"]: row for row in self.table(sid)["rows"]}
        assert row_id in rows, f"no row {row_id}"
        return rows[row_id]

    def choice(
        self, row_id: str, column: str, value: str, sid: str = "relay-sites"
    ) -> tuple[str, dict[str, Any]]:
        """Return the call a pick makes: the event's context, the row's, then the pick."""
        component = self.table(sid)
        event = component["onChoose"]["event"]
        owner = component["defaults"] if row_id == "" else self.row(row_id, sid)
        context = self._context(sid, event) | owner.get("context", {})
        return str(event["name"]), context | {"row": row_id, "column": column, "value": value}

    async def choose(self, row_id: str, column: str, value: str, sid: str = "relay-sites") -> Any:
        return await self.call(*self.choice(row_id, column, value, sid))

    def menu_item(
        self, row_id: str, item: str, sid: str = "relay-sites"
    ) -> tuple[str, dict[str, Any]]:
        items = {entry["id"]: entry for entry in self.row(row_id, sid).get("menu", [])}
        assert item in items, f"{item} is not in {row_id}'s menu"
        event = items[item]["action"]["event"]
        return str(event["name"]), self._context(sid, event) | {"row": row_id}

    async def menu(self, row_id: str, item: str, sid: str = "relay-sites") -> Any:
        return await self.call(*self.menu_item(row_id, item, sid))

    async def add(self, sid: str = "relay-sites") -> Any:
        event = self.table(sid)["add"]["action"]["event"]
        return await self.call(str(event["name"]), self._context(sid, event))

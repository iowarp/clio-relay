"""The Compute page's change notifications: one per change, none for a reread."""

from __future__ import annotations

import asyncio
from typing import Any

import mcp_types
from fastmcp import Client
from mcp.client.extension import advertise

from clio_relay.surface.a2ui import EXTENSION_ID, TAB_URI
from clio_relay.surface.a2ui.tab import WATCH_INTERVAL_S
from clio_relay.surface.server import create_server
from clio_relay.surface.settings import RelayPaths

from .conftest import add_site
from .a2ui_host import AD, Host


def test_changes_from_the_command_line_refresh_the_page(relay_paths: RelayPaths) -> None:
    heard: asyncio.Queue[str] = asyncio.Queue()

    async def on_message(message: Any) -> None:
        if isinstance(message, mcp_types.ResourceUpdatedNotification):
            await heard.put(str(message.params.uri))

    async def scenario() -> None:
        async with Client(
            create_server(relay_paths),
            mode="legacy",
            extensions=[advertise(EXTENSION_ID, AD)],
            message_handler=on_message,
        ) as client:
            host = Host(client)
            await host.open()
            add_site(relay_paths, "cli")
            async with asyncio.timeout(15):
                assert await heard.get() == TAB_URI
            await host.open()
            assert host.row("cli")["id"] == "cli"
            # Rereading the page, as a host does on the notification, is not a second change.
            await asyncio.sleep(WATCH_INTERVAL_S * 2.5)
            assert heard.empty()

    asyncio.run(scenario())


def test_an_out_of_date_button_refreshes_the_page_once(relay_paths: RelayPaths) -> None:
    add_site(relay_paths)
    heard: list[str] = []

    async def on_message(message: Any) -> None:
        if isinstance(message, mcp_types.ResourceUpdatedNotification):
            heard.append(str(message.params.uri))

    async def scenario() -> None:
        async with Client(
            create_server(relay_paths),
            mode="legacy",
            extensions=[advertise(EXTENSION_ID, AD)],
            message_handler=on_message,
        ) as client:
            await Host(client).open()
            result = await client.call_tool(
                "compute_probe_site", {"token": "made-up"}, raise_on_error=False
            )
            assert result.is_error
            # The watcher looks at the state file this refusal wrote and must stay quiet.
            await asyncio.sleep(WATCH_INTERVAL_S * 2.5)
            assert heard == [TAB_URI]

    asyncio.run(scenario())

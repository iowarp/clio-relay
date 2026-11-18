"""Change events reach a client on both protocol eras (HS3 server obligation)."""

from __future__ import annotations

import asyncio
from typing import Any

import mcp_types
import pytest
from fastmcp import FastMCP
from mcp import Client
from mcp.server.connection import Connection
from mcp.shared.subscriptions import ResourcesListChanged, ResourceUpdated

from clio_relay.surface.server import RelayServer
from clio_relay.surface.service import RelayService
from clio_relay.surface.settings import RelayPaths

URI = "relay://test/compute"


def _relay(paths: RelayPaths) -> tuple[RelayServer, FastMCP[Any]]:
    relay = RelayServer(RelayService(paths))
    server = relay.build()

    def compute() -> str:
        return "{}"

    server.resource(URI)(compute)
    return relay, server


def test_listen_stream_carries_resource_changes(relay_paths: RelayPaths) -> None:
    relay, server = _relay(relay_paths)

    async def scenario() -> None:
        async with Client(server._mcp_server, mode="auto") as client:  # pyright: ignore[reportPrivateUsage]
            assert client.protocol_version == "2026-07-28"
            async with client.listen(
                resource_subscriptions=[URI], resources_list_changed=True
            ) as subscription:
                await relay.changes.resource_updated(URI)
                await relay.changes.resource_updated("relay://test/other")
                await relay.changes.resources_list_changed()
                events: list[object] = []
                async with asyncio.timeout(10):
                    async for event in subscription:
                        events.append(event)
                        if len(events) == 2:
                            break
            assert isinstance(events[0], ResourceUpdated)
            assert events[0].uri == URI
            assert isinstance(events[1], ResourcesListChanged)

    asyncio.run(scenario())


def test_legacy_session_gets_the_bare_notification(relay_paths: RelayPaths) -> None:
    relay, server = _relay(relay_paths)
    heard: asyncio.Queue[str] = asyncio.Queue()

    async def on_message(message: Any) -> None:
        if isinstance(message, mcp_types.ResourceUpdatedNotification):
            await heard.put(str(message.params.uri))

    async def scenario() -> None:
        async with Client(
            server._mcp_server,  # pyright: ignore[reportPrivateUsage]
            mode="legacy",
            message_handler=on_message,
        ) as client:
            assert client.protocol_version != "2026-07-28"
            await client.list_tools()
            await relay.changes.resource_updated(URI)
            async with asyncio.timeout(10):
                assert await heard.get() == URI

    asyncio.run(scenario())


def test_a_listening_legacy_session_hears_each_change_once(
    relay_paths: RelayPaths, monkeypatch: pytest.MonkeyPatch
) -> None:
    relay, server = _relay(relay_paths)
    bare: list[str] = []
    send = Connection.send_resource_updated

    async def counted(self: Connection, uri: str, **kwargs: Any) -> None:
        bare.append(uri)
        await send(self, uri, **kwargs)

    monkeypatch.setattr(Connection, "send_resource_updated", counted)

    async def scenario() -> None:
        async with Client(server._mcp_server, mode="legacy") as client:  # pyright: ignore[reportPrivateUsage]
            await client.list_tools()
            async with client.listen(resource_subscriptions=[URI]) as subscription:
                await client.list_tools()
                await relay.changes.resource_updated(URI)
                async with asyncio.timeout(10):
                    event = await anext(aiter(subscription))
                assert isinstance(event, ResourceUpdated)
                assert bare == [], "the listen stream already carried it"
            # With the stream closed, the next request makes it a bare listener again.
            await client.list_tools()
            await relay.changes.resource_updated(URI)
            assert bare == [URI]

    asyncio.run(scenario())

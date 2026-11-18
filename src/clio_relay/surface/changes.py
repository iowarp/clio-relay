"""Tell clients what changed, on every protocol relay speaks.

At ``2026-07-28`` a bare ``notifications/resources/updated`` or ``list_changed``
no longer reaches a client: change events travel only on a
``subscriptions/listen`` stream, which FastMCP does not serve. Relay registers
the SDK's listen handler itself and publishes each change on its bus. Older
clients still get the bare notification on the connections relay has seen.
"""

from __future__ import annotations

import logging
import weakref
from typing import Any

import mcp_types
from fastmcp import FastMCP
from fastmcp.server.middleware import CallNext, Middleware, MiddlewareContext
from mcp.server.connection import Connection
from mcp.server.session import ServerSession
from mcp.server.subscriptions import InMemorySubscriptionBus, ListenHandler
from mcp.shared.subscriptions import (
    ResourcesListChanged,
    ResourceUpdated,
    ServerEvent,
    ToolsListChanged,
)
from mcp_types.version import MODERN_PROTOCOL_VERSIONS

logger = logging.getLogger(__name__)

LISTEN_METHOD = "subscriptions/listen"


class ServerChanges:
    """The one place relay announces a changed resource, resource list or tool list.

    Producers (the Compute tab resource, site skills, mounted tools) call
    :meth:`resource_updated`, :meth:`resources_list_changed` or
    :meth:`tools_list_changed`. A ``2026-07-28`` client hears it on its listen
    stream; a ``2025-11-25`` client gets the bare notification.
    """

    def __init__(self) -> None:
        self.bus = InMemorySubscriptionBus()
        self.listen = ListenHandler(self.bus)
        self._legacy: weakref.WeakSet[Connection] = weakref.WeakSet()

    def install(self, server: FastMCP[Any]) -> None:
        """Serve ``subscriptions/listen`` on ``server`` and remember legacy connections."""
        server._mcp_server.add_request_handler(  # pyright: ignore[reportPrivateUsage]
            LISTEN_METHOD, mcp_types.SubscriptionsListenRequestParams, self.listen
        )
        server.add_middleware(_SessionRecorder(self))

    def remember(self, session: ServerSession) -> None:
        """Keep the session's connection if it predates listen streams."""
        connection: Connection = session._connection  # pyright: ignore[reportPrivateUsage]
        if connection.protocol_version in MODERN_PROTOCOL_VERSIONS:
            return
        if connection.has_standalone_channel:
            self._legacy.add(connection)

    async def resource_updated(self, uri: str) -> None:
        await self._publish(ResourceUpdated(uri=uri))

    async def resources_list_changed(self) -> None:
        await self._publish(ResourcesListChanged())

    async def tools_list_changed(self) -> None:
        await self._publish(ToolsListChanged())

    def close(self) -> None:
        """End every open listen stream gracefully."""
        self.listen.close()

    async def _publish(self, event: ServerEvent) -> None:
        await self.bus.publish(event)
        for connection in list(self._legacy):
            try:
                if isinstance(event, ResourceUpdated):
                    await connection.send_resource_updated(event.uri)
                elif isinstance(event, ResourcesListChanged):
                    await connection.send_resource_list_changed()
                elif isinstance(event, ToolsListChanged):
                    await connection.send_tool_list_changed()
            except Exception:  # a closed connection must not stop the others hearing it
                logger.debug("dropping a connection that could not take a change notification")
                self._legacy.discard(connection)


class _SessionRecorder(Middleware):
    def __init__(self, changes: ServerChanges) -> None:
        self._changes = changes

    async def on_request(
        self, context: MiddlewareContext[Any], call_next: CallNext[Any, Any]
    ) -> Any:
        ctx = context.fastmcp_context
        if ctx is not None:
            try:
                session = ctx.session
            except RuntimeError:
                session = None
            if isinstance(session, ServerSession):
                self._changes.remember(session)
        return await call_next(context)

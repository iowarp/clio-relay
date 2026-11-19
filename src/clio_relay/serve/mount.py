"""Served MCP servers, mounted on relay's own server (RL21).

Relay holds one client to each served MCP server for the life of the service
and mounts its tools as ``<site>_<service>_<tool>``. Metadata, including MCP
Apps metadata, passes through unchanged. Resources and resource templates keep
their own URIs, so a ``ui://`` resource a tool names still resolves; when two
services offer the same URI, the first mounted wins.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import AsyncGenerator, Callable, Coroutine, Sequence
from dataclasses import dataclass, field
from typing import Any

from fastmcp import Client, FastMCP
from fastmcp.resources import Resource, ResourceTemplate
from fastmcp.server.providers import Provider
from fastmcp.server.providers.proxy import ProxyResource, ProxyTemplate, ProxyTool
from fastmcp.tools import Tool
from fastmcp.utilities.versions import VersionSpec

logger = logging.getLogger(__name__)


def tool_prefix(site: str, service: str) -> str:
    """Return the prefix of a mounted server's tools."""
    return f"{site}_{service}_"


@dataclass
class Mount:
    """One mounted server and the client relay holds to it."""

    operation_id: str
    prefix: str
    url: str
    client: Client[Any]
    tools: list[Tool] = field(default_factory=list[Tool])
    resources: list[Resource] = field(default_factory=list[Resource])
    templates: list[ResourceTemplate] = field(default_factory=list[ResourceTemplate])


class MountProvider(Provider):
    """Supplies every mounted server's tools and resources to relay's server."""

    def __init__(self) -> None:
        super().__init__()
        self.mounts: dict[str, Mount] = {}
        self.on_start: Callable[[], Coroutine[Any, Any, None]] | None = None

    @contextlib.asynccontextmanager
    async def lifespan(self) -> AsyncGenerator[None]:
        """Run the startup work, such as re-binding ports, when relay's server starts."""
        task: asyncio.Task[None] | None = None
        if self.on_start is not None:
            task = asyncio.get_running_loop().create_task(self.on_start())
        try:
            yield
        finally:
            if task is not None and not task.done():
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task

    async def _list_tools(self) -> Sequence[Tool]:
        return [tool for mount in self.mounts.values() for tool in mount.tools]

    async def _get_tool(self, name: str, version: VersionSpec | None = None) -> Tool | None:
        for mount in self.mounts.values():
            for tool in mount.tools:
                if tool.name == name:
                    return tool
        return None

    async def _list_resources(self) -> Sequence[Resource]:
        seen: set[str] = set()
        found: list[Resource] = []
        for mount in self.mounts.values():
            for resource in mount.resources:
                if str(resource.uri) not in seen:
                    seen.add(str(resource.uri))
                    found.append(resource)
        return found

    async def _get_resource(self, uri: str, version: VersionSpec | None = None) -> Resource | None:
        for resource in await self._list_resources():
            if str(resource.uri) == uri:
                return resource
        return None

    async def _list_resource_templates(self) -> Sequence[ResourceTemplate]:
        return [template for mount in self.mounts.values() for template in mount.templates]

    async def _get_resource_template(
        self, uri: str, version: VersionSpec | None = None
    ) -> ResourceTemplate | None:
        for template in await self._list_resource_templates():
            if template.matches(uri) is not None:
                return template
        return None


class Mounter:
    """Opens, holds and closes the clients behind mounted servers."""

    def __init__(self) -> None:
        self.provider = MountProvider()
        self.server: FastMCP[Any] | None = None
        self.changes: Any = None

    def attach(self, server: FastMCP[Any], changes: Any) -> None:
        """Add the mount provider to relay's server once."""
        if self.server is server:
            return
        server.add_provider(self.provider)
        self.server = server
        self.changes = changes

    def mounted(self, operation_id: str) -> Mount | None:
        """Return the mount of one operation."""
        return self.provider.mounts.get(operation_id)

    async def mount(
        self, operation_id: str, prefix: str, url: str, *, secret: str | None = None
    ) -> Mount:
        """Open one held client to ``url`` and mount what it offers."""
        existing = self.provider.mounts.get(operation_id)
        if existing is not None:
            return existing
        taken = next((m for m in self.provider.mounts.values() if m.prefix == prefix), None)
        if taken is not None:
            raise ValueError(f"{prefix}* is already mounted for {taken.operation_id}")
        client: Client[Any] = Client(url, auth=secret) if secret else Client(url)
        await client.__aenter__()
        try:
            listed = await client.list_tools()

            def factory() -> Client[Any]:
                return client

            # fastmcp leaves the client type parameter open in these signatures.
            proxy_tool: Any = ProxyTool
            proxy_resource: Any = ProxyResource
            proxy_template: Any = ProxyTemplate
            tools: list[Tool] = [
                proxy_tool.from_mcp_tool(factory, tool).model_copy(
                    update={"name": f"{prefix}{tool.name}"}
                )
                for tool in listed
            ]
            resources: list[Resource] = []
            templates: list[ResourceTemplate] = []
            with contextlib.suppress(Exception):
                resources = [
                    proxy_resource.from_mcp_resource(factory, item)
                    for item in await client.list_resources()
                ]
            with contextlib.suppress(Exception):
                templates = [
                    proxy_template.from_mcp_template(factory, item)
                    for item in await client.list_resource_templates()
                ]
        except BaseException:
            with contextlib.suppress(Exception):
                await _close(client)
            raise
        mount = Mount(operation_id, prefix, url, client, tools, resources, templates)
        self.provider.mounts[operation_id] = mount
        await self._announce(resources=bool(resources or templates))
        return mount

    async def unmount(self, operation_id: str) -> None:
        """Close the held client and take the tools away."""
        mount = self.provider.mounts.pop(operation_id, None)
        if mount is None:
            return
        with contextlib.suppress(Exception):
            await _close(mount.client)
        await self._announce(resources=bool(mount.resources or mount.templates))

    async def _announce(self, *, resources: bool) -> None:
        if self.changes is None:
            return
        try:
            await self.changes.tools_list_changed()
            if resources:
                await self.changes.resources_list_changed()
        except Exception:
            logger.debug("could not announce a changed tool list", exc_info=True)

    async def close(self) -> None:
        """Close every held client."""
        for operation_id in list(self.provider.mounts):
            await self.unmount(operation_id)


async def _close(client: Any) -> None:
    await client.__aexit__(None, None, None)

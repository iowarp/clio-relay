"""FastMCP tool/provider wiring: ``RelayTool`` and ``RelayToolProvider``.

Extracted from ``fastmcp_server.py`` (clio-relay#231 decomposition): the two
FastMCP components exposing the relay's static and dynamic (remote MCP)
catalog as native FastMCP tools -- ``RelayTool`` delegates one call to
``RelayMcpRuntime.call_tool`` instead of running a second tool runtime;
``RelayToolProvider`` lists/resolves the complete catalog each dispatch,
observing the served catalog revision into the compatibility session cache
so a later call can assert it hasn't rotated underneath the caller.

**Patch-seam note.** ``mcp_tool_definitions_and_remote_catalog`` is looked up
through the ``clio_relay.fastmcp_server`` facade module object via a
function-local import at its one call site (``_definitions_with_revision``),
for the same reason ``mcp_task_runtime.py``'s own docstring documents for
``call_mcp_tool``/``status_mcp_job``/``wait_mcp_job``: the existing test
suite patches this name against the facade module object, and a bare
``from ... import`` binding here would leave that patch silently dead.
"""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from datetime import timedelta
from typing import Any, cast

import mcp_types
from fastmcp.server.providers import Provider
from fastmcp.tools import Tool, ToolResult
from fastmcp.utilities.tasks import TaskConfig
from fastmcp.utilities.versions import VersionSpec
from pydantic import PrivateAttr

from clio_relay.jarvis_mcp import is_virtual_jarvis_tool
from clio_relay.mcp_agent_input_guard import _AGENT_TASK_TOOL_NAMES
from clio_relay.mcp_server import static_mcp_tool_names
from clio_relay.mcp_session_state_codec import _load_session, _save_session
from clio_relay.mcp_task_projection import TASK_POLL_INTERVAL_MS
from clio_relay.mcp_task_runtime import RelayMcpRuntime

JSON = dict[str, Any]


class RelayTool(Tool):
    """FastMCP component delegating to one existing relay MCP tool definition."""

    _runtime: RelayMcpRuntime = PrivateAttr()
    _catalog_revision: str | None = PrivateAttr(default=None)

    def __init__(
        self,
        definition: JSON,
        *,
        runtime: RelayMcpRuntime,
        catalog_revision: str | None,
        task_capable: bool,
    ) -> None:
        meta = dict(cast(dict[str, Any], definition.get("_meta") or {}))
        if catalog_revision is not None:
            meta["clio-relay/catalog-revision"] = catalog_revision
        raw_annotations = definition.get("annotations")
        annotations = (
            None
            if raw_annotations is None
            else mcp_types.ToolAnnotations.model_validate(raw_annotations)
        )
        super().__init__(
            name=cast(str, definition["name"]),
            title=cast(str | None, definition.get("title")),
            description=cast(str | None, definition.get("description")),
            parameters=cast(JSON, definition["inputSchema"]),
            output_schema=cast(JSON | None, definition.get("outputSchema")),
            annotations=annotations,
            meta=meta or None,
            task_config=TaskConfig(
                mode="optional" if task_capable else "forbidden",
                poll_interval=timedelta(milliseconds=TASK_POLL_INTERVAL_MS),
            ),
        )
        self._runtime = runtime
        self._catalog_revision = catalog_revision

    @property
    def catalog_revision(self) -> str | None:
        """Return the exact dynamic catalog revision bound at dispatch."""
        return self._catalog_revision

    async def run(self, arguments: dict[str, Any]) -> ToolResult:
        """Execute the established relay dispatcher without a second tool runtime."""
        return await self._runtime.call_tool(
            name=self.name,
            arguments=arguments,
            catalog_revision=self._catalog_revision,
        )


class RelayToolProvider(Provider):
    """Expose the complete static and dynamic relay catalog through FastMCP."""

    def __init__(self, runtime: RelayMcpRuntime) -> None:
        super().__init__()
        self._runtime = runtime

    async def _definitions(self) -> tuple[list[JSON], str]:
        return await asyncio.to_thread(lambda: _definitions_with_revision(self._runtime.profile))

    async def _list_tools(self) -> Sequence[Tool]:
        definitions, revision = await self._definitions()
        session = await _load_session()
        session.observe_remote_mcp_catalog(
            profile=self._runtime.profile,
            revision=revision,
        )
        await _save_session(session)
        static_names = static_mcp_tool_names()
        return [
            RelayTool(
                definition,
                runtime=self._runtime,
                catalog_revision=(
                    None if cast(str, definition["name"]) in static_names else revision
                ),
                task_capable=_task_capable_tool_name(
                    cast(str, definition["name"]),
                    static_names,
                ),
            )
            for definition in definitions
        ]

    async def _get_tool(
        self,
        name: str,
        version: VersionSpec | None = None,
    ) -> Tool | None:
        if version is not None:
            return None
        definitions, revision = await self._definitions()
        static_names = static_mcp_tool_names()
        for definition in definitions:
            if definition.get("name") != name:
                continue
            return RelayTool(
                definition,
                runtime=self._runtime,
                catalog_revision=None if name in static_names else revision,
                task_capable=_task_capable_tool_name(name, static_names),
            )
        return None


def _task_capable_tool_name(name: str, static_names: set[str]) -> bool:
    """Return whether one admitted-job tool supports SEP-2663 task projection."""
    return (
        name not in static_names
        or name == "relay_call_jarvis_mcp"
        or name in _AGENT_TASK_TOOL_NAMES
        or is_virtual_jarvis_tool(name)
    )


def _definitions_with_revision(profile: str) -> tuple[list[JSON], str]:
    import clio_relay.fastmcp_server as fastmcp_server

    definitions, catalog = fastmcp_server.mcp_tool_definitions_and_remote_catalog(profile=profile)
    return definitions, catalog.revision

"""MCP tool catalog: assembles the static JSON-schema tool definitions the
relay stdio MCP server advertises via ``tools/list`` out of four
tool-domain owner modules, plus the small pure functions derived from the
assembled catalog (built-in tool-name sets, per-profile authorization
filtering).

Split out of mcp_server.py (iowarp/clio-relay#231) -- ``_all_tool_definitions``
alone was 1,101 lines, one function returning a literal list of JSON-schema
dicts for every relay MCP tool (the doc's own §4.5/§5 catalog/dispatch row).
That first pass left this module itself at 1,267 lines, over the 800-line
ratchet cap -- a real seam split, not a docstring justification, per the
split recipe. The seam: four tool-domain groups (job lifecycle, monitoring,
queue/retention, gateway session), each in its own leaf module
(``mcp_tool_catalog_job_lifecycle.py`` / ``_monitoring.py`` /
``_queue_retention.py`` / ``_gateway_session.py``), assembled here in the
original catalog's exact literal order -- no tool schema was reordered,
reworded, or otherwise changed, only relocated.

This module and its four group modules are all leaves with respect to
mcp_server.py: pure static data plus pure functions over that data, no
dependency on live server/session state (cluster membership, remote
catalogs, in-flight jobs). mcp_server.py imports from here; nothing here
imports mcp_server, so there is no back-reference and no load-order cycle.

``USER_MCP_TOOL_NAMES``, ``static_mcp_tool_names``, and
``_authorized_static_tool_names`` stay here (not in a group module) since
they are pure functions of the *assembled* catalog, not any one group.
``clio_relay.mcp_server`` re-exports the public names it inherited call
sites for (``USER_MCP_TOOL_NAMES``, ``static_mcp_tool_names``) so
``cli.py`` / ``fastmcp_server.py`` / ``mcp_stdio_validation.py`` keep
importing them from ``clio_relay.mcp_server`` unchanged.
"""

from __future__ import annotations

from typing import Any

from clio_relay.jarvis_mcp import is_virtual_jarvis_tool, virtual_jarvis_tool_definitions
from clio_relay.mcp_tool_catalog_gateway_session import _gateway_session_tool_definitions
from clio_relay.mcp_tool_catalog_job_lifecycle import (
    MAX_AGENT_LOG_READ_BYTES,
    _job_lifecycle_tool_definitions,
)
from clio_relay.mcp_tool_catalog_monitoring import _monitoring_and_artifact_tool_definitions
from clio_relay.mcp_tool_catalog_queue_retention import _queue_and_retention_tool_definitions

JSON = dict[str, Any]

__all__ = [
    "MAX_AGENT_LOG_READ_BYTES",
    "USER_MCP_TOOL_NAMES",
    "_all_tool_definitions",
    "_authorized_static_tool_names",
    "static_mcp_tool_names",
]

USER_MCP_TOOL_NAMES = {
    "relay_remote_mcp_context",
    "relay_submit_agent",
    "relay_status",
    "relay_cancel",
    "relay_observe",
    "relay_wait",
    "relay_queue_list",
    "relay_queue_diagnose",
    "relay_queue_stale",
    "relay_storage_status",
    "relay_bind_jarvis_runtime",
    "relay_artifact_lineage",
    "relay_list_artifacts",
    "relay_read_artifact",
}


def static_mcp_tool_names() -> set[str]:
    """Return built-in local tool names reserved from generated aliases."""
    return {str(tool["name"]) for tool in _all_tool_definitions()}


def _authorized_static_tool_names(profile: str) -> set[str]:
    """Return built-in tools callable through one normalized MCP profile.

    MCP clients are not required to call ``tools/list`` before ``tools/call``.
    Authorization therefore belongs at dispatch time rather than only in the
    discovery response. Remote aliases are authorized separately from their
    profile-filtered catalog so a corrupt cache cannot block static safety tools.
    """
    all_static = static_mcp_tool_names()
    if profile in {"admin", "operator", "all"}:
        return all_static
    return {
        name for name in all_static if name in USER_MCP_TOOL_NAMES or is_virtual_jarvis_tool(name)
    }


def _all_tool_definitions(
    *,
    clusters: list[str] | None = None,
    jarvis_clusters: list[str] | None = None,
) -> list[JSON]:
    """Return static tools with independent generic and built-in JARVIS routes."""
    resolved_jarvis_clusters = clusters if jarvis_clusters is None else jarvis_clusters
    return [
        *_job_lifecycle_tool_definitions(),
        *_monitoring_and_artifact_tool_definitions(),
        *_queue_and_retention_tool_definitions(),
        *_gateway_session_tool_definitions(clusters=clusters),
        *virtual_jarvis_tool_definitions(clusters=resolved_jarvis_clusters),
    ]

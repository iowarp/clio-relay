"""FastMCP context (de)serialization for the compatibility MCP session cache.

Extracted from ``fastmcp_server.py`` (clio-relay#231 decomposition): the
codec round-tripping ``McpSessionState`` (the pre-existing relay/mcp_server.py
remote-catalog-revision + remote-job-route cache) through one FastMCP request
context's own session state store. Depends only on ``mcp_server.py``'s
``McpSessionState`` model and the FastMCP context accessor -- no relay
queue/storage dependency -- so both ``mcp_task_runtime.py`` and
``mcp_tool_provider.py`` depend on this module, never the other way.
"""

from __future__ import annotations

from typing import Any, cast

from fastmcp.server.dependencies import get_context

from clio_relay.mcp_server import McpSessionState

JSON = dict[str, Any]

SESSION_STATE_KEY = "clio-relay/mcp-session-state"


def _session_to_json(session: McpSessionState) -> JSON:
    """Serialize the compatibility session cache into FastMCP session state."""
    return {
        "remote_mcp_catalog_revisions": dict(session.remote_mcp_catalog_revisions),
        "remote_job_routes": {
            job_id: [list(route) for route in sorted(routes)]
            for job_id, routes in session.remote_job_routes.items()
        },
    }


def _session_from_json(value: object) -> McpSessionState:
    """Restore a validated compatibility session cache."""
    session = McpSessionState()
    if value is None:
        return session
    if not isinstance(value, dict):
        raise ValueError("stored MCP session state is not an object")
    document = cast(JSON, value)
    revisions_value = document.get("remote_mcp_catalog_revisions", {})
    routes_value = document.get("remote_job_routes", {})
    if not isinstance(revisions_value, dict) or not isinstance(routes_value, dict):
        raise ValueError("stored MCP session state contains invalid collections")
    revisions = cast(dict[str, object], revisions_value)
    routes = cast(dict[str, object], routes_value)
    session.remote_mcp_catalog_revisions = {
        str(key): str(revision) for key, revision in revisions.items()
    }
    restored_routes: dict[str, set[tuple[str, str]]] = {}
    for job_id, raw_routes in routes.items():
        if not isinstance(raw_routes, list):
            raise ValueError("stored MCP job routes are not a list")
        route_items = cast(list[object], raw_routes)
        parsed_routes: set[tuple[str, str]] = set()
        for route in route_items:
            if not isinstance(route, list):
                raise ValueError("stored MCP job route is malformed")
            route_parts = cast(list[object], route)
            if len(route_parts) != 2:
                raise ValueError("stored MCP job route is malformed")
            parsed_routes.add((str(route_parts[0]), str(route_parts[1])))
        restored_routes[str(job_id)] = parsed_routes
        if len(restored_routes[str(job_id)]) != len(route_items):
            raise ValueError("stored MCP job route is malformed")
    session.remote_job_routes = restored_routes
    return session


async def _load_session() -> McpSessionState:
    context = get_context()
    return _session_from_json(await context.get_state(SESSION_STATE_KEY))


async def _save_session(session: McpSessionState) -> None:
    context = get_context()
    await context.set_state(SESSION_STATE_KEY, _session_to_json(session))

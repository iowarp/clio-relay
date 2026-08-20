"""Remote-MCP-catalog resolution and MCP-profile normalization.

Split out of mcp_server.py (iowarp/clio-relay#231): the cluster that
resolves the live, cache-backed virtual remote MCP catalog for one relay
cluster registry (`_remote_mcp_catalog`), the stable local cluster-name
list (`_configured_cluster_names`), MCP profile normalization
(`_normalize_profile`/`_mcp_profile_from_env`), the assembled
static-plus-remote tool list for one profile
(`_tool_definitions_and_remote_catalog`), and two small validators
(`_require_compatible_remote_mcp_catalog`, `_validated_route_revision`,
`_route_revision`).

A clean leaf: none of these 9 functions call back into any mcp_server.py-only
business function (confirmed by grep before the move), only each other and
already-safe external/leaf imports. `_remote_mcp_catalog` and
`_configured_cluster_names` are directly monkeypatched by tests at
`mcp_server_module.<name>`, and `_route_revision` alone has 30+ bare call
sites across functions that stay in mcp_server.py -- mcp_server.py imports
every name in this module back into its own namespace so all of that keeps
resolving unchanged, the same "re-exported names resolve identically to
locally-defined ones" rule the tool-catalog slices already relied on.
"""

from __future__ import annotations

import os
import re
from datetime import UTC, datetime
from typing import Any

from pydantic import ValidationError

from clio_relay.cluster_config import (
    ClusterDefinition,
    ClusterRegistry,
    cluster_route_revision,
    default_registry_path,
)
from clio_relay.errors import ConfigurationError
from clio_relay.jarvis_mcp import (
    JARVIS_MCP_CACHE_SERVER_NAME,
    is_virtual_jarvis_tool,
    jarvis_mcp_artifact_binding_from_entry,
)
from clio_relay.mcp_arguments import _stable_digest
from clio_relay.mcp_tool_catalog import (
    USER_MCP_TOOL_NAMES,
    _all_tool_definitions,
    static_mcp_tool_names,
)
from clio_relay.remote_mcp import (
    RemoteMcpSchemaCache,
    VirtualRemoteMcpCatalog,
    default_remote_mcp_cache_path,
    load_virtual_remote_mcp_catalog,
    unavailable_virtual_remote_mcp_catalog,
)

JSON = dict[str, Any]

MCP_PROFILE_ENV = "CLIO_RELAY_MCP_PROFILE"


def _tool_definitions_and_remote_catalog(
    *,
    profile: str | None = None,
) -> tuple[list[JSON], VirtualRemoteMcpCatalog]:
    """Render tools and return the exact remote catalog used for this list.

    ``_remote_mcp_catalog``/``_configured_cluster_names`` are reached through
    the `_mcp_server` back-reference, not a same-module bare call, even
    though both are also defined in this module: they are the two names
    tests/test_mcp_server.py directly monkeypatches at
    `mcp_server_module.<name>`, and mcp_server.py's own re-export means that
    patch target IS this module's function -- but only a caller that
    resolves the name through mcp_server's namespace at call time sees a
    substituted fake. A bare same-module call here would resolve through
    mcp_remote_catalog's own globals instead and silently miss every patch.
    """
    from clio_relay import mcp_server as _mcp_server

    normalized = _normalize_profile(profile or _mcp_profile_from_env())
    reserved_names = static_mcp_tool_names()
    catalog = _mcp_server._remote_mcp_catalog(
        profile=normalized,
        reserved_names=reserved_names,
    )
    configured_clusters = _mcp_server._configured_cluster_names()
    jarvis_clusters = _bound_virtual_jarvis_clusters(catalog)
    tools = _all_tool_definitions(
        clusters=configured_clusters,
        jarvis_clusters=jarvis_clusters,
    )
    if not jarvis_clusters:
        tools = [tool for tool in tools if not is_virtual_jarvis_tool(str(tool["name"]))]
    if normalized in {"admin", "operator", "all"}:
        selected = tools
    else:
        selected = [
            tool
            for tool in tools
            if tool["name"] in USER_MCP_TOOL_NAMES or is_virtual_jarvis_tool(str(tool["name"]))
        ]
    return [*selected, *catalog.tool_definitions()], catalog


def _bound_virtual_jarvis_clusters(catalog: VirtualRemoteMcpCatalog) -> list[str]:
    """Return clusters with an advertised, verified built-in JARVIS identity."""

    return sorted(
        cluster
        for cluster, artifact_digest in catalog.jarvis_artifact_bindings.items()
        if artifact_digest is not None and cluster in catalog.cluster_route_revisions
    )


def _mcp_profile_from_env() -> str:
    return os.environ.get(MCP_PROFILE_ENV, "user")


def _normalize_profile(profile: str) -> str:
    normalized = profile.strip().lower()
    if normalized in {"", "user", "agent"}:
        return "user"
    if normalized in {"admin", "operator", "all"}:
        return normalized
    raise ValueError("MCP profile must be user, admin, operator, or all")


def _require_compatible_remote_mcp_catalog(
    *,
    profile: str,
    observed_revision: str | None,
    current_revision: str,
) -> None:
    """Reject catalog churn on a connection that advertised an older revision.

    MCP clients may cache a prior ``tools/list`` result and open a fresh stdio
    connection only when they execute a tool.  In that case there is no
    connection-local revision to compare, so dispatch uses the current durable,
    profile-filtered catalog as the authority.  The caller still requires the
    alias to exist in that catalog before selecting its immutable route.
    """
    if observed_revision is None:
        return
    if observed_revision != current_revision:
        raise ValueError(
            "remote MCP catalog changed after tools/list for profile "
            f"{profile!r}; call tools/list again before invoking a virtual remote MCP tool"
        )


def _remote_mcp_catalog(
    *,
    profile: str,
    reserved_names: set[str],
) -> VirtualRemoteMcpCatalog:
    try:
        catalog = load_virtual_remote_mcp_catalog(
            profile=profile,
            reserved_names=reserved_names,
        )
        cache = RemoteMcpSchemaCache.load(default_remote_mcp_cache_path())
    except (ConfigurationError, OSError, ValidationError) as exc:
        return unavailable_virtual_remote_mcp_catalog(str(exc))
    now = datetime.now(UTC)
    jarvis_bindings: dict[str, str | None] = {}
    for cluster in catalog.cluster_route_revisions:
        entry = cache.entry_for(cluster, JARVIS_MCP_CACHE_SERVER_NAME)
        if entry is None:
            jarvis_bindings[cluster] = None
            continue
        try:
            jarvis_bindings[cluster] = jarvis_mcp_artifact_binding_from_entry(entry, now=now)
        except ValueError:
            jarvis_bindings[cluster] = None
    revision = _stable_digest(
        {
            "remote_mcp_catalog_revision": catalog.revision,
            "jarvis_artifact_bindings": jarvis_bindings,
        }
    )
    return VirtualRemoteMcpCatalog(
        revision=revision,
        tools=catalog.tools,
        issues=catalog.issues,
        cluster_route_revisions=catalog.cluster_route_revisions,
        jarvis_artifact_bindings=jarvis_bindings,
    )


def _configured_cluster_names() -> list[str]:
    """Return the stable cluster labels available to local agent tools."""
    registry_path = default_registry_path()
    if not registry_path.exists():
        return []
    try:
        return sorted(ClusterRegistry.load(registry_path).clusters)
    except (ConfigurationError, OSError, ValidationError):
        return []


def _route_revision(definition: ClusterDefinition) -> str:
    """Bind a returned job handle to one durable cluster queue route."""
    return cluster_route_revision(definition)


def _validated_route_revision(value: object) -> str:
    """Validate one opaque route token before comparing or routing with it."""

    if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise ValueError(
            "route_revision must be the 64-character lowercase hexadecimal token "
            "copied from the same relay job receipt"
        )
    return value

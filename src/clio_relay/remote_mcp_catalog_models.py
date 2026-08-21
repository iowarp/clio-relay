"""Virtual remote MCP catalog data model: routes, tools, and the resolved catalog.

Extracted from :mod:`clio_relay.remote_mcp` (iowarp/clio-relay#231; see
``docs/design/relay-architecture-2026-08.md`` §4.5/§5, the deferred
"validator families" row). This module owns the resolved-catalog shape every
other remote-MCP owner module builds or reads: one selected execution route
(:class:`RemoteMcpRoute`), one agent-facing alias backed by equivalent remote
schemas across clusters (:class:`VirtualRemoteMcpTool`, whose
``definition()`` renders the async relay-submission tool contract and whose
``forwarded_arguments``/``relay_arguments`` split the local control envelope
from the exact remote arguments), the resolved catalog for one local MCP
profile (:class:`VirtualRemoteMcpCatalog`), the internal per-tool discovery
candidate (:class:`_Candidate`, used only while assembling a catalog in
``remote_mcp_catalog_build.py``), and the fail-closed unavailable-catalog
constructor.

``VirtualRemoteMcpTool``, ``RemoteMcpRoute``, and ``VirtualRemoteMcpCatalog``
are re-exported under their original names (external callers across several
modules and tests import them directly from ``clio_relay.remote_mcp``).
``unavailable_virtual_remote_mcp_catalog`` is re-exported too (``mcp_server.py``
imports it directly). ``_virtual_remote_mcp_relay_arguments`` and
``_Candidate`` are private with no caller outside ``remote_mcp.py``
(confirmed by grep before the move; ``remote_mcp_catalog_build.py`` imports
``_Candidate`` directly from here, not from ``remote_mcp.py``), so they are
imported directly rather than re-exported.

``VirtualRemoteMcpTool.definition()`` reads ``CLIO_KIT_JARVIS_USER_TOOL_NAMES``,
one of the contract-pin constants that still lives in ``remote_mcp.py``
(unsequenced, post-campaign per the design doc). A module-scope import back
into ``remote_mcp.py`` (which imports this module for the re-export above)
would be a load-order circular import; importing it inside the method body
instead is the proven idiom for that shape (see ``remote_mcp_wire_schemas.py``'s
own ``virtual_jarvis_job_output_schema``).
"""

from __future__ import annotations

import math
from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any, cast

from clio_relay.cluster_config import RemoteMcpServerConfig
from clio_relay.models import REGISTERED_JARVIS_USER_CONTRACT, McpControlQueryEvidence
from clio_relay.remote_mcp_acceptance_models import RemoteMcpCatalogIssue
from clio_relay.remote_mcp_schema_wrapping import (
    MAX_VIRTUAL_REMOTE_MCP_LOG_BYTES,
    VIRTUAL_REMOTE_MCP_RELAY_CONTROL_FIELDS,
    inject_cluster_argument,
)
from clio_relay.remote_mcp_tool_schema import (
    RemoteMcpDiscoveryProvenance,
    RemoteMcpToolSchema,
    _stable_digest,
)
from clio_relay.remote_mcp_wire_schemas import (
    VIRTUAL_REMOTE_MCP_JOB_OUTPUT_SCHEMA,
    virtual_jarvis_job_output_schema,
)

JSON = dict[str, Any]


@dataclass(frozen=True)
class RemoteMcpRoute:
    """Execution route selected by a virtual tool alias and cluster argument."""

    cluster: str
    server_name: str
    command: str
    args: tuple[str, ...]
    env_from: tuple[tuple[str, str], ...]
    expected_server_artifact_digest: str | None
    remote_tool_name: str
    timeout_seconds: int
    contract: str | None
    cluster_route_revision: str
    registration_revision: str
    control_query_evidence: McpControlQueryEvidence | None = None


def _virtual_remote_mcp_relay_arguments(arguments: JSON) -> JSON:
    """Validate and copy relay-only controls from an agent-facing invocation."""

    controls: JSON = {}
    if "idempotency_key" in arguments:
        idempotency_key = arguments["idempotency_key"]
        if (
            not isinstance(idempotency_key, str)
            or not idempotency_key
            or len(idempotency_key) > 512
        ):
            raise ValueError("idempotency_key must be a non-empty string of at most 512 characters")
        controls["idempotency_key"] = idempotency_key
    for field_name in ("wait_for_terminal", "include_logs"):
        if field_name not in arguments:
            continue
        value = arguments[field_name]
        if not isinstance(value, bool):
            raise ValueError(f"{field_name} must be a boolean")
        controls[field_name] = value
    for field_name in ("wait_timeout_seconds", "poll_seconds"):
        if field_name not in arguments:
            continue
        value = arguments[field_name]
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError(f"{field_name} must be a number")
        if not math.isfinite(value) or value <= 0:
            raise ValueError(f"{field_name} must be positive and finite")
        controls[field_name] = value
    if "log_limit" in arguments:
        log_limit = arguments["log_limit"]
        if isinstance(log_limit, bool) or not isinstance(log_limit, int):
            raise ValueError("log_limit must be an integer")
        if log_limit < 1 or log_limit > MAX_VIRTUAL_REMOTE_MCP_LOG_BYTES:
            raise ValueError(f"log_limit must be between 1 and {MAX_VIRTUAL_REMOTE_MCP_LOG_BYTES}")
        controls["log_limit"] = log_limit
    return controls


@dataclass(frozen=True)
class VirtualRemoteMcpTool:
    """One agent-facing alias backed by equivalent remote schemas."""

    alias: str
    namespace: str
    remote_tool: RemoteMcpToolSchema
    routes: dict[str, RemoteMcpRoute]
    arguments_wrapped: bool

    def definition(self) -> JSON:
        """Render the asynchronous relay-submission contract for a remote tool."""
        from clio_relay.remote_mcp import CLIO_KIT_JARVIS_USER_TOOL_NAMES

        clusters = sorted(self.routes)
        input_schema = inject_cluster_argument(self.remote_tool.input_schema, clusters=clusters)
        description = self.remote_tool.description or f"Call {self.remote_tool.name}."
        wait_guidance = (
            "Set wait_for_terminal=true to return the bounded remote MCP result in this same "
            "call; otherwise use relay job tools with the returned handle."
        )
        exact_v36_routes = bool(self.routes) and all(
            route.contract == REGISTERED_JARVIS_USER_CONTRACT for route in self.routes.values()
        )
        output_schema = (
            virtual_jarvis_job_output_schema(self.remote_tool.name, clusters=clusters)
            if exact_v36_routes and self.remote_tool.name in CLIO_KIT_JARVIS_USER_TOOL_NAMES
            else deepcopy(VIRTUAL_REMOTE_MCP_JOB_OUTPUT_SCHEMA)
        )
        if exact_v36_routes and self.remote_tool.name == "jarvis_describe":
            properties = cast(JSON, input_schema["properties"])
            wait_schema = cast(JSON, properties["wait_for_terminal"])
            wait_schema["default"] = True
            wait_schema["description"] = (
                "Always reconciled to a terminal result within the bounded local wait for the "
                "registered JARVIS v3.6 contract. The bound never cancels or fails the durable "
                "remote operation."
            )
            description += (
                " Registered JARVIS v3.6 descriptions are transparently reconciled to a "
                "terminal structured result so package semantics survive client restarts."
            )
            wait_guidance = (
                "The terminal reconciliation is automatic; if its bound expires, relay "
                "returns an error containing the still-observable durable job handle."
            )
        elif exact_v36_routes and self.remote_tool.name == "jarvis_run":
            description += (
                " Before running, inspect every selected package deployment contract. For each "
                "unavailable runtime with a Spack provider resolution, locate it and pass the "
                "returned immutable load_spec in spack_specs; presence in a site store does not "
                "place the runtime on the execution PATH."
                " On each genuinely new run identity, relay securely reconciles every tracked "
                "local-file setting and pins the execution to an immutable input manifest. "
                "Retrying the same idempotency key reuses that admitted manifest without "
                "rescanning mutable Host files."
            )
        definition: JSON = {
            "name": self.alias,
            "description": (
                f"{description} Routed through registered remote MCP namespace "
                f"'{self.namespace}' on the selected cluster. The call is submitted "
                f"as a durable relay job. {wait_guidance}"
            ),
            "inputSchema": input_schema,
            "outputSchema": output_schema,
        }
        if self.remote_tool.title is not None:
            definition["title"] = self.remote_tool.title
        if self.remote_tool.annotations is not None:
            definition["annotations"] = deepcopy(self.remote_tool.annotations)
        return definition

    def forwarded_arguments(self, arguments: JSON) -> JSON:
        """Remove the relay envelope and return the exact remote arguments object."""
        if not self.arguments_wrapped:
            return {
                key: value
                for key, value in arguments.items()
                if key != "cluster" and key not in VIRTUAL_REMOTE_MCP_RELAY_CONTROL_FIELDS
            }
        unexpected = sorted(
            set(arguments) - {"cluster", "arguments"} - VIRTUAL_REMOTE_MCP_RELAY_CONTROL_FIELDS
        )
        if unexpected:
            raise ValueError(
                "wrapped virtual remote MCP arguments contain unexpected local fields: "
                + ", ".join(unexpected)
            )
        remote_arguments = arguments.get("arguments")
        if not isinstance(remote_arguments, dict):
            raise ValueError("wrapped virtual remote MCP call requires an arguments object")
        return deepcopy(cast(JSON, remote_arguments))

    def relay_arguments(self, arguments: JSON) -> JSON:
        """Return validated local controls without any remote tool arguments."""

        return _virtual_remote_mcp_relay_arguments(arguments)


@dataclass(frozen=True)
class VirtualRemoteMcpCatalog:
    """Resolved virtual tool catalog for one local MCP profile."""

    revision: str
    tools: dict[str, VirtualRemoteMcpTool]
    issues: tuple[RemoteMcpCatalogIssue, ...]
    cluster_route_revisions: dict[str, str] = field(default_factory=lambda: dict[str, str]())
    jarvis_artifact_bindings: dict[str, str | None] = field(
        default_factory=lambda: dict[str, str | None]()
    )

    def tool_definitions(self) -> list[JSON]:
        """Return deterministic agent-facing tool definitions."""
        return [self.tools[name].definition() for name in sorted(self.tools)]

    def resolve(self, alias: str, cluster: str) -> RemoteMcpRoute:
        """Resolve an alias and cluster without forwarding the selector remotely."""
        try:
            tool = self.tools[alias]
        except KeyError as exc:
            raise ValueError(f"unknown or unavailable virtual remote MCP tool: {alias}") from exc
        try:
            return tool.routes[cluster]
        except KeyError as exc:
            available = ", ".join(sorted(tool.routes))
            raise ValueError(
                f"virtual remote MCP tool {alias} is not available on cluster {cluster}; "
                f"available clusters: {available}"
            ) from exc

    def forwarded_arguments(self, alias: str, arguments: JSON) -> JSON:
        """Return arguments for the remote tool without local routing structure."""
        try:
            tool = self.tools[alias]
        except KeyError as exc:
            raise ValueError(f"unknown or unavailable virtual remote MCP tool: {alias}") from exc
        return tool.forwarded_arguments(arguments)

    def relay_arguments(self, alias: str, arguments: JSON) -> JSON:
        """Return the validated relay envelope for one virtual tool invocation."""

        try:
            tool = self.tools[alias]
        except KeyError as exc:
            raise ValueError(f"unknown or unavailable virtual remote MCP tool: {alias}") from exc
        return tool.relay_arguments(arguments)


@dataclass(frozen=True)
class _Candidate:
    cluster: str
    server_name: str
    namespace: str
    registration: RemoteMcpServerConfig
    tool: RemoteMcpToolSchema
    base_alias: str
    identity: str
    expected_server_artifact_digest: str | None
    discovery_provenance: RemoteMcpDiscoveryProvenance
    discovery_schema_digest: str


def unavailable_virtual_remote_mcp_catalog(reason: str) -> VirtualRemoteMcpCatalog:
    """Return a fail-closed catalog while preserving built-in MCP safety tools."""
    bounded_reason = reason[:4_096]
    return VirtualRemoteMcpCatalog(
        revision=_stable_digest({"unavailable": bounded_reason}),
        tools={},
        issues=(
            RemoteMcpCatalogIssue(
                cluster="*",
                server_name="*",
                reason=f"remote MCP catalog unavailable: {bounded_reason}",
            ),
        ),
    )

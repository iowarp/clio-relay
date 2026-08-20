"""Agent-facing JSON-Schema builders for remote MCP job receipts and handoffs.

Extracted from :mod:`clio_relay.remote_mcp` (iowarp/clio-relay#231; see
``docs/design/relay-architecture-2026-08.md`` §4.5/§5). This module owns
the pure JSON-Schema construction shared by every virtual remote MCP tool
definition and its relay job receipt: the opaque cluster-route revision
field (:func:`cluster_route_revision_json_schema`), the JARVIS service
handoff object one ``jarvis_get_execution`` result may carry
(:func:`jarvis_service_runtime_handoff_json_schema`), the generic relay job
receipt schema (:data:`VIRTUAL_REMOTE_MCP_JOB_OUTPUT_SCHEMA`), and its
per-tool specialization for the registered JARVIS v3.6 contract
(:func:`virtual_jarvis_job_output_schema`).

Every one of these four names is imported directly from
:mod:`clio_relay.remote_mcp` by other modules (``mcp_server.py``,
``jarvis_mcp.py``, ``jarvis_mcp_validation.py``) and by tests, so
``remote_mcp.py`` re-exports all four under their original names; its own
``VirtualRemoteMcpTool.definition()`` is also a real local reader of
``virtual_jarvis_job_output_schema`` and ``VIRTUAL_REMOTE_MCP_JOB_OUTPUT_SCHEMA``.

:func:`virtual_jarvis_job_output_schema` checks membership in
``CLIO_KIT_JARVIS_USER_TOOL_NAMES``, one of the contract-pin constants that
still lives in ``remote_mcp.py`` (unsequenced, post-campaign per the design
doc). A module-scope import back into ``remote_mcp.py`` (which imports this
module for the re-export above) would be a load-order circular import;
importing it inside the function body instead is the proven idiom for that
shape.
"""

from __future__ import annotations

from copy import deepcopy
from typing import Any, cast

from clio_relay.identifiers import durable_record_id_json_schema

JSON = dict[str, Any]


def cluster_route_revision_json_schema() -> JSON:
    """Return the agent-facing schema for an opaque cluster route revision."""
    return {
        "type": "string",
        "pattern": "^[0-9a-f]{64}$",
        "description": (
            "Opaque cluster-route revision copied exactly from a relay job receipt. "
            "This is not a scientific-dataset catalog revision."
        ),
    }


VIRTUAL_REMOTE_MCP_JOB_OUTPUT_SCHEMA: JSON = {
    "type": "object",
    "properties": {
        "cluster": {"type": "string"},
        "job_id": {"type": "string"},
        "state": {
            "type": "string",
            "enum": ["queued", "leased", "running", "succeeded", "failed", "canceled"],
        },
        "kind": {"type": "string", "const": "mcp_call"},
        "terminal": {"type": "boolean"},
        "remote": {"type": "boolean"},
        "route_revision": cluster_route_revision_json_schema(),
        "catalog_revision": {
            "type": "string",
            "pattern": "^[0-9a-f]{64}$",
            "description": (
                "Opaque revision of the locally advertised remote-MCP tool catalog; "
                "this is not a scientific-dataset catalog revision."
            ),
        },
        "last_error": {"type": ["string", "null"]},
        "observation": {
            "type": "object",
            "properties": {
                "outcome": {
                    "type": "string",
                    "enum": ["terminal", "observation_unknown"],
                },
                "timeout_seconds": {"type": "number", "exclusiveMinimum": 0},
                "scheduler_action": {"type": "string", "const": "none"},
                "relay_action": {"type": "string", "const": "none"},
            },
            "required": [
                "outcome",
                "timeout_seconds",
                "scheduler_action",
                "relay_action",
            ],
            "additionalProperties": False,
            "description": (
                "Result of the bounded observation requested by this call. "
                "observation_unknown preserves the durable job for later observation."
            ),
        },
        "mcp_result": {"type": "object"},
        "mcp_result_artifact": {"type": "object"},
        "logs": {"type": "object"},
    },
    "required": [
        "cluster",
        "job_id",
        "state",
        "kind",
        "terminal",
        "route_revision",
        "catalog_revision",
    ],
    "additionalProperties": False,
}


def jarvis_service_runtime_handoff_json_schema(
    *,
    clusters: list[str] | None = None,
) -> JSON:
    """Return the exact agent-facing JARVIS service handoff schema."""
    return {
        "type": "object",
        "properties": {
            "cluster": {
                "type": "string",
                "minLength": 1,
                "maxLength": 256,
                **({"enum": sorted(clusters)} if clusters is not None else {}),
            },
            "source_job_id": durable_record_id_json_schema(),
            "source_artifact_id": durable_record_id_json_schema(),
            "package_id": {"type": "string", "minLength": 1, "maxLength": 256},
            "package_name": {"type": "string", "minLength": 1, "maxLength": 256},
            "service_instance_id": {
                "type": "string",
                "minLength": 1,
                "maxLength": 512,
            },
        },
        "required": [
            "cluster",
            "source_job_id",
            "source_artifact_id",
            "package_id",
            "package_name",
            "service_instance_id",
        ],
        "additionalProperties": False,
    }


def virtual_jarvis_job_output_schema(
    remote_tool: str,
    *,
    clusters: list[str] | None = None,
) -> JSON:
    """Return the exact relay job receipt schema for one verified JARVIS tool."""
    from clio_relay.remote_mcp import CLIO_KIT_JARVIS_USER_TOOL_NAMES

    if remote_tool not in CLIO_KIT_JARVIS_USER_TOOL_NAMES:
        raise ValueError(f"unknown virtual JARVIS tool: {remote_tool}")
    output_schema = deepcopy(VIRTUAL_REMOTE_MCP_JOB_OUTPUT_SCHEMA)
    if remote_tool == "jarvis_get_execution":
        output_properties = cast(JSON, output_schema["properties"])
        output_properties["service_runtime_bindings"] = {
            "type": "array",
            "description": (
                "Ready-service handoffs derived from the verified durable MCP result. "
                "Pass one item unchanged as relay_bind_jarvis_runtime.binding."
            ),
            "items": jarvis_service_runtime_handoff_json_schema(clusters=clusters),
            "maxItems": 4_096,
        }
    return output_schema

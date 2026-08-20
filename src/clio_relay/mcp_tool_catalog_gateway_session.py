"""Gateway-session MCP tool definitions: durable scheduler-backed gateway
service session lifecycle (create/list/get/update/close), binding a JARVIS
runtime to one, and storage status.

One of four tool-domain groups mcp_tool_catalog.py's `_all_tool_definitions`
assembles (iowarp/clio-relay#231, see mcp_tool_catalog.py's own module
docstring). Pure static JSON-schema data, no live server/session state.
"""

from __future__ import annotations

from typing import Any

from clio_relay.identifiers import durable_record_id_json_schema
from clio_relay.pagination import DEFAULT_RESPONSE_PAGE_RECORDS, MAX_RESPONSE_PAGE_RECORDS
from clio_relay.remote_mcp import jarvis_service_runtime_handoff_json_schema

JSON = dict[str, Any]


def _gateway_session_tool_definitions(*, clusters: list[str] | None = None) -> list[JSON]:
    """Return gateway-service-session lifecycle and storage-status tool schemas.

    ``clusters`` bounds ``relay_bind_jarvis_runtime``'s ``binding``/``cluster``
    schemas to the configured cluster set (an ``enum`` when known, unbounded
    ``None``) -- the one group whose schema is itself configuration-dependent.
    """
    return [
        {
            "name": "relay_create_gateway_session",
            "description": "Create a durable scheduler-backed gateway service session.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "cluster": {"type": "string"},
                    "name": {"type": "string"},
                    "state": {
                        "type": "string",
                        "enum": [
                            "created",
                            "submitted",
                            "pending",
                            "allocated",
                            "starting",
                            "ready",
                            "degraded",
                            "failed",
                            "closed",
                            "unknown",
                        ],
                        "default": "created",
                    },
                    "queue_state": {"type": "string"},
                    "node": {"type": "string"},
                    "requested_resources": {"type": "object", "default": {}},
                    "stdout_uri": {"type": "string"},
                    "stderr_uri": {"type": "string"},
                    "log_uris": {"type": "array", "items": {"type": "string"}, "default": []},
                    "gateway": {"type": "object", "default": {}},
                    "metadata": {"type": "object", "default": {}},
                },
                "required": ["cluster", "name"],
                "additionalProperties": False,
            },
        },
        {
            "name": "relay_list_gateway_sessions",
            "description": "List one global source window of durable gateway sessions.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "cluster": {"type": "string"},
                    "cursor": {"type": "integer", "default": 1, "minimum": 1},
                    "limit": {
                        "type": "integer",
                        "default": DEFAULT_RESPONSE_PAGE_RECORDS,
                        "minimum": 1,
                        "maximum": MAX_RESPONSE_PAGE_RECORDS,
                    },
                },
                "additionalProperties": False,
            },
        },
        {
            "name": "relay_bind_jarvis_runtime",
            "description": (
                "Bind local relay connectors to one ready service reported by a completed, "
                "artifact-bound JARVIS execution query with service runtimes included. "
                "Pass one service_runtime_bindings item returned either by a "
                "wait_for_terminal jarvis_get_execution call or by relay_wait for its exact "
                "remote job handle unchanged as binding. jarvis_run is not a valid binding "
                "source, and a JARVIS execution_id is not a gateway_session_id. "
                "Runtime host, paths, scheduler identity, and dataset metadata are read "
                "only from the durable JARVIS result. The relay allocates the desktop "
                "loopback port. A bounded readiness miss returns outcome=pending with "
                "nullable URLs and an exact retry_selector; it does not fail, cancel, or "
                "replace the JARVIS execution or its connectors. Reissue this tool with "
                "the same binding, name, and policy to resume the same gateway. When "
                "outcome=ready, copy the top-level gateway_session_id unchanged into the "
                "viewer-opening tool; service_instance_id is not a gateway identity."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "binding": jarvis_service_runtime_handoff_json_schema(clusters=clusters),
                    "cluster": {
                        "type": "string",
                        **({"enum": sorted(clusters)} if clusters is not None else {}),
                    },
                    "source_job_id": durable_record_id_json_schema(),
                    "source_artifact_id": durable_record_id_json_schema(),
                    "package_id": {"type": "string", "minLength": 1, "maxLength": 256},
                    "package_name": {"type": "string", "minLength": 1, "maxLength": 256},
                    "name": {"type": "string", "minLength": 1, "maxLength": 256},
                    "readiness_timeout_seconds": {
                        "type": "number",
                        "exclusiveMinimum": 0,
                        "maximum": 3600,
                        "default": 300,
                    },
                    "poll_seconds": {
                        "type": "number",
                        "exclusiveMinimum": 0,
                        "maximum": 60,
                        "default": 2,
                    },
                },
                "if": {"required": ["binding"]},
                "then": {
                    "not": {
                        "anyOf": [
                            {"required": ["cluster"]},
                            {"required": ["source_job_id"]},
                            {"required": ["source_artifact_id"]},
                            {"required": ["package_id"]},
                            {"required": ["package_name"]},
                        ]
                    }
                },
                "else": {
                    "required": [
                        "cluster",
                        "source_job_id",
                        "source_artifact_id",
                        "package_id",
                        "package_name",
                    ]
                },
                "additionalProperties": False,
            },
            "outputSchema": {
                "type": "object",
                "properties": {
                    "outcome": {"type": "string", "enum": ["ready", "pending"]},
                    "retry_selector": {
                        "anyOf": [{"type": "object"}, {"type": "null"}],
                    },
                    "scheduler_action": {"const": "none"},
                    "relay_action": {"const": "none"},
                    "gateway_session_id": {
                        **durable_record_id_json_schema(),
                        "pattern": r"^gateway_[0-9a-f]{32}$",
                        "description": (
                            "Exact relay gateway identity to pass unchanged to a viewer-opening "
                            "tool. It is equal to gateway_session.session_id."
                        ),
                    },
                    "gateway_session": {"type": "object"},
                    "connect_url": {"type": ["string", "null"]},
                    "health_url": {"type": ["string", "null"]},
                    "stream_url": {"type": ["string", "null"]},
                    "events_url": {"type": ["string", "null"]},
                    "state_url": {"type": ["string", "null"]},
                    "command_url": {"type": ["string", "null"]},
                    "scheduler_cancel_requested": {"const": False},
                },
                "required": [
                    "outcome",
                    "retry_selector",
                    "scheduler_action",
                    "relay_action",
                    "gateway_session_id",
                    "gateway_session",
                    "connect_url",
                    "health_url",
                    "stream_url",
                    "events_url",
                    "state_url",
                    "command_url",
                    "scheduler_cancel_requested",
                ],
                "allOf": [
                    {
                        "if": {"properties": {"outcome": {"const": "ready"}}},
                        "then": {
                            "properties": {
                                "retry_selector": {"type": "null"},
                                "connect_url": {"type": "string"},
                                "health_url": {"type": "string"},
                                "stream_url": {"type": "string"},
                                "events_url": {"type": "string"},
                                "state_url": {"type": "string"},
                                "command_url": {"type": "string"},
                            }
                        },
                    },
                    {
                        "if": {"properties": {"outcome": {"const": "pending"}}},
                        "then": {
                            "properties": {
                                "retry_selector": {"type": "object"},
                                "connect_url": {"type": "null"},
                                "health_url": {"type": "null"},
                                "stream_url": {"type": "null"},
                                "events_url": {"type": "null"},
                                "state_url": {"type": "null"},
                                "command_url": {"type": "null"},
                            }
                        },
                    },
                ],
                "additionalProperties": False,
            },
        },
        {
            "name": "relay_storage_status",
            "description": "Return machine-readable relay storage admission readiness.",
            "inputSchema": {
                "type": "object",
                "properties": {},
                "additionalProperties": False,
            },
        },
        {
            "name": "relay_get_gateway_session",
            "description": "Read a durable gateway service session.",
            "inputSchema": {
                "type": "object",
                "properties": {"session_id": durable_record_id_json_schema()},
                "required": ["session_id"],
                "additionalProperties": False,
            },
        },
        {
            "name": "relay_update_gateway_session",
            "description": "Update a gateway service session with scheduler or gateway state.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "session_id": durable_record_id_json_schema(),
                    "state": {"type": "string"},
                    "queue_state": {"type": "string"},
                    "node": {"type": "string"},
                    "requested_resources": {"type": "object"},
                    "stdout_uri": {"type": "string"},
                    "stderr_uri": {"type": "string"},
                    "log_uris": {"type": "array", "items": {"type": "string"}},
                    "gateway": {"type": "object"},
                    "artifacts": {"type": "array", "items": {"type": "string"}},
                    "metadata": {"type": "object", "default": {}},
                },
                "required": ["session_id"],
                "additionalProperties": False,
            },
        },
        {
            "name": "relay_close_gateway_session",
            "description": "Mark a gateway service session closed.",
            "inputSchema": {
                "type": "object",
                "properties": {"session_id": durable_record_id_json_schema()},
                "required": ["session_id"],
                "additionalProperties": False,
            },
        },
    ]

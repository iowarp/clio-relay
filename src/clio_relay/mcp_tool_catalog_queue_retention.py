"""Queue, retention, and monitor-rule MCP tool definitions: relay-queue
introspection (list/diagnose/stale/cleanup-stale), artifact retention
(plan/status/collect), worker status, and monitor-rule management.

One of four tool-domain groups mcp_tool_catalog.py's `_all_tool_definitions`
assembles (iowarp/clio-relay#231, see mcp_tool_catalog.py's own module
docstring). Pure static JSON-schema data, no live server/session state.
"""

from __future__ import annotations

from typing import Any

from clio_relay.identifiers import durable_record_id_json_schema
from clio_relay.pagination import DEFAULT_RESPONSE_PAGE_RECORDS, MAX_RESPONSE_PAGE_RECORDS
from clio_relay.queue_management import DEFAULT_STALE_SCAN_LIMIT
from clio_relay.remote_mcp import cluster_route_revision_json_schema

JSON = dict[str, Any]


def _queue_and_retention_tool_definitions() -> list[JSON]:
    """Return relay-queue/retention/worker-status/monitor-rule tool schemas."""
    return [
        {
            "name": "relay_queue_list",
            "description": "List relay queue jobs with queue-position metadata.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "cluster": {"type": "string"},
                    "route_revision": cluster_route_revision_json_schema(),
                    "state": {
                        "type": "string",
                        "enum": ["queued", "leased", "running", "succeeded", "failed", "canceled"],
                    },
                    "kind": {
                        "type": "string",
                        "enum": ["jarvis", "remote_agent", "mcp_call"],
                    },
                    "include_terminal": {"type": "boolean", "default": False},
                    "cursor": {"type": "integer", "default": 1, "minimum": 1},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 500, "default": 100},
                    "scan_limit": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 10000,
                        "default": 1000,
                    },
                },
                "additionalProperties": False,
            },
        },
        {
            "name": "relay_queue_diagnose",
            "description": "Diagnose stuck relay queue state such as expired leases.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "job_id": durable_record_id_json_schema(),
                    "cluster": {"type": "string"},
                    "route_revision": cluster_route_revision_json_schema(),
                    "older_than_seconds": {
                        "type": "integer",
                        "minimum": 1,
                        "default": 7200,
                    },
                    "scan_limit": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 10000,
                        "default": 1000,
                    },
                },
                "required": ["job_id"],
                "additionalProperties": False,
            },
        },
        {
            "name": "relay_queue_stale",
            "description": "Discover stale active relay jobs without changing queue state.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "cluster": {"type": "string"},
                    "route_revision": cluster_route_revision_json_schema(),
                    "job_id": durable_record_id_json_schema(),
                    "older_than_seconds": {"type": "integer", "minimum": 1},
                    "kind": {
                        "type": "string",
                        "enum": ["jarvis", "remote_agent", "mcp_call"],
                    },
                    "limit": {"type": "integer", "minimum": 1, "maximum": 500, "default": 100},
                    "scan_limit": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 10000,
                        "default": DEFAULT_STALE_SCAN_LIMIT,
                    },
                },
                "required": ["cluster", "older_than_seconds"],
                "additionalProperties": False,
            },
        },
        {
            "name": "relay_queue_cleanup_stale",
            "description": (
                "Preview or execute relay-only stale recovery; queued cancellation is explicit."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "cluster": {"type": "string"},
                    "route_revision": cluster_route_revision_json_schema(),
                    "job_id": durable_record_id_json_schema(),
                    "older_than_seconds": {
                        "type": "integer",
                        "minimum": 1,
                        "default": 7200,
                    },
                    "kind": {
                        "type": "string",
                        "enum": ["jarvis", "remote_agent", "mcp_call"],
                    },
                    "max_attempts": {"type": "integer", "minimum": 1, "default": 3},
                    "dry_run": {"type": "boolean", "default": True},
                    "cancel_queued": {"type": "boolean", "default": False},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 500, "default": 100},
                    "scan_limit": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 10000,
                        "default": DEFAULT_STALE_SCAN_LIMIT,
                    },
                },
                "required": ["cluster"],
                "additionalProperties": False,
            },
        },
        {
            "name": "relay_retention_plan",
            "description": "Build a read-only terminal-job retention plan.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "job_id": durable_record_id_json_schema(),
                    "expected_updated_at": {"type": "string", "format": "date-time"},
                },
                "required": ["job_id"],
                "additionalProperties": False,
            },
        },
        {
            "name": "relay_retention_status",
            "description": "Read the crash-resumable terminal-retention phase.",
            "inputSchema": {
                "type": "object",
                "properties": {"job_id": durable_record_id_json_schema()},
                "required": ["job_id"],
                "additionalProperties": False,
            },
        },
        {
            "name": "relay_retention_collect",
            "description": (
                "Dry-run by default or advance bounded terminal retention. "
                "This tool never cancels scheduler jobs."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "job_id": durable_record_id_json_schema(),
                    "execute": {"type": "boolean", "default": False},
                    "batch_size": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 100,
                        "default": 100,
                    },
                    "expected_updated_at": {"type": "string", "format": "date-time"},
                },
                "required": ["job_id"],
                "additionalProperties": False,
            },
        },
        {
            "name": "relay_worker_status",
            "description": "Show registered worker capacity and leases.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "cluster": {"type": "string"},
                    "route_revision": cluster_route_revision_json_schema(),
                },
                "additionalProperties": False,
            },
        },
        {
            "name": "relay_create_monitor_rule",
            "description": "Create a regex monitor rule over a job event stream.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "job_id": durable_record_id_json_schema(),
                    "pattern": {"type": "string"},
                    "action": {
                        "type": "string",
                        "enum": ["emit_event", "submit_agent", "record_progress"],
                        "default": "emit_event",
                    },
                    "event_types": {
                        "type": "array",
                        "items": {"type": "string"},
                        "default": [],
                    },
                    "action_payload": {"type": "object", "default": {}},
                },
                "required": ["job_id", "pattern"],
                "additionalProperties": False,
            },
        },
        {
            "name": "relay_list_monitor_rules",
            "description": "List one global source window of monitor rules.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "job_id": durable_record_id_json_schema(),
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
            "name": "relay_evaluate_monitor_rules",
            "description": "Evaluate enabled monitor rules once.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "limit": {
                        "type": "integer",
                        "default": DEFAULT_RESPONSE_PAGE_RECORDS,
                        "minimum": 1,
                        "maximum": MAX_RESPONSE_PAGE_RECORDS,
                    }
                },
                "additionalProperties": False,
            },
        },
    ]

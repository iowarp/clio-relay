"""Monitoring MCP tool definitions: reading job/task state, job logs,
progress records, and artifacts (list/lineage/read), plus the legacy
`relay_cancel_job` alias.

One of four tool-domain groups mcp_tool_catalog.py's `_all_tool_definitions`
assembles (iowarp/clio-relay#231, see mcp_tool_catalog.py's own module
docstring). Pure static JSON-schema data, no live server/session state.
"""

from __future__ import annotations

from typing import Any

from clio_relay.identifiers import durable_record_id_json_schema
from clio_relay.pagination import DEFAULT_RESPONSE_PAGE_RECORDS, MAX_RESPONSE_PAGE_RECORDS
from clio_relay.remote_mcp import cluster_route_revision_json_schema
from clio_relay.spool import MAX_LOG_READ_BYTES

JSON = dict[str, Any]


def _monitoring_and_artifact_tool_definitions() -> list[JSON]:
    """Return job/task/log/progress/artifact read-tool schemas."""
    return [
        {
            "name": "relay_get_job",
            "description": "Read a relay job record by id.",
            "inputSchema": {
                "type": "object",
                "properties": {"job_id": durable_record_id_json_schema()},
                "required": ["job_id"],
                "additionalProperties": False,
            },
        },
        {
            "name": "relay_get_job_status",
            "description": "Read job state, relay queue position, and scheduler status.",
            "inputSchema": {
                "type": "object",
                "properties": {"job_id": durable_record_id_json_schema()},
                "required": ["job_id"],
                "additionalProperties": False,
            },
        },
        {
            "name": "relay_monitor_job",
            "description": "Read job state and event stream data from a cursor.",
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
                "required": ["job_id"],
                "additionalProperties": False,
            },
        },
        {
            "name": "relay_watch_job_events",
            "description": "Read relay job events from a cursor.",
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
                "required": ["job_id"],
                "additionalProperties": False,
            },
        },
        {
            "name": "relay_list_tasks",
            "description": "List one stable page of durable task records for a relay job.",
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
                "required": ["job_id"],
                "additionalProperties": False,
            },
        },
        {
            "name": "relay_record_task_event",
            "description": "Record a structured, resumable timeline event for one relay task.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "task_id": durable_record_id_json_schema(),
                    "event_type": {"type": "string"},
                    "label": {"type": "string"},
                    "status": {
                        "type": "string",
                        "enum": ["planned", "running", "succeeded", "warning", "error", "canceled"],
                        "default": "running",
                    },
                    "summary": {"type": "string"},
                    "detail": {"type": "string"},
                    "artifact_refs": {
                        "type": "array",
                        "items": durable_record_id_json_schema(),
                        "default": [],
                    },
                    "path_refs": {
                        "type": "array",
                        "items": {"type": "string"},
                        "default": [],
                    },
                    "metadata": {"type": "object", "default": {}},
                },
                "required": ["task_id", "event_type", "label", "summary"],
                "additionalProperties": False,
            },
        },
        {
            "name": "relay_watch_task_events",
            "description": "Read task timeline events from a task cursor.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "task_id": durable_record_id_json_schema(),
                    "cursor": {"type": "integer", "default": 1, "minimum": 1},
                    "limit": {
                        "type": "integer",
                        "default": DEFAULT_RESPONSE_PAGE_RECORDS,
                        "minimum": 1,
                        "maximum": MAX_RESPONSE_PAGE_RECORDS,
                    },
                },
                "required": ["task_id"],
                "additionalProperties": False,
            },
        },
        {
            "name": "relay_read_job_log",
            "description": "Read stdout or stderr text from a job log by byte offset.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "job_id": durable_record_id_json_schema(),
                    "stream": {"type": "string", "enum": ["stdout", "stderr"]},
                    "offset": {"type": "integer", "default": 0, "minimum": 0},
                    "limit": {
                        "type": "integer",
                        "default": 65536,
                        "minimum": 1,
                        "maximum": MAX_LOG_READ_BYTES,
                    },
                },
                "required": ["job_id", "stream"],
                "additionalProperties": False,
            },
        },
        {
            "name": "relay_list_artifacts",
            "description": (
                "List one stable page of artifact references indexed for a job. Pass the "
                "cluster and route_revision from that job's own submission receipt to list "
                "artifacts for a job that ran on a configured remote cluster."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "job_id": durable_record_id_json_schema(),
                    "cluster": {"type": "string"},
                    "route_revision": cluster_route_revision_json_schema(),
                    "cursor": {"type": "integer", "default": 1, "minimum": 1},
                    "limit": {
                        "type": "integer",
                        "default": DEFAULT_RESPONSE_PAGE_RECORDS,
                        "minimum": 1,
                        "maximum": MAX_RESPONSE_PAGE_RECORDS,
                    },
                },
                "required": ["job_id"],
                "dependentRequired": {
                    "cluster": ["route_revision"],
                    "route_revision": ["cluster"],
                },
                "additionalProperties": False,
            },
        },
        {
            "name": "relay_artifact_lineage",
            "description": "Query artifact lineage by job_id or artifact_id; includes produced-by.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "job_id": durable_record_id_json_schema(),
                    "artifact_id": durable_record_id_json_schema(),
                    "cluster": {"type": "string"},
                    "route_revision": cluster_route_revision_json_schema(),
                    "cursor": durable_record_id_json_schema(),
                    "limit": {
                        "type": "integer",
                        "default": DEFAULT_RESPONSE_PAGE_RECORDS,
                        "minimum": 1,
                        "maximum": MAX_RESPONSE_PAGE_RECORDS,
                    },
                },
                "if": {"required": ["job_id"]},
                "then": {"not": {"required": ["artifact_id"]}},
                "else": {"required": ["artifact_id"]},
                "dependentRequired": {
                    "cluster": ["route_revision"],
                    "route_revision": ["cluster"],
                },
                "additionalProperties": False,
            },
        },
        {
            "name": "relay_record_progress",
            "description": "Record a structured progress observation for a relay job.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "job_id": durable_record_id_json_schema(),
                    "label": {"type": "string", "default": "progress"},
                    "current": {"type": "number"},
                    "total": {"type": "number", "exclusiveMinimum": 0},
                    "unit": {"type": "string"},
                    "message": {"type": "string"},
                    "source_event_seq": {"type": "integer", "minimum": 1},
                    "metadata": {"type": "object", "default": {}},
                },
                "required": ["job_id"],
                "additionalProperties": False,
            },
        },
        {
            "name": "relay_list_progress",
            "description": (
                "List one stable page of structured progress observations for a relay job."
            ),
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
                "required": ["job_id"],
                "additionalProperties": False,
            },
        },
        {
            "name": "relay_read_artifact",
            "description": (
                "Read a model-readable file artifact payload as base64. Internal protocol and "
                "credential-bearing artifacts are intentionally unavailable through this tool. "
                "Pass the cluster and route_revision from the artifact's owning job's submission "
                "receipt to read an artifact registered on a configured remote cluster."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "artifact_id": durable_record_id_json_schema(),
                    "cluster": {"type": "string"},
                    "route_revision": cluster_route_revision_json_schema(),
                },
                "required": ["artifact_id"],
                "dependentRequired": {
                    "cluster": ["route_revision"],
                    "route_revision": ["cluster"],
                },
                "additionalProperties": False,
            },
        },
        {
            "name": "relay_cancel_job",
            "description": "Request cancellation for a queued, leased, or running relay job.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "job_id": durable_record_id_json_schema(),
                    "cluster": {"type": "string"},
                    "route_revision": cluster_route_revision_json_schema(),
                    "cancel_scheduler_job": {"type": "boolean", "default": False},
                },
                "required": ["job_id"],
                "additionalProperties": False,
            },
        },
    ]

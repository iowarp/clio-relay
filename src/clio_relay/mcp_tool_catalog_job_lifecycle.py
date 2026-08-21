"""Job-lifecycle MCP tool definitions: the remote-MCP-context helper plus
every tool for submitting a job (agent/JARVIS pipeline/JARVIS job/remote
agent/generic MCP call) and then observing it (status/cancel/observe/wait).

One of four tool-domain groups mcp_tool_catalog.py's `_all_tool_definitions`
assembles -- iowarp/clio-relay#231's real seam split for the >800-line
catalog concern (see mcp_tool_catalog.py's own module docstring for why the
split exists). Pure static JSON-schema data plus the two small shared
pieces only this group needs (the artifact-use-refs schema, the JARVIS
wait/observe description strings and the agent-log byte cap) -- no live
server/session state, so this is a leaf module the assembler imports from
with no back-reference.
"""

from __future__ import annotations

from typing import Any

from clio_relay.identifiers import durable_record_id_json_schema
from clio_relay.pagination import DEFAULT_RESPONSE_PAGE_RECORDS, MAX_RESPONSE_PAGE_RECORDS
from clio_relay.remote_mcp import cluster_route_revision_json_schema

JSON = dict[str, Any]

MAX_AGENT_LOG_READ_BYTES = 32_768

JARVIS_WAIT_FOR_TERMINAL_DESCRIPTION = (
    "Observe this submission until it becomes terminal within the current call. "
    "The observation bound never becomes a relay, JARVIS, or scheduler execution "
    "deadline and never fails, cancels, or resubmits the underlying job. Expiry returns "
    "the same receipt with observation.outcome=observation_unknown."
)
JARVIS_WAIT_TIMEOUT_DESCRIPTION = (
    "Maximum seconds to observe this submission in the current call when "
    "wait_for_terminal is true. Observation expiry never fails, cancels, or "
    "resubmits the underlying relay, JARVIS, or scheduler job; expiry returns the same "
    "receipt with observation.outcome=observation_unknown so it can be observed again later."
)
JARVIS_LEGACY_WAIT_TIMEOUT_DESCRIPTION = (
    "Deprecated observation-only alias for wait_timeout_seconds. It is not an "
    "execution deadline and never fails, cancels, or resubmits the underlying "
    "relay, JARVIS, or scheduler job. If both aliases are supplied, their values "
    "must be equal."
)


def _artifact_use_refs_json_schema() -> JSON:
    """Return the shared content-pinned artifact dependency schema."""
    return {
        "type": "array",
        "items": {
            "type": "object",
            "properties": {
                "artifact_id": durable_record_id_json_schema(),
                "sha256": {"type": "string", "pattern": "^[0-9a-fA-F]{64}$"},
                "provenance": {
                    "type": "object",
                    "properties": {
                        "schema_version": {
                            "type": "string",
                            "const": "clio-relay.artifact-use-provenance.v1",
                        },
                        "evidence": {
                            "type": "string",
                            "enum": [
                                "schema-arg",
                                "hash-pair",
                                "lease-window",
                                "authority",
                                "assertion",
                            ],
                        },
                        "authority": {"type": "string", "maxLength": 4096},
                        "external_ref": {"type": "string", "maxLength": 4096},
                        "arg": {"type": "string", "maxLength": 512},
                        "note": {"type": "string", "maxLength": 512},
                    },
                    "required": ["evidence"],
                    "additionalProperties": False,
                },
            },
            "required": ["artifact_id", "sha256"],
            "additionalProperties": False,
        },
        "maxItems": 1_000,
        "default": [],
    }


def _job_lifecycle_tool_definitions() -> list[JSON]:
    """Return remote-context/submission/status/cancel/observe/wait tool schemas."""
    return [
        {
            "name": "relay_remote_mcp_context",
            "description": (
                "Return agent instructions, cache revision, and availability diagnostics for "
                "clio-relay virtual remote MCP tools."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {},
                "additionalProperties": False,
            },
        },
        {
            "name": "relay_submit_agent",
            "description": "Submit a remote agent task to a configured relay cluster.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "cluster": {"type": "string"},
                    "prompt_path": {"type": "string"},
                    "mcp_config_path": {"type": "string"},
                    "model": {"type": "string"},
                    "workdir": {"type": "string"},
                    "timeout_seconds": {"type": "integer", "minimum": 1},
                    "request_followup_message": {
                        "type": "boolean",
                        "default": False,
                        "description": (
                            "After durable admission, park one bounded message round that "
                            "resumes through tasks/update."
                        ),
                    },
                    "idempotency_key": {"type": "string"},
                    "used_artifact_refs": _artifact_use_refs_json_schema(),
                    "wait_for_terminal": {"type": "boolean", "default": False},
                    "wait_timeout_seconds": {"type": "number", "default": 600},
                    "poll_seconds": {"type": "number", "default": 2},
                },
                "required": ["cluster", "prompt_path"],
                "additionalProperties": False,
            },
        },
        {
            "name": "relay_status",
            "description": (
                "Read relay job state, relay queue position, and scheduler status. For a "
                "remote job, copy cluster, job_id, and route_revision unchanged from its "
                "submission receipt on every follow-up call, including on the same MCP "
                "connection. job_id alone is only for a local relay job."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "job_id": durable_record_id_json_schema(),
                    "cluster": {"type": "string"},
                    "route_revision": cluster_route_revision_json_schema(),
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
            "name": "relay_cancel",
            "description": (
                "Request cancellation for a relay job. For a remote job, copy cluster, "
                "job_id, and route_revision unchanged from its submission receipt on every "
                "follow-up call, including on the same MCP connection. job_id alone is only "
                "for a local relay job."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "job_id": durable_record_id_json_schema(),
                    "cluster": {"type": "string"},
                    "route_revision": cluster_route_revision_json_schema(),
                    "cancel_scheduler_job": {"type": "boolean", "default": False},
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
            "name": "relay_observe",
            "description": (
                "Read job events from a cursor and optionally return when a regex pattern "
                "matches stdout, stderr, progress, or event text. Set until_pattern to "
                "hold this call open until the streamed output/events match, or until the "
                "job reaches terminal, whichever happens first; it never returns a TTL-shaped "
                "poll-me-again result. A match returns matched=true with match.stream, "
                "match.excerpt, match.position, and match.timestamp. Terminal without a "
                "match returns matched=false and the terminal job state. For a remote job, "
                "copy cluster, "
                "job_id, and route_revision unchanged from its submission receipt on every "
                "follow-up call, including on the same MCP connection. job_id alone is only "
                "for a local relay job."
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
                    "pattern": {"type": "string"},
                    "until_pattern": {
                        "type": "string",
                        "maxLength": 512,
                        "description": (
                            "Hold open until this regex matches or the job becomes terminal. "
                            "Invalid and potentially catastrophic regexes are typed refusals."
                        ),
                    },
                    "pattern_scope": {
                        "type": "array",
                        "items": {
                            "type": "string",
                            "enum": ["stdout", "stderr", "progress", "events"],
                        },
                        "uniqueItems": True,
                        "minItems": 1,
                        "default": ["events", "progress", "stderr", "stdout"],
                        "description": (
                            "Streams searched by until_pattern; defaults to all available streams."
                        ),
                    },
                    "include_logs": {"type": "boolean", "default": True},
                    "log_limit": {
                        "type": "integer",
                        "default": MAX_AGENT_LOG_READ_BYTES,
                        "minimum": 1,
                        "maximum": MAX_AGENT_LOG_READ_BYTES,
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
            "name": "relay_wait",
            "description": (
                "Observe a relay job for a bounded period and return final status, verified "
                "MCP result evidence, and optional logs if it finishes. Observation expiry "
                "never fails, cancels, or resubmits the underlying relay, JARVIS, or scheduler "
                "job; it returns current durable status with "
                "observation.outcome=observation_unknown. Preserve the receipt and call "
                "relay_wait again later. For a remote job, "
                "copy cluster, job_id, and "
                "route_revision unchanged from its submission receipt on every follow-up "
                "call, including on the same MCP connection. job_id alone is only for a "
                "local relay job. Treat mcp_result.structured_result as the authoritative "
                "remote tool output; do not call relay_observe merely to recover that result. "
                "A terminal jarvis_get_execution requested with "
                "include_service_runtimes=true returns service_runtime_bindings; pass one "
                "unchanged to relay_bind_jarvis_runtime, then use that bind result's "
                "gateway_session_id. Never use a JARVIS execution_id as gateway_session_id."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "job_id": durable_record_id_json_schema(),
                    "cluster": {"type": "string"},
                    "route_revision": cluster_route_revision_json_schema(),
                    "timeout_seconds": {
                        "type": "number",
                        "exclusiveMinimum": 0,
                        "default": 600,
                        "description": (
                            "Maximum seconds for this observation call. Expiry never changes "
                            "the underlying relay, JARVIS, or scheduler job state and never "
                            "fails, cancels, or resubmits that work."
                        ),
                    },
                    "poll_seconds": {"type": "number", "default": 2},
                    "include_logs": {"type": "boolean", "default": False},
                    "log_limit": {
                        "type": "integer",
                        "default": MAX_AGENT_LOG_READ_BYTES,
                        "minimum": 1,
                        "maximum": MAX_AGENT_LOG_READ_BYTES,
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
            "name": "relay_submit_jarvis_pipeline",
            "description": (
                "Submit a JARVIS pipeline YAML document to a configured relay cluster. "
                "Submission is asynchronous by default. Any requested wait bounds only the "
                "current observation; it never limits, fails, cancels, or resubmits the "
                "underlying relay, JARVIS, or scheduler job."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "cluster": {"type": "string"},
                    "pipeline_yaml": {"type": "string"},
                    "idempotency_key": {"type": "string"},
                    "used_artifact_refs": _artifact_use_refs_json_schema(),
                    "wait_for_terminal": {
                        "type": "boolean",
                        "default": False,
                        "description": JARVIS_WAIT_FOR_TERMINAL_DESCRIPTION,
                    },
                    "wait_timeout_seconds": {
                        "type": "number",
                        "exclusiveMinimum": 0,
                        "default": 600,
                        "description": JARVIS_WAIT_TIMEOUT_DESCRIPTION,
                    },
                    "timeout_seconds": {
                        "type": "number",
                        "exclusiveMinimum": 0,
                        "deprecated": True,
                        "description": JARVIS_LEGACY_WAIT_TIMEOUT_DESCRIPTION,
                    },
                    "poll_seconds": {"type": "number", "default": 2},
                },
                "required": ["cluster", "pipeline_yaml"],
                "additionalProperties": False,
            },
        },
        {
            "name": "relay_submit_jarvis_job",
            "description": (
                "Submit an existing JARVIS pipeline by name on a configured relay cluster. "
                "Submission is asynchronous by default. Any requested wait bounds only the "
                "current observation; it never limits, fails, cancels, or resubmits the "
                "underlying relay, JARVIS, or scheduler job."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "cluster": {"type": "string"},
                    "pipeline_name": {"type": "string"},
                    "idempotency_key": {"type": "string"},
                    "used_artifact_refs": _artifact_use_refs_json_schema(),
                    "wait_for_terminal": {
                        "type": "boolean",
                        "default": False,
                        "description": JARVIS_WAIT_FOR_TERMINAL_DESCRIPTION,
                    },
                    "wait_timeout_seconds": {
                        "type": "number",
                        "exclusiveMinimum": 0,
                        "default": 600,
                        "description": JARVIS_WAIT_TIMEOUT_DESCRIPTION,
                    },
                    "timeout_seconds": {
                        "type": "number",
                        "exclusiveMinimum": 0,
                        "deprecated": True,
                        "description": JARVIS_LEGACY_WAIT_TIMEOUT_DESCRIPTION,
                    },
                    "poll_seconds": {"type": "number", "default": 2},
                },
                "required": ["cluster", "pipeline_name"],
                "additionalProperties": False,
            },
        },
        {
            "name": "relay_submit_remote_agent",
            "description": "Submit a generic remote-agent task to a configured relay cluster.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "cluster": {"type": "string"},
                    "prompt_path": {"type": "string"},
                    "mcp_config_path": {"type": "string"},
                    "model": {"type": "string"},
                    "workdir": {"type": "string"},
                    "timeout_seconds": {"type": "integer", "minimum": 1},
                    "request_followup_message": {
                        "type": "boolean",
                        "default": False,
                        "description": (
                            "After durable admission, park one bounded message round that "
                            "resumes through tasks/update."
                        ),
                    },
                    "idempotency_key": {"type": "string"},
                    "used_artifact_refs": _artifact_use_refs_json_schema(),
                    "wait_for_terminal": {"type": "boolean", "default": False},
                    "wait_timeout_seconds": {"type": "number", "default": 600},
                    "poll_seconds": {"type": "number", "default": 2},
                },
                "required": ["cluster", "prompt_path"],
                "additionalProperties": False,
            },
        },
        {
            "name": "relay_submit_mcp_call",
            "description": "Submit a remote MCP tools/call task through a configured cluster.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "cluster": {"type": "string"},
                    "server": {"type": "string"},
                    "server_args": {
                        "type": "array",
                        "items": {"type": "string"},
                        "default": [],
                    },
                    "env_from": {
                        "type": "object",
                        "additionalProperties": {"type": "string"},
                        "default": {},
                        "description": (
                            "Child environment name to endpoint source environment name. "
                            "Values are references, never secret values."
                        ),
                    },
                    "tool": {"type": "string"},
                    "arguments": {"type": "object", "default": {}},
                    "timeout_seconds": {"type": "integer", "minimum": 1},
                    "idempotency_key": {"type": "string"},
                    "used_artifact_refs": _artifact_use_refs_json_schema(),
                    "wait_for_terminal": {"type": "boolean", "default": False},
                    "wait_timeout_seconds": {"type": "number", "default": 600},
                    "poll_seconds": {"type": "number", "default": 2},
                },
                "required": ["cluster", "server", "tool"],
                "additionalProperties": False,
            },
        },
        {
            "name": "relay_call_jarvis_mcp",
            "description": (
                "Submit a tool call to the target cluster's built-in JARVIS MCP server. "
                "The server is launched on the cluster with the clio-kit PyPI command."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "cluster": {"type": "string"},
                    "tool": {"type": "string"},
                    "arguments": {"type": "object", "default": {}},
                    "timeout_seconds": {"type": "integer", "minimum": 1},
                    "idempotency_key": {"type": "string"},
                    "used_artifact_refs": _artifact_use_refs_json_schema(),
                    "wait_for_terminal": {"type": "boolean", "default": False},
                    "wait_timeout_seconds": {"type": "number", "default": 600},
                    "poll_seconds": {"type": "number", "default": 2},
                },
                "required": ["cluster", "tool"],
                "additionalProperties": False,
            },
        },
    ]

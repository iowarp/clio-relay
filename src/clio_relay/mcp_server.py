"""Stdio MCP server for relay job submission tools."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import re
import sys
from datetime import datetime
from json import JSONDecodeError
from typing import Any, TextIO, cast
from uuid import uuid4

from pydantic import ValidationError

from clio_relay import __version__
from clio_relay.cluster_config import ClusterDefinition, ClusterRegistry, default_registry_path
from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.errors import ConfigurationError
from clio_relay.jarvis_mcp import (
    is_virtual_jarvis_tool,
    jarvis_mcp_artifact_binding,
    jarvis_mcp_server,
    jarvis_mcp_server_args,
    render_virtual_jarvis_agent_context,
    virtual_jarvis_call_arguments,
    virtual_jarvis_tool_definitions,
)
from clio_relay.models import (
    Cursor,
    GatewaySession,
    GatewaySessionState,
    JarvisRunSpec,
    JobKind,
    JobState,
    McpCallSpec,
    MonitorRule,
    MonitorRuleAction,
    ProgressRecord,
    RelayJob,
    RemoteAgentTaskSpec,
    TaskEventStatus,
    TaskTimelineEvent,
)
from clio_relay.pagination import (
    DEFAULT_RESPONSE_PAGE_RECORDS,
    MAX_RESPONSE_PAGE_RECORDS,
    validate_record_cursor,
    validate_response_page_limit,
)
from clio_relay.progress_provenance import external_progress_metadata
from clio_relay.queue_management import (
    cancel_queue_job,
    cleanup_stale_jobs,
    diagnose_job,
    discover_stale_jobs,
    list_queue_jobs,
    worker_status,
)
from clio_relay.relay_ops import (
    evaluate_monitor_rules,
    job_status,
    monitor_job,
    read_artifact_bytes,
    read_job_log,
    wait_for_terminal,
)
from clio_relay.remote_cli import (
    remove_remote_file,
    run_remote_clio,
    should_execute_on_cluster,
    write_remote_file,
)
from clio_relay.remote_mcp import (
    VirtualRemoteMcpCatalog,
    default_remote_mcp_cache_path,
    load_virtual_remote_mcp_catalog,
    unavailable_virtual_remote_mcp_catalog,
)
from clio_relay.retention import TerminalRetentionCoordinator
from clio_relay.spool import MAX_LOG_READ_BYTES
from clio_relay.storage_runtime import (
    StorageAdmissionError,
    StorageManagedQueue,
    storage_managed_queue,
)

JSON = dict[str, Any]
MCP_PROFILE_ENV = "CLIO_RELAY_MCP_PROFILE"
MAX_INTERNAL_COLLECTION_RECORDS = 10_000
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
}


def serve_stdio(
    *,
    stdin: TextIO = sys.stdin,
    stdout: TextIO = sys.stdout,
    settings: RelaySettings | None = None,
    profile: str | None = None,
) -> None:
    """Serve a minimal MCP JSON-RPC server over newline-delimited stdio."""
    resolved = settings or RelaySettings.from_env()
    resolved_profile = _normalize_profile(profile or _mcp_profile_from_env())
    queue = storage_managed_queue(resolved)
    queue.initialize()
    first_line = True
    for line in stdin:
        if first_line:
            line = line.removeprefix("\ufeff")
            first_line = False
        if not line.strip():
            continue
        try:
            request = json.loads(line)
        except JSONDecodeError as exc:
            response = _error(None, -32700, f"parse error: {exc.msg}")
        else:
            response = handle_request(
                request,
                queue=queue,
                settings=resolved,
                profile=resolved_profile,
            )
        if response is None:
            continue
        stdout.write(json.dumps(response, separators=(",", ":")) + "\n")
        stdout.flush()


def handle_request(
    request: JSON,
    *,
    queue: ClioCoreQueue,
    settings: RelaySettings | None = None,
    profile: str | None = None,
) -> JSON | None:
    """Handle one JSON-RPC MCP request."""
    request_id = request.get("id")
    method = request.get("method")
    resolved_profile = _normalize_profile(profile or _mcp_profile_from_env())
    if method == "notifications/initialized":
        return None
    try:
        if method == "initialize":
            result = _initialize_result()
        elif method == "tools/list":
            result = {"tools": _tool_definitions(profile=resolved_profile)}
        elif method == "tools/call":
            params = _object(request.get("params"))
            result = _call_tool(
                params,
                queue=queue,
                settings=settings or RelaySettings.from_env(),
                profile=resolved_profile,
            )
        else:
            return _error(request_id, -32601, f"unknown method: {method}")
    except StorageAdmissionError as exc:
        return _error(
            request_id,
            -32007,
            "relay storage admission denied",
            data={"storage_decision": exc.decision.to_dict()},
        )
    except Exception as exc:
        return _error(request_id, -32000, str(exc))
    return {"jsonrpc": "2.0", "id": request_id, "result": result}


def render_agent_mcp_profile(
    *,
    settings: RelaySettings | None = None,
) -> str:
    """Render an agent MCP profile TOML snippet for the relay MCP server."""
    resolved = settings or RelaySettings.from_env()
    registry_path = default_registry_path().expanduser().resolve()
    cache_path = default_remote_mcp_cache_path(registry_path=registry_path).expanduser().resolve()
    return "\n".join(
        [
            "[mcp_servers.clio-relay]",
            'command = "clio-relay"',
            'args = ["mcp-server"]',
            "",
            "[mcp_servers.clio-relay.env]",
            f"CLIO_RELAY_CORE_DIR = {_toml_string(str(resolved.core_dir))}",
            f"CLIO_RELAY_SPOOL_DIR = {_toml_string(str(resolved.spool_dir))}",
            f"CLIO_RELAY_CLUSTER_REGISTRY = {_toml_string(str(registry_path))}",
            f"CLIO_RELAY_REMOTE_MCP_CACHE = {_toml_string(str(cache_path))}",
            "",
        ]
    )


def render_codex_mcp_profile(
    *,
    settings: RelaySettings | None = None,
) -> str:
    """Render a Codex-compatible MCP profile TOML snippet for the relay MCP server."""
    return render_agent_mcp_profile(settings=settings)


def load_registered_remote_mcp_catalog(profile: str) -> VirtualRemoteMcpCatalog:
    """Load the exact registered-tool catalog used by this local MCP server."""
    normalized = _normalize_profile(profile)
    return load_virtual_remote_mcp_catalog(
        profile=normalized,
        reserved_names=static_mcp_tool_names(),
    )


def static_mcp_tool_names() -> set[str]:
    """Return built-in local tool names reserved from generated aliases."""
    return {str(tool["name"]) for tool in _all_tool_definitions()}


def _initialize_result() -> JSON:
    return {
        "protocolVersion": "2024-11-05",
        "capabilities": {"tools": {}},
        "serverInfo": {"name": "clio-relay", "version": __version__},
    }


def _tool_definitions(*, profile: str | None = None) -> list[JSON]:
    tools = _all_tool_definitions(clusters=_configured_cluster_names())
    normalized = _normalize_profile(profile or _mcp_profile_from_env())
    catalog = _remote_mcp_catalog(
        profile=normalized,
        reserved_names={str(tool["name"]) for tool in tools},
    )
    if normalized in {"admin", "operator", "all"}:
        selected = tools
    else:
        selected = [
            tool
            for tool in tools
            if tool["name"] in USER_MCP_TOOL_NAMES or is_virtual_jarvis_tool(str(tool["name"]))
        ]
    return [*selected, *catalog.tool_definitions()]


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


def _all_tool_definitions(*, clusters: list[str] | None = None) -> list[JSON]:
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
                    "idempotency_key": {"type": "string"},
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
            "description": "Read relay job state, relay queue position, and scheduler status.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "job_id": {"type": "string"},
                    "cluster": {"type": "string"},
                    "route_revision": {"type": "string"},
                },
                "required": ["job_id"],
                "additionalProperties": False,
            },
        },
        {
            "name": "relay_cancel",
            "description": "Request cancellation for a relay job.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "job_id": {"type": "string"},
                    "cluster": {"type": "string"},
                    "route_revision": {"type": "string"},
                    "cancel_scheduler_job": {"type": "boolean", "default": False},
                },
                "required": ["job_id"],
                "additionalProperties": False,
            },
        },
        {
            "name": "relay_observe",
            "description": (
                "Read job events from a cursor and optionally return when a regex pattern "
                "matches stdout, stderr, or event text."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "job_id": {"type": "string"},
                    "cluster": {"type": "string"},
                    "route_revision": {"type": "string"},
                    "cursor": {"type": "integer", "default": 1, "minimum": 1},
                    "limit": {
                        "type": "integer",
                        "default": DEFAULT_RESPONSE_PAGE_RECORDS,
                        "minimum": 1,
                        "maximum": MAX_RESPONSE_PAGE_RECORDS,
                    },
                    "pattern": {"type": "string"},
                    "include_logs": {"type": "boolean", "default": True},
                    "log_limit": {
                        "type": "integer",
                        "default": 65536,
                        "minimum": 1,
                        "maximum": MAX_LOG_READ_BYTES,
                    },
                },
                "required": ["job_id"],
                "additionalProperties": False,
            },
        },
        {
            "name": "relay_wait",
            "description": "Wait for a relay job to finish and return final status and logs.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "job_id": {"type": "string"},
                    "cluster": {"type": "string"},
                    "route_revision": {"type": "string"},
                    "timeout_seconds": {"type": "number", "default": 600},
                    "poll_seconds": {"type": "number", "default": 2},
                    "include_logs": {"type": "boolean", "default": True},
                    "log_limit": {
                        "type": "integer",
                        "default": 65536,
                        "minimum": 1,
                        "maximum": MAX_LOG_READ_BYTES,
                    },
                },
                "required": ["job_id"],
                "additionalProperties": False,
            },
        },
        {
            "name": "relay_submit_jarvis_pipeline",
            "description": "Submit a JARVIS pipeline YAML document to a configured relay cluster.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "cluster": {"type": "string"},
                    "pipeline_yaml": {"type": "string"},
                    "idempotency_key": {"type": "string"},
                    "wait_for_terminal": {"type": "boolean", "default": False},
                    "timeout_seconds": {"type": "number", "default": 600},
                    "poll_seconds": {"type": "number", "default": 2},
                },
                "required": ["cluster", "pipeline_yaml"],
                "additionalProperties": False,
            },
        },
        {
            "name": "relay_submit_jarvis_job",
            "description": (
                "Submit an existing JARVIS pipeline by name on a configured relay cluster."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "cluster": {"type": "string"},
                    "pipeline_name": {"type": "string"},
                    "idempotency_key": {"type": "string"},
                    "wait_for_terminal": {"type": "boolean", "default": False},
                    "timeout_seconds": {"type": "number", "default": 600},
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
                    "idempotency_key": {"type": "string"},
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
                    "wait_for_terminal": {"type": "boolean", "default": False},
                    "wait_timeout_seconds": {"type": "number", "default": 600},
                    "poll_seconds": {"type": "number", "default": 2},
                },
                "required": ["cluster", "tool"],
                "additionalProperties": False,
            },
        },
        {
            "name": "relay_get_job",
            "description": "Read a relay job record by id.",
            "inputSchema": {
                "type": "object",
                "properties": {"job_id": {"type": "string"}},
                "required": ["job_id"],
                "additionalProperties": False,
            },
        },
        {
            "name": "relay_get_job_status",
            "description": "Read job state, relay queue position, and scheduler status.",
            "inputSchema": {
                "type": "object",
                "properties": {"job_id": {"type": "string"}},
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
                    "job_id": {"type": "string"},
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
                    "job_id": {"type": "string"},
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
                    "job_id": {"type": "string"},
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
                    "task_id": {"type": "string"},
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
                        "items": {"type": "string"},
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
                    "task_id": {"type": "string"},
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
                    "job_id": {"type": "string"},
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
            "description": "List one stable page of artifact references indexed for a job.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "job_id": {"type": "string"},
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
            "name": "relay_record_progress",
            "description": "Record a structured progress observation for a relay job.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "job_id": {"type": "string"},
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
                    "job_id": {"type": "string"},
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
            "description": "Read a file artifact payload as base64.",
            "inputSchema": {
                "type": "object",
                "properties": {"artifact_id": {"type": "string"}},
                "required": ["artifact_id"],
                "additionalProperties": False,
            },
        },
        {
            "name": "relay_cancel_job",
            "description": "Request cancellation for a queued, leased, or running relay job.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "job_id": {"type": "string"},
                    "cluster": {"type": "string"},
                    "cancel_scheduler_job": {"type": "boolean", "default": False},
                },
                "required": ["job_id"],
                "additionalProperties": False,
            },
        },
        {
            "name": "relay_queue_list",
            "description": "List relay queue jobs with queue-position metadata.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "cluster": {"type": "string"},
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
                    "job_id": {"type": "string"},
                    "cluster": {"type": "string"},
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
                    "job_id": {"type": "string"},
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
                        "default": 1000,
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
                    "job_id": {"type": "string"},
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
                        "default": 1000,
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
                    "job_id": {"type": "string"},
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
                "properties": {"job_id": {"type": "string"}},
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
                    "job_id": {"type": "string"},
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
                "properties": {"cluster": {"type": "string"}},
                "additionalProperties": False,
            },
        },
        {
            "name": "relay_create_monitor_rule",
            "description": "Create a regex monitor rule over a job event stream.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "job_id": {"type": "string"},
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
                    "job_id": {"type": "string"},
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
                    "scheduler": {"type": "string", "default": "external"},
                    "scheduler_job_id": {"type": "string"},
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
                "properties": {"session_id": {"type": "string"}},
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
                    "session_id": {"type": "string"},
                    "state": {"type": "string"},
                    "scheduler_job_id": {"type": "string"},
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
                "properties": {"session_id": {"type": "string"}},
                "required": ["session_id"],
                "additionalProperties": False,
            },
        },
        *virtual_jarvis_tool_definitions(clusters=clusters),
    ]


def _mcp_profile_from_env() -> str:
    return os.environ.get(MCP_PROFILE_ENV, "user")


def _normalize_profile(profile: str) -> str:
    normalized = profile.strip().lower()
    if normalized in {"", "user", "agent"}:
        return "user"
    if normalized in {"admin", "operator", "all"}:
        return normalized
    raise ValueError("MCP profile must be user, admin, operator, or all")


def _call_tool(
    params: JSON,
    *,
    queue: ClioCoreQueue,
    settings: RelaySettings,
    profile: str,
) -> JSON:
    name = _required_str(params, "name")
    static_names = static_mcp_tool_names()
    catalog: VirtualRemoteMcpCatalog | None = None
    if name in static_names:
        if name not in _authorized_static_tool_names(profile):
            raise ValueError(f"tool is not available in MCP profile {profile!r}: {name}")
    else:
        catalog = _remote_mcp_catalog(profile=profile, reserved_names=static_names)
        if name not in catalog.tools:
            raise ValueError(f"tool is not available in MCP profile {profile!r}: {name}")
    arguments = _object(params.get("arguments", {}))
    if name == "relay_submit_jarvis_pipeline":
        result = _submit_jarvis_pipeline(arguments, queue=queue)
    elif name == "relay_storage_status":
        if not isinstance(queue, StorageManagedQueue):
            raise ValueError("MCP queue is not storage managed")
        result = queue.storage_runtime.status()
    elif name == "relay_remote_mcp_context":
        catalog = _remote_mcp_catalog(profile=profile, reserved_names=static_mcp_tool_names())
        result = {
            "context": _render_remote_mcp_context(catalog),
            "catalog_revision": catalog.revision,
            "virtual_remote_tools": sorted(catalog.tools),
            "catalog_issues": [issue.model_dump(mode="json") for issue in catalog.issues],
        }
    elif name == "relay_submit_agent":
        result = _submit_remote_agent(arguments, queue=queue)
    elif name == "relay_status":
        result = _status_job(arguments, queue=queue)
    elif name == "relay_cancel":
        result = _cancel_job(arguments, queue=queue)
    elif name == "relay_observe":
        result = _observe_job(arguments, queue=queue, settings=settings)
    elif name == "relay_wait":
        result = _wait_job(arguments, queue=queue, settings=settings)
    elif name == "relay_submit_jarvis_job":
        result = _submit_jarvis_job(arguments, queue=queue)
    elif name == "relay_submit_remote_agent":
        result = _submit_remote_agent(arguments, queue=queue)
    elif name == "relay_submit_mcp_call":
        result = _submit_mcp_call(arguments, queue=queue)
    elif name == "relay_call_jarvis_mcp":
        result = _submit_jarvis_mcp_call(arguments, queue=queue)
    elif is_virtual_jarvis_tool(name):
        result = _submit_jarvis_mcp_call(
            virtual_jarvis_call_arguments(name, arguments),
            queue=queue,
        )
    elif catalog is not None and name in catalog.tools:
        cluster = _required_str(arguments, "cluster")
        route = catalog.resolve(name, cluster)
        forwarded_arguments = catalog.forwarded_arguments(name, arguments)
        result = _submit_mcp_call(
            {
                "cluster": cluster,
                "registered_route": True,
                "server": route.command,
                "server_args": list(route.args),
                "env_from": dict(route.env_from),
                "expected_server_artifact_digest": route.expected_server_artifact_digest,
                "tool": route.remote_tool_name,
                "arguments": forwarded_arguments,
                "timeout_seconds": route.timeout_seconds,
                "idempotency_key": (
                    f"mcp:virtual:{cluster}:{route.server_name}:"
                    f"{route.remote_tool_name}:{uuid4().hex}"
                ),
            },
            queue=queue,
        )
    elif name == "relay_get_job":
        result = queue.get_job(_required_str(arguments, "job_id")).model_dump(mode="json")
    elif name == "relay_get_job_status":
        result = job_status(queue, _required_str(arguments, "job_id"))
    elif name == "relay_monitor_job":
        result = monitor_job(
            queue,
            _required_str(arguments, "job_id"),
            cursor=int(arguments.get("cursor", 1)),
            limit=_response_page_limit(arguments),
        )
    elif name == "relay_watch_job_events":
        events, cursor = queue.drain_events(
            Cursor(
                job_id=_required_str(arguments, "job_id"),
                next_seq=int(arguments.get("cursor", 1)),
            ),
            limit=_response_page_limit(arguments),
        )
        result = {
            "events": [event.model_dump(mode="json") for event in events],
            "next_cursor": cursor.next_seq,
        }
    elif name == "relay_list_tasks":
        cursor = _response_page_cursor(arguments)
        limit = _response_page_limit(arguments)
        tasks, next_cursor, total = queue.list_tasks_page(
            _required_str(arguments, "job_id"),
            cursor=cursor,
            limit=limit,
        )
        result = _record_page(
            "tasks",
            [task.model_dump(mode="json") for task in tasks],
            cursor=cursor,
            limit=limit,
            next_cursor=next_cursor,
            total=total,
        )
    elif name == "relay_record_task_event":
        result = _record_task_event(arguments, queue=queue)
    elif name == "relay_watch_task_events":
        events, cursor = queue.drain_task_events(
            _required_str(arguments, "task_id"),
            cursor=int(arguments.get("cursor", 1)),
            limit=_response_page_limit(arguments),
        )
        result = {
            "events": [event.model_dump(mode="json") for event in events],
            "next_cursor": cursor,
        }
    elif name == "relay_read_job_log":
        job = queue.get_job(_required_str(arguments, "job_id"))
        stream = _required_str(arguments, "stream")
        if stream not in {"stdout", "stderr"}:
            raise ValueError("stream must be stdout or stderr")
        result = read_job_log(
            settings,
            job,
            stream_name="stdout" if stream == "stdout" else "stderr",
            offset=int(arguments.get("offset", 0)),
            limit=_job_log_limit(arguments),
        )
    elif name == "relay_list_artifacts":
        cursor = _response_page_cursor(arguments)
        limit = _response_page_limit(arguments)
        artifacts, next_cursor, total = queue.list_artifacts_page(
            _required_str(arguments, "job_id"),
            cursor=cursor,
            limit=limit,
        )
        result = _record_page(
            "artifacts",
            [artifact.model_dump(mode="json") for artifact in artifacts],
            cursor=cursor,
            limit=limit,
            next_cursor=next_cursor,
            total=total,
        )
    elif name == "relay_read_artifact":
        result = read_artifact_bytes(queue, _required_str(arguments, "artifact_id"))
    elif name == "relay_record_progress":
        result = _record_progress(arguments, queue=queue)
    elif name == "relay_list_progress":
        cursor = _response_page_cursor(arguments)
        limit = _response_page_limit(arguments)
        progress, next_cursor, total = queue.list_progress_page(
            _required_str(arguments, "job_id"),
            cursor=cursor,
            limit=limit,
        )
        result = _record_page(
            "progress",
            [record.model_dump(mode="json") for record in progress],
            cursor=cursor,
            limit=limit,
            next_cursor=next_cursor,
            total=total,
        )
    elif name == "relay_cancel_job":
        result = cancel_queue_job(
            queue,
            _required_str(arguments, "job_id"),
            cluster=_optional_str(arguments, "cluster"),
            scheduler_policy=(
                "request-scheduler"
                if arguments.get("cancel_scheduler_job") is True
                else "relay-only"
            ),
        )
    elif name == "relay_queue_list":
        raw_state = arguments.get("state")
        state = JobState(raw_state) if isinstance(raw_state, str) else None
        raw_kind = arguments.get("kind")
        kind = JobKind(raw_kind) if isinstance(raw_kind, str) else None
        result = list_queue_jobs(
            queue,
            cluster=_optional_str(arguments, "cluster"),
            state=state,
            kind=kind,
            include_terminal=arguments.get("include_terminal") is True,
            cursor=_response_page_cursor(arguments),
            limit=_response_page_limit(arguments),
            scan_limit=int(arguments.get("scan_limit", 1000)),
        )
    elif name == "relay_queue_diagnose":
        result = diagnose_job(
            queue,
            _required_str(arguments, "job_id"),
            cluster=_optional_str(arguments, "cluster"),
            stale_after_seconds=int(arguments.get("older_than_seconds", 7200)),
            scan_limit=int(arguments.get("scan_limit", 1000)),
        )
    elif name == "relay_queue_stale":
        raw_kind = arguments.get("kind")
        result = discover_stale_jobs(
            queue,
            cluster=_required_str(arguments, "cluster"),
            older_than_seconds=int(arguments["older_than_seconds"]),
            job_id=_optional_str(arguments, "job_id"),
            kind=JobKind(raw_kind) if isinstance(raw_kind, str) else None,
            limit=int(arguments.get("limit", 100)),
            scan_limit=int(arguments.get("scan_limit", 1000)),
        )
    elif name == "relay_queue_cleanup_stale":
        raw_kind = arguments.get("kind")
        result = cleanup_stale_jobs(
            queue,
            cluster=_required_str(arguments, "cluster"),
            older_than_seconds=int(arguments.get("older_than_seconds", 7200)),
            job_id=_optional_str(arguments, "job_id"),
            kind=JobKind(raw_kind) if isinstance(raw_kind, str) else None,
            max_attempts=int(arguments.get("max_attempts", 3)),
            dry_run=arguments.get("dry_run", True) is not False,
            cancel_queued=arguments.get("cancel_queued") is True,
            limit=int(arguments.get("limit", 100)),
            scan_limit=int(arguments.get("scan_limit", 1000)),
        )
    elif name == "relay_retention_plan":
        plan = TerminalRetentionCoordinator(queue, settings.spool_dir).plan(
            _required_str(arguments, "job_id"),
            expected_updated_at=_optional_datetime_argument(
                arguments,
                "expected_updated_at",
            ),
        )
        result = {
            "plan": plan.model_dump(mode="json"),
            "scheduler_cancel_requested": False,
        }
    elif name == "relay_retention_status":
        job_id = _required_str(arguments, "job_id")
        plan = TerminalRetentionCoordinator(queue, settings.spool_dir).plan(job_id)
        result = {
            "job_id": job_id,
            "receipt_id": plan.receipt_id,
            "phase": None if plan.receipt_phase is None else plan.receipt_phase.value,
            "complete": plan.receipt_phase is not None and plan.receipt_phase.value == "complete",
            "eligible": plan.eligible,
            "protections": plan.protections,
            "scheduler_cancel_requested": False,
        }
    elif name == "relay_retention_collect":
        execute = arguments.get("execute") is True
        if execute and not isinstance(queue, StorageManagedQueue):
            raise ValueError("retention mutation requires a storage-managed queue")
        result = (
            TerminalRetentionCoordinator(queue, settings.spool_dir)
            .collect(
                _required_str(arguments, "job_id"),
                execute=execute,
                batch_size=_bounded_integer_limit(
                    arguments,
                    field_name="batch_size",
                    default=100,
                    maximum=100,
                ),
                expected_updated_at=_optional_datetime_argument(
                    arguments,
                    "expected_updated_at",
                ),
            )
            .model_dump(mode="json")
        )
    elif name == "relay_worker_status":
        result = worker_status(queue, cluster=_optional_str(arguments, "cluster"))
    elif name == "relay_create_monitor_rule":
        result = queue.append_monitor_rule(_monitor_rule_from_arguments(arguments)).model_dump(
            mode="json"
        )
    elif name == "relay_list_monitor_rules":
        job_id = arguments.get("job_id")
        if job_id is not None and not isinstance(job_id, str):
            raise ValueError("job_id must be a string")
        cursor = _response_page_cursor(arguments)
        limit = _response_page_limit(arguments)
        rules, next_cursor, total = queue.list_monitor_rules_page(
            cursor=cursor,
            limit=limit,
            job_id=job_id,
        )
        result = {
            "rules": [rule.model_dump(mode="json") for rule in rules],
            "source_cursor": cursor,
            "source_limit": limit,
            "source_next_cursor": next_cursor,
            "source_total": total,
            "source_total_semantics": "global_monitor_rule_sequence_high_water",
            "filters_apply_within_source_window": True,
        }
    elif name == "relay_evaluate_monitor_rules":
        result = {"actions": evaluate_monitor_rules(queue, limit=_response_page_limit(arguments))}
    elif name == "relay_create_gateway_session":
        result = _create_gateway_session(arguments, queue=queue)
    elif name == "relay_list_gateway_sessions":
        cursor = _response_page_cursor(arguments)
        limit = _response_page_limit(arguments)
        sessions, next_cursor, total = queue.list_gateway_sessions_page(
            cursor=cursor,
            limit=limit,
            cluster=_optional_str(arguments, "cluster"),
        )
        result = {
            "gateway_sessions": [session.model_dump(mode="json") for session in sessions],
            "source_cursor": cursor,
            "source_limit": limit,
            "source_next_cursor": next_cursor,
            "source_total": total,
            "source_total_semantics": "global_gateway_sequence_high_water",
            "filters_apply_within_source_window": True,
        }
    elif name == "relay_get_gateway_session":
        result = queue.get_gateway_session(_required_str(arguments, "session_id")).model_dump(
            mode="json"
        )
    elif name == "relay_update_gateway_session":
        result = _update_gateway_session(arguments, queue=queue)
    elif name == "relay_close_gateway_session":
        result = queue.close_gateway_session(_required_str(arguments, "session_id")).model_dump(
            mode="json"
        )
    else:
        raise ValueError(f"unknown tool: {name}")
    return {
        "content": [{"type": "text", "text": json.dumps(result, sort_keys=True)}],
        "structuredContent": result,
        "isError": False,
    }


def _remote_mcp_catalog(
    *,
    profile: str,
    reserved_names: set[str],
) -> VirtualRemoteMcpCatalog:
    try:
        return load_virtual_remote_mcp_catalog(
            profile=profile,
            reserved_names=reserved_names,
        )
    except (ConfigurationError, OSError, ValidationError) as exc:
        return unavailable_virtual_remote_mcp_catalog(str(exc))


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
    return _stable_digest(
        {
            "cluster": definition.name,
            "ssh_host": definition.ssh_host,
            "core_dir": definition.core_dir,
            "spool_dir": definition.spool_dir,
        }
    )


def _job_target(arguments: JSON) -> ClusterDefinition | None:
    """Resolve and verify an optional self-routing cluster job handle."""
    raw_cluster = arguments.get("cluster")
    raw_revision = arguments.get("route_revision")
    if raw_cluster is None:
        if raw_revision is not None:
            raise ValueError("route_revision requires cluster")
        return None
    if not isinstance(raw_cluster, str) or not raw_cluster:
        raise ValueError("cluster must be a non-empty string")
    definition = _remote_cluster_definition(raw_cluster)
    expected_revision = _route_revision(definition)
    if raw_revision is not None and raw_revision != expected_revision:
        raise ValueError(
            f"cluster route changed for {raw_cluster}; refuse to route an existing job handle"
        )
    return definition


def _require_local_job_cluster(
    queue: ClioCoreQueue,
    job_id: str,
    target: ClusterDefinition | None,
) -> None:
    if target is None:
        return
    job = queue.get_job(job_id)
    if job.cluster != target.name:
        raise ValueError(
            f"job {job_id} belongs to cluster {job.cluster}, not requested cluster {target.name}"
        )


def _status_job(arguments: JSON, *, queue: ClioCoreQueue) -> JSON:
    job_id = _required_str(arguments, "job_id")
    target = _job_target(arguments)
    if target is not None and should_execute_on_cluster(target):
        result = _remote_json(target, ["job", "status", job_id], "remote job status")
        result["cluster"] = target.name
        result["route_revision"] = _route_revision(target)
        return result
    _require_local_job_cluster(queue, job_id, target)
    result = job_status(queue, job_id)
    if target is not None:
        result["cluster"] = target.name
        result["route_revision"] = _route_revision(target)
    return result


def _remote_json(
    definition: ClusterDefinition,
    args: list[str],
    label: str,
) -> JSON:
    value = _remote_json_value(definition, args, label)
    if not isinstance(value, dict):
        raise ValueError(f"{label} must return a JSON object")
    return cast(JSON, value)


def _remote_json_value(
    definition: ClusterDefinition,
    args: list[str],
    label: str,
) -> object:
    output = run_remote_clio(definition, args)
    try:
        return cast(object, json.loads(output))
    except json.JSONDecodeError as exc:
        raise ValueError(f"{label} returned invalid JSON") from exc


def _complete_local_artifacts(queue: ClioCoreQueue, job_id: str) -> list[JSON]:
    """Read all artifacts under an explicit cap or reject incomplete evidence."""
    cursor = 1
    expected_total: int | None = None
    records: list[JSON] = []
    while True:
        page, next_cursor, total = queue.list_artifacts_page(
            job_id,
            cursor=cursor,
            limit=MAX_RESPONSE_PAGE_RECORDS,
        )
        expected_total = _validate_complete_collection_page(
            label=f"artifacts for {job_id}",
            cursor=cursor,
            page_count=len(page),
            next_cursor=next_cursor,
            total=total,
            expected_total=expected_total,
            collected_count=len(records),
        )
        records.extend(artifact.model_dump(mode="json") for artifact in page)
        if next_cursor is None:
            if len(records) != total:
                raise ValueError(f"artifacts for {job_id} changed during bounded discovery")
            return records
        cursor = next_cursor


def _complete_remote_collection(
    definition: ClusterDefinition,
    command: list[str],
    *,
    record_key: str,
    label: str,
) -> list[JSON]:
    """Drain a remote paged CLI collection or reject partial/moving evidence."""
    cursor = 1
    expected_total: int | None = None
    records: list[JSON] = []
    while True:
        payload = _remote_json(
            definition,
            [
                *command,
                "--cursor",
                str(cursor),
                "--limit",
                str(MAX_RESPONSE_PAGE_RECORDS),
            ],
            label,
        )
        raw_records = payload.get(record_key)
        if not isinstance(raw_records, list):
            raise ValueError(f"{label} must contain a {record_key} array")
        page: list[JSON] = []
        for item in cast(list[object], raw_records):
            if not isinstance(item, dict):
                raise ValueError(f"{label} returned a non-object {record_key} entry")
            page.append(cast(JSON, item))
        total = payload.get("total")
        returned_cursor = payload.get("cursor")
        returned_limit = payload.get("limit")
        next_cursor = payload.get("next_cursor")
        if isinstance(total, bool) or not isinstance(total, int) or total < 0:
            raise ValueError(f"{label} returned an invalid total")
        if returned_cursor != cursor or returned_limit != MAX_RESPONSE_PAGE_RECORDS:
            raise ValueError(f"{label} returned inconsistent page metadata")
        if next_cursor is not None and (
            isinstance(next_cursor, bool) or not isinstance(next_cursor, int)
        ):
            raise ValueError(f"{label} returned an invalid next_cursor")
        expected_total = _validate_complete_collection_page(
            label=label,
            cursor=cursor,
            page_count=len(page),
            next_cursor=next_cursor,
            total=total,
            expected_total=expected_total,
            collected_count=len(records),
        )
        records.extend(page)
        if next_cursor is None:
            if len(records) != total:
                raise ValueError(f"{label} changed during bounded discovery")
            return records
        cursor = next_cursor


def _validate_complete_collection_page(
    *,
    label: str,
    cursor: int,
    page_count: int,
    next_cursor: int | None,
    total: int,
    expected_total: int | None,
    collected_count: int,
) -> int:
    """Reject oversized, discontinuous, or moving internal page chains."""
    if total > MAX_INTERNAL_COLLECTION_RECORDS:
        raise ValueError(
            f"{label} exceeds the bounded completeness limit {MAX_INTERNAL_COLLECTION_RECORDS}"
        )
    if expected_total is not None and total != expected_total:
        raise ValueError(f"{label} changed during bounded discovery")
    if collected_count + page_count > total:
        raise ValueError(f"{label} returned more records than its total")
    if next_cursor is not None and (
        page_count == 0 or next_cursor != cursor + page_count or next_cursor > total
    ):
        raise ValueError(f"{label} returned a non-contiguous page cursor")
    if next_cursor is None and collected_count + page_count != total:
        raise ValueError(f"{label} ended before its declared total")
    return total


def _remote_job_logs(
    definition: ClusterDefinition,
    job_id: str,
    *,
    limit: int,
) -> JSON:
    return {
        stream: _remote_json(
            definition,
            [
                "job",
                "read-log",
                job_id,
                "--stream",
                stream,
                "--offset",
                "0",
                "--limit",
                str(limit),
            ],
            f"remote {stream} log",
        )
        for stream in ("stdout", "stderr")
    }


def _verified_mcp_result(
    definition: ClusterDefinition,
    job_id: str,
    artifacts: list[JSON],
) -> JSON | None:
    artifact = next(
        (
            item
            for item in artifacts
            if item.get("kind") == "mcp_result" and item.get("job_id") == job_id
        ),
        None,
    )
    if artifact is None:
        return None
    artifact_id = artifact.get("artifact_id")
    if not isinstance(artifact_id, str) or not artifact_id:
        raise ValueError("remote MCP result artifact has no artifact_id")
    envelope = _remote_json(
        definition,
        ["job", "read-artifact", artifact_id],
        "remote MCP result artifact",
    )
    return _decode_verified_mcp_result(envelope, artifact=artifact, job_id=job_id)


def _verified_local_mcp_result(queue: ClioCoreQueue, job_id: str) -> JSON | None:
    artifact = next(
        (
            item
            for item in _complete_local_artifacts(queue, job_id)
            if item.get("kind") == "mcp_result" and item.get("job_id") == job_id
        ),
        None,
    )
    if artifact is None:
        return None
    artifact_id = artifact.get("artifact_id")
    if not isinstance(artifact_id, str) or not artifact_id:
        raise ValueError("local MCP result artifact has no artifact_id")
    envelope = cast(JSON, read_artifact_bytes(queue, artifact_id))
    return _decode_verified_mcp_result(
        envelope,
        artifact=artifact,
        job_id=job_id,
    )


def _decode_verified_mcp_result(envelope: JSON, *, artifact: JSON, job_id: str) -> JSON:
    envelope_artifact = envelope.get("artifact")
    if not isinstance(envelope_artifact, dict):
        raise ValueError("MCP result artifact envelope is missing durable metadata")
    typed_envelope_artifact = cast(JSON, envelope_artifact)
    for key in ("artifact_id", "job_id", "sha256"):
        if typed_envelope_artifact.get(key) != artifact.get(key):
            raise ValueError(f"MCP result artifact envelope {key} does not match durable metadata")
    if artifact.get("job_id") != job_id:
        raise ValueError("MCP result artifact belongs to a different job")
    expected_sha256 = artifact.get("sha256")
    encoded = envelope.get("data")
    if not isinstance(expected_sha256, str) or len(expected_sha256) != 64:
        raise ValueError("MCP result artifact has no valid SHA-256")
    if envelope.get("encoding") != "base64" or not isinstance(encoded, str):
        raise ValueError("MCP result artifact envelope must contain base64 data")
    try:
        payload = base64.b64decode(encoded, validate=True)
    except ValueError as exc:
        raise ValueError("MCP result artifact contains invalid base64") from exc
    observed_sha256 = hashlib.sha256(payload).hexdigest()
    if not hmac.compare_digest(observed_sha256, expected_sha256):
        raise ValueError("MCP result artifact SHA-256 does not match durable metadata")
    try:
        document = json.loads(payload.decode("utf-8-sig"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("MCP result artifact must contain UTF-8 JSON") from exc
    if not isinstance(document, dict):
        raise ValueError("MCP result artifact must contain a JSON object")
    typed = cast(JSON, document)
    return {
        key: typed.get(key)
        for key in (
            "operation",
            "tool",
            "returncode",
            "timed_out",
            "protocol_error",
            "structured_result",
            "protocol_result",
            "protocol_version",
            "server_info",
        )
    }


def _render_remote_mcp_context(catalog: VirtualRemoteMcpCatalog) -> str:
    generic = (
        " Registered remote MCP tools are exposed with remote_<server>_<tool> aliases; "
        "their cluster argument selects the execution target and is not forwarded to the "
        "remote tool. Operators explicitly refresh the durable schema cache before new or "
        "changed tools appear."
    )
    available = ""
    if catalog.tools:
        available = " Available registered aliases: " + ", ".join(sorted(catalog.tools)) + "."
    return render_virtual_jarvis_agent_context() + generic + available


def _cancel_job(arguments: JSON, *, queue: ClioCoreQueue) -> JSON:
    target = _job_target(arguments)
    if target is not None and should_execute_on_cluster(target):
        job_id = _required_str(arguments, "job_id")
        command = ["job", "cancel", job_id]
        cancel_scheduler = arguments.get("cancel_scheduler_job") is True
        if cancel_scheduler:
            command.append("--cancel-scheduler-job")
        run_remote_clio(target, command)
        result = _remote_json(target, ["job", "status", job_id], "remote job status")
        result["cancel_requested"] = True
        result["scheduler_policy"] = "request-scheduler" if cancel_scheduler else "relay-only"
        result["cluster"] = target.name
        result["route_revision"] = _route_revision(target)
        return result
    _require_local_job_cluster(queue, _required_str(arguments, "job_id"), target)
    result = cancel_queue_job(
        queue,
        _required_str(arguments, "job_id"),
        scheduler_policy=(
            "request-scheduler" if arguments.get("cancel_scheduler_job") is True else "relay-only"
        ),
    )
    job = _object(result["job"])
    response: JSON = {
        **result,
        "job_id": job["job_id"],
        "state": job["state"],
    }
    if target is not None:
        response["cluster"] = target.name
        response["route_revision"] = _route_revision(target)
    return response


def _observe_job(arguments: JSON, *, queue: ClioCoreQueue, settings: RelaySettings) -> JSON:
    job_id = _required_str(arguments, "job_id")
    cursor = int(arguments.get("cursor", 1))
    limit = _response_page_limit(arguments)
    target = _job_target(arguments)
    if target is not None and should_execute_on_cluster(target):
        observed = _remote_json(
            target,
            ["job", "monitor", job_id, "--cursor", str(cursor), "--limit", str(limit)],
            "remote job monitor",
        )
    else:
        _require_local_job_cluster(queue, job_id, target)
        observed = monitor_job(queue, job_id, cursor=cursor, limit=limit)
    pattern = _optional_str(arguments, "pattern")
    matches: list[JSON] = []
    logs: JSON | None = None
    if arguments.get("include_logs", True) is not False:
        log_limit = _log_limit(arguments)
        logs = (
            _remote_job_logs(target, job_id, limit=log_limit)
            if target is not None and should_execute_on_cluster(target)
            else _job_logs(queue, settings, job_id, limit=log_limit)
        )
    if pattern is not None:
        compiled = re.compile(pattern)
        for event in cast(list[JSON], observed.get("events", [])):
            for text in _event_match_candidates(event):
                for match in compiled.finditer(text):
                    matches.append(
                        {
                            "event_seq": event.get("seq"),
                            "event_type": event.get("event_type"),
                            "text": text,
                            "match": match.group(0),
                            "groups": list(match.groups()),
                            "groupdict": match.groupdict(),
                        }
                    )
        if logs is not None:
            for stream_name in ("stdout", "stderr"):
                stream = _object(logs[stream_name])
                text = stream.get("text")
                if not isinstance(text, str):
                    continue
                for match in compiled.finditer(text):
                    matches.append(
                        {
                            "source": stream_name,
                            "text": text,
                            "match": match.group(0),
                            "groups": list(match.groups()),
                            "groupdict": match.groupdict(),
                        }
                    )
    result: JSON = {**observed, "matched": bool(matches), "matches": matches}
    if logs is not None:
        result["logs"] = logs
    if target is not None:
        result["cluster"] = target.name
        result["route_revision"] = _route_revision(target)
    return result


def _wait_job(arguments: JSON, *, queue: ClioCoreQueue, settings: RelaySettings) -> JSON:
    job_id = _required_str(arguments, "job_id")
    target = _job_target(arguments)
    if target is not None and should_execute_on_cluster(target):
        run_remote_clio(
            target,
            [
                "job",
                "wait",
                job_id,
                "--timeout-seconds",
                str(float(arguments.get("timeout_seconds", 600))),
                "--poll-seconds",
                str(float(arguments.get("poll_seconds", 2))),
            ],
        )
        result = _remote_json(target, ["job", "status", job_id], "remote job status")
        if arguments.get("include_logs", True) is not False:
            result["logs"] = _remote_job_logs(
                target,
                job_id,
                limit=_log_limit(arguments),
            )
        artifact_records = _complete_remote_collection(
            target,
            ["job", "list-artifacts", job_id],
            record_key="artifacts",
            label=f"remote artifacts for {job_id}",
        )
        result["artifacts"] = artifact_records
        parsed_result = _verified_mcp_result(target, job_id, artifact_records)
        if parsed_result is not None:
            result["mcp_result"] = parsed_result
        result["cluster"] = target.name
        result["route_revision"] = _route_revision(target)
        return result
    _require_local_job_cluster(queue, job_id, target)
    job = wait_for_terminal(
        queue,
        job_id,
        timeout_seconds=float(arguments.get("timeout_seconds", 600)),
        poll_seconds=float(arguments.get("poll_seconds", 2)),
    )
    result = job_status(queue, job.job_id)
    if arguments.get("include_logs", True) is not False:
        result["logs"] = _job_logs(
            queue,
            settings,
            job.job_id,
            limit=_log_limit(arguments),
        )
    result["artifacts"] = _complete_local_artifacts(queue, job.job_id)
    parsed_result = _verified_local_mcp_result(queue, job.job_id)
    if parsed_result is not None:
        result["mcp_result"] = parsed_result
    if target is not None:
        result["cluster"] = target.name
        result["route_revision"] = _route_revision(target)
    return result


def _job_logs(
    queue: ClioCoreQueue,
    settings: RelaySettings,
    job_id: str,
    *,
    limit: int,
) -> JSON:
    job = queue.get_job(job_id)
    return {
        "stdout": read_job_log(settings, job, stream_name="stdout", offset=0, limit=limit),
        "stderr": read_job_log(settings, job, stream_name="stderr", offset=0, limit=limit),
    }


def _event_match_candidates(event: JSON) -> list[str]:
    candidates: list[str] = []
    for key in ("message", "event_type"):
        value = event.get(key)
        if isinstance(value, str):
            candidates.append(value)
    payload = event.get("payload")
    if isinstance(payload, dict):
        typed_payload = cast(JSON, payload)
        for key in ("text", "stdout", "stderr", "message"):
            value = typed_payload.get(key)
            if isinstance(value, str):
                candidates.append(value)
    return candidates


def _submit_jarvis_pipeline(arguments: JSON, *, queue: ClioCoreQueue) -> JSON:
    cluster = _required_str(arguments, "cluster")
    pipeline_yaml = _required_str(arguments, "pipeline_yaml")
    digest = hashlib.sha256(pipeline_yaml.encode("utf-8")).hexdigest()
    idempotency_key = str(arguments.get("idempotency_key") or f"mcp:jarvis:{cluster}:{digest}")
    job = queue.submit_job(
        RelayJob(
            cluster=cluster,
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml=pipeline_yaml),
            idempotency_key=idempotency_key,
        )
    )
    if bool(arguments.get("wait_for_terminal", False)):
        job = wait_for_terminal(
            queue,
            job.job_id,
            timeout_seconds=float(arguments.get("timeout_seconds", 600)),
            poll_seconds=float(arguments.get("poll_seconds", 2)),
        )
    return {
        "job_id": job.job_id,
        "state": job.state.value,
        "kind": job.kind.value,
        "terminal": job.state.value in {"succeeded", "failed", "canceled"},
    }


def _submit_jarvis_job(arguments: JSON, *, queue: ClioCoreQueue) -> JSON:
    cluster = _required_str(arguments, "cluster")
    definition = _optional_cluster_definition(cluster)
    if definition is not None and should_execute_on_cluster(definition):
        remote_args = [
            "job",
            "submit-pipeline",
            "--cluster",
            cluster,
            "--pipeline-name",
            _required_str(arguments, "pipeline_name"),
            "--idempotency-key",
            str(
                arguments.get("idempotency_key")
                or f"mcp:jarvis-job:{cluster}:{_required_str(arguments, 'pipeline_name')}"
            ),
        ]
        output = run_remote_clio(definition, remote_args)
        return _remote_submission_result(output, kind=JobKind.JARVIS, definition=definition)
    pipeline_name = _required_str(arguments, "pipeline_name")
    idempotency_key = str(
        arguments.get("idempotency_key") or f"mcp:jarvis-job:{cluster}:{pipeline_name}"
    )
    job = queue.submit_job(
        RelayJob(
            cluster=cluster,
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_name=pipeline_name),
            idempotency_key=idempotency_key,
        )
    )
    return _submission_result(job, arguments, queue=queue, definition=definition)


def _submit_remote_agent(arguments: JSON, *, queue: ClioCoreQueue) -> JSON:
    cluster = _required_str(arguments, "cluster")
    prompt_path = _required_str(arguments, "prompt_path")
    mcp_config_path = _optional_str(arguments, "mcp_config_path")
    model = _optional_str(arguments, "model")
    workdir = _optional_str(arguments, "workdir")
    timeout_seconds = _optional_int(arguments, "timeout_seconds")
    idempotency_key = str(
        arguments.get("idempotency_key")
        or "mcp:remote-agent:"
        + _stable_digest(
            {
                "cluster": cluster,
                "prompt_path": prompt_path,
                "mcp_config_path": mcp_config_path,
                "model": model,
                "workdir": workdir,
                "timeout_seconds": timeout_seconds,
            }
        )
    )
    job = queue.submit_job(
        RelayJob(
            cluster=cluster,
            kind=JobKind.REMOTE_AGENT,
            spec=RemoteAgentTaskSpec(
                prompt_path=prompt_path,
                mcp_config_path=mcp_config_path,
                model=model,
                workdir=workdir,
                timeout_seconds=timeout_seconds,
            ),
            idempotency_key=idempotency_key,
        )
    )
    return _submission_result(job, arguments, queue=queue)


def _submit_mcp_call(arguments: JSON, *, queue: ClioCoreQueue) -> JSON:
    cluster = _required_str(arguments, "cluster")
    server = _required_str(arguments, "server")
    server_args = _string_list(arguments.get("server_args", []), "server_args")
    env_from = _string_mapping(arguments.get("env_from", {}), "env_from")
    expected_server_artifact_digest = _optional_str(
        arguments,
        "expected_server_artifact_digest",
    )
    tool = _required_str(arguments, "tool")
    tool_arguments = _object(arguments.get("arguments", {}))
    timeout_seconds = _optional_int(arguments, "timeout_seconds")
    digest = hashlib.sha256(
        json.dumps(tool_arguments, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    idempotency_key = str(
        arguments.get("idempotency_key")
        or "mcp:mcp-call:"
        + _stable_digest(
            {
                "cluster": cluster,
                "server": server,
                "server_args": server_args,
                "env_from": env_from,
                "expected_server_artifact_digest": expected_server_artifact_digest,
                "tool": tool,
                "arguments_digest": digest,
                "timeout_seconds": timeout_seconds,
            }
        )
    )
    registered_route = arguments.get("registered_route") is True
    definition = (
        _remote_cluster_definition(cluster)
        if registered_route
        else _optional_cluster_definition(cluster)
    )
    if definition is not None and should_execute_on_cluster(definition):
        remote_args_path = (
            ".local/share/clio-relay/desktop-submissions/"
            f"mcp-{_stable_digest({'cluster': cluster, 'tool': tool, 'arguments': tool_arguments})}"
            f"-{uuid4().hex}"
            "/arguments.json"
        )
        remote_args = [
            "mcp-call",
            "--cluster",
            cluster,
            "--server",
            server,
            "--tool",
            tool,
            "--arguments-json-file",
            remote_args_path,
            "--idempotency-key",
            idempotency_key,
        ]
        if timeout_seconds is not None:
            remote_args.extend(["--timeout-seconds", str(timeout_seconds)])
        for item in server_args:
            remote_args.extend(["--server-arg", item])
        for child_name, source_name in sorted(env_from.items()):
            remote_args.extend(["--env-from", f"{child_name}={source_name}"])
        if expected_server_artifact_digest is not None:
            remote_args.extend(
                ["--expected-server-artifact-digest", expected_server_artifact_digest]
            )
        try:
            write_remote_file(
                definition,
                remote_args_path,
                json.dumps(tool_arguments, sort_keys=True, separators=(",", ":")).encode("utf-8"),
            )
            output = run_remote_clio(definition, remote_args)
        finally:
            remove_remote_file(definition, remote_args_path, remove_empty_parent=True)
        return _remote_submission_result(output, kind=JobKind.MCP_CALL, definition=definition)
    job = queue.submit_job(
        RelayJob(
            cluster=cluster,
            kind=JobKind.MCP_CALL,
            spec=McpCallSpec(
                server=server,
                server_args=server_args,
                env_from=env_from,
                expected_server_artifact_digest=expected_server_artifact_digest,
                tool=tool,
                arguments=tool_arguments,
                timeout_seconds=timeout_seconds,
            ),
            idempotency_key=idempotency_key,
        )
    )
    return _submission_result(job, arguments, queue=queue, definition=definition)


def _remote_cluster_definition(cluster: str) -> ClusterDefinition:
    registry_path = default_registry_path()
    if not registry_path.exists():
        raise ValueError(f"cluster is not configured: {cluster}")
    registry = ClusterRegistry.load(registry_path)
    return registry.require(cluster)


def _optional_cluster_definition(cluster: str) -> ClusterDefinition | None:
    registry_path = default_registry_path()
    if not registry_path.exists():
        return None
    return ClusterRegistry.load(registry_path).clusters.get(cluster)


def _remote_submission_result(
    output: str,
    *,
    kind: JobKind,
    definition: ClusterDefinition,
) -> JSON:
    job_id = output.strip().splitlines()[-1].strip()
    return {
        "cluster": definition.name,
        "job_id": job_id,
        "state": JobState.QUEUED.value,
        "kind": kind.value,
        "terminal": False,
        "remote": True,
        "route_revision": _route_revision(definition),
    }


def _submit_jarvis_mcp_call(arguments: JSON, *, queue: ClioCoreQueue) -> JSON:
    forwarded = dict(arguments)
    cluster = _required_str(arguments, "cluster")
    tool = _required_str(arguments, "tool")
    tool_arguments = _object(arguments.get("arguments", {}))
    digest = hashlib.sha256(
        json.dumps(tool_arguments, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    idempotency_key = str(
        forwarded.get("idempotency_key") or f"mcp:{cluster}:jarvis:{tool}:{digest}"
    )
    forwarded["idempotency_key"] = idempotency_key
    registered_route = arguments.get("registered_route") is True
    definition = (
        _remote_cluster_definition(cluster)
        if registered_route
        else _optional_cluster_definition(cluster)
    )
    expected_server_artifact_digest = (
        jarvis_mcp_artifact_binding(cluster) if registered_route else None
    )
    if expected_server_artifact_digest is not None:
        forwarded["expected_server_artifact_digest"] = expected_server_artifact_digest
    if definition is not None and should_execute_on_cluster(definition):
        routing_digest = _stable_digest(
            {"cluster": cluster, "tool": tool, "arguments": tool_arguments}
        )
        remote_args_path = (
            ".local/share/clio-relay/desktop-submissions/"
            f"jarvis-mcp-{routing_digest}-{uuid4().hex}/arguments.json"
        )
        remote_args = [
            "jarvis-mcp-call",
            "--cluster",
            cluster,
            "--tool",
            tool,
            "--arguments-json-file",
            remote_args_path,
            "--idempotency-key",
            idempotency_key,
        ]
        timeout_seconds = _optional_int(arguments, "timeout_seconds")
        if timeout_seconds is not None:
            remote_args.extend(["--timeout-seconds", str(timeout_seconds)])
        if expected_server_artifact_digest is not None:
            remote_args.extend(
                ["--expected-server-artifact-digest", expected_server_artifact_digest]
            )
        try:
            write_remote_file(
                definition,
                remote_args_path,
                json.dumps(tool_arguments, sort_keys=True, separators=(",", ":")).encode("utf-8"),
            )
            output = run_remote_clio(definition, remote_args)
        finally:
            remove_remote_file(definition, remote_args_path, remove_empty_parent=True)
        return _remote_submission_result(output, kind=JobKind.MCP_CALL, definition=definition)
    server = jarvis_mcp_server()
    server_args = jarvis_mcp_server_args()
    forwarded["server"] = server
    forwarded["server_args"] = server_args
    return _submit_mcp_call(forwarded, queue=queue)


def _submission_result(
    job: RelayJob,
    arguments: JSON,
    *,
    queue: ClioCoreQueue,
    definition: ClusterDefinition | None = None,
) -> JSON:
    if bool(arguments.get("wait_for_terminal", False)):
        job = wait_for_terminal(
            queue,
            job.job_id,
            timeout_seconds=float(arguments.get("wait_timeout_seconds", 600)),
            poll_seconds=float(arguments.get("poll_seconds", 2)),
        )
    result: JSON = {
        "cluster": job.cluster,
        "job_id": job.job_id,
        "state": job.state.value,
        "kind": job.kind.value,
        "terminal": job.state.value in {"succeeded", "failed", "canceled"},
    }
    if definition is not None:
        result["route_revision"] = _route_revision(definition)
    return result


def _monitor_rule_from_arguments(arguments: JSON) -> MonitorRule:
    action_payload = arguments.get("action_payload", {})
    if not isinstance(action_payload, dict):
        raise ValueError("action_payload must be an object")
    event_types_value = arguments.get("event_types", [])
    if not isinstance(event_types_value, list):
        raise ValueError("event_types must be a string array")
    event_type_items = cast(list[object], event_types_value)
    if not all(isinstance(item, str) for item in event_type_items):
        raise ValueError("event_types must be a string array")
    event_types = cast(list[str], event_type_items)
    return MonitorRule(
        job_id=_required_str(arguments, "job_id"),
        pattern=_required_str(arguments, "pattern"),
        action=MonitorRuleAction(str(arguments.get("action", "emit_event"))),
        event_types=event_types,
        action_payload=cast(dict[str, Any], action_payload),
    )


def _record_progress(arguments: JSON, *, queue: ClioCoreQueue) -> JSON:
    metadata = arguments.get("metadata", {})
    if not isinstance(metadata, dict):
        raise ValueError("metadata must be an object")
    typed_metadata = external_progress_metadata("external_mcp", cast(dict[str, Any], metadata))
    progress = queue.append_progress(
        ProgressRecord(
            job_id=_required_str(arguments, "job_id"),
            label=str(arguments.get("label", "progress")),
            current=_optional_float(arguments, "current"),
            total=_optional_float(arguments, "total"),
            unit=_optional_str(arguments, "unit"),
            message=_optional_str(arguments, "message"),
            source_event_seq=_optional_int(arguments, "source_event_seq"),
            metadata=typed_metadata,
        )
    )
    return progress.model_dump(mode="json")


def _record_task_event(arguments: JSON, *, queue: ClioCoreQueue) -> JSON:
    metadata = _object(arguments.get("metadata", {}))
    event = queue.append_task_event(
        TaskTimelineEvent(
            task_id=_required_str(arguments, "task_id"),
            event_type=_required_str(arguments, "event_type"),
            label=_required_str(arguments, "label"),
            status=TaskEventStatus(str(arguments.get("status", "running"))),
            summary=_required_str(arguments, "summary"),
            detail=_optional_str(arguments, "detail"),
            artifact_refs=_string_list(arguments.get("artifact_refs", []), "artifact_refs"),
            path_refs=_string_list(arguments.get("path_refs", []), "path_refs"),
            metadata=metadata,
        )
    )
    return event.model_dump(mode="json")


def _create_gateway_session(arguments: JSON, *, queue: ClioCoreQueue) -> JSON:
    session = queue.create_gateway_session(
        GatewaySession(
            cluster=_required_str(arguments, "cluster"),
            name=_required_str(arguments, "name"),
            state=GatewaySessionState(str(arguments.get("state", "created"))),
            scheduler=str(arguments.get("scheduler", "external")),
            scheduler_job_id=_optional_str(arguments, "scheduler_job_id"),
            queue_state=_optional_str(arguments, "queue_state"),
            node=_optional_str(arguments, "node"),
            requested_resources=_object(arguments.get("requested_resources", {})),
            stdout_uri=_optional_str(arguments, "stdout_uri"),
            stderr_uri=_optional_str(arguments, "stderr_uri"),
            log_uris=_string_list(arguments.get("log_uris", []), "log_uris"),
            gateway=_object(arguments.get("gateway", {})),
            metadata=_object(arguments.get("metadata", {})),
        )
    )
    return session.model_dump(mode="json")


def _update_gateway_session(arguments: JSON, *, queue: ClioCoreQueue) -> JSON:
    updates: dict[str, object] = {}
    for key in {
        "scheduler_job_id",
        "queue_state",
        "node",
        "stdout_uri",
        "stderr_uri",
    }:
        value = arguments.get(key)
        if isinstance(value, str):
            updates[key] = value
    for key in {"requested_resources", "gateway"}:
        if key in arguments:
            updates[key] = _object(arguments.get(key))
    for key in {"log_uris", "artifacts"}:
        if key in arguments:
            updates[key] = _string_list(arguments.get(key), key)
    state_value = arguments.get("state")
    state = GatewaySessionState(str(state_value)) if state_value is not None else None
    session = queue.update_gateway_session(
        _required_str(arguments, "session_id"),
        state=state,
        metadata=_object(arguments.get("metadata", {})),
        **updates,
    )
    return session.model_dump(mode="json")


def _object(value: Any) -> JSON:
    if not isinstance(value, dict):
        raise ValueError("expected object")
    return cast(JSON, value)


def _required_str(value: JSON, key: str) -> str:
    item = value.get(key)
    if not isinstance(item, str) or not item:
        raise ValueError(f"{key} is required")
    return item


def _optional_str(value: JSON, key: str) -> str | None:
    item = value.get(key)
    if item is None:
        return None
    if not isinstance(item, str) or not item:
        raise ValueError(f"{key} must be a non-empty string")
    return item


def _string_list(value: Any, name: str) -> list[str]:
    if not isinstance(value, list):
        raise ValueError(f"{name} must be a string array")
    items = cast(list[object], value)
    if not all(isinstance(item, str) for item in items):
        raise ValueError(f"{name} must be a string array")
    return cast(list[str], items)


def _string_mapping(value: Any, name: str) -> dict[str, str]:
    if not isinstance(value, dict):
        raise ValueError(f"{name} must be a string object")
    mapping = cast(dict[object, object], value)
    if not all(isinstance(key, str) and isinstance(item, str) for key, item in mapping.items()):
        raise ValueError(f"{name} must be a string object")
    return cast(dict[str, str], mapping)


def _log_limit(arguments: JSON) -> int:
    return _bounded_integer_limit(
        arguments,
        field_name="log_limit",
        default=65_536,
        maximum=MAX_LOG_READ_BYTES,
    )


def _job_log_limit(arguments: JSON) -> int:
    return _bounded_integer_limit(
        arguments,
        field_name="limit",
        default=65_536,
        maximum=MAX_LOG_READ_BYTES,
    )


def _bounded_integer_limit(
    arguments: JSON,
    *,
    field_name: str,
    default: int,
    maximum: int,
) -> int:
    value = arguments.get(field_name, default)
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{field_name} must be an integer")
    if value < 1 or value > maximum:
        raise ValueError(f"{field_name} must be between 1 and {maximum}")
    return value


def _response_page_limit(arguments: JSON) -> int:
    return validate_response_page_limit(arguments.get("limit", DEFAULT_RESPONSE_PAGE_RECORDS))


def _response_page_cursor(arguments: JSON) -> int:
    return validate_record_cursor(arguments.get("cursor", 1))


def _record_page(
    record_key: str,
    records: list[JSON],
    *,
    cursor: int,
    limit: int,
    next_cursor: int | None,
    total: int,
) -> JSON:
    """Build the shared one-based collection response used by MCP tools."""
    return {
        record_key: records,
        "cursor": cursor,
        "limit": limit,
        "next_cursor": next_cursor,
        "total": total,
    }


def _optional_int(value: JSON, key: str) -> int | None:
    item = value.get(key)
    if item is None:
        return None
    return int(item)


def _optional_float(value: JSON, key: str) -> float | None:
    item = value.get(key)
    if item is None:
        return None
    if isinstance(item, bool):
        raise ValueError(f"{key} must be a number")
    return float(item)


def _optional_datetime_argument(value: JSON, key: str) -> datetime | None:
    item = value.get(key)
    if item is None:
        return None
    if not isinstance(item, str):
        raise ValueError(f"{key} must be an ISO-8601 string")
    try:
        parsed = datetime.fromisoformat(item)
    except ValueError as exc:
        raise ValueError(f"{key} must be an ISO-8601 string") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"{key} must include a timezone")
    return parsed


def _stable_digest(value: dict[str, object]) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _error(
    request_id: Any,
    code: int,
    message: str,
    *,
    data: JSON | None = None,
) -> JSON:
    error: JSON = {"code": code, "message": message}
    if data is not None:
        error["data"] = data
    return {"jsonrpc": "2.0", "id": request_id, "error": error}


def _toml_string(value: str) -> str:
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'

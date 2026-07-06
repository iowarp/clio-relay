"""Stdio MCP server for relay job submission tools."""

from __future__ import annotations

import hashlib
import json
import sys
from json import JSONDecodeError
from typing import Any, TextIO, cast

from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.models import Cursor, JarvisRunSpec, JobKind, RelayJob

JSON = dict[str, Any]


def serve_stdio(
    *,
    stdin: TextIO = sys.stdin,
    stdout: TextIO = sys.stdout,
    settings: RelaySettings | None = None,
) -> None:
    """Serve a minimal MCP JSON-RPC server over newline-delimited stdio."""
    resolved = settings or RelaySettings.from_env()
    queue = ClioCoreQueue(resolved.core_dir)
    queue.initialize()
    for line in stdin:
        if not line.strip():
            continue
        try:
            request = json.loads(line)
        except JSONDecodeError as exc:
            response = _error(None, -32700, f"parse error: {exc.msg}")
        else:
            response = handle_request(request, queue=queue)
        if response is None:
            continue
        stdout.write(json.dumps(response, separators=(",", ":")) + "\n")
        stdout.flush()


def handle_request(request: JSON, *, queue: ClioCoreQueue) -> JSON | None:
    """Handle one JSON-RPC MCP request."""
    request_id = request.get("id")
    method = request.get("method")
    if method == "notifications/initialized":
        return None
    try:
        if method == "initialize":
            result = _initialize_result()
        elif method == "tools/list":
            result = {"tools": _tool_definitions()}
        elif method == "tools/call":
            params = _object(request.get("params"))
            result = _call_tool(params, queue=queue)
        else:
            return _error(request_id, -32601, f"unknown method: {method}")
    except Exception as exc:
        return _error(request_id, -32000, str(exc))
    return {"jsonrpc": "2.0", "id": request_id, "result": result}


def render_codex_mcp_profile(
    *,
    settings: RelaySettings | None = None,
) -> str:
    """Render a Codex profile TOML snippet for the relay MCP server."""
    resolved = settings or RelaySettings.from_env()
    return "\n".join(
        [
            "[mcp_servers.clio-relay]",
            'command = "clio-relay"',
            'args = ["mcp-server"]',
            "",
            "[mcp_servers.clio-relay.env]",
            f"CLIO_RELAY_CORE_DIR = {_toml_string(str(resolved.core_dir))}",
            f"CLIO_RELAY_SPOOL_DIR = {_toml_string(str(resolved.spool_dir))}",
            "",
        ]
    )


def _initialize_result() -> JSON:
    return {
        "protocolVersion": "2024-11-05",
        "capabilities": {"tools": {}},
        "serverInfo": {"name": "clio-relay", "version": "0.1.0"},
    }


def _tool_definitions() -> list[JSON]:
    return [
        {
            "name": "relay_submit_jarvis_pipeline",
            "description": "Submit a JARVIS pipeline YAML document to a configured relay cluster.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "cluster": {"type": "string"},
                    "pipeline_yaml": {"type": "string"},
                    "idempotency_key": {"type": "string"},
                },
                "required": ["cluster", "pipeline_yaml"],
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
            "name": "relay_watch_job_events",
            "description": "Read relay job events from a cursor.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "job_id": {"type": "string"},
                    "cursor": {"type": "integer", "default": 1, "minimum": 1},
                    "limit": {"type": "integer", "default": 100, "minimum": 1},
                },
                "required": ["job_id"],
                "additionalProperties": False,
            },
        },
    ]


def _call_tool(params: JSON, *, queue: ClioCoreQueue) -> JSON:
    name = _required_str(params, "name")
    arguments = _object(params.get("arguments", {}))
    if name == "relay_submit_jarvis_pipeline":
        result = _submit_jarvis_pipeline(arguments, queue=queue)
    elif name == "relay_get_job":
        result = queue.get_job(_required_str(arguments, "job_id")).model_dump(mode="json")
    elif name == "relay_watch_job_events":
        events, cursor = queue.drain_events(
            Cursor(
                job_id=_required_str(arguments, "job_id"),
                next_seq=int(arguments.get("cursor", 1)),
            ),
            limit=int(arguments.get("limit", 100)),
        )
        result = {
            "events": [event.model_dump(mode="json") for event in events],
            "next_cursor": cursor.next_seq,
        }
    else:
        raise ValueError(f"unknown tool: {name}")
    return {
        "content": [{"type": "text", "text": json.dumps(result, sort_keys=True)}],
        "structuredContent": result,
        "isError": False,
    }


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
    return {"job_id": job.job_id, "state": job.state.value, "kind": job.kind.value}


def _object(value: Any) -> JSON:
    if not isinstance(value, dict):
        raise ValueError("expected object")
    return cast(JSON, value)


def _required_str(value: JSON, key: str) -> str:
    item = value.get(key)
    if not isinstance(item, str) or not item:
        raise ValueError(f"{key} is required")
    return item


def _error(request_id: Any, code: int, message: str) -> JSON:
    return {"jsonrpc": "2.0", "id": request_id, "error": {"code": code, "message": message}}


def _toml_string(value: str) -> str:
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'

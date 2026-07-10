"""Stdio MCP server for relay job submission tools."""

from __future__ import annotations

import hashlib
import json
import sys
from json import JSONDecodeError
from typing import Any, TextIO, cast

from clio_relay import __version__
from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
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
from clio_relay.progress_provenance import external_progress_metadata
from clio_relay.queue_management import (
    cancel_queue_job,
    cleanup_stale_jobs,
    diagnose_queue,
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
            response = handle_request(request, queue=queue, settings=resolved)
        if response is None:
            continue
        stdout.write(json.dumps(response, separators=(",", ":")) + "\n")
        stdout.flush()


def handle_request(
    request: JSON,
    *,
    queue: ClioCoreQueue,
    settings: RelaySettings | None = None,
) -> JSON | None:
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
            result = _call_tool(params, queue=queue, settings=settings or RelaySettings.from_env())
        else:
            return _error(request_id, -32601, f"unknown method: {method}")
    except Exception as exc:
        return _error(request_id, -32000, str(exc))
    return {"jsonrpc": "2.0", "id": request_id, "result": result}


def render_agent_mcp_profile(
    *,
    settings: RelaySettings | None = None,
) -> str:
    """Render an agent MCP profile TOML snippet for the relay MCP server."""
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


def render_codex_mcp_profile(
    *,
    settings: RelaySettings | None = None,
) -> str:
    """Render a Codex-compatible MCP profile TOML snippet for the relay MCP server."""
    return render_agent_mcp_profile(settings=settings)


def _initialize_result() -> JSON:
    return {
        "protocolVersion": "2024-11-05",
        "capabilities": {"tools": {}},
        "serverInfo": {"name": "clio-relay", "version": __version__},
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
                    "wait_for_terminal": {"type": "boolean", "default": False},
                    "timeout_seconds": {"type": "number", "default": 600},
                    "poll_seconds": {"type": "number", "default": 2},
                },
                "required": ["cluster", "pipeline_yaml"],
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
                    "limit": {"type": "integer", "default": 100, "minimum": 1},
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
                    "limit": {"type": "integer", "default": 100, "minimum": 1},
                },
                "required": ["job_id"],
                "additionalProperties": False,
            },
        },
        {
            "name": "relay_list_tasks",
            "description": "List durable task records for a relay job.",
            "inputSchema": {
                "type": "object",
                "properties": {"job_id": {"type": "string"}},
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
                    "limit": {"type": "integer", "default": 100, "minimum": 1},
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
                    "limit": {"type": "integer", "default": 65536, "minimum": 1},
                },
                "required": ["job_id", "stream"],
                "additionalProperties": False,
            },
        },
        {
            "name": "relay_list_artifacts",
            "description": "List artifact references indexed for a job.",
            "inputSchema": {
                "type": "object",
                "properties": {"job_id": {"type": "string"}},
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
            "description": "List structured progress observations for a relay job.",
            "inputSchema": {
                "type": "object",
                "properties": {"job_id": {"type": "string"}},
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
                    "include_terminal": {"type": "boolean", "default": False},
                },
                "additionalProperties": False,
            },
        },
        {
            "name": "relay_queue_diagnose",
            "description": "Diagnose stuck relay queue state such as expired leases.",
            "inputSchema": {
                "type": "object",
                "properties": {"cluster": {"type": "string"}},
                "additionalProperties": False,
            },
        },
        {
            "name": "relay_queue_cleanup_stale",
            "description": "Recover jobs with expired worker leases.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "cluster": {"type": "string"},
                    "max_attempts": {"type": "integer", "minimum": 1, "default": 3},
                    "dry_run": {"type": "boolean", "default": True},
                },
                "required": ["cluster"],
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
            "description": "List monitor rules, optionally filtered by job id.",
            "inputSchema": {
                "type": "object",
                "properties": {"job_id": {"type": "string"}},
                "additionalProperties": False,
            },
        },
        {
            "name": "relay_evaluate_monitor_rules",
            "description": "Evaluate enabled monitor rules once.",
            "inputSchema": {
                "type": "object",
                "properties": {"limit": {"type": "integer", "default": 100, "minimum": 1}},
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
    ]


def _call_tool(params: JSON, *, queue: ClioCoreQueue, settings: RelaySettings) -> JSON:
    name = _required_str(params, "name")
    arguments = _object(params.get("arguments", {}))
    if name == "relay_submit_jarvis_pipeline":
        result = _submit_jarvis_pipeline(arguments, queue=queue)
    elif name == "relay_submit_remote_agent":
        result = _submit_remote_agent(arguments, queue=queue)
    elif name == "relay_submit_mcp_call":
        result = _submit_mcp_call(arguments, queue=queue)
    elif name == "relay_get_job":
        result = queue.get_job(_required_str(arguments, "job_id")).model_dump(mode="json")
    elif name == "relay_get_job_status":
        result = job_status(queue, _required_str(arguments, "job_id"))
    elif name == "relay_monitor_job":
        result = monitor_job(
            queue,
            _required_str(arguments, "job_id"),
            cursor=int(arguments.get("cursor", 1)),
            limit=int(arguments.get("limit", 100)),
        )
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
    elif name == "relay_list_tasks":
        result = {
            "tasks": [
                task.model_dump(mode="json")
                for task in queue.list_tasks(_required_str(arguments, "job_id"))
            ]
        }
    elif name == "relay_record_task_event":
        result = _record_task_event(arguments, queue=queue)
    elif name == "relay_watch_task_events":
        events, cursor = queue.drain_task_events(
            _required_str(arguments, "task_id"),
            cursor=int(arguments.get("cursor", 1)),
            limit=int(arguments.get("limit", 100)),
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
            limit=int(arguments.get("limit", 65536)),
        )
    elif name == "relay_list_artifacts":
        result = {
            "artifacts": [
                artifact.model_dump(mode="json")
                for artifact in queue.list_artifacts(_required_str(arguments, "job_id"))
            ]
        }
    elif name == "relay_read_artifact":
        result = read_artifact_bytes(queue, _required_str(arguments, "artifact_id"))
    elif name == "relay_record_progress":
        result = _record_progress(arguments, queue=queue)
    elif name == "relay_list_progress":
        result = {
            "progress": [
                progress.model_dump(mode="json")
                for progress in queue.list_progress(_required_str(arguments, "job_id"))
            ]
        }
    elif name == "relay_cancel_job":
        result = cancel_queue_job(
            queue,
            _required_str(arguments, "job_id"),
            scheduler_policy=(
                "request-scheduler"
                if arguments.get("cancel_scheduler_job") is True
                else "relay-only"
            ),
        )
    elif name == "relay_queue_list":
        raw_state = arguments.get("state")
        state = JobState(raw_state) if isinstance(raw_state, str) else None
        result = list_queue_jobs(
            queue,
            cluster=_optional_str(arguments, "cluster"),
            state=state,
            include_terminal=arguments.get("include_terminal") is True,
        )
    elif name == "relay_queue_diagnose":
        result = diagnose_queue(queue, cluster=_optional_str(arguments, "cluster"))
    elif name == "relay_queue_cleanup_stale":
        result = cleanup_stale_jobs(
            queue,
            cluster=_required_str(arguments, "cluster"),
            max_attempts=int(arguments.get("max_attempts", 3)),
            dry_run=arguments.get("dry_run", True) is not False,
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
        result = {
            "rules": [
                rule.model_dump(mode="json") for rule in queue.list_monitor_rules(job_id=job_id)
            ]
        }
    elif name == "relay_evaluate_monitor_rules":
        result = {"actions": evaluate_monitor_rules(queue, limit=int(arguments.get("limit", 100)))}
    elif name == "relay_create_gateway_session":
        result = _create_gateway_session(arguments, queue=queue)
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
                "tool": tool,
                "arguments_digest": digest,
                "timeout_seconds": timeout_seconds,
            }
        )
    )
    job = queue.submit_job(
        RelayJob(
            cluster=cluster,
            kind=JobKind.MCP_CALL,
            spec=McpCallSpec(
                server=server,
                tool=tool,
                arguments=tool_arguments,
                timeout_seconds=timeout_seconds,
            ),
            idempotency_key=idempotency_key,
        )
    )
    return _submission_result(job, arguments, queue=queue)


def _submission_result(job: RelayJob, arguments: JSON, *, queue: ClioCoreQueue) -> JSON:
    if bool(arguments.get("wait_for_terminal", False)):
        job = wait_for_terminal(
            queue,
            job.job_id,
            timeout_seconds=float(arguments.get("wait_timeout_seconds", 600)),
            poll_seconds=float(arguments.get("poll_seconds", 2)),
        )
    return {
        "job_id": job.job_id,
        "state": job.state.value,
        "kind": job.kind.value,
        "terminal": job.state.value in {"succeeded", "failed", "canceled"},
    }


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


def _stable_digest(value: dict[str, object]) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _error(request_id: Any, code: int, message: str) -> JSON:
    return {"jsonrpc": "2.0", "id": request_id, "error": {"code": code, "message": message}}


def _toml_string(value: str) -> str:
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'

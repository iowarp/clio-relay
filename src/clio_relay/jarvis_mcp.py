"""Built-in remote JARVIS MCP integration."""

from __future__ import annotations

import json
import os
from copy import deepcopy
from typing import Any, cast

CLIO_KIT_JARVIS_MCP_VERSION = "2.2.6"
DEFAULT_JARVIS_MCP_COMMAND = [
    "uvx",
    "--from",
    f"clio-kit=={CLIO_KIT_JARVIS_MCP_VERSION}",
    "clio-kit",
    "mcp-server",
    "jarvis",
]
JARVIS_MCP_COMMAND_ENV = "CLIO_RELAY_JARVIS_MCP_COMMAND"
VIRTUAL_JARVIS_PREFIX = "jarvis_"

JSON = dict[str, Any]

_STRING_SCHEMA: JSON = {"type": "string"}
_OPTIONAL_STRING_SCHEMA: JSON = {"type": "string"}
_BOOL_SCHEMA: JSON = {"type": "boolean"}
_OBJECT_SCHEMA: JSON = {"type": "object", "default": {}}

_VIRTUAL_JARVIS_TOOLS: dict[str, JSON] = {
    "jarvis_create_pipeline": {
        "description": "Create a JARVIS pipeline on the selected cluster.",
        "properties": {"pipeline_id": _STRING_SCHEMA, "execution": _OBJECT_SCHEMA},
        "required": ["pipeline_id"],
    },
    "jarvis_describe": {
        "description": (
            "Describe JARVIS packages, one package, a pipeline, or one pipeline step "
            "on the selected cluster."
        ),
        "properties": {
            "target": {
                "type": "string",
                "enum": ["packages", "package", "pipeline", "step"],
            },
            "pipeline_id": _STRING_SCHEMA,
            "step_id": _OPTIONAL_STRING_SCHEMA,
            "package_name": _OPTIONAL_STRING_SCHEMA,
            "include_yaml": _BOOL_SCHEMA,
        },
        "required": ["target"],
    },
    "jarvis_add_step": {
        "description": ("Add a package-backed step to a JARVIS pipeline on the selected cluster."),
        "properties": {
            "pipeline_id": _STRING_SCHEMA,
            "package_name": _STRING_SCHEMA,
            "step_id": _OPTIONAL_STRING_SCHEMA,
            "config": _OBJECT_SCHEMA,
            "do_configure": _BOOL_SCHEMA,
        },
        "required": ["pipeline_id", "package_name"],
    },
    "jarvis_edit_step": {
        "description": "Edit the configuration of a JARVIS pipeline step.",
        "properties": {
            "pipeline_id": _STRING_SCHEMA,
            "step_id": _STRING_SCHEMA,
            "config": _OBJECT_SCHEMA,
        },
        "required": ["pipeline_id", "step_id", "config"],
    },
    "jarvis_remove_step": {
        "description": "Remove a step from a JARVIS pipeline without deleting package files.",
        "properties": {"pipeline_id": _STRING_SCHEMA, "step_id": _STRING_SCHEMA},
        "required": ["pipeline_id", "step_id"],
    },
    "jarvis_run": {
        "description": ("Run a configured JARVIS pipeline through the cluster-local JARVIS MCP."),
        "properties": {
            "pipeline_id": _STRING_SCHEMA,
            "execution": _OBJECT_SCHEMA,
            "submit": _BOOL_SCHEMA,
            "wait": _BOOL_SCHEMA,
        },
        "required": ["pipeline_id"],
    },
}


def jarvis_mcp_command() -> list[str]:
    """Return the command used on the cluster to launch the JARVIS MCP server."""
    configured = os.environ.get(JARVIS_MCP_COMMAND_ENV)
    if configured is None or configured.strip() == "":
        return list(DEFAULT_JARVIS_MCP_COMMAND)
    try:
        decoded = json.loads(configured)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{JARVIS_MCP_COMMAND_ENV} must be a JSON string array") from exc
    if not isinstance(decoded, list):
        raise ValueError(f"{JARVIS_MCP_COMMAND_ENV} must be a JSON string array")
    items = cast(list[object], decoded)
    if not all(isinstance(item, str) and item for item in items):
        raise ValueError(f"{JARVIS_MCP_COMMAND_ENV} must be a JSON string array")
    return cast(list[str], items)


def jarvis_mcp_server() -> str:
    """Return the executable component of the JARVIS MCP command."""
    return jarvis_mcp_command()[0]


def jarvis_mcp_server_args() -> list[str]:
    """Return the argument component of the JARVIS MCP command."""
    return jarvis_mcp_command()[1:]


def virtual_jarvis_tool_definitions() -> list[JSON]:
    """Return agent-facing virtual tools for the cluster-local JARVIS MCP server."""
    tools: list[JSON] = []
    for remote_tool, definition in _VIRTUAL_JARVIS_TOOLS.items():
        properties = {
            "cluster": {
                "type": "string",
                "description": "Configured clio-relay cluster target.",
            },
            **deepcopy(cast(JSON, definition["properties"])),
            "timeout_seconds": {"type": "integer", "minimum": 1},
            "idempotency_key": {"type": "string"},
            "wait_for_terminal": {"type": "boolean", "default": False},
            "wait_timeout_seconds": {"type": "number", "default": 600},
            "poll_seconds": {"type": "number", "default": 2},
        }
        tools.append(
            {
                "name": virtual_jarvis_tool_name(remote_tool),
                "description": definition["description"],
                "inputSchema": {
                    "type": "object",
                    "properties": properties,
                    "required": ["cluster", *cast(list[str], definition["required"])],
                    "additionalProperties": False,
                },
            }
        )
    return tools


def virtual_jarvis_tool_name(remote_tool: str) -> str:
    """Return the local virtual tool name for a remote JARVIS MCP tool."""
    if remote_tool.startswith(VIRTUAL_JARVIS_PREFIX):
        return remote_tool
    return f"{VIRTUAL_JARVIS_PREFIX}{remote_tool}"


def is_virtual_jarvis_tool(tool_name: str) -> bool:
    """Return true when a local MCP tool name represents a virtual JARVIS tool."""
    return tool_name in _VIRTUAL_JARVIS_TOOLS


def virtual_jarvis_remote_tool(tool_name: str) -> str:
    """Return the remote JARVIS MCP tool name for a local virtual tool."""
    if tool_name not in _VIRTUAL_JARVIS_TOOLS:
        raise ValueError(f"unknown virtual JARVIS tool: {tool_name}")
    return tool_name


def virtual_jarvis_call_arguments(tool_name: str, arguments: JSON) -> JSON:
    """Map virtual tool arguments to the generic relay JARVIS MCP call contract."""
    remote_tool = virtual_jarvis_remote_tool(tool_name)
    forwarded = dict(arguments)
    cluster = _pop_required_str(forwarded, "cluster")
    call: JSON = {
        "cluster": cluster,
        "tool": remote_tool,
        "arguments": _remote_tool_arguments(forwarded),
    }
    for key in (
        "timeout_seconds",
        "idempotency_key",
        "wait_for_terminal",
        "wait_timeout_seconds",
        "poll_seconds",
    ):
        if key in arguments:
            call[key] = arguments[key]
    return call


def render_virtual_jarvis_agent_context() -> str:
    """Render prompt text that explains the virtual JARVIS tools to an agent."""
    tool_names = ", ".join(sorted(virtual_jarvis_tool_name(name) for name in _VIRTUAL_JARVIS_TOOLS))
    return (
        "clio-relay virtualizes the cluster-local JARVIS MCP as concrete tools. "
        "Call jarvis_create_pipeline, jarvis_describe, jarvis_add_step, "
        "jarvis_edit_step, jarvis_remove_step, and jarvis_run with a cluster "
        "argument. clio-relay routes each call to the JARVIS MCP server running "
        "on that cluster and returns a durable relay job_id. Available "
        f"virtual JARVIS tools: {tool_names}."
    )


def _remote_tool_arguments(arguments: JSON) -> JSON:
    control_keys = {
        "timeout_seconds",
        "idempotency_key",
        "wait_for_terminal",
        "wait_timeout_seconds",
        "poll_seconds",
    }
    return {key: value for key, value in arguments.items() if key not in control_keys}


def _pop_required_str(arguments: JSON, key: str) -> str:
    value = arguments.pop(key, None)
    if not isinstance(value, str) or not value:
        raise ValueError(f"{key} is required")
    return value

"""Built-in remote JARVIS MCP integration."""

from __future__ import annotations

import hashlib
import json
import os
from copy import deepcopy
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast
from uuid import uuid4

from clio_relay.remote_mcp import (
    VIRTUAL_REMOTE_MCP_JOB_OUTPUT_SCHEMA,
    RemoteMcpSchemaCache,
    RemoteMcpSchemaCacheEntry,
    default_remote_mcp_cache_path,
    remote_mcp_server_artifact_digest,
)

if TYPE_CHECKING:
    from clio_relay.installation import InstallReceipt

CLIO_KIT_JARVIS_MCP_VERSION = "3.0.0"
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
JARVIS_MCP_CACHE_SERVER_NAME = "__builtin_jarvis__"

JSON = dict[str, Any]

CLIO_KIT_JARVIS_USER_CONTRACT_SHA256 = (
    "150159ffba27725d7cd7146cf0cec2e32e41834d213ab6043e0be43f99075120"
)


def _nullable(schema: JSON) -> JSON:
    return {"anyOf": [schema, {"type": "null"}], "default": None}


def _execution_intent_schema() -> JSON:
    properties: JSON = {
        name: _nullable({"type": "string"})
        for name in (
            "account",
            "error",
            "hostfile",
            "job_name",
            "output",
            "partition",
            "qos",
            "walltime",
        )
    }
    properties.update(
        {
            name: _nullable({"type": "integer", "exclusiveMinimum": 0})
            for name in (
                "cpus_per_task",
                "gpus",
                "gpus_per_node",
                "nodes",
                "tasks",
                "tasks_per_node",
            )
        }
    )
    properties.update(
        {
            "exclusive": _nullable({"type": "boolean"}),
            "hosts": _nullable({"type": "array", "items": {"type": "string"}, "minItems": 1}),
            "mode": {
                "type": "string",
                "enum": ["auto", "local", "direct", "cluster", "scheduler", "hostfile"],
                "default": "auto",
            },
        }
    )
    return {
        "type": "object",
        "description": "Validated, backend-neutral execution request for a JARVIS pipeline.",
        "properties": properties,
        "additionalProperties": False,
    }


# clio-kit's locked FastMCP serializer dereferences this model on the wire.
# Keep the relay's local agent schema identical to that canonical projection.
_EXECUTION_PROPERTY: JSON = _nullable(_execution_intent_schema())
_JARVIS_OUTPUT_SCHEMA: JSON = {"type": "object", "additionalProperties": True}

_VIRTUAL_JARVIS_TOOLS: dict[str, JSON] = {
    "jarvis_create_pipeline": {
        "description": (
            "Create a JARVIS pipeline. Optionally pass execution intent such as local, "
            "cluster, or hostfile mode; backend details are resolved where the MCP server runs."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "pipeline_id": {"type": "string"},
                "execution": _EXECUTION_PROPERTY,
            },
            "required": ["pipeline_id"],
            "additionalProperties": False,
        },
        "outputSchema": _JARVIS_OUTPUT_SCHEMA,
        "annotations": {
            "readOnlyHint": False,
            "destructiveHint": False,
            "idempotentHint": False,
        },
    },
    "jarvis_describe": {
        "description": ("Describe JARVIS packages, one package, a pipeline, or one pipeline step."),
        "inputSchema": {
            "type": "object",
            "properties": {
                "target": {
                    "type": "string",
                    "enum": ["packages", "package", "pipeline", "step"],
                },
                "pipeline_id": _nullable({"type": "string"}),
                "step_id": _nullable({"type": "string"}),
                "package_name": _nullable({"type": "string"}),
                "include_yaml": {"type": "boolean", "default": True},
            },
            "required": ["target"],
            "additionalProperties": False,
        },
        "outputSchema": _JARVIS_OUTPUT_SCHEMA,
        "annotations": {
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
        },
    },
    "jarvis_add_step": {
        "description": (
            "Add a package-backed step to a JARVIS pipeline and optionally configure that "
            "step with package-owned settings."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "pipeline_id": {"type": "string"},
                "package_name": {"type": "string"},
                "step_id": _nullable({"type": "string"}),
                "config": _nullable({"type": "object", "additionalProperties": True}),
                "do_configure": {"type": "boolean", "default": True},
            },
            "required": ["pipeline_id", "package_name"],
            "additionalProperties": False,
        },
        "outputSchema": _JARVIS_OUTPUT_SCHEMA,
        "annotations": {
            "readOnlyHint": False,
            "destructiveHint": False,
            "idempotentHint": False,
        },
    },
    "jarvis_edit_step": {
        "description": (
            "Edit or remove a step in a JARVIS pipeline. Use operation='edit' with config, "
            "or operation='remove' without config."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "pipeline_id": {"type": "string"},
                "step_id": {"type": "string"},
                "config": _nullable({"type": "object", "additionalProperties": True}),
                "operation": {
                    "type": "string",
                    "enum": ["edit", "remove"],
                    "default": "edit",
                },
            },
            "required": ["pipeline_id", "step_id"],
            "additionalProperties": False,
        },
        "outputSchema": _JARVIS_OUTPUT_SCHEMA,
        "annotations": {
            "readOnlyHint": False,
            "destructiveHint": True,
            "idempotentHint": False,
        },
    },
    "jarvis_run": {
        "description": (
            "Run a configured JARVIS pipeline. Optional execution intent selects local, cluster, "
            "or hostfile mode without exposing scheduler internals. Optional spack_specs are "
            "resolved into a filtered environment that JARVIS persists before direct or "
            "scheduler execution."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "pipeline_id": {"type": "string"},
                "execution": _EXECUTION_PROPERTY,
                "submit": {"type": "boolean", "default": True},
                "wait": {"type": "boolean", "default": False},
                "spack_specs": _nullable({"type": "array", "items": {"type": "string"}}),
            },
            "required": ["pipeline_id"],
            "additionalProperties": False,
        },
        "outputSchema": _JARVIS_OUTPUT_SCHEMA,
        "annotations": {
            "readOnlyHint": False,
            "destructiveHint": False,
            "idempotentHint": False,
        },
    },
}


def jarvis_mcp_command() -> list[str]:
    """Return the command used on the cluster to launch the JARVIS MCP server."""
    configured = os.environ.get(JARVIS_MCP_COMMAND_ENV)
    if configured is not None and configured.strip():
        return _decode_command(configured)
    from clio_relay.installation import default_install_receipt_path, load_install_receipt

    receipt_path = default_install_receipt_path()
    if not receipt_path.exists():
        return list(DEFAULT_JARVIS_MCP_COMMAND)
    receipt = load_install_receipt(receipt_path)
    identity = jarvis_mcp_runtime_identity(receipt)
    if identity.get("artifact_identity_verified") is not True:
        reason = identity.get("error") or "receipt-bound clio-kit runtime identity did not verify"
        raise ValueError(str(reason))
    command = identity.get("command")
    if not isinstance(command, list):
        raise ValueError("install receipt has no valid clio-kit runtime command")
    command_items = cast(list[object], command)
    if not all(isinstance(item, str) for item in command_items):
        raise ValueError("install receipt has no valid clio-kit runtime command")
    return cast(list[str], command_items)


def jarvis_mcp_runtime_identity(receipt: InstallReceipt) -> dict[str, object]:
    """Verify the selected JARVIS MCP command against the stored clio-kit wheel."""
    component = receipt.component_artifacts.get("clio-kit")
    if component is None:
        return {
            "artifact_identity_verified": False,
            "command_matches_receipt": False,
            "error": "install receipt has no clio-kit component artifact",
        }
    configured = os.environ.get(JARVIS_MCP_COMMAND_ENV)
    try:
        command = (
            _decode_command(configured)
            if configured is not None and configured.strip()
            else list(component.runtime_command)
        )
    except ValueError as exc:
        return {
            "artifact_identity_verified": False,
            "command_matches_receipt": False,
            "error": str(exc),
        }
    command_matches_receipt = command == component.runtime_command and bool(command)
    runtime_path = component.runtime_artifact_path
    command_artifact = _command_install_artifact(command)
    expected_digest = component.artifact_sha256
    observed_digest: str | None = None
    artifact_exists = False
    artifact_path_matches = False
    if runtime_path is not None:
        expected_path = Path(runtime_path).expanduser()
        command_path = Path(command_artifact).expanduser() if command_artifact else None
        try:
            resolved_expected = expected_path.resolve(strict=True)
            artifact_exists = resolved_expected.is_file()
            artifact_path_matches = (
                command_path is not None and command_path.resolve(strict=True) == resolved_expected
            )
            if artifact_exists:
                observed_digest = _sha256(resolved_expected)
        except OSError:
            artifact_exists = False
    artifact_identity_verified = (
        command_matches_receipt
        and artifact_exists
        and artifact_path_matches
        and expected_digest is not None
        and observed_digest == expected_digest
    )
    error: str | None = None
    if not command_matches_receipt:
        error = "selected JARVIS MCP command does not match the install receipt"
    elif runtime_path is None or not artifact_exists:
        error = "receipt-bound clio-kit runtime wheel is missing"
    elif not artifact_path_matches:
        error = "JARVIS MCP command does not reference the receipt-bound clio-kit wheel"
    elif expected_digest is None or observed_digest != expected_digest:
        error = "receipt-bound clio-kit runtime wheel SHA-256 does not match"
    return {
        "source": "environment" if configured is not None and configured.strip() else "receipt",
        "command": command,
        "receipt_command": component.runtime_command,
        "runtime_artifact_path": runtime_path,
        "expected_artifact_sha256": expected_digest,
        "observed_artifact_sha256": observed_digest,
        "artifact_exists": artifact_exists,
        "artifact_path_matches": artifact_path_matches,
        "command_matches_receipt": command_matches_receipt,
        "artifact_identity_verified": artifact_identity_verified,
        "error": error,
    }


def _decode_command(value: str) -> list[str]:
    try:
        decoded = json.loads(value)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{JARVIS_MCP_COMMAND_ENV} must be a JSON string array") from exc
    if not isinstance(decoded, list):
        raise ValueError(f"{JARVIS_MCP_COMMAND_ENV} must be a JSON string array")
    items = cast(list[object], decoded)
    if not all(isinstance(item, str) and item for item in items):
        raise ValueError(f"{JARVIS_MCP_COMMAND_ENV} must be a JSON string array")
    return cast(list[str], items)


def _command_install_artifact(command: list[str]) -> str | None:
    for index, argument in enumerate(command[:-1]):
        if argument == "--from":
            return command[index + 1]
    return None


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def jarvis_mcp_server() -> str:
    """Return the executable component of the JARVIS MCP command."""
    return jarvis_mcp_command()[0]


def jarvis_mcp_server_args() -> list[str]:
    """Return the argument component of the JARVIS MCP command."""
    return jarvis_mcp_command()[1:]


def virtual_jarvis_tool_definitions(*, clusters: list[str] | None = None) -> list[JSON]:
    """Return agent-facing virtual tools for the cluster-local JARVIS MCP server."""
    tools: list[JSON] = []
    for remote_tool, definition in _VIRTUAL_JARVIS_TOOLS.items():
        input_schema = deepcopy(cast(JSON, definition["inputSchema"]))
        properties = cast(JSON, input_schema["properties"])
        input_schema["properties"] = {
            "cluster": {
                "type": "string",
                "description": "Configured clio-relay cluster target.",
                **({"enum": sorted(clusters)} if clusters is not None else {}),
            },
            **properties,
            "timeout_seconds": {"type": "integer", "minimum": 1},
            "idempotency_key": {"type": "string"},
            "wait_for_terminal": {"type": "boolean", "default": False},
            "wait_timeout_seconds": {"type": "number", "default": 600},
            "poll_seconds": {"type": "number", "default": 2},
        }
        required = cast(list[str], input_schema.get("required", []))
        input_schema["required"] = ["cluster", *required]
        tools.append(
            {
                "name": virtual_jarvis_tool_name(remote_tool),
                "description": (
                    f"{definition['description']} Routed through the verified cluster-local "
                    "clio-kit JARVIS MCP and returned as a durable relay job."
                ),
                "inputSchema": input_schema,
                "outputSchema": deepcopy(VIRTUAL_REMOTE_MCP_JOB_OUTPUT_SCHEMA),
                "annotations": deepcopy(cast(JSON, definition["annotations"])),
            }
        )
    return tools


def jarvis_user_contract() -> dict[str, JSON]:
    """Return a defensive copy of the pinned released clio-kit user contract."""
    return deepcopy(_VIRTUAL_JARVIS_TOOLS)


def jarvis_user_contract_digest() -> str:
    """Return the bundled released clio-kit JARVIS user-contract digest."""
    tools = [
        {
            "name": name,
            "title": None,
            "description": definition["description"],
            "input_schema": definition["inputSchema"],
            "output_schema": definition["outputSchema"],
            "annotations": definition["annotations"],
        }
        for name, definition in sorted(_VIRTUAL_JARVIS_TOOLS.items())
    ]
    return hashlib.sha256(
        json.dumps(
            {"tools": tools},
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def jarvis_mcp_artifact_binding(
    cluster: str,
    *,
    cache_path: Path | None = None,
    now: datetime | None = None,
) -> str:
    """Return the verified discovery-time artifact binding for built-in JARVIS calls."""
    resolved_cache = cache_path or default_remote_mcp_cache_path()
    entry = RemoteMcpSchemaCache.load(resolved_cache).entry_for(
        cluster,
        JARVIS_MCP_CACHE_SERVER_NAME,
    )
    if entry is None:
        raise ValueError(
            f"JARVIS MCP identity is not discovered for {cluster}; run jarvis-mcp-refresh"
        )
    return jarvis_mcp_artifact_binding_from_entry(entry, now=now)


def jarvis_mcp_artifact_binding_from_entry(
    entry: RemoteMcpSchemaCacheEntry,
    *,
    now: datetime | None = None,
) -> str:
    """Validate one durable JARVIS discovery entry and return its artifact digest."""
    current = now or datetime.now(UTC)
    if entry.server_name != JARVIS_MCP_CACHE_SERVER_NAME:
        raise ValueError("JARVIS MCP discovery cache entry has the wrong server identity")
    if not entry.is_fresh(now=current):
        raise ValueError(
            f"JARVIS MCP identity for {entry.cluster} expired at "
            f"{entry.expires_at.astimezone(UTC).isoformat()}; run jarvis-mcp-refresh"
        )
    if entry.schema_digest != CLIO_KIT_JARVIS_USER_CONTRACT_SHA256:
        raise ValueError("JARVIS MCP discovered contract does not match clio-kit 3.0.0")
    if {tool.name for tool in entry.tools} != set(_VIRTUAL_JARVIS_TOOLS):
        raise ValueError("JARVIS MCP discovery does not contain the exact user tool set")
    server_artifact = entry.provenance.server_artifact
    if (
        server_artifact.get("verified") is not True
        or server_artifact.get("server_process_artifact_verified") is not True
        or not isinstance(server_artifact.get("executable"), dict)
    ):
        raise ValueError("JARVIS MCP discovered server artifact identity is unverified")
    return remote_mcp_server_artifact_digest(server_artifact)


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
        "registered_route": True,
        "tool": remote_tool,
        "arguments": _remote_tool_arguments(forwarded),
        # JARVIS mutations and runs are not implicitly idempotent. A caller
        # that wants retry de-duplication must opt in with an explicit key.
        "idempotency_key": str(
            arguments.get("idempotency_key")
            or f"mcp:virtual:{cluster}:jarvis:{remote_tool}:{uuid4().hex}"
        ),
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
        "jarvis_edit_step, and jarvis_run with a cluster "
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

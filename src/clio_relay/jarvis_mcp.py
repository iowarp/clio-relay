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
    from clio_relay.installation import ComponentArtifactIdentity, InstallReceipt

CLIO_KIT_JARVIS_MCP_VERSION = "2.5.1"
CLIO_KIT_JARVIS_MCP_WHEEL_FILENAME = f"clio_kit-{CLIO_KIT_JARVIS_MCP_VERSION}-py3-none-any.whl"
CLIO_KIT_JARVIS_MCP_WHEEL_URL = (
    "https://github.com/iowarp/clio-kit/releases/download/"
    f"v{CLIO_KIT_JARVIS_MCP_VERSION}/{CLIO_KIT_JARVIS_MCP_WHEEL_FILENAME}"
)
CLIO_KIT_JARVIS_MCP_WHEEL_SHA256 = (
    "e2710b915e1b77d758f25118ed5cdf522687d2a813bdbf1abd3891164b9676d1"
)
CLIO_KIT_JARVIS_USER_CONTRACT_ID = "clio-kit-jarvis-user-v3.2"
DEFAULT_JARVIS_MCP_COMMAND = [
    "clio-kit",
    "mcp-server",
    "jarvis",
]
JARVIS_MCP_COMMAND_ENV = "CLIO_RELAY_JARVIS_MCP_COMMAND"
JARVIS_MCP_SPACK_COMMAND_ENV = "JARVIS_MCP_SPACK_COMMAND"
VIRTUAL_JARVIS_PREFIX = "jarvis_"
JARVIS_MCP_CACHE_SERVER_NAME = "__builtin_jarvis__"

JSON = dict[str, Any]

CLIO_KIT_JARVIS_USER_CONTRACT_SHA256 = (
    "12f6d349c9d44d8ce3594943dcd4018ec9b6e01ebb0e59d468bb1bb783a1ad5d"
)
CLIO_KIT_JARVIS_USER_WIRE_SHA256 = (
    "bda0abe2b57d5e52ef639bf530e967c3b65072ebc4761d25cd9cbbcf0cd934e9"
)
_JARVIS_USER_CONTRACT_PATH = Path(__file__).with_name("_contracts") / "jarvis-user-v3.2.json"
_EXPECTED_JARVIS_USER_TOOLS = {
    "jarvis_add_step",
    "jarvis_create_pipeline",
    "jarvis_describe",
    "jarvis_edit_step",
    "jarvis_get_execution",
    "jarvis_run",
}


def _reject_duplicate_json_keys(pairs: list[tuple[str, Any]]) -> JSON:
    result: JSON = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JARVIS contract key: {key}")
        result[key] = value
    return result


def _load_bundled_jarvis_user_contract() -> dict[str, JSON]:
    """Load and verify the canonical clio-kit user contract shipped by the relay."""
    try:
        payload = _JARVIS_USER_CONTRACT_PATH.read_bytes()
    except OSError as exc:
        raise RuntimeError("bundled clio-kit JARVIS user contract is unavailable") from exc
    if len(payload) > 4 * 1024 * 1024:
        raise RuntimeError("bundled clio-kit JARVIS user contract exceeded its byte limit")
    try:
        decoded = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_reject_duplicate_json_keys,
        )
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise RuntimeError("bundled clio-kit JARVIS user contract is invalid") from exc
    if not isinstance(decoded, dict):
        raise RuntimeError("bundled clio-kit JARVIS user contract is not an object")
    artifact = cast(JSON, decoded)
    if (
        artifact.get("schema_version") != "clio-kit.mcp-user-contract.v1"
        or artifact.get("contract_id") != CLIO_KIT_JARVIS_USER_CONTRACT_ID
        or artifact.get("profile") != "user"
        or artifact.get("contract_sha256") != CLIO_KIT_JARVIS_USER_CONTRACT_SHA256
    ):
        raise RuntimeError("bundled clio-kit JARVIS user contract identity did not match")
    raw_tools = artifact.get("tools")
    if not isinstance(raw_tools, list):
        raise RuntimeError("bundled clio-kit JARVIS user contract omitted its tools")
    tools: dict[str, JSON] = {}
    wire_tools: list[JSON] = []
    for raw_tool in cast(list[object], raw_tools):
        if not isinstance(raw_tool, dict):
            raise RuntimeError("bundled clio-kit JARVIS user contract contains an invalid tool")
        tool = cast(JSON, raw_tool)
        name = tool.get("name")
        if not isinstance(name, str) or not name or name in tools:
            raise RuntimeError("bundled clio-kit JARVIS user contract repeated a tool")
        definition = {
            "description": tool.get("description"),
            "inputSchema": tool.get("inputSchema"),
            "outputSchema": tool.get("outputSchema"),
            "annotations": tool.get("annotations"),
        }
        if (
            not isinstance(definition["description"], str)
            or not isinstance(definition["inputSchema"], dict)
            or not isinstance(definition["outputSchema"], dict)
            or not isinstance(definition["annotations"], dict)
        ):
            raise RuntimeError("bundled clio-kit JARVIS tool schema was incomplete")
        wire_tools.append(deepcopy(tool))
        tools[name] = cast(JSON, definition)
    if set(tools) != _EXPECTED_JARVIS_USER_TOOLS:
        raise RuntimeError("bundled clio-kit JARVIS user tool set did not match")
    if _jarvis_contract_digest(tools) != CLIO_KIT_JARVIS_USER_CONTRACT_SHA256:
        raise RuntimeError("bundled clio-kit JARVIS user contract digest did not match")
    if (
        artifact.get("wire_sha256") != CLIO_KIT_JARVIS_USER_WIRE_SHA256
        or _jarvis_wire_digest(wire_tools) != CLIO_KIT_JARVIS_USER_WIRE_SHA256
    ):
        raise RuntimeError("bundled clio-kit JARVIS wire contract digest did not match")
    return tools


def _jarvis_contract_digest(tools: dict[str, JSON]) -> str:
    projection = [
        {
            "name": name,
            "title": None,
            "description": definition["description"],
            "input_schema": definition["inputSchema"],
            "output_schema": definition["outputSchema"],
            "annotations": definition["annotations"],
        }
        for name, definition in sorted(tools.items())
    ]
    return hashlib.sha256(
        json.dumps(
            {"tools": projection},
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def _jarvis_wire_digest(tools: list[JSON]) -> str:
    """Return clio-kit's canonical digest of the exact MCP Tool wire objects."""
    ordered = sorted(tools, key=lambda tool: str(tool.get("name")))
    return hashlib.sha256(
        json.dumps(
            {"tools": ordered},
            allow_nan=False,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


_VIRTUAL_JARVIS_TOOLS = _load_bundled_jarvis_user_contract()


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


def jarvis_mcp_env_from() -> dict[str, str]:
    """Return the sole operator-configured site variable allowed into JARVIS MCP."""
    value = os.environ.get(JARVIS_MCP_SPACK_COMMAND_ENV)
    if value is None:
        return {}
    if not value or value != value.strip() or any(item in value for item in "\x00\r\n"):
        raise ValueError(f"{JARVIS_MCP_SPACK_COMMAND_ENV} must name one executable path")
    return {JARVIS_MCP_SPACK_COMMAND_ENV: JARVIS_MCP_SPACK_COMMAND_ENV}


def jarvis_mcp_runtime_identity(receipt: InstallReceipt) -> dict[str, object]:
    """Verify the persistent clio-kit tool against its receipt-bound source wheel."""
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
    expected_digest = component.artifact_sha256
    observed_digest: str | None = None
    artifact_exists = False
    runtime_path_resolved: Path | None = None
    if runtime_path is not None:
        expected_path = Path(runtime_path).expanduser()
        try:
            runtime_path_resolved = expected_path.resolve(strict=True)
            artifact_exists = runtime_path_resolved.is_file()
            if artifact_exists:
                observed_digest = _sha256(runtime_path_resolved)
        except OSError:
            artifact_exists = False
    tool_identity = _persistent_clio_kit_tool_identity(
        component=component,
        command=command,
        source_artifact=runtime_path_resolved,
    )
    artifact_identity_verified = (
        command_matches_receipt
        and artifact_exists
        and expected_digest is not None
        and observed_digest == expected_digest
        and tool_identity.get("persistent_tool_verified") is True
    )
    error: str | None = None
    if not command_matches_receipt:
        error = "selected JARVIS MCP command does not match the install receipt"
    elif runtime_path is None or not artifact_exists:
        error = "receipt-bound clio-kit runtime wheel is missing"
    elif expected_digest is None or observed_digest != expected_digest:
        error = "receipt-bound clio-kit runtime wheel SHA-256 does not match"
    elif tool_identity.get("persistent_tool_verified") is not True:
        error = str(tool_identity.get("error") or "persistent clio-kit tool did not verify")
    return {
        "source": "environment" if configured is not None and configured.strip() else "receipt",
        "launcher": "uv tool",
        "command": command,
        "receipt_command": component.runtime_command,
        "runtime_artifact_path": runtime_path,
        "expected_artifact_sha256": expected_digest,
        "observed_artifact_sha256": observed_digest,
        "artifact_exists": artifact_exists,
        "artifact_path_matches": tool_identity.get("source_identity_verified") is True,
        "command_matches_receipt": command_matches_receipt,
        "artifact_identity_verified": artifact_identity_verified,
        **tool_identity,
        "error": error,
    }


def _persistent_clio_kit_tool_identity(
    *,
    component: ComponentArtifactIdentity,
    command: list[str],
    source_artifact: Path | None,
) -> JSON:
    """Verify a uv-managed clio-kit console tool and its wheel provenance."""
    from clio_relay.errors import ConfigurationError
    from clio_relay.installation import probe_persistent_uv_tool_identity

    evidence: JSON = {
        "persistent_tool_verified": False,
        "provider_interpreter_verified": False,
        "distribution_identity_verified": False,
        "source_identity_verified": False,
        "tool_executable_verified": False,
        "uv_tool_environment_verified": False,
        "record_closure_verified": False,
        "provider_interpreter": component.runtime_interpreters.get("provider"),
        "tool_executable": component.runtime_executables.get("clio-kit"),
        "uv_executable": component.runtime_executables.get("uv"),
        "distribution": None,
        "distribution_version": None,
        "persistent_tool_identity": None,
        "error": None,
    }
    expected_identity = component.persistent_tool
    provider = component.runtime_interpreters.get("provider")
    recorded_executable = component.runtime_executables.get("clio-kit")
    uv_executable = component.runtime_executables.get("uv")
    if (
        expected_identity is None
        or not isinstance(provider, str)
        or not provider
        or not isinstance(recorded_executable, str)
        or not recorded_executable
        or not isinstance(uv_executable, str)
        or not uv_executable
        or len(command) != 3
        or command[1:] != ["mcp-server", "jarvis"]
    ):
        evidence["error"] = "install receipt has no persistent clio-kit tool identity"
        return evidence
    try:
        executable_path = Path(recorded_executable).expanduser().resolve(strict=True)
        selected_path = Path(command[0]).expanduser().resolve(strict=True)
    except OSError as exc:
        evidence["error"] = f"persistent clio-kit tool path is unavailable: {exc}"
        return evidence
    executable_verified = selected_path == executable_path and executable_path.is_file()
    evidence["tool_executable_verified"] = executable_verified
    if not executable_verified:
        evidence["error"] = "persistent clio-kit tool executable did not match the receipt"
        return evidence
    if source_artifact is None:
        evidence["error"] = "persistent clio-kit tool source wheel is unavailable"
        return evidence
    try:
        observed_identity = probe_persistent_uv_tool_identity(
            uv_executable=uv_executable,
            tool_executable=recorded_executable,
            provider_interpreter=provider,
            source_artifact=source_artifact,
            distribution="clio-kit",
            distribution_version=component.distribution_version or "",
            entry_point="clio-kit",
        )
    except ConfigurationError as exc:
        evidence["error"] = str(exc)
        return evidence
    expected_payload = expected_identity.model_dump(mode="json")
    observed_payload = observed_identity.model_dump(mode="json")
    identity_matches_receipt = observed_identity == expected_identity
    evidence.update(
        {
            "provider_interpreter": observed_identity.provider_interpreter,
            "tool_executable": observed_identity.tool_executable,
            "uv_executable": observed_identity.uv_executable,
            "distribution": observed_identity.distribution,
            "distribution_version": observed_identity.distribution_version,
            "persistent_tool_identity": observed_payload,
            "receipt_persistent_tool_identity": expected_payload,
            "provider_interpreter_verified": identity_matches_receipt,
            "distribution_identity_verified": identity_matches_receipt,
            "source_identity_verified": identity_matches_receipt,
            "uv_tool_environment_verified": identity_matches_receipt,
            "record_closure_verified": identity_matches_receipt,
            "persistent_tool_verified": executable_verified and identity_matches_receipt,
        }
    )
    if evidence["persistent_tool_verified"] is not True:
        evidence["error"] = "persistent clio-kit uv tool identity changed after installation"
    return evidence


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
    """Return a defensive copy of the pinned clio-kit user contract."""
    return deepcopy(_VIRTUAL_JARVIS_TOOLS)


def jarvis_user_contract_digest() -> str:
    """Return the bundled clio-kit JARVIS user-contract digest."""
    return _jarvis_contract_digest(_VIRTUAL_JARVIS_TOOLS)


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
        raise ValueError(
            f"JARVIS MCP discovered contract does not match clio-kit {CLIO_KIT_JARVIS_MCP_VERSION}"
        )
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
        "env_from": jarvis_mcp_env_from(),
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
        "jarvis_edit_step, jarvis_run, and jarvis_get_execution with a cluster "
        "argument. clio-relay routes each call to the JARVIS MCP server running "
        "on that cluster and returns a durable relay job_id. jarvis_get_execution "
        "includes progress by default and can optionally return a bounded artifact "
        "page without adding another agent tool. Use jarvis_describe with "
        "target='package_search' for bounded package discovery, then describe the "
        "selected canonical package name. "
        "When wait_for_terminal=true, the same JARVIS tool returns a bounded, "
        "artifact-bound mcp_result instead of requiring a second status or log call. "
        "For later job queries, preserve cluster, job_id, and the opaque 64-character "
        "route_revision from one receipt as a single handle; never substitute a catalog "
        "or dataset revision. "
        f"Available virtual JARVIS tools: {tool_names}."
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

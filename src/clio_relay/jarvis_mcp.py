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

from clio_relay.dev_mode import VerificationFindings, dev_mode_enabled
from clio_relay.remote_mcp import (
    CLIO_KIT_JARVIS_USER_CONTRACT_SHA256,
    CLIO_KIT_JARVIS_USER_WIRE_SHA256,
    MAX_PINNED_CONTROL_QUERY_TIMEOUT_SECONDS,
    RemoteMcpSchemaCache,
    RemoteMcpSchemaCacheEntry,
    default_remote_mcp_cache_path,
    remote_mcp_server_artifact_digest,
    virtual_jarvis_job_output_schema,
)
from clio_relay.remote_mcp import (
    jarvis_service_runtime_handoff_json_schema as jarvis_service_runtime_handoff_json_schema,
)

if TYPE_CHECKING:
    from clio_relay.installation import ComponentArtifactIdentity, InstallReceipt

CLIO_KIT_JARVIS_MCP_VERSION = "2.7.2"
CLIO_KIT_JARVIS_MCP_WHEEL_FILENAME = f"clio_kit-{CLIO_KIT_JARVIS_MCP_VERSION}-py3-none-any.whl"
CLIO_KIT_JARVIS_MCP_WHEEL_URL = (
    "https://github.com/iowarp/clio-kit/releases/download/"
    f"v{CLIO_KIT_JARVIS_MCP_VERSION}/{CLIO_KIT_JARVIS_MCP_WHEEL_FILENAME}"
)
CLIO_KIT_JARVIS_MCP_WHEEL_SHA256 = (
    "8ebe41bf366e475a7da703a52c968231780d5d9013fc5fc913fe0f0539c6b6b5"
)
CLIO_KIT_JARVIS_USER_CONTRACT_ID = "clio-kit-jarvis-user-v3.6"
DEFAULT_JARVIS_MCP_COMMAND = [
    "clio-kit",
    "mcp-server",
    "jarvis",
]
JARVIS_MCP_COMMAND_ENV = "CLIO_RELAY_JARVIS_MCP_COMMAND"
JARVIS_MCP_SPACK_COMMAND_ENV = "JARVIS_MCP_SPACK_COMMAND"
VIRTUAL_JARVIS_PREFIX = "jarvis_"
JARVIS_MCP_CACHE_SERVER_NAME = "__builtin_jarvis__"

#: RelayJob.metadata key a dev-mode-downgraded PINNED launcher resolution is
#: recorded under (clio-relay#228 rework). dev mode relaxes VERIFICATION of a
#: pinned receipt -- it must never SUBSTITUTE a different binary -- so when a
#: pinned receipt's identity fails to verify and dev mode is on, the pinned
#: receipt's OWN (unverified) launcher is still used and this typed reason is
#: attached to the durable job spec, exactly the "never a silent downgrade"
#: contract clio_relay.dev_mode.VerificationFindings already establishes for
#: every other dev-mode-gated check.
JARVIS_MCP_LAUNCHER_DOWNGRADE_METADATA_KEY = "jarvis_mcp_launcher_downgrade"

#: Typed reason recorded when a PINNED launcher's identity failed to verify
#: but dev mode downgraded the failure to a warning and used the pinned
#: receipt's own launcher anyway (never the box-global default).
JARVIS_MCP_PINNED_LAUNCHER_UNVERIFIED_REASON = "jarvis_mcp_pinned_launcher_identity_unverified"

#: Typed reason recorded when an AMBIENT (no cluster-scoped pin) launcher's
#: identity failed to verify but dev mode downgraded the failure to a
#: warning and fell back to :data:`DEFAULT_JARVIS_MCP_COMMAND`, its
#: historical, unchanged behavior.
JARVIS_MCP_AMBIENT_LAUNCHER_UNVERIFIED_REASON = "jarvis_mcp_ambient_launcher_identity_unverified"

JSON = dict[str, Any]

# CLIO_KIT_JARVIS_USER_CONTRACT_SHA256 / CLIO_KIT_JARVIS_USER_WIRE_SHA256 are
# imported from clio_relay.remote_mcp above, not redefined here: the two
# modules carried independent duplicate literals for the same clio-kit
# contract digest until clio-relay#199 consolidated them to remote_mcp's
# CLIO_KIT_JARVIS_USER_CONTRACT_SHA256_BY_ID as the sole source of truth.
_JARVIS_USER_CONTRACT_PATH = Path(__file__).with_name("_contracts") / "jarvis-user-v3.6.json"
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


def _load_bundled_jarvis_user_contract() -> tuple[dict[str, JSON], dict[str, str | None]]:
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
    titles: dict[str, str | None] = {}
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
        title = tool.get("title")
        if (
            not isinstance(definition["description"], str)
            or not isinstance(definition["inputSchema"], dict)
            or not isinstance(definition["outputSchema"], dict)
            or not isinstance(definition["annotations"], dict)
            or (title is not None and not isinstance(title, str))
        ):
            raise RuntimeError("bundled clio-kit JARVIS tool schema was incomplete")
        wire_tools.append(deepcopy(tool))
        tools[name] = cast(JSON, definition)
        titles[name] = title
    if set(tools) != _EXPECTED_JARVIS_USER_TOOLS:
        raise RuntimeError("bundled clio-kit JARVIS user tool set did not match")
    require_handle_first_jarvis_run_schema(
        cast(JSON, tools["jarvis_run"]["inputSchema"]),
        error_type=RuntimeError,
        label="bundled clio-kit JARVIS user contract",
    )
    if _jarvis_contract_digest(tools, titles) != CLIO_KIT_JARVIS_USER_CONTRACT_SHA256:
        raise RuntimeError("bundled clio-kit JARVIS user contract digest did not match")
    if (
        artifact.get("wire_sha256") != CLIO_KIT_JARVIS_USER_WIRE_SHA256
        or _jarvis_wire_digest(wire_tools) != CLIO_KIT_JARVIS_USER_WIRE_SHA256
    ):
        raise RuntimeError("bundled clio-kit JARVIS wire contract digest did not match")
    return tools, titles


def _jarvis_contract_digest(tools: dict[str, JSON], titles: dict[str, str | None]) -> str:
    """Recompute clio-kit's canonical agent-facing contract digest.

    ``titles`` must carry the exact per-tool ``title`` clio-kit shipped
    (``None`` when a tool declares none); the digest is only correct when it
    reflects the real value, never a hardcoded placeholder (clio-kit 2.7.2
    added ``title`` to every user-profile tool, which shifts this digest).
    """
    projection = [
        {
            "name": name,
            "title": titles[name],
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


def require_handle_first_jarvis_run_schema(
    input_schema: JSON,
    *,
    error_type: type[Exception] = ValueError,
    label: str = "JARVIS MCP discovery contract",
) -> None:
    """Reject a public JARVIS run schema that exposes internal blocking controls."""
    properties = input_schema.get("properties")
    if not isinstance(properties, dict):
        raise error_type(f"{label} has no jarvis_run property map")
    if "wait" in properties:
        raise error_type(
            f"{label} exposes internal jarvis_run wait; the public contract must return "
            "a durable execution handle and use jarvis_get_execution for workload lifecycle"
        )


_VIRTUAL_JARVIS_TOOLS, _VIRTUAL_JARVIS_TOOL_TITLES = _load_bundled_jarvis_user_contract()


def jarvis_mcp_command(
    *,
    receipt_path: Path | None = None,
    cluster: str | None = None,
    dev_mode: bool | None = None,
    findings: VerificationFindings | None = None,
) -> list[str]:
    """Return the command used on the cluster to launch the JARVIS MCP server.

    ``receipt_path`` pins resolution to one exact install receipt -- normally
    the CLUSTER's own registered ``relay_install_receipt`` (clio-relay#205's
    per-cluster pin) -- instead of this PROCESS's ambient current installation
    (:func:`clio_relay.installation.default_install_receipt_path`, which
    resolves through the box-global ``current`` symlink). A multi-tenant host
    running more than one relay deployment must never let one deployment's
    JARVIS MCP launcher resolve through another's shared box-global state
    (clio-relay#228); omit it only where no cluster-scoped pin is available.

    A ``receipt_path`` explicitly passed by the caller is an EXPLICIT pin: if
    it does not exist or cannot be loaded, resolution raises a typed
    ``ValueError`` naming ``cluster`` when given (matching
    :func:`clio_relay.installation.worker_runtime_info`'s "cluster {cluster}
    pinned install receipt could not be loaded" refusal) rather than
    silently falling back to :data:`DEFAULT_JARVIS_MCP_COMMAND` -- that
    fallback is reserved for the OMITTED (ambient, no cluster-scoped pin
    available) case only, exactly as before clio-relay#228 (clio-relay#228
    rework). Silently running the box-global default launcher in place of an
    unloadable explicit pin is the exact wrong-tenant hazard this fix exists
    to kill.

    ``dev_mode`` defaults to :func:`clio_relay.dev_mode.dev_mode_enabled` (the
    ``CLIO_RELAY_DEV_MODE`` environment switch) when omitted, so every
    existing caller honors the environment switch for free (clio-relay#211).
    When the receipt-bound clio-kit contract/digest identity does not
    verify, dev mode records the exact production error on ``findings``.

    dev mode relaxes VERIFICATION of a receipt -- it must NEVER substitute a
    different binary for a PINNED (explicit ``receipt_path``) route
    (clio-relay#228 rework). So on a pinned route, an unverified identity
    still resolves to the PINNED receipt's own (unverified) launcher command
    when one can be constructed from it; only when the receipt carries no
    constructible launcher at all (e.g. no clio-kit component artifact) does
    it raise the same typed refusal a failed OMITTED-path receipt already
    would. The OMITTED (ambient, no cluster-scoped pin) case keeps its
    historical behavior exactly: falls back to
    :data:`DEFAULT_JARVIS_MCP_COMMAND` -- the same fallback already used
    when no receipt exists at all -- instead of raising and blocking the
    worker's JARVIS MCP server from starting at all.
    """
    resolved_dev_mode = dev_mode_enabled() if dev_mode is None else dev_mode
    findings = findings if findings is not None else VerificationFindings()
    configured = os.environ.get(JARVIS_MCP_COMMAND_ENV)
    if configured is not None and configured.strip():
        return _decode_command(configured)
    from clio_relay.errors import ConfigurationError
    from clio_relay.installation import default_install_receipt_path, load_install_receipt

    explicit_receipt_path = receipt_path is not None
    resolved_receipt_path = (
        receipt_path if receipt_path is not None else default_install_receipt_path()
    )
    if explicit_receipt_path:
        try:
            receipt = load_install_receipt(resolved_receipt_path)
        except ConfigurationError as exc:
            prefix = f"cluster {cluster} " if cluster else ""
            raise ValueError(f"{prefix}pinned install receipt could not be loaded: {exc}") from exc
    else:
        if not resolved_receipt_path.exists():
            return list(DEFAULT_JARVIS_MCP_COMMAND)
        receipt = load_install_receipt(resolved_receipt_path)
    identity = jarvis_mcp_runtime_identity(receipt)
    if identity.get("artifact_identity_verified") is not True:
        reason = identity.get("error") or "receipt-bound clio-kit runtime identity did not verify"
        if not resolved_dev_mode:
            raise ValueError(str(reason))
        findings.record(str(reason))
        if explicit_receipt_path:
            # clio-relay#228 rework: dev mode relaxes VERIFICATION only. A
            # PINNED route must never have its launcher silently SUBSTITUTED
            # for the box-global default -- that is the exact wrong-tenant
            # hazard #228 exists to kill, and returning
            # DEFAULT_JARVIS_MCP_COMMAND here would recreate it on every
            # multi-tenant host with dev mode on. Use the pinned receipt's
            # own (unverified) launcher command when one is constructible;
            # fall through to the same typed refusal below otherwise.
            unverified_command = identity.get("command")
            if isinstance(unverified_command, list) and all(
                isinstance(item, str) for item in cast(list[object], unverified_command)
            ):
                return cast(list[str], unverified_command)
            raise ValueError("pinned install receipt has no valid clio-kit runtime command")
        return list(DEFAULT_JARVIS_MCP_COMMAND)
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


def jarvis_cd_lock_binding_expectation() -> dict[str, str]:
    """Return the exact relay release pin required by its built-in JARVIS MCP."""
    from clio_relay.bootstrap import (
        JARVIS_CD_VERSION,
        JARVIS_CD_WHEEL_SHA256,
        JARVIS_CD_WHEEL_URL,
    )

    return {
        "schema_version": "clio-relay.jarvis-cd-lock-expectation.v1",
        "version": JARVIS_CD_VERSION,
        "url": JARVIS_CD_WHEEL_URL,
        "sha256": JARVIS_CD_WHEEL_SHA256,
    }


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
    locked_server_runtime_verified = _receipt_locked_server_runtime_verified(component)
    artifact_identity_verified = (
        command_matches_receipt
        and artifact_exists
        and expected_digest is not None
        and observed_digest == expected_digest
        and tool_identity.get("persistent_tool_verified") is True
        and locked_server_runtime_verified
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
    elif not locked_server_runtime_verified:
        error = "receipt-bound clio-kit locked JARVIS dependency did not verify"
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
        "locked_server_runtime": component.locked_server_runtime,
        "locked_server_runtime_verified": locked_server_runtime_verified,
        "artifact_identity_verified": artifact_identity_verified,
        **tool_identity,
        "error": error,
    }


def _receipt_locked_server_runtime_verified(component: ComponentArtifactIdentity) -> bool:
    """Verify the bootstrap-recorded clio-kit to JARVIS-CD dependency edge."""
    return jarvis_cd_lock_binding_verified(component.locked_server_runtime)


def jarvis_cd_lock_binding_verified(value: object) -> bool:
    """Return whether one nested runtime proves the relay's built-in JARVIS-CD pin."""
    runtime = cast(dict[str, object], value) if isinstance(value, dict) else None
    if not isinstance(runtime, dict):
        return False
    raw_binding = runtime.get("jarvis_cd_lock_binding")
    if not isinstance(raw_binding, dict):
        return False
    binding = cast(dict[str, object], raw_binding)
    expected = jarvis_cd_lock_binding_expectation()
    return (
        runtime.get("schema_version") == "clio-kit.locked-server.v4"
        and runtime.get("server_name") == "jarvis"
        and runtime.get("locked_runtime_verified") is True
        and binding.get("schema_version") == "clio-relay.jarvis-cd-lock-binding.v1"
        and binding.get("dependency") == "jarvis-cd"
        and binding.get("verified") is True
        and binding.get("error") is None
        and binding.get("expected_version") == expected["version"]
        and binding.get("expected_url") == expected["url"]
        and binding.get("expected_sha256") == expected["sha256"]
        and binding.get("observed_version") == expected["version"]
        and binding.get("observed_source_url") == expected["url"]
        and binding.get("observed_wheel_url") == expected["url"]
        and binding.get("observed_wheel_sha256") == expected["sha256"]
        and binding.get("jarvis_mcp_package_entry_count") == 1
        and binding.get("resolved_dependency_entry_count") == 1
        and binding.get("observed_resolved_dependency_entries") == [{"name": "jarvis-cd"}]
        and binding.get("metadata_requirement_entry_count") == 1
        and binding.get("observed_metadata_requirement_entries")
        == [{"name": "jarvis-cd", "url": expected["url"]}]
        and binding.get("observed_metadata_requirement_urls") == [expected["url"]]
        and binding.get("package_entry_count") == 1
        and binding.get("wheel_entry_count") == 1
    )


def jarvis_mcp_server_artifact_verified(value: object) -> bool:
    """Verify the exact released outer clio-kit artifact and its nested JARVIS-CD pin."""
    if not isinstance(value, dict):
        return False
    server_artifact = cast(dict[str, object], value)
    python_runtime_value = server_artifact.get("python_distribution_runtime")
    nested_runtime = server_artifact.get("nested_runtime")
    if not isinstance(python_runtime_value, dict) or not isinstance(nested_runtime, dict):
        return False
    python_runtime = cast(dict[str, object], python_runtime_value)
    return (
        server_artifact.get("verified") is True
        and server_artifact.get("server_process_artifact_verified") is True
        and isinstance(server_artifact.get("executable"), dict)
        and server_artifact.get("install_source") == "uv-tool"
        and server_artifact.get("install_artifact_sha256") == CLIO_KIT_JARVIS_MCP_WHEEL_SHA256
        and str(python_runtime.get("distribution", "")).lower().replace("_", "-") == "clio-kit"
        and python_runtime.get("distribution_version") == CLIO_KIT_JARVIS_MCP_VERSION
        and python_runtime.get("entry_point") == "clio-kit"
        and python_runtime.get("runtime_closure_verified") is True
        and cast(dict[str, object], nested_runtime).get("persistent_tool") is True
        and jarvis_cd_lock_binding_verified(cast(dict[str, object], nested_runtime))
    )


def jarvis_mcp_server_artifact_binding_verified(
    value: object,
    *,
    expected_digest: str | None,
) -> bool:
    """Verify an exact built-in server artifact and its content-derived digest."""
    if expected_digest is None or not jarvis_mcp_server_artifact_verified(value):
        return False
    server_artifact = cast(JSON, value)
    return remote_mcp_server_artifact_digest(server_artifact) == expected_digest


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
            tool_directory=expected_identity.tool_directory,
            tool_bin_directory=expected_identity.tool_bin_directory,
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


def jarvis_mcp_server(
    *,
    receipt_path: Path | None = None,
    cluster: str | None = None,
    dev_mode: bool | None = None,
    findings: VerificationFindings | None = None,
) -> str:
    """Return the executable component of the JARVIS MCP command."""
    return jarvis_mcp_command(
        receipt_path=receipt_path,
        cluster=cluster,
        dev_mode=dev_mode,
        findings=findings,
    )[0]


def jarvis_mcp_server_args(
    *,
    receipt_path: Path | None = None,
    cluster: str | None = None,
    dev_mode: bool | None = None,
    findings: VerificationFindings | None = None,
) -> list[str]:
    """Return the argument component of the JARVIS MCP command."""
    return jarvis_mcp_command(
        receipt_path=receipt_path,
        cluster=cluster,
        dev_mode=dev_mode,
        findings=findings,
    )[1:]


def is_virtual_jarvis_control_query(remote_tool: str) -> bool:
    """Return whether the pinned virtual JARVIS contract marks a tool read-only."""
    definition = _VIRTUAL_JARVIS_TOOLS.get(remote_tool)
    if definition is None:
        return False
    annotations = definition.get("annotations")
    if not isinstance(annotations, dict):
        return False
    typed_annotations = cast(JSON, annotations)
    return bool(
        typed_annotations.get("readOnlyHint") is True
        and typed_annotations.get("destructiveHint") is False
    )


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
            "timeout_seconds": {
                "type": "integer",
                "minimum": 1,
                **(
                    {"maximum": MAX_PINNED_CONTROL_QUERY_TIMEOUT_SECONDS}
                    if is_virtual_jarvis_control_query(remote_tool)
                    else {}
                ),
            },
            "idempotency_key": {"type": "string"},
            "wait_for_terminal": {
                "type": "boolean",
                "default": False,
                "description": (
                    "Set true when the current turn needs this JARVIS operation's result. "
                    "This waits only for the bounded remote MCP dispatch, never for the "
                    "scheduler workload. Leave false only when intentionally preserving an "
                    "asynchronous relay receipt for a later relay_wait call."
                ),
            },
            "wait_timeout_seconds": {"type": "number", "default": 600},
            "poll_seconds": {"type": "number", "default": 2},
        }
        required = cast(list[str], input_schema.get("required", []))
        input_schema["required"] = ["cluster", *required]
        output_schema = virtual_jarvis_job_output_schema(remote_tool, clusters=clusters)
        tool_guidance = ""
        if remote_tool == "jarvis_get_execution":
            tool_guidance = (
                " If this call returns queued, call relay_wait with its exact cluster, job_id, "
                "and route_revision. The terminal wait returns service_runtime_bindings when "
                "include_service_runtimes=true; bind one with relay_bind_jarvis_runtime before "
                "opening a viewer. Never use execution_id as gateway_session_id."
            )
        elif remote_tool == "jarvis_run":
            tool_guidance = (
                " wait_for_terminal is a clio-relay transport control: it waits only for the "
                "brief remote MCP dispatch to return the durable execution handle. It never "
                "waits for workload completion. Use jarvis_get_execution with the returned "
                "pipeline_id and execution_id for lifecycle, progress, artifacts, and services."
            )
        tools.append(
            {
                "name": virtual_jarvis_tool_name(remote_tool),
                "description": (
                    f"{definition['description']} Routed through the verified cluster-local "
                    "clio-kit JARVIS MCP and returned as a durable relay job. Preserve the "
                    "returned cluster, job_id, and route_revision unchanged for every remote "
                    "follow-up. For ordinary interactive use, set wait_for_terminal=true so "
                    "the bounded MCP result returns in this call; leave it false only when "
                    f"intentionally queuing the transport operation.{tool_guidance}"
                ),
                "inputSchema": input_schema,
                "outputSchema": output_schema,
                "annotations": deepcopy(cast(JSON, definition["annotations"])),
            }
        )
    return tools


def jarvis_user_contract() -> dict[str, JSON]:
    """Return a defensive copy of the pinned clio-kit user contract."""
    return deepcopy(_VIRTUAL_JARVIS_TOOLS)


def jarvis_user_contract_titles() -> dict[str, str | None]:
    """Return a defensive copy of clio-kit's per-tool ``title`` for the pinned contract.

    Kept separate from :func:`jarvis_user_contract` because that function's
    return value is also the relay's own contract-identity projection (which
    intentionally excludes ``title``); callers that need the exact clio-kit
    wire title per tool (for example to reconstruct a faithful mock of a
    live clio-kit ``tools/list`` response) use this instead.
    """
    return dict(_VIRTUAL_JARVIS_TOOL_TITLES)


def jarvis_user_contract_digest() -> str:
    """Return the bundled clio-kit JARVIS user-contract digest."""
    return _jarvis_contract_digest(_VIRTUAL_JARVIS_TOOLS, _VIRTUAL_JARVIS_TOOL_TITLES)


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
    if not jarvis_mcp_server_artifact_verified(server_artifact):
        raise ValueError(
            "JARVIS MCP discovered persistent server or JARVIS-CD lock identity is "
            "unverified; run jarvis-mcp-refresh"
        )
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
        "For ordinary interactive operations, set wait_for_terminal=true so the bounded MCP "
        "result returns in the current call; leave it false only when intentionally queuing "
        "transport for a later relay_wait. "
        "When wait_for_terminal=true, the same JARVIS tool waits only for its bounded remote "
        "MCP dispatch and returns the artifact-bound mcp_result. For jarvis_run this means the "
        "durable execution handle, not workload completion; query that lifecycle with "
        "jarvis_get_execution. "
        "If a JARVIS call returns queued, call relay_wait with its exact cluster, job_id, and "
        "route_revision. A completed jarvis_get_execution call with "
        "include_service_runtimes=true returns service_runtime_bindings either directly or "
        "from that terminal relay_wait. Pass one item unchanged as the binding argument to "
        "relay_bind_jarvis_runtime before opening a viewer; jarvis_run does not produce these "
        "handoffs, and a JARVIS execution_id is never a gateway_session_id. "
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

"""Stdio MCP server for relay job submission tools."""

from __future__ import annotations

import base64
import copy
import hashlib
import hmac
import json
import math
import os
import re
import sys
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import UTC, datetime
from json import JSONDecodeError
from pathlib import Path
from typing import Any, TextIO, cast
from uuid import uuid4

from pydantic import ValidationError

from clio_relay import __version__
from clio_relay.cluster_config import (
    ClusterDefinition,
    ClusterRegistry,
    cluster_route_revision,
    default_registry_path,
)
from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.errors import (
    ConfigurationError,
    NotFoundError,
    ObservationTimeoutError,
)
from clio_relay.filesystem_paths import logical_filesystem_text
from clio_relay.identifiers import (
    durable_record_id_json_schema,
    validate_durable_record_id,
)
from clio_relay.input_staging import (
    REGISTERED_JARVIS_CONTRACT_ID,
    JarvisPackageInputContract,
    jarvis_package_input_contract_from_record,
    jarvis_package_input_contract_record,
    jarvis_package_input_route,
    jarvis_pipeline_input_route,
    merge_artifact_uses,
    parse_jarvis_package_input_contract,
    reconcile_jarvis_run_inputs,
    stage_jarvis_add_step_inputs,
    stage_jarvis_edit_step_inputs,
)
from clio_relay.jarvis_mcp import (
    JARVIS_MCP_CACHE_SERVER_NAME,
    is_virtual_jarvis_control_query,
    is_virtual_jarvis_tool,
    jarvis_cd_lock_binding_expectation,
    jarvis_mcp_artifact_binding,
    jarvis_mcp_artifact_binding_from_entry,
    jarvis_mcp_server,
    jarvis_mcp_server_args,
    jarvis_service_runtime_handoff_json_schema,
    render_virtual_jarvis_agent_context,
    virtual_jarvis_call_arguments,
    virtual_jarvis_tool_definitions,
)
from clio_relay.jarvis_service_runtime import (
    JarvisServiceRuntimeHandoff,
    derive_jarvis_service_runtime_handoffs,
    resolve_jarvis_service_runtime,
)
from clio_relay.models import (
    MCP_ADMISSION_AUTHORITY_METADATA_KEY,
    TERMINAL_STATES,
    ArtifactRef,
    ArtifactUse,
    Cursor,
    GatewaySession,
    GatewaySessionState,
    JarvisPackageInputRoute,
    JarvisPipelineInputBinding,
    JarvisRunInputManifest,
    JarvisRunSpec,
    JobKind,
    JobState,
    JobWaitResult,
    McpAdmissionClass,
    McpCallSpec,
    McpControlQueryEvidence,
    McpOperation,
    MonitorRule,
    MonitorRuleAction,
    ProgressRecord,
    RelayJob,
    RemoteAgentTaskSpec,
    TaskEventStatus,
    TaskTimelineEvent,
    artifact_use_payload,
    validate_artifact_use_collection,
)
from clio_relay.owner_session_admission import owner_session_gateway_admission
from clio_relay.pagination import (
    DEFAULT_RESPONSE_PAGE_RECORDS,
    MAX_RESPONSE_PAGE_RECORDS,
    validate_record_cursor,
    validate_response_page_limit,
)
from clio_relay.progress_provenance import external_progress_metadata
from clio_relay.public_records import public_gateway_session
from clio_relay.queue_management import (
    DEFAULT_STALE_SCAN_LIMIT,
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
    remote_command_timeout,
    remove_remote_file,
    run_remote_clio,
    should_execute_on_cluster,
    staged_remote_cluster_registry,
    write_remote_file,
)
from clio_relay.remote_mcp import (
    MAX_PINNED_CONTROL_QUERY_TIMEOUT_SECONDS,
    RemoteMcpSchemaCache,
    VirtualRemoteMcpCatalog,
    cluster_route_revision_json_schema,
    default_remote_mcp_cache_path,
    is_remote_mcp_control_query,
    load_virtual_remote_mcp_catalog,
    remote_mcp_registration_revision,
    resolve_pinned_mcp_admission,
    resolve_registered_remote_mcp_admission,
    unavailable_virtual_remote_mcp_catalog,
)
from clio_relay.retention import TerminalRetentionCoordinator
from clio_relay.service_runtime import (
    ServiceRuntimePendingResult,
    ServiceRuntimeSupervisor,
)
from clio_relay.session_api import (
    OWNED_SESSION_WAIT_RESPONSE_GRACE_SECONDS,
    OwnedSessionApiClient,
    submit_owned_session_job,
)
from clio_relay.spool import MAX_LOG_READ_BYTES
from clio_relay.storage_runtime import (
    StorageAdmissionError,
    StorageManagedQueue,
    storage_managed_queue,
)
from clio_relay.validation_report import redact_sensitive_values

JSON = dict[str, Any]
MCP_PROFILE_ENV = "CLIO_RELAY_MCP_PROFILE"
MAX_INTERNAL_COLLECTION_RECORDS = 10_000
MAX_AGENT_LOG_READ_BYTES = 32_768
MAX_INLINE_MCP_RESULT_BYTES = 65_536
MCP_RESULT_DELIVERY_SCHEMA = "clio-relay.mcp-result-delivery.v1"
MCP_RESULT_INLINE_LIMIT_CODE = "inline_result_limit_exceeded"
MCP_RESULT_INLINE_LIMIT_MESSAGE = (
    "The remote MCP operation reached a terminal state, but its result exceeded the safe "
    "inline response limit and is unavailable to the agent. Immutable private evidence was "
    "preserved for operator diagnosis. Remote side effects may have occurred; inspect the "
    "job before retrying."
)
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
MAX_OBSERVE_MATCHES = 100
MAX_OBSERVE_MATCH_TEXT_CHARS = 1_024
REMOTE_WAIT_STATUS_TIMEOUT_SECONDS = 30.0
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
    "relay_bind_jarvis_runtime",
    "relay_artifact_lineage",
}
_REMOTE_JOB_FOLLOWUP_TOOL_NAMES = frozenset(
    {
        "relay_status",
        "relay_cancel",
        "relay_observe",
        "relay_wait",
    }
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


@dataclass
class McpSessionState:
    """Catalog and remote-job routes observed by one connected MCP client."""

    remote_mcp_catalog_revisions: dict[str, str] = field(default_factory=lambda: dict[str, str]())
    remote_job_routes: dict[str, set[tuple[str, str]]] = field(
        default_factory=lambda: dict[str, set[tuple[str, str]]]()
    )
    jarvis_package_input_contracts: dict[str, JarvisPackageInputContract] = field(
        default_factory=lambda: dict[str, JarvisPackageInputContract]()
    )
    jarvis_pipeline_input_uses: dict[str, tuple[ArtifactUse, ...]] = field(
        default_factory=lambda: dict[str, tuple[ArtifactUse, ...]]()
    )

    def reset(self) -> None:
        """Forget catalogs and job routes observed before a new MCP initialization."""
        self.remote_mcp_catalog_revisions.clear()
        self.remote_job_routes.clear()
        self.jarvis_package_input_contracts.clear()
        self.jarvis_pipeline_input_uses.clear()

    def observe_remote_mcp_catalog(self, *, profile: str, revision: str) -> None:
        """Record the exact remote-tool catalog rendered by ``tools/list``."""
        self.remote_mcp_catalog_revisions[profile] = revision

    def observed_remote_mcp_catalog_revision(self, *, profile: str) -> str | None:
        """Return the catalog revision advertised for one MCP profile."""
        return self.remote_mcp_catalog_revisions.get(profile)

    def observe_remote_job_result(self, result: JSON) -> None:
        """Remember the exact route from one remote submission receipt."""
        if result.get("remote") is not True or "job_id" not in result:
            return
        job_id = validate_durable_record_id(result["job_id"])
        cluster = result.get("cluster")
        if not isinstance(cluster, str) or not cluster:
            raise ValueError("remote job receipt omitted its cluster route")
        route_revision = _validated_route_revision(result.get("route_revision"))
        self.remote_job_routes.setdefault(job_id, set()).add((cluster, route_revision))

    def remote_job_route(self, job_id: str) -> tuple[str, str] | None:
        """Return one unambiguous route learned for a remote job in this session."""
        routes = self.remote_job_routes.get(job_id, set())
        if not routes:
            return None
        if len(routes) != 1:
            raise ValueError(
                f"remote job_id {job_id} is ambiguous in this MCP session; pass cluster and "
                "route_revision from the intended receipt"
            )
        return next(iter(routes))

    def remember_jarvis_package_inputs(self, contract: JarvisPackageInputContract) -> None:
        """Remember one structured package description for this initialized client."""
        self.jarvis_package_input_contracts[contract.cache_key] = contract

    def jarvis_package_inputs(self, cache_key: str) -> JarvisPackageInputContract | None:
        """Return one exact structured package input contract, if it was observed."""
        return self.jarvis_package_input_contracts.get(cache_key)

    def remember_jarvis_pipeline_inputs(
        self,
        cache_key: str,
        uses: tuple[ArtifactUse, ...],
    ) -> None:
        """Remember immutable inputs accepted by one exact JARVIS pipeline route."""
        existing = self.jarvis_pipeline_input_uses.get(cache_key, ())
        self.jarvis_pipeline_input_uses[cache_key] = tuple(
            merge_artifact_uses(list(existing), uses)
        )

    def jarvis_pipeline_inputs(self, cache_key: str) -> tuple[ArtifactUse, ...]:
        """Return immutable inputs previously accepted for one exact pipeline route."""
        return self.jarvis_pipeline_input_uses.get(cache_key, ())


@dataclass(frozen=True)
class _VerifiedMcpResult:
    """SHA-verified full MCP artifact plus its bounded public projection."""

    document: JSON
    public: JSON


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
    session = McpSessionState()
    first_line = True
    try:
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
                    session=session,
                )
            if response is None:
                continue
            stdout.write(json.dumps(response, separators=(",", ":")) + "\n")
            stdout.flush()
    finally:
        queue.close()


def handle_request(
    request: JSON,
    *,
    queue: ClioCoreQueue,
    settings: RelaySettings | None = None,
    profile: str | None = None,
    session: McpSessionState | None = None,
) -> JSON | None:
    """Handle one JSON-RPC MCP request."""
    request_id = request.get("id")
    method = request.get("method")
    resolved_profile = _normalize_profile(profile or _mcp_profile_from_env())
    if method == "notifications/initialized":
        return None
    try:
        if method == "initialize":
            if session is not None:
                session.reset()
            result = _initialize_result()
        elif method == "tools/list":
            tool_definitions, catalog = _tool_definitions_and_remote_catalog(
                profile=resolved_profile
            )
            if session is not None:
                session.observe_remote_mcp_catalog(
                    profile=resolved_profile,
                    revision=catalog.revision,
                )
            result = {
                "tools": tool_definitions,
                "_meta": {
                    "clio-relay/remote-mcp-catalog-revision": catalog.revision,
                    "clio-relay/profile": resolved_profile,
                },
            }
        elif method == "tools/call":
            params = _object(request.get("params"))
            result = _call_tool(
                params,
                queue=queue,
                settings=settings or RelaySettings.from_env(),
                profile=resolved_profile,
                session=session,
                observed_remote_mcp_catalog_revision=(
                    session.observed_remote_mcp_catalog_revision(profile=resolved_profile)
                    if session is not None
                    else None
                ),
                require_advertised_remote_mcp_catalog=session is not None,
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
        public_error = redact_sensitive_values(
            {
                "request": request,
                "error": logical_filesystem_text(str(exc)),
            }
        )
        public_error_document = (
            cast(dict[str, object], public_error) if isinstance(public_error, dict) else {}
        )
        error_message = public_error_document.get("error")
        return _error(
            request_id,
            -32000,
            error_message if isinstance(error_message, str) else "relay tool request failed",
        )
    return {"jsonrpc": "2.0", "id": request_id, "result": result}


def render_agent_mcp_profile(
    *,
    settings: RelaySettings | None = None,
) -> str:
    """Render an agent MCP profile TOML snippet for the relay MCP server."""
    resolved = settings or RelaySettings.from_env()
    registry_path = default_registry_path().expanduser().resolve()
    cache_path = default_remote_mcp_cache_path(registry_path=registry_path).expanduser().resolve()
    input_workspace_root = (resolved.input_workspace_root or Path.cwd()).expanduser().resolve()
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
            (f"CLIO_RELAY_INPUT_WORKSPACE_ROOT = {_toml_string(str(input_workspace_root))}"),
            (
                "CLIO_RELAY_INPUT_FILE_MAX_BYTES = "
                f"{_toml_string(str(resolved.input_file_max_bytes))}"
            ),
            (
                "CLIO_RELAY_INPUT_TOTAL_MAX_BYTES = "
                f"{_toml_string(str(resolved.input_total_max_bytes))}"
            ),
            (
                "CLIO_RELAY_INPUT_FILE_MAX_COUNT = "
                f"{_toml_string(str(resolved.input_file_max_count))}"
            ),
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


def _tool_definitions_and_remote_catalog(
    *,
    profile: str | None = None,
) -> tuple[list[JSON], VirtualRemoteMcpCatalog]:
    """Render tools and return the exact remote catalog used for this list."""
    normalized = _normalize_profile(profile or _mcp_profile_from_env())
    reserved_names = static_mcp_tool_names()
    catalog = _remote_mcp_catalog(
        profile=normalized,
        reserved_names=reserved_names,
    )
    configured_clusters = _configured_cluster_names()
    jarvis_clusters = _bound_virtual_jarvis_clusters(catalog)
    tools = _all_tool_definitions(
        clusters=configured_clusters,
        jarvis_clusters=jarvis_clusters,
    )
    if not jarvis_clusters:
        tools = [tool for tool in tools if not is_virtual_jarvis_tool(str(tool["name"]))]
    if normalized in {"admin", "operator", "all"}:
        selected = tools
    else:
        selected = [
            tool
            for tool in tools
            if tool["name"] in USER_MCP_TOOL_NAMES or is_virtual_jarvis_tool(str(tool["name"]))
        ]
    return [*selected, *catalog.tool_definitions()], catalog


def _bound_virtual_jarvis_clusters(catalog: VirtualRemoteMcpCatalog) -> list[str]:
    """Return clusters with an advertised, verified built-in JARVIS identity."""

    return sorted(
        cluster
        for cluster, artifact_digest in catalog.jarvis_artifact_bindings.items()
        if artifact_digest is not None and cluster in catalog.cluster_route_revisions
    )


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


def _all_tool_definitions(
    *,
    clusters: list[str] | None = None,
    jarvis_clusters: list[str] | None = None,
) -> list[JSON]:
    """Return static tools with independent generic and built-in JARVIS routes."""

    resolved_jarvis_clusters = clusters if jarvis_clusters is None else jarvis_clusters
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
                "matches stdout, stderr, or event text. For a remote job, copy cluster, "
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
            "description": "List one stable page of artifact references indexed for a job.",
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
            "name": "relay_artifact_lineage",
            "description": (
                "Query artifact lineage in either direction: pass job_id for the artifacts "
                "that job used, or artifact_id for the jobs that used that artifact."
            ),
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
                "credential-bearing artifacts are intentionally unavailable through this tool."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {"artifact_id": durable_record_id_json_schema()},
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
                    "job_id": durable_record_id_json_schema(),
                    "cluster": {"type": "string"},
                    "route_revision": cluster_route_revision_json_schema(),
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
            "name": "relay_bind_jarvis_runtime",
            "description": (
                "Bind local relay connectors to one ready service reported by a completed, "
                "artifact-bound JARVIS execution query with service runtimes included. "
                "Pass one service_runtime_bindings item returned either by a "
                "wait_for_terminal jarvis_get_execution call or by relay_wait for its exact "
                "remote job handle unchanged as binding. jarvis_run is not a valid binding "
                "source, and a JARVIS execution_id is not a gateway_session_id. "
                "Runtime host, paths, scheduler identity, and dataset metadata are read "
                "only from the durable JARVIS result. The relay allocates the desktop "
                "loopback port. A bounded readiness miss returns outcome=pending with "
                "nullable URLs and an exact retry_selector; it does not fail, cancel, or "
                "replace the JARVIS execution or its connectors. Reissue this tool with "
                "the same binding, name, and policy to resume the same gateway. When "
                "outcome=ready, copy the top-level gateway_session_id unchanged into the "
                "viewer-opening tool; service_instance_id is not a gateway identity."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "binding": jarvis_service_runtime_handoff_json_schema(clusters=clusters),
                    "cluster": {
                        "type": "string",
                        **({"enum": sorted(clusters)} if clusters is not None else {}),
                    },
                    "source_job_id": durable_record_id_json_schema(),
                    "source_artifact_id": durable_record_id_json_schema(),
                    "package_id": {"type": "string", "minLength": 1, "maxLength": 256},
                    "package_name": {"type": "string", "minLength": 1, "maxLength": 256},
                    "name": {"type": "string", "minLength": 1, "maxLength": 256},
                    "readiness_timeout_seconds": {
                        "type": "number",
                        "exclusiveMinimum": 0,
                        "maximum": 3600,
                        "default": 300,
                    },
                    "poll_seconds": {
                        "type": "number",
                        "exclusiveMinimum": 0,
                        "maximum": 60,
                        "default": 2,
                    },
                },
                "if": {"required": ["binding"]},
                "then": {
                    "not": {
                        "anyOf": [
                            {"required": ["cluster"]},
                            {"required": ["source_job_id"]},
                            {"required": ["source_artifact_id"]},
                            {"required": ["package_id"]},
                            {"required": ["package_name"]},
                        ]
                    }
                },
                "else": {
                    "required": [
                        "cluster",
                        "source_job_id",
                        "source_artifact_id",
                        "package_id",
                        "package_name",
                    ]
                },
                "additionalProperties": False,
            },
            "outputSchema": {
                "type": "object",
                "properties": {
                    "outcome": {"type": "string", "enum": ["ready", "pending"]},
                    "retry_selector": {
                        "anyOf": [{"type": "object"}, {"type": "null"}],
                    },
                    "scheduler_action": {"const": "none"},
                    "relay_action": {"const": "none"},
                    "gateway_session_id": {
                        **durable_record_id_json_schema(),
                        "pattern": r"^gateway_[0-9a-f]{32}$",
                        "description": (
                            "Exact relay gateway identity to pass unchanged to a viewer-opening "
                            "tool. It is equal to gateway_session.session_id."
                        ),
                    },
                    "gateway_session": {"type": "object"},
                    "connect_url": {"type": ["string", "null"]},
                    "health_url": {"type": ["string", "null"]},
                    "stream_url": {"type": ["string", "null"]},
                    "events_url": {"type": ["string", "null"]},
                    "state_url": {"type": ["string", "null"]},
                    "command_url": {"type": ["string", "null"]},
                    "scheduler_cancel_requested": {"const": False},
                },
                "required": [
                    "outcome",
                    "retry_selector",
                    "scheduler_action",
                    "relay_action",
                    "gateway_session_id",
                    "gateway_session",
                    "connect_url",
                    "health_url",
                    "stream_url",
                    "events_url",
                    "state_url",
                    "command_url",
                    "scheduler_cancel_requested",
                ],
                "allOf": [
                    {
                        "if": {"properties": {"outcome": {"const": "ready"}}},
                        "then": {
                            "properties": {
                                "retry_selector": {"type": "null"},
                                "connect_url": {"type": "string"},
                                "health_url": {"type": "string"},
                                "stream_url": {"type": "string"},
                                "events_url": {"type": "string"},
                                "state_url": {"type": "string"},
                                "command_url": {"type": "string"},
                            }
                        },
                    },
                    {
                        "if": {"properties": {"outcome": {"const": "pending"}}},
                        "then": {
                            "properties": {
                                "retry_selector": {"type": "object"},
                                "connect_url": {"type": "null"},
                                "health_url": {"type": "null"},
                                "stream_url": {"type": "null"},
                                "events_url": {"type": "null"},
                                "state_url": {"type": "null"},
                                "command_url": {"type": "null"},
                            }
                        },
                    },
                ],
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
                "properties": {"session_id": durable_record_id_json_schema()},
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
                    "session_id": durable_record_id_json_schema(),
                    "state": {"type": "string"},
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
                "properties": {"session_id": durable_record_id_json_schema()},
                "required": ["session_id"],
                "additionalProperties": False,
            },
        },
        *virtual_jarvis_tool_definitions(clusters=resolved_jarvis_clusters),
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
    session: McpSessionState | None,
    observed_remote_mcp_catalog_revision: str | None,
    require_advertised_remote_mcp_catalog: bool,
) -> JSON:
    name = _required_str(params, "name")
    static_names = static_mcp_tool_names()
    catalog: VirtualRemoteMcpCatalog | None = None
    if name in static_names:
        if name not in _authorized_static_tool_names(profile):
            raise ValueError(f"tool is not available in MCP profile {profile!r}: {name}")
    else:
        catalog = _remote_mcp_catalog(profile=profile, reserved_names=static_names)
        _require_compatible_remote_mcp_catalog(
            profile=profile,
            observed_revision=observed_remote_mcp_catalog_revision,
            current_revision=catalog.revision,
        )
        if name not in catalog.tools:
            raise ValueError(f"tool is not available in MCP profile {profile!r}: {name}")
    if is_virtual_jarvis_tool(name):
        catalog = _remote_mcp_catalog(profile=profile, reserved_names=static_names)
        if require_advertised_remote_mcp_catalog:
            _require_compatible_remote_mcp_catalog(
                profile=profile,
                observed_revision=observed_remote_mcp_catalog_revision,
                current_revision=catalog.revision,
            )
    arguments = _object(params.get("arguments", {}))
    arguments = _restore_session_remote_job_route(
        name=name,
        arguments=arguments,
        queue=queue,
        session=session,
    )
    if name == "relay_submit_jarvis_pipeline":
        result = _submit_jarvis_pipeline(arguments, queue=queue, settings=settings)
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
        result = _submit_remote_agent(arguments, queue=queue, settings=settings)
    elif name == "relay_status":
        result = _status_job(arguments, queue=queue, settings=settings)
    elif name == "relay_cancel":
        result = _cancel_job(arguments, queue=queue, settings=settings)
    elif name == "relay_observe":
        result = _observe_job(arguments, queue=queue, settings=settings)
    elif name == "relay_wait":
        result = _wait_job(arguments, queue=queue, settings=settings)
    elif name == "relay_submit_jarvis_job":
        result = _submit_jarvis_job(arguments, queue=queue, settings=settings)
    elif name == "relay_submit_remote_agent":
        result = _submit_remote_agent(arguments, queue=queue, settings=settings)
    elif name == "relay_submit_mcp_call":
        result = _submit_mcp_call(arguments, queue=queue, settings=settings)
    elif name == "relay_call_jarvis_mcp":
        result = _submit_jarvis_mcp_call(arguments, queue=queue, settings=settings)
    elif is_virtual_jarvis_tool(name):
        call_arguments = virtual_jarvis_call_arguments(name, arguments)
        if require_advertised_remote_mcp_catalog:
            if catalog is None:
                raise ValueError("JARVIS virtual tool catalog was not resolved")
            cluster = _required_str(call_arguments, "cluster")
            expected_route_revision = catalog.cluster_route_revisions.get(cluster)
            if expected_route_revision is None:
                raise ValueError(
                    f"cluster route is not available in the advertised catalog: {cluster}"
                )
            expected_artifact_digest = catalog.jarvis_artifact_bindings.get(cluster)
            if expected_artifact_digest is None:
                raise ValueError(
                    "JARVIS MCP identity is not available in the advertised catalog for "
                    f"{cluster}; refresh JARVIS MCP discovery and call tools/list again"
                )
            call_arguments["expected_cluster_route_revision"] = expected_route_revision
            call_arguments["catalog_expected_server_artifact_digest"] = expected_artifact_digest
        result = _submit_jarvis_mcp_call(call_arguments, queue=queue, settings=settings)
        if catalog is None:
            raise ValueError("JARVIS virtual tool catalog was not resolved")
        result["catalog_revision"] = catalog.revision
    elif catalog is not None and name in catalog.tools:
        cluster = _required_str(arguments, "cluster")
        virtual_tool = catalog.tools[name]
        route = catalog.resolve(name, cluster)
        forwarded_arguments = catalog.forwarded_arguments(name, arguments)
        relay_arguments = catalog.relay_arguments(name, arguments)
        described_package_name: str | None = None
        package_input_route: JarvisPackageInputRoute | None = None
        package_cache_key: str | None = None
        pipeline_cache_key: str | None = None
        pipeline_input_route = None
        automatic_input_uses: tuple[ArtifactUse, ...] = ()
        staged_input_manifest_sha256: str | None = None
        staged_input_bindings: tuple[JarvisPipelineInputBinding, ...] = ()
        removed_input_binding_identities: tuple[tuple[str, str], ...] = ()
        binding_mutation_required = False
        run_input_manifest: JarvisRunInputManifest | None = None
        base_idempotency_key: str | None = None
        if route.contract == REGISTERED_JARVIS_CONTRACT_ID:
            if session is None:
                raise ValueError("registered JARVIS package semantics require an MCP session")
            if route.remote_tool_name == "jarvis_describe":
                # v3.6 descriptions are control queries whose structured result is part of
                # the agent-facing contract. Reconcile them transparently even when the
                # generated default is omitted or a legacy client sends false.
                relay_arguments["wait_for_terminal"] = True
                if forwarded_arguments.get("target") == "package":
                    described_package_name = _required_str(forwarded_arguments, "package_name")
                    package_input_route = jarvis_package_input_route(
                        cluster=cluster,
                        server_name=route.server_name,
                        cluster_route_revision=route.cluster_route_revision,
                        registration_revision=route.registration_revision,
                        expected_server_artifact_digest=route.expected_server_artifact_digest,
                        package_name=described_package_name,
                    )
                    package_cache_key = package_input_route.identity_sha256()
            elif route.remote_tool_name == "jarvis_add_step":
                package_name = _required_str(forwarded_arguments, "package_name")
                package_input_route = jarvis_package_input_route(
                    cluster=cluster,
                    server_name=route.server_name,
                    cluster_route_revision=route.cluster_route_revision,
                    registration_revision=route.registration_revision,
                    expected_server_artifact_digest=route.expected_server_artifact_digest,
                    package_name=package_name,
                )
                package_cache_key = package_input_route.identity_sha256()
                package_contract = session.jarvis_package_inputs(package_cache_key)
                if package_contract is None:
                    durable_contract = queue.get_jarvis_package_input_contract(package_input_route)
                    if durable_contract is not None:
                        package_contract = jarvis_package_input_contract_from_record(
                            durable_contract
                        )
                        session.remember_jarvis_package_inputs(package_contract)
                if package_contract is None:
                    raise ValueError(
                        "jarvis_add_step requires a successful jarvis_describe package call on "
                        "this exact registered cluster route before package configuration"
                    )
                staged = stage_jarvis_add_step_inputs(
                    forwarded_arguments,
                    contract=package_contract,
                    definition=_remote_cluster_definition(cluster),
                    settings=settings,
                )
                forwarded_arguments = staged.arguments
                automatic_input_uses = staged.artifact_uses
                staged_input_manifest_sha256 = staged.manifest_sha256
                staged_input_bindings = staged.bindings
                if automatic_input_uses:
                    # Staged inputs become durable pipeline lineage only after JARVIS accepts
                    # this exact add-step. Never return an asynchronous receipt that can lose
                    # that acceptance edge across an MCP restart.
                    relay_arguments["wait_for_terminal"] = True
                pipeline_input_route = jarvis_pipeline_input_route(
                    cluster=cluster,
                    server_name=route.server_name,
                    cluster_route_revision=route.cluster_route_revision,
                    registration_revision=route.registration_revision,
                    expected_server_artifact_digest=route.expected_server_artifact_digest,
                    pipeline_id=_required_str(forwarded_arguments, "pipeline_id"),
                    owner_session_id=settings.owner_session_id,
                    owner_session_generation_id=settings.owner_session_generation_id,
                )
                pipeline_cache_key = pipeline_input_route.identity_sha256()
            elif route.remote_tool_name == "jarvis_edit_step":
                pipeline_input_route = jarvis_pipeline_input_route(
                    cluster=cluster,
                    server_name=route.server_name,
                    cluster_route_revision=route.cluster_route_revision,
                    registration_revision=route.registration_revision,
                    expected_server_artifact_digest=route.expected_server_artifact_digest,
                    pipeline_id=_required_str(forwarded_arguments, "pipeline_id"),
                    owner_session_id=settings.owner_session_id,
                    owner_session_generation_id=settings.owner_session_generation_id,
                )
                pipeline_cache_key = pipeline_input_route.identity_sha256()
                current_bindings = queue.get_jarvis_pipeline_input_bindings(pipeline_input_route)
                if current_bindings is not None:
                    staged = stage_jarvis_edit_step_inputs(
                        forwarded_arguments,
                        current=current_bindings,
                        definition=_remote_cluster_definition(cluster),
                        settings=settings,
                    )
                    forwarded_arguments = staged.arguments
                    automatic_input_uses = staged.artifact_uses
                    staged_input_manifest_sha256 = staged.manifest_sha256
                    staged_input_bindings = staged.bindings
                    removed_input_binding_identities = staged.removed_binding_identities
                    binding_mutation_required = bool(
                        staged_input_bindings or removed_input_binding_identities
                    )
                    if binding_mutation_required:
                        relay_arguments["wait_for_terminal"] = True
            elif route.remote_tool_name == "jarvis_run":
                pipeline_input_route = jarvis_pipeline_input_route(
                    cluster=cluster,
                    server_name=route.server_name,
                    cluster_route_revision=route.cluster_route_revision,
                    registration_revision=route.registration_revision,
                    expected_server_artifact_digest=route.expected_server_artifact_digest,
                    pipeline_id=_required_str(forwarded_arguments, "pipeline_id"),
                    owner_session_id=settings.owner_session_id,
                    owner_session_generation_id=settings.owner_session_generation_id,
                )
                pipeline_cache_key = pipeline_input_route.identity_sha256()
                base_idempotency_key = str(
                    relay_arguments.get("idempotency_key")
                    or (
                        f"mcp:virtual:{cluster}:{route.server_name}:"
                        f"{route.remote_tool_name}:{uuid4().hex}"
                    )
                )
                current_bindings = queue.get_jarvis_pipeline_input_bindings(pipeline_input_route)
                if current_bindings is not None and current_bindings.bindings:
                    run_input_manifest = queue.get_jarvis_run_input_manifest(
                        pipeline_input_route,
                        idempotency_key=base_idempotency_key,
                    )
                    if run_input_manifest is None:
                        resolutions = reconcile_jarvis_run_inputs(
                            current_bindings,
                            definition=_remote_cluster_definition(cluster),
                            settings=settings,
                        )
                        run_input_manifest = queue.put_jarvis_run_input_manifest(
                            JarvisRunInputManifest.create(
                                route=pipeline_input_route,
                                idempotency_key=base_idempotency_key,
                                resolutions=resolutions,
                            )
                        )
                        queue.update_jarvis_pipeline_input_bindings(
                            pipeline_input_route,
                            upserts=tuple(item.binding for item in run_input_manifest.resolutions),
                        )
                    automatic_input_uses = run_input_manifest.artifact_uses
                else:
                    durable_lineage = queue.get_jarvis_pipeline_input_lineage(pipeline_input_route)
                    durable_uses = () if durable_lineage is None else durable_lineage.artifact_uses
                    automatic_input_uses = tuple(
                        merge_artifact_uses(
                            list(session.jarvis_pipeline_inputs(pipeline_cache_key)),
                            durable_uses,
                        )
                    )
        merged_input_uses = merge_artifact_uses(
            _artifact_use_refs(relay_arguments),
            automatic_input_uses,
        )
        if merged_input_uses:
            relay_arguments["used_artifact_refs"] = [
                artifact_use_payload(item) for item in merged_input_uses
            ]
        reconciliation_required = (
            route.contract == REGISTERED_JARVIS_CONTRACT_ID
            and route.remote_tool_name == "jarvis_describe"
        ) or bool(automatic_input_uses)
        reconciliation_idempotency_key = "mcp:virtual:reconcile:" + _stable_digest(
            {
                "cluster": cluster,
                "server_name": route.server_name,
                "cluster_route_revision": route.cluster_route_revision,
                "registration_revision": route.registration_revision,
                "expected_server_artifact_digest": route.expected_server_artifact_digest,
                "tool": route.remote_tool_name,
                "arguments": forwarded_arguments,
                "used_artifact_refs": [artifact_use_payload(item) for item in merged_input_uses],
            }
        )
        if base_idempotency_key is None:
            base_idempotency_key = str(
                relay_arguments.get("idempotency_key")
                or (
                    reconciliation_idempotency_key
                    if reconciliation_required
                    else (
                        f"mcp:virtual:{cluster}:{route.server_name}:"
                        f"{route.remote_tool_name}:{uuid4().hex}"
                    )
                )
            )
        result = _submit_mcp_call(
            {
                "cluster": cluster,
                "registered_route": True,
                "registered_remote_mcp_route": True,
                "server": route.command,
                "server_args": list(route.args),
                "env_from": dict(route.env_from),
                "expected_server_artifact_digest": route.expected_server_artifact_digest,
                "expected_registered_contract": (
                    route.contract if route.contract == REGISTERED_JARVIS_CONTRACT_ID else None
                ),
                "expected_cluster_route_revision": route.cluster_route_revision,
                "registered_server_name": route.server_name,
                "expected_remote_mcp_registration_revision": (route.registration_revision),
                "control_query_evidence": (
                    route.control_query_evidence.model_dump(mode="json")
                    if route.expected_server_artifact_digest is not None
                    and is_remote_mcp_control_query(virtual_tool.remote_tool)
                    and route.control_query_evidence is not None
                    else None
                ),
                "tool": route.remote_tool_name,
                "arguments": forwarded_arguments,
                "jarvis_input_manifest": (
                    run_input_manifest.model_dump(mode="json")
                    if run_input_manifest is not None
                    else None
                ),
                "timeout_seconds": route.timeout_seconds,
                **relay_arguments,
                "idempotency_key": base_idempotency_key,
            },
            queue=queue,
            settings=settings,
        )
        if (
            route.contract == REGISTERED_JARVIS_CONTRACT_ID
            and route.remote_tool_name == "jarvis_describe"
        ):
            _require_terminal_staging_reconciliation(result, operation="jarvis_describe")
        if described_package_name is not None and package_cache_key is not None:
            assert session is not None
            package_contract = parse_jarvis_package_input_contract(
                result,
                cache_key=package_cache_key,
            )
            if result.get("state") == "succeeded" and package_contract is None:
                raise ValueError(
                    "successful jarvis_describe package call omitted its package input contract"
                )
            if package_contract is not None:
                if described_package_name not in package_contract.package_names:
                    raise ValueError(
                        "jarvis_describe returned a different package than the requested package"
                    )
                for package_name in package_contract.package_names:
                    alias_route = jarvis_package_input_route(
                        cluster=cluster,
                        server_name=route.server_name,
                        cluster_route_revision=route.cluster_route_revision,
                        registration_revision=route.registration_revision,
                        expected_server_artifact_digest=route.expected_server_artifact_digest,
                        package_name=package_name,
                    )
                    alias_contract = JarvisPackageInputContract(
                        cache_key=alias_route.identity_sha256(),
                        package_names=package_contract.package_names,
                        local_file_settings=package_contract.local_file_settings,
                        settings_sha256=package_contract.settings_sha256,
                    )
                    saved_contract = queue.put_jarvis_package_input_contract(
                        jarvis_package_input_contract_record(
                            route=alias_route,
                            contract=alias_contract,
                        )
                    )
                    session.remember_jarvis_package_inputs(
                        jarvis_package_input_contract_from_record(saved_contract)
                    )
        if route.remote_tool_name == "jarvis_add_step" and automatic_input_uses:
            _require_terminal_staging_reconciliation(result, operation="jarvis_add_step")
        if route.remote_tool_name == "jarvis_edit_step" and binding_mutation_required:
            _require_terminal_staging_reconciliation(result, operation="jarvis_edit_step")
        if (
            route.remote_tool_name == "jarvis_add_step"
            and pipeline_input_route is not None
            and staged_input_bindings
            and result.get("state") == "succeeded"
            and result.get("terminal") is True
        ):
            observed_step_id = _jarvis_add_step_result_step_id(result)
            expected_step_ids = {item.step_id for item in staged_input_bindings}
            if expected_step_ids != {observed_step_id}:
                raise ValueError(
                    "jarvis_add_step returned a different step identity than its staged inputs"
                )
            queue.update_jarvis_pipeline_input_bindings(
                pipeline_input_route,
                upserts=staged_input_bindings,
            )
        if (
            route.remote_tool_name == "jarvis_edit_step"
            and pipeline_input_route is not None
            and binding_mutation_required
            and result.get("state") == "succeeded"
            and result.get("terminal") is True
        ):
            queue.update_jarvis_pipeline_input_bindings(
                pipeline_input_route,
                upserts=staged_input_bindings,
                remove=removed_input_binding_identities,
            )
        if (
            route.remote_tool_name == "jarvis_add_step"
            and pipeline_cache_key is not None
            and pipeline_input_route is not None
            and automatic_input_uses
            and staged_input_manifest_sha256 is not None
            and result.get("state") == "succeeded"
            and result.get("terminal") is True
        ):
            assert session is not None
            durable_lineage = queue.merge_jarvis_pipeline_input_lineage(
                pipeline_input_route,
                automatic_input_uses,
                manifest_sha256=staged_input_manifest_sha256,
            )
            session.remember_jarvis_pipeline_inputs(
                pipeline_cache_key,
                durable_lineage.artifact_uses,
            )
        result["catalog_revision"] = catalog.revision
    elif name == "relay_get_job":
        job_id = _required_durable_record_id(arguments, "job_id")
        result = queue.get_job(job_id).model_dump(mode="json")
        transform = queue.get_transform_ref(job_id)
        result["transform"] = None if transform is None else transform.model_dump(mode="json")
    elif name == "relay_get_job_status":
        result = job_status(queue, _required_durable_record_id(arguments, "job_id"))
    elif name == "relay_monitor_job":
        result = monitor_job(
            queue,
            _required_durable_record_id(arguments, "job_id"),
            cursor=int(arguments.get("cursor", 1)),
            limit=_response_page_limit(arguments),
        )
    elif name == "relay_watch_job_events":
        events, cursor = queue.drain_events(
            Cursor(
                job_id=_required_durable_record_id(arguments, "job_id"),
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
            _required_durable_record_id(arguments, "job_id"),
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
            _required_durable_record_id(arguments, "task_id"),
            cursor=int(arguments.get("cursor", 1)),
            limit=_response_page_limit(arguments),
        )
        result = {
            "events": [event.model_dump(mode="json") for event in events],
            "next_cursor": cursor,
        }
    elif name == "relay_read_job_log":
        job = queue.get_job(_required_durable_record_id(arguments, "job_id"))
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
            _required_durable_record_id(arguments, "job_id"),
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
    elif name == "relay_artifact_lineage":
        has_job = arguments.get("job_id") is not None
        has_artifact = arguments.get("artifact_id") is not None
        if has_job == has_artifact:
            raise ValueError("pass exactly one of job_id or artifact_id")
        result = (
            _used_artifacts_tool(arguments, queue=queue, settings=settings)
            if has_job
            else _used_by_tool(arguments, queue=queue, settings=settings)
        )
    elif name == "relay_read_artifact":
        result = _read_model_artifact_bytes(
            queue,
            _required_durable_record_id(arguments, "artifact_id"),
        )
    elif name == "relay_record_progress":
        result = _record_progress(arguments, queue=queue)
    elif name == "relay_list_progress":
        cursor = _response_page_cursor(arguments)
        limit = _response_page_limit(arguments)
        progress, next_cursor, total = queue.list_progress_page(
            _required_durable_record_id(arguments, "job_id"),
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
        result = _queue_cancel_tool(arguments, queue=queue, settings=settings)
    elif name == "relay_queue_list":
        result = _queue_list_tool(arguments, queue=queue, settings=settings)
    elif name == "relay_queue_diagnose":
        result = _queue_diagnose_tool(arguments, queue=queue, settings=settings)
    elif name == "relay_queue_stale":
        result = _queue_stale_tool(arguments, queue=queue, settings=settings)
    elif name == "relay_queue_cleanup_stale":
        result = _queue_cleanup_stale_tool(arguments, queue=queue, settings=settings)
    elif name == "relay_retention_plan":
        plan = TerminalRetentionCoordinator(queue, settings.spool_dir).plan(
            _required_durable_record_id(arguments, "job_id"),
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
        job_id = _required_durable_record_id(arguments, "job_id")
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
                _required_durable_record_id(arguments, "job_id"),
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
        result = _worker_status_tool(arguments, queue=queue)
    elif name == "relay_create_monitor_rule":
        result = queue.append_monitor_rule(_monitor_rule_from_arguments(arguments)).model_dump(
            mode="json"
        )
    elif name == "relay_list_monitor_rules":
        job_id = _optional_durable_record_id(arguments, "job_id")
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
    elif name == "relay_bind_jarvis_runtime":
        result = _bind_jarvis_runtime(arguments, queue=queue, settings=settings)
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
            "gateway_sessions": [public_gateway_session(session) for session in sessions],
            "source_cursor": cursor,
            "source_limit": limit,
            "source_next_cursor": next_cursor,
            "source_total": total,
            "source_total_semantics": "global_gateway_sequence_high_water",
            "filters_apply_within_source_window": True,
        }
    elif name == "relay_get_gateway_session":
        result = public_gateway_session(
            queue.get_gateway_session(_required_durable_record_id(arguments, "session_id"))
        )
    elif name == "relay_update_gateway_session":
        result = _update_gateway_session(arguments, queue=queue)
    elif name == "relay_close_gateway_session":
        result = public_gateway_session(
            queue.close_gateway_session(_required_durable_record_id(arguments, "session_id"))
        )
    else:
        raise ValueError(f"unknown tool: {name}")
    if session is not None:
        session.observe_remote_job_result(result)
    return {
        "content": [{"type": "text", "text": _serialize_tool_result(result)}],
        "structuredContent": result,
        "isError": _mcp_result_delivery_failed(result),
    }


def _serialize_tool_result(result: JSON) -> str:
    """Keep actionable verified MCP output ahead of bulk operational evidence."""
    if "service_runtime_bindings" in result or "mcp_result" in result:
        compact_keys = (
            "service_runtime_bindings",
            "mcp_result_artifact",
            "cluster",
            "job_id",
            "route_revision",
            "state",
            "kind",
            "terminal",
            "remote",
            "last_error",
            "mcp_result",
        )
        bulk_keys = ("job", "logs", "artifacts")
        ordered: JSON = {}
        for key in compact_keys:
            if key in result:
                ordered[key] = result[key]
        for key, value in result.items():
            if key not in compact_keys and key not in bulk_keys:
                ordered[key] = value
        for key in bulk_keys:
            if key in result:
                ordered[key] = result[key]
        return json.dumps(ordered)
    return json.dumps(result, sort_keys=True)


def _restore_session_remote_job_route(
    *,
    name: str,
    arguments: JSON,
    queue: ClioCoreQueue,
    session: McpSessionState | None,
) -> JSON:
    """Restore an omitted remote route learned on this MCP connection.

    Explicit handles remain authoritative and reconnecting clients must still
    preserve the complete ``cluster + job_id + route_revision`` receipt. This
    connection-local convenience only prevents a returned remote job ID from
    being mistaken for a desktop-queue job on the immediate follow-up call.
    """
    if (
        session is None
        or name not in _REMOTE_JOB_FOLLOWUP_TOOL_NAMES
        or "cluster" in arguments
        or "route_revision" in arguments
    ):
        return arguments
    raw_job_id = arguments.get("job_id")
    if not isinstance(raw_job_id, str):
        return arguments
    job_id = validate_durable_record_id(raw_job_id)
    try:
        queue.get_job(job_id)
    except NotFoundError:
        pass
    else:
        return arguments
    route = session.remote_job_route(job_id)
    if route is None:
        return arguments
    cluster, route_revision = route
    return {
        **arguments,
        "cluster": cluster,
        "route_revision": route_revision,
    }


def _require_compatible_remote_mcp_catalog(
    *,
    profile: str,
    observed_revision: str | None,
    current_revision: str,
) -> None:
    """Reject catalog churn on a connection that advertised an older revision.

    MCP clients may cache a prior ``tools/list`` result and open a fresh stdio
    connection only when they execute a tool.  In that case there is no
    connection-local revision to compare, so dispatch uses the current durable,
    profile-filtered catalog as the authority.  The caller still requires the
    alias to exist in that catalog before selecting its immutable route.
    """
    if observed_revision is None:
        return
    if observed_revision != current_revision:
        raise ValueError(
            "remote MCP catalog changed after tools/list for profile "
            f"{profile!r}; call tools/list again before invoking a virtual remote MCP tool"
        )


def _remote_mcp_catalog(
    *,
    profile: str,
    reserved_names: set[str],
) -> VirtualRemoteMcpCatalog:
    try:
        catalog = load_virtual_remote_mcp_catalog(
            profile=profile,
            reserved_names=reserved_names,
        )
        cache = RemoteMcpSchemaCache.load(default_remote_mcp_cache_path())
    except (ConfigurationError, OSError, ValidationError) as exc:
        return unavailable_virtual_remote_mcp_catalog(str(exc))
    now = datetime.now(UTC)
    jarvis_bindings: dict[str, str | None] = {}
    for cluster in catalog.cluster_route_revisions:
        entry = cache.entry_for(cluster, JARVIS_MCP_CACHE_SERVER_NAME)
        if entry is None:
            jarvis_bindings[cluster] = None
            continue
        try:
            jarvis_bindings[cluster] = jarvis_mcp_artifact_binding_from_entry(entry, now=now)
        except ValueError:
            jarvis_bindings[cluster] = None
    revision = _stable_digest(
        {
            "remote_mcp_catalog_revision": catalog.revision,
            "jarvis_artifact_bindings": jarvis_bindings,
        }
    )
    return VirtualRemoteMcpCatalog(
        revision=revision,
        tools=catalog.tools,
        issues=catalog.issues,
        cluster_route_revisions=catalog.cluster_route_revisions,
        jarvis_artifact_bindings=jarvis_bindings,
    )


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
    return cluster_route_revision(definition)


def _validated_route_revision(value: object) -> str:
    """Validate one opaque route token before comparing or routing with it."""

    if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise ValueError(
            "route_revision must be the 64-character lowercase hexadecimal token "
            "copied from the same relay job receipt"
        )
    return value


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
    if raw_revision is None:
        raise ValueError("route_revision is required when cluster routes an existing job handle")
    revision = _validated_route_revision(raw_revision)
    definition = _remote_cluster_definition(raw_cluster)
    expected_revision = _route_revision(definition)
    if not hmac.compare_digest(revision, expected_revision):
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


def _status_job(
    arguments: JSON,
    *,
    queue: ClioCoreQueue,
    settings: RelaySettings,
) -> JSON:
    job_id = _required_durable_record_id(arguments, "job_id")
    target = _job_target(arguments)
    if target is not None and should_execute_on_cluster(target):
        if settings.owner_session_id is not None:
            with OwnedSessionApiClient(definition=target, settings=settings) as client:
                result = _owned_json(
                    client,
                    method="GET",
                    path=f"/jobs/{job_id}/status",
                    label="owned remote job status",
                )
            _validate_owned_job_status(result, job_id=job_id, cluster=target.name)
        else:
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


def _used_artifacts_tool(
    arguments: JSON,
    *,
    queue: ClioCoreQueue,
    settings: RelaySettings,
) -> JSON:
    """Query a job's immutable artifact dependencies through its bound route."""
    job_id = _required_durable_record_id(arguments, "job_id")
    cursor = _optional_durable_record_id(arguments, "cursor")
    limit = _response_page_limit(arguments)
    target = _job_target(arguments)
    if target is not None and should_execute_on_cluster(target):
        query: dict[str, object] = {"limit": limit}
        command = ["job", "used-artifacts", job_id, "--limit", str(limit)]
        if cursor is not None:
            query["cursor"] = cursor
            command.extend(["--cursor", cursor])
        if settings.owner_session_id is not None:
            with OwnedSessionApiClient(definition=target, settings=settings) as client:
                result = _owned_json(
                    client,
                    method="GET",
                    path=f"/jobs/{job_id}/used-artifacts",
                    query=query,
                    label="owned remote used-artifact query",
                )
        else:
            result = _remote_json(target, command, "remote used-artifact query")
        result["cluster"] = target.name
        result["route_revision"] = _route_revision(target)
        return result
    _require_local_job_cluster(queue, job_id, target)
    records, next_cursor, total = queue.list_used_artifacts_page(
        job_id,
        cursor=cursor,
        limit=limit,
    )
    result: JSON = {
        "used_artifacts": [record.model_dump(mode="json") for record in records],
        "cursor": cursor,
        "limit": limit,
        "next_cursor": next_cursor,
        "total": total,
    }
    if target is not None:
        result["cluster"] = target.name
        result["route_revision"] = _route_revision(target)
    return result


def _used_by_tool(
    arguments: JSON,
    *,
    queue: ClioCoreQueue,
    settings: RelaySettings,
) -> JSON:
    """Query downstream consumers of an artifact through its bound route."""
    artifact_id = _required_durable_record_id(arguments, "artifact_id")
    cursor = _optional_durable_record_id(arguments, "cursor")
    limit = _response_page_limit(arguments)
    target = _job_target(arguments)
    if target is not None and should_execute_on_cluster(target):
        query: dict[str, object] = {"limit": limit}
        command = ["job", "used-by", artifact_id, "--limit", str(limit)]
        if cursor is not None:
            query["cursor"] = cursor
            command.extend(["--cursor", cursor])
        if settings.owner_session_id is not None:
            with OwnedSessionApiClient(definition=target, settings=settings) as client:
                result = _owned_json(
                    client,
                    method="GET",
                    path=f"/artifacts/{artifact_id}/used-by",
                    query=query,
                    label="owned remote artifact-consumer query",
                )
        else:
            result = _remote_json(target, command, "remote artifact-consumer query")
        result["cluster"] = target.name
        result["route_revision"] = _route_revision(target)
        return result
    artifact = queue.get_artifact(artifact_id)
    _require_local_job_cluster(queue, artifact.job_id, target)
    records, next_cursor, total = queue.list_artifact_users_page(
        artifact_id,
        cursor=cursor,
        limit=limit,
    )
    result = {
        "used_by": [record.model_dump(mode="json") for record in records],
        "cursor": cursor,
        "limit": limit,
        "next_cursor": next_cursor,
        "total": total,
    }
    if target is not None:
        result["cluster"] = target.name
        result["route_revision"] = _route_revision(target)
    return result


def _queue_cancel_tool(
    arguments: JSON,
    *,
    queue: ClioCoreQueue,
    settings: RelaySettings,
) -> JSON:
    """Cancel one queue job through the same local-or-SSH route as the CLI."""
    job_id = _required_durable_record_id(arguments, "job_id")
    target = _queue_tool_target(arguments)
    cluster = _optional_str(arguments, "cluster")
    cancel_scheduler = _boolean_argument(arguments, "cancel_scheduler_job", default=False)
    if target is not None and should_execute_on_cluster(target):
        if settings.owner_session_id is not None:
            with OwnedSessionApiClient(definition=target, settings=settings) as client:
                payload = _owned_json(
                    client,
                    method="POST",
                    path=f"/queue/jobs/{job_id}/cancel",
                    body={
                        "cluster": target.name,
                        "cancel_scheduler_job": cancel_scheduler,
                    },
                    label="owned remote queue cancellation",
                )
            _validate_owned_job_status(payload, job_id=job_id, cluster=target.name)
            return _queue_route_result(payload, target=target, remote=True)
        command = ["queue", "cancel", job_id, "--cluster", target.name]
        command.append("--cancel-scheduler-job" if cancel_scheduler else "--keep-scheduler-job")
        return _queue_route_result(
            _remote_json(target, command, "remote queue cancellation"),
            target=target,
            remote=True,
        )
    result = cast(
        JSON,
        cancel_queue_job(
            queue,
            job_id,
            cluster=cluster,
            scheduler_policy="request-scheduler" if cancel_scheduler else "relay-only",
        ),
    )
    return _queue_route_result(result, target=target, remote=False)


def _queue_list_tool(
    arguments: JSON,
    *,
    queue: ClioCoreQueue,
    settings: RelaySettings,
) -> JSON:
    """List the selected cluster queue locally or through its configured SSH route."""
    target = _queue_tool_target(arguments)
    cluster = _optional_str(arguments, "cluster")
    raw_state = arguments.get("state")
    if raw_state is not None and not isinstance(raw_state, str):
        raise ValueError("state must be a string")
    state = JobState(raw_state) if isinstance(raw_state, str) else None
    raw_kind = arguments.get("kind")
    if raw_kind is not None and not isinstance(raw_kind, str):
        raise ValueError("kind must be a string")
    kind = JobKind(raw_kind) if isinstance(raw_kind, str) else None
    include_terminal = _boolean_argument(arguments, "include_terminal", default=False)
    cursor = _response_page_cursor(arguments)
    limit = _response_page_limit(arguments)
    scan_limit = _bounded_integer_limit(
        arguments,
        field_name="scan_limit",
        default=1_000,
        maximum=10_000,
    )
    if scan_limit < limit:
        raise ValueError("scan_limit must be greater than or equal to limit")
    if target is not None and should_execute_on_cluster(target):
        if settings.owner_session_id is not None:
            query: dict[str, object] = {
                "cluster": target.name,
                "include_terminal": include_terminal,
                "cursor": cursor,
                "limit": limit,
                "scan_limit": scan_limit,
            }
            if state is not None:
                query["state"] = state.value
            if kind is not None:
                query["kind"] = kind.value
            with OwnedSessionApiClient(definition=target, settings=settings) as client:
                payload = _owned_json(
                    client,
                    method="GET",
                    path="/queue",
                    query=query,
                    label="owned remote queue listing",
                )
            return _queue_route_result(payload, target=target, remote=True)
        command = [
            "queue",
            "list",
            "--cluster",
            target.name,
            "--cursor",
            str(cursor),
            "--limit",
            str(limit),
            "--scan-limit",
            str(scan_limit),
        ]
        if state is not None:
            command.extend(["--state", state.value])
        if kind is not None:
            command.extend(["--kind", kind.value])
        if include_terminal:
            command.append("--include-terminal")
        return _queue_route_result(
            _remote_json(target, command, "remote queue listing"),
            target=target,
            remote=True,
        )
    result = cast(
        JSON,
        list_queue_jobs(
            queue,
            cluster=cluster,
            state=state,
            kind=kind,
            include_terminal=include_terminal,
            cursor=cursor,
            limit=limit,
            scan_limit=scan_limit,
        ),
    )
    return _queue_route_result(result, target=target, remote=False)


def _queue_diagnose_tool(
    arguments: JSON,
    *,
    queue: ClioCoreQueue,
    settings: RelaySettings,
) -> JSON:
    """Diagnose one exact queue job on its configured local or SSH route."""
    job_id = _required_durable_record_id(arguments, "job_id")
    target = _queue_tool_target(arguments)
    cluster = _optional_str(arguments, "cluster")
    older_than_seconds = _positive_integer_argument(
        arguments,
        "older_than_seconds",
        default=7_200,
    )
    scan_limit = _bounded_integer_limit(
        arguments,
        field_name="scan_limit",
        default=1_000,
        maximum=10_000,
    )
    if target is not None and should_execute_on_cluster(target):
        if settings.owner_session_id is not None:
            with OwnedSessionApiClient(definition=target, settings=settings) as client:
                payload = _owned_json(
                    client,
                    method="GET",
                    path=f"/queue/jobs/{job_id}/diagnose",
                    query={
                        "cluster": target.name,
                        "older_than_seconds": older_than_seconds,
                        "scan_limit": scan_limit,
                    },
                    label="owned remote queue diagnosis",
                )
            _validate_owned_job_status(payload, job_id=job_id, cluster=target.name)
            return _queue_route_result(payload, target=target, remote=True)
        return _queue_route_result(
            _remote_json(
                target,
                [
                    "queue",
                    "diagnose",
                    job_id,
                    "--cluster",
                    target.name,
                    "--older-than",
                    f"{older_than_seconds}s",
                    "--scan-limit",
                    str(scan_limit),
                ],
                "remote queue diagnosis",
            ),
            target=target,
            remote=True,
        )
    result = cast(
        JSON,
        diagnose_job(
            queue,
            job_id,
            cluster=cluster,
            stale_after_seconds=older_than_seconds,
            scan_limit=scan_limit,
        ),
    )
    return _queue_route_result(result, target=target, remote=False)


def _queue_stale_tool(
    arguments: JSON,
    *,
    queue: ClioCoreQueue,
    settings: RelaySettings,
) -> JSON:
    """Discover stale jobs on the selected local or SSH-backed cluster queue."""
    target = _queue_tool_target(arguments)
    cluster = _required_str(arguments, "cluster")
    older_than_seconds = _positive_integer_argument(
        arguments,
        "older_than_seconds",
        required=True,
    )
    job_id = _optional_durable_record_id(arguments, "job_id")
    raw_kind = arguments.get("kind")
    if raw_kind is not None and not isinstance(raw_kind, str):
        raise ValueError("kind must be a string")
    kind = JobKind(raw_kind) if isinstance(raw_kind, str) else None
    limit = _response_page_limit(arguments)
    scan_limit = _bounded_integer_limit(
        arguments,
        field_name="scan_limit",
        default=DEFAULT_STALE_SCAN_LIMIT,
        maximum=10_000,
    )
    if scan_limit < limit:
        raise ValueError("scan_limit must be greater than or equal to limit")
    if target is not None and should_execute_on_cluster(target):
        if settings.owner_session_id is not None:
            raise ValueError(
                "stale discovery is unavailable for an owned relay session because it requires "
                "global queue visibility; diagnose an exact owned job instead"
            )
        command = [
            "queue",
            "stale",
            "--cluster",
            target.name,
            "--older-than",
            f"{older_than_seconds}s",
            "--limit",
            str(limit),
            "--scan-limit",
            str(scan_limit),
        ]
        if job_id is not None:
            command.extend(["--job-id", job_id])
        if kind is not None:
            command.extend(["--kind", kind.value])
        return _queue_route_result(
            _remote_json(target, command, "remote stale queue discovery"),
            target=target,
            remote=True,
        )
    result = cast(
        JSON,
        discover_stale_jobs(
            queue,
            cluster=cluster,
            older_than_seconds=older_than_seconds,
            job_id=job_id,
            kind=kind,
            limit=limit,
            scan_limit=scan_limit,
        ),
    )
    return _queue_route_result(result, target=target, remote=False)


def _queue_cleanup_stale_tool(
    arguments: JSON,
    *,
    queue: ClioCoreQueue,
    settings: RelaySettings,
) -> JSON:
    """Preview or execute stale cleanup on the selected cluster queue route."""
    target = _queue_tool_target(arguments)
    cluster = _required_str(arguments, "cluster")
    older_than_seconds = _positive_integer_argument(
        arguments,
        "older_than_seconds",
        default=7_200,
    )
    max_attempts = _positive_integer_argument(arguments, "max_attempts", default=3)
    dry_run = _boolean_argument(arguments, "dry_run", default=True)
    cancel_queued = _boolean_argument(arguments, "cancel_queued", default=False)
    job_id = _optional_durable_record_id(arguments, "job_id")
    raw_kind = arguments.get("kind")
    if raw_kind is not None and not isinstance(raw_kind, str):
        raise ValueError("kind must be a string")
    kind = JobKind(raw_kind) if isinstance(raw_kind, str) else None
    limit = _response_page_limit(arguments)
    scan_limit = _bounded_integer_limit(
        arguments,
        field_name="scan_limit",
        default=DEFAULT_STALE_SCAN_LIMIT,
        maximum=10_000,
    )
    if scan_limit < limit:
        raise ValueError("scan_limit must be greater than or equal to limit")
    if target is not None and should_execute_on_cluster(target):
        if settings.owner_session_id is not None:
            raise ValueError(
                "stale cleanup is unavailable for an owned relay session because it requires "
                "global queue mutation authority"
            )
        command = [
            "queue",
            "cleanup-stale",
            "--cluster",
            target.name,
            "--older-than",
            f"{older_than_seconds}s",
            "--max-attempts",
            str(max_attempts),
            "--limit",
            str(limit),
            "--scan-limit",
            str(scan_limit),
            "--dry-run" if dry_run else "--no-dry-run",
        ]
        if job_id is not None:
            command.extend(["--job-id", job_id])
        if kind is not None:
            command.extend(["--kind", kind.value])
        if cancel_queued:
            command.append("--cancel-queued")
        return _queue_route_result(
            _remote_json(target, command, "remote stale queue cleanup"),
            target=target,
            remote=True,
        )
    result = cast(
        JSON,
        cleanup_stale_jobs(
            queue,
            cluster=cluster,
            older_than_seconds=older_than_seconds,
            job_id=job_id,
            kind=kind,
            max_attempts=max_attempts,
            dry_run=dry_run,
            cancel_queued=cancel_queued,
            limit=limit,
            scan_limit=scan_limit,
        ),
    )
    return _queue_route_result(result, target=target, remote=False)


def _worker_status_tool(arguments: JSON, *, queue: ClioCoreQueue) -> JSON:
    """Read worker capacity from the selected local or SSH-backed queue route."""
    target = _queue_tool_target(arguments)
    cluster = _optional_str(arguments, "cluster")
    if target is not None and should_execute_on_cluster(target):
        return _queue_route_result(
            _remote_json(
                target,
                ["worker", "status", "--cluster", target.name],
                "remote worker status",
            ),
            target=target,
            remote=True,
        )
    result = cast(JSON, worker_status(queue, cluster=cluster))
    return _queue_route_result(result, target=target, remote=False)


def _queue_tool_target(arguments: JSON) -> ClusterDefinition | None:
    """Resolve an optional cluster route while preserving unregistered local queues."""
    raw_cluster = arguments.get("cluster")
    raw_revision = arguments.get("route_revision")
    if raw_cluster is None:
        if raw_revision is not None:
            raise ValueError("route_revision requires cluster")
        return None
    if not isinstance(raw_cluster, str) or not raw_cluster:
        raise ValueError("cluster must be a non-empty string")
    if raw_revision is not None:
        _validated_route_revision(raw_revision)
    registry_path = default_registry_path()
    if not registry_path.exists():
        if raw_revision is not None:
            raise ValueError(f"cluster route is not configured: {raw_cluster}")
        return None
    definition = ClusterRegistry.load(registry_path).clusters.get(raw_cluster)
    if definition is None:
        if raw_revision is not None:
            raise ValueError(f"cluster route is not configured: {raw_cluster}")
        return None
    expected_revision = _route_revision(definition)
    if raw_revision is not None and not hmac.compare_digest(raw_revision, expected_revision):
        raise ValueError(
            f"cluster route changed for {raw_cluster}; refuse to use stale queue routing"
        )
    return definition


def _queue_route_result(
    result: JSON,
    *,
    target: ClusterDefinition | None,
    remote: bool,
) -> JSON:
    """Attach stable route identity to queue results when a target is configured."""
    if target is None:
        return result
    result["cluster"] = target.name
    result["route_revision"] = _route_revision(target)
    result["remote"] = remote
    return result


def _positive_integer_argument(
    arguments: JSON,
    field_name: str,
    *,
    default: int | None = None,
    required: bool = False,
) -> int:
    """Read one positive integer without treating booleans as integers."""
    if field_name not in arguments:
        if required or default is None:
            raise ValueError(f"{field_name} is required")
        return default
    value = arguments[field_name]
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{field_name} must be a positive integer")
    return value


def _boolean_argument(arguments: JSON, field_name: str, *, default: bool) -> bool:
    """Read one strict boolean argument."""
    value = arguments.get(field_name, default)
    if not isinstance(value, bool):
        raise ValueError(f"{field_name} must be a boolean")
    return value


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


def _owned_json(
    client: OwnedSessionApiClient,
    *,
    method: str,
    path: str,
    label: str,
    query: dict[str, object] | None = None,
    body: dict[str, object] | None = None,
    response_timeout_seconds: float | None = None,
) -> JSON:
    """Read one object from an exact-generation, identity-proven session API."""
    if response_timeout_seconds is None:
        value = client.request_json(method=method, path=path, query=query, body=body)
    else:
        value = client.request_json(
            method=method,
            path=path,
            query=query,
            body=body,
            response_timeout_seconds=response_timeout_seconds,
        )
    if not isinstance(value, dict):
        raise ValueError(f"{label} must return a JSON object")
    return cast(JSON, value)


def _validate_owned_job_status(payload: JSON, *, job_id: str, cluster: str) -> None:
    raw_job = payload.get("job")
    if not isinstance(raw_job, dict):
        raise ValueError("owned session job response is missing its durable job record")
    job = cast(JSON, raw_job)
    if job.get("job_id") != job_id or job.get("cluster") != cluster:
        raise ValueError("owned session job response does not match the requested handle")


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


def _complete_owned_collection(
    client: OwnedSessionApiClient,
    *,
    path: str,
    record_key: str,
    label: str,
) -> list[JSON]:
    """Drain an owned HTTP collection on one already identity-proven connection."""
    cursor = 1
    expected_total: int | None = None
    records: list[JSON] = []
    while True:
        payload = _owned_json(
            client,
            method="GET",
            path=path,
            query={"cursor": cursor, "limit": MAX_RESPONSE_PAGE_RECORDS},
            label=label,
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


def _owned_job_logs(
    client: OwnedSessionApiClient,
    job_id: str,
    *,
    limit: int,
) -> JSON:
    return {
        stream: _owned_json(
            client,
            method="GET",
            path=f"/jobs/{job_id}/logs/{stream}",
            query={"offset": 0, "limit": limit},
            label=f"owned remote {stream} log",
        )
        for stream in ("stdout", "stderr")
    }


def _verified_mcp_result(
    definition: ClusterDefinition,
    job_id: str,
    artifacts: list[JSON],
) -> _VerifiedMcpResult | None:
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


def _verified_owned_mcp_result(
    client: OwnedSessionApiClient,
    job_id: str,
    artifacts: list[JSON],
) -> _VerifiedMcpResult | None:
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
        raise ValueError("owned remote MCP result artifact has no artifact_id")
    envelope = _owned_json(
        client,
        method="GET",
        path=f"/artifacts/{artifact_id}/content",
        label="owned remote MCP result artifact",
    )
    return _decode_verified_mcp_result(envelope, artifact=artifact, job_id=job_id)


def _verified_local_mcp_result(
    queue: ClioCoreQueue,
    job_id: str,
    *,
    artifacts: list[JSON] | None = None,
) -> _VerifiedMcpResult | None:
    artifact_records = (
        artifacts if artifacts is not None else _complete_local_artifacts(queue, job_id)
    )
    artifact = next(
        (
            item
            for item in artifact_records
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


def _decode_verified_mcp_result(
    envelope: JSON,
    *,
    artifact: JSON,
    job_id: str,
) -> _VerifiedMcpResult:
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
    public = {
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
            "result_validation",
        )
    }
    return _VerifiedMcpResult(document=typed, public=public)


def _mcp_result_artifact(artifacts: list[JSON], *, job_id: str) -> JSON | None:
    """Return the unique durable MCP-result artifact for one job, if present."""

    matches = [
        artifact
        for artifact in artifacts
        if artifact.get("job_id") == job_id and artifact.get("kind") == "mcp_result"
    ]
    if len(matches) > 1:
        raise ValueError(f"job {job_id} has multiple MCP result artifacts")
    return matches[0] if matches else None


def _bounded_mcp_result(result: JSON) -> JSON:
    """Return a bounded agent projection while the artifact retains full protocol evidence."""

    original = copy.deepcopy(result)
    sanitized = redact_sensitive_values(original)
    if not isinstance(sanitized, dict):
        raise ValueError("MCP result redaction did not preserve its object shape")
    projected = cast(JSON, sanitized)
    sensitive_values_redacted = projected != result
    if projected.get("structured_result") is not None and "protocol_result" in projected:
        projected.pop("protocol_result")
        projected["protocol_result_omitted"] = "redundant_with_structured_result"
    if sensitive_values_redacted:
        projected["sensitive_values_redacted"] = True

    encoded = json.dumps(
        projected,
        allow_nan=False,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    if len(encoded) <= MAX_INLINE_MCP_RESULT_BYTES:
        return projected

    # Arbitrary MCP output has no generic, safe pagination or redaction contract.
    # Returning a successful-looking partial projection would lose the only
    # agent-readable result, while exposing selected fields could disclose
    # application-defined secrets. Keep the full artifact private and fail the
    # MCP delivery explicitly without changing the immutable remote job state.
    failure: JSON = {
        "content_truncated": True,
        "result_available": False,
        "delivery": {
            "schema_version": MCP_RESULT_DELIVERY_SCHEMA,
            "status": "failed",
            "code": MCP_RESULT_INLINE_LIMIT_CODE,
            "max_inline_bytes": MAX_INLINE_MCP_RESULT_BYTES,
            "private_evidence_preserved": True,
            "remote_side_effects_may_have_occurred": True,
            "message": MCP_RESULT_INLINE_LIMIT_MESSAGE,
        },
    }
    if sensitive_values_redacted:
        failure["sensitive_values_redacted"] = True
    return failure


def _mcp_result_delivery_failed(result: JSON) -> bool:
    """Return whether a terminal MCP result could not be delivered to the agent."""

    mcp_result = result.get("mcp_result")
    if not isinstance(mcp_result, dict):
        return False
    delivery = cast(dict[str, object], mcp_result).get("delivery")
    if not isinstance(delivery, dict):
        return False
    typed_delivery = cast(dict[str, object], delivery)
    return (
        typed_delivery.get("schema_version") == MCP_RESULT_DELIVERY_SCHEMA
        and typed_delivery.get("status") == "failed"
        and typed_delivery.get("code") == MCP_RESULT_INLINE_LIMIT_CODE
    )


def _read_model_artifact_bytes(queue: ClioCoreQueue, artifact_id: str) -> dict[str, object]:
    """Read one public artifact without exposing internal protocol capabilities."""

    artifact = queue.get_artifact(artifact_id)
    if artifact.kind == "mcp_result" or artifact.metadata.get("model_readable") is False:
        raise ValueError(
            "artifact is internal protocol evidence and is not model-readable; use relay_wait "
            "for its bounded public result"
        )
    return read_artifact_bytes(queue, artifact_id)


def _public_mcp_result_artifact(artifact: JSON) -> JSON:
    """Return the compact immutable binding for a durable MCP result artifact."""

    return {
        key: artifact.get(key)
        for key in (
            "artifact_id",
            "job_id",
            "kind",
            "size_bytes",
            "sha256",
            "created_at",
        )
    }


def _attach_terminal_mcp_evidence(
    receipt: JSON,
    *,
    source_job: RelayJob,
    last_error: str | None,
    artifacts: list[JSON],
    parsed_result: _VerifiedMcpResult | None,
) -> None:
    """Attach bounded terminal MCP evidence to a waited submission receipt."""

    receipt["last_error"] = last_error
    if parsed_result is None:
        return
    artifact = _mcp_result_artifact(artifacts, job_id=source_job.job_id)
    if artifact is None:
        raise ValueError(f"verified MCP result for {source_job.job_id} has no durable artifact")
    receipt["mcp_result_artifact"] = _public_mcp_result_artifact(artifact)
    if (
        source_job.state is JobState.SUCCEEDED
        and isinstance(source_job.spec, McpCallSpec)
        and source_job.spec.tool == "jarvis_get_execution"
        and source_job.spec.arguments.get("include_service_runtimes") is True
    ):
        source_artifact = ArtifactRef.model_validate(artifact)
        receipt["service_runtime_bindings"] = [
            handoff.model_dump(mode="json")
            for handoff in derive_jarvis_service_runtime_handoffs(
                cluster=source_job.cluster,
                source_job=source_job,
                source_artifact=source_artifact,
                document=parsed_result.document,
            )
        ]
    receipt["mcp_result"] = _bounded_mcp_result(parsed_result.public)


def _render_remote_mcp_context(catalog: VirtualRemoteMcpCatalog) -> str:
    jarvis_clusters = _bound_virtual_jarvis_clusters(catalog)
    jarvis = (
        render_virtual_jarvis_agent_context()
        + " Built-in virtual JARVIS tools are available on: "
        + ", ".join(jarvis_clusters)
        + "."
        if jarvis_clusters
        else (
            "Built-in virtual JARVIS tools are not advertised because no configured cluster "
            "has a verified JARVIS MCP artifact binding. Use an available registered remote "
            "MCP alias or ask an operator to refresh the built-in JARVIS discovery."
        )
    )
    generic = (
        " Registered remote MCP tools are exposed with remote_<server>_<tool> aliases; "
        "their cluster argument selects the execution target and is not forwarded to the "
        "remote tool. Operators explicitly refresh the durable schema cache before new or "
        "changed tools appear. Treat cluster, job_id, and the opaque 64-character "
        "route_revision returned by one submission as an indivisible handle. A route "
        "revision is never interchangeable with this tool catalog's revision or a "
        "scientific dataset's catalog revision."
    )
    available = ""
    if catalog.tools:
        available = " Available registered aliases: " + ", ".join(sorted(catalog.tools)) + "."
    return jarvis + generic + available


def _cancel_job(
    arguments: JSON,
    *,
    queue: ClioCoreQueue,
    settings: RelaySettings,
) -> JSON:
    target = _job_target(arguments)
    if target is not None and should_execute_on_cluster(target):
        job_id = _required_durable_record_id(arguments, "job_id")
        cancel_scheduler = arguments.get("cancel_scheduler_job") is True
        if settings.owner_session_id is not None:
            with OwnedSessionApiClient(definition=target, settings=settings) as client:
                result = _owned_json(
                    client,
                    method="POST",
                    path=f"/queue/jobs/{job_id}/cancel",
                    body={
                        "cluster": target.name,
                        "cancel_scheduler_job": cancel_scheduler,
                    },
                    label="owned remote job cancellation",
                )
            _validate_owned_job_status(result, job_id=job_id, cluster=target.name)
            job = _object(result["job"])
            result["job_id"] = job["job_id"]
            result["state"] = job["state"]
            result["cluster"] = target.name
            result["route_revision"] = _route_revision(target)
            return result
        command = ["job", "cancel", job_id]
        if cancel_scheduler:
            command.append("--cancel-scheduler-job")
        run_remote_clio(target, command)
        result = _remote_json(target, ["job", "status", job_id], "remote job status")
        result["cancel_requested"] = True
        result["scheduler_policy"] = "request-scheduler" if cancel_scheduler else "relay-only"
        result["cluster"] = target.name
        result["route_revision"] = _route_revision(target)
        return result
    _require_local_job_cluster(
        queue,
        _required_durable_record_id(arguments, "job_id"),
        target,
    )
    result = cancel_queue_job(
        queue,
        _required_durable_record_id(arguments, "job_id"),
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
    job_id = _required_durable_record_id(arguments, "job_id")
    cursor = int(arguments.get("cursor", 1))
    limit = _response_page_limit(arguments)
    target = _job_target(arguments)
    owned_logs: JSON | None = None
    if target is not None and should_execute_on_cluster(target):
        if settings.owner_session_id is not None:
            with OwnedSessionApiClient(definition=target, settings=settings) as client:
                observed = _owned_json(
                    client,
                    method="GET",
                    path=f"/jobs/{job_id}/monitor",
                    query={"cursor": cursor, "limit": limit},
                    label="owned remote job monitor",
                )
                _validate_owned_job_status(observed, job_id=job_id, cluster=target.name)
                if arguments.get("include_logs", True) is not False:
                    owned_logs = _owned_job_logs(
                        client,
                        job_id,
                        limit=_log_limit(arguments),
                    )
        else:
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
    matches_truncated = False
    logs: JSON | None = None
    if arguments.get("include_logs", True) is not False:
        log_limit = _log_limit(arguments)
        if target is not None and should_execute_on_cluster(target):
            if settings.owner_session_id is not None:
                if owned_logs is None:
                    raise ValueError("owned remote log retrieval did not complete")
                logs = owned_logs
            else:
                logs = _remote_job_logs(target, job_id, limit=log_limit)
        else:
            logs = _job_logs(queue, settings, job_id, limit=log_limit)
    if pattern is not None:
        compiled = re.compile(pattern)
        for event in cast(list[JSON], observed.get("events", [])):
            for text in _event_match_candidates(event):
                matches_truncated = _append_bounded_observe_matches(
                    matches,
                    compiled=compiled,
                    text=text,
                    identity={
                        "event_seq": event.get("seq"),
                        "event_type": event.get("event_type"),
                    },
                )
                if matches_truncated:
                    break
            if matches_truncated:
                break
        if logs is not None:
            for stream_name in ("stdout", "stderr"):
                if matches_truncated:
                    break
                stream = _object(logs[stream_name])
                text = stream.get("text")
                if not isinstance(text, str):
                    continue
                matches_truncated = _append_bounded_observe_matches(
                    matches,
                    compiled=compiled,
                    text=text,
                    identity={"source": stream_name},
                )
    result: JSON = {
        **observed,
        "matched": bool(matches),
        "matches": matches,
        "matches_truncated": matches_truncated,
    }
    if logs is not None:
        result["logs"] = logs
    if target is not None:
        result["cluster"] = target.name
        result["route_revision"] = _route_revision(target)
    return result


def _wait_job(arguments: JSON, *, queue: ClioCoreQueue, settings: RelaySettings) -> JSON:
    job_id = _required_durable_record_id(arguments, "job_id")
    target = _job_target(arguments)
    timeout_seconds = _observation_timeout_seconds(arguments, "timeout_seconds")
    poll_seconds = _observation_timeout_seconds(arguments, "poll_seconds", default=2.0)
    logs: JSON | None = None
    if target is not None and should_execute_on_cluster(target):
        if settings.owner_session_id is not None:
            try:
                with OwnedSessionApiClient(definition=target, settings=settings) as client:
                    waited = _owned_json(
                        client,
                        method="POST",
                        path=f"/jobs/{job_id}/wait",
                        query={
                            "timeout_seconds": timeout_seconds,
                            "poll_seconds": poll_seconds,
                        },
                        label="owned remote job wait",
                        response_timeout_seconds=(
                            timeout_seconds + OWNED_SESSION_WAIT_RESPONSE_GRACE_SECONDS
                        ),
                    )
                    if waited.get("job_id") != job_id or waited.get("cluster") != target.name:
                        raise ValueError("owned remote wait returned a different job")
            except ObservationTimeoutError:
                pass
            with OwnedSessionApiClient(definition=target, settings=settings) as client:
                result = _owned_json(
                    client,
                    method="GET",
                    path=f"/jobs/{job_id}/status",
                    label="owned remote job status",
                )
                _validate_owned_job_status(result, job_id=job_id, cluster=target.name)
                source_job, observation_unknown = _observed_remote_wait_job(
                    result,
                    job_id=job_id,
                    cluster=target.name,
                )
                if observation_unknown:
                    _attach_wait_observation(
                        result,
                        observation_unknown=True,
                        timeout_seconds=timeout_seconds,
                    )
                    result["cluster"] = target.name
                    result["route_revision"] = _route_revision(target)
                    return result
                if arguments.get("include_logs", False) is True:
                    logs = _owned_job_logs(
                        client,
                        job_id,
                        limit=_log_limit(arguments),
                    )
                artifact_records = _complete_owned_collection(
                    client,
                    path=f"/jobs/{job_id}/artifacts",
                    record_key="artifacts",
                    label=f"owned remote artifacts for {job_id}",
                )
                parsed_result = _verified_owned_mcp_result(client, job_id, artifact_records)
        else:
            try:
                with remote_command_timeout(
                    timeout_seconds + OWNED_SESSION_WAIT_RESPONSE_GRACE_SECONDS
                ):
                    run_remote_clio(
                        target,
                        [
                            "job",
                            "wait",
                            job_id,
                            "--timeout-seconds",
                            str(timeout_seconds),
                            "--poll-seconds",
                            str(poll_seconds),
                        ],
                    )
            except ObservationTimeoutError:
                pass
            with remote_command_timeout(REMOTE_WAIT_STATUS_TIMEOUT_SECONDS):
                result = _remote_json(target, ["job", "status", job_id], "remote job status")
            source_job, observation_unknown = _observed_remote_wait_job(
                result,
                job_id=job_id,
                cluster=target.name,
            )
            if observation_unknown:
                _attach_wait_observation(
                    result,
                    observation_unknown=True,
                    timeout_seconds=timeout_seconds,
                )
                result["cluster"] = target.name
                result["route_revision"] = _route_revision(target)
                return result
            if arguments.get("include_logs", False) is True:
                logs = _remote_job_logs(
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
            parsed_result = _verified_mcp_result(target, job_id, artifact_records)
    else:
        _require_local_job_cluster(queue, job_id, target)
        with suppress(TimeoutError):
            wait_for_terminal(
                queue,
                job_id,
                timeout_seconds=timeout_seconds,
                poll_seconds=poll_seconds,
            )
        result = job_status(queue, job_id)
        source_job = RelayJob.model_validate(_object(result.get("job")))
        if source_job.job_id != job_id:
            raise ValueError("local wait status returned a different job")
        if source_job.state not in TERMINAL_STATES:
            _attach_wait_observation(
                result,
                observation_unknown=True,
                timeout_seconds=timeout_seconds,
            )
            return result
        if arguments.get("include_logs", False) is True:
            logs = _job_logs(
                queue,
                settings,
                source_job.job_id,
                limit=_log_limit(arguments),
            )
        artifact_records = _complete_local_artifacts(queue, source_job.job_id)
        parsed_result = _verified_local_mcp_result(
            queue,
            source_job.job_id,
            artifacts=artifact_records,
        )

    if target is not None:
        result["cluster"] = target.name
        result["route_revision"] = _route_revision(target)
    _attach_wait_observation(
        result,
        observation_unknown=False,
        timeout_seconds=timeout_seconds,
    )
    if parsed_result is not None:
        _attach_terminal_mcp_evidence(
            result,
            source_job=source_job,
            last_error=source_job.last_error,
            artifacts=artifact_records,
            parsed_result=parsed_result,
        )
    if logs is not None:
        result["logs"] = logs
    result["artifacts"] = artifact_records
    return result


def _observed_remote_wait_job(
    result: JSON,
    *,
    job_id: str,
    cluster: str,
) -> tuple[RelayJob, bool]:
    """Validate an exact remote job after one bounded terminal observation."""

    source_job = RelayJob.model_validate(_object(result.get("job")))
    if source_job.job_id != job_id or source_job.cluster != cluster:
        raise ValueError("remote wait returned a different job")
    terminal = source_job.state in TERMINAL_STATES
    if result.get("terminal") is not terminal:
        raise ValueError("remote wait status disagrees with its durable job state")
    return source_job, not terminal


def _relay_job_from_wait_document(document: JSON) -> RelayJob:
    """Validate current and legacy HTTP wait documents without discarding the outcome."""
    if "observation" not in document:
        return RelayJob.model_validate(document)
    result = JobWaitResult.model_validate(document)
    terminal = result.state in TERMINAL_STATES
    expected_outcome = "terminal" if terminal else "observation_unknown"
    if result.observation.outcome != expected_outcome:
        raise ValueError("remote wait observation disagrees with its durable job state")
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


def _bounded_observe_value(value: str | None) -> str | None:
    """Bound one regex-derived value before returning it to an agent."""

    if value is None or len(value) <= MAX_OBSERVE_MATCH_TEXT_CHARS:
        return value
    return value[:MAX_OBSERVE_MATCH_TEXT_CHARS]


def _append_bounded_observe_matches(
    matches: list[JSON],
    *,
    compiled: re.Pattern[str],
    text: str,
    identity: JSON,
) -> bool:
    """Append bounded regex matches and report whether more matches were omitted."""

    for match in compiled.finditer(text):
        if len(matches) >= MAX_OBSERVE_MATCHES:
            return True
        start, end = match.span()
        context_start = max(0, start - MAX_OBSERVE_MATCH_TEXT_CHARS // 4)
        context_end = min(len(text), context_start + MAX_OBSERVE_MATCH_TEXT_CHARS)
        if context_end - context_start < MAX_OBSERVE_MATCH_TEXT_CHARS:
            context_start = max(0, context_end - MAX_OBSERVE_MATCH_TEXT_CHARS)
        raw_match = match.group(0)
        groups = [_bounded_observe_value(value) for value in match.groups()]
        groupdict = {key: _bounded_observe_value(value) for key, value in match.groupdict().items()}
        matches.append(
            {
                **identity,
                "text": text[context_start:context_end],
                "text_start": context_start,
                "text_truncated": context_start != 0 or context_end != len(text),
                "match": _bounded_observe_value(raw_match),
                "match_start": start,
                "match_end": end,
                "match_truncated": len(raw_match) > MAX_OBSERVE_MATCH_TEXT_CHARS,
                "groups": groups,
                "groupdict": groupdict,
            }
        )
    return False


def _submit_jarvis_pipeline(
    arguments: JSON,
    *,
    queue: ClioCoreQueue,
    settings: RelaySettings,
) -> JSON:
    cluster = _required_str(arguments, "cluster")
    pipeline_yaml = _required_str(arguments, "pipeline_yaml")
    wait_timeout_seconds = _jarvis_submission_wait_timeout_seconds(arguments)
    used_artifact_refs = _artifact_use_refs(arguments)
    digest = hashlib.sha256(pipeline_yaml.encode("utf-8")).hexdigest()
    dependency_digest = _stable_digest(
        {"used_artifact_refs": [artifact_use_payload(item) for item in used_artifact_refs]}
    )
    dependency_suffix = f":{dependency_digest}" if used_artifact_refs else ""
    idempotency_key = str(
        arguments.get("idempotency_key") or f"mcp:jarvis:{cluster}:{digest}{dependency_suffix}"
    )
    definition = _optional_cluster_definition(cluster)
    if (
        definition is not None
        and should_execute_on_cluster(definition)
        and settings.owner_session_id is not None
    ):
        job = submit_owned_session_job(
            definition=definition,
            settings=settings,
            path="/jobs/jarvis",
            payload={
                "cluster": cluster,
                "pipeline_yaml": pipeline_yaml,
                "idempotency_key": idempotency_key,
                "used_artifact_refs": [artifact_use_payload(item) for item in used_artifact_refs],
            },
        )
        return _owned_session_submission_result(
            job,
            definition=definition,
            settings=settings,
            wait_for_terminal_result=bool(arguments.get("wait_for_terminal", False)),
            wait_timeout_seconds=wait_timeout_seconds,
            poll_seconds=float(arguments.get("poll_seconds", 2)),
        )
    job = _submit_local_job(
        queue,
        RelayJob(
            cluster=cluster,
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml=pipeline_yaml),
            idempotency_key=idempotency_key,
            used_artifact_refs=used_artifact_refs,
        ),
        settings=settings,
    )
    return _submission_result(
        job,
        {**arguments, "wait_timeout_seconds": wait_timeout_seconds},
        queue=queue,
        definition=definition,
    )


def _submit_jarvis_job(
    arguments: JSON,
    *,
    queue: ClioCoreQueue,
    settings: RelaySettings,
) -> JSON:
    cluster = _required_str(arguments, "cluster")
    wait_timeout_seconds = _jarvis_submission_wait_timeout_seconds(arguments)
    used_artifact_refs = _artifact_use_refs(arguments)
    dependency_digest = _stable_digest(
        {"used_artifact_refs": [artifact_use_payload(item) for item in used_artifact_refs]}
    )
    dependency_suffix = f":{dependency_digest}" if used_artifact_refs else ""
    definition = _optional_cluster_definition(cluster)
    if definition is not None and should_execute_on_cluster(definition):
        pipeline_name = _required_str(arguments, "pipeline_name")
        idempotency_key = str(
            arguments.get("idempotency_key")
            or f"mcp:jarvis-job:{cluster}:{pipeline_name}{dependency_suffix}"
        )
        if settings.owner_session_id is not None:
            job = submit_owned_session_job(
                definition=definition,
                settings=settings,
                path="/jobs/jarvis-pipeline",
                payload={
                    "cluster": cluster,
                    "pipeline_name": pipeline_name,
                    "idempotency_key": idempotency_key,
                    "used_artifact_refs": [
                        artifact_use_payload(item) for item in used_artifact_refs
                    ],
                },
            )
            return _owned_session_submission_result(
                job,
                definition=definition,
                settings=settings,
                wait_for_terminal_result=bool(arguments.get("wait_for_terminal", False)),
                wait_timeout_seconds=wait_timeout_seconds,
                poll_seconds=float(arguments.get("poll_seconds", 2)),
            )
        if bool(arguments.get("wait_for_terminal", False)):
            raise ValueError(
                "wait_for_terminal is unavailable for a direct remote JARVIS pipeline "
                "submission without an owned relay session; submit asynchronously, preserve "
                "the remote receipt, and call relay_wait with its cluster, job_id, and "
                "route_revision"
            )
        remote_args = [
            "job",
            "submit-pipeline",
            "--cluster",
            cluster,
            "--pipeline-name",
            pipeline_name,
            "--idempotency-key",
            str(idempotency_key),
        ]
        for item in used_artifact_refs:
            remote_args.extend(["--used-artifact", _artifact_use_cli_value(item)])
        output = run_remote_clio(definition, remote_args)
        return _remote_submission_result(output, kind=JobKind.JARVIS, definition=definition)
    pipeline_name = _required_str(arguments, "pipeline_name")
    idempotency_key = str(
        arguments.get("idempotency_key")
        or f"mcp:jarvis-job:{cluster}:{pipeline_name}{dependency_suffix}"
    )
    job = _submit_local_job(
        queue,
        RelayJob(
            cluster=cluster,
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_name=pipeline_name),
            idempotency_key=idempotency_key,
            used_artifact_refs=used_artifact_refs,
        ),
        settings=settings,
    )
    wait_arguments = {
        **arguments,
        "wait_timeout_seconds": wait_timeout_seconds,
    }
    return _submission_result(job, wait_arguments, queue=queue, definition=definition)


def _submit_remote_agent(
    arguments: JSON,
    *,
    queue: ClioCoreQueue,
    settings: RelaySettings,
) -> JSON:
    cluster = _required_str(arguments, "cluster")
    used_artifact_refs = _artifact_use_refs(arguments)
    prompt_path = _required_str(arguments, "prompt_path")
    mcp_config_path = _optional_str(arguments, "mcp_config_path")
    model = _optional_str(arguments, "model")
    workdir = _optional_str(arguments, "workdir")
    timeout_seconds = _optional_int(arguments, "timeout_seconds")
    identity: dict[str, object] = {
        "cluster": cluster,
        "prompt_path": prompt_path,
        "mcp_config_path": mcp_config_path,
        "model": model,
        "workdir": workdir,
        "timeout_seconds": timeout_seconds,
    }
    if used_artifact_refs:
        identity["used_artifact_refs"] = [artifact_use_payload(item) for item in used_artifact_refs]
    idempotency_key = str(
        arguments.get("idempotency_key") or "mcp:remote-agent:" + _stable_digest(identity)
    )
    definition = _optional_cluster_definition(cluster)
    if (
        definition is not None
        and should_execute_on_cluster(definition)
        and settings.owner_session_id is not None
    ):
        payload: dict[str, object] = {
            "cluster": cluster,
            "prompt_path": prompt_path,
            "idempotency_key": idempotency_key,
            "used_artifact_refs": [artifact_use_payload(item) for item in used_artifact_refs],
        }
        for key, value in {
            "mcp_config_path": mcp_config_path,
            "model": model,
            "workdir": workdir,
            "timeout_seconds": timeout_seconds,
        }.items():
            if value is not None:
                payload[key] = value
        job = submit_owned_session_job(
            definition=definition,
            settings=settings,
            path="/jobs/remote-agent",
            payload=payload,
        )
        return _owned_session_submission_result(
            job,
            definition=definition,
            settings=settings,
            wait_for_terminal_result=bool(arguments.get("wait_for_terminal", False)),
            wait_timeout_seconds=float(arguments.get("wait_timeout_seconds", 600)),
            poll_seconds=float(arguments.get("poll_seconds", 2)),
        )
    job = _submit_local_job(
        queue,
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
            used_artifact_refs=used_artifact_refs,
        ),
        settings=settings,
    )
    return _submission_result(job, arguments, queue=queue)


def _submit_mcp_call(
    arguments: JSON,
    *,
    queue: ClioCoreQueue,
    settings: RelaySettings,
    pinned_jarvis: bool = False,
) -> JSON:
    cluster = _required_str(arguments, "cluster")
    used_artifact_refs = _artifact_use_refs(arguments)
    server = _required_str(arguments, "server")
    server_args = _string_list(arguments.get("server_args", []), "server_args")
    env_from = _string_mapping(arguments.get("env_from", {}), "env_from")
    expected_server_artifact_digest = _optional_str(
        arguments,
        "expected_server_artifact_digest",
    )
    expected_registered_contract = _optional_str(
        arguments,
        "expected_registered_contract",
    )
    raw_jarvis_input_manifest = arguments.get("jarvis_input_manifest")
    try:
        jarvis_input_manifest = (
            JarvisRunInputManifest.model_validate(raw_jarvis_input_manifest)
            if raw_jarvis_input_manifest is not None
            else None
        )
    except ValidationError as exc:
        raise ValueError("invalid JARVIS run input manifest") from exc
    raw_expected_jarvis_cd_lock_binding = arguments.get("expected_jarvis_cd_lock_binding")
    expected_jarvis_cd_lock_binding = (
        _string_mapping(
            raw_expected_jarvis_cd_lock_binding,
            "expected_jarvis_cd_lock_binding",
        )
        if raw_expected_jarvis_cd_lock_binding is not None
        else None
    )
    raw_control_query_evidence = arguments.get("control_query_evidence")
    try:
        control_query_evidence = (
            McpControlQueryEvidence.model_validate(raw_control_query_evidence)
            if raw_control_query_evidence is not None
            else None
        )
    except ValidationError as exc:
        raise ValueError("invalid MCP control-query discovery evidence") from exc
    tool = _required_str(arguments, "tool")
    tool_arguments = _object(arguments.get("arguments", {}))
    timeout_seconds = _optional_int(arguments, "timeout_seconds")
    digest = hashlib.sha256(
        json.dumps(tool_arguments, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    identity: dict[str, object] = {
        "cluster": cluster,
        "server": server,
        "server_args": server_args,
        "env_from": env_from,
        "expected_server_artifact_digest": expected_server_artifact_digest,
        "tool": tool,
        "arguments_digest": digest,
        "timeout_seconds": timeout_seconds,
    }
    if control_query_evidence is not None:
        identity["control_query_evidence"] = control_query_evidence.model_dump(mode="json")
    if expected_registered_contract is not None:
        identity["expected_registered_contract"] = expected_registered_contract
    if jarvis_input_manifest is not None:
        identity["jarvis_input_manifest"] = jarvis_input_manifest.model_dump(mode="json")
    if expected_jarvis_cd_lock_binding is not None:
        identity["expected_jarvis_cd_lock_binding"] = expected_jarvis_cd_lock_binding
    if used_artifact_refs:
        identity["used_artifact_refs"] = [artifact_use_payload(item) for item in used_artifact_refs]
    idempotency_key = str(
        arguments.get("idempotency_key") or "mcp:mcp-call:" + _stable_digest(identity)
    )
    registered_route = arguments.get("registered_route") is True
    registered_remote_mcp_route = arguments.get("registered_remote_mcp_route") is True
    if registered_remote_mcp_route and not registered_route:
        raise ValueError("registered remote MCP route requires a strict cluster route")
    expected_cluster_route_revision = _optional_str(
        arguments,
        "expected_cluster_route_revision",
    )
    registered_server_name = _optional_str(arguments, "registered_server_name")
    expected_registration_revision = _optional_str(
        arguments,
        "expected_remote_mcp_registration_revision",
    )
    definition = (
        _remote_cluster_definition(cluster)
        if registered_route
        else _optional_cluster_definition(cluster)
    )
    if definition is not None and expected_cluster_route_revision is not None:
        observed_cluster_route_revision = _route_revision(definition)
        if not hmac.compare_digest(
            observed_cluster_route_revision,
            expected_cluster_route_revision,
        ):
            raise ValueError(
                f"cluster route changed for {cluster}; call tools/list again before submission"
            )
    if registered_remote_mcp_route:
        if registered_server_name is None or expected_registration_revision is None:
            raise ValueError("registered remote MCP route is missing its revision binding")
        if definition is None:
            raise ValueError(f"cluster is not configured: {cluster}")
        current_registration = definition.remote_mcp_servers.get(registered_server_name)
        if current_registration is None:
            raise ValueError(
                f"remote MCP registration changed for {cluster}/{registered_server_name}; "
                "call tools/list again before submission"
            )
        current_registration_revision = remote_mcp_registration_revision(current_registration)
        if not hmac.compare_digest(
            current_registration_revision,
            expected_registration_revision,
        ):
            raise ValueError(
                f"remote MCP registration changed for {cluster}/{registered_server_name}; "
                "call tools/list again before submission"
            )
        if expected_registered_contract is not None and (
            current_registration.contract != expected_registered_contract
        ):
            raise ValueError("registered MCP semantic contract changed after discovery")
    elif expected_registered_contract is not None:
        raise ValueError("registered MCP semantic contract requires a registered remote route")
    if control_query_evidence is not None:
        if not registered_remote_mcp_route:
            raise ValueError("MCP control-query evidence requires a registered remote route")
        if (
            control_query_evidence.cluster != cluster
            or control_query_evidence.registered_server_name != registered_server_name
            or control_query_evidence.cluster_route_revision != expected_cluster_route_revision
            or control_query_evidence.registration_revision != expected_registration_revision
            or control_query_evidence.expected_server_artifact_digest
            != expected_server_artifact_digest
        ):
            raise ValueError("MCP control-query evidence does not match its selected route")
    if definition is not None and should_execute_on_cluster(definition):
        if settings.owner_session_id is not None:
            payload: dict[str, object] = {
                "cluster": cluster,
                "server": server,
                "server_args": server_args,
                "env_from": env_from,
                "operation": McpOperation.TOOLS_CALL.value,
                "tool": tool,
                "arguments": tool_arguments,
                "idempotency_key": idempotency_key,
                "used_artifact_refs": [artifact_use_payload(item) for item in used_artifact_refs],
            }
            if control_query_evidence is not None:
                payload["control_query_evidence"] = control_query_evidence.model_dump(mode="json")
            if timeout_seconds is not None:
                payload["timeout_seconds"] = timeout_seconds
            if expected_server_artifact_digest is not None:
                payload["expected_server_artifact_digest"] = expected_server_artifact_digest
            if expected_registered_contract is not None:
                payload["expected_registered_contract"] = expected_registered_contract
            if jarvis_input_manifest is not None:
                payload["jarvis_input_manifest"] = jarvis_input_manifest.model_dump(mode="json")
            job = submit_owned_session_job(
                definition=definition,
                settings=settings,
                path="/jobs/mcp-call",
                payload=payload,
            )
            return _owned_session_submission_result(
                job,
                definition=definition,
                settings=settings,
                wait_for_terminal_result=bool(arguments.get("wait_for_terminal", False)),
                wait_timeout_seconds=float(arguments.get("wait_timeout_seconds", 600)),
                poll_seconds=float(arguments.get("poll_seconds", 2)),
                include_terminal_mcp_result=True,
                include_terminal_logs=bool(arguments.get("include_logs", False)),
                terminal_log_limit=_log_limit(arguments),
            )
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
        if control_query_evidence is not None:
            remote_args.extend(
                [
                    "--control-query-evidence-json",
                    json.dumps(
                        control_query_evidence.model_dump(mode="json"),
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                ]
            )
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
        if expected_registered_contract is not None:
            remote_args.extend(["--expected-registered-contract", expected_registered_contract])
        for item in used_artifact_refs:
            remote_args.extend(["--used-artifact", _artifact_use_cli_value(item)])
        with staged_remote_cluster_registry(definition) as remote_registry_path:
            try:
                write_remote_file(
                    definition,
                    remote_args_path,
                    json.dumps(tool_arguments, sort_keys=True, separators=(",", ":")).encode(
                        "utf-8"
                    ),
                )
                output = run_remote_clio(
                    definition,
                    remote_args,
                    cluster_registry_path=remote_registry_path,
                )
            finally:
                remove_remote_file(
                    definition,
                    remote_args_path,
                    remove_empty_parent=True,
                )
        return _remote_mcp_submission_result(
            output,
            definition=definition,
            arguments=arguments,
        )
    operation = McpOperation.TOOLS_CALL
    if pinned_jarvis:
        admission_class, admission_authority = resolve_pinned_mcp_admission(
            operation=operation,
            tool=tool,
            expected_server_artifact_digest=expected_server_artifact_digest,
            pinned_control_query=is_virtual_jarvis_control_query(tool),
            timeout_seconds=timeout_seconds,
        )
    elif control_query_evidence is not None:
        if definition is None:
            raise ValueError("registered MCP control-query admission requires a cluster route")
        admission_class, admission_authority = resolve_registered_remote_mcp_admission(
            queue=queue,
            definition=definition,
            cluster=cluster,
            server=server,
            server_args=server_args,
            env_from=env_from,
            operation=operation,
            tool=tool,
            expected_server_artifact_digest=expected_server_artifact_digest,
            evidence=control_query_evidence,
            expected_registered_contract=expected_registered_contract,
            timeout_seconds=timeout_seconds,
        )
    else:
        admission_class = McpAdmissionClass.WORKLOAD
        admission_authority = None
    metadata = (
        {}
        if admission_authority is None
        else {MCP_ADMISSION_AUTHORITY_METADATA_KEY: admission_authority.model_dump(mode="json")}
    )
    job = _submit_local_job(
        queue,
        RelayJob(
            cluster=cluster,
            kind=JobKind.MCP_CALL,
            spec=McpCallSpec(
                server=server,
                server_args=server_args,
                env_from=env_from,
                expected_server_artifact_digest=expected_server_artifact_digest,
                expected_registered_contract=expected_registered_contract,
                expected_jarvis_cd_lock_binding=expected_jarvis_cd_lock_binding,
                admission_class=admission_class,
                operation=operation,
                tool=tool,
                arguments=tool_arguments,
                jarvis_input_manifest=jarvis_input_manifest,
                timeout_seconds=timeout_seconds,
            ),
            idempotency_key=idempotency_key,
            used_artifact_refs=used_artifact_refs,
            metadata=metadata,
        ),
        settings=settings,
    )
    return _submission_result(
        job,
        arguments,
        queue=queue,
        settings=settings,
        definition=definition,
        include_terminal_mcp_result=True,
    )


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


def _remote_mcp_submission_result(
    output: str,
    *,
    definition: ClusterDefinition,
    arguments: JSON,
) -> JSON:
    """Return a remote MCP receipt and bounded result when the caller waited."""

    result = _remote_submission_result(output, kind=JobKind.MCP_CALL, definition=definition)
    if not bool(arguments.get("wait_for_terminal", False)):
        return result
    job_id = _required_durable_record_id(result, "job_id")
    wait_timeout_seconds = _observation_timeout_seconds(
        arguments,
        "wait_timeout_seconds",
    )
    try:
        with remote_command_timeout(
            wait_timeout_seconds + OWNED_SESSION_WAIT_RESPONSE_GRACE_SECONDS
        ):
            run_remote_clio(
                definition,
                [
                    "job",
                    "wait",
                    job_id,
                    "--timeout-seconds",
                    str(wait_timeout_seconds),
                    "--poll-seconds",
                    str(
                        _observation_timeout_seconds(
                            arguments,
                            "poll_seconds",
                            default=2.0,
                        )
                    ),
                ],
            )
    except ObservationTimeoutError:
        pass
    with remote_command_timeout(REMOTE_WAIT_STATUS_TIMEOUT_SECONDS):
        status = _remote_json(definition, ["job", "status", job_id], "remote job status")
    job = _object(status.get("job"))
    if job.get("job_id") != job_id or job.get("cluster") != definition.name:
        raise ValueError("remote MCP wait returned a different job")
    source_job = RelayJob.model_validate(job)
    state = source_job.state.value
    terminal = source_job.state in TERMINAL_STATES
    if status.get("terminal") is not terminal:
        raise ValueError("remote MCP wait status disagrees with its durable job state")
    result.update({"state": state, "terminal": terminal})
    _attach_wait_observation(
        result,
        observation_unknown=not terminal,
        timeout_seconds=wait_timeout_seconds,
    )
    if not terminal:
        return result
    artifacts = _complete_remote_collection(
        definition,
        ["job", "list-artifacts", job_id],
        record_key="artifacts",
        label=f"remote artifacts for {job_id}",
    )
    parsed_result = _verified_mcp_result(definition, job_id, artifacts)
    logs: JSON | None = None
    if arguments.get("include_logs", False) is True:
        logs = _remote_job_logs(
            definition,
            job_id,
            limit=_log_limit(arguments),
        )
    last_error = job.get("last_error")
    if last_error is not None and not isinstance(last_error, str):
        raise ValueError("remote MCP job returned an invalid last_error")
    _attach_terminal_mcp_evidence(
        result,
        source_job=source_job,
        last_error=last_error,
        artifacts=artifacts,
        parsed_result=parsed_result,
    )
    if logs is not None:
        result["logs"] = logs
    return result


def _owned_session_submission_result(
    job: RelayJob,
    *,
    definition: ClusterDefinition,
    settings: RelaySettings,
    wait_for_terminal_result: bool,
    wait_timeout_seconds: float,
    poll_seconds: float,
    include_terminal_mcp_result: bool = False,
    include_terminal_logs: bool = False,
    terminal_log_limit: int = MAX_AGENT_LOG_READ_BYTES,
) -> JSON:
    """Return an owned receipt, optionally waiting through the same protected API."""
    artifacts: list[JSON] = []
    parsed_result: _VerifiedMcpResult | None = None
    logs: JSON | None = None
    observation_unknown = False
    if wait_for_terminal_result:
        try:
            with OwnedSessionApiClient(definition=definition, settings=settings) as client:
                document = _owned_json(
                    client,
                    method="POST",
                    path=f"/jobs/{job.job_id}/wait",
                    query={
                        "timeout_seconds": wait_timeout_seconds,
                        "poll_seconds": poll_seconds,
                    },
                    label="owned remote submitted job wait",
                    response_timeout_seconds=(
                        wait_timeout_seconds + OWNED_SESSION_WAIT_RESPONSE_GRACE_SECONDS
                    ),
                )
                waited = _relay_job_from_wait_document(document)
        except ObservationTimeoutError:
            with OwnedSessionApiClient(definition=definition, settings=settings) as client:
                status = _owned_json(
                    client,
                    method="GET",
                    path=f"/jobs/{job.job_id}/status",
                    label="owned remote submitted job status after bounded wait",
                )
                _validate_owned_job_status(
                    status,
                    job_id=job.job_id,
                    cluster=definition.name,
                )
                waited = RelayJob.model_validate(_object(status.get("job")))
        if (
            waited.job_id != job.job_id
            or waited.cluster != definition.name
            or waited.metadata.get("owner_session_id") != settings.owner_session_id
            or waited.metadata.get("owner_session_generation_id")
            != settings.owner_session_generation_id
        ):
            raise ValueError("owned remote wait returned a different submission receipt")
        observation_unknown = waited.state not in TERMINAL_STATES
        job = waited
        if not observation_unknown and (include_terminal_mcp_result or include_terminal_logs):
            with OwnedSessionApiClient(definition=definition, settings=settings) as client:
                if include_terminal_mcp_result:
                    artifacts = _complete_owned_collection(
                        client,
                        path=f"/jobs/{job.job_id}/artifacts",
                        record_key="artifacts",
                        label=f"owned remote artifacts for {job.job_id}",
                    )
                    parsed_result = _verified_owned_mcp_result(client, job.job_id, artifacts)
                if include_terminal_logs:
                    logs = _owned_job_logs(
                        client,
                        job.job_id,
                        limit=terminal_log_limit,
                    )
    result: JSON = {
        "cluster": definition.name,
        "job_id": job.job_id,
        "state": job.state.value,
        "kind": job.kind.value,
        "terminal": job.state in {JobState.SUCCEEDED, JobState.FAILED, JobState.CANCELED},
        "remote": True,
        "route_revision": _route_revision(definition),
    }
    if wait_for_terminal_result:
        _attach_wait_observation(
            result,
            observation_unknown=observation_unknown,
            timeout_seconds=wait_timeout_seconds,
        )
    if wait_for_terminal_result and not observation_unknown and include_terminal_mcp_result:
        _attach_terminal_mcp_evidence(
            result,
            source_job=job,
            last_error=job.last_error,
            artifacts=artifacts,
            parsed_result=parsed_result,
        )
    if wait_for_terminal_result and not observation_unknown and logs is not None:
        result["logs"] = logs
    return result


def _require_terminal_staging_reconciliation(result: JSON, *, operation: str) -> None:
    """Refuse to lose package semantics or staged-input lineage after a bounded wait."""
    if result.get("terminal") is True:
        return
    cluster = result.get("cluster")
    job_id = result.get("job_id")
    route_revision = result.get("route_revision")
    handle = f"cluster={cluster!r}, job_id={job_id!r}, route_revision={route_revision!r}"
    raise ValueError(
        f"{operation} did not become terminal during bounded contract reconciliation; "
        f"the durable relay job remains observable ({handle}). Use relay_wait on that exact "
        "handle, then retry this call with identical arguments. Relay reuses the deterministic "
        "reconciliation identity when idempotency_key was omitted. No package contract or "
        "staged-input lineage was accepted locally."
    )


def _jarvis_add_step_result_step_id(result: JSON) -> str:
    """Extract the exact accepted step identity from a terminal add-step result."""
    raw_mcp_result = result.get("mcp_result")
    if not isinstance(raw_mcp_result, dict):
        raise ValueError("jarvis_add_step terminal result omitted its MCP result")
    raw_structured = cast(JSON, raw_mcp_result).get("structured_result")
    if not isinstance(raw_structured, dict):
        raise ValueError("jarvis_add_step terminal result omitted structured output")
    raw_payload = cast(JSON, raw_structured).get("result")
    if not isinstance(raw_payload, dict):
        raise ValueError("jarvis_add_step structured output omitted its result object")
    step_id = cast(JSON, raw_payload).get("step_id")
    if not isinstance(step_id, str) or not step_id:
        raise ValueError("jarvis_add_step structured output omitted its step identity")
    return step_id


def _submit_local_job(
    queue: ClioCoreQueue,
    job: RelayJob,
    *,
    settings: RelaySettings,
) -> RelayJob:
    """Stamp local session ownership only after exact durable admission is open."""
    session_id = settings.owner_session_id
    generation_id = settings.owner_session_generation_id
    if session_id is None or generation_id is None:
        return queue.submit_job(job)
    admission = queue.owner_session_generation_status(
        session_id,
        session_generation_id=generation_id,
    )
    if admission.get("open") is not True:
        raise ValueError("owner session generation is not open for local MCP submission")
    metadata = dict(job.metadata)
    if {
        "owner",
        "owner_session_id",
        "owner_session_generation_id",
        "owner_session_admission_id",
    }.intersection(metadata):
        raise ValueError("local MCP job cannot supply relay-managed ownership metadata")
    metadata.update(
        {
            "owner": "clio-relay",
            "owner_session_id": session_id,
            "owner_session_generation_id": generation_id,
        }
    )
    return queue.submit_job(job.model_copy(update={"metadata": metadata}))


def _submit_jarvis_mcp_call(
    arguments: JSON,
    *,
    queue: ClioCoreQueue,
    settings: RelaySettings,
) -> JSON:
    forwarded = dict(arguments)
    cluster = _required_str(arguments, "cluster")
    used_artifact_refs = _artifact_use_refs(arguments)
    tool = _required_str(arguments, "tool")
    tool_arguments = _object(arguments.get("arguments", {}))
    if tool == "jarvis_run" and "wait" in tool_arguments:
        raise ValueError("jarvis_run does not accept internal wait; use jarvis_get_execution")
    digest = hashlib.sha256(
        json.dumps(tool_arguments, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    forwarded["expected_jarvis_cd_lock_binding"] = jarvis_cd_lock_binding_expectation()
    registered_route = arguments.get("registered_route") is True
    definition = (
        _remote_cluster_definition(cluster)
        if registered_route
        else _optional_cluster_definition(cluster)
    )
    expected_cluster_route_revision = _optional_str(
        arguments,
        "expected_cluster_route_revision",
    )
    if definition is not None and expected_cluster_route_revision is not None:
        observed_cluster_route_revision = _route_revision(definition)
        if not hmac.compare_digest(
            observed_cluster_route_revision,
            expected_cluster_route_revision,
        ):
            raise ValueError(
                f"cluster route changed for {cluster}; call tools/list again before submission"
            )
    expected_server_artifact_digest = (
        jarvis_mcp_artifact_binding(cluster)
        if registered_route or settings.owner_session_id is not None
        else None
    )
    catalog_expected_server_artifact_digest = _optional_str(
        arguments,
        "catalog_expected_server_artifact_digest",
    )
    if catalog_expected_server_artifact_digest is not None and (
        expected_server_artifact_digest is None
        or not hmac.compare_digest(
            expected_server_artifact_digest,
            catalog_expected_server_artifact_digest,
        )
    ):
        raise ValueError(
            f"JARVIS MCP identity changed for {cluster}; call tools/list again before submission"
        )
    if expected_server_artifact_digest is not None:
        forwarded["expected_server_artifact_digest"] = expected_server_artifact_digest
    timeout_seconds = _optional_int(arguments, "timeout_seconds")
    admission_class, admission_authority = resolve_pinned_mcp_admission(
        operation=McpOperation.TOOLS_CALL,
        tool=tool,
        expected_server_artifact_digest=expected_server_artifact_digest,
        pinned_control_query=is_virtual_jarvis_control_query(tool),
        timeout_seconds=timeout_seconds,
    )
    if admission_class is McpAdmissionClass.CONTROL_QUERY and timeout_seconds is None:
        timeout_seconds = MAX_PINNED_CONTROL_QUERY_TIMEOUT_SECONDS
    if timeout_seconds is not None:
        forwarded["timeout_seconds"] = timeout_seconds
    dependency_suffix = (
        ":"
        + _stable_digest(
            {"used_artifact_refs": [artifact_use_payload(item) for item in used_artifact_refs]}
        )
        if used_artifact_refs
        else ""
    )
    legacy_idempotency_key = f"mcp:{cluster}:jarvis:{tool}:{digest}{dependency_suffix}"
    derived_idempotency_key = (
        legacy_idempotency_key
        if admission_class is McpAdmissionClass.WORKLOAD
        else (
            f"mcp:{cluster}:jarvis:{tool}:{digest}:"
            f"{expected_server_artifact_digest or 'unbound'}:{admission_class.value}:"
            f"{admission_authority.source if admission_authority is not None else 'none'}:"
            f"timeout={timeout_seconds}{dependency_suffix}"
        )
    )
    idempotency_key = str(forwarded.get("idempotency_key") or derived_idempotency_key)
    forwarded["idempotency_key"] = idempotency_key
    if definition is not None and should_execute_on_cluster(definition):
        if settings.owner_session_id is not None:
            if expected_server_artifact_digest is None:
                raise ValueError(
                    "owned JARVIS MCP submission requires a discovered server artifact binding"
                )
            payload: dict[str, object] = {
                "cluster": cluster,
                "operation": McpOperation.TOOLS_CALL.value,
                "tool": tool,
                "arguments": tool_arguments,
                "expected_server_artifact_digest": expected_server_artifact_digest,
                "idempotency_key": idempotency_key,
                "used_artifact_refs": [artifact_use_payload(item) for item in used_artifact_refs],
            }
            if timeout_seconds is not None:
                payload["timeout_seconds"] = timeout_seconds
            job = submit_owned_session_job(
                definition=definition,
                settings=settings,
                path="/jobs/jarvis-mcp-call",
                payload=payload,
            )
            return _owned_session_submission_result(
                job,
                definition=definition,
                settings=settings,
                wait_for_terminal_result=bool(arguments.get("wait_for_terminal", False)),
                wait_timeout_seconds=float(arguments.get("wait_timeout_seconds", 600)),
                poll_seconds=float(arguments.get("poll_seconds", 2)),
                include_terminal_mcp_result=True,
            )
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
        if timeout_seconds is not None:
            remote_args.extend(["--timeout-seconds", str(timeout_seconds)])
        if expected_server_artifact_digest is not None:
            remote_args.extend(
                ["--expected-server-artifact-digest", expected_server_artifact_digest]
            )
        for item in used_artifact_refs:
            remote_args.extend(["--used-artifact", _artifact_use_cli_value(item)])
        try:
            write_remote_file(
                definition,
                remote_args_path,
                json.dumps(tool_arguments, sort_keys=True, separators=(",", ":")).encode("utf-8"),
            )
            output = run_remote_clio(definition, remote_args)
        finally:
            remove_remote_file(definition, remote_args_path, remove_empty_parent=True)
        return _remote_mcp_submission_result(
            output,
            definition=definition,
            arguments=arguments,
        )
    server = jarvis_mcp_server()
    server_args = jarvis_mcp_server_args()
    forwarded["server"] = server
    forwarded["server_args"] = server_args
    return _submit_mcp_call(
        forwarded,
        queue=queue,
        settings=settings,
        pinned_jarvis=True,
    )


def _submission_result(
    job: RelayJob,
    arguments: JSON,
    *,
    queue: ClioCoreQueue,
    settings: RelaySettings | None = None,
    definition: ClusterDefinition | None = None,
    include_terminal_mcp_result: bool = False,
) -> JSON:
    waited = bool(arguments.get("wait_for_terminal", False))
    observation_unknown = False
    wait_timeout_seconds = _observation_timeout_seconds(
        arguments,
        "wait_timeout_seconds",
    )
    if waited:
        try:
            job = wait_for_terminal(
                queue,
                job.job_id,
                timeout_seconds=wait_timeout_seconds,
                poll_seconds=_observation_timeout_seconds(
                    arguments,
                    "poll_seconds",
                    default=2.0,
                ),
            )
        except TimeoutError:
            job = queue.get_job(job.job_id)
        observation_unknown = job.state not in TERMINAL_STATES
    result: JSON = {
        "cluster": job.cluster,
        "job_id": job.job_id,
        "state": job.state.value,
        "kind": job.kind.value,
        "terminal": job.state.value in {"succeeded", "failed", "canceled"},
    }
    if definition is not None:
        result["route_revision"] = _route_revision(definition)
    if waited:
        _attach_wait_observation(
            result,
            observation_unknown=observation_unknown,
            timeout_seconds=wait_timeout_seconds,
        )
    if waited and not observation_unknown and include_terminal_mcp_result:
        artifacts = _complete_local_artifacts(queue, job.job_id)
        _attach_terminal_mcp_evidence(
            result,
            source_job=job,
            last_error=job.last_error,
            artifacts=artifacts,
            parsed_result=_verified_local_mcp_result(queue, job.job_id),
        )
    if waited and not observation_unknown and arguments.get("include_logs", False) is True:
        if settings is None:
            raise ValueError("local waited log retrieval requires relay settings")
        result["logs"] = _job_logs(
            queue,
            settings,
            job.job_id,
            limit=_log_limit(arguments),
        )
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
        job_id=_required_durable_record_id(arguments, "job_id"),
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
            job_id=_required_durable_record_id(arguments, "job_id"),
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
            task_id=_required_durable_record_id(arguments, "task_id"),
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
    _reject_generic_gateway_runtime_fields(arguments, creating=True)
    session = queue.create_gateway_session(
        GatewaySession(
            cluster=_required_str(arguments, "cluster"),
            name=_required_str(arguments, "name"),
            state=GatewaySessionState(str(arguments.get("state", "created"))),
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
    return public_gateway_session(session)


def _bind_jarvis_runtime(
    arguments: JSON,
    *,
    queue: ClioCoreQueue,
    settings: RelaySettings,
) -> JSON:
    """Bind only connector resources derived from one verified JARVIS result."""
    allowed = {
        "binding",
        "cluster",
        "source_job_id",
        "source_artifact_id",
        "package_id",
        "package_name",
        "name",
        "readiness_timeout_seconds",
        "poll_seconds",
    }
    unexpected = sorted(set(arguments) - allowed)
    if unexpected:
        raise ValueError(
            "relay_bind_jarvis_runtime does not accept caller-supplied runtime metadata: "
            + ", ".join(unexpected)
        )
    (
        cluster,
        source_job_id,
        source_artifact_id,
        package_id,
        package_name,
        service_instance_id,
    ) = _jarvis_runtime_binding_selectors(arguments)
    definition = _remote_cluster_definition(cluster)
    verified = resolve_jarvis_service_runtime(
        queue=queue,
        definition=definition,
        settings=settings,
        source_job_id=source_job_id,
        source_artifact_id=source_artifact_id,
        package_id=package_id,
        package_name=package_name,
        service_instance_id=service_instance_id,
    )
    readiness_timeout_seconds = _positive_float_argument(
        arguments,
        "readiness_timeout_seconds",
        default=300.0,
        maximum=3_600.0,
    )
    poll_seconds = _positive_float_argument(
        arguments,
        "poll_seconds",
        default=2.0,
        maximum=60.0,
    )
    runtime_name = _optional_str(arguments, "name") or (
        f"{verified.runtime.package_name}-{verified.runtime.service_instance_id}"
    )
    if len(runtime_name) > 256:
        raise ValueError("name must not exceed 256 characters")
    transport = definition.frp_transport
    supervisor = ServiceRuntimeSupervisor(
        settings=settings,
        queue=queue,
        cluster=cluster,
        definition=definition,
        token=_required_environment_secret(transport.token_env, "frp token"),
        secret_key=_required_environment_secret(
            transport.stcp_secret_env,
            "stcp secret",
        ),
    )
    owner_session_id = settings.owner_session_id
    owner_session_generation_id = settings.owner_session_generation_id
    if owner_session_id is None or owner_session_generation_id is None:
        started = supervisor.bind_verified_jarvis_runtime(
            name=runtime_name,
            verified=verified,
            readiness_timeout_seconds=readiness_timeout_seconds,
            poll_seconds=poll_seconds,
        )
    else:
        if settings.resolved_owner_session_cluster() != cluster:
            raise ConfigurationError(
                "owned runtime binding requires CLIO_RELAY_OWNER_SESSION_CLUSTER to match "
                "the selected route"
            )
        with owner_session_gateway_admission(
            queue=queue,
            definition=definition,
            cluster=cluster,
            session_id=owner_session_id,
            session_generation_id=owner_session_generation_id,
        ) as admission:
            started = supervisor.bind_verified_jarvis_runtime(
                name=runtime_name,
                verified=verified,
                owner_session_id=admission.owner_session_id,
                owner_session_generation_id=admission.owner_session_generation_id,
                owner_session_admission_id=admission.owner_session_admission_id,
                readiness_timeout_seconds=readiness_timeout_seconds,
                poll_seconds=poll_seconds,
            )
    gateway_session = public_gateway_session(started.session)
    gateway_session_id = gateway_session.get("session_id")
    if gateway_session_id != started.session.session_id:
        raise ValueError("public gateway session identity did not match the bound runtime")
    if isinstance(started, ServiceRuntimePendingResult):
        pending_gateway = dict(gateway_session)
        nested_gateway = _object(pending_gateway.get("gateway", {}))
        for key in (
            "connect_url",
            "health_url",
            "stream_url",
            "events_url",
            "state_url",
            "command_url",
            "compatibility_urls",
        ):
            nested_gateway.pop(key, None)
        pending_gateway["gateway"] = nested_gateway
        return {
            "outcome": started.outcome,
            "retry_selector": started.retry_selector(),
            "scheduler_action": started.scheduler_action,
            "relay_action": started.relay_action,
            "gateway_session_id": gateway_session_id,
            "gateway_session": pending_gateway,
            "connect_url": None,
            "health_url": None,
            "stream_url": None,
            "events_url": None,
            "state_url": None,
            "command_url": None,
            "scheduler_cancel_requested": False,
        }
    if any(
        value is None
        for value in (
            started.stream_url,
            started.events_url,
            started.state_url,
            started.command_url,
        )
    ):
        raise ValueError("verified JARVIS runtime did not produce the complete URL contract")
    return {
        "outcome": "ready",
        "retry_selector": None,
        "scheduler_action": "none",
        "relay_action": "none",
        "gateway_session_id": gateway_session_id,
        "gateway_session": gateway_session,
        "connect_url": started.connect_url,
        "health_url": started.health_url,
        "stream_url": started.stream_url,
        "events_url": started.events_url,
        "state_url": started.state_url,
        "command_url": started.command_url,
        "scheduler_cancel_requested": False,
    }


def _jarvis_runtime_binding_selectors(
    arguments: JSON,
) -> tuple[str, str, str, str, str, str | None]:
    """Accept one exact handoff object or the legacy scalar selector contract."""
    scalar_fields = {
        "cluster",
        "source_job_id",
        "source_artifact_id",
        "package_id",
        "package_name",
    }
    if "binding" in arguments:
        mixed = sorted(scalar_fields.intersection(arguments))
        if mixed:
            raise ValueError(
                "relay_bind_jarvis_runtime binding cannot be mixed with legacy selectors: "
                + ", ".join(mixed)
            )
        try:
            handoff = JarvisServiceRuntimeHandoff.model_validate(arguments["binding"])
        except ValidationError as exc:
            raise ValueError(f"relay_bind_jarvis_runtime binding is invalid: {exc}") from exc
        return (
            handoff.cluster,
            validate_durable_record_id(handoff.source_job_id),
            validate_durable_record_id(handoff.source_artifact_id),
            handoff.package_id,
            handoff.package_name,
            handoff.service_instance_id,
        )
    return (
        _required_str(arguments, "cluster"),
        _required_durable_record_id(arguments, "source_job_id"),
        _required_durable_record_id(arguments, "source_artifact_id"),
        _required_str(arguments, "package_id"),
        _required_str(arguments, "package_name"),
        None,
    )


def _update_gateway_session(arguments: JSON, *, queue: ClioCoreQueue) -> JSON:
    _reject_generic_gateway_runtime_fields(arguments, creating=False)
    updates: dict[str, object] = {}
    for key in {
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
        _required_durable_record_id(arguments, "session_id"),
        state=state,
        metadata=_object(arguments.get("metadata", {})),
        reject_relay_managed_fields=True,
        **updates,
    )
    return public_gateway_session(session)


_RELAY_RUNTIME_GATEWAY_KEYS = frozenset(
    {
        "runtime_spec",
        "jarvis_runtime_binding",
        "browser_attachment",
        "ownership_intents",
        "teardown_intent",
        "teardown",
        "detach",
        "scheduler_provider",
        "scheduler_job_id",
        "scheduler_native_id",
    }
)
_RELAY_RUNTIME_CONNECTOR_KEYS = frozenset(
    {"browser_proxy", "desktop_connector", "remote_connector"}
)
_RELAY_OWNERSHIP_METADATA_KEYS = frozenset(
    {
        "owner",
        "owner_session_id",
        "owner_session_generation_id",
        "owner_session_admission_id",
        "runtime_kind",
        "binding_source",
        "source_relay_job_id",
        "source_relay_artifact_id",
        "jarvis_execution_id",
        "scheduler_provider",
        "scheduler_job_id",
        "scheduler_native_id",
    }
)


def _reject_generic_gateway_runtime_fields(arguments: JSON, *, creating: bool) -> None:
    """Keep generic MCP gateway tools outside relay-owned runtime identity."""
    protected: list[str] = []
    top_level = {"scheduler_job_id"}
    if creating:
        top_level.add("scheduler")
    protected.extend(sorted(top_level.intersection(arguments)))
    gateway = _object(arguments.get("gateway", {}))
    protected.extend(sorted(_RELAY_RUNTIME_GATEWAY_KEYS.intersection(gateway)))
    transport = gateway.get("transport")
    if isinstance(transport, dict):
        typed_transport = cast(JSON, transport)
        protected.extend(
            f"gateway.transport.{key}"
            for key in sorted(_RELAY_RUNTIME_CONNECTOR_KEYS.intersection(typed_transport))
        )
    metadata = _object(arguments.get("metadata", {}))
    protected.extend(
        f"metadata.{key}" for key in sorted(_RELAY_OWNERSHIP_METADATA_KEYS.intersection(metadata))
    )
    if protected:
        raise ValueError(
            "generic gateway tools cannot write relay-managed runtime fields: "
            + ", ".join(protected)
        )


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


def _required_durable_record_id(value: JSON, key: str) -> str:
    """Read and validate a required durable record ID before queue access."""
    return validate_durable_record_id(_required_str(value, key))


def _optional_durable_record_id(value: JSON, key: str) -> str | None:
    """Read and validate an optional durable record ID before queue access."""
    item = _optional_str(value, key)
    return None if item is None else validate_durable_record_id(item)


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


def _artifact_use_refs(arguments: JSON) -> list[ArtifactUse]:
    """Parse and canonicalize content-pinned artifact dependencies."""
    raw = arguments.get("used_artifact_refs", [])
    if not isinstance(raw, list):
        raise ValueError("used_artifact_refs must be an array")
    values = cast(list[object], raw)
    if len(values) > 1_000:
        raise ValueError("used_artifact_refs must contain at most 1000 records")
    try:
        refs = [ArtifactUse.model_validate(value) for value in values]
    except ValidationError as exc:
        raise ValueError(f"used_artifact_refs is invalid: {exc}") from exc
    artifact_ids = [ref.artifact_id for ref in refs]
    if len(artifact_ids) != len(set(artifact_ids)):
        raise ValueError("used_artifact_refs must contain unique artifact_id values")
    canonical = sorted(refs, key=lambda ref: ref.artifact_id)
    validate_artifact_use_collection(canonical)
    return canonical


def _artifact_use_cli_value(ref: ArtifactUse) -> str:
    """Render a legacy shorthand or canonical JSON dependency for remote CLI transport."""
    if ref.provenance is None:
        return f"{ref.artifact_id}={ref.sha256}"
    return json.dumps(
        artifact_use_payload(ref),
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    )


def _log_limit(arguments: JSON) -> int:
    return _bounded_integer_limit(
        arguments,
        field_name="log_limit",
        default=MAX_AGENT_LOG_READ_BYTES,
        maximum=MAX_AGENT_LOG_READ_BYTES,
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


def _positive_float_argument(
    arguments: JSON,
    field_name: str,
    *,
    default: float,
    maximum: float,
) -> float:
    """Read a positive bounded numeric MCP argument without accepting booleans."""
    raw = arguments.get(field_name, default)
    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
        raise ValueError(f"{field_name} must be a number")
    value = float(raw)
    if value <= 0 or value > maximum:
        raise ValueError(f"{field_name} must be greater than 0 and at most {maximum:g}")
    return value


def _observation_timeout_seconds(
    arguments: JSON,
    field_name: str,
    *,
    default: float = 600.0,
) -> float:
    """Read one finite positive observation bound without creating an execution deadline."""
    raw = arguments.get(field_name, default)
    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
        raise ValueError(f"{field_name} must be a number")
    value = float(raw)
    if not math.isfinite(value) or value <= 0:
        raise ValueError(f"{field_name} must be a finite number greater than 0")
    return value


def _attach_wait_observation(
    result: JSON,
    *,
    observation_unknown: bool,
    timeout_seconds: float,
) -> None:
    """Attach a machine-readable outcome for one bounded wait without mutating its job."""
    result["observation"] = {
        "outcome": "observation_unknown" if observation_unknown else "terminal",
        "timeout_seconds": timeout_seconds,
        "scheduler_action": "none",
        "relay_action": "none",
    }


def _jarvis_submission_wait_timeout_seconds(arguments: JSON) -> float:
    """Resolve canonical and legacy JARVIS submission observation bounds."""
    resolved: dict[str, float] = {}
    for field_name in ("wait_timeout_seconds", "timeout_seconds"):
        if field_name not in arguments:
            continue
        raw = arguments[field_name]
        if isinstance(raw, bool) or not isinstance(raw, (int, float)):
            raise ValueError(f"{field_name} must be a number")
        value = float(raw)
        if not math.isfinite(value) or value <= 0:
            raise ValueError(f"{field_name} must be a finite number greater than 0")
        resolved[field_name] = value
    canonical = resolved.get("wait_timeout_seconds")
    legacy = resolved.get("timeout_seconds")
    if canonical is not None and legacy is not None and canonical != legacy:
        raise ValueError(
            "wait_timeout_seconds and legacy timeout_seconds must be equal when both are "
            "provided; both fields bound observation only"
        )
    if canonical is not None:
        return canonical
    if legacy is not None:
        return legacy
    return 600.0


def _required_environment_secret(name: str, label: str) -> str:
    """Resolve one configured transport secret without exposing it in records."""
    value = os.environ.get(name)
    if value is None or not value:
        raise ValueError(f"{label} is required in environment variable {name}")
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

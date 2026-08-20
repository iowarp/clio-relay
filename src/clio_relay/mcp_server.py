"""Stdio MCP server for relay job submission tools."""

from __future__ import annotations

import base64
import copy
import hashlib
import hmac
import json
import re
import sys
from contextlib import suppress
from dataclasses import dataclass, field
from json import JSONDecodeError
from pathlib import Path
from typing import Any, TextIO, cast
from uuid import uuid4

from pydantic import ValidationError

from clio_relay import __version__
from clio_relay.bounded_payload import (
    build_delivery_refusal,
    is_delivery_refusal,
    is_delivery_refusal_failed,
)
from clio_relay.cluster_config import (
    ClusterDefinition,
    ClusterRegistry,
    default_registry_path,
)
from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.errors import (
    NotFoundError,
    ObservationPatternError,
    ObservationTimeoutError,
)
from clio_relay.filesystem_paths import logical_filesystem_text
from clio_relay.identifiers import (
    validate_durable_record_id,
)
from clio_relay.input_staging import (
    JarvisPackageInputContract,
    merge_artifact_uses,
)
from clio_relay.jarvis_input_plane import (
    JarvisInputPlan,
    builtin_jarvis_staging_route,
    jarvis_submission_idempotency_key,
    prepare_jarvis_inputs,
)
from clio_relay.jarvis_mcp import (
    is_virtual_jarvis_control_query,
    jarvis_cd_lock_binding_expectation,
    jarvis_mcp_artifact_binding,
    jarvis_mcp_server,
    jarvis_mcp_server_args,
    render_virtual_jarvis_agent_context,
    virtual_jarvis_remote_tool,
)
from clio_relay.jarvis_mcp import (
    # Re-exported (not called bare from this module any more since
    # _tool_definitions_and_remote_catalog moved to mcp_remote_catalog.py):
    # tests/test_mcp_server.py reads mcp_server_module.is_virtual_jarvis_tool
    # directly (not a monkeypatch, just an attribute access checking a real
    # tool name). The `as <same name>` form keeps ruff's F401 from stripping
    # an import whose only remaining purpose is being re-exported.
    is_virtual_jarvis_tool as is_virtual_jarvis_tool,
)
from clio_relay.jarvis_service_runtime import (
    JARVIS_SERVICE_RUNTIME_SCHEMA_V2,
    JarvisServiceAuthorization,
    derive_jarvis_service_runtime_handoffs,
)
from clio_relay.jarvis_service_runtime import (
    # Re-exported (no longer called bare from this module: its only caller,
    # _bind_jarvis_runtime, moved to mcp_gateway_tools.py): tests directly
    # monkeypatch mcp_server_module.resolve_jarvis_service_runtime, and
    # mcp_gateway_tools.py's _bind_jarvis_runtime reaches it only through
    # the `_mcp_server.<name>` back-reference is NOT used here (it calls
    # this one bare, same-module -- see mcp_gateway_tools.py's own import
    # of it). The re-export exists purely so the monkeypatch target
    # resolves; the `as <same name>` form keeps ruff's F401 from stripping
    # an import that looks unused within this file's own source but is not.
    resolve_jarvis_service_runtime as resolve_jarvis_service_runtime,
)
from clio_relay.mcp_arguments import (
    _artifact_use_cli_value,
    _artifact_use_refs,
    _attach_wait_observation,
    _boolean_argument,
    _jarvis_submission_wait_timeout_seconds,
    _log_limit,
    _object,
    _observation_timeout_seconds,
    _optional_durable_record_id,
    _optional_int,
    _optional_str,
    _required_durable_record_id,
    _required_str,
    _response_page_limit,
    _stable_digest,
    _string_list,
    _string_mapping,
)
from clio_relay.mcp_dispatch import _call_tool
from clio_relay.mcp_gateway_tools import (
    # Re-exported (no longer called bare from this module): every
    # name here is reached only through mcp_dispatch.py's
    # `_mcp_server.<name>(...)` back-reference, which resolves through
    # THIS module's namespace -- the `as <same name>` form keeps
    # ruff's F401 from stripping an import that looks unused within
    # this file's own source but is not.
    _bind_jarvis_runtime as _bind_jarvis_runtime,
)
from clio_relay.mcp_gateway_tools import (
    _create_gateway_session as _create_gateway_session,
)
from clio_relay.mcp_gateway_tools import (
    _monitor_rule_from_arguments as _monitor_rule_from_arguments,
)
from clio_relay.mcp_gateway_tools import (
    _record_progress as _record_progress,
)
from clio_relay.mcp_gateway_tools import (
    _record_task_event as _record_task_event,
)
from clio_relay.mcp_gateway_tools import (
    _update_gateway_session as _update_gateway_session,
)
from clio_relay.mcp_queue_tools import (
    # Re-exported (no longer called bare from this module): every
    # name here is reached only through mcp_dispatch.py's
    # `_mcp_server.<name>(...)` back-reference, which resolves through
    # THIS module's namespace -- the `as <same name>` form keeps
    # ruff's F401 from stripping an import that looks unused within
    # this file's own source but is not.
    _queue_cancel_tool as _queue_cancel_tool,
)
from clio_relay.mcp_queue_tools import (
    _queue_cleanup_stale_tool as _queue_cleanup_stale_tool,
)
from clio_relay.mcp_queue_tools import (
    _queue_diagnose_tool as _queue_diagnose_tool,
)
from clio_relay.mcp_queue_tools import (
    _queue_list_tool as _queue_list_tool,
)
from clio_relay.mcp_queue_tools import (
    _queue_stale_tool as _queue_stale_tool,
)
from clio_relay.mcp_queue_tools import (
    _worker_status_tool as _worker_status_tool,
)
from clio_relay.mcp_remote_catalog import (
    _bound_virtual_jarvis_clusters,
    _mcp_profile_from_env,
    _normalize_profile,
    _route_revision,
    _tool_definitions_and_remote_catalog,
    _validated_route_revision,
)
from clio_relay.mcp_remote_catalog import (
    # Re-exported (no longer called bare from this module): mcp_dispatch.py's
    # `_call_tool` reaches these only through the `_mcp_server.<name>(...)`
    # back-reference, and tests/test_mcp_server.py monkeypatches
    # `_remote_mcp_catalog`/`_configured_cluster_names` directly at
    # mcp_server_module.<name>. Both need the name bound in THIS module's
    # namespace, not just mcp_remote_catalog's -- the `as <same name>` form
    # keeps ruff's F401 from stripping an import that looks unused within
    # this file's own source but is not.
    _configured_cluster_names as _configured_cluster_names,
)
from clio_relay.mcp_remote_catalog import (
    _remote_mcp_catalog as _remote_mcp_catalog,
)
from clio_relay.mcp_remote_catalog import (
    _require_compatible_remote_mcp_catalog as _require_compatible_remote_mcp_catalog,
)
from clio_relay.mcp_tool_catalog import (
    MAX_AGENT_LOG_READ_BYTES,
    static_mcp_tool_names,
)
from clio_relay.mcp_tool_catalog import (
    # Re-exported (not called from this module any more): cli.py /
    # fastmcp_server.py / mcp_stdio_validation.py import USER_MCP_TOOL_NAMES
    # from clio_relay.mcp_server, and tests/test_identifiers.py calls
    # mcp_server_module._all_tool_definitions(...) directly. The `as <same
    # name>` form is the standard explicit-reexport idiom -- it keeps ruff's
    # F401 (and pyright's reportUnusedImport) from stripping an import whose
    # only remaining purpose is being re-exported through this module's
    # namespace.
    USER_MCP_TOOL_NAMES as USER_MCP_TOOL_NAMES,
)
from clio_relay.mcp_tool_catalog import (
    _all_tool_definitions as _all_tool_definitions,
)
from clio_relay.models import (
    MCP_ADMISSION_AUTHORITY_METADATA_KEY,
    TERMINAL_STATES,
    ArtifactRef,
    ArtifactUse,
    JarvisRunInputManifest,
    JarvisRunSpec,
    JobKind,
    JobState,
    JobWaitResult,
    McpAdmissionClass,
    McpCallSpec,
    McpControlQueryEvidence,
    McpOperation,
    RelayJob,
    RemoteAgentTaskSpec,
    artifact_use_payload,
)
from clio_relay.observation import (
    MAX_OBSERVATION_SCAN_BYTES,
    compile_observation_pattern,
    normalize_pattern_scope,
    observe_until_pattern,
    observe_until_pattern_snapshots,
)
from clio_relay.pagination import (
    MAX_RESPONSE_PAGE_RECORDS,
)
from clio_relay.queue_management import (
    cancel_queue_job,
)
from clio_relay.relay_ops import (
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
    VirtualRemoteMcpCatalog,
    default_remote_mcp_cache_path,
    load_virtual_remote_mcp_catalog,
    remote_mcp_registration_revision,
    resolve_pinned_mcp_admission,
    resolve_registered_remote_mcp_admission,
)
from clio_relay.session_api import (
    OWNED_SESSION_WAIT_RESPONSE_GRACE_SECONDS,
    OwnedSessionApiClient,
    submit_owned_session_job,
)
from clio_relay.storage_runtime import (
    StorageAdmissionError,
    storage_managed_queue,
)
from clio_relay.validation_report import redact_sensitive_values

JSON = dict[str, Any]
MAX_INTERNAL_COLLECTION_RECORDS = 10_000
MAX_INLINE_MCP_RESULT_BYTES = 65_536
MCP_RESULT_INLINE_LIMIT_CODE = "inline_result_limit_exceeded"
MCP_RESULT_INLINE_LIMIT_MESSAGE = (
    "The remote MCP operation reached a terminal state, but its result exceeded the safe "
    "inline response limit and is unavailable to the agent. Immutable private evidence was "
    "preserved for operator diagnosis. Remote side effects may have occurred; inspect the "
    "job before retrying."
)
MAX_OBSERVE_MATCHES = 100
MAX_OBSERVE_MATCH_TEXT_CHARS = 1_024
REMOTE_WAIT_STATUS_TIMEOUT_SECONDS = 30.0
_REMOTE_JOB_FOLLOWUP_TOOL_NAMES = frozenset(
    {
        "relay_status",
        "relay_cancel",
        "relay_observe",
        "relay_wait",
    }
)


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


def normalize_mcp_profile(profile: str) -> str:
    """Normalize a public MCP tool-profile name."""
    return _normalize_profile(profile)


def mcp_tool_definitions_and_remote_catalog(
    *,
    profile: str,
) -> tuple[list[JSON], VirtualRemoteMcpCatalog]:
    """Return the complete static and dynamic MCP tool catalog."""
    return _tool_definitions_and_remote_catalog(profile=profile)


def call_mcp_tool(
    params: JSON,
    *,
    queue: ClioCoreQueue,
    settings: RelaySettings,
    profile: str,
    session: McpSessionState | None,
    observed_remote_mcp_catalog_revision: str | None,
    require_advertised_remote_mcp_catalog: bool,
) -> JSON:
    """Call one established relay MCP tool through its shared dispatcher."""
    return _call_tool(
        params,
        queue=queue,
        settings=settings,
        profile=profile,
        session=session,
        observed_remote_mcp_catalog_revision=observed_remote_mcp_catalog_revision,
        require_advertised_remote_mcp_catalog=require_advertised_remote_mcp_catalog,
    )


def status_mcp_job(
    arguments: JSON,
    *,
    queue: ClioCoreQueue,
    settings: RelaySettings,
) -> JSON:
    """Read one local or routed relay job through MCP handle semantics."""
    return _status_job(arguments, queue=queue, settings=settings)


def wait_mcp_job(
    arguments: JSON,
    *,
    queue: ClioCoreQueue,
    settings: RelaySettings,
) -> JSON:
    """Boundedly reconcile one local or routed MCP job."""
    return _wait_job(arguments, queue=queue, settings=settings)


def serialize_mcp_tool_result(result: JSON) -> str:
    """Serialize a bounded human-readable relay tool result."""
    return _serialize_tool_result(result)


def mcp_tool_result_failed(result: JSON) -> bool:
    """Return whether a relay result maps to CallToolResult.isError."""
    return _mcp_tool_result_failed(result)


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
    except ObservationPatternError as exc:
        return _error(
            request_id,
            -32602,
            str(exc),
            data={"reason": exc.reason},
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


def _initialize_result() -> JSON:
    return {
        "protocolVersion": "2024-11-05",
        "capabilities": {"tools": {}},
        "serverInfo": {"name": "clio-relay", "version": __version__},
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
        "produced_by": artifact.metadata.get("produced_by"),
        "cursor": cursor,
        "limit": limit,
        "next_cursor": next_cursor,
        "total": total,
    }
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
            # D16: no post-loop completeness check needed here (unlike this
            # function's two siblings above) -- ``_validate_complete_
            # collection_page`` above already enforces the identical
            # ``next_cursor is None and collected_count + page_count != total``
            # invariant on every iteration, before ``records.extend(page)``
            # runs, so a page that understates its own total never reaches
            # this line. Do not re-add a post-loop check here; it would be
            # provably dead code (verified: this function's own regression
            # test still fails on the in-loop message, never a post-loop one).
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


def _owned_mcp_result_is_required(job: RelayJob) -> bool:
    """Whether a succeeded job's worker is expected to have written an mcp_result.

    True for any MCP_CALL job that reached ``SUCCEEDED``, ``WORKLOAD`` and
    ``CONTROL_QUERY`` admission classes alike: the worker unconditionally
    writes the ``mcp_result`` artifact before marking success (D17), so a
    missing one there is always a defect (D14/D15), never a legitimate
    state.

    C2 (review): this used to exempt ``CONTROL_QUERY`` on the theory that it
    is "answered outside the ordinary spooled worker/artifact pipeline" --
    false against live evidence. ``CONTROL_QUERY`` is only a worker-lane and
    admission-cap distinction (``endpoint.py``'s ``_serve_worker_slots``
    builds an identical ``EndpointWorker`` per lane; ``core_queue.py`` uses
    it solely to pick the concurrency ceiling); the SAME spooled
    worker/artifact pipeline runs both classes. A live production
    ``jarvis_describe``/``jarvis_get_execution`` job -- always admitted as
    CONTROL_QUERY, since the owned session path requires the pinned
    read-only ``expected_server_artifact_digest`` that triggers that
    admission class -- carries its own ``mcp_result`` artifact just like any
    WORKLOAD job. The old exclusion silently exempted exactly the two
    curated read operations this guard exists to protect.
    """
    return isinstance(job.spec, McpCallSpec) and job.state is JobState.SUCCEEDED


def _verified_owned_mcp_result(
    client: OwnedSessionApiClient,
    job_id: str,
    artifacts: list[JSON],
    *,
    require_result: bool = False,
) -> _VerifiedMcpResult | None:
    """Find and verify one job's durable MCP result artifact, if present.

    ``require_result`` is set by callers that already know this job kind
    always produces an ``mcp_result`` artifact on success (an MCP_CALL job
    that reached ``SUCCEEDED``): for them, a missing artifact is never a
    legitimate "nothing to attach" case (D14/D15/D17) -- it means a
    succeeded job would otherwise report a bounded receipt with no result
    and ``isError`` left false, indistinguishable from real success. Raise
    loud and typed instead of returning a bare ``None`` a caller could
    silently accept. Ordinary callers observing a job of unknown/mixed kind
    (e.g. ``relay_wait`` on any job) leave this ``False`` and keep the
    existing silent-None contract for job kinds that never have one.
    """
    artifact = next(
        (
            item
            for item in artifacts
            if item.get("kind") == "mcp_result" and item.get("job_id") == job_id
        ),
        None,
    )
    if artifact is None:
        if require_result:
            raise ValueError(
                f"job {job_id} succeeded but no mcp_result artifact was found among "
                f"{len(artifacts)} indexed artifact(s)"
            )
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
    if is_delivery_refusal(envelope):
        # T2 (doc §6.4): the durable mcp_result artifact itself exceeded
        # relay_ops.MAX_ARTIFACT_CONTENT_BYTES -- read_artifact_bytes already
        # refused delivery with a typed document instead of raising. Surface
        # that refusal as-is rather than let it fall into
        # _decode_verified_mcp_result, which expects a base64 envelope and
        # would otherwise misreport this as a generic malformed-artifact
        # ValueError.
        return _VerifiedMcpResult(document=envelope, public=envelope)
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


def _bounded_mcp_result(
    result: JSON,
    *,
    preserve_verified_jarvis_authorization_descriptors: bool = False,
) -> JSON:
    """Return a bounded agent projection while the artifact retains full protocol evidence."""

    original = copy.deepcopy(result)
    sanitized = redact_sensitive_values(original)
    if not isinstance(sanitized, dict):
        raise ValueError("MCP result redaction did not preserve its object shape")
    projected = cast(JSON, sanitized)
    if preserve_verified_jarvis_authorization_descriptors:
        _restore_jarvis_service_authorization_descriptors(
            original=original,
            projected=projected,
        )
    sensitive_values_redacted = projected != result
    protocol_result = projected.get("protocol_result")
    protocol_result_is_error = (
        isinstance(protocol_result, dict)
        and cast(dict[str, object], protocol_result).get("isError") is True
    )
    if (
        projected.get("structured_result") is not None
        and "protocol_result" in projected
        and not protocol_result_is_error
    ):
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
    # F7 (#231 R6 review): built through bounded_payload.build_delivery_refusal
    # -- the T2 precedent this function originated -- instead of its own
    # inline dict literal, so every raw payload path shares one constructor
    # (single owner, the slice's own rule).
    failure: JSON = build_delivery_refusal(
        code=MCP_RESULT_INLINE_LIMIT_CODE,
        message=MCP_RESULT_INLINE_LIMIT_MESSAGE,
        max_bytes=MAX_INLINE_MCP_RESULT_BYTES,
        remote_side_effects_may_have_occurred=True,
    )
    if sensitive_values_redacted:
        failure["sensitive_values_redacted"] = True
    return failure


def _restore_jarvis_service_authorization_descriptors(
    *,
    original: JSON,
    projected: JSON,
) -> None:
    """Restore only exact public JARVIS bearer fingerprints after source verification.

    The caller enables this only after the durable MCP artifact has passed the
    exact built-in or registered JARVIS contract validation. This second,
    deliberately narrow check accepts only service-runtime v2 authorization
    descriptors with the strict public shape defined by JARVIS. Raw bearer
    capabilities, additional keys, and malformed digests remain redacted.
    """

    original_runtimes = _jarvis_service_runtime_items(original)
    projected_runtimes = _jarvis_service_runtime_items(projected)
    if original_runtimes is None or projected_runtimes is None:
        return
    if len(original_runtimes) != len(projected_runtimes):
        return
    for original_runtime, projected_runtime in zip(
        original_runtimes,
        projected_runtimes,
        strict=True,
    ):
        if (
            original_runtime.get("schema_version") != JARVIS_SERVICE_RUNTIME_SCHEMA_V2
            or projected_runtime.get("schema_version") != JARVIS_SERVICE_RUNTIME_SCHEMA_V2
        ):
            continue
        raw_authorization = original_runtime.get("authorization")
        try:
            authorization = JarvisServiceAuthorization.model_validate(raw_authorization)
        except ValidationError:
            continue
        descriptor = authorization.model_dump(mode="json")
        if raw_authorization != descriptor:
            continue
        projected_runtime["authorization"] = descriptor


def _jarvis_service_runtime_items(result: JSON) -> list[JSON] | None:
    """Return the exact execution-v2 service-runtime list, if structurally present."""

    structured = result.get("structured_result")
    if not isinstance(structured, dict):
        return None
    snapshot = cast(dict[str, object], structured).get("service_runtimes")
    if not isinstance(snapshot, dict):
        return None
    raw_runtimes = cast(dict[str, object], snapshot).get("service_runtimes")
    if not isinstance(raw_runtimes, list):
        return None
    typed_runtimes = cast(list[object], raw_runtimes)
    if not all(isinstance(runtime, dict) for runtime in typed_runtimes):
        return None
    return [cast(JSON, runtime) for runtime in typed_runtimes]


def _mcp_tool_result_failed(result: JSON) -> bool:
    """Keep failed terminal remote MCP operations failed at the agent tool boundary."""

    if is_delivery_refusal_failed(result):
        # F5 (#231 R6 review): a tool's OWN result document can be a T2
        # refusal directly (e.g. relay_read_artifact/_read_model_artifact_
        # bytes reading a too-large artifact) -- not only nested under
        # mcp_result below (relay_wait's job-status shape).
        return True

    if (
        result.get("kind") == JobKind.MCP_CALL.value
        and result.get("terminal") is True
        and result.get("state") in {JobState.FAILED.value, JobState.CANCELED.value}
    ):
        return True

    mcp_result = result.get("mcp_result")
    if not isinstance(mcp_result, dict):
        return False
    typed_result = cast(dict[str, object], mcp_result)
    returncode = typed_result.get("returncode")
    if (
        typed_result.get("timed_out") is True
        or isinstance(typed_result.get("protocol_error"), str)
        or (isinstance(returncode, int) and not isinstance(returncode, bool) and returncode != 0)
    ):
        return True
    protocol_result = typed_result.get("protocol_result")
    if (
        isinstance(protocol_result, dict)
        and cast(dict[str, object], protocol_result).get("isError") is True
    ):
        return True
    return is_delivery_refusal_failed(typed_result)


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
    verified_jarvis_authorization_descriptors = False
    if (
        source_job.state is JobState.SUCCEEDED
        and isinstance(source_job.spec, McpCallSpec)
        and source_job.spec.tool == "jarvis_get_execution"
        and source_job.spec.arguments.get("include_service_runtimes") is True
    ):
        source_artifact = ArtifactRef.model_validate(artifact)
        service_runtime_handoffs = derive_jarvis_service_runtime_handoffs(
            cluster=source_job.cluster,
            source_job=source_job,
            source_artifact=source_artifact,
            document=parsed_result.document,
        )
        receipt["service_runtime_bindings"] = [
            handoff.model_dump(mode="json") for handoff in service_runtime_handoffs
        ]
        verified_jarvis_authorization_descriptors = True
    receipt["mcp_result"] = _bounded_mcp_result(
        parsed_result.public,
        preserve_verified_jarvis_authorization_descriptors=(
            verified_jarvis_authorization_descriptors
        ),
    )


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
    until_pattern = _optional_str(arguments, "until_pattern")
    if until_pattern is not None:
        if arguments.get("pattern") is not None:
            raise ValueError("pattern and until_pattern cannot be used together")
        compiled = compile_observation_pattern(until_pattern)
        scopes = normalize_pattern_scope(arguments.get("pattern_scope"))
        if target is not None and should_execute_on_cluster(target):
            result = _observe_remote_pattern(
                target,
                settings=settings,
                job_id=job_id,
                compiled=compiled,
                scopes=scopes,
                cursor=cursor,
                limit=limit,
                include_logs=arguments.get("include_logs", True) is not False,
                log_limit=_log_limit(arguments),
            )
            result["cluster"] = target.name
            result["route_revision"] = _route_revision(target)
            return result
        _require_local_job_cluster(queue, job_id, target)
        result = observe_until_pattern(
            queue,
            settings,
            job_id,
            compiled=compiled,
            scopes=scopes,
            cursor=cursor,
            limit=limit,
            include_logs=arguments.get("include_logs", True) is not False,
            log_limit=_log_limit(arguments),
        )
        if target is not None:
            result["cluster"] = target.name
            result["route_revision"] = _route_revision(target)
        return result
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


def _observe_remote_pattern(
    target: ClusterDefinition,
    *,
    settings: RelaySettings,
    job_id: str,
    compiled: re.Pattern[str],
    scopes: tuple[str, ...],
    cursor: int,
    limit: int,
    include_logs: bool,
    log_limit: int,
) -> JSON:
    """Hold a routed observation open over the remote monitor/log surfaces."""
    next_cursor = cursor
    log_offsets = {stream: 0 for stream in ("stdout", "stderr")}

    def read_snapshot() -> JSON:
        nonlocal next_cursor
        if settings.owner_session_id is not None:
            with OwnedSessionApiClient(definition=target, settings=settings) as client:
                observed = _owned_json(
                    client,
                    method="GET",
                    path=f"/jobs/{job_id}/monitor",
                    query={"cursor": next_cursor, "limit": limit},
                    label="owned remote pattern monitor",
                )
                _validate_owned_job_status(observed, job_id=job_id, cluster=target.name)
        else:
            observed = _remote_json(
                target,
                ["job", "monitor", job_id, "--cursor", str(next_cursor), "--limit", str(limit)],
                "remote pattern monitor",
            )
        returned_cursor = observed.get("next_cursor")
        if not isinstance(returned_cursor, int):
            raise ValueError("remote pattern monitor returned an invalid event cursor")
        next_cursor = returned_cursor
        return observed

    def read_logs() -> JSON | None:
        if not (set(scopes) & {"stdout", "stderr"}):
            return None
        logs: JSON = {}
        scan_limit = min(log_limit, MAX_OBSERVATION_SCAN_BYTES)
        for stream in ("stdout", "stderr"):
            if stream not in scopes:
                continue
            offset = log_offsets[stream]
            if settings.owner_session_id is not None:
                with OwnedSessionApiClient(definition=target, settings=settings) as client:
                    page = _owned_json(
                        client,
                        method="GET",
                        path=f"/jobs/{job_id}/logs/{stream}",
                        query={"offset": offset, "limit": scan_limit},
                        label=f"owned remote pattern {stream} log",
                    )
            else:
                page = _remote_json(
                    target,
                    [
                        "job",
                        "read-log",
                        job_id,
                        "--stream",
                        stream,
                        "--offset",
                        str(offset),
                        "--limit",
                        str(scan_limit),
                    ],
                    f"remote pattern {stream} log",
                )
            next_offset = page.get("next_offset")
            if not isinstance(next_offset, int):
                raise ValueError(f"remote pattern {stream} log returned an invalid offset")
            log_offsets[stream] = next_offset
            logs[stream] = page
        return logs

    return observe_until_pattern_snapshots(
        read_snapshot,
        compiled=compiled,
        scopes=scopes,
        log_reader=read_logs,
        include_logs=include_logs,
    )


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
                parsed_result = _verified_owned_mcp_result(
                    client,
                    job_id,
                    artifact_records,
                    require_result=_owned_mcp_result_is_required(source_job),
                )
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
    request_followup_message = _boolean_argument(
        arguments,
        "request_followup_message",
        default=False,
    )
    identity: dict[str, object] = {
        "cluster": cluster,
        "prompt_path": prompt_path,
        "mcp_config_path": mcp_config_path,
        "model": model,
        "workdir": workdir,
        "timeout_seconds": timeout_seconds,
    }
    if request_followup_message:
        identity["request_followup_message"] = True
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
                    parsed_result = _verified_owned_mcp_result(
                        client,
                        job.job_id,
                        artifacts,
                        require_result=_owned_mcp_result_is_required(job),
                    )
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


def _stage_builtin_jarvis_inputs(
    call_arguments: JSON,
    *,
    tool_name: str,
    cluster: str,
    queue: ClioCoreQueue,
    settings: RelaySettings,
    session: McpSessionState | None,
    requested_idempotency_key: object,
) -> JarvisInputPlan | None:
    """Engage the shared input-staging plane for one built-in JARVIS door call.

    The built-in door reaches the same clio-kit JARVIS contract as a registered
    route, so a package setting declared as a local file is staged, ingested, and
    rewritten identically. When the built-in JARVIS MCP runs on this host there is
    no machine boundary to cross: the configured path is already the path the
    package reads, so the plane stays out of the call.
    """
    definition = _remote_cluster_definition(cluster)
    if not should_execute_on_cluster(definition):
        return None
    requested_key = str(requested_idempotency_key) if requested_idempotency_key else None
    plan = prepare_jarvis_inputs(
        _object(call_arguments.get("arguments", {})),
        route=builtin_jarvis_staging_route(
            cluster=cluster,
            cluster_route_revision=_route_revision(definition),
            expected_server_artifact_digest=jarvis_mcp_artifact_binding(cluster),
            remote_tool_name=virtual_jarvis_remote_tool(tool_name),
        ),
        queue=queue,
        settings=settings,
        session=session,
        resolve_definition=_remote_cluster_definition,
        requested_idempotency_key=requested_key,
    )
    call_arguments["arguments"] = plan.arguments
    if plan.require_terminal_wait:
        call_arguments["wait_for_terminal"] = True
    merged_input_uses = merge_artifact_uses([], plan.automatic_artifact_uses)
    if merged_input_uses:
        call_arguments["used_artifact_refs"] = [
            artifact_use_payload(item) for item in merged_input_uses
        ]
    staged_idempotency_key = jarvis_submission_idempotency_key(
        plan,
        merged_artifact_uses=merged_input_uses,
        requested_idempotency_key=requested_key,
    )
    if staged_idempotency_key is not None:
        call_arguments["idempotency_key"] = staged_idempotency_key
    return plan


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

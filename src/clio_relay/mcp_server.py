"""Stdio MCP server for relay job submission tools."""

from __future__ import annotations

import json
import sys
from dataclasses import dataclass, field
from json import JSONDecodeError
from pathlib import Path
from typing import Any, TextIO, cast

from clio_relay import __version__
from clio_relay.cluster_config import (
    default_registry_path,
)
from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.errors import (
    NotFoundError,
    ObservationPatternError,
)
from clio_relay.filesystem_paths import logical_filesystem_text
from clio_relay.identifiers import (
    validate_durable_record_id,
)
from clio_relay.input_staging import (
    JarvisPackageInputContract,
    merge_artifact_uses,
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
from clio_relay.jarvis_mcp import (
    jarvis_cd_lock_binding_expectation as jarvis_cd_lock_binding_expectation,
)
from clio_relay.jarvis_mcp import jarvis_mcp_artifact_binding as jarvis_mcp_artifact_binding
from clio_relay.jarvis_mcp import jarvis_mcp_server as jarvis_mcp_server
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
from clio_relay.mcp_arguments import _artifact_use_cli_value as _artifact_use_cli_value
from clio_relay.mcp_arguments import (
    _object,
)
from clio_relay.mcp_arguments import _stable_digest as _stable_digest
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
from clio_relay.mcp_job_lifecycle import (
    # Re-exported (no longer called bare from this module): both are
    # reached only through mcp_dispatch.py's `_call_tool` back-reference
    # -- the `as <same name>` form keeps ruff's F401 from stripping an
    # import that looks unused within this file's own source but is not.
    _cancel_job as _cancel_job,
)
from clio_relay.mcp_job_lifecycle import _job_logs as _job_logs
from clio_relay.mcp_job_lifecycle import (
    _observe_job as _observe_job,
)
from clio_relay.mcp_job_lifecycle import (
    _wait_job,
)
from clio_relay.mcp_job_status import _status_job
from clio_relay.mcp_job_status import (
    # Re-exported (no longer called bare from this module): reached only
    # through mcp_dispatch.py's `_call_tool` back-reference.
    _used_artifacts_tool as _used_artifacts_tool,
)
from clio_relay.mcp_job_status import (
    _used_by_tool as _used_by_tool,
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
    _mcp_profile_from_env,
    _normalize_profile,
    _tool_definitions_and_remote_catalog,
    _validated_route_revision,
)
from clio_relay.mcp_remote_catalog import (
    _remote_mcp_catalog as _remote_mcp_catalog,
)
from clio_relay.mcp_remote_catalog import (
    _require_compatible_remote_mcp_catalog as _require_compatible_remote_mcp_catalog,
)
from clio_relay.mcp_remote_catalog import _route_revision as _route_revision
from clio_relay.mcp_remote_transport import _complete_local_artifacts as _complete_local_artifacts
from clio_relay.mcp_remote_transport import _complete_owned_collection as _complete_owned_collection
from clio_relay.mcp_remote_transport import (
    _complete_remote_collection as _complete_remote_collection,
)
from clio_relay.mcp_remote_transport import _owned_json as _owned_json
from clio_relay.mcp_remote_transport import _remote_json as _remote_json
from clio_relay.mcp_remote_transport import _validate_owned_job_status as _validate_owned_job_status
from clio_relay.mcp_result_verification import (
    _attach_terminal_mcp_evidence as _attach_terminal_mcp_evidence,
)
from clio_relay.mcp_result_verification import (
    # Re-exported (no longer called bare from this module): tests directly
    # call mcp_server_module._bounded_mcp_result(...) for inspection.
    _bounded_mcp_result as _bounded_mcp_result,
)
from clio_relay.mcp_result_verification import (
    # Re-exported (no longer called bare from this module): tests directly
    # call mcp_server_module._decode_verified_mcp_result(...) for inspection.
    _decode_verified_mcp_result as _decode_verified_mcp_result,
)
from clio_relay.mcp_result_verification import (
    _mcp_tool_result_failed,
)
from clio_relay.mcp_result_verification import (
    _render_remote_mcp_context as _render_remote_mcp_context,
)
from clio_relay.mcp_result_verification import (
    _verified_local_mcp_result as _verified_local_mcp_result,
)
from clio_relay.mcp_result_verification import _verified_mcp_result as _verified_mcp_result
from clio_relay.mcp_result_verification import (
    _verified_owned_mcp_result as _verified_owned_mcp_result,
)
from clio_relay.mcp_result_verification import _VerifiedMcpResult as _VerifiedMcpResult
from clio_relay.mcp_submission_agent import _submit_jarvis_job as _submit_jarvis_job
from clio_relay.mcp_submission_agent import _submit_jarvis_pipeline as _submit_jarvis_pipeline
from clio_relay.mcp_submission_agent import _submit_remote_agent as _submit_remote_agent
from clio_relay.mcp_submission_mcp_call import (
    _stage_builtin_jarvis_inputs as _stage_builtin_jarvis_inputs,
)
from clio_relay.mcp_submission_mcp_call import _submit_jarvis_mcp_call as _submit_jarvis_mcp_call
from clio_relay.mcp_submission_mcp_call import _submit_mcp_call as _submit_mcp_call
from clio_relay.mcp_submission_result import (
    # Re-exported (no longer called bare from this module): each is
    # directly monkeypatched by tests at mcp_server_module.<name>, and/or
    # reached only through another owner module's own
    # `_mcp_server.<name>(...)` back-reference (mcp_job_status.py's
    # `_job_target` and mcp_gateway_tools.py's `_bind_jarvis_runtime` both
    # call `_remote_cluster_definition` that way) -- the `as <same name>`
    # form keeps ruff's F401 from stripping an import that looks unused
    # within this file's own source but is not.
    _optional_cluster_definition as _optional_cluster_definition,
)
from clio_relay.mcp_submission_result import (
    _owned_session_submission_result as _owned_session_submission_result,
)
from clio_relay.mcp_submission_result import (
    _remote_cluster_definition as _remote_cluster_definition,
)
from clio_relay.mcp_submission_result import (
    _remote_mcp_submission_result as _remote_mcp_submission_result,
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
from clio_relay.mcp_tool_catalog import (
    static_mcp_tool_names,
)
from clio_relay.models import (
    ArtifactUse,
)
from clio_relay.relay_ops import wait_for_terminal as wait_for_terminal
from clio_relay.remote_cli import remove_remote_file as remove_remote_file
from clio_relay.remote_cli import run_remote_clio as run_remote_clio
from clio_relay.remote_cli import should_execute_on_cluster as should_execute_on_cluster
from clio_relay.remote_cli import write_remote_file as write_remote_file
from clio_relay.remote_mcp import (
    VirtualRemoteMcpCatalog,
    default_remote_mcp_cache_path,
    load_virtual_remote_mcp_catalog,
)

# Re-exported (no longer called bare from this module's own remaining
# ~750 lines): each is either directly monkeypatched by tests at
# mcp_server_module.<name>, read directly off mcp_server_module by tests
# (e.g. mcp_server_module._VerifiedMcpResult(...) construction), or
# reached only through another owner module's own
# `_mcp_server.<name>(...)` back-reference (mcp_dispatch.py's
# dispatcher above all). Found by two AST passes over every test file
# and every owner module (comprehensive_check.py /
# check_all_backref_targets.py) rather than manual tracing, given the
# volume the final submission-cluster slice moved out in one step. The
# `as <same name>` form keeps ruff's F401 from stripping an import that
# looks unused within this file's own source but is not.
from clio_relay.session_api import OwnedSessionApiClient as OwnedSessionApiClient
from clio_relay.session_api import submit_owned_session_job as submit_owned_session_job
from clio_relay.storage_runtime import (
    StorageAdmissionError,
    storage_managed_queue,
)
from clio_relay.validation_report import redact_sensitive_values

JSON = dict[str, Any]
MAX_INLINE_MCP_RESULT_BYTES = 65_536
MCP_RESULT_INLINE_LIMIT_CODE = "inline_result_limit_exceeded"
MCP_RESULT_INLINE_LIMIT_MESSAGE = (
    "The remote MCP operation reached a terminal state, but its result exceeded the safe "
    "inline response limit and is unavailable to the agent. Immutable private evidence was "
    "preserved for operator diagnosis. Remote side effects may have occurred; inspect the "
    "job before retrying."
)
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

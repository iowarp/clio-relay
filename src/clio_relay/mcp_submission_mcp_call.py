"""MCP-call job-submission tools: submitting a generic remote MCP tool
call (relay_submit_mcp_call), staging a built-in JARVIS package's inputs
before a virtual JARVIS MCP tool call, and the virtual-JARVIS-aware MCP
call submission (relay_call_jarvis_mcp / the registered-route path) that
builds on the generic one.

Split out of mcp_server.py (iowarp/clio-relay#231) as one of three seams
the job/MCP-call submission cluster split into (a single module would
have measured well over 800 lines; mcp_submission_result.py holds the
shared result-assembly helpers, mcp_submission_agent.py the
generic/pipeline/job/agent submission path). `_submit_mcp_call` itself is
directly monkeypatched by tests at `mcp_server_module.<name>`, and
`_submit_jarvis_mcp_call` calls it bare -- along with several other
monkeypatched names (`_remote_cluster_definition`,
`_optional_cluster_definition`, `jarvis_mcp_artifact_binding`,
`should_execute_on_cluster`, `submit_owned_session_job`,
`_owned_session_submission_result`, `write_remote_file`,
`run_remote_clio`, `remove_remote_file`, `jarvis_mcp_server`). Every one
of those call sites goes through the function-scope
`_mcp_server.<name>(...)` back-reference established in slices 3-8 --
found and rewritten by the same AST-based extraction script slice 8
introduced (exact line/column spans, not a hand-written per-function list
or a regex over the source text).
"""

from __future__ import annotations

import hashlib
import hmac
import json
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from pydantic import ValidationError

from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.input_staging import (
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
    jarvis_mcp_server_args,
    virtual_jarvis_remote_tool,
)
from clio_relay.mcp_arguments import (
    _artifact_use_cli_value,
    _artifact_use_refs,
    _log_limit,
    _object,
    _optional_int,
    _optional_str,
    _required_str,
    _stable_digest,
    _string_list,
    _string_mapping,
)
from clio_relay.mcp_remote_catalog import _route_revision
from clio_relay.mcp_submission_agent import _submit_local_job
from clio_relay.mcp_submission_result import (
    _remote_mcp_submission_result,
    _submission_result,
)
from clio_relay.models import (
    MCP_ADMISSION_AUTHORITY_METADATA_KEY,
    JarvisRunInputManifest,
    JobKind,
    McpAdmissionClass,
    McpCallSpec,
    McpControlQueryEvidence,
    McpOperation,
    RelayJob,
    artifact_use_payload,
)
from clio_relay.remote_cli import (
    staged_remote_cluster_registry,
)
from clio_relay.remote_mcp import (
    MAX_PINNED_CONTROL_QUERY_TIMEOUT_SECONDS,
    remote_mcp_registration_revision,
    resolve_pinned_mcp_admission,
    resolve_registered_remote_mcp_admission,
)

if TYPE_CHECKING:
    from clio_relay.mcp_server import McpSessionState

JSON = dict[str, Any]


def _submit_mcp_call(
    arguments: JSON,
    *,
    queue: ClioCoreQueue,
    settings: RelaySettings,
    pinned_jarvis: bool = False,
) -> JSON:
    from clio_relay import mcp_server as _mcp_server

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
        _mcp_server._remote_cluster_definition(cluster)
        if registered_route
        else _mcp_server._optional_cluster_definition(cluster)
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
    if definition is not None and _mcp_server.should_execute_on_cluster(definition):
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
            job = _mcp_server.submit_owned_session_job(
                definition=definition,
                settings=settings,
                path="/jobs/mcp-call",
                payload=payload,
            )
            return _mcp_server._owned_session_submission_result(
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
                _mcp_server.write_remote_file(
                    definition,
                    remote_args_path,
                    json.dumps(tool_arguments, sort_keys=True, separators=(",", ":")).encode(
                        "utf-8"
                    ),
                )
                output = _mcp_server.run_remote_clio(
                    definition,
                    remote_args,
                    cluster_registry_path=remote_registry_path,
                )
            finally:
                _mcp_server.remove_remote_file(
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
    from clio_relay import mcp_server as _mcp_server

    definition = _mcp_server._remote_cluster_definition(cluster)
    if not _mcp_server.should_execute_on_cluster(definition):
        return None
    requested_key = str(requested_idempotency_key) if requested_idempotency_key else None
    plan = prepare_jarvis_inputs(
        _object(call_arguments.get("arguments", {})),
        route=builtin_jarvis_staging_route(
            cluster=cluster,
            cluster_route_revision=_route_revision(definition),
            expected_server_artifact_digest=_mcp_server.jarvis_mcp_artifact_binding(cluster),
            remote_tool_name=virtual_jarvis_remote_tool(tool_name),
        ),
        queue=queue,
        settings=settings,
        session=session,
        resolve_definition=_mcp_server._remote_cluster_definition,
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
        settings=settings,
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
    from clio_relay import mcp_server as _mcp_server

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
        _mcp_server._remote_cluster_definition(cluster)
        if registered_route
        else _mcp_server._optional_cluster_definition(cluster)
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
        _mcp_server.jarvis_mcp_artifact_binding(cluster)
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
    if definition is not None and _mcp_server.should_execute_on_cluster(definition):
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
            job = _mcp_server.submit_owned_session_job(
                definition=definition,
                settings=settings,
                path="/jobs/jarvis-mcp-call",
                payload=payload,
            )
            return _mcp_server._owned_session_submission_result(
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
            _mcp_server.write_remote_file(
                definition,
                remote_args_path,
                json.dumps(tool_arguments, sort_keys=True, separators=(",", ":")).encode("utf-8"),
            )
            output = _mcp_server.run_remote_clio(definition, remote_args)
        finally:
            _mcp_server.remove_remote_file(definition, remote_args_path, remove_empty_parent=True)
        return _remote_mcp_submission_result(
            output,
            definition=definition,
            arguments=arguments,
        )
    server = _mcp_server.jarvis_mcp_server()
    server_args = jarvis_mcp_server_args()
    forwarded["server"] = server
    forwarded["server_args"] = server_args
    return _mcp_server._submit_mcp_call(
        forwarded,
        queue=queue,
        settings=settings,
        pinned_jarvis=True,
    )

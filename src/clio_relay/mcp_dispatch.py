"""MCP tool-call dispatcher: `_call_tool`, the 43-branch router from a
relay MCP tool name to its business-logic implementation.

Split out of mcp_server.py (iowarp/clio-relay#231) per
docs/design/relay-architecture-2026-08.md's own "mcp_server.py's tool
catalog + dispatcher" owner-module row (§4.5/§5) -- the catalog half moved
in an earlier slice to mcp_tool_catalog.py; this slice moves the
dispatcher half.

The business-logic functions the dispatcher routes to (job submission,
status/cancel/observe/wait, queue tools, gateway-session tools, ...) stay
defined in mcp_server.py -- several are directly monkeypatched by tests at
`mcp_server_module.<name>`, and moving them is a separate, larger slice.
Calling them from here at module scope is also not possible regardless of
monkeypatching: mcp_server.py itself imports `_call_tool` from this module,
so a module-level `from clio_relay.mcp_server import _wait_job, ...` here
would be a load-order cycle. Both problems have the same proven fix (the
split recipe's function-scope back-reference idiom): every call to a name
that still lives in mcp_server.py goes through `_mcp_server.<name>(...)`,
where `_mcp_server` is imported inside `_call_tool`'s own body (not at
module top) via `from clio_relay import mcp_server as _mcp_server`. That
import only executes the first time `_call_tool` is actually invoked, by
which point mcp_server.py has always finished loading (nothing can call
`_call_tool` before its own home module, mcp_server.py, exists), and it
resolves `_mcp_server.<name>` by attribute lookup on the live module object
-- exactly what `monkeypatch.setattr(mcp_server_module, "<name>", fake)`
mutates, so every existing patch keeps working unchanged.

Names already extracted to their own leaf modules in earlier slices
(argument coercion in mcp_arguments.py, the tool catalog/authorization
helpers in mcp_tool_catalog.py) are imported normally at module top here,
same as any other external dependency -- they are leaves with no
back-reference to mcp_server.py, so there is no cycle to avoid.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from uuid import uuid4

from clio_relay import artifact_routing
from clio_relay.input_staging import REGISTERED_JARVIS_CONTRACT_ID, merge_artifact_uses
from clio_relay.jarvis_input_plane import (
    JarvisInputPlan,
    JarvisStagingRoute,
    jarvis_submission_idempotency_key,
    prepare_jarvis_inputs,
    record_jarvis_inputs,
)
from clio_relay.jarvis_mcp import is_virtual_jarvis_tool, virtual_jarvis_call_arguments
from clio_relay.mcp_arguments import (
    _artifact_use_refs,
    _bounded_integer_limit,
    _job_log_limit,
    _object,
    _optional_datetime_argument,
    _optional_durable_record_id,
    _optional_str,
    _record_page,
    _required_durable_record_id,
    _required_str,
    _response_page_cursor,
    _response_page_limit,
)
from clio_relay.mcp_tool_catalog import _authorized_static_tool_names, static_mcp_tool_names
from clio_relay.models import Cursor, artifact_use_payload
from clio_relay.public_records import public_gateway_session
from clio_relay.relay_ops import (
    evaluate_monitor_rules,
    job_status,
    monitor_job,
    read_job_log,
)
from clio_relay.remote_mcp import VirtualRemoteMcpCatalog, is_remote_mcp_control_query
from clio_relay.retention import TerminalRetentionCoordinator
from clio_relay.storage_runtime import StorageManagedQueue

if TYPE_CHECKING:
    from clio_relay.config import RelaySettings
    from clio_relay.core_queue import ClioCoreQueue
    from clio_relay.mcp_server import McpSessionState

JSON = dict[str, Any]


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
    from clio_relay import mcp_server as _mcp_server

    name = _required_str(params, "name")
    static_names = static_mcp_tool_names()
    catalog: VirtualRemoteMcpCatalog | None = None
    if name in static_names:
        if name not in _authorized_static_tool_names(profile):
            raise ValueError(f"tool is not available in MCP profile {profile!r}: {name}")
    else:
        catalog = _mcp_server._remote_mcp_catalog(profile=profile, reserved_names=static_names)
        _mcp_server._require_compatible_remote_mcp_catalog(
            profile=profile,
            observed_revision=observed_remote_mcp_catalog_revision,
            current_revision=catalog.revision,
        )
        if name not in catalog.tools:
            raise ValueError(f"tool is not available in MCP profile {profile!r}: {name}")
    if is_virtual_jarvis_tool(name):
        catalog = _mcp_server._remote_mcp_catalog(profile=profile, reserved_names=static_names)
        if require_advertised_remote_mcp_catalog:
            _mcp_server._require_compatible_remote_mcp_catalog(
                profile=profile,
                observed_revision=observed_remote_mcp_catalog_revision,
                current_revision=catalog.revision,
            )
    arguments = _object(params.get("arguments", {}))
    arguments = _mcp_server._restore_session_remote_job_route(
        name=name,
        arguments=arguments,
        queue=queue,
        session=session,
    )
    if name == "relay_submit_jarvis_pipeline":
        result = _mcp_server._submit_jarvis_pipeline(arguments, queue=queue, settings=settings)
    elif name == "relay_storage_status":
        if not isinstance(queue, StorageManagedQueue):
            raise ValueError("MCP queue is not storage managed")
        result = queue.storage_runtime.status()
    elif name == "relay_remote_mcp_context":
        catalog = _mcp_server._remote_mcp_catalog(
            profile=profile, reserved_names=static_mcp_tool_names()
        )
        result = {
            "context": _mcp_server._render_remote_mcp_context(catalog),
            "catalog_revision": catalog.revision,
            "virtual_remote_tools": sorted(catalog.tools),
            "catalog_issues": [issue.model_dump(mode="json") for issue in catalog.issues],
        }
    elif name == "relay_submit_agent":
        result = _mcp_server._submit_remote_agent(arguments, queue=queue, settings=settings)
    elif name == "relay_status":
        result = _mcp_server._status_job(arguments, queue=queue, settings=settings)
    elif name == "relay_cancel":
        result = _mcp_server._cancel_job(arguments, queue=queue, settings=settings)
    elif name == "relay_observe":
        result = _mcp_server._observe_job(arguments, queue=queue, settings=settings)
    elif name == "relay_wait":
        result = _mcp_server._wait_job(arguments, queue=queue, settings=settings)
    elif name == "relay_submit_jarvis_job":
        result = _mcp_server._submit_jarvis_job(arguments, queue=queue, settings=settings)
    elif name == "relay_submit_remote_agent":
        result = _mcp_server._submit_remote_agent(arguments, queue=queue, settings=settings)
    elif name == "relay_submit_mcp_call":
        result = _mcp_server._submit_mcp_call(arguments, queue=queue, settings=settings)
    elif name == "relay_call_jarvis_mcp":
        result = _mcp_server._submit_jarvis_mcp_call(arguments, queue=queue, settings=settings)
    elif is_virtual_jarvis_tool(name):
        call_arguments = virtual_jarvis_call_arguments(name, arguments)
        cluster = _required_str(call_arguments, "cluster")
        if require_advertised_remote_mcp_catalog:
            if catalog is None:
                raise ValueError("JARVIS virtual tool catalog was not resolved")
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
        builtin_plan = _mcp_server._stage_builtin_jarvis_inputs(
            call_arguments,
            tool_name=name,
            cluster=cluster,
            queue=queue,
            settings=settings,
            session=session,
            requested_idempotency_key=arguments.get("idempotency_key"),
        )
        result = _mcp_server._submit_jarvis_mcp_call(call_arguments, queue=queue, settings=settings)
        if builtin_plan is not None:
            record_jarvis_inputs(result, plan=builtin_plan, queue=queue, session=session)
        if catalog is None:
            raise ValueError("JARVIS virtual tool catalog was not resolved")
        result["catalog_revision"] = catalog.revision
    elif catalog is not None and name in catalog.tools:
        cluster = _required_str(arguments, "cluster")
        virtual_tool = catalog.tools[name]
        route = catalog.resolve(name, cluster)
        forwarded_arguments = catalog.forwarded_arguments(name, arguments)
        relay_arguments = catalog.relay_arguments(name, arguments)
        raw_requested_key = relay_arguments.get("idempotency_key")
        requested_idempotency_key = str(raw_requested_key) if raw_requested_key else None
        input_plan: JarvisInputPlan | None = None
        if route.contract == REGISTERED_JARVIS_CONTRACT_ID:
            if session is None:
                raise ValueError("registered JARVIS package semantics require an MCP session")
            input_plan = prepare_jarvis_inputs(
                forwarded_arguments,
                route=JarvisStagingRoute(
                    cluster=cluster,
                    server_name=route.server_name,
                    cluster_route_revision=route.cluster_route_revision,
                    registration_revision=route.registration_revision,
                    expected_server_artifact_digest=route.expected_server_artifact_digest,
                    remote_tool_name=route.remote_tool_name,
                    carries_run_input_manifest=True,
                    reconciles_every_description=True,
                ),
                queue=queue,
                settings=settings,
                session=session,
                resolve_definition=_mcp_server._remote_cluster_definition,
                requested_idempotency_key=requested_idempotency_key,
            )
            forwarded_arguments = input_plan.arguments
            if input_plan.require_terminal_wait:
                relay_arguments["wait_for_terminal"] = True
        automatic_input_uses = () if input_plan is None else input_plan.automatic_artifact_uses
        run_input_manifest = None if input_plan is None else input_plan.run_input_manifest
        merged_input_uses = merge_artifact_uses(
            _artifact_use_refs(relay_arguments),
            automatic_input_uses,
        )
        if merged_input_uses:
            relay_arguments["used_artifact_refs"] = [
                artifact_use_payload(item) for item in merged_input_uses
            ]
        staged_idempotency_key = (
            None
            if input_plan is None
            else jarvis_submission_idempotency_key(
                input_plan,
                merged_artifact_uses=merged_input_uses,
                requested_idempotency_key=requested_idempotency_key,
            )
        )
        base_idempotency_key = staged_idempotency_key or str(
            requested_idempotency_key
            or (f"mcp:virtual:{cluster}:{route.server_name}:{route.remote_tool_name}:{uuid4().hex}")
        )
        result = _mcp_server._submit_mcp_call(
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
        if input_plan is not None:
            record_jarvis_inputs(result, plan=input_plan, queue=queue, session=session)
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
        result = _mcp_server._record_task_event(arguments, queue=queue)
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
        list_artifacts_target = _mcp_server._job_target(arguments)
        result = artifact_routing.list_artifacts(
            arguments,
            queue=queue,
            settings=settings,
            target=list_artifacts_target,
        )
        if list_artifacts_target is not None:
            result["cluster"] = list_artifacts_target.name
            result["route_revision"] = _mcp_server._route_revision(list_artifacts_target)
    elif name == "relay_artifact_lineage":
        has_job = arguments.get("job_id") is not None
        has_artifact = arguments.get("artifact_id") is not None
        if has_job == has_artifact:
            raise ValueError("pass exactly one of job_id or artifact_id")
        result = (
            _mcp_server._used_artifacts_tool(arguments, queue=queue, settings=settings)
            if has_job
            else _mcp_server._used_by_tool(arguments, queue=queue, settings=settings)
        )
    elif name == "relay_read_artifact":
        read_artifact_target = _mcp_server._job_target(arguments)
        result = artifact_routing.read_artifact(
            arguments,
            queue=queue,
            settings=settings,
            target=read_artifact_target,
        )
        if read_artifact_target is not None:
            result["cluster"] = read_artifact_target.name
            result["route_revision"] = _mcp_server._route_revision(read_artifact_target)
    elif name == "relay_record_progress":
        result = _mcp_server._record_progress(arguments, queue=queue)
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
        result = _mcp_server._queue_cancel_tool(arguments, queue=queue, settings=settings)
    elif name == "relay_queue_list":
        result = _mcp_server._queue_list_tool(arguments, queue=queue, settings=settings)
    elif name == "relay_queue_diagnose":
        result = _mcp_server._queue_diagnose_tool(arguments, queue=queue, settings=settings)
    elif name == "relay_queue_stale":
        result = _mcp_server._queue_stale_tool(arguments, queue=queue, settings=settings)
    elif name == "relay_queue_cleanup_stale":
        result = _mcp_server._queue_cleanup_stale_tool(arguments, queue=queue, settings=settings)
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
        result = _mcp_server._worker_status_tool(arguments, queue=queue)
    elif name == "relay_create_monitor_rule":
        rule = _mcp_server._monitor_rule_from_arguments(arguments)
        result = queue.append_monitor_rule(rule).model_dump(mode="json")
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
        result = _mcp_server._bind_jarvis_runtime(arguments, queue=queue, settings=settings)
    elif name == "relay_create_gateway_session":
        result = _mcp_server._create_gateway_session(arguments, queue=queue)
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
        result = _mcp_server._update_gateway_session(arguments, queue=queue)
    elif name == "relay_close_gateway_session":
        result = public_gateway_session(
            queue.close_gateway_session(_required_durable_record_id(arguments, "session_id"))
        )
    else:
        raise ValueError(f"unknown tool: {name}")
    if session is not None:
        session.observe_remote_job_result(result)
    return {
        "content": [{"type": "text", "text": _mcp_server._serialize_tool_result(result)}],
        "structuredContent": result,
        "isError": _mcp_server._mcp_tool_result_failed(result),
    }

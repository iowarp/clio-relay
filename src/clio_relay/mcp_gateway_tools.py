"""Gateway-session MCP tools: monitor-rule construction, progress/task-event
recording, durable gateway-service session lifecycle (create/update), and
binding a verified JARVIS service runtime to one.

Split out of mcp_server.py (iowarp/clio-relay#231). Two names
`_bind_jarvis_runtime` calls are directly monkeypatched by tests at
`mcp_server_module.<name>`: `_remote_cluster_definition` (which also stays
defined in mcp_server.py -- many other functions there call it too) and
`resolve_jarvis_service_runtime` (imported from clio_relay.jarvis_service_runtime
everywhere else, but reached here through the back-reference specifically
because of the monkeypatch, not a cycle). Both go through the
function-scope `_mcp_server.<name>(...)` back-reference established in
slices 3-5 (`from clio_relay import mcp_server as _mcp_server`, imported
inside the function body, not at module top, to avoid the load-order
cycle a module-level back-reference would create). mcp_server.py
re-exports `resolve_jarvis_service_runtime` purely so that monkeypatch
target resolves -- nothing there calls it bare any more.

`_jarvis_runtime_binding_selectors`, `_reject_generic_gateway_runtime_fields`,
and `_required_environment_secret` are used only within this cluster
(confirmed by grep before the move), so they move as plain same-module
bare calls -- no back-reference, no cycle.
"""

from __future__ import annotations

import os
from typing import Any, cast

from pydantic import ValidationError

from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.errors import ConfigurationError
from clio_relay.identifiers import validate_durable_record_id
from clio_relay.jarvis_service_runtime import JarvisServiceRuntimeHandoff
from clio_relay.mcp_arguments import (
    _object,
    _optional_float,
    _optional_int,
    _optional_str,
    _positive_float_argument,
    _required_durable_record_id,
    _required_str,
    _string_list,
)
from clio_relay.models import (
    GatewaySession,
    GatewaySessionState,
    MonitorRule,
    MonitorRuleAction,
    ProgressRecord,
    TaskEventStatus,
    TaskTimelineEvent,
)
from clio_relay.owner_session_admission import owner_session_gateway_admission
from clio_relay.progress_provenance import external_progress_metadata
from clio_relay.public_records import public_gateway_session
from clio_relay.service_runtime import ServiceRuntimePendingResult, ServiceRuntimeSupervisor

JSON = dict[str, Any]


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
    from clio_relay import mcp_server as _mcp_server

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
    definition = _mcp_server._remote_cluster_definition(cluster)
    verified = _mcp_server.resolve_jarvis_service_runtime(
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


def _required_environment_secret(name: str, label: str) -> str:
    """Resolve one configured transport secret without exposing it in records."""
    value = os.environ.get(name)
    if value is None or not value:
        raise ValueError(f"{label} is required in environment variable {name}")
    return value

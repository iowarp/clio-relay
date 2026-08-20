"""The v3.6 secure runtime acceptance lifecycle: query, bind, browser, teardown.

Extracted from ``live_acceptance.py`` (#231 rework): the single audited
state machine exercising one authenticated JARVIS service end to end --
query for a ready service-runtime handoff, bind private authority through
an isolated packaged-MCP child, exercise the four authenticated browser
surfaces across a detach/reconnect cycle, revoke every browser capability
issued, and always tear the gateway down (even on partial failure) before
proving the entire evidence trail is secret-free.

This stays one function: every phase depends on state an earlier phase
produced, and the ``try/except/finally`` wrapping all of it needs most of
that same state to choose correct cleanup on partial failure. Every
phase's real work -- packaged-MCP evidence, handoff/bind validation,
browser observation/correlation, secret-free enforcement, and (the
module's own real seam) failure cleanup itself
(``live_acceptance_handoff._cleanup_secure_runtime_failure``) -- already
lives in its owner module and is only called from here. Splitting the
orchestration further would mean threading its dozen-plus mutable locals
through a context object passed to every phase alike: a real redesign,
not a slice, adding indirection to a security-critical proof without
reducing its actual complexity.
"""

from __future__ import annotations

import time
from typing import Any, Literal, cast

import clio_relay.live_acceptance_browser_evidence as live_acceptance_browser_evidence
from clio_relay.browser_gateway import BrowserAttachmentGrant
from clio_relay.config import RelaySettings
from clio_relay.errors import ObservationTimeoutError, RelayError
from clio_relay.jarvis_service_runtime import JarvisServiceRuntimeHandoff
from clio_relay.live_acceptance_browser_evidence import (
    _assert_browser_capability_revoked,
    _browser_attachment_capability,
    _browser_evidence_reference,
    _correlate_secure_runtime_browser_document,
    _wait_for_changed_browser_state,
    _wait_for_changed_sse_event,
)
from clio_relay.live_acceptance_handoff import (
    _cleanup_secure_runtime_failure,
    _gateway_sessions_for_acceptance,
    _query_source_artifact_sha256,
    _secure_runtime_cleanup_candidate,
    _select_secure_runtime_handoff,
    _validated_secure_runtime_bind,
    _validated_secure_runtime_pending_bind,
)
from clio_relay.live_acceptance_models import (
    LiveAcceptanceOptions,
    PackagedMcpAcceptanceEvidence,
    SecureRuntimeAcceptanceEvidence,
    SecureRuntimeHttpEvidence,
    SecureRuntimeProbeConfig,
    _AcceptanceObservationPending,
    _secure_runtime_canonical_json_sha256,
    _secure_runtime_json_pointer_value,
)
from clio_relay.live_acceptance_packaged_mcp import (
    _configured_runtime_secret,
    _isolated_runtime_child_environment,
    _packaged_mcp_acceptance_evidence,
    _packaged_mcp_structured_result,
    _validation_check,
)
from clio_relay.live_acceptance_secret_redaction import (
    _assert_secret_free_document,
    _record_runtime_cleanup,
    _redact_exception_values,
    _validate_secure_runtime_cleanup,
)
from clio_relay.mcp_stdio_validation import run_packaged_mcp_stdio_session
from clio_relay.models import GatewaySessionState
from clio_relay.public_records import public_gateway_session
from clio_relay.service_runtime import ServiceRuntimeStopResult, ServiceRuntimeSupervisor
from clio_relay.storage_runtime import StorageManagedQueue, storage_managed_queue
from clio_relay.validation_report import (
    EvidenceReference,
    ValidationRecorder,
    ValidationResource,
    redact_sensitive_values,
)


def _verify_secure_runtime_acceptance(
    options: LiveAcceptanceOptions,
    *,
    config: SecureRuntimeProbeConfig,
    runtime_metadata: dict[str, Any],
    recorder: ValidationRecorder,
) -> set[str]:
    """Exercise one authenticated JARVIS service through bind, browser, and cleanup."""
    pipeline_id = runtime_metadata.get("pipeline_id")
    execution_id = runtime_metadata.get("execution_id")
    if not isinstance(pipeline_id, str) or not pipeline_id:
        raise RelayError("secure runtime metadata omitted pipeline_id")
    if not isinstance(execution_id, str) or not execution_id:
        raise RelayError("secure runtime metadata omitted execution_id")

    token = _configured_runtime_secret(
        explicit=options.transport_token,
        environment_name=options.definition.frp_transport.token_env,
        label="frp token",
    )
    secret_key = _configured_runtime_secret(
        explicit=options.transport_secret_key,
        environment_name=options.definition.frp_transport.stcp_secret_env,
        label="stcp secret",
    )
    forbidden_values = {token, secret_key}
    public_documents: list[object] = []
    gateway_session_id: str | None = None
    active_attachment: BrowserAttachmentGrant | None = None
    teardown_complete = False
    browser_observations: list[SecureRuntimeHttpEvidence] = []
    attachment_ids: list[str] = []
    revoked_grants: list[tuple[BrowserAttachmentGrant, bool]] = []
    lifecycle_states: list[Literal["ready", "degraded", "closed"]] = []
    supervisor: ServiceRuntimeSupervisor | None = None
    runtime_queue: StorageManagedQueue | None = None
    baseline_gateway_session_ids: set[str] | None = None
    handoff: JarvisServiceRuntimeHandoff | None = None
    teardown_result: ServiceRuntimeStopResult | None = None

    primary_error: Exception | None = None
    try:
        with _validation_check(
            recorder,
            "secure-runtime.jarvis-v3.6-query",
            "query one execution-owned service through the pinned JARVIS v3.6 contract",
            forbidden_values=forbidden_values,
        ) as evidence:
            query_deadline = time.monotonic() + options.timeout_seconds
            query_attempt = 0
            first_query_identity: PackagedMcpAcceptanceEvidence | None = None
            handoff: JarvisServiceRuntimeHandoff | None = None

            def _query_timeout_pending(message: str) -> _AcceptanceObservationPending:
                return _AcceptanceObservationPending(
                    f"{message}: {execution_id}",
                    phase="secure_runtime_query",
                    identifiers={"pipeline_id": pipeline_id, "execution_id": execution_id},
                )

            while True:
                remaining = query_deadline - time.monotonic()
                if remaining <= 0:
                    raise _query_timeout_pending(
                        "timed out waiting for one ready JARVIS service runtime binding"
                    )
                query_attempt += 1
                try:
                    query_session = run_packaged_mcp_stdio_session(
                        profile="user",
                        tool="jarvis_get_execution",
                        arguments={
                            "cluster": options.cluster,
                            "pipeline_id": pipeline_id,
                            "execution_id": execution_id,
                            "include_service_runtimes": True,
                            "wait_for_terminal": True,
                            "wait_timeout_seconds": remaining,
                            "poll_seconds": options.poll_seconds,
                        },
                        timeout_seconds=remaining + 30.0,
                        require_enforceable_containment=True,
                    )
                except (ObservationTimeoutError, TimeoutError):
                    raise _query_timeout_pending(
                        "timed out observing one ready JARVIS service runtime binding"
                    ) from None
                if time.monotonic() >= query_deadline:
                    raise _query_timeout_pending(
                        "timed out waiting for one ready JARVIS service runtime binding"
                    )
                query_result = _packaged_mcp_structured_result(
                    query_session,
                    expected_tool="jarvis_get_execution",
                )
                query_mcp_evidence = _packaged_mcp_acceptance_evidence(
                    query_session,
                    expected_tool="jarvis_get_execution",
                )
                if first_query_identity is None:
                    first_query_identity = query_mcp_evidence
                elif query_mcp_evidence != first_query_identity:
                    raise RelayError(
                        "packaged MCP identity changed while waiting for service readiness"
                    )
                public_documents.append(query_result)
                candidate_handoff = _select_secure_runtime_handoff(
                    query_result,
                    cluster=options.cluster,
                    config=config,
                )
                if candidate_handoff is not None:
                    handoff = candidate_handoff
                    break
                evidence.append(
                    EvidenceReference(
                        kind="packaged_mcp_stdio",
                        reference=(
                            f"packaged-mcp://jarvis_get_execution/readiness-attempt/{query_attempt}"
                        ),
                        excerpt="execution query returned no ready service runtime binding",
                        metadata={
                            **query_mcp_evidence.model_dump(mode="json"),
                            "pipeline_id": pipeline_id,
                            "execution_id": execution_id,
                            "ready_binding_count": 0,
                        },
                    )
                )
                remaining = query_deadline - time.monotonic()
                if remaining <= 0:
                    raise _query_timeout_pending(
                        "timed out waiting for one ready JARVIS service runtime binding"
                    )
                time.sleep(min(options.poll_seconds, remaining))

            assert handoff is not None
            source_artifact_sha256 = _query_source_artifact_sha256(
                query_result,
                handoff=handoff,
            )
            evidence.append(
                EvidenceReference(
                    kind="packaged_mcp_stdio",
                    reference=(
                        f"relay-job://{handoff.cluster}/{handoff.source_job_id}/"
                        f"{handoff.source_artifact_id}"
                    ),
                    sha256=source_artifact_sha256,
                    metadata={
                        **query_mcp_evidence.model_dump(mode="json"),
                        "pipeline_id": pipeline_id,
                        "execution_id": execution_id,
                    },
                )
            )
            recorder.add_resource(
                ValidationResource(
                    kind="relay_job",
                    resource_id=handoff.source_job_id,
                    role="secure_runtime_query",
                    cluster=options.cluster,
                    state="succeeded",
                )
            )
            recorder.add_resource(
                ValidationResource(
                    kind="artifact",
                    resource_id=handoff.source_artifact_id,
                    role="private_mcp_result",
                    cluster=options.cluster,
                    metadata={"sha256": source_artifact_sha256, "model_readable": False},
                )
            )

        with _isolated_runtime_child_environment(
            token_name=options.definition.frp_transport.token_env,
            token=token,
            secret_name=options.definition.frp_transport.stcp_secret_env,
            secret=secret_key,
        ) as runtime_child_environment:
            settings = RelaySettings.from_env()
            runtime_queue = storage_managed_queue(settings)
            baseline_gateway_session_ids = {
                session.session_id
                for session in _gateway_sessions_for_acceptance(
                    runtime_queue,
                    cluster=options.cluster,
                )
            }
            supervisor = ServiceRuntimeSupervisor(
                settings=settings,
                queue=runtime_queue,
                cluster=options.cluster,
                definition=options.definition,
                token=token,
                secret_key=secret_key,
            )
            with _validation_check(
                recorder,
                "secure-runtime.private-authority-bind",
                "resolve exact private authority and bind authenticated relay connectors",
                forbidden_values=forbidden_values,
            ) as evidence:
                try:
                    bind_session = run_packaged_mcp_stdio_session(
                        profile="user",
                        tool="relay_bind_jarvis_runtime",
                        arguments={
                            "binding": handoff.model_dump(mode="json"),
                            "readiness_timeout_seconds": options.timeout_seconds,
                            "poll_seconds": options.poll_seconds,
                        },
                        timeout_seconds=options.timeout_seconds + 30.0,
                        extra_environment=runtime_child_environment,
                        require_enforceable_containment=True,
                    )
                except (ObservationTimeoutError, TimeoutError):
                    raise _AcceptanceObservationPending(
                        "timed out observing the exact secure runtime bind",
                        phase="secure_runtime_bind",
                        identifiers={
                            "pipeline_id": pipeline_id,
                            "execution_id": execution_id,
                            "source_job_id": handoff.source_job_id,
                            "source_artifact_id": handoff.source_artifact_id,
                            "service_instance_id": handoff.service_instance_id,
                        },
                    ) from None
                bind_result = _packaged_mcp_structured_result(
                    bind_session,
                    expected_tool="relay_bind_jarvis_runtime",
                )
                bind_mcp_evidence = _packaged_mcp_acceptance_evidence(
                    bind_session,
                    expected_tool="relay_bind_jarvis_runtime",
                )
                if (
                    bind_mcp_evidence.canonical_executable
                    != query_mcp_evidence.canonical_executable
                    or bind_mcp_evidence.executable_sha256 != query_mcp_evidence.executable_sha256
                    or bind_mcp_evidence.jarvis_virtual_tools_sha256
                    != query_mcp_evidence.jarvis_virtual_tools_sha256
                ):
                    raise RelayError("packaged MCP identity changed between query and bind")
                public_documents.append(bind_result)
                if bind_result.get("outcome") == "pending":
                    gateway_session_id = _validated_secure_runtime_pending_bind(
                        bind_result,
                        handoff=handoff,
                    )
                    raise _AcceptanceObservationPending(
                        "secure runtime bind remained pending at the observation boundary",
                        phase="secure_runtime_bind",
                        identifiers={
                            "pipeline_id": pipeline_id,
                            "execution_id": execution_id,
                            "source_job_id": handoff.source_job_id,
                            "source_artifact_id": handoff.source_artifact_id,
                            "service_instance_id": handoff.service_instance_id,
                            "gateway_session_id": gateway_session_id,
                        },
                    )
                gateway_session_id = _secure_runtime_cleanup_candidate(
                    bind_result,
                    handoff=handoff,
                )
                validated_session_id, binding = _validated_secure_runtime_bind(
                    bind_result,
                    handoff=handoff,
                    expected_execution_id=execution_id,
                    expected_source_artifact_sha256=source_artifact_sha256,
                )
                if validated_session_id != gateway_session_id:
                    raise RelayError("secure runtime bind changed its cleanup identity")
                gateway = cast(dict[str, Any], bind_result["gateway_session"])
                public_documents.append(gateway)
                lifecycle_states.append("ready")
                evidence.append(
                    EvidenceReference(
                        kind="private_authority_resolution",
                        reference=f"gateway-runtime://{options.cluster}/{gateway_session_id}",
                        sha256=cast(str, binding.authorization_sha256),
                        metadata={
                            "resolver_identity_complete": True,
                            "pipeline_id": pipeline_id,
                            "execution_id": binding.jarvis_execution_id,
                            "package_id": binding.package_id,
                            "service_instance_id": binding.service_instance_id,
                            "service_revision": binding.service_revision,
                            "raw_authority_material_in_public_evidence": False,
                        },
                    )
                )
                recorder.add_resource(
                    ValidationResource(
                        kind="secure_runtime_binding",
                        resource_id=(f"{gateway_session_id}:revision:{binding.service_revision}"),
                        role="private_authority_bind",
                        cluster=options.cluster,
                        state="ready",
                        metadata={
                            "binding_schema_version": binding.schema_version,
                            "evidence_scope": ("clio-relay-core-lifecycle-and-public-evidence"),
                            "service_runtime_schema_version": (
                                binding.service_runtime_schema_version
                            ),
                            "source_relay_job_id": binding.source_relay_job_id,
                            "source_relay_artifact_id": binding.source_relay_artifact_id,
                            "source_relay_artifact_sha256": (binding.source_relay_artifact_sha256),
                            "jarvis_execution_id": binding.jarvis_execution_id,
                            "package_id": binding.package_id,
                            "package_name": binding.package_name,
                            "service_instance_id": binding.service_instance_id,
                            "service_revision": binding.service_revision,
                            "authorization_sha256": binding.authorization_sha256,
                            "dataset_descriptor_sha256": (binding.dataset_descriptor_sha256),
                            "query_mcp_containment_mode": query_mcp_evidence.containment_mode,
                            "query_mcp_containment_enforceable": (
                                query_mcp_evidence.containment_enforceable
                            ),
                            "bind_mcp_containment_mode": bind_mcp_evidence.containment_mode,
                            "bind_mcp_containment_enforceable": (
                                bind_mcp_evidence.containment_enforceable
                            ),
                        },
                    )
                )

            with _validation_check(
                recorder,
                "secure-runtime.browser-protocol",
                "exercise authenticated health, state, command, and SSE browser surfaces",
                forbidden_values=forbidden_values,
            ) as evidence:
                command_id = cast(
                    str,
                    _secure_runtime_json_pointer_value(
                        config.command,
                        config.protocol_adapter.command_request_id_pointer,
                        label="command request identity",
                    ),
                )
                event_name = cast(str, config.protocol_adapter.events.event_name)
                active_attachment = supervisor.browser_attach(
                    session_id=gateway_session_id,
                    ttl_seconds=config.browser_attachment_ttl_seconds,
                )
                attachment_ids.append(active_attachment.attachment_id)
                browser_capability = _browser_attachment_capability(active_attachment)
                forbidden_values.add(browser_capability)
                initial_health, initial_state, initial_event, initial_revisions = (
                    live_acceptance_browser_evidence._observe_correlated_browser_triad(  # pyright: ignore[reportPrivateUsage]  # noqa: E501, SLF001
                        active_attachment,
                        config=config,
                        binding=binding,
                        health_command_id=None,
                        state_command_id=None,
                        event_command_id=None,
                        event_name=event_name,
                        timeout_seconds=min(options.timeout_seconds, 60.0),
                    )
                )
                if initial_revisions != {binding.service_revision}:
                    raise RelayError("secure runtime initial surfaces changed binding revision")
                command_observation, command_response = (
                    live_acceptance_browser_evidence._browser_json_observation(  # pyright: ignore[reportPrivateUsage]  # noqa: E501, SLF001
                        active_attachment.command_url,
                        endpoint="command",
                        method="POST",
                        body=config.command,
                        timeout_seconds=min(options.timeout_seconds, 60.0),
                    )
                )
                command_observation, command_revision = _correlate_secure_runtime_browser_document(
                    command_response,
                    command_observation,
                    endpoint="command",
                    adapter=config.protocol_adapter.command,
                    expected_service_instance_id=binding.service_instance_id,
                    expected_execution_id=binding.jarvis_execution_id,
                    expected_dataset_descriptor_sha256=binding.dataset_descriptor_sha256,
                    expected_command_id=command_id,
                )
                # initial_revisions == {binding.service_revision} was just proven above, so
                # binding.service_revision stands in for the discarded initial_state_revision.
                if command_revision <= binding.service_revision:
                    raise RelayError("secure runtime command did not advance service revision")
                changed_event, changed_event_document = _wait_for_changed_sse_event(
                    active_attachment.events_url,
                    previous=initial_event,
                    require_change=config.require_sse_change,
                    timeout_seconds=min(options.timeout_seconds, 60.0),
                    poll_seconds=options.poll_seconds,
                    expected_event_name=event_name,
                )
                changed_event, changed_event_revision = _correlate_secure_runtime_browser_document(
                    changed_event_document,
                    changed_event,
                    endpoint="events",
                    adapter=config.protocol_adapter.events,
                    expected_service_instance_id=binding.service_instance_id,
                    expected_execution_id=binding.jarvis_execution_id,
                    expected_dataset_descriptor_sha256=binding.dataset_descriptor_sha256,
                    expected_command_id=command_id,
                )
                changed_state, changed_state_document = _wait_for_changed_browser_state(
                    active_attachment.state_url,
                    previous=initial_state,
                    require_change=config.require_state_change,
                    timeout_seconds=min(options.timeout_seconds, 60.0),
                    poll_seconds=options.poll_seconds,
                )
                changed_state, changed_state_revision = _correlate_secure_runtime_browser_document(
                    changed_state_document,
                    changed_state,
                    endpoint="state",
                    adapter=config.protocol_adapter.state,
                    expected_service_instance_id=binding.service_instance_id,
                    expected_execution_id=binding.jarvis_execution_id,
                    expected_dataset_descriptor_sha256=binding.dataset_descriptor_sha256,
                    expected_command_id=command_id,
                )
                if {changed_event_revision, changed_state_revision} != {command_revision}:
                    raise RelayError("secure runtime command correlation changed its revision")
                first_observations = [
                    initial_health,
                    initial_state,
                    initial_event,
                    command_observation,
                    changed_event,
                    changed_state,
                ]
                browser_observations.extend(first_observations)
                evidence.extend(
                    _browser_evidence_reference(
                        active_attachment.attachment_id,
                        observation,
                    )
                    for observation in first_observations
                )

            with _validation_check(
                recorder,
                "secure-runtime.browser-revocation",
                "revoke the one-time browser capability before runtime detach",
                forbidden_values=forbidden_values,
            ) as evidence:
                revoked_grant = active_attachment
                detached_browser = supervisor.browser_detach(
                    session_id=gateway_session_id,
                    attachment_id=revoked_grant.attachment_id,
                )
                active_attachment = None
                if detached_browser.attachment_id != revoked_grant.attachment_id:
                    raise RelayError("browser detach returned a different attachment identity")
                if not detached_browser.capability_revoked or not detached_browser.proxy_stopped:
                    raise RelayError("browser detach did not revoke and stop its exact proxy")
                revoked_grants.append((revoked_grant, detached_browser.proxy_stopped))
                _assert_browser_capability_revoked(
                    revoked_grant.health_url,
                    timeout_seconds=min(options.poll_seconds, 2.0),
                    proxy_stopped=detached_browser.proxy_stopped,
                )
                evidence.append(
                    EvidenceReference(
                        kind="browser_capability_revocation",
                        reference=(
                            f"browser-attachment://{gateway_session_id}/"
                            f"{revoked_grant.attachment_id}"
                        ),
                        excerpt="revocation observed before runtime detach",
                    )
                )

            with _validation_check(
                recorder,
                "secure-runtime.detach",
                "detach desktop connector while retaining remote and scheduler resources",
                forbidden_values=forbidden_values,
            ) as evidence:
                detached = supervisor.detach(session_id=gateway_session_id)
                _validate_secure_runtime_cleanup(
                    detached,
                    expected_mode="detach",
                    expected_session_id=gateway_session_id,
                )
                lifecycle_states.append("degraded")
                public_detach = cast(
                    dict[str, Any], redact_sensitive_values(detached.json_payload())
                )
                public_documents.append(public_detach)
                _record_runtime_cleanup(
                    recorder,
                    detached,
                    role="secure_runtime_detach",
                )
                evidence.append(
                    EvidenceReference(
                        kind="gateway_cleanup",
                        reference=f"gateway-runtime://{options.cluster}/{gateway_session_id}",
                        excerpt="desktop detached; remote runtime and scheduler work retained",
                        metadata={"mode": "detach", "scheduler_cancel_requested": False},
                    )
                )

            with _validation_check(
                recorder,
                "secure-runtime.reconnect",
                "reattach relay connector and issue a fresh browser capability",
                forbidden_values=forbidden_values,
            ) as evidence:
                reattached = supervisor.attach(session_id=gateway_session_id)
                if (
                    reattached.session.session_id != gateway_session_id
                    or reattached.session.state is not GatewaySessionState.READY
                ):
                    raise RelayError("secure runtime reattachment did not restore the gateway")
                lifecycle_states.append("ready")
                public_documents.append(public_gateway_session(reattached.session))
                active_attachment = supervisor.browser_attach(
                    session_id=gateway_session_id,
                    ttl_seconds=config.browser_attachment_ttl_seconds,
                )
                attachment_ids.append(active_attachment.attachment_id)
                browser_capability = _browser_attachment_capability(active_attachment)
                if browser_capability in forbidden_values:
                    raise RelayError("secure runtime reconnect reused a browser capability")
                forbidden_values.add(browser_capability)
                for old_grant, proxy_stopped in revoked_grants:
                    _assert_browser_capability_revoked(
                        old_grant.health_url,
                        timeout_seconds=min(options.poll_seconds, 2.0),
                        proxy_stopped=proxy_stopped,
                    )
                reconnected_health, reconnected_state, reconnected_event, reconnected_revisions = (
                    live_acceptance_browser_evidence._observe_correlated_browser_triad(  # pyright: ignore[reportPrivateUsage]  # noqa: E501, SLF001
                        active_attachment,
                        config=config,
                        binding=binding,
                        health_command_id=None,
                        state_command_id=command_id,
                        event_command_id=command_id,
                        event_name=event_name,
                        timeout_seconds=min(options.timeout_seconds, 60.0),
                    )
                )
                if reconnected_revisions != {command_revision}:
                    raise RelayError("secure runtime reconnect changed command revision")
                reconnected_observations = [
                    reconnected_health,
                    reconnected_state,
                    reconnected_event,
                ]
                browser_observations.extend(reconnected_observations)
                evidence.extend(
                    _browser_evidence_reference(
                        active_attachment.attachment_id,
                        observation,
                    )
                    for observation in reconnected_observations
                )

            with _validation_check(
                recorder,
                "secure-runtime.teardown",
                "revoke browser access and close relay resources without scheduler cancellation",
                forbidden_values=forbidden_values,
            ) as evidence:
                assert active_attachment is not None
                final_grant = active_attachment
                final_detachment = supervisor.browser_detach(
                    session_id=gateway_session_id,
                    attachment_id=final_grant.attachment_id,
                )
                active_attachment = None
                if (
                    final_detachment.attachment_id != final_grant.attachment_id
                    or not final_detachment.capability_revoked
                    or not final_detachment.proxy_stopped
                ):
                    raise RelayError("final browser detach did not revoke and stop its exact proxy")
                revoked_grants.append((final_grant, final_detachment.proxy_stopped))
                _assert_browser_capability_revoked(
                    final_grant.health_url,
                    timeout_seconds=min(options.poll_seconds, 2.0),
                    proxy_stopped=final_detachment.proxy_stopped,
                )
                teardown_result = supervisor.stop(
                    session_id=gateway_session_id,
                    cancel_scheduler_job=False,
                )
                _validate_secure_runtime_cleanup(
                    teardown_result,
                    expected_mode="teardown",
                    expected_session_id=gateway_session_id,
                )
                teardown_complete = True
                lifecycle_states.append("closed")
                public_teardown = cast(
                    dict[str, Any],
                    redact_sensitive_values(teardown_result.json_payload()),
                )
                public_documents.append(public_teardown)
                _record_runtime_cleanup(
                    recorder,
                    teardown_result,
                    role="secure_runtime_teardown",
                )
                for old_grant, proxy_stopped in revoked_grants:
                    _assert_browser_capability_revoked(
                        old_grant.health_url,
                        timeout_seconds=min(options.poll_seconds, 2.0),
                        proxy_stopped=proxy_stopped,
                    )
                evidence.append(
                    EvidenceReference(
                        kind="gateway_cleanup",
                        reference=f"gateway-runtime://{options.cluster}/{gateway_session_id}",
                        excerpt="gateway closed; scheduler cancellation not requested",
                        metadata={
                            "mode": "teardown",
                            "scheduler_cancel_requested": False,
                            "remaining_resources": 0,
                        },
                    )
                )

        assert gateway_session_id is not None
        assert teardown_result is not None
        secure_evidence = SecureRuntimeAcceptanceEvidence(
            cluster=options.cluster,
            query_mcp_session=query_mcp_evidence,
            bind_mcp_session=bind_mcp_evidence,
            handoff=handoff,
            source_artifact_sha256=source_artifact_sha256,
            gateway_session_id=gateway_session_id,
            binding_schema_version=cast(
                Literal["clio-relay.jarvis-service-runtime-binding.v2"],
                binding.schema_version,
            ),
            service_runtime_schema_version=cast(
                Literal["jarvis.service-runtime.v2"],
                binding.service_runtime_schema_version,
            ),
            service_revision=binding.service_revision,
            authorization_sha256=cast(str, binding.authorization_sha256),
            dataset_descriptor_sha256=binding.dataset_descriptor_sha256,
            browser_attachment_ids=attachment_ids,
            browser_observations=browser_observations,
            lifecycle_states=lifecycle_states,
            scheduler_cancel_requested=False,
            browser_capability_in_public_evidence=False,
            raw_authority_material_in_public_evidence=False,
            secret_values_absent_from_public_evidence=True,
        )
        public_documents.append(secure_evidence.model_dump(mode="json"))
        with _validation_check(
            recorder,
            "secure-runtime.secrets-absent",
            "prove private authority, browser capabilities, and connector secrets are absent",
            forbidden_values=forbidden_values,
        ) as evidence:
            for index, document in enumerate(public_documents):
                _assert_secret_free_document(
                    document,
                    forbidden_values=forbidden_values,
                    label=f"secure runtime public document {index}",
                )
            _assert_secret_free_document(
                recorder.report.model_dump(mode="json"),
                forbidden_values=forbidden_values,
                label="secure runtime report before final evidence",
            )
            evidence.append(
                EvidenceReference(
                    kind="secure_runtime_acceptance",
                    reference=f"gateway-runtime://{options.cluster}/{gateway_session_id}",
                    sha256=_secure_runtime_canonical_json_sha256(
                        secure_evidence.model_dump(mode="json")
                    ),
                    metadata=secure_evidence.model_dump(mode="json"),
                )
            )
        return forbidden_values
    except Exception as exc:
        primary_error = exc
        _redact_exception_values(exc, forbidden_values)
        raise
    finally:
        _cleanup_secure_runtime_failure(
            cluster=options.cluster,
            recorder=recorder,
            supervisor=supervisor,
            runtime_queue=runtime_queue,
            baseline_gateway_session_ids=baseline_gateway_session_ids,
            handoff=handoff,
            gateway_session_id=gateway_session_id,
            active_attachment=active_attachment,
            teardown_complete=teardown_complete,
            primary_error=primary_error,
            forbidden_values=forbidden_values,
        )

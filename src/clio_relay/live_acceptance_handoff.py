"""Secure runtime service-handoff selection and bind-result validation.

Extracted from ``live_acceptance.py`` (#231 rework): the concern of
selecting exactly one JARVIS v3.6 service-runtime handoff from a query
result, proving a bind/pending-bind result is bound to that exact handoff
(source job/artifact, package, service instance) with no unverified or
secret-bearing field, and reading the gateway-session inventory a cleanup
candidate is recovered from.
"""

from __future__ import annotations

import re
import urllib.parse
from typing import Any, cast

from clio_relay.browser_gateway import BrowserAttachmentGrant
from clio_relay.errors import RelayError
from clio_relay.identifiers import validate_durable_record_id
from clio_relay.jarvis_service_runtime import (
    JARVIS_SERVICE_RUNTIME_SCHEMA_V2,
    RELAY_JARVIS_RUNTIME_BINDING_SCHEMA_V2,
    JarvisServiceRuntimeBinding,
    JarvisServiceRuntimeHandoff,
)
from clio_relay.live_acceptance_models import (
    MAX_ACCEPTANCE_COLLECTION_RECORDS,
    SecureRuntimeProbeConfig,
    _AcceptanceObservationPending,
    _secure_runtime_canonical_json_sha256,
)
from clio_relay.live_acceptance_secret_redaction import (
    _assert_secret_free_document,
    _record_runtime_cleanup,
    _redacted_error_text,
    _redacted_text,
)
from clio_relay.models import GatewaySession
from clio_relay.pagination import MAX_RESPONSE_PAGE_RECORDS
from clio_relay.service_runtime import ServiceRuntimeSupervisor
from clio_relay.storage_runtime import StorageManagedQueue
from clio_relay.validation_report import ValidationRecorder


def _select_secure_runtime_handoff(
    query_result: dict[str, Any],
    *,
    cluster: str,
    config: SecureRuntimeProbeConfig,
) -> JarvisServiceRuntimeHandoff | None:
    """Select exactly one artifact-bound service handoff using configured identities."""
    if query_result.get("terminal") is not True or query_result.get("state") != "succeeded":
        raise RelayError("JARVIS execution query did not complete successfully")
    if query_result.get("cluster") != cluster:
        raise RelayError("JARVIS execution query changed cluster identity")
    _query_receipt_artifact_identity(query_result)
    raw_bindings = query_result.get("service_runtime_bindings")
    if not isinstance(raw_bindings, list):
        raise RelayError("JARVIS v3.6 execution query omitted service_runtime_bindings")
    if not raw_bindings:
        return None
    bindings: list[JarvisServiceRuntimeHandoff] = []
    for raw in cast(list[object], raw_bindings):
        try:
            binding = JarvisServiceRuntimeHandoff.model_validate(raw)
        except ValueError as exc:
            raise RelayError(f"JARVIS service runtime handoff was invalid: {exc}") from exc
        if binding.cluster != cluster:
            raise RelayError("JARVIS service runtime handoff changed cluster identity")
        if binding.package_name != config.package_name:
            continue
        if config.package_id is not None and binding.package_id != config.package_id:
            continue
        if (
            config.service_instance_id is not None
            and binding.service_instance_id != config.service_instance_id
        ):
            continue
        bindings.append(binding)
    if len(bindings) != 1:
        raise RelayError(
            "secure runtime selectors must identify exactly one ready service; "
            f"matched={len(bindings)}"
        )
    return bindings[0]


def _query_source_artifact_sha256(
    query_result: dict[str, Any],
    *,
    handoff: JarvisServiceRuntimeHandoff,
) -> str:
    """Bind the compact handoff to the same immutable private MCP artifact."""
    job_id, artifact_id, digest = _query_receipt_artifact_identity(query_result)
    if job_id != handoff.source_job_id:
        raise RelayError("service runtime handoff source job differs from its query receipt")
    if artifact_id != handoff.source_artifact_id:
        raise RelayError("service runtime handoff source artifact differs from its query receipt")
    return digest


def _query_receipt_artifact_identity(
    query_result: dict[str, Any],
) -> tuple[str, str, str]:
    """Validate one durable query receipt and its private result-artifact identity."""
    try:
        job_id = validate_durable_record_id(query_result.get("job_id"))
    except (TypeError, ValueError) as exc:
        raise RelayError("JARVIS execution query omitted a durable relay job identity") from exc
    raw_artifact = query_result.get("mcp_result_artifact")
    if not isinstance(raw_artifact, dict):
        raise RelayError("JARVIS execution query omitted mcp_result_artifact")
    artifact = cast(dict[str, object], raw_artifact)
    try:
        artifact_id = validate_durable_record_id(artifact.get("artifact_id"))
        artifact_job_id = validate_durable_record_id(artifact.get("job_id"))
    except (TypeError, ValueError) as exc:
        raise RelayError("JARVIS execution query artifact identity was invalid") from exc
    if artifact_job_id != job_id or artifact.get("kind") != "mcp_result":
        raise RelayError("JARVIS execution query artifact does not match its receipt")
    digest = artifact.get("sha256")
    if not isinstance(digest, str) or re.fullmatch(r"[0-9a-f]{64}", digest) is None:
        raise RelayError("service runtime source artifact omitted a canonical SHA-256")
    return job_id, artifact_id, digest


def _secure_runtime_cleanup_candidate(
    bind_result: dict[str, Any],
    *,
    handoff: JarvisServiceRuntimeHandoff,
) -> str:
    """Recover only the exact owned session identity safe to tear down after bind failure."""
    session_id = bind_result.get("gateway_session_id")
    gateway_value = bind_result.get("gateway_session")
    if not isinstance(session_id, str) or not isinstance(gateway_value, dict):
        raise RelayError("secure runtime bind omitted its cleanup identity")
    gateway = cast(dict[str, object], gateway_value)
    metadata_value = gateway.get("metadata")
    metadata = cast(dict[str, object], metadata_value) if isinstance(metadata_value, dict) else {}
    gateway_data_value = gateway.get("gateway")
    gateway_data = (
        cast(dict[str, object], gateway_data_value) if isinstance(gateway_data_value, dict) else {}
    )
    binding_value = gateway_data.get("jarvis_runtime_binding")
    binding = cast(dict[str, object], binding_value) if isinstance(binding_value, dict) else {}
    if (
        gateway.get("session_id") != session_id
        or gateway.get("cluster") != handoff.cluster
        or metadata.get("owner") != "clio-relay"
        or binding.get("source_relay_job_id") != handoff.source_job_id
        or binding.get("source_relay_artifact_id") != handoff.source_artifact_id
        or binding.get("package_id") != handoff.package_id
        or binding.get("package_name") != handoff.package_name
        or binding.get("service_instance_id") != handoff.service_instance_id
    ):
        raise RelayError("secure runtime bind did not prove an exact owned cleanup identity")
    return session_id


def _gateway_session_matches_handoff(
    session: GatewaySession,
    *,
    handoff: JarvisServiceRuntimeHandoff,
) -> bool:
    """Identify only a newly created owned gateway for the exact requested service."""
    binding_value = session.gateway.get("jarvis_runtime_binding")
    binding = cast(dict[str, object], binding_value) if isinstance(binding_value, dict) else {}
    return (
        session.cluster == handoff.cluster
        and session.metadata.get("owner") == "clio-relay"
        and binding.get("source_relay_job_id") == handoff.source_job_id
        and binding.get("source_relay_artifact_id") == handoff.source_artifact_id
        and binding.get("package_id") == handoff.package_id
        and binding.get("package_name") == handoff.package_name
        and binding.get("service_instance_id") == handoff.service_instance_id
    )


def _gateway_sessions_for_acceptance(
    queue: StorageManagedQueue,
    *,
    cluster: str,
) -> list[GatewaySession]:
    """Read one target's gateway records through bounded canonical pagination."""
    sessions: list[GatewaySession] = []
    cursor = 1
    while True:
        page, next_cursor, total = queue.list_gateway_sessions_page(
            cursor=cursor,
            limit=MAX_RESPONSE_PAGE_RECORDS,
            cluster=cluster,
        )
        sessions.extend(page)
        if total > MAX_ACCEPTANCE_COLLECTION_RECORDS or len(sessions) > total:
            raise RelayError(
                "secure runtime acceptance gateway inventory exceeded "
                f"{MAX_ACCEPTANCE_COLLECTION_RECORDS} records"
            )
        if next_cursor is None:
            return sessions
        if next_cursor <= cursor:
            raise RelayError("secure runtime acceptance gateway pagination did not advance")
        cursor = next_cursor


def _validated_secure_runtime_pending_bind(
    bind_result: dict[str, Any],
    *,
    handoff: JarvisServiceRuntimeHandoff,
) -> str:
    """Validate a durable bind observation without treating it as a ready endpoint."""
    if (
        bind_result.get("outcome") != "pending"
        or bind_result.get("scheduler_cancel_requested") is not False
        or bind_result.get("scheduler_action") != "none"
        or bind_result.get("relay_action") != "none"
    ):
        raise RelayError("secure runtime pending bind changed its no-action contract")
    gateway_session_id = _secure_runtime_cleanup_candidate(bind_result, handoff=handoff)
    gateway = cast(dict[str, Any], bind_result["gateway_session"])
    if gateway.get("state") not in {"created", "pending", "allocated", "starting", "degraded"}:
        raise RelayError("secure runtime pending bind reported an invalid gateway state")
    retry_selector = bind_result.get("retry_selector")
    if not isinstance(retry_selector, dict):
        raise RelayError("secure runtime pending bind omitted its retry selector")
    typed_selector = cast(dict[str, object], retry_selector)
    if (
        typed_selector.get("cluster") != handoff.cluster
        or typed_selector.get("gateway_session_id") != gateway_session_id
    ):
        raise RelayError("secure runtime pending bind changed its retry identity")
    for key in (
        "connect_url",
        "health_url",
        "stream_url",
        "events_url",
        "state_url",
        "command_url",
    ):
        if bind_result.get(key) is not None:
            raise RelayError(f"secure runtime pending bind exposed unverified {key}")
    _assert_secret_free_document(
        bind_result,
        forbidden_values=set(),
        label="secure runtime pending bind",
    )
    return gateway_session_id


def _validated_secure_runtime_bind(
    bind_result: dict[str, Any],
    *,
    handoff: JarvisServiceRuntimeHandoff,
    expected_execution_id: str,
    expected_source_artifact_sha256: str,
) -> tuple[str, JarvisServiceRuntimeBinding]:
    """Validate the public v2 bind result without accepting caller-owned runtime data."""
    if bind_result.get("scheduler_cancel_requested") is not False:
        raise RelayError("secure runtime bind unexpectedly requested scheduler cancellation")
    gateway_session_id = bind_result.get("gateway_session_id")
    gateway = bind_result.get("gateway_session")
    if not isinstance(gateway_session_id, str) or not isinstance(gateway, dict):
        raise RelayError("secure runtime bind omitted its gateway identity")
    typed_gateway = cast(dict[str, Any], gateway)
    if (
        typed_gateway.get("session_id") != gateway_session_id
        or typed_gateway.get("cluster") != handoff.cluster
        or typed_gateway.get("state") != "ready"
    ):
        raise RelayError("secure runtime bind returned an inconsistent gateway")
    gateway_data = typed_gateway.get("gateway")
    if not isinstance(gateway_data, dict):
        raise RelayError("secure runtime gateway omitted its public binding")
    try:
        binding = JarvisServiceRuntimeBinding.model_validate(
            cast(dict[str, Any], gateway_data).get("jarvis_runtime_binding")
        )
    except ValueError as exc:
        raise RelayError(f"secure runtime public binding was invalid: {exc}") from exc
    if (
        binding.schema_version != RELAY_JARVIS_RUNTIME_BINDING_SCHEMA_V2
        or binding.service_runtime_schema_version != JARVIS_SERVICE_RUNTIME_SCHEMA_V2
        or binding.authorization_sha256 is None
        or binding.source_relay_job_id != handoff.source_job_id
        or binding.source_relay_artifact_id != handoff.source_artifact_id
        or binding.source_relay_artifact_sha256 != expected_source_artifact_sha256
        or binding.jarvis_execution_id != expected_execution_id
        or binding.package_id != handoff.package_id
        or binding.package_name != handoff.package_name
        or binding.service_instance_id != handoff.service_instance_id
        or binding.dataset_descriptor_sha256
        != _secure_runtime_canonical_json_sha256(binding.dataset_descriptor.model_dump(mode="json"))
    ):
        raise RelayError("secure runtime public binding changed its exact handoff identity")
    for key in (
        "connect_url",
        "health_url",
        "stream_url",
        "events_url",
        "state_url",
        "command_url",
    ):
        value = bind_result.get(key)
        if not isinstance(value, str):
            raise RelayError(f"secure runtime bind omitted {key}")
        parsed = urllib.parse.urlsplit(value)
        if (
            parsed.scheme not in {"http", "https"}
            or parsed.hostname != "127.0.0.1"
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
        ):
            raise RelayError(f"secure runtime public {key} is not a clean loopback URL")
    _assert_secret_free_document(bind_result, forbidden_values=set(), label="secure runtime bind")
    return gateway_session_id, binding


def _cleanup_secure_runtime_failure(
    *,
    cluster: str,
    recorder: ValidationRecorder,
    supervisor: ServiceRuntimeSupervisor | None,
    runtime_queue: StorageManagedQueue | None,
    baseline_gateway_session_ids: set[str] | None,
    handoff: JarvisServiceRuntimeHandoff | None,
    gateway_session_id: str | None,
    active_attachment: BrowserAttachmentGrant | None,
    teardown_complete: bool,
    primary_error: Exception | None,
    forbidden_values: set[str],
) -> None:
    """Best-effort cleanup for a secure runtime acceptance that failed mid-lifecycle.

    Discovers the exact orphaned gateway session by handoff match when the
    lifecycle failed before binding produced one, then -- unless teardown
    already completed, or the failure was itself a bounded pending
    observation (never a cleanup trigger) -- detaches any still-open
    browser attachment and stops the runtime without a scheduler
    cancellation, attaching every redacted cleanup error as a note on
    ``primary_error`` rather than replacing it.
    """
    cleanup_session_ids: list[str] = []
    if gateway_session_id is not None:
        cleanup_session_ids.append(gateway_session_id)
    elif (
        supervisor is not None
        and runtime_queue is not None
        and baseline_gateway_session_ids is not None
        and handoff is not None
    ):
        try:
            cleanup_session_ids.extend(
                session.session_id
                for session in _gateway_sessions_for_acceptance(runtime_queue, cluster=cluster)
                if session.session_id not in baseline_gateway_session_ids
                and _gateway_session_matches_handoff(session, handoff=handoff)
            )
        except Exception as cleanup_discovery_exc:
            if primary_error is not None:
                primary_error.add_note(
                    "secure runtime cleanup discovery: "
                    + _redacted_error_text(cleanup_discovery_exc, forbidden_values)
                )
    if (
        supervisor is not None
        and cleanup_session_ids
        and not teardown_complete
        and not isinstance(primary_error, _AcceptanceObservationPending)
    ):
        cleanup_errors: list[str] = []
        if active_attachment is not None and gateway_session_id is not None:
            try:
                supervisor.browser_detach(
                    session_id=gateway_session_id,
                    attachment_id=active_attachment.attachment_id,
                )
            except Exception as cleanup_exc:
                cleanup_errors.append(_redacted_error_text(cleanup_exc, forbidden_values))
        for cleanup_session_id in cleanup_session_ids:
            try:
                cleanup = supervisor.stop(
                    session_id=cleanup_session_id,
                    cancel_scheduler_job=False,
                )
                _record_runtime_cleanup(recorder, cleanup, role="secure_runtime_failure_cleanup")
                if cleanup.errors or cleanup.residual_resources:
                    cleanup_errors.extend(
                        _redacted_text(item, forbidden_values) for item in cleanup.errors
                    )
            except Exception as cleanup_exc:
                cleanup_errors.append(_redacted_error_text(cleanup_exc, forbidden_values))
        if cleanup_errors and primary_error is not None:
            primary_error.add_note("secure runtime cleanup: " + "; ".join(cleanup_errors))

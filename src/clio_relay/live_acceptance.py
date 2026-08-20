"""Configurable live acceptance runner for cluster relay deployments."""

from __future__ import annotations

import hashlib
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, cast
from uuid import uuid4

import clio_relay.live_acceptance_agent_prompt as live_acceptance_agent_prompt
import clio_relay.live_acceptance_browser_evidence as live_acceptance_browser_evidence
import clio_relay.live_acceptance_handoff as live_acceptance_handoff
import clio_relay.live_acceptance_job_verification as live_acceptance_job_verification
import clio_relay.live_acceptance_models as live_acceptance_models
import clio_relay.live_acceptance_packaged_mcp as live_acceptance_packaged_mcp
import clio_relay.live_acceptance_progress as live_acceptance_progress
import clio_relay.live_acceptance_remote_io as live_acceptance_remote_io
import clio_relay.live_acceptance_secret_redaction as live_acceptance_secret_redaction
import clio_relay.live_acceptance_transport as live_acceptance_transport
from clio_relay.browser_gateway import BrowserAttachmentGrant
from clio_relay.cluster_config import ClusterDefinition
from clio_relay.config import RelaySettings
from clio_relay.doctor import run_cluster_doctor
from clio_relay.errors import ConfigurationError, ObservationTimeoutError, RelayError
from clio_relay.identifiers import DurableRecordId
from clio_relay.jarvis_service_runtime import (
    JarvisServiceRuntimeHandoff,
)
from clio_relay.live_acceptance_models import (
    LIVE_ACCEPTANCE_CHECKPOINT_RESOURCE_KIND,
    LIVE_ACCEPTANCE_CHECKPOINT_SCHEMA,
    CommandRunner,
    LiveAcceptanceCheckpoint,
    LiveAcceptanceOptions,
    PackagedMcpAcceptanceEvidence,
    RuntimeMetadataAcceptance,
    SecureRuntimeAcceptanceEvidence,
    SecureRuntimeHttpEvidence,
    SecureRuntimeProbeConfig,
    SecureRuntimeProtocolAdapter,  # noqa: F401 -- unused here; tests bare-import it from this module
)
from clio_relay.mcp_stdio_validation import (
    run_packaged_mcp_stdio_session,
)
from clio_relay.models import (
    TERMINAL_STATES,
    GatewaySessionState,
    JobState,
    RelayJob,
)
from clio_relay.public_records import public_gateway_session
from clio_relay.runtime_metadata import (
    RUNTIME_METADATA_SCHEMA,
    JarvisRuntimeMetadata,
    RuntimeMetadataSource,
)
from clio_relay.service_runtime import ServiceRuntimeStopResult, ServiceRuntimeSupervisor
from clio_relay.storage_runtime import StorageManagedQueue, storage_managed_queue
from clio_relay.transport_probe import (
    transport_evidence_lines_from_error,
)
from clio_relay.validation_report import (
    CleanupEvidence,
    EvidenceReference,
    LiveValidationReport,
    ValidationCheck,
    ValidationRecorder,
    ValidationResource,
    ValidationStatus,
    load_validation_report,
    new_live_validation_report,
    redact_sensitive_values,
)

# live_acceptance_models.py (#231 rework) owns these -- they keep their
# original underscore-private names here via qualified re-export (the
# cli_support._run_or_exit / cli.py:782 idiom) so every call site below,
# plus test_live_acceptance.py's direct `from clio_relay.live_acceptance
# import _Name` imports, keeps resolving unchanged.
_AcceptanceObservationPending = (
    live_acceptance_models._AcceptanceObservationPending  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_BrowserHttpRequestError = (
    live_acceptance_models._BrowserHttpRequestError  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_BrowserHttpResponse = (
    live_acceptance_models._BrowserHttpResponse  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_LiveAcceptancePending = (
    live_acceptance_models._LiveAcceptancePending  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_LiveAcceptanceState = (
    live_acceptance_models._LiveAcceptanceState  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_ValidationLines = (
    live_acceptance_models._ValidationLines  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_acceptance_run_id = (
    live_acceptance_models._acceptance_run_id  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_configured_path = (
    live_acceptance_models._configured_path  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_live_acceptance_checkpoint_sha256 = (
    live_acceptance_models._live_acceptance_checkpoint_sha256  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_secure_runtime_canonical_json_sha256 = (
    live_acceptance_models._secure_runtime_canonical_json_sha256  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_secure_runtime_json_pointer_value = (
    live_acceptance_models._secure_runtime_json_pointer_value  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)

# live_acceptance_remote_io.py (#231 rework) owns these -- the remote
# shell/artifact IO leaf primitives, re-exported the same qualified way so
# every still-resident caller below (the secure runtime acceptance
# orchestrator, job verification, agent-prompt generation) and
# test_live_acceptance.py's direct imports keep resolving unchanged.
_cluster_agent_bin = (
    live_acceptance_remote_io._cluster_agent_bin  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_command_error = (
    live_acceptance_remote_io._command_error  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_decode_artifact_text = (
    live_acceptance_remote_io._decode_artifact_text  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_delivery_refusal_error = (
    live_acceptance_remote_io._delivery_refusal_error  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_format_run_id = (
    live_acceptance_remote_io._format_run_id  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_remote_clio_json = (
    live_acceptance_remote_io._remote_clio_json  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_remote_command_failure = (
    live_acceptance_remote_io._remote_command_failure  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_remote_env = (
    live_acceptance_remote_io._remote_env  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_remote_job_collection = (
    live_acceptance_remote_io._remote_job_collection  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_remote_shell = (
    live_acceptance_remote_io._remote_shell  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_remote_write_file = (
    live_acceptance_remote_io._remote_write_file  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_run_command = (
    live_acceptance_remote_io._run_command  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_stage_acceptance_files = (
    live_acceptance_remote_io._stage_acceptance_files  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)

# live_acceptance_transport.py (#231 rework) owns these -- transport-mode
# acceptance (frp-relay/frp-direct/SSH-forward HTTP round trips, worker
# deployment verification), re-exported the same qualified way for the
# facade's five call sites and test_live_acceptance.py's direct imports.
# _unique_transport_port/_verify_transport_http_api/_wait_for_transport_
# http_success stay internal to the new module -- nothing outside it (nor
# any test) references them directly.
_assert_direct_xtcp_acceptance = (
    live_acceptance_transport._assert_direct_xtcp_acceptance  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_http_json = (
    live_acceptance_transport._http_json  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_require_transport_secrets = (
    live_acceptance_transport._require_transport_secrets  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_verify_cluster_deployment = (
    live_acceptance_transport._verify_cluster_deployment  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_verify_direct_transport = (
    live_acceptance_transport._verify_direct_transport  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_verify_ssh_transport = (
    live_acceptance_transport._verify_ssh_transport  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_verify_transport = (
    live_acceptance_transport._verify_transport  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)

# live_acceptance_secret_redaction.py (#231 rework) owns these -- secret-
# free evidence enforcement for the secure runtime cleanup lifecycle,
# re-exported the same qualified way for the still-resident secure runtime
# acceptance orchestrator's many call sites and test_live_acceptance.py's
# direct import of _assert_secret_free_document.
_assert_secret_free_document = (
    live_acceptance_secret_redaction._assert_secret_free_document  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_record_runtime_cleanup = (
    live_acceptance_secret_redaction._record_runtime_cleanup  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_redact_exception_values = (
    live_acceptance_secret_redaction._redact_exception_values  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_redacted_error_text = (
    live_acceptance_secret_redaction._redacted_error_text  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_redacted_text = (
    live_acceptance_secret_redaction._redacted_text  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_validate_secure_runtime_cleanup = (
    live_acceptance_secret_redaction._validate_secure_runtime_cleanup  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)

# live_acceptance_packaged_mcp.py (#231 rework) owns these -- packaged MCP
# stdio child evidence (isolated transport secrets, structured tool-call
# validation, contract-digest re-derivation), re-exported the same
# qualified way for the still-resident secure runtime acceptance
# orchestrator and test_live_acceptance.py's direct import of
# _packaged_mcp_acceptance_evidence.
_configured_runtime_secret = (
    live_acceptance_packaged_mcp._configured_runtime_secret  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_isolated_runtime_child_environment = (
    live_acceptance_packaged_mcp._isolated_runtime_child_environment  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_packaged_mcp_acceptance_evidence = (
    live_acceptance_packaged_mcp._packaged_mcp_acceptance_evidence  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_packaged_mcp_structured_result = (
    live_acceptance_packaged_mcp._packaged_mcp_structured_result  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_validation_check = (
    live_acceptance_packaged_mcp._validation_check  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)

# live_acceptance_handoff.py (#231 rework) owns these -- secure runtime
# service-handoff selection and bind-result validation, re-exported the
# same qualified way for the still-resident secure runtime acceptance
# orchestrator and test_live_acceptance.py's direct import of
# _select_secure_runtime_handoff/_validated_secure_runtime_pending_bind.
# _query_receipt_artifact_identity stays internal -- nothing outside the
# module (nor any test) references it directly.
_gateway_session_matches_handoff = (
    live_acceptance_handoff._gateway_session_matches_handoff  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_gateway_sessions_for_acceptance = (
    live_acceptance_handoff._gateway_sessions_for_acceptance  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_query_source_artifact_sha256 = (
    live_acceptance_handoff._query_source_artifact_sha256  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_secure_runtime_cleanup_candidate = (
    live_acceptance_handoff._secure_runtime_cleanup_candidate  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_select_secure_runtime_handoff = (
    live_acceptance_handoff._select_secure_runtime_handoff  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_validated_secure_runtime_bind = (
    live_acceptance_handoff._validated_secure_runtime_bind  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_validated_secure_runtime_pending_bind = (
    live_acceptance_handoff._validated_secure_runtime_pending_bind  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)

# live_acceptance_browser_evidence.py (#231 rework) owns these -- turning a
# raw browser HTTP/SSE response (live_acceptance_browser_http) into
# SecureRuntimeHttpEvidence, correlating it against the declared adapter,
# and polling for a durable change. Re-exported the same qualified way for
# the still-resident secure runtime acceptance orchestrator's many call
# sites; _browser_json_observation/_browser_sse_observation are also
# test_live_acceptance.py monkeypatch targets, repointed below.
_assert_browser_capability_revoked = (
    live_acceptance_browser_evidence._assert_browser_capability_revoked  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_browser_attachment_capability = (
    live_acceptance_browser_evidence._browser_attachment_capability  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_browser_evidence_reference = (
    live_acceptance_browser_evidence._browser_evidence_reference  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_browser_json_observation = (
    live_acceptance_browser_evidence._browser_json_observation  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_browser_sse_observation = (
    live_acceptance_browser_evidence._browser_sse_observation  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_correlate_secure_runtime_browser_document = (
    live_acceptance_browser_evidence._correlate_secure_runtime_browser_document  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_wait_for_changed_browser_state = (
    live_acceptance_browser_evidence._wait_for_changed_browser_state  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_wait_for_changed_sse_event = (
    live_acceptance_browser_evidence._wait_for_changed_sse_event  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)

# live_acceptance_agent_prompt.py (#231 rework) owns this -- the generated
# agent-child prompt writer, re-exported the same qualified way for the
# still-resident facade's one call site. _remote_home/_generated_agent_
# prompt stay internal -- nothing outside the module references them.
_write_generated_agent_prompt = (
    live_acceptance_agent_prompt._write_generated_agent_prompt  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)

# live_acceptance_job_verification.py (#231 rework) owns these -- proving a
# completed relay job and finding an agent's non-stale child job.
# _find_agent_child_job/_remote_job_has_event are re-exported bare (not
# monkeypatched; _remote_job_has_event's only caller, _verify_live_package_
# progress, is still resident below pending its own slice).
# _verify_completed_job IS a monkeypatch target (test_live_acceptance.py)
# with two still-resident callers in _run_live_acceptance below, so its
# call sites are qualified rather than routed through this bare re-export
# (the cli_support._run_or_exit idiom) -- a bare reference here would not
# observe a patch applied to the new module after import.
_find_agent_child_job = (
    live_acceptance_job_verification._find_agent_child_job  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_remote_job_has_event = (
    live_acceptance_job_verification._remote_job_has_event  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)

# live_acceptance_progress.py (#231 rework) owns these -- runtime metadata
# decoding and package-progress attestation, re-exported the same
# qualified way so every still-resident caller below (the facade, transport
# verification, and the secure runtime acceptance orchestrator) and
# test_live_acceptance.py's direct imports keep resolving unchanged.
_assert_progress_adapter = (
    live_acceptance_progress._assert_progress_adapter  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_decode_runtime_metadata_payload = (
    live_acceptance_progress._decode_runtime_metadata_payload  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_expected_progress_adapter = (
    live_acceptance_progress._expected_progress_adapter  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_expected_progress_declaration = (
    live_acceptance_progress._expected_progress_declaration  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_expected_progress_package = (
    live_acceptance_progress._expected_progress_package  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_has_progress_adapter = (
    live_acceptance_progress._has_progress_adapter  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_native_progress_attestation = (
    live_acceptance_progress._native_progress_attestation  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_progress_attestation_identity = (
    live_acceptance_progress._progress_attestation_identity  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_progress_provider_attestation = (
    live_acceptance_progress._progress_provider_attestation  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_runtime_metadata_document_facts = (
    live_acceptance_progress._runtime_metadata_document_facts  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_runtime_metadata_facts = (
    live_acceptance_progress._runtime_metadata_facts  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_runtime_metadata_from_job_status = (
    live_acceptance_progress._runtime_metadata_from_job_status  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_secure_runtime_probe_config = (
    live_acceptance_progress._secure_runtime_probe_config  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_verify_progress_monitor = (
    live_acceptance_progress._verify_progress_monitor  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)
_verify_runtime_metadata_artifact = (
    live_acceptance_progress._verify_runtime_metadata_artifact  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
)


def _live_acceptance_intent_sha256(
    options: LiveAcceptanceOptions,
    *,
    jarvis_yaml: Path,
    pipeline_sha256: str,
    monitor_pattern: str | None,
    progress_pattern: str | None,
    progress_action_payload: dict[str, object],
    agent_prompt: str | None,
    agent_mcp_config: str | None,
    agent_child_jarvis_yaml: Path | None,
    require_agent_child_job: bool,
    verify_transport: bool,
    verify_direct_transport: bool,
    allow_direct_transport_fallback: bool,
) -> str:
    """Bind a checkpoint to every semantic input while allowing a new wait window."""
    child_sha256 = (
        hashlib.sha256(agent_child_jarvis_yaml.read_bytes()).hexdigest()
        if agent_child_jarvis_yaml is not None
        else None
    )
    intent = {
        "cluster": options.cluster,
        "definition": options.definition.model_dump(mode="json"),
        "jarvis_yaml": str(jarvis_yaml.resolve()),
        "pipeline_sha256": pipeline_sha256,
        "monitor_pattern": monitor_pattern,
        "progress_pattern": progress_pattern,
        "progress_action_payload": progress_action_payload,
        "agent_prompt": agent_prompt,
        "agent_mcp_config": agent_mcp_config,
        "agent_child_jarvis_yaml": (
            str(agent_child_jarvis_yaml.resolve()) if agent_child_jarvis_yaml is not None else None
        ),
        "agent_child_jarvis_sha256": child_sha256,
        "require_agent_child_job": require_agent_child_job,
        "verify_transport": verify_transport,
        "verify_direct_transport": verify_direct_transport,
        "verify_ssh_transport": options.verify_ssh_transport,
        "allow_direct_transport_fallback": allow_direct_transport_fallback,
        "transport_local_bind_port": options.transport_local_bind_port,
        "transport_remote_api_port": options.transport_remote_api_port,
        "transport_proxy_name": options.transport_proxy_name,
        "ssh_transport_local_bind_port": options.ssh_transport_local_bind_port,
        "ssh_transport_remote_api_port": options.ssh_transport_remote_api_port,
        "ssh_transport_session_id": options.ssh_transport_session_id,
        "require_structured_runtime_metadata": options.require_structured_runtime_metadata,
        "validation_scenario": options.validation_scenario,
        "verify_cluster_deployment": options.verify_cluster_deployment,
    }
    return _live_acceptance_checkpoint_sha256(intent)


def _current_live_acceptance_intent(
    options: LiveAcceptanceOptions,
) -> tuple[str, str]:
    """Resolve and hash all local resume inputs without performing remote work."""
    jarvis_yaml = options.jarvis_yaml or _configured_path(options.definition.live_test.jarvis_yaml)
    if jarvis_yaml is None or not jarvis_yaml.exists():
        raise ConfigurationError("resume report requires the original live-test JARVIS YAML")
    source_pipeline = jarvis_yaml.read_text(encoding="utf-8")
    pipeline_sha256 = hashlib.sha256(source_pipeline.encode("utf-8")).hexdigest()
    monitor_pattern = options.monitor_pattern or options.definition.live_test.monitor_pattern
    progress_pattern = options.progress_pattern or options.definition.live_test.progress_pattern
    progress_action_payload = (
        options.progress_action_payload
        if options.progress_action_payload
        else options.definition.live_test.progress_action_payload
    )
    agent_prompt = options.agent_prompt or options.definition.live_test.agent_prompt
    child_yaml = options.agent_child_jarvis_yaml or _configured_path(
        options.definition.live_test.agent_child_jarvis_yaml
    )
    agent_mcp_config = options.agent_mcp_config or options.definition.live_test.agent_mcp_config
    require_agent_child_job = (
        agent_mcp_config is not None
        if options.require_agent_child_job is None
        else options.require_agent_child_job
    )
    verify_transport = (
        options.definition.live_test.verify_transport
        if options.verify_transport is None
        else options.verify_transport
    )
    verify_direct = (
        options.definition.live_test.verify_direct_transport
        if options.verify_direct_transport is None
        else options.verify_direct_transport
    )
    allow_direct_fallback = (
        options.definition.live_test.allow_direct_transport_fallback
        if options.allow_direct_transport_fallback is None
        else options.allow_direct_transport_fallback
    )
    return pipeline_sha256, _live_acceptance_intent_sha256(
        options,
        jarvis_yaml=jarvis_yaml,
        pipeline_sha256=pipeline_sha256,
        monitor_pattern=monitor_pattern,
        progress_pattern=progress_pattern,
        progress_action_payload=progress_action_payload,
        agent_prompt=agent_prompt,
        agent_mcp_config=agent_mcp_config,
        agent_child_jarvis_yaml=child_yaml,
        require_agent_child_job=require_agent_child_job,
        verify_transport=verify_transport,
        verify_direct_transport=verify_direct,
        allow_direct_transport_fallback=allow_direct_fallback,
    )


def _load_live_acceptance_resume(
    options: LiveAcceptanceOptions,
) -> tuple[LiveValidationReport, LiveAcceptanceCheckpoint]:
    """Validate one pending checkpoint completely before any remote command executes."""
    assert options.resume_report_path is not None
    report = load_validation_report(options.resume_report_path)
    if report.status is not ValidationStatus.PENDING:
        raise ConfigurationError("--resume-report must contain a pending live-test report")
    if report.cluster != options.cluster or report.scenario != options.validation_scenario:
        raise ConfigurationError("resume report cluster or scenario changed")
    checkpoint_resources = [
        resource
        for resource in report.resources
        if resource.kind == LIVE_ACCEPTANCE_CHECKPOINT_RESOURCE_KIND
        and resource.role == "resume_checkpoint"
    ]
    if len(checkpoint_resources) != 1:
        raise ConfigurationError("resume report must contain exactly one live-test checkpoint")
    resource = checkpoint_resources[0]
    raw_checkpoint = resource.metadata.get("checkpoint")
    try:
        checkpoint = LiveAcceptanceCheckpoint.model_validate(raw_checkpoint)
    except ValueError as exc:
        raise ConfigurationError(f"live-test resume checkpoint is invalid: {exc}") from exc
    if (
        checkpoint.source_report_id != report.report_id
        or checkpoint.cluster != report.cluster
        or checkpoint.scenario != report.scenario
        or resource.resource_id != checkpoint.run_id
        or resource.cluster != checkpoint.cluster
        or resource.state != ValidationStatus.PENDING.value
        or resource.metadata.get("retry_selector") != checkpoint.retry_selector()
        or resource.metadata.get("scheduler_action") != "none"
        or resource.metadata.get("relay_action") != "observe_existing"
    ):
        raise ConfigurationError("live-test resume checkpoint disagrees with its report evidence")
    pipeline_sha256, intent_sha256 = _current_live_acceptance_intent(options)
    if checkpoint.pipeline_sha256 != pipeline_sha256 or checkpoint.intent_sha256 != intent_sha256:
        raise ConfigurationError("live-test resume inputs changed from the pending checkpoint")
    primary_resources = [
        item
        for item in report.resources
        if item.kind == "relay_job"
        and item.resource_id == checkpoint.primary_job_id
        and item.cluster == checkpoint.cluster
    ]
    if len(primary_resources) != 1:
        raise ConfigurationError("resume report omitted its exact primary relay job evidence")
    primary = primary_resources[0]
    if (
        primary.metadata.get("idempotency_key") != checkpoint.primary_idempotency_key
        or primary.metadata.get("retained") is not True
        or primary.metadata.get("scheduler_cancel_requested") is not False
    ):
        raise ConfigurationError("resume report primary relay job evidence was altered")
    matching_checks = [
        check
        for check in report.checks
        if check.check_id == "live-test.observation"
        and check.status is ValidationStatus.PENDING
        and any(
            evidence.metadata.get("retry_selector") == checkpoint.retry_selector()
            for evidence in check.evidence
        )
    ]
    if len(matching_checks) != 1:
        raise ConfigurationError("resume report omitted its exact pending observation evidence")
    if report.cleanup.cancel_scheduler_jobs:
        raise ConfigurationError("pending live-test report unexpectedly requested job cancellation")
    return report, checkpoint


def _resumed_live_acceptance_report(
    source: LiveValidationReport,
    *,
    report_id: DurableRecordId | None,
) -> LiveValidationReport:
    """Create a sibling observation report while preserving the original checkpoint."""
    return source.model_copy(
        deep=True,
        update={
            "report_id": report_id or f"validation_{uuid4().hex}",
            "started_at": datetime.now(UTC),
            "completed_at": None,
            "status": ValidationStatus.FAILED,
            "error": None,
            "checks": [check for check in source.checks if check.status is ValidationStatus.PASSED],
            "resources": [
                resource
                for resource in source.resources
                if resource.kind != LIVE_ACCEPTANCE_CHECKPOINT_RESOURCE_KIND
            ],
        },
    )


def _live_acceptance_pending(
    options: LiveAcceptanceOptions,
    *,
    state: _LiveAcceptanceState,
    recorder: ValidationRecorder | None,
    pending: _AcceptanceObservationPending,
) -> _LiveAcceptancePending:
    """Bind a bounded observation to the exact durable acceptance identities."""
    if recorder is None or state.primary_job_id is None:
        raise ConfigurationError("pending live acceptance omitted durable report or job identity")
    for name, value in pending.identifiers.items():
        if not hasattr(state, name):
            raise RelayError(f"pending live acceptance used an unknown identity: {name}")
        prior = getattr(state, name)
        if prior is not None and prior != value:
            raise RelayError(f"pending live acceptance changed its {name}")
        setattr(state, name, value)
    payload: dict[str, object] = {
        "schema_version": LIVE_ACCEPTANCE_CHECKPOINT_SCHEMA,
        "source_report_id": recorder.report.report_id,
        "cluster": options.cluster,
        "scenario": options.validation_scenario,
        "run_id": state.run_id,
        "phase": pending.phase,
        "intent_sha256": state.intent_sha256,
        "pipeline_sha256": state.pipeline_sha256,
        "remote_pipeline_path": state.remote_pipeline_path,
        "primary_job_id": state.primary_job_id,
        "primary_idempotency_key": state.primary_idempotency_key,
        "agent_prompt": state.agent_prompt,
        "agent_job_id": state.agent_job_id,
        "agent_child_job_id": state.agent_child_job_id,
        "pipeline_id": state.pipeline_id,
        "execution_id": state.execution_id,
        "source_job_id": state.source_job_id,
        "source_artifact_id": state.source_artifact_id,
        "service_instance_id": state.service_instance_id,
        "gateway_session_id": state.gateway_session_id,
        "scheduler_action": "none",
        "relay_action": "observe_existing",
    }
    payload["integrity_sha256"] = _live_acceptance_checkpoint_sha256(payload)
    checkpoint = LiveAcceptanceCheckpoint.model_validate(payload)
    return _LiveAcceptancePending(str(pending), checkpoint=checkpoint)


def _record_live_acceptance_pending(
    recorder: ValidationRecorder,
    pending: _LiveAcceptancePending,
) -> None:
    """Persist a nonterminal observation without manufacturing failure or cleanup."""
    checkpoint = pending.checkpoint
    recorder.report.checks = [
        check
        for check in recorder.report.checks
        if not (
            check.status is ValidationStatus.FAILED
            and check.error is not None
            and str(pending) in check.error
        )
    ]
    now = datetime.now(UTC)
    retry_selector = checkpoint.retry_selector()
    recorder.report.checks.append(
        ValidationCheck(
            check_id="live-test.observation",
            summary="bounded live acceptance observation remains resumable",
            status=ValidationStatus.PENDING,
            started_at=now,
            completed_at=now,
            evidence=[
                EvidenceReference(
                    kind="live_acceptance_resume_selector",
                    reference=(f"relay-job://{checkpoint.cluster}/{checkpoint.primary_job_id}"),
                    excerpt=str(pending),
                    metadata={
                        "retry_selector": retry_selector,
                        "scheduler_action": "none",
                        "relay_action": "observe_existing",
                        "checkpoint_has_ttl": False,
                    },
                )
            ],
        )
    )
    recorder.report.resources = [
        resource
        for resource in recorder.report.resources
        if resource.kind != LIVE_ACCEPTANCE_CHECKPOINT_RESOURCE_KIND
    ]
    recorder.add_resource(
        ValidationResource(
            kind=LIVE_ACCEPTANCE_CHECKPOINT_RESOURCE_KIND,
            resource_id=checkpoint.run_id,
            role="resume_checkpoint",
            cluster=checkpoint.cluster,
            state=ValidationStatus.PENDING.value,
            metadata={
                "checkpoint": checkpoint.model_dump(mode="json"),
                "retry_selector": retry_selector,
                "scheduler_action": "none",
                "relay_action": "observe_existing",
                "checkpoint_has_ttl": False,
            },
        )
    )
    recorder.add_resource(
        ValidationResource(
            kind="relay_job",
            resource_id=checkpoint.primary_job_id,
            role="primary",
            cluster=checkpoint.cluster,
            state="pending",
            metadata={
                "idempotency_key": checkpoint.primary_idempotency_key,
                "retained": True,
                "scheduler_cancel_requested": False,
                "resume_phase": checkpoint.phase,
            },
        )
    )
    recorder.report.status = ValidationStatus.PENDING
    recorder.report.completed_at = now
    recorder.report.error = None
    recorder.report.cleanup.cancel_scheduler_jobs = False


def run_live_acceptance(
    options: LiveAcceptanceOptions,
    *,
    runner: CommandRunner | None = None,
) -> list[str]:
    """Run live checks and persist a report even when acceptance fails."""
    command_runner = runner or _run_command
    resume_report: LiveValidationReport | None = None
    resume_checkpoint: LiveAcceptanceCheckpoint | None = None
    if options.resume_report_path is not None:
        if options.report_path is None:
            raise ConfigurationError("resuming live acceptance requires a new report path")
        if options.report_path.resolve() == options.resume_report_path.resolve():
            raise ConfigurationError(
                "--report must differ from --resume-report so the checkpoint is preserved"
            )
        resume_report, resume_checkpoint = _load_live_acceptance_resume(options)
    recorder: ValidationRecorder | None = None
    if options.report_path is not None:
        transport_modes: list[str] = []
        verify_transport = (
            options.definition.live_test.verify_transport
            if options.verify_transport is None
            else options.verify_transport
        )
        verify_direct = (
            options.definition.live_test.verify_direct_transport
            if options.verify_direct_transport is None
            else options.verify_direct_transport
        )
        if verify_transport:
            transport_modes.append("frp-relay")
        if verify_direct:
            transport_modes.append("frp-direct")
        if options.verify_ssh_transport:
            transport_modes.append("ssh-forward")
        report = (
            _resumed_live_acceptance_report(
                resume_report,
                report_id=options.report_id,
            )
            if resume_report is not None
            else new_live_validation_report(
                scenario=options.validation_scenario,
                cluster=options.cluster,
                transport_modes=transport_modes,
                launcher=options.validation_launcher,
                install_source=options.validation_install_source,
                artifact_sha256=options.validation_artifact_sha256,
                report_id=options.report_id,
            )
        )
        recorder = ValidationRecorder(report)
        if transport_modes:
            recorder.report.cleanup = CleanupEvidence(
                requested=True,
                mode="transport_probe_teardown",
                cancel_scheduler_jobs=False,
            )
    try:
        lines = _run_live_acceptance(
            options,
            runner=command_runner,
            recorder=recorder,
            resume_checkpoint=resume_checkpoint,
        )
    except _LiveAcceptancePending as pending:
        if recorder is None or options.report_path is None:
            raise ConfigurationError(
                "a pending live acceptance observation requires a machine-readable report"
            ) from pending
        _record_live_acceptance_pending(recorder, pending)
        recorder.write(options.report_path, options.markdown_report_path)
        return [
            "validation.status=pending",
            f"acceptance.run_id={pending.checkpoint.run_id}",
            f"acceptance.job_id={pending.checkpoint.primary_job_id}",
            f"acceptance.pending_phase={pending.checkpoint.phase}",
            "acceptance.scheduler_action=none",
            "acceptance.relay_action=observe_existing",
            f"validation.report={options.report_path.resolve()}",
        ]
    except BaseException as exc:
        if recorder is not None:
            for evidence_line in transport_evidence_lines_from_error(exc):
                try:
                    recorder.observe_line(evidence_line)
                except Exception as evidence_error:
                    recorder.record_failure(
                        "transport.structured-evidence",
                        "ingest structured transport cleanup evidence",
                        evidence_error,
                    )
            recorder.record_failure(
                "live-test.completed", "complete configured live acceptance", exc
            )
            recorder.finish(exc)
            assert options.report_path is not None
            recorder.write(options.report_path, options.markdown_report_path)
        raise
    if recorder is not None:
        recorder.finish()
        assert options.report_path is not None
        recorder.write(options.report_path, options.markdown_report_path)
        lines.append(f"validation.report={options.report_path.resolve()}")
    return lines


def _run_live_acceptance(
    options: LiveAcceptanceOptions,
    *,
    runner: CommandRunner,
    recorder: ValidationRecorder | None,
    resume_checkpoint: LiveAcceptanceCheckpoint | None = None,
) -> list[str]:
    """Execute the acceptance workflow while emitting structured facts."""
    command_runner = runner
    jarvis_yaml = options.jarvis_yaml or _configured_path(options.definition.live_test.jarvis_yaml)
    monitor_pattern = options.monitor_pattern or options.definition.live_test.monitor_pattern
    progress_pattern = options.progress_pattern or options.definition.live_test.progress_pattern
    progress_action_payload = (
        options.progress_action_payload
        if options.progress_action_payload
        else options.definition.live_test.progress_action_payload
    )
    agent_prompt = options.agent_prompt or options.definition.live_test.agent_prompt
    agent_child_jarvis_yaml = options.agent_child_jarvis_yaml or _configured_path(
        options.definition.live_test.agent_child_jarvis_yaml
    )
    agent_mcp_config = options.agent_mcp_config or options.definition.live_test.agent_mcp_config
    require_agent_child_job = (
        agent_mcp_config is not None
        if options.require_agent_child_job is None
        else options.require_agent_child_job
    )
    verify_transport = (
        options.definition.live_test.verify_transport
        if options.verify_transport is None
        else options.verify_transport
    )
    if jarvis_yaml is None:
        raise ConfigurationError(
            "live-test requires --jarvis-yaml or cluster live_test.jarvis_yaml"
        )
    if not jarvis_yaml.exists():
        raise ConfigurationError(f"live-test JARVIS YAML does not exist: {jarvis_yaml}")
    if agent_child_jarvis_yaml is not None and not agent_child_jarvis_yaml.exists():
        raise ConfigurationError(
            f"live-test agent child JARVIS YAML does not exist: {agent_child_jarvis_yaml}"
        )
    if agent_child_jarvis_yaml is not None and agent_mcp_config is None:
        raise ConfigurationError(
            "live-test --agent-child-jarvis-yaml requires --agent-mcp-config "
            "or cluster live_test.agent_mcp_config"
        )
    if agent_child_jarvis_yaml is not None and agent_prompt is not None:
        raise ConfigurationError(
            "live-test cannot use both an explicit agent prompt and agent_child_jarvis_yaml"
        )
    transport_token: str | None = None
    transport_secret_key: str | None = None
    verify_direct_transport = (
        options.definition.live_test.verify_direct_transport
        if options.verify_direct_transport is None
        else options.verify_direct_transport
    )
    allow_direct_transport_fallback = (
        options.definition.live_test.allow_direct_transport_fallback
        if options.allow_direct_transport_fallback is None
        else options.allow_direct_transport_fallback
    )
    if verify_transport or verify_direct_transport:
        transport_token, transport_secret_key = _require_transport_secrets(
            token=options.transport_token,
            secret_key=options.transport_secret_key,
        )
    source_pipeline_yaml = jarvis_yaml.read_text(encoding="utf-8")
    pipeline_sha256 = hashlib.sha256(source_pipeline_yaml.encode("utf-8")).hexdigest()
    intent_sha256 = _live_acceptance_intent_sha256(
        options,
        jarvis_yaml=jarvis_yaml,
        pipeline_sha256=pipeline_sha256,
        monitor_pattern=monitor_pattern,
        progress_pattern=progress_pattern,
        progress_action_payload=progress_action_payload,
        agent_prompt=agent_prompt,
        agent_mcp_config=agent_mcp_config,
        agent_child_jarvis_yaml=agent_child_jarvis_yaml,
        require_agent_child_job=require_agent_child_job,
        verify_transport=verify_transport,
        verify_direct_transport=verify_direct_transport,
        allow_direct_transport_fallback=allow_direct_transport_fallback,
    )
    if resume_checkpoint is None:
        run_id = _acceptance_run_id(jarvis_yaml)
        remote_yaml = f".local/share/clio-relay/live-tests/{run_id}/pipeline.yaml"
        state = _LiveAcceptanceState(
            run_id=run_id,
            intent_sha256=intent_sha256,
            pipeline_sha256=pipeline_sha256,
            remote_pipeline_path=remote_yaml,
            primary_idempotency_key=f"live-test:{options.cluster}:{run_id}:jarvis",
            agent_prompt=agent_prompt,
        )
    else:
        state = _LiveAcceptanceState.from_checkpoint(resume_checkpoint)
        run_id = state.run_id
        remote_yaml = state.remote_pipeline_path
        agent_prompt = state.agent_prompt
    secure_runtime_probe = _secure_runtime_probe_config(source_pipeline_yaml)
    pipeline_yaml_text = _stage_acceptance_files(
        options.definition,
        jarvis_yaml=jarvis_yaml,
        pipeline_yaml_text=source_pipeline_yaml,
        run_id=run_id,
        runner=command_runner,
        write_remote=resume_checkpoint is None,
    )
    expected_progress_adapter = _expected_progress_adapter(pipeline_yaml_text)
    expected_progress_package = _expected_progress_package(pipeline_yaml_text)
    lines: list[str] = _ValidationLines(recorder)
    if expected_progress_adapter is not None:
        if expected_progress_package is None:
            raise ConfigurationError(
                "an explicit package progress adapter requires exactly one non-empty pkg_type"
            )
        lines.append("acceptance.application_boundary=package_progress_provider")
        lines.append(f"acceptance.package_adapter={expected_progress_adapter}")
        lines.append(f"acceptance.package_owner={expected_progress_package}")

    if resume_checkpoint is None:
        lines.extend(run_cluster_doctor(options.definition))
        lines.append("acceptance.cluster_doctor=passed")
    else:
        lines.append(f"acceptance.resume_run_id={run_id}")
        lines.append(f"acceptance.resume_phase={resume_checkpoint.phase}")
    if options.verify_cluster_deployment and resume_checkpoint is None:
        lines.extend(
            _verify_cluster_deployment(
                options.definition,
                runner=command_runner,
                expected_artifact_sha256=options.validation_artifact_sha256,
                expected_install_source=(
                    recorder.report.install_source.kind.value if recorder is not None else None
                ),
            )
        )
    if secure_runtime_probe is not None and resume_checkpoint is None:
        if recorder is None:
            raise ConfigurationError(
                "secure runtime acceptance requires a machine-readable report path"
            )
        with _validation_check(
            recorder,
            "secure-runtime.control-query-capacity",
            "verify one free reserved control-query slot before source submission",
            forbidden_values=set(),
        ) as evidence:
            _require_secure_runtime_control_capacity(
                options.definition,
                cluster=options.cluster,
                runner=command_runner,
                evidence=evidence,
            )
        lines.append("secure-runtime.control_query_capacity=ready")
    if verify_transport and resume_checkpoint is None:
        assert transport_token is not None
        assert transport_secret_key is not None
        lines.extend(
            _verify_transport(
                options,
                token=transport_token,
                secret_key=transport_secret_key,
                pipeline_yaml=pipeline_yaml_text,
                expected_progress_adapter=expected_progress_adapter,
                expected_progress_package=expected_progress_package,
            )
        )
    if verify_direct_transport and resume_checkpoint is None:
        assert transport_token is not None
        assert transport_secret_key is not None
        direct_lines = _verify_direct_transport(
            options,
            token=transport_token,
            secret_key=transport_secret_key,
            allow_stcp_fallback=allow_direct_transport_fallback,
            pipeline_yaml=pipeline_yaml_text,
            expected_progress_adapter=expected_progress_adapter,
            expected_progress_package=expected_progress_package,
        )
        if not allow_direct_transport_fallback:
            _assert_direct_xtcp_acceptance(direct_lines)
        lines.extend(direct_lines)
    if options.verify_ssh_transport and resume_checkpoint is None:
        lines.extend(_verify_ssh_transport(options, pipeline_yaml=pipeline_yaml_text))
    if resume_checkpoint is None:
        _remote_write_file(
            options.definition.ssh_host,
            remote_yaml,
            pipeline_yaml_text.encode("utf-8"),
            runner=command_runner,
        )
    lines.append(f"acceptance.pipeline={remote_yaml}")
    if agent_child_jarvis_yaml is not None and resume_checkpoint is None:
        agent_prompt = _write_generated_agent_prompt(
            options.definition,
            cluster=options.cluster,
            run_id=run_id,
            child_yaml=agent_child_jarvis_yaml,
            runner=command_runner,
        )
        state.agent_prompt = agent_prompt
        lines.append(f"acceptance.agent_prompt={agent_prompt}")
    elif agent_prompt is not None:
        lines.append(f"acceptance.agent_prompt={agent_prompt}")

    if resume_checkpoint is None:
        submit = _remote_clio_json(
            options.definition,
            [
                "job",
                "submit",
                "--cluster",
                options.cluster,
                "--jarvis-yaml",
                remote_yaml,
                "--idempotency-key",
                state.primary_idempotency_key,
            ],
            runner=command_runner,
            raw_text=True,
        )
        job_id = submit.strip().splitlines()[-1]
        if not job_id.startswith("job_"):
            raise RelayError(f"live-test submit did not return a job id: {submit}")
        state.primary_job_id = job_id
    else:
        assert state.primary_job_id is not None
        job_id = state.primary_job_id
    lines.append(f"acceptance.job_id={job_id}")

    resume_phase = resume_checkpoint.phase if resume_checkpoint is not None else None
    post_primary_phases = {
        "secure_runtime_metadata",
        "secure_runtime_query",
        "secure_runtime_bind",
        "agent_job_wait",
        "agent_child_job_wait",
    }

    if expected_progress_adapter is not None and resume_checkpoint is None:
        _verify_live_package_progress(
            options.definition,
            job_id,
            expected_progress_adapter,
            package_name=expected_progress_package,
            timeout_seconds=options.timeout_seconds,
            poll_seconds=options.poll_seconds,
            runner=command_runner,
        )
        lines.append(f"acceptance.live_progress_adapter={expected_progress_adapter}")

    secure_runtime_forbidden_values: set[str] = set()
    if secure_runtime_probe is None:
        if resume_phase not in post_primary_phases:
            try:
                _wait_for_success(
                    options.definition,
                    job_id,
                    timeout_seconds=options.timeout_seconds,
                    poll_seconds=options.poll_seconds,
                    runner=command_runner,
                    pending_phase="primary_job_wait",
                )
            except _AcceptanceObservationPending as pending:
                raise _live_acceptance_pending(
                    options,
                    state=state,
                    recorder=recorder,
                    pending=pending,
                ) from None
            lines.append("acceptance.job_state=succeeded")
            if options.verify_cluster_deployment:
                lines.append("worker.execute=passed")

            live_acceptance_job_verification._verify_completed_job(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
                options.definition,
                job_id,
                line_prefix="acceptance",
                lines=lines,
                runner=command_runner,
                expected_progress_adapter=expected_progress_adapter,
                expected_progress_package=expected_progress_package,
                recorder=recorder,
                require_structured_runtime_metadata=options.require_structured_runtime_metadata,
            )
    else:
        assert recorder is not None
        if resume_phase in {"secure_runtime_query", "secure_runtime_bind"}:
            assert state.pipeline_id is not None and state.execution_id is not None
            runtime_document = {
                "pipeline_id": state.pipeline_id,
                "execution_id": state.execution_id,
            }
        else:
            try:
                with _validation_check(
                    recorder,
                    "secure-runtime.source-live-metadata",
                    "observe trusted runtime metadata while retaining the running source job",
                    forbidden_values=set(),
                ) as evidence:
                    runtime_metadata = _wait_for_live_structured_runtime_metadata(
                        options.definition,
                        job_id,
                        line_prefix="acceptance",
                        lines=lines,
                        timeout_seconds=options.timeout_seconds,
                        poll_seconds=options.poll_seconds,
                        runner=command_runner,
                    )
                    runtime_document = runtime_metadata.document
                    runtime_source = str(runtime_document["source"])
                    evidence.append(
                        EvidenceReference(
                            kind="relay_job_status",
                            reference=f"relay-job://{options.cluster}/{job_id}",
                            metadata={
                                "state": JobState.RUNNING.value,
                                "runtime_metadata_source": runtime_source,
                                "source_job_retained": True,
                                "cancel_scheduler_job": False,
                            },
                        )
                    )
                    recorder.add_resource(
                        ValidationResource(
                            kind="relay_job",
                            resource_id=job_id,
                            role="secure_runtime_source",
                            cluster=options.cluster,
                            state=JobState.RUNNING.value,
                            metadata={
                                "runtime_metadata_source": runtime_source,
                                "retained": True,
                                "cancel_scheduler_job": False,
                            },
                        )
                    )
            except _AcceptanceObservationPending as pending:
                raise _live_acceptance_pending(
                    options,
                    state=state,
                    recorder=recorder,
                    pending=pending,
                ) from None
            state.pipeline_id = cast(str, runtime_document["pipeline_id"])
            state.execution_id = cast(str, runtime_document["execution_id"])
        try:
            secure_runtime_forbidden_values = _verify_secure_runtime_acceptance(
                options,
                config=secure_runtime_probe,
                runtime_metadata=runtime_document,
                recorder=recorder,
            )
        except _AcceptanceObservationPending as pending:
            raise _live_acceptance_pending(
                options,
                state=state,
                recorder=recorder,
                pending=pending,
            ) from None
        lines.append("secure-runtime.acceptance=ok")

    resuming_agent_phase = resume_phase in {"agent_job_wait", "agent_child_job_wait"}
    if monitor_pattern is not None and not resuming_agent_phase:
        _remote_clio_json(
            options.definition,
            [
                "monitor",
                "add-regex",
                job_id,
                "--pattern",
                monitor_pattern,
                "--event-type",
                "stdout.delta",
            ],
            runner=command_runner,
        )
        actions = _remote_clio_json(
            options.definition,
            ["monitor", "run-once", "--limit", "250"],
            runner=command_runner,
        )
        if not actions:
            raise RelayError(f"acceptance monitor pattern did not match: {monitor_pattern}")
        lines.append("acceptance.monitor=ok")

    if progress_pattern is not None and not resuming_agent_phase:
        _verify_progress_monitor(
            options.definition,
            job_id,
            pattern=progress_pattern,
            action_payload=progress_action_payload,
            lines=lines,
            runner=command_runner,
        )

    if agent_prompt is not None:
        if resuming_agent_phase:
            assert state.agent_job_id is not None
            agent_job_id = state.agent_job_id
        else:
            agent_args = [
                "agent",
                "run",
                "--cluster",
                options.cluster,
                "--prompt",
                agent_prompt,
                "--idempotency-key",
                f"live-test:{options.cluster}:{run_id}:agent",
            ]
            if agent_mcp_config is not None:
                agent_args.extend(["--mcp-config", agent_mcp_config])
            agent_submit = _remote_clio_json(
                options.definition,
                agent_args,
                runner=command_runner,
                raw_text=True,
            )
            agent_job_id = agent_submit.strip().splitlines()[-1]
            if not agent_job_id.startswith("job_"):
                raise RelayError(f"live-test agent submit did not return a job id: {agent_submit}")
            state.agent_job_id = agent_job_id
        if resume_phase != "agent_child_job_wait":
            try:
                agent_job = _wait_for_success(
                    options.definition,
                    agent_job_id,
                    timeout_seconds=options.timeout_seconds,
                    poll_seconds=options.poll_seconds,
                    runner=command_runner,
                    pending_phase="agent_job_wait",
                )
            except _AcceptanceObservationPending as pending:
                raise _live_acceptance_pending(
                    options,
                    state=state,
                    recorder=recorder,
                    pending=pending,
                ) from None
        else:
            agent_job = {}
        lines.append(f"acceptance.agent_job_id={agent_job_id}")
        lines.append("acceptance.agent_state=succeeded")
        if require_agent_child_job:
            if resume_phase == "agent_child_job_wait":
                assert state.agent_child_job_id is not None
                child_job_id = state.agent_child_job_id
            else:
                child_job_id = _find_agent_child_job(
                    options.definition,
                    agent_job_id,
                    agent_created_at=str(agent_job["created_at"]),
                    runner=command_runner,
                )
                state.agent_child_job_id = child_job_id
            try:
                _wait_for_success(
                    options.definition,
                    child_job_id,
                    timeout_seconds=options.timeout_seconds,
                    poll_seconds=options.poll_seconds,
                    runner=command_runner,
                    pending_phase="agent_child_job_wait",
                )
            except _AcceptanceObservationPending as pending:
                raise _live_acceptance_pending(
                    options,
                    state=state,
                    recorder=recorder,
                    pending=pending,
                ) from None
            lines.append(f"acceptance.agent_child_job_id={child_job_id}")
            live_acceptance_job_verification._verify_completed_job(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
                options.definition,
                child_job_id,
                line_prefix="acceptance.agent_child",
                lines=lines,
                runner=command_runner,
                expected_progress_adapter=expected_progress_adapter,
                expected_progress_package=expected_progress_package,
                recorder=recorder,
                require_structured_runtime_metadata=options.require_structured_runtime_metadata,
            )

    lines.append("live acceptance passed")
    expected_transport_cleanups = (
        0
        if resume_checkpoint is not None
        else sum([verify_transport, verify_direct_transport, options.verify_ssh_transport])
    )
    observed_transport_cleanups = lines.count("transport.cleanup=passed")
    if observed_transport_cleanups < expected_transport_cleanups:
        raise RelayError(
            "transport cleanup evidence is incomplete: "
            f"expected={expected_transport_cleanups} observed={observed_transport_cleanups}"
        )
    if recorder is not None and recorder.transport_probe_count < expected_transport_cleanups:
        raise RelayError(
            "structured transport cleanup evidence is incomplete: "
            f"expected={expected_transport_cleanups} observed={recorder.transport_probe_count}"
        )
    if recorder is not None and recorder.report.cleanup.remaining_resources:
        raise RelayError(
            "transport cleanup left structured residual resources: "
            f"count={len(recorder.report.cleanup.remaining_resources)}"
        )
    if recorder is not None and secure_runtime_probe is not None:
        _assert_secret_free_document(
            recorder.report.model_dump(mode="json"),
            forbidden_values=secure_runtime_forbidden_values,
            label="secure runtime validation report",
        )
    return lines


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
            while True:
                remaining = query_deadline - time.monotonic()
                if remaining <= 0:
                    raise _AcceptanceObservationPending(
                        "timed out waiting for one ready JARVIS service runtime binding: "
                        f"{execution_id}",
                        phase="secure_runtime_query",
                        identifiers={
                            "pipeline_id": pipeline_id,
                            "execution_id": execution_id,
                        },
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
                    raise _AcceptanceObservationPending(
                        "timed out observing one ready JARVIS service runtime binding: "
                        f"{execution_id}",
                        phase="secure_runtime_query",
                        identifiers={
                            "pipeline_id": pipeline_id,
                            "execution_id": execution_id,
                        },
                    ) from None
                if time.monotonic() >= query_deadline:
                    raise _AcceptanceObservationPending(
                        "timed out waiting for one ready JARVIS service runtime binding: "
                        f"{execution_id}",
                        phase="secure_runtime_query",
                        identifiers={
                            "pipeline_id": pipeline_id,
                            "execution_id": execution_id,
                        },
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
                    raise _AcceptanceObservationPending(
                        "timed out waiting for one ready JARVIS service runtime binding: "
                        f"{execution_id}",
                        phase="secure_runtime_query",
                        identifiers={
                            "pipeline_id": pipeline_id,
                            "execution_id": execution_id,
                        },
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
                initial_health, initial_health_document = (
                    live_acceptance_browser_evidence._browser_json_observation(  # pyright: ignore[reportPrivateUsage]  # noqa: E501, SLF001
                        active_attachment.health_url,
                        endpoint="health",
                        method="GET",
                        body=None,
                        timeout_seconds=min(options.timeout_seconds, 60.0),
                    )
                )
                initial_health, initial_health_revision = (
                    _correlate_secure_runtime_browser_document(
                        initial_health_document,
                        initial_health,
                        endpoint="health",
                        adapter=config.protocol_adapter.health,
                        expected_service_instance_id=binding.service_instance_id,
                        expected_execution_id=binding.jarvis_execution_id,
                        expected_dataset_descriptor_sha256=binding.dataset_descriptor_sha256,
                        expected_command_id=None,
                    )
                )
                initial_state, initial_state_document = (
                    live_acceptance_browser_evidence._browser_json_observation(  # pyright: ignore[reportPrivateUsage]  # noqa: E501, SLF001
                        active_attachment.state_url,
                        endpoint="state",
                        method="GET",
                        body=None,
                        timeout_seconds=min(options.timeout_seconds, 60.0),
                    )
                )
                initial_state, initial_state_revision = _correlate_secure_runtime_browser_document(
                    initial_state_document,
                    initial_state,
                    endpoint="state",
                    adapter=config.protocol_adapter.state,
                    expected_service_instance_id=binding.service_instance_id,
                    expected_execution_id=binding.jarvis_execution_id,
                    expected_dataset_descriptor_sha256=binding.dataset_descriptor_sha256,
                    expected_command_id=None,
                )
                initial_event, initial_event_document = (
                    live_acceptance_browser_evidence._browser_sse_observation(  # pyright: ignore[reportPrivateUsage]  # noqa: E501, SLF001
                        active_attachment.events_url,
                        timeout_seconds=min(options.timeout_seconds, 60.0),
                        expected_event_name=event_name,
                    )
                )
                initial_event, initial_event_revision = _correlate_secure_runtime_browser_document(
                    initial_event_document,
                    initial_event,
                    endpoint="events",
                    adapter=config.protocol_adapter.events,
                    expected_service_instance_id=binding.service_instance_id,
                    expected_execution_id=binding.jarvis_execution_id,
                    expected_dataset_descriptor_sha256=binding.dataset_descriptor_sha256,
                    expected_command_id=None,
                )
                if {
                    initial_health_revision,
                    initial_state_revision,
                    initial_event_revision,
                } != {binding.service_revision}:
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
                if command_revision <= initial_state_revision:
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
                reconnected_health, reconnected_health_document = (
                    live_acceptance_browser_evidence._browser_json_observation(  # pyright: ignore[reportPrivateUsage]  # noqa: E501, SLF001
                        active_attachment.health_url,
                        endpoint="health",
                        method="GET",
                        body=None,
                        timeout_seconds=min(options.timeout_seconds, 60.0),
                    )
                )
                reconnected_health, reconnected_health_revision = (
                    _correlate_secure_runtime_browser_document(
                        reconnected_health_document,
                        reconnected_health,
                        endpoint="health",
                        adapter=config.protocol_adapter.health,
                        expected_service_instance_id=binding.service_instance_id,
                        expected_execution_id=binding.jarvis_execution_id,
                        expected_dataset_descriptor_sha256=binding.dataset_descriptor_sha256,
                        expected_command_id=None,
                    )
                )
                reconnected_state, reconnected_state_document = (
                    live_acceptance_browser_evidence._browser_json_observation(  # pyright: ignore[reportPrivateUsage]  # noqa: E501, SLF001
                        active_attachment.state_url,
                        endpoint="state",
                        method="GET",
                        body=None,
                        timeout_seconds=min(options.timeout_seconds, 60.0),
                    )
                )
                reconnected_state, reconnected_state_revision = (
                    _correlate_secure_runtime_browser_document(
                        reconnected_state_document,
                        reconnected_state,
                        endpoint="state",
                        adapter=config.protocol_adapter.state,
                        expected_service_instance_id=binding.service_instance_id,
                        expected_execution_id=binding.jarvis_execution_id,
                        expected_dataset_descriptor_sha256=binding.dataset_descriptor_sha256,
                        expected_command_id=command_id,
                    )
                )
                reconnected_event, reconnected_event_document = (
                    live_acceptance_browser_evidence._browser_sse_observation(  # pyright: ignore[reportPrivateUsage]  # noqa: E501, SLF001
                        active_attachment.events_url,
                        timeout_seconds=min(options.timeout_seconds, 60.0),
                        expected_event_name=event_name,
                    )
                )
                reconnected_event, reconnected_event_revision = (
                    _correlate_secure_runtime_browser_document(
                        reconnected_event_document,
                        reconnected_event,
                        endpoint="events",
                        adapter=config.protocol_adapter.events,
                        expected_service_instance_id=binding.service_instance_id,
                        expected_execution_id=binding.jarvis_execution_id,
                        expected_dataset_descriptor_sha256=binding.dataset_descriptor_sha256,
                        expected_command_id=command_id,
                    )
                )
                if {
                    reconnected_health_revision,
                    reconnected_state_revision,
                    reconnected_event_revision,
                } != {command_revision}:
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
                    for session in _gateway_sessions_for_acceptance(
                        runtime_queue,
                        cluster=options.cluster,
                    )
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
                    _record_runtime_cleanup(
                        recorder,
                        cleanup,
                        role="secure_runtime_failure_cleanup",
                    )
                    if cleanup.errors or cleanup.residual_resources:
                        cleanup_errors.extend(
                            _redacted_text(item, forbidden_values) for item in cleanup.errors
                        )
                except Exception as cleanup_exc:
                    cleanup_errors.append(_redacted_error_text(cleanup_exc, forbidden_values))
            if cleanup_errors and primary_error is not None:
                primary_error.add_note("secure runtime cleanup: " + "; ".join(cleanup_errors))


def _require_secure_runtime_control_capacity(
    definition: ClusterDefinition,
    *,
    cluster: str,
    runner: CommandRunner,
    evidence: list[EvidenceReference] | None = None,
) -> dict[str, object]:
    """Return verified free control-query capacity before scheduling a service."""
    raw_status = _remote_clio_json(
        definition,
        ["worker", "status", "--cluster", cluster],
        runner=runner,
    )
    if not isinstance(raw_status, dict):
        raise RelayError("secure runtime worker status was not a JSON object")
    status = cast(dict[str, object], raw_status)
    configured_control = status.get("configured_control_query_concurrency")
    configured_workload = status.get("configured_workload_concurrency")
    consistent = status.get("control_query_concurrency_consistent")
    scan_truncated = status.get("scan_truncated")
    active_raw = status.get("active_leases_by_mcp_admission_class")
    active = cast(dict[str, object], active_raw) if isinstance(active_raw, dict) else None
    active_control = None if active is None else active.get("control_query")
    observed: dict[str, object] = {
        "configured_workload_concurrency": configured_workload,
        "configured_control_query_concurrency": configured_control,
        "active_control_query_leases": active_control,
        "control_query_concurrency_consistent": consistent,
        "scan_truncated": scan_truncated,
        "worker_generation_id": status.get("worker_generation_id"),
        "worker_generation_complete": status.get("worker_generation_complete"),
        "source_submitted": False,
        "scheduler_job_created": False,
    }
    if (
        type(configured_control) is int
        and type(active_control) is int
        and configured_control >= active_control
    ):
        observed["free_control_query_slots"] = configured_control - active_control
    if evidence is not None:
        evidence.append(
            EvidenceReference(
                kind="worker_capacity",
                reference=f"relay-worker://{cluster}/control-query",
                metadata=observed,
            )
        )
    if scan_truncated is not False:
        raise RelayError("secure runtime worker-capacity scan was incomplete")
    if consistent is not True:
        raise RelayError("secure runtime worker control-query policy is inconsistent")
    if type(configured_workload) is not int or configured_workload < 1:
        raise RelayError("secure runtime requires at least one workload worker slot")
    if type(configured_control) is not int or configured_control < 1:
        raise RelayError("secure runtime requires at least one reserved control-query slot")
    if type(active_control) is not int or active_control < 0:
        raise RelayError("secure runtime worker status omitted active control-query usage")
    if active_control >= configured_control:
        raise RelayError("secure runtime has no free reserved control-query slot")
    observed.update(
        {
            "free_control_query_slots": configured_control - active_control,
            "control_query_concurrency_consistent": True,
            "scan_truncated": False,
        }
    )
    return observed


def _wait_for_live_structured_runtime_metadata(
    definition: ClusterDefinition,
    job_id: str,
    *,
    line_prefix: str,
    lines: list[str],
    timeout_seconds: float,
    poll_seconds: float,
    runner: CommandRunner,
) -> RuntimeMetadataAcceptance:
    """Wait for trusted runtime metadata without waiting for its source job to finish."""
    deadline = time.monotonic() + timeout_seconds
    structured_sources = {
        RuntimeMetadataSource.JARVIS_MCP,
        RuntimeMetadataSource.JARVIS_SIDECAR,
    }
    while True:
        raw_status = _remote_clio_json(
            definition,
            ["job", "status", job_id],
            runner=runner,
        )
        if not isinstance(raw_status, dict):
            raise RelayError("secure runtime source job status was not a JSON object")
        status = cast(dict[str, Any], raw_status)
        raw_job = status.get("job")
        try:
            job = RelayJob.model_validate(raw_job)
        except ValueError as exc:
            raise RelayError(f"secure runtime source RelayJob was invalid: {exc}") from exc
        if job.job_id != job_id:
            raise RelayError(
                "secure runtime source job status changed identity: "
                f"expected={job_id} observed={job.job_id}"
            )
        reported_terminal = status.get("terminal")
        actual_terminal = job.state in TERMINAL_STATES
        if not isinstance(reported_terminal, bool) or reported_terminal is not actual_terminal:
            raise RelayError("secure runtime source job status had inconsistent terminal state")
        if actual_terminal:
            lines.append(f"{line_prefix}.job_state={job.state.value}")
            if job.state.value in {"failed", "canceled"}:
                raise RelayError(
                    "secure runtime source job "
                    f"{job.state.value} before structured runtime metadata was usable"
                )
            raise RelayError(
                "secure runtime source job succeeded before a live structured runtime was available"
            )

        raw_runtime = job.metadata.get("runtime_metadata")
        if raw_runtime is not None:
            if not isinstance(raw_runtime, dict):
                raise RelayError("secure runtime source metadata was not a JSON object")
            try:
                validated = JarvisRuntimeMetadata.model_validate(raw_runtime)
            except ValueError as exc:
                raise RelayError(f"secure runtime source metadata was invalid: {exc}") from exc
            if validated.schema_version != RUNTIME_METADATA_SCHEMA:
                raise RelayError(
                    "secure runtime source metadata used an unsupported schema version: "
                    f"{validated.schema_version}"
                )
            if validated.source in structured_sources:
                if not validated.pipeline_id or not validated.execution_id:
                    raise RelayError(
                        "secure runtime source metadata omitted pipeline_id or execution_id"
                    )
                if job.state is not JobState.RUNNING:
                    if time.monotonic() >= deadline:
                        lines.append(f"{line_prefix}.job_state={job.state.value}")
                        raise _AcceptanceObservationPending(
                            "timed out waiting for the secure runtime source job to run; "
                            "the bounded observation expired while the retained job remained "
                            f"{job.state.value}: {job_id}",
                            phase="secure_runtime_metadata",
                            identifiers={"primary_job_id": job_id},
                        )
                    time.sleep(poll_seconds)
                    continue
                document = validated.model_dump(mode="json")
                lines.append(f"{line_prefix}.job_state={job.state.value}")
                lines.extend(
                    _runtime_metadata_document_facts(
                        document,
                        line_prefix=line_prefix,
                    )
                )
                lines.append(f"{line_prefix}.source_job_retained=ok")
                return RuntimeMetadataAcceptance(document=document, structured=True)

        if time.monotonic() >= deadline:
            lines.append(f"{line_prefix}.job_state={job.state.value}")
            raise _AcceptanceObservationPending(
                "timed out waiting for structured runtime metadata from secure runtime source "
                f"job; the bounded observation expired without changing the workload: {job_id}",
                phase="secure_runtime_metadata",
                identifiers={"primary_job_id": job_id},
            )
        time.sleep(poll_seconds)


def _wait_for_success(
    definition: ClusterDefinition,
    job_id: str,
    *,
    timeout_seconds: float,
    poll_seconds: float,
    runner: CommandRunner,
    pending_phase: Literal[
        "primary_job_wait",
        "agent_job_wait",
        "agent_child_job_wait",
    ] = "primary_job_wait",
) -> dict[str, Any]:
    job = _remote_clio_json(
        definition,
        [
            "job",
            "wait",
            job_id,
            "--timeout-seconds",
            str(timeout_seconds),
            "--poll-seconds",
            str(poll_seconds),
        ],
        runner=runner,
    )
    if not isinstance(job, dict):
        raise RelayError("acceptance job wait did not return a JSON object")
    typed = cast(dict[str, Any], job)
    observed_job_id = typed.get("job_id")
    state = typed.get("state")
    if observed_job_id != job_id or not isinstance(state, str):
        raise RelayError("acceptance job wait changed or omitted its durable identity")
    if state == "succeeded":
        return typed
    if state in {"failed", "canceled"}:
        raise RelayError(f"acceptance job did not succeed: {state}")
    raise _AcceptanceObservationPending(
        f"bounded observation expired while acceptance job remained {state}: {job_id}",
        phase=pending_phase,
        identifiers={
            (
                "primary_job_id"
                if pending_phase == "primary_job_wait"
                else "agent_job_id"
                if pending_phase == "agent_job_wait"
                else "agent_child_job_id"
            ): job_id
        },
    )


def _verify_live_package_progress(
    definition: ClusterDefinition,
    job_id: str,
    expected_adapter: str,
    *,
    package_name: str | None,
    timeout_seconds: float,
    poll_seconds: float,
    runner: CommandRunner,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    saw_running = False
    while time.monotonic() < deadline:
        monitor = _remote_clio_json(
            definition,
            ["job", "monitor", job_id, "--cursor", "1", "--limit", "500"],
            runner=runner,
        )
        events = cast(list[dict[str, Any]], monitor["events"])
        event_types = {str(event.get("event_type")) for event in events}
        saw_running = saw_running or "job.running" in event_types
        progress = _remote_job_collection(
            definition,
            ["job", "progress", job_id],
            record_key="progress",
            label=f"live package progress for {job_id}",
            runner=runner,
        )
        if _has_progress_adapter(
            progress,
            expected_adapter,
            job_id=job_id,
            package_name=package_name,
        ):
            if not saw_running and not _remote_job_has_event(
                definition,
                job_id,
                "job.running",
                runner=runner,
            ):
                raise RelayError("package progress was recorded before job.running")
            return
        status = _remote_clio_json(
            definition,
            ["job", "status", job_id],
            runner=runner,
        )
        runtime_metadata = _runtime_metadata_from_job_status(status, job_id=job_id)
        native_attestation = _native_progress_attestation(
            runtime_metadata,
            expected_adapter,
            package_name=package_name,
            require_nonterminal=True,
        )
        if native_attestation is not None:
            if not saw_running and not _remote_job_has_event(
                definition,
                job_id,
                "job.running",
                runner=runner,
            ):
                raise RelayError("package progress was recorded before job.running")
            return
        if event_types & {"job.succeeded", "job.failed", "job.canceled"}:
            break
        time.sleep(poll_seconds)
    raise RelayError(
        f"expected live package progress before terminal job state: {expected_adapter}"
    )

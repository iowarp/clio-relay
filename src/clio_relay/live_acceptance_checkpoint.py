"""Resumable checkpoint intent, resume validation, and pending-observation recording.

Extracted from ``live_acceptance.py`` (#231 rework): the concern of
binding a run's every semantic input to one intent digest, validating a
``--resume-report`` checkpoint completely before any remote command
executes, and recording (or resuming into) a nonterminal observation so a
bounded wait's expiry never manufactures a false failure or cleanup.
"""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.identifiers import DurableRecordId
from clio_relay.live_acceptance_models import (
    LIVE_ACCEPTANCE_CHECKPOINT_RESOURCE_KIND,
    LIVE_ACCEPTANCE_CHECKPOINT_SCHEMA,
    LiveAcceptanceCheckpoint,
    LiveAcceptanceOptions,
    _AcceptanceObservationPending,
    _configured_path,
    _live_acceptance_checkpoint_sha256,
    _LiveAcceptancePending,
    _LiveAcceptanceState,
)
from clio_relay.validation_report import (
    EvidenceReference,
    LiveValidationReport,
    ValidationCheck,
    ValidationRecorder,
    ValidationResource,
    ValidationStatus,
    load_validation_report,
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

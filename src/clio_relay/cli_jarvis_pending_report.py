"""JARVIS validation pending-report construction (iowarp/clio-relay#231
continuation): the helpers that build the typed "still pending"
reports ``jarvis-mcp-validate`` emits while a dispatch or query has not
yet reached a terminal state."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any, cast

import clio_relay.cli_jarvis_execution_types as cli_jarvis_execution_types
import clio_relay.jarvis_mcp_validation as jarvis_mcp_validation
from clio_relay.validation_report import (
    EvidenceReference,
    LiveValidationReport,
    ValidationCheck,
    ValidationResource,
    ValidationStatus,
    new_live_validation_report,
)


def _new_jarvis_intent_pending_report(
    checkpoint: dict[str, Any],
) -> LiveValidationReport:
    """Represent an ambiguous stdio response as replayable intent, never workload failure."""
    selector = cast(dict[str, object], checkpoint["retry_selector"])
    inputs = cast(dict[str, object], checkpoint["pre_dispatch_inputs"])
    report = new_live_validation_report(
        scenario="remote-mcp",
        cluster=cast(str, selector["cluster"]),
        launcher=cast(str | None, inputs.get("launcher")),
        install_source=cast(str | None, inputs.get("install_source")),
        artifact_sha256=cast(str | None, inputs.get("artifact_sha256")),
    )
    now = datetime.now(UTC)
    report.completed_at = now
    report.status = ValidationStatus.PENDING
    report.checks = [
        ValidationCheck(
            check_id="remote-mcp.jarvis-run-intent",
            summary="idempotent jarvis_run dispatch response remains observable",
            status=ValidationStatus.PENDING,
            started_at=report.started_at,
            completed_at=now,
            evidence=[
                EvidenceReference(
                    kind="jarvis_run_intent_resume_selector",
                    excerpt=json.dumps(selector, sort_keys=True),
                    metadata={
                        **selector,
                        "scheduler_action": "none",
                        "relay_action": "replay_same_idempotency_key",
                    },
                )
            ],
        )
    ]
    report.resources = [
        ValidationResource(
            kind="jarvis_dispatch_intent",
            resource_id=cast(str, selector["execution_intent_sha256"]),
            role="resumable_jarvis_run_intent",
            cluster=cast(str, selector["cluster"]),
            state="response_unobserved",
            metadata={
                "retry_selector": selector,
                "outcome": "observation_pending",
                "scheduler_action": "none",
                "relay_action": "replay_same_idempotency_key",
                "resume_checkpoint": checkpoint,
            },
        )
    ]
    return report


def _convert_jarvis_checks_to_pending(
    report: LiveValidationReport,
    *,
    pending_check_ids: frozenset[str],
    resource: ValidationResource,
) -> LiveValidationReport:
    """Downgrade only checks whose evidence is unavailable within this observation window."""
    failed_ids = {
        check.check_id for check in report.checks if check.status is ValidationStatus.FAILED
    }
    if not failed_ids or failed_ids - pending_check_ids:
        return report
    updated_checks = [
        check.model_copy(update={"status": ValidationStatus.PENDING, "error": None})
        if check.status is ValidationStatus.FAILED and check.check_id in pending_check_ids
        else check
        for check in report.checks
    ]
    return report.model_copy(
        update={
            "status": ValidationStatus.PENDING,
            "error": None,
            "checks": updated_checks,
            "resources": [*report.resources, resource],
        }
    )


def _build_jarvis_dispatch_pending_report(
    checkpoint: dict[str, Any],
) -> LiveValidationReport:
    """Retain one accepted relay job while its terminal result remains unobserved."""
    builder_inputs = cast(dict[str, Any], checkpoint["builder_inputs"])
    selector = cast(dict[str, object], checkpoint["retry_selector"])
    report = jarvis_mcp_validation.build_jarvis_mcp_validation_report(
        **builder_inputs,
        query_tools_list_response=None,
        query_call_response=None,
        query_call_job_id="",
        query_call_status={},
        query_artifacts=[],
        query_mcp_result=None,
        query_provenance=None,
        query_initialize_response=None,
        query_stdio_evidence=None,
        query_lifecycle_observations=[],
    )
    resource = ValidationResource(
        kind="relay_job",
        resource_id=cast(str, selector["relay_job_id"]),
        role="resumable_jarvis_run_dispatch",
        cluster=cast(str, selector["cluster"]),
        state="observation_pending",
        metadata={
            "retry_selector": selector,
            "outcome": "observation_pending",
            "scheduler_action": "none",
            "relay_action": "retain",
            "resume_checkpoint": checkpoint,
        },
    )
    return _convert_jarvis_checks_to_pending(
        report,
        pending_check_ids=frozenset(
            {
                "remote-mcp.jarvis-call",
                "remote-mcp.server-artifact",
                "remote-mcp.durable-result",
                "remote-mcp.jarvis-live-progress",
                "jarvis.spack-runtime-environment",
                "jarvis.structured-runtime-metadata",
                "remote-mcp.jarvis-execution-query",
            }
        ),
        resource=resource,
    )


def _build_unobserved_jarvis_query_pending_report(
    *,
    builder_inputs: dict[str, Any],
    execution_query: cli_jarvis_execution_types._JarvisExecutionQueryPending,
    checkpoint: dict[str, Any],
) -> LiveValidationReport:
    """Retain exact execution identity when no query result arrives in the window."""
    import clio_relay.cli as cli

    selector = execution_query.retry_selector()
    report = jarvis_mcp_validation.build_jarvis_mcp_validation_report(
        **builder_inputs,
        query_tools_list_response=None,
        query_call_response=None,
        query_call_job_id="",
        query_call_status={},
        query_artifacts=[],
        query_mcp_result=None,
        query_provenance=None,
        query_initialize_response=None,
        query_stdio_evidence=None,
        query_lifecycle_observations=[],
    )
    provider = selector.get("scheduler_provider")
    resource = ValidationResource(
        kind="jarvis_execution",
        resource_id=execution_query.execution_id,
        role="resumable_acceptance_workload",
        cluster=execution_query.cluster,
        provider=provider if isinstance(provider, str) else None,
        state="observation_pending",
        metadata={
            "retry_selector": selector,
            "outcome": execution_query.outcome,
            "scheduler_action": execution_query.scheduler_action,
            "relay_action": execution_query.relay_action,
            "resume_checkpoint": checkpoint,
        },
    )
    return _convert_jarvis_checks_to_pending(
        report,
        pending_check_ids=cli._JARVIS_NONTERMINAL_VALIDATION_CHECKS,
        resource=resource,
    )


def _mark_jarvis_validation_pending(
    report: LiveValidationReport,
    *,
    execution_query: cli_jarvis_execution_types._JarvisExecutionQueryAcceptance,
    resume_checkpoint: dict[str, Any] | None = None,
) -> LiveValidationReport:
    """Convert only terminal-dependent failures into honest resumable evidence."""
    import clio_relay.cli as cli

    failed_check_ids = {
        check.check_id for check in report.checks if check.status is ValidationStatus.FAILED
    }
    unexpected_failures = failed_check_ids - cli._JARVIS_NONTERMINAL_VALIDATION_CHECKS
    if unexpected_failures or not _jarvis_nonterminal_failures_are_resumable(
        report,
        execution_query=execution_query,
    ):
        return report
    selector = execution_query.retry_selector()
    latest = execution_query.lifecycle_observations[-1]
    updated_checks = [
        check.model_copy(
            update={
                "status": ValidationStatus.PENDING,
                "error": None,
                "evidence": [
                    *check.evidence,
                    EvidenceReference(
                        kind="jarvis_execution_resume_selector",
                        excerpt=json.dumps(selector, sort_keys=True),
                        metadata={
                            **selector,
                            "scheduler_action": execution_query.scheduler_action,
                            "relay_action": execution_query.relay_action,
                        },
                    ),
                ],
            }
        )
        if check.check_id in cli._JARVIS_NONTERMINAL_VALIDATION_CHECKS
        and check.status is ValidationStatus.FAILED
        else check
        for check in report.checks
    ]
    provider = selector.get("scheduler_provider")
    resource = ValidationResource(
        kind="jarvis_execution",
        resource_id=execution_query.execution_id,
        role="resumable_acceptance_workload",
        cluster=execution_query.cluster,
        provider=provider if isinstance(provider, str) else None,
        state=str(latest.get("state")) if latest.get("state") is not None else None,
        metadata={
            "retry_selector": selector,
            "outcome": execution_query.outcome,
            "scheduler_action": execution_query.scheduler_action,
            "relay_action": execution_query.relay_action,
            **({"resume_checkpoint": resume_checkpoint} if resume_checkpoint is not None else {}),
        },
    )
    return report.model_copy(
        update={
            "status": ValidationStatus.PENDING,
            "error": None,
            "checks": updated_checks,
            "resources": [*report.resources, resource],
        }
    )


def _jarvis_nonterminal_failures_are_resumable(
    report: LiveValidationReport,
    *,
    execution_query: cli_jarvis_execution_types._JarvisExecutionQueryAcceptance,
) -> bool:
    """Require all nonterminal integrity assertions before downgrading terminal checks."""
    required_assertions = {
        "remote-mcp.jarvis-live-progress": {
            "observation_count_bounded",
            "query_identities_coherent",
            "scheduler_identity_optional_coherent_and_stable",
            "lifecycle_prefix_coherent",
            "package_progress_nonregressing",
        },
        "remote-mcp.jarvis-execution-query": {
            "local_query_surface_verified",
            "server_artifact_binding_verified",
            "resumable_query_job_verified",
            "resumable_result_transport_verified",
            "resumable_result_envelope_verified",
            "resumable_identity_coherent",
            "resumable_lifecycle_coherent",
            "resumable_runner_semantic_validation_verified",
        },
    }
    if not execution_query.lifecycle_observations:
        return False
    latest = execution_query.lifecycle_observations[-1]
    terminal = latest.get("terminal")
    if execution_query.outcome == "observation_unknown" and terminal is not False:
        return False
    if execution_query.outcome == "terminal_artifacts_pending" and terminal is not True:
        return False
    for check in report.checks:
        required = required_assertions.get(check.check_id)
        if check.status is not ValidationStatus.FAILED or required is None:
            continue
        if len(check.evidence) != 1:
            return False
        assertions = check.evidence[0].metadata.get("assertions")
        if not isinstance(assertions, dict) or not all(
            cast(dict[str, object], assertions).get(name) is True for name in required
        ):
            return False
    return True

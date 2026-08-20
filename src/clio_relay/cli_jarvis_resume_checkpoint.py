"""JARVIS resume-checkpoint loading and validation (iowarp/clio-relay#231
continuation): reads a prior dispatch's durable checkpoint back and
validates it before ``jarvis-mcp-validate`` resumes from it."""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any, cast

from pydantic import ValidationError

import clio_relay.cli_jarvis_artifact_io as cli_jarvis_artifact_io
import clio_relay.cli_jarvis_intent_checkpoint as cli_jarvis_intent_checkpoint
import clio_relay.cli_jarvis_query_observation as cli_jarvis_query_observation
from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.runtime_metadata import RUNTIME_METADATA_SCHEMA, native_execution_documents
from clio_relay.validation_report import (
    ValidationResource,
    ValidationStatus,
    load_validation_report,
)


def _load_jarvis_validation_resume_checkpoint(
    path: Path,
    *,
    cluster: str,
) -> dict[str, Any]:
    """Load one exact pending acceptance checkpoint without trusting caller selectors."""
    import clio_relay.cli as cli

    report = load_validation_report(path)
    if report.scenario != "remote-mcp" or report.cluster != cluster:
        raise ConfigurationError(
            "JARVIS validation resume report does not match the requested cluster/scenario"
        )
    if report.status is not ValidationStatus.PENDING:
        raise ConfigurationError("JARVIS validation resume requires a pending report")
    candidates = [
        (resource, resource.metadata.get("resume_checkpoint"))
        for resource in report.resources
        if isinstance(resource.metadata.get("resume_checkpoint"), dict)
        and (
            (
                resource.kind == "jarvis_execution"
                and resource.role == "resumable_acceptance_workload"
            )
            or (resource.kind == "relay_job" and resource.role == "resumable_jarvis_run_dispatch")
            or (
                resource.kind == "jarvis_dispatch_intent"
                and resource.role == "resumable_jarvis_run_intent"
            )
        )
    ]
    if len(candidates) != 1:
        raise ConfigurationError(
            "pending JARVIS validation report must contain one resume checkpoint"
        )
    resource, raw_checkpoint = candidates[0]
    checkpoint = cast(dict[str, Any], raw_checkpoint)
    schema_version = checkpoint.get("schema_version")
    if schema_version == cli._JARVIS_VALIDATION_RESUME_CHECKPOINT_SCHEMA_V1:
        phase = cli._JARVIS_VALIDATION_PHASE_QUERY
    elif schema_version == cli._JARVIS_VALIDATION_RESUME_CHECKPOINT_SCHEMA:
        phase = checkpoint.get("phase")
    else:
        raise ConfigurationError("pending JARVIS validation resume checkpoint is invalid")
    if phase in {cli._JARVIS_VALIDATION_PHASE_INTENT, cli._JARVIS_VALIDATION_PHASE_DISPATCH}:
        return _validate_jarvis_dispatch_resume_checkpoint(
            checkpoint,
            resource=resource,
            cluster=cluster,
        )
    if phase != cli._JARVIS_VALIDATION_PHASE_QUERY:
        raise ConfigurationError("pending JARVIS validation resume checkpoint phase is invalid")
    selector = checkpoint.get("retry_selector")
    builder_inputs = checkpoint.get("builder_inputs")
    observations = checkpoint.get("lifecycle_observations")
    profile = checkpoint.get("profile")
    unobserved = (
        schema_version == cli._JARVIS_VALIDATION_RESUME_CHECKPOINT_SCHEMA
        and checkpoint.get("observation_state") == "not_observed"
    )
    if (
        not isinstance(selector, dict)
        or resource.kind != "jarvis_execution"
        or resource.role != "resumable_acceptance_workload"
        or cast(dict[str, object], selector).get("cluster") != cluster
        or not isinstance(cast(dict[str, object], selector).get("pipeline_id"), str)
        or not cast(str, cast(dict[str, object], selector).get("pipeline_id"))
        or not isinstance(cast(dict[str, object], selector).get("execution_id"), str)
        or not cast(str, cast(dict[str, object], selector).get("execution_id"))
        or not isinstance(builder_inputs, dict)
        or cast(dict[str, object], builder_inputs).get("cluster") != cluster
        or "scheduler_cluster" not in cast(dict[str, object], builder_inputs)
        or cast(dict[str, object], builder_inputs).get("tool") != "jarvis_run"
        or not isinstance(cast(dict[str, object], builder_inputs).get("runtime_metadata"), dict)
        or not isinstance(observations, list)
        or (not observations and not unobserved)
        or (bool(cast(list[object], observations)) and unobserved)
        or len(cast(list[object], observations)) > cli._MAX_JARVIS_EXECUTION_QUERY_OBSERVATIONS
        or profile not in {"user", "admin", "operator", "all"}
        or (
            schema_version == cli._JARVIS_VALIDATION_RESUME_CHECKPOINT_SCHEMA
            and checkpoint.get("observation_state") not in {"not_observed", "observed"}
        )
    ):
        raise ConfigurationError("pending JARVIS validation resume checkpoint is invalid")
    typed_selector = cast(dict[str, object], selector)
    pipeline_id = cast(str, typed_selector["pipeline_id"])
    execution_id = cast(str, typed_selector["execution_id"])
    scheduler_cluster = typed_selector.get("scheduler_cluster")
    scheduler_provider = typed_selector.get("scheduler_provider")
    scheduler_native_id = typed_selector.get("scheduler_native_id")
    last_query_job_id = typed_selector.get("last_query_job_id")
    builder_scheduler_cluster = cast(dict[str, object], builder_inputs).get("scheduler_cluster")
    expected_mode = "scheduler" if scheduler_provider is not None else "direct"
    if (
        "scheduler_cluster" not in typed_selector
        or builder_scheduler_cluster != scheduler_cluster
        or (
            scheduler_cluster is not None
            and (not isinstance(scheduler_cluster, str) or not scheduler_cluster)
        )
        or (
            scheduler_provider is not None
            and (not isinstance(scheduler_provider, str) or not scheduler_provider)
        )
        or (
            scheduler_native_id is not None
            and (not isinstance(scheduler_native_id, str) or not scheduler_native_id)
        )
        or (scheduler_provider is None and scheduler_native_id is not None)
        or (
            unobserved
            and (
                last_query_job_id is not None
                or resource.state != "observation_pending"
                or resource.metadata.get("outcome") != "observation_pending"
            )
        )
        or (not unobserved and (not isinstance(last_query_job_id, str) or not last_query_job_id))
        or resource.resource_id != execution_id
        or resource.cluster != cluster
        or resource.provider != scheduler_provider
        or resource.metadata.get("retry_selector") != selector
    ):
        raise ConfigurationError("pending JARVIS validation resume identity is invalid")
    typed_observations: list[dict[str, object]] = []
    validated_prefix: list[dict[str, Any]] = []
    scheduler_native_id_assigned = False
    scheduler_cluster_assigned = False
    for raw_observation in cast(list[object], observations):
        if not isinstance(raw_observation, dict):
            raise ConfigurationError("pending JARVIS validation observation is invalid")
        observation = {
            str(key): value for key, value in cast(dict[object, object], raw_observation).items()
        }
        handle = observation.get("execution_handle")
        if not isinstance(handle, dict):
            raise ConfigurationError("pending JARVIS validation observation is invalid")
        typed_handle = cast(dict[str, object], handle)
        observation_scheduler_cluster = typed_handle.get("cluster")
        observation_native_id = typed_handle.get("scheduler_native_id")
        if (
            observation.get("pipeline_id") != pipeline_id
            or observation.get("execution_id") != execution_id
            or not isinstance(observation.get("query_job_id"), str)
            or not observation.get("query_job_id")
            or typed_handle.get("pipeline_id") != pipeline_id
            or typed_handle.get("execution_id") != execution_id
            or typed_handle.get("mode") != expected_mode
            or typed_handle.get("scheduler_provider") != scheduler_provider
        ):
            raise ConfigurationError("pending JARVIS validation observation identity changed")
        if scheduler_native_id is None:
            if observation_native_id is not None:
                raise ConfigurationError("pending JARVIS validation observation identity changed")
        elif observation_native_id is None:
            if scheduler_native_id_assigned:
                raise ConfigurationError("pending JARVIS validation observation identity changed")
        elif observation_native_id != scheduler_native_id:
            raise ConfigurationError("pending JARVIS validation observation identity changed")
        else:
            scheduler_native_id_assigned = True
        if scheduler_cluster is None:
            if observation_scheduler_cluster is not None:
                raise ConfigurationError("pending JARVIS validation observation identity changed")
        elif observation_scheduler_cluster is None:
            if scheduler_cluster_assigned:
                raise ConfigurationError("pending JARVIS validation observation identity changed")
        elif observation_scheduler_cluster != scheduler_cluster:
            raise ConfigurationError("pending JARVIS validation observation identity changed")
        else:
            scheduler_cluster_assigned = True
        if observation.get(cli._JARVIS_QUERY_INTEGRITY_KEY) is not None:
            raise ConfigurationError("pending JARVIS validation observation integrity failed")
        gap_marker = observation.get(cli._JARVIS_VERIFIED_GAP_KEY)
        crossed_verified_gap = gap_marker is not None
        if crossed_verified_gap and (
            not validated_prefix
            or not cli_jarvis_query_observation._valid_jarvis_verified_gap_marker(
                gap_marker,
                previous=validated_prefix[-1],
                current=cast(dict[str, Any], observation),
            )
        ):
            raise ConfigurationError("pending JARVIS validation observation gap is invalid")
        integrity_violation = cli_jarvis_query_observation._jarvis_query_integrity_violation(
            validated_prefix,
            cast(dict[str, Any], observation),
            crossed_verified_gap=crossed_verified_gap,
        )
        if integrity_violation is not None:
            raise ConfigurationError(
                "pending JARVIS validation observation integrity failed: "
                f"{integrity_violation['reason']}"
            )
        validated_prefix.append(cast(dict[str, Any], observation))
        typed_observations.append(observation)
    if typed_observations:
        latest = typed_observations[-1]
        latest_handle = cast(dict[str, object], latest["execution_handle"])
        if (
            latest.get("query_job_id") != last_query_job_id
            or resource.state != latest.get("state")
            or latest_handle.get("cluster") != scheduler_cluster
            or latest_handle.get("scheduler_native_id") != scheduler_native_id
        ):
            raise ConfigurationError("pending JARVIS validation latest observation changed")
    typed_runtime = cast(
        dict[str, Any],
        cast(dict[str, object], builder_inputs)["runtime_metadata"],
    )
    runtime_scheduler_job_id = typed_runtime.get("scheduler_job_id")
    runtime_details = typed_runtime.get("details")
    runtime_native_execution = (
        cast(dict[str, object], runtime_details).get("native_execution")
        if isinstance(runtime_details, dict)
        else None
    )
    if (
        typed_runtime.get("schema_version") != RUNTIME_METADATA_SCHEMA
        or typed_runtime.get("source") != "jarvis_mcp"
        or typed_runtime.get("pipeline_id") != pipeline_id
        or typed_runtime.get("execution_id") != execution_id
        or typed_runtime.get("scheduler_provider") != scheduler_provider
        or (
            runtime_scheduler_job_id is not None and runtime_scheduler_job_id != scheduler_native_id
        )
    ):
        raise ConfigurationError("pending JARVIS validation runtime identity changed")
    if not isinstance(runtime_native_execution, dict):
        raise ConfigurationError("pending JARVIS validation runtime identity changed")
    try:
        native_documents = native_execution_documents(
            cast(dict[str, Any], runtime_native_execution)
        )
    except (ValidationError, ValueError) as exc:
        raise ConfigurationError("pending JARVIS validation runtime identity changed") from exc
    if native_documents is None:
        raise ConfigurationError("pending JARVIS validation runtime identity changed")
    runtime_handle = native_documents.execution_handle
    runtime_record = native_documents.execution_record
    runtime_progress = native_documents.progress
    runtime_terminal = typed_runtime.get("terminal")
    if not isinstance(runtime_terminal, dict):
        raise ConfigurationError("pending JARVIS validation runtime identity changed")
    typed_runtime_terminal = cast(dict[str, object], runtime_terminal)
    first_state = typed_observations[0].get("state") if typed_observations else runtime_record.state
    runtime_rank = cli._JARVIS_EXECUTION_STATE_RANK.get(runtime_record.state)
    first_rank = (
        cli._JARVIS_EXECUTION_STATE_RANK.get(first_state) if isinstance(first_state, str) else None
    )
    if (
        runtime_handle.pipeline_id != pipeline_id
        or runtime_handle.execution_id != execution_id
        or runtime_handle.mode != expected_mode
        or runtime_handle.scheduler_provider != scheduler_provider
        or runtime_handle.scheduler_native_id != runtime_scheduler_job_id
        or (
            runtime_handle.scheduler_native_id is not None
            and runtime_handle.scheduler_native_id != scheduler_native_id
        )
        or (runtime_handle.cluster is not None and runtime_handle.cluster != scheduler_cluster)
        or typed_runtime_terminal.get("state") != runtime_record.state
        or typed_runtime_terminal.get("terminal") is not runtime_record.terminal
        or typed_runtime_terminal.get("returncode") != runtime_record.return_code
        or typed_runtime_terminal.get("reason") != runtime_record.error
        or runtime_progress.pipeline_id != pipeline_id
        or runtime_progress.execution_id != execution_id
        or (runtime_rank is not None and first_rank is not None and first_rank < runtime_rank)
        or (
            typed_observations
            and runtime_record.terminal
            and (
                typed_observations[0].get("terminal") is not True
                or first_state != runtime_record.state
            )
        )
    ):
        raise ConfigurationError("pending JARVIS validation runtime identity changed")
    return checkpoint


def _validate_jarvis_dispatch_resume_checkpoint(
    checkpoint: dict[str, Any],
    *,
    resource: ValidationResource,
    cluster: str,
) -> dict[str, Any]:
    """Fail closed on any change to a pre-query JARVIS dispatch identity."""
    import clio_relay.cli as cli

    phase = checkpoint.get("phase")
    profile = checkpoint.get("profile")
    selector = checkpoint.get("retry_selector")
    intent = checkpoint.get("execution_intent")
    pre_dispatch_inputs = checkpoint.get("pre_dispatch_inputs")
    if (
        checkpoint.get("schema_version") != cli._JARVIS_VALIDATION_RESUME_CHECKPOINT_SCHEMA
        or phase not in {cli._JARVIS_VALIDATION_PHASE_INTENT, cli._JARVIS_VALIDATION_PHASE_DISPATCH}
        or profile not in {"user", "admin", "operator", "all"}
        or not isinstance(selector, dict)
        or not isinstance(intent, dict)
        or not isinstance(pre_dispatch_inputs, dict)
    ):
        raise ConfigurationError("pending JARVIS dispatch checkpoint is invalid")
    typed_selector = cast(dict[str, object], selector)
    typed_intent = cast(dict[str, object], intent)
    typed_pre_dispatch_inputs = cast(dict[str, Any], pre_dispatch_inputs)
    raw_arguments = typed_intent.get("arguments")
    if not isinstance(raw_arguments, dict):
        raise ConfigurationError("pending JARVIS dispatch intent is invalid")
    arguments = cast(dict[str, object], raw_arguments)
    idempotency_key = arguments.get("idempotency_key")
    pipeline_id = arguments.get("pipeline_id")
    expected_selector_fields = {
        "cluster",
        "pipeline_id",
        "relay_job_id",
        "idempotency_key",
        "idempotency_key_sha256",
        "execution_intent_sha256",
        "pre_dispatch_inputs_sha256",
        "call_response_sha256",
        "dispatch_evidence_sha256",
    }
    if (
        set(typed_selector) != expected_selector_fields
        or typed_intent.get("cluster") != cluster
        or typed_intent.get("profile") != profile
        or typed_intent.get("tool") != "jarvis_run"
        or arguments.get("cluster") != cluster
        or not isinstance(pipeline_id, str)
        or not pipeline_id
        or not isinstance(idempotency_key, str)
        or not idempotency_key
        or len(idempotency_key) > 512
        or typed_selector.get("cluster") != cluster
        or typed_selector.get("pipeline_id") != pipeline_id
        or typed_selector.get("idempotency_key") != idempotency_key
        or typed_selector.get("execution_intent_sha256")
        != cli_jarvis_intent_checkpoint._canonical_jarvis_validation_digest(typed_intent)
        or typed_selector.get("pre_dispatch_inputs_sha256")
        != cli_jarvis_intent_checkpoint._canonical_jarvis_validation_digest(
            typed_pre_dispatch_inputs
        )
        or typed_selector.get("idempotency_key_sha256")
        != hashlib.sha256(idempotency_key.encode("utf-8")).hexdigest()
        or typed_pre_dispatch_inputs.get("cluster") != cluster
        or typed_pre_dispatch_inputs.get("tool") != "jarvis_run"
        or not isinstance(typed_pre_dispatch_inputs.get("package_search_query"), str)
        or not typed_pre_dispatch_inputs.get("package_search_query")
    ):
        raise ConfigurationError("pending JARVIS dispatch identity changed")
    if (
        resource.cluster != cluster
        or resource.provider is not None
        or resource.metadata.get("retry_selector") != selector
        or resource.metadata.get("resume_checkpoint") != checkpoint
        or resource.metadata.get("outcome") != "observation_pending"
        or resource.metadata.get("scheduler_action") != "none"
    ):
        raise ConfigurationError("pending JARVIS dispatch resource identity changed")
    relay_job_id = typed_selector.get("relay_job_id")
    if phase == cli._JARVIS_VALIDATION_PHASE_INTENT:
        if (
            relay_job_id is not None
            or typed_selector.get("call_response_sha256") is not None
            or typed_selector.get("dispatch_evidence_sha256") is not None
            or "builder_inputs" in checkpoint
            or resource.kind != "jarvis_dispatch_intent"
            or resource.role != "resumable_jarvis_run_intent"
            or resource.resource_id != typed_selector.get("execution_intent_sha256")
            or resource.state != "response_unobserved"
            or resource.metadata.get("relay_action") != "replay_same_idempotency_key"
        ):
            raise ConfigurationError("pending JARVIS dispatch intent changed")
        return checkpoint
    builder_inputs = checkpoint.get("builder_inputs")
    if (
        not isinstance(builder_inputs, dict)
        or not isinstance(relay_job_id, str)
        or not relay_job_id
    ):
        raise ConfigurationError("pending JARVIS relay dispatch checkpoint is invalid")
    typed_builder = cast(dict[str, Any], builder_inputs)
    call_response = typed_builder.get("call_response")
    typed_call_response = (
        cast(dict[str, Any], call_response) if isinstance(call_response, dict) else None
    )
    try:
        response_job_id = (
            cli_jarvis_artifact_io._mcp_response_job_id(typed_call_response)
            if typed_call_response is not None
            else None
        )
    except RelayError as exc:
        raise ConfigurationError("pending JARVIS relay dispatch response is invalid") from exc
    if (
        response_job_id != relay_job_id
        or typed_builder.get("cluster") != cluster
        or typed_builder.get("tool") != "jarvis_run"
        or typed_builder.get("call_job_id") != relay_job_id
        or typed_builder.get("scheduler_cluster") is not None
        or typed_builder.get("call_status") != {}
        or typed_builder.get("artifacts") != []
        or typed_builder.get("mcp_result") is not None
        or typed_builder.get("provenance") is not None
        or typed_builder.get("runtime_metadata") is not None
        or typed_builder.get("progress") != []
        or typed_builder.get("live_progress_observation") is not None
        or any(typed_builder.get(key) != value for key, value in typed_pre_dispatch_inputs.items())
        or typed_selector.get("call_response_sha256")
        != cli_jarvis_intent_checkpoint._canonical_jarvis_validation_digest(typed_call_response)
        or typed_selector.get("dispatch_evidence_sha256")
        != cli_jarvis_intent_checkpoint._canonical_jarvis_validation_digest(typed_builder)
        or resource.kind != "relay_job"
        or resource.role != "resumable_jarvis_run_dispatch"
        or resource.resource_id != relay_job_id
        or resource.state != "observation_pending"
        or resource.metadata.get("relay_action") != "retain"
    ):
        raise ConfigurationError("pending JARVIS relay dispatch identity changed")
    return checkpoint


def _require_same_jarvis_resume_identity(
    *,
    expected: dict[str, Any],
    observed: dict[str, object],
) -> None:
    """Reject a resume snapshot whose durable workload identity changed."""
    for field in ("cluster", "pipeline_id", "execution_id", "scheduler_provider"):
        if observed.get(field) != expected.get(field):
            raise RelayError(f"JARVIS validation resume changed {field}")
    expected_native_id = expected.get("scheduler_native_id")
    observed_native_id = observed.get("scheduler_native_id")
    if expected_native_id is not None and observed_native_id != expected_native_id:
        raise RelayError("JARVIS validation resume changed scheduler_native_id")
    if observed_native_id is not None and (
        not isinstance(observed_native_id, str) or not observed_native_id
    ):
        raise RelayError("JARVIS validation resume returned an invalid scheduler_native_id")
    expected_scheduler_cluster = expected.get("scheduler_cluster")
    observed_scheduler_cluster = observed.get("scheduler_cluster")
    if (
        expected_scheduler_cluster is not None
        and observed_scheduler_cluster != expected_scheduler_cluster
    ):
        raise RelayError("JARVIS validation resume changed scheduler_cluster")
    if observed_scheduler_cluster is not None and (
        not isinstance(observed_scheduler_cluster, str) or not observed_scheduler_cluster
    ):
        raise RelayError("JARVIS validation resume returned an invalid scheduler_cluster")

"""JARVIS execution-recovery dispatch trust and orchestration
(iowarp/clio-relay#231).

Owner module for validating and orchestrating one durable JARVIS
execution-recovery attempt end to end:

- ``_trusted_jarvis_mcp_route`` verifies a durable job targets one supported
  artifact-bound JARVIS MCP call (registered contract or the configured
  built-in route); every other function here calls it first.
- ``_jarvis_execution_recovery_intent``/``_durable_jarvis_execution_
  recovery`` build (before dispatch) and validate (on every read) the
  nested recovery intent persisted in task metadata -- the durable state
  machine's own shape guard.
- ``_jarvis_execution_recovery_is_pending`` is the one-line pending check
  callers use instead of re-deriving intent state.
- ``_jarvis_mcp_result_identity_matches``/``trusted_jarvis_mcp_result``
  verify one MCP result document actually came from this job's pinned
  route (identity is independent of outcome -- a tool-error answer still
  proves which route produced it) and, for ``trusted_jarvis_mcp_result``,
  that the call completed successfully with a persisted result. Public
  (clio-relay#271 direction): every owner module across the endpoint
  decomposition imports it, so the leading underscore was pure
  reportPrivateUsage noise, not a real privacy boundary.
- ``_attributed_jarvis_dispatch_refusal`` returns the typed refusal only
  when the result's identity is proven to be this job's own.
- ``_durable_jarvis_dispatch_refusal_detail`` renders the typed refusal
  reason recorded in task metadata for display.
- ``_durable_runtime_recovery_state`` restores the prior trusted runtime
  snapshot (and its content digest) used for transition checks.
- ``_trusted_jarvis_execution_query_validation`` requires the bundled
  runner's exact native execution-query attestation shape.
- ``minimal_mcp_runner_environment``/``endpoint_mcp_runner_command`` build
  the subprocess environment and command used to invoke the packaged relay
  MCP runner for a recovery query. Also public, same #271 reasoning.

Depends on ``endpoint_sidecar_types.py`` (schema/byte-budget constants) and
``endpoint_recovery_directory.py`` (``_recovery_timestamp``,
``_recovery_directory_anchor_metadata_is_valid``,
``_recovery_query_process_is_valid``) -- both leaves relative to this
module, so it stays acyclic. ``EndpointWorker`` (still resident in
``endpoint.py``) is this module's main caller.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import secrets
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, cast

from clio_relay.command_evidence import bounded_error_detail
from clio_relay.endpoint_recovery_directory import (
    _recovery_directory_anchor_metadata_is_valid,
    _recovery_query_process_is_valid,
    _recovery_timestamp,
)
from clio_relay.endpoint_sidecar_types import (
    MCP_JARVIS_EXECUTION_RECOVERY_SCHEMA,
    MCP_RUNNER_BASE_ENV_NAMES,
)
from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.jarvis_dispatch_failure import (
    JARVIS_DISPATCH_REFUSAL_RESOLUTION,
    JarvisDispatchRefusal,
    jarvis_dispatch_refusal,
)
from clio_relay.jarvis_mcp import (
    jarvis_cd_lock_binding_expectation,
    jarvis_mcp_command,
    jarvis_mcp_server_artifact_binding_verified,
)
from clio_relay.models import (
    REGISTERED_JARVIS_EXECUTION_CONTRACTS,
    JobKind,
    McpCallSpec,
    RelayJob,
    RelayTask,
)
from clio_relay.remote_mcp import remote_mcp_server_artifact_binding_verified
from clio_relay.runtime_metadata import JarvisRuntimeMetadata


def _trusted_jarvis_mcp_route(
    job: RelayJob,
    *,
    expected_tool: str = "jarvis_run",
) -> tuple[bool, str]:
    """Verify a durable job targets one supported artifact-bound JARVIS call."""
    if job.kind is not JobKind.MCP_CALL or not isinstance(job.spec, McpCallSpec):
        return False, "relay job is not an MCP call"
    if job.spec.tool != expected_tool:
        return False, f"MCP tool is not the owned {expected_tool} operation"
    if job.spec.expected_registered_contract is not None:
        if job.spec.expected_registered_contract not in REGISTERED_JARVIS_EXECUTION_CONTRACTS:
            return False, "registered MCP call does not use the supported JARVIS contract"
        if job.spec.expected_jarvis_cd_lock_binding is not None:
            return False, "registered JARVIS route also supplied a built-in lock pin"
        if job.spec.expected_server_artifact_digest is None:
            return False, "MCP call is not bound to its discovered server artifact"
        return True, "registered JARVIS MCP contract and artifact binding matched"
    try:
        configured_command = jarvis_mcp_command()
    except (ConfigurationError, ValueError) as exc:
        return False, f"configured JARVIS MCP command is invalid: {exc}"
    if [job.spec.server, *job.spec.server_args] != configured_command:
        return False, "MCP command does not match the configured JARVIS server"
    if job.spec.expected_jarvis_cd_lock_binding != jarvis_cd_lock_binding_expectation():
        return False, "MCP call did not enforce the relay JARVIS-CD lock pin"
    if job.spec.expected_server_artifact_digest is None:
        return False, "MCP call is not bound to its discovered server artifact"
    return True, "configured JARVIS MCP route and artifact binding matched"


def _jarvis_execution_recovery_intent(
    job: RelayJob,
    *,
    created_at: datetime,
) -> dict[str, object] | None:
    """Build the nested JARVIS intent persisted before a trusted run dispatch."""
    trusted, _reason = _trusted_jarvis_mcp_route(job)
    if not trusted:
        return None
    assert isinstance(job.spec, McpCallSpec)
    pipeline_id = job.spec.arguments.get("pipeline_id")
    execution_id = job.spec.arguments.get("execution_id")
    if not isinstance(pipeline_id, str) or not pipeline_id:
        raise ConfigurationError("trusted jarvis_run requires a pipeline_id")
    if not isinstance(execution_id, str) or not execution_id:
        raise ConfigurationError("trusted jarvis_run requires a durable execution_id")
    recovery_directory_name = f".jarvis-execution-recovery-{secrets.token_hex(16)}"
    return {
        "schema_version": MCP_JARVIS_EXECUTION_RECOVERY_SCHEMA,
        "state": "pending",
        "pipeline_id": pipeline_id,
        "execution_id": execution_id,
        "scheduler_expected": "unknown",
        "expected_server_artifact_digest": job.spec.expected_server_artifact_digest,
        "idempotency_key_sha256": hashlib.sha256(job.idempotency_key.encode("utf-8")).hexdigest(),
        "created_at": created_at.isoformat(),
        "dispatch_state": "prepared",
        "dispatch_started_at": None,
        "attempts": 0,
        "last_attempt_at": None,
        "next_retry_at": None,
        "last_error": None,
        "result_sha256": None,
        "result_relative_path": f"{recovery_directory_name}/mcp-result.json",
        "recovery_directory_name": recovery_directory_name,
        "recovery_directory_anchor": None,
        "resolved_at": None,
        "resolution": None,
        "scheduler_provider": None,
        "scheduler_job_id": None,
        "query_process": None,
    }


def _durable_jarvis_execution_recovery(
    job: RelayJob,
    task: RelayTask,
) -> dict[str, Any] | None:
    """Validate and return one pending or resolved nested execution intent."""
    raw = task.metadata.get("jarvis_execution_recovery")
    if raw is None:
        return None
    if not isinstance(raw, dict):
        raise RelayError(f"JARVIS execution recovery intent is invalid for {task.task_id}")
    intent = cast(dict[str, Any], raw)
    expected_fields = {
        "schema_version",
        "state",
        "pipeline_id",
        "execution_id",
        "scheduler_expected",
        "expected_server_artifact_digest",
        "idempotency_key_sha256",
        "created_at",
        "dispatch_state",
        "dispatch_started_at",
        "attempts",
        "last_attempt_at",
        "next_retry_at",
        "last_error",
        "result_sha256",
        "result_relative_path",
        "recovery_directory_name",
        "recovery_directory_anchor",
        "resolved_at",
        "resolution",
        "scheduler_provider",
        "scheduler_job_id",
        "query_process",
    }
    route_valid, _route_reason = _trusted_jarvis_mcp_route(job)
    spec = job.spec
    expected_pipeline_id = (
        spec.arguments.get("pipeline_id") if isinstance(spec, McpCallSpec) else None
    )
    expected_execution_id = (
        spec.arguments.get("execution_id") if isinstance(spec, McpCallSpec) else None
    )
    attempts = intent.get("attempts")
    raw_created_at = intent.get("created_at")
    created_at = _recovery_timestamp(raw_created_at) if isinstance(raw_created_at, str) else None
    raw_dispatch_started_at = intent.get("dispatch_started_at")
    dispatch_started_at = (
        _recovery_timestamp(raw_dispatch_started_at)
        if isinstance(raw_dispatch_started_at, str)
        else None
    )
    raw_last_attempt_at = intent.get("last_attempt_at")
    last_attempt_at = (
        _recovery_timestamp(raw_last_attempt_at) if isinstance(raw_last_attempt_at, str) else None
    )
    next_retry_at = intent.get("next_retry_at")
    parsed_next_retry_at = (
        _recovery_timestamp(next_retry_at) if isinstance(next_retry_at, str) else None
    )
    raw_resolved_at = intent.get("resolved_at")
    resolved_at = _recovery_timestamp(raw_resolved_at) if isinstance(raw_resolved_at, str) else None
    last_error = intent.get("last_error")
    result_sha256 = intent.get("result_sha256")
    resolution = intent.get("resolution")
    scheduler_provider = intent.get("scheduler_provider")
    scheduler_job_id = intent.get("scheduler_job_id")
    query_process = intent.get("query_process")
    directory_anchor = intent.get("recovery_directory_anchor")
    recovery_directory_name = intent.get("recovery_directory_name")
    if (
        not route_valid
        or not isinstance(spec, McpCallSpec)
        or set(intent) != expected_fields
        or intent.get("schema_version") != MCP_JARVIS_EXECUTION_RECOVERY_SCHEMA
        or intent.get("state") not in {"pending", "resolved"}
        or intent.get("pipeline_id") != expected_pipeline_id
        or intent.get("execution_id") != expected_execution_id
        or intent.get("scheduler_expected") != "unknown"
        or created_at is None
        or intent.get("dispatch_state") not in {"prepared", "started"}
        or (raw_dispatch_started_at is not None and dispatch_started_at is None)
        or (intent.get("dispatch_state") == "prepared" and raw_dispatch_started_at is not None)
        or (intent.get("dispatch_state") == "started" and dispatch_started_at is None)
        or intent.get("expected_server_artifact_digest") != spec.expected_server_artifact_digest
        or intent.get("idempotency_key_sha256")
        != hashlib.sha256(job.idempotency_key.encode("utf-8")).hexdigest()
        or not isinstance(recovery_directory_name, str)
        or re.fullmatch(r"\.jarvis-execution-recovery-[0-9a-f]{32}", recovery_directory_name)
        is None
        or intent.get("result_relative_path") != f"{recovery_directory_name}/mcp-result.json"
        or isinstance(attempts, bool)
        or not isinstance(attempts, int)
        or attempts < 0
        or (raw_last_attempt_at is not None and last_attempt_at is None)
        or (next_retry_at is not None and parsed_next_retry_at is None)
        or (raw_resolved_at is not None and resolved_at is None)
        or (last_error is not None and (not isinstance(last_error, str) or not last_error))
        or (
            result_sha256 is not None
            and (
                not isinstance(result_sha256, str)
                or re.fullmatch(r"[0-9a-f]{64}", result_sha256) is None
            )
        )
        or resolution
        not in {
            None,
            "dispatch_result",
            "execution_query",
            JARVIS_DISPATCH_REFUSAL_RESOLUTION,
        }
        or (
            scheduler_provider is not None
            and (not isinstance(scheduler_provider, str) or not scheduler_provider)
        )
        or (
            scheduler_job_id is not None
            and (not isinstance(scheduler_job_id, str) or not scheduler_job_id)
        )
        or (query_process is not None and not isinstance(query_process, dict))
        or not _recovery_directory_anchor_metadata_is_valid(directory_anchor)
    ):
        raise RelayError(f"JARVIS execution recovery intent is invalid for {task.task_id}")
    if dispatch_started_at is not None and dispatch_started_at < created_at:
        raise RelayError(f"JARVIS execution recovery intent is invalid for {task.task_id}")
    if attempts == 0 and last_attempt_at is not None:
        raise RelayError(f"JARVIS execution recovery intent is invalid for {task.task_id}")
    if attempts > 0 and (
        intent["dispatch_state"] != "started"
        or dispatch_started_at is None
        or last_attempt_at is None
        or last_attempt_at < dispatch_started_at
    ):
        raise RelayError(f"JARVIS execution recovery intent is invalid for {task.task_id}")
    if parsed_next_retry_at is not None and (
        intent["state"] != "pending"
        or attempts == 0
        or last_attempt_at is None
        or parsed_next_retry_at < last_attempt_at
    ):
        raise RelayError(f"JARVIS execution recovery intent is invalid for {task.task_id}")
    if last_error is not None and (
        intent["state"] != "pending" or attempts == 0 or parsed_next_retry_at is None
    ):
        raise RelayError(f"JARVIS execution recovery intent is invalid for {task.task_id}")
    if query_process is not None and (
        intent["state"] != "pending"
        or attempts == 0
        or last_attempt_at is None
        or not _recovery_query_process_is_valid(
            cast(dict[str, object], query_process),
            attempted_at=last_attempt_at,
        )
    ):
        raise RelayError(f"JARVIS execution recovery intent is invalid for {task.task_id}")
    if intent["state"] == "pending":
        if (
            resolved_at is not None
            or resolution is not None
            or scheduler_provider is not None
            or scheduler_job_id is not None
            or (attempts == 0 and result_sha256 is not None)
        ):
            raise RelayError(f"JARVIS execution recovery intent is invalid for {task.task_id}")
    elif (
        intent["dispatch_state"] != "started"
        or dispatch_started_at is None
        or resolved_at is None
        or resolved_at < dispatch_started_at
        or (last_attempt_at is not None and resolved_at < last_attempt_at)
        or result_sha256 is None
        or resolution is None
        or last_error is not None
        or parsed_next_retry_at is not None
        or query_process is not None
        or ((scheduler_provider is None) != (scheduler_job_id is None))
        or (resolution == "execution_query" and attempts == 0)
    ):
        raise RelayError(f"JARVIS execution recovery intent is invalid for {task.task_id}")
    return intent


def _jarvis_execution_recovery_is_pending(job: RelayJob, task: RelayTask) -> bool:
    """Return whether scheduler identity is awaiting an exact JARVIS query."""
    intent = _durable_jarvis_execution_recovery(job, task)
    return intent is not None and intent["state"] == "pending"


def _attributed_jarvis_dispatch_refusal(
    job: RelayJob,
    document: object,
) -> JarvisDispatchRefusal | None:
    """Return the typed refusal only when the result came from this job's route."""
    identity_matched, _identity_reason = _jarvis_mcp_result_identity_matches(job, document)
    if not identity_matched:
        return None
    return jarvis_dispatch_refusal(document)


def _durable_jarvis_dispatch_refusal_detail(task: RelayTask) -> str:
    """Return the typed reason recorded when JARVIS refused one durable run."""
    payload = task.metadata.get("jarvis_dispatch_refusal")
    if isinstance(payload, dict):
        typed = cast(dict[str, object], payload)
        code = typed.get("code")
        message = typed.get("message")
        if isinstance(code, str) and code and isinstance(message, str) and message:
            bounded = bounded_error_detail(f"{code}: {message}")
            if bounded is not None:
                return bounded
    return "JARVIS refused the run without a recorded typed reason"


def _durable_runtime_recovery_state(
    task: RelayTask,
) -> tuple[JarvisRuntimeMetadata | None, set[str]]:
    """Restore the prior trusted runtime snapshot used for transition checks."""
    raw_runtime = task.metadata.get("runtime_metadata")
    if raw_runtime is None:
        return None, set()
    if not isinstance(raw_runtime, dict):
        raise RelayError(f"durable runtime metadata is invalid for {task.task_id}")
    try:
        runtime = JarvisRuntimeMetadata.model_validate(raw_runtime)
    except ValueError as exc:
        raise RelayError(f"durable runtime metadata is invalid for {task.task_id}: {exc}") from exc
    digest_payload = runtime.model_dump(mode="json", exclude={"observed_at"})
    digest = hashlib.sha256(json.dumps(digest_payload, sort_keys=True).encode("utf-8")).hexdigest()
    return runtime, {digest}


def _jarvis_mcp_result_identity_matches(
    job: RelayJob,
    document: object,
    *,
    expected_tool: str = "jarvis_run",
) -> tuple[bool, str]:
    """Verify one MCP result document came from this durable job's pinned route.

    Identity is independent of the call's outcome: a dispatch that answered with
    a tool error still proves which route produced it, which is what lets the
    worker record that answer as this job's typed failure.
    """
    route_valid, route_reason = _trusted_jarvis_mcp_route(
        job,
        expected_tool=expected_tool,
    )
    if not route_valid:
        return False, route_reason
    assert isinstance(job.spec, McpCallSpec)
    if not isinstance(document, dict):
        return False, "MCP result artifact is not an object"
    typed = cast(dict[str, object], document)
    if typed.get("server") != job.spec.server or typed.get("server_args") != job.spec.server_args:
        return False, "MCP result command does not match the durable job spec"
    if typed.get("operation") != "tools/call" or typed.get("tool") != job.spec.tool:
        return False, "MCP result route does not match the durable job spec"
    if typed.get("arguments") != job.spec.arguments:
        return False, "MCP result arguments do not match the durable job spec"
    if typed.get("env_from") != job.spec.env_from:
        return False, "MCP result environment references do not match the durable job spec"
    if typed.get("expected_jarvis_cd_lock_binding") != job.spec.expected_jarvis_cd_lock_binding:
        return False, "MCP result JARVIS-CD lock pin does not match the durable job spec"
    if typed.get("expected_registered_contract") != job.spec.expected_registered_contract:
        return False, "MCP result registered contract does not match the durable job spec"
    if (
        typed.get("expected_server_artifact_digest") != job.spec.expected_server_artifact_digest
        or typed.get("observed_server_artifact_digest") != job.spec.expected_server_artifact_digest
    ):
        from clio_relay.dev_mode import dev_mode_enabled

        if not dev_mode_enabled():
            return False, "MCP result server artifact does not match the durable job spec"
    if job.spec.expected_registered_contract is not None:
        artifact_verified = remote_mcp_server_artifact_binding_verified(
            typed.get("server_artifact"),
            expected_digest=job.spec.expected_server_artifact_digest,
        )
        artifact_failure = (
            "MCP result server artifact identity is not the immutable registered route"
        )
    else:
        artifact_verified = jarvis_mcp_server_artifact_binding_verified(
            typed.get("server_artifact"),
            expected_digest=job.spec.expected_server_artifact_digest,
        )
        artifact_failure = "MCP result server artifact identity is not the exact relay release pin"
    if not artifact_verified:
        from clio_relay.dev_mode import dev_mode_enabled

        if not dev_mode_enabled():
            return False, artifact_failure
    return True, "configured JARVIS MCP command and durable route matched"


def trusted_jarvis_mcp_result(
    job: RelayJob,
    document: object,
    *,
    expected_tool: str = "jarvis_run",
) -> tuple[bool, str]:
    """Verify runtime identity came from the configured owned JARVIS MCP call."""
    identity_matched, identity_reason = _jarvis_mcp_result_identity_matches(
        job,
        document,
        expected_tool=expected_tool,
    )
    if not identity_matched:
        return False, identity_reason
    typed = cast(dict[str, object], document)
    if (
        typed.get("returncode") != 0
        or typed.get("timed_out") is True
        or typed.get("protocol_error") is not None
    ):
        return False, "MCP call did not complete successfully"
    if not isinstance(typed.get("structured_result"), dict) and not isinstance(
        typed.get("protocol_result"), dict
    ):
        return False, "MCP result has no persisted structured protocol result"
    protocol_result = typed.get("protocol_result")
    if (
        isinstance(protocol_result, dict)
        and cast(dict[str, object], protocol_result).get("isError") is True
    ):
        return False, "JARVIS MCP tool returned isError"
    return True, "configured JARVIS MCP command and durable result matched"


def _trusted_jarvis_execution_query_validation(
    document: object,
    *,
    pipeline_id: str,
    execution_id: str,
) -> bool:
    """Require the bundled runner's exact native execution-query attestation."""
    if not isinstance(document, dict):
        return False
    validation = cast(dict[str, object], document).get("result_validation")
    if not isinstance(validation, dict):
        return False
    typed = cast(dict[str, object], validation)
    return (
        typed.get("schema_version") == "clio-relay.jarvis-execution-query-validation.v1"
        and typed.get("pipeline_id") == pipeline_id
        and typed.get("execution_id") == execution_id
        and typed.get("include_progress") is True
        and typed.get("progress_included") is True
        and typed.get("include_service_runtimes") is False
        and typed.get("service_runtimes_included") is False
        and typed.get("service_runtime_count") == 0
        and typed.get("artifacts_requested") is False
        and typed.get("artifact_filters") == {}
        and typed.get("returned_artifact_count") == 0
        and typed.get("next_cursor_present") is False
    )


def minimal_mcp_runner_environment(env_from: dict[str, str]) -> dict[str, str]:
    """Expose only runtime basics and explicitly referenced MCP source variables."""
    environment = {
        name: os.environ[name] for name in MCP_RUNNER_BASE_ENV_NAMES if name in os.environ
    }
    if os.environ.get("CLIO_RELAY_DEV_MODE"):
        environment["CLIO_RELAY_DEV_MODE"] = os.environ["CLIO_RELAY_DEV_MODE"]
    for source_name in env_from.values():
        if source_name not in os.environ:
            raise ConfigurationError(f"MCP env_from source is not set: {source_name}")
        environment[source_name] = os.environ[source_name]
    return environment


def endpoint_mcp_runner_command(request_path: Path) -> list[str]:
    """Return the packaged relay runner command for one endpoint-owned request."""
    package_root = Path(__file__).resolve().parent
    candidates = (
        package_root / "mcp_call" / "runner.py",
        package_root.parents[1]
        / "jarvis-packages"
        / "clio_relay"
        / "clio_relay"
        / "mcp_call"
        / "runner.py",
    )
    for candidate in candidates:
        try:
            resolved = candidate.resolve(strict=True)
        except (OSError, RuntimeError):
            continue
        if resolved.is_file():
            return [sys.executable, str(resolved), request_path.name]
    raise ConfigurationError("packaged endpoint MCP runner is unavailable")

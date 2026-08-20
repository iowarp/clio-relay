"""Post-run unified JARVIS execution-query (``jarvis_get_execution``) evidence.

Owner module for the ``jarvis_mcp_validation.py`` split (clio-relay split/
jarvis-mcp-validation): validates the durable ``jarvis_get_execution`` call --
local tool surface/contract, request bounds (both the full artifact-page
request and the lighter resumable progress-only request), the verified server
artifact binding, the result envelope/identity/lifecycle coherence (in both
the terminal artifact-page and the resumable progress-only shapes), the
generated-artifact pagination/filter coherence, and the runner's own
result-validation attestation. Called by ``build_jarvis_mcp_validation_report``
in ``jarvis_mcp_validation_report.py``.
"""

from __future__ import annotations

from typing import cast

from clio_relay.installation import CLIO_KIT_JARVIS_EXECUTION_SCHEMA
from clio_relay.jarvis_mcp import (
    jarvis_cd_lock_binding_expectation,
    jarvis_mcp_server_artifact_verified,
)
from clio_relay.jarvis_mcp_validation_contract import _local_jarvis_contract
from clio_relay.jarvis_mcp_validation_core import (
    JSON,
    _is_sha256,
    _listed_tool,
    _mapping,
    _nonnegative_int,
    _positive_int,
    _response_job_id,
    _stdio_initialize_passed,
)


def _jarvis_execution_query_evidence(
    *,
    cluster: str,
    pipeline_id: object,
    execution_id: object,
    expected_server_artifact_digest: object,
    expected_server_artifact: JSON | None,
    tools_list_response: JSON | None,
    call_response: JSON | None,
    call_job_id: str,
    call_status: JSON,
    artifacts: list[JSON],
    mcp_result: JSON | None,
    provenance: JSON | None,
    initialize_response: JSON | None,
    stdio_evidence: JSON | None,
) -> tuple[JSON, bool, list[JSON]]:
    """Validate the durable post-run unified execution query and expose its evidence."""
    tool = _listed_tool(tools_list_response, "jarvis_get_execution")
    input_schema = _mapping(tool.get("inputSchema")) if tool else None
    properties = _mapping(input_schema.get("properties")) if input_schema else None
    required = input_schema.get("required") if input_schema else None
    local_contract, local_contract_passed = _local_jarvis_contract(
        tool,
        "jarvis_get_execution",
    )
    stdio_passed = _stdio_initialize_passed(
        initialize_response=initialize_response,
        evidence=stdio_evidence,
    )
    local_surface_passed = (
        properties is not None
        and isinstance(properties.get("cluster"), dict)
        and isinstance(required, list)
        and "cluster" in required
        and local_contract_passed
        and stdio_passed
    )

    job = _mapping(call_status.get("job")) or {}
    spec = _mapping(job.get("spec")) or {}
    arguments = _mapping(spec.get("arguments")) or {}
    artifact_request = _mapping(arguments.get("artifacts"))
    page_size = artifact_request.get("page_size") if artifact_request else None
    artifact_request_bounded = (
        isinstance(pipeline_id, str)
        and bool(pipeline_id)
        and isinstance(execution_id, str)
        and bool(execution_id)
        and set(arguments) == {"pipeline_id", "execution_id", "include_progress", "artifacts"}
        and arguments.get("pipeline_id") == pipeline_id
        and arguments.get("execution_id") == execution_id
        and arguments.get("include_progress") is True
        and artifact_request is not None
        and set(artifact_request) == {"page_size"}
        and _positive_int(page_size)
        and page_size <= 100
    )
    progress_request_bounded = (
        isinstance(pipeline_id, str)
        and bool(pipeline_id)
        and isinstance(execution_id, str)
        and bool(execution_id)
        and set(arguments) == {"pipeline_id", "execution_id", "include_progress"}
        and arguments.get("pipeline_id") == pipeline_id
        and arguments.get("execution_id") == execution_id
        and arguments.get("include_progress") is True
    )
    response_job_id = _response_job_id(call_response)
    durable_artifacts = {
        str(artifact.get("kind")): artifact
        for artifact in artifacts
        if isinstance(artifact.get("kind"), str)
    }
    required_artifacts = {"stdout", "stderr", "mcp_result", "provenance"}
    provenance_job = _mapping(provenance.get("job")) if provenance else None
    durable_job_base_passed = (
        response_job_id == call_job_id
        and job.get("job_id") == call_job_id
        and job.get("cluster") == cluster
        and job.get("kind") == "mcp_call"
        and job.get("state") == "succeeded"
        and call_status.get("terminal") is True
        and spec.get("operation") == "tools/call"
        and spec.get("tool") == "jarvis_get_execution"
        and required_artifacts.issubset(durable_artifacts)
        and provenance_job is not None
        and provenance_job.get("job_id") == call_job_id
    )
    job_passed = durable_job_base_passed and artifact_request_bounded
    resumable_job_passed = durable_job_base_passed and (
        artifact_request_bounded or progress_request_bounded
    )

    result_server_artifact = _mapping(mcp_result.get("server_artifact")) if mcp_result else None
    expected_jarvis_cd_lock_binding = jarvis_cd_lock_binding_expectation()
    server_binding_passed = (
        _is_sha256(expected_server_artifact_digest)
        and jarvis_mcp_server_artifact_verified(expected_server_artifact)
        and spec.get("expected_server_artifact_digest") == expected_server_artifact_digest
        and spec.get("expected_jarvis_cd_lock_binding") == expected_jarvis_cd_lock_binding
        and mcp_result is not None
        and mcp_result.get("expected_jarvis_cd_lock_binding") == expected_jarvis_cd_lock_binding
        and mcp_result.get("expected_server_artifact_digest") == expected_server_artifact_digest
        and mcp_result.get("observed_server_artifact_digest") == expected_server_artifact_digest
        and result_server_artifact == expected_server_artifact
    )

    structured = _mapping(mcp_result.get("structured_result")) if mcp_result else None
    handle = _mapping(structured.get("execution_handle")) if structured else None
    record = _mapping(structured.get("execution_record")) if structured else None
    progress = _mapping(structured.get("progress")) if structured else None
    artifact_page = _mapping(structured.get("artifact_page")) if structured else None
    service_runtimes = _mapping(structured.get("service_runtimes")) if structured else None
    runtime = _mapping(structured.get("runtime_metadata")) if structured else None
    raw_generated = artifact_page.get("artifacts") if artifact_page else None
    generated_artifacts = (
        [cast(JSON, item) for item in cast(list[object], raw_generated) if isinstance(item, dict)]
        if isinstance(raw_generated, list)
        else []
    )
    expected_envelope = {
        "schema_version",
        "pipeline_id",
        "execution_id",
        "execution_handle",
        "execution_record",
        "runtime_metadata",
        "progress",
        "artifact_page",
        "service_runtimes",
    }
    envelope_passed = (
        structured is not None
        and set(structured) == expected_envelope
        and structured.get("schema_version") == CLIO_KIT_JARVIS_EXECUTION_SCHEMA
        and runtime is not None
        and handle is not None
        and record is not None
        and progress is not None
        and artifact_page is not None
        and service_runtimes is None
    )
    progress_envelope_passed = (
        structured is not None
        and set(structured) == expected_envelope
        and structured.get("schema_version") == CLIO_KIT_JARVIS_EXECUTION_SCHEMA
        and handle is not None
        and record is not None
        and progress is not None
        and artifact_page is None
        and service_runtimes is None
    )
    identity_fields = (
        "execution_id",
        "pipeline_id",
        "mode",
        "scheduler_provider",
        "scheduler_native_id",
        "cluster",
    )
    identity_passed = bool(
        envelope_passed
        and isinstance(pipeline_id, str)
        and isinstance(execution_id, str)
        and structured is not None
        and structured.get("pipeline_id") == pipeline_id
        and structured.get("execution_id") == execution_id
        and handle is not None
        and record is not None
        and all(handle.get(key) == record.get(key) for key in identity_fields)
        and handle.get("pipeline_id") == pipeline_id
        and handle.get("execution_id") == execution_id
        and progress is not None
        and progress.get("pipeline_id") == pipeline_id
        and progress.get("execution_id") == execution_id
        and artifact_page is not None
        and artifact_page.get("pipeline_id") == pipeline_id
        and artifact_page.get("execution_id") == execution_id
        and all(artifact.get("execution_id") == execution_id for artifact in generated_artifacts)
    )
    progress_identity_passed = bool(
        progress_envelope_passed
        and isinstance(pipeline_id, str)
        and isinstance(execution_id, str)
        and structured is not None
        and structured.get("pipeline_id") == pipeline_id
        and structured.get("execution_id") == execution_id
        and handle is not None
        and record is not None
        and all(handle.get(key) == record.get(key) for key in identity_fields)
        and handle.get("pipeline_id") == pipeline_id
        and handle.get("execution_id") == execution_id
        and progress is not None
        and progress.get("pipeline_id") == pipeline_id
        and progress.get("execution_id") == execution_id
    )
    state = record.get("state") if record else None
    terminal = record.get("terminal") if record else None
    lifecycle_passed = (
        state == "completed"
        and terminal is True
        and record is not None
        and record.get("return_code") == 0
        and record.get("error") is None
        and progress is not None
        and progress.get("execution_state") == state
        and progress.get("terminal") is terminal
        and artifact_page is not None
        and artifact_page.get("execution_state") == state
        and artifact_page.get("terminal") is terminal
    )
    progress_lifecycle_passed = bool(
        progress_identity_passed
        and isinstance(state, str)
        and isinstance(terminal, bool)
        and record is not None
        and progress is not None
        and progress.get("execution_state") == state
        and progress.get("terminal") is terminal
        and (
            (
                terminal is False
                and record.get("return_code") is None
                and record.get("error") is None
            )
            or (
                terminal is True
                and state == "completed"
                and record.get("return_code") == 0
                and record.get("error") is None
            )
        )
    )

    returned = artifact_page.get("returned_artifact_count") if artifact_page else None
    matching = artifact_page.get("matching_artifact_count") if artifact_page else None
    next_cursor = artifact_page.get("next_cursor") if artifact_page else None
    pagination_passed = (
        artifact_page is not None
        and artifact_page.get("producer_schema_version") == "jarvis.execution.artifacts.v1"
        and _nonnegative_int(returned)
        and returned == len(generated_artifacts)
        and _nonnegative_int(matching)
        and matching >= returned
        and _positive_int(page_size)
        and returned <= page_size
        and (
            next_cursor is None
            or (isinstance(next_cursor, str) and bool(next_cursor) and bool(generated_artifacts))
        )
    )

    expected_filters: JSON = {
        "package_id": None,
        "role": None,
        "state": None,
        "artifact_id": None,
        "page_size": page_size,
        "cursor": None,
    }
    filters_passed = all(
        _artifact_matches_query(artifact, expected_filters) for artifact in generated_artifacts
    )
    runner_validation = _mapping(mcp_result.get("result_validation")) if mcp_result else None
    runner_attested = (
        runner_validation is not None
        and runner_validation.get("schema_version")
        == "clio-relay.jarvis-execution-query-validation.v1"
        and runner_validation.get("pipeline_id") == pipeline_id
        and runner_validation.get("execution_id") == execution_id
        and runner_validation.get("include_progress") is True
        and runner_validation.get("progress_included") is True
        and runner_validation.get("include_service_runtimes") is False
        and runner_validation.get("service_runtimes_included") is False
        and runner_validation.get("service_runtime_count") == 0
        and runner_validation.get("artifacts_requested") is True
        and runner_validation.get("artifact_filters") == expected_filters
        and runner_validation.get("returned_artifact_count") == returned
        and runner_validation.get("next_cursor_present") is (next_cursor is not None)
    )
    progress_runner_attested = (
        runner_validation is not None
        and runner_validation.get("schema_version")
        == "clio-relay.jarvis-execution-query-validation.v1"
        and runner_validation.get("pipeline_id") == pipeline_id
        and runner_validation.get("execution_id") == execution_id
        and runner_validation.get("include_progress") is True
        and runner_validation.get("progress_included") is True
        and runner_validation.get("include_service_runtimes") is False
        and runner_validation.get("service_runtimes_included") is False
        and runner_validation.get("service_runtime_count") == 0
        and runner_validation.get("artifacts_requested") is False
        and runner_validation.get("artifact_filters") == {}
        and runner_validation.get("returned_artifact_count") == 0
        and runner_validation.get("next_cursor_present") is False
    )
    result_passed = (
        mcp_result is not None
        and mcp_result.get("returncode") == 0
        and mcp_result.get("operation") == "tools/call"
        and mcp_result.get("tool") == "jarvis_get_execution"
        and mcp_result.get("protocol_error") is None
        and envelope_passed
        and identity_passed
        and lifecycle_passed
        and pagination_passed
        and filters_passed
        and runner_attested
    )
    resumable_result_passed = (
        mcp_result is not None
        and mcp_result.get("returncode") == 0
        and mcp_result.get("operation") == "tools/call"
        and mcp_result.get("tool") == "jarvis_get_execution"
        and mcp_result.get("protocol_error") is None
        and (envelope_passed or progress_envelope_passed)
        and (identity_passed or progress_identity_passed)
        and (lifecycle_passed or progress_lifecycle_passed)
        and (runner_attested or progress_runner_attested)
    )
    assertions: JSON = {
        "local_query_surface_verified": local_surface_passed,
        "durable_query_job_verified": job_passed,
        "server_artifact_binding_verified": server_binding_passed,
        "result_transport_verified": result_passed,
        "result_envelope_verified": envelope_passed,
        "identity_coherent": identity_passed,
        "lifecycle_coherent": lifecycle_passed,
        "terminal_success_verified": lifecycle_passed,
        "pagination_coherent": pagination_passed,
        "artifact_filters_coherent": filters_passed,
        "runner_semantic_validation_verified": runner_attested,
        "resumable_query_job_verified": resumable_job_passed,
        "resumable_result_transport_verified": resumable_result_passed,
        "resumable_result_envelope_verified": envelope_passed or progress_envelope_passed,
        "resumable_identity_coherent": identity_passed or progress_identity_passed,
        "resumable_lifecycle_coherent": lifecycle_passed or progress_lifecycle_passed,
        "resumable_runner_semantic_validation_verified": (
            runner_attested or progress_runner_attested
        ),
    }
    evidence: JSON = {
        "execution_id": structured.get("execution_id") if structured else None,
        "query_job_id": call_job_id,
        "response_job_id": response_job_id,
        "request": arguments,
        "job_state": job.get("state"),
        "terminal": call_status.get("terminal"),
        "required_artifact_kinds": sorted(required_artifacts),
        "artifact_kinds": sorted(durable_artifacts),
        "expected_server_artifact_digest": expected_server_artifact_digest,
        "expected_jarvis_cd_lock_binding": expected_jarvis_cd_lock_binding,
        "spec_jarvis_cd_lock_binding": spec.get("expected_jarvis_cd_lock_binding"),
        "result_jarvis_cd_lock_binding": (
            mcp_result.get("expected_jarvis_cd_lock_binding") if mcp_result else None
        ),
        "local_contract": local_contract,
        "packaged_stdio": stdio_evidence or {},
        "runner_validation": runner_validation or {},
        "result_identity": {
            "pipeline_id": structured.get("pipeline_id") if structured else None,
            "execution_id": structured.get("execution_id") if structured else None,
        },
        "result_lifecycle": {"state": state, "terminal": terminal},
        "artifact_page": {
            "producer_schema_version": (
                artifact_page.get("producer_schema_version") if artifact_page else None
            ),
            "matching_artifact_count": matching,
            "returned_artifact_count": returned,
            "next_cursor": next_cursor,
            "requested_page_size": page_size,
        },
        "assertions": assertions,
    }
    return (
        evidence,
        all(cast(bool, value) for value in assertions.values()),
        generated_artifacts,
    )


def _artifact_matches_query(artifact: JSON, query: JSON) -> bool:
    """Return whether one generated artifact satisfies the normalized request filters."""
    return all(
        query.get(filter_name) is None or artifact.get(artifact_name) == query.get(filter_name)
        for filter_name, artifact_name in (
            ("package_id", "package_id"),
            ("role", "role"),
            ("state", "state"),
            ("artifact_id", "artifact_id"),
        )
    )

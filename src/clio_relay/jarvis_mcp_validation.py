"""Acceptance evidence for the built-in virtual JARVIS MCP tools."""

from __future__ import annotations

import json
import math
from copy import deepcopy
from datetime import UTC, datetime
from typing import Any, TypeGuard, cast

from pydantic import ValidationError

from clio_relay.installation import (
    CLIO_KIT_JARVIS_EXECUTION_SCHEMA,
    JARVIS_EXECUTION_SERVICE_RUNTIMES_SCHEMA,
)
from clio_relay.jarvis_mcp import (
    CLIO_KIT_JARVIS_MCP_VERSION,
    CLIO_KIT_JARVIS_USER_CONTRACT_SHA256,
    jarvis_cd_lock_binding_expectation,
    jarvis_mcp_server_artifact_verified,
    jarvis_user_contract,
    virtual_jarvis_job_output_schema,
)
from clio_relay.remote_mcp import (
    RemoteMcpToolSchema,
    remote_mcp_schema_digest,
    remote_mcp_server_artifact_digest,
)
from clio_relay.runtime_metadata import RUNTIME_METADATA_SCHEMA, RuntimeMetadataSource
from clio_relay.validation_report import (
    EvidenceReference,
    LiveValidationReport,
    ValidationCheck,
    ValidationResource,
    ValidationStatus,
    new_live_validation_report,
)

JSON = dict[str, Any]
_NATIVE_PROGRESS_IDENTITY_KEYS = (
    "execution_id",
    "pipeline_id",
    "package_id",
    "package_name",
    "server_artifact_digest",
)
_JARVIS_PROGRESS_STATES = frozenset(
    {"pending", "starting", "running", "ready", "completed", "failed", "canceled"}
)
_MAX_JARVIS_PROGRESS_IDENTITY_TEXT = 256
_UNBOUND_JARVIS_IDENTITY = object()


def build_jarvis_mcp_validation_report(
    *,
    cluster: str,
    tool: str,
    tools_list_response: JSON | None,
    call_response: JSON | None,
    call_job_id: str,
    call_status: JSON,
    artifacts: list[JSON],
    mcp_result: JSON | None,
    provenance: JSON | None,
    runtime_metadata: JSON | None,
    progress: list[JSON],
    live_progress_observation: JSON | None,
    remote_tools_list_result: JSON | None = None,
    remote_discovery_job_id: str | None = None,
    remote_discovery_artifacts: list[JSON] | None = None,
    initialize_response: JSON | None = None,
    stdio_evidence: JSON | None = None,
    package_search_query: str,
    package_search_tools_list_response: JSON | None,
    package_search_call_response: JSON | None,
    package_search_call_job_id: str,
    package_search_call_status: JSON,
    package_search_artifacts: list[JSON],
    package_search_mcp_result: JSON | None,
    package_search_provenance: JSON | None,
    package_search_initialize_response: JSON | None,
    package_search_stdio_evidence: JSON | None,
    query_tools_list_response: JSON | None,
    query_call_response: JSON | None,
    query_call_job_id: str,
    query_call_status: JSON,
    query_artifacts: list[JSON],
    query_mcp_result: JSON | None,
    query_provenance: JSON | None,
    query_initialize_response: JSON | None,
    query_stdio_evidence: JSON | None,
    query_lifecycle_observations: list[JSON] | None = None,
    scheduler_cluster: object = _UNBOUND_JARVIS_IDENTITY,
    launcher: str | None = None,
    install_source: str | None = None,
    artifact_sha256: str | None = None,
) -> LiveValidationReport:
    """Build canonical evidence for one built-in virtual JARVIS MCP call."""
    report = new_live_validation_report(
        scenario="remote-mcp",
        cluster=cluster,
        launcher=launcher,
        install_source=install_source,
        artifact_sha256=artifact_sha256,
    )
    observed_at = datetime.now(UTC)
    report.completed_at = observed_at
    expected_jarvis_cd_lock_binding = jarvis_cd_lock_binding_expectation()

    tool_definition = _listed_tool(tools_list_response, tool)
    input_schema = _mapping(tool_definition.get("inputSchema")) if tool_definition else None
    properties = _mapping(input_schema.get("properties")) if input_schema else None
    required = cast(object, input_schema.get("required")) if input_schema else None
    required_fields: set[str] = (
        {item for item in cast(list[object], required) if isinstance(item, str)}
        if isinstance(required, list)
        else set[str]()
    )
    local_contract_evidence, local_contract_passed = _local_jarvis_contract(
        tool_definition,
        tool,
    )
    stdio_boundary_passed = _stdio_initialize_passed(
        initialize_response=initialize_response,
        evidence=stdio_evidence,
    )
    discovery_passed = (
        tool_definition is not None
        and properties is not None
        and isinstance(properties.get("cluster"), dict)
        and "cluster" in required_fields
        and local_contract_passed
        and stdio_boundary_passed
    )
    report.checks.append(
        _check(
            "remote-mcp.jarvis-discovery",
            "built-in JARVIS tool is exposed with an explicit cluster route",
            discovery_passed,
            report.started_at,
            observed_at,
            {
                "tool": tool,
                "listed": tool_definition is not None,
                "cluster_property": properties.get("cluster") if properties else None,
                "required": required,
                "local_contract": local_contract_evidence,
                "packaged_stdio": stdio_evidence or {},
            },
        )
    )

    remote_contract_evidence, remote_contract_passed = _remote_jarvis_contract(
        remote_tools_list_result
    )
    report.checks.append(
        _check(
            "remote-mcp.jarvis-remote-contract",
            "remote JARVIS MCP exposes the locked native-execution user contract",
            remote_contract_passed,
            report.started_at,
            observed_at,
            remote_contract_evidence,
        )
    )

    package_search_evidence, package_search_passed = _jarvis_package_search_evidence(
        cluster=cluster,
        query=package_search_query,
        expected_server_artifact=(
            _mapping(remote_tools_list_result.get("server_artifact"))
            if remote_tools_list_result
            else None
        ),
        tools_list_response=package_search_tools_list_response,
        call_response=package_search_call_response,
        call_job_id=package_search_call_job_id,
        call_status=package_search_call_status,
        artifacts=package_search_artifacts,
        mcp_result=package_search_mcp_result,
        provenance=package_search_provenance,
        initialize_response=package_search_initialize_response,
        stdio_evidence=package_search_stdio_evidence,
    )
    report.checks.append(
        _check(
            "remote-mcp.jarvis-package-search",
            "bounded JARVIS package discovery returned a durable summary page",
            package_search_passed,
            report.started_at,
            observed_at,
            package_search_evidence,
        )
    )

    job = _mapping(call_status.get("job")) or {}
    spec = _mapping(job.get("spec")) or {}
    response_job_id = _response_job_id(call_response)
    call_passed = (
        response_job_id == call_job_id
        and job.get("job_id") == call_job_id
        and job.get("cluster") == cluster
        and job.get("kind") == "mcp_call"
        and isinstance(spec.get("server"), str)
        and bool(spec.get("server"))
        and isinstance(spec.get("server_args"), list)
        and spec.get("operation") == "tools/call"
        and spec.get("tool") == tool
        and stdio_boundary_passed
    )
    report.checks.append(
        _check(
            "remote-mcp.jarvis-call",
            "virtual JARVIS tool created the expected durable cluster call",
            call_passed,
            report.started_at,
            observed_at,
            {
                "response_job_id": response_job_id,
                "job_id": job.get("job_id"),
                "cluster": job.get("cluster"),
                "kind": job.get("kind"),
                "spec": spec,
                "packaged_stdio": stdio_evidence or {},
            },
        )
    )

    server_artifact = _mapping(mcp_result.get("server_artifact")) if mcp_result else None
    discovery_server_artifact = (
        _mapping(remote_tools_list_result.get("server_artifact"))
        if remote_tools_list_result
        else None
    )
    expected_server_artifact_digest = spec.get("expected_server_artifact_digest")
    computed_server_artifact_digest = (
        remote_mcp_server_artifact_digest(server_artifact) if server_artifact is not None else None
    )
    python_runtime = (
        _mapping(server_artifact.get("python_distribution_runtime"))
        if server_artifact is not None
        else None
    )
    nested_runtime = (
        _mapping(server_artifact.get("nested_runtime")) if server_artifact is not None else None
    )
    server_artifact_passed = (
        server_artifact is not None
        and jarvis_mcp_server_artifact_verified(server_artifact)
        and server_artifact.get("verified") is True
        and server_artifact.get("server_process_artifact_verified") is True
        and bool(server_artifact.get("executable"))
        and server_artifact.get("install_source") == "uv-tool"
        and _is_sha256(server_artifact.get("install_artifact_sha256"))
        and server_artifact.get("requested_command") == spec.get("server")
        and spec.get("server_args") == ["mcp-server", "jarvis"]
        and isinstance(server_artifact.get("install_spec"), str)
        and str(server_artifact.get("install_spec")).endswith(".whl")
        and python_runtime is not None
        and str(python_runtime.get("distribution", "")).lower().replace("_", "-") == "clio-kit"
        and python_runtime.get("entry_point") == "clio-kit"
        and python_runtime.get("runtime_closure_verified") is True
        and nested_runtime is not None
        and nested_runtime.get("server_name") == "jarvis"
        and nested_runtime.get("persistent_tool") is True
        and nested_runtime.get("locked_runtime_verified") is True
        and server_artifact == discovery_server_artifact
        and _is_sha256(expected_server_artifact_digest)
        and expected_server_artifact_digest == computed_server_artifact_digest
        and spec.get("expected_jarvis_cd_lock_binding") == expected_jarvis_cd_lock_binding
        and mcp_result is not None
        and mcp_result.get("expected_jarvis_cd_lock_binding") == expected_jarvis_cd_lock_binding
        and mcp_result.get("expected_server_artifact_digest") == expected_server_artifact_digest
        and mcp_result.get("observed_server_artifact_digest") == expected_server_artifact_digest
    )
    report.checks.append(
        _check(
            "remote-mcp.server-artifact",
            "JARVIS MCP call used a verified executable and immutable install source",
            server_artifact_passed,
            report.started_at,
            observed_at,
            {
                "call_server_artifact": server_artifact or {},
                "discovery_server_artifact": discovery_server_artifact or {},
                "expected_server_artifact_digest": expected_server_artifact_digest,
                "computed_server_artifact_digest": computed_server_artifact_digest,
                "expected_jarvis_cd_lock_binding": expected_jarvis_cd_lock_binding,
                "spec_jarvis_cd_lock_binding": spec.get("expected_jarvis_cd_lock_binding"),
                "result_jarvis_cd_lock_binding": (
                    mcp_result.get("expected_jarvis_cd_lock_binding") if mcp_result else None
                ),
                "launcher": "uv tool",
                "python_distribution_runtime": python_runtime or {},
                "nested_runtime": nested_runtime or {},
                "result_expected_server_artifact_digest": (
                    mcp_result.get("expected_server_artifact_digest") if mcp_result else None
                ),
                "result_observed_server_artifact_digest": (
                    mcp_result.get("observed_server_artifact_digest") if mcp_result else None
                ),
            },
        )
    )

    artifacts_by_kind = {
        str(artifact.get("kind")): artifact
        for artifact in artifacts
        if isinstance(artifact.get("kind"), str)
    }
    required_artifacts = {"stdout", "stderr", "mcp_result", "provenance"}
    provenance_job = _mapping(provenance.get("job")) if provenance else None
    durable_passed = (
        job.get("state") == "succeeded"
        and call_status.get("terminal") is True
        and required_artifacts.issubset(artifacts_by_kind)
        and mcp_result is not None
        and mcp_result.get("returncode") == 0
        and mcp_result.get("operation") == "tools/call"
        and mcp_result.get("tool") == tool
        and provenance_job is not None
        and provenance_job.get("job_id") == call_job_id
    )
    report.checks.append(
        _check(
            "remote-mcp.durable-result",
            "terminal JARVIS MCP call has logs, result, and matching provenance",
            durable_passed,
            report.started_at,
            observed_at,
            {
                "state": job.get("state"),
                "terminal": call_status.get("terminal"),
                "artifact_kinds": sorted(artifacts_by_kind),
                "required_artifact_kinds": sorted(required_artifacts),
                "mcp_returncode": mcp_result.get("returncode") if mcp_result else None,
                "provenance_job_id": provenance_job.get("job_id") if provenance_job else None,
            },
        )
    )

    expected_pipeline_id = (
        spec.get("arguments", {}).get("pipeline_id")
        if isinstance(spec.get("arguments"), dict)
        else None
    )
    expected_execution_id = (
        spec.get("arguments", {}).get("execution_id")
        if isinstance(spec.get("arguments"), dict)
        else None
    )
    if query_lifecycle_observations is None:
        progress_evidence, progress_passed, progress_resource = _jarvis_live_progress_evidence(
            progress=progress,
            live_observation=live_progress_observation,
            call_job_id=call_job_id,
            pipeline_id=expected_pipeline_id,
            expected_server_artifact_digest=expected_server_artifact_digest,
            mcp_result=mcp_result,
            runtime_metadata=runtime_metadata,
        )
        progress_summary = (
            "jarvis_run exposed provider-valid progress before completion and replayed it only "
            "after execution binding"
        )
    else:
        (
            progress_evidence,
            progress_passed,
            progress_resource,
        ) = _jarvis_query_lifecycle_progress_evidence(
            observations=query_lifecycle_observations,
            pipeline_id=expected_pipeline_id,
            execution_id=expected_execution_id,
            scheduler_cluster=(
                _jarvis_runtime_scheduler_cluster(runtime_metadata)
                if scheduler_cluster is _UNBOUND_JARVIS_IDENTITY
                else scheduler_cluster
            ),
            scheduler_provider=(
                runtime_metadata.get("scheduler_provider") if runtime_metadata is not None else None
            ),
        )
        progress_summary = (
            "jarvis_get_execution observed provider-valid in-flight progress and a coherent "
            "terminal workload snapshot"
        )
    report.checks.append(
        _check(
            "remote-mcp.jarvis-live-progress",
            progress_summary,
            progress_passed,
            report.started_at,
            observed_at,
            progress_evidence,
        )
    )

    raw_spack_specs = cast(
        object,
        (_mapping(spec.get("arguments")) or {}).get("spack_specs"),
    )
    spack_specs = raw_spack_specs if _is_string_list(raw_spack_specs) else None
    if spack_specs is not None:
        environment = _spack_environment_metadata(runtime_metadata)
        spack_runtime_passed = (
            len(spack_specs) > 0
            and environment is not None
            and environment.get("specs") == spack_specs
            and environment.get("persisted") is True
            and environment.get("scheduler_reload") == "saved_pipeline_environment"
        )
        report.checks.append(
            _check(
                "jarvis.spack-runtime-environment",
                "jarvis_run persisted the requested Spack environment for scheduler reload",
                spack_runtime_passed,
                report.started_at,
                observed_at,
                {"spack_specs": spack_specs, "environment": environment or {}},
            )
        )

    source = runtime_metadata.get("source") if runtime_metadata else None
    field_sources = _mapping(runtime_metadata.get("field_sources")) if runtime_metadata else None
    terminal = _mapping(runtime_metadata.get("terminal")) if runtime_metadata else None
    scheduler_provider = runtime_metadata.get("scheduler_provider") if runtime_metadata else None
    scheduler_job_id = runtime_metadata.get("scheduler_job_id") if runtime_metadata else None
    authoritative_runtime_sources = {
        RuntimeMetadataSource.JARVIS_MCP.value,
        RuntimeMetadataSource.JARVIS_SIDECAR.value,
    }
    scheduler_provider_source = field_sources.get("scheduler_provider") if field_sources else None
    scheduler_job_id_source = field_sources.get("scheduler_job_id") if field_sources else None
    runtime_details = _mapping(runtime_metadata.get("details")) if runtime_metadata else None
    producer_contract = (
        _mapping(runtime_details.get("producer_contract")) if runtime_details else None
    )
    native_execution = (
        _mapping(runtime_details.get("native_execution")) if runtime_details else None
    )
    native_handle = _mapping(native_execution.get("execution_handle")) if native_execution else None
    native_record = _mapping(native_execution.get("execution_record")) if native_execution else None
    native_progress = _mapping(native_execution.get("progress")) if native_execution else None
    scheduler_pair_coherent = (scheduler_provider is None and scheduler_job_id is None) or (
        isinstance(scheduler_provider, str)
        and bool(scheduler_provider)
        and (
            scheduler_job_id is None
            or (isinstance(scheduler_job_id, str) and bool(scheduler_job_id))
        )
    )
    scheduler_sources_coherent = (
        scheduler_provider_source is None and scheduler_job_id_source is None
        if scheduler_provider is None
        else scheduler_provider_source in authoritative_runtime_sources
        and (scheduler_job_id is None or scheduler_job_id_source in authoritative_runtime_sources)
    )
    initial_terminal_coherent = bool(
        native_record is not None
        and terminal is not None
        and isinstance(native_record.get("terminal"), bool)
        and terminal.get("terminal") is native_record.get("terminal")
        and terminal.get("state") == native_record.get("state")
        and (
            terminal.get("returncode") == 0
            if terminal.get("terminal") is True
            else terminal.get("returncode") is None
        )
    )
    runtime_passed = (
        tool == "jarvis_run"
        and runtime_metadata is not None
        and runtime_metadata.get("schema_version") == RUNTIME_METADATA_SCHEMA
        and source == RuntimeMetadataSource.JARVIS_MCP.value
        and runtime_metadata.get("pipeline_id") == expected_pipeline_id
        and runtime_metadata.get("execution_id") == expected_execution_id
        and scheduler_pair_coherent
        and bool(field_sources)
        and RuntimeMetadataSource.LEGACY_STDOUT.value not in set(field_sources.values())
        and scheduler_sources_coherent
        and producer_contract is not None
        and producer_contract.get("trusted") is True
        and producer_contract.get("contract_kind") == "native_execution"
        and producer_contract.get("producer_schema_version") == "jarvis.execution.record.v1"
        and producer_contract.get("handle_schema_version") == "jarvis.execution.handle.v1"
        and producer_contract.get("progress_schema_version") == "jarvis.execution.progress.v1"
        and native_handle is not None
        and native_handle.get("schema_version") == "jarvis.execution.handle.v1"
        and native_record is not None
        and native_record.get("schema_version") == "jarvis.execution.record.v1"
        and native_progress is not None
        and native_progress.get("schema_version") == "jarvis.execution.progress.v1"
        and native_handle.get("execution_id") == runtime_metadata.get("execution_id")
        and native_record.get("execution_id") == runtime_metadata.get("execution_id")
        and native_progress.get("execution_id") == runtime_metadata.get("execution_id")
        and native_handle.get("scheduler_provider") == scheduler_provider
        and native_record.get("scheduler_provider") == scheduler_provider
        and native_handle.get("scheduler_native_id") == scheduler_job_id
        and native_record.get("scheduler_native_id") == scheduler_job_id
        and (
            native_handle.get("cluster") is None
            or (
                isinstance(native_handle.get("cluster"), str) and bool(native_handle.get("cluster"))
            )
        )
        and native_record.get("cluster") == native_handle.get("cluster")
        and (
            scheduler_cluster is _UNBOUND_JARVIS_IDENTITY
            or native_handle.get("cluster") is None
            or native_handle.get("cluster") == scheduler_cluster
        )
        and native_handle.get("mode")
        == ("scheduler" if scheduler_provider is not None else "direct")
        and native_record.get("mode") == native_handle.get("mode")
        and initial_terminal_coherent
        and "runtime_metadata" in artifacts_by_kind
    )
    report.checks.append(
        _check(
            "jarvis.structured-runtime-metadata",
            "JARVIS run metadata is structured, durable, and not stdout-derived",
            runtime_passed,
            report.started_at,
            observed_at,
            {
                "schema_version": (
                    runtime_metadata.get("schema_version") if runtime_metadata else None
                ),
                "execution_id": (
                    runtime_metadata.get("execution_id") if runtime_metadata else None
                ),
                "source": source,
                "pipeline_id": runtime_metadata.get("pipeline_id") if runtime_metadata else None,
                "scheduler_provider": scheduler_provider,
                "scheduler_job_id": scheduler_job_id,
                "scheduler_provider_source": scheduler_provider_source,
                "scheduler_job_id_source": scheduler_job_id_source,
                "scheduler_identity_optional_and_coherent": scheduler_pair_coherent,
                "field_sources": field_sources or {},
                "producer_contract": producer_contract or {},
                "native_execution": native_execution or {},
                "dispatch_snapshot_terminal": terminal or {},
                "runtime_artifact_id": (
                    artifacts_by_kind.get("runtime_metadata", {}).get("artifact_id")
                ),
            },
        )
    )

    query_evidence, query_passed, generated_artifacts = _jarvis_execution_query_evidence(
        cluster=cluster,
        pipeline_id=runtime_metadata.get("pipeline_id") if runtime_metadata else None,
        execution_id=runtime_metadata.get("execution_id") if runtime_metadata else None,
        expected_server_artifact_digest=expected_server_artifact_digest,
        expected_server_artifact=server_artifact,
        tools_list_response=query_tools_list_response,
        call_response=query_call_response,
        call_job_id=query_call_job_id,
        call_status=query_call_status,
        artifacts=query_artifacts,
        mcp_result=query_mcp_result,
        provenance=query_provenance,
        initialize_response=query_initialize_response,
        stdio_evidence=query_stdio_evidence,
    )
    report.checks.append(
        _check(
            "remote-mcp.jarvis-execution-query",
            "post-run JARVIS query returned coherent progress and a bounded artifact page",
            query_passed,
            report.started_at,
            observed_at,
            query_evidence,
        )
    )

    if isinstance(job.get("job_id"), str):
        execution_id = runtime_metadata.get("execution_id") if runtime_metadata else None
        report.resources.append(
            ValidationResource(
                kind="relay_job",
                resource_id=cast(str, job["job_id"]),
                role="virtual_jarvis_mcp_call",
                cluster=cluster,
                state=str(job.get("state")) if job.get("state") is not None else None,
                metadata={**job, "execution_id": execution_id},
            )
        )
    query_job = _mapping(query_call_status.get("job")) or {}
    if isinstance(query_job.get("job_id"), str):
        execution_id = runtime_metadata.get("execution_id") if runtime_metadata else None
        report.resources.append(
            ValidationResource(
                kind="relay_job",
                resource_id=cast(str, query_job["job_id"]),
                role="jarvis_mcp_execution_query",
                cluster=cluster,
                state=(str(query_job.get("state")) if query_job.get("state") is not None else None),
                metadata={**query_job, "execution_id": execution_id},
            )
        )
    package_search_job = _mapping(package_search_call_status.get("job")) or {}
    if isinstance(package_search_job.get("job_id"), str):
        report.resources.append(
            ValidationResource(
                kind="relay_job",
                resource_id=cast(str, package_search_job["job_id"]),
                role="jarvis_mcp_package_search",
                cluster=cluster,
                state=(
                    str(package_search_job.get("state"))
                    if package_search_job.get("state") is not None
                    else None
                ),
                metadata=package_search_job,
            )
        )
    if remote_discovery_job_id is not None:
        report.resources.append(
            ValidationResource(
                kind="relay_job",
                resource_id=remote_discovery_job_id,
                role="jarvis_mcp_remote_discovery",
                cluster=cluster,
                state="succeeded" if remote_contract_passed else "failed",
            )
        )
    if server_artifact is not None:
        identity = (
            str(server_artifact.get("install_spec"))
            if server_artifact.get("install_spec") is not None
            else str(server_artifact.get("resolved_executable", "jarvis"))
        )
        report.resources.append(
            ValidationResource(
                kind="mcp_server",
                resource_id=f"jarvis:{identity}",
                role="jarvis_mcp_server",
                cluster=cluster,
                state="verified" if server_artifact_passed else "unverified",
                metadata={
                    "server_name": "jarvis",
                    "server_info": (_mapping(mcp_result.get("server_info")) if mcp_result else {})
                    or {},
                    **server_artifact,
                },
            )
        )
    for artifact in artifacts:
        artifact_id = artifact.get("artifact_id")
        if not isinstance(artifact_id, str):
            continue
        uri = artifact.get("uri")
        report.resources.append(
            ValidationResource(
                kind="artifact",
                resource_id=artifact_id,
                role=str(artifact.get("kind", "artifact")),
                cluster=cluster,
                references=[str(uri)] if isinstance(uri, str) else [],
                metadata=artifact,
            )
        )
        report.artifacts.append(
            EvidenceReference(
                kind=str(artifact.get("kind", "artifact")),
                reference=(
                    str(uri)
                    if isinstance(uri, str)
                    else f"relay-artifact://{cluster}/{artifact_id}"
                ),
                sha256=(
                    str(artifact["sha256"]) if isinstance(artifact.get("sha256"), str) else None
                ),
            )
        )
    for artifact in remote_discovery_artifacts or []:
        artifact_id = artifact.get("artifact_id")
        if not isinstance(artifact_id, str):
            continue
        report.resources.append(
            ValidationResource(
                kind="artifact",
                resource_id=artifact_id,
                role="jarvis_mcp_remote_schema",
                cluster=cluster,
                metadata=artifact,
            )
        )
    for artifact in query_artifacts:
        artifact_id = artifact.get("artifact_id")
        if not isinstance(artifact_id, str):
            continue
        uri = artifact.get("uri")
        report.resources.append(
            ValidationResource(
                kind="artifact",
                resource_id=artifact_id,
                role=f"jarvis_execution_query_{artifact.get('kind', 'artifact')}",
                cluster=cluster,
                references=[str(uri)] if isinstance(uri, str) else [],
                metadata=artifact,
            )
        )
    for artifact in package_search_artifacts:
        artifact_id = artifact.get("artifact_id")
        if not isinstance(artifact_id, str):
            continue
        uri = artifact.get("uri")
        report.resources.append(
            ValidationResource(
                kind="artifact",
                resource_id=artifact_id,
                role=f"jarvis_package_search_{artifact.get('kind', 'artifact')}",
                cluster=cluster,
                references=[str(uri)] if isinstance(uri, str) else [],
                metadata=artifact,
            )
        )
    for artifact in generated_artifacts:
        artifact_id = artifact.get("artifact_id")
        if not isinstance(artifact_id, str):
            continue
        report.resources.append(
            ValidationResource(
                kind="jarvis_generated_artifact",
                resource_id=artifact_id,
                role=str(artifact.get("role", "artifact")),
                cluster=cluster,
                state=str(artifact.get("state")) if artifact.get("state") is not None else None,
                provider="jarvis-cd",
                references=_artifact_location_references(artifact),
                metadata=artifact,
            )
        )
    final_query_result = (
        _mapping(query_mcp_result.get("structured_result")) if query_mcp_result else None
    )
    final_query_record = (
        _mapping(final_query_result.get("execution_record")) if final_query_result else None
    )
    final_scheduler_provider = (
        final_query_record.get("scheduler_provider") if final_query_record else None
    )
    final_scheduler_job_id = (
        final_query_record.get("scheduler_native_id") if final_query_record else None
    )
    if isinstance(final_scheduler_provider, str) and isinstance(final_scheduler_job_id, str):
        report.resources.append(
            ValidationResource(
                kind="scheduler_job",
                resource_id=final_scheduler_job_id,
                role="jarvis_owned_execution",
                cluster=cluster,
                state=(
                    str(final_query_record.get("state"))
                    if final_query_record and final_query_record.get("state") is not None
                    else None
                ),
                provider=final_scheduler_provider,
                metadata=final_query_record or {},
            )
        )
    if progress_resource is not None:
        report.resources.append(
            ValidationResource(
                kind="jarvis_execution_progress",
                resource_id=str(progress_resource["resource_id"]),
                role="jarvis_mcp_native_progress",
                cluster=cluster,
                state="verified" if progress_passed else "unverified",
                provider=str(progress_resource["provider"]),
                metadata=cast(JSON, progress_resource["metadata"]),
            )
        )

    passed = all(check.status == ValidationStatus.PASSED for check in report.checks)
    report.status = ValidationStatus.PASSED if passed else ValidationStatus.FAILED
    report.error = None if passed else "one or more virtual JARVIS MCP checks failed"
    return report


def _artifact_location_references(artifact: dict[str, object]) -> list[str]:
    """Render transport-neutral JARVIS artifact locations as typed references."""
    location = _mapping(artifact.get("location")) or {}
    uri = location.get("uri")
    if isinstance(uri, str) and uri:
        return [uri]
    kind = location.get("kind")
    value = location.get("value")
    if isinstance(kind, str) and kind and isinstance(value, str) and value:
        return [f"{kind}:{value}"]
    return []


def _jarvis_package_search_evidence(
    *,
    cluster: str,
    query: str,
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
) -> tuple[JSON, bool]:
    """Validate one durable, bounded package-search result from JARVIS."""
    tool = _listed_tool(tools_list_response, "jarvis_describe")
    input_schema = _mapping(tool.get("inputSchema")) if tool else None
    properties = _mapping(input_schema.get("properties")) if input_schema else None
    required = cast(object, input_schema.get("required")) if input_schema else None
    required_fields: set[str] = (
        {item for item in cast(list[object], required) if isinstance(item, str)}
        if isinstance(required, list)
        else set[str]()
    )
    local_contract, local_contract_passed = _local_jarvis_contract(
        tool,
        "jarvis_describe",
    )
    stdio_passed = _stdio_initialize_passed(
        initialize_response=initialize_response,
        evidence=stdio_evidence,
    )
    local_surface_passed = bool(
        properties is not None
        and isinstance(properties.get("cluster"), dict)
        and {"cluster", "target"}.issubset(required_fields)
        and local_contract_passed
        and stdio_passed
    )

    job = _mapping(call_status.get("job")) or {}
    spec = _mapping(job.get("spec")) or {}
    arguments = _mapping(spec.get("arguments")) or {}
    page_size = cast(object, arguments.get("page_size"))
    page_size_value: int | None = page_size if _positive_int(page_size) else None
    request_bounded = bool(
        bool(query)
        and len(query) <= 256
        and set(arguments) == {"target", "query", "page_size"}
        and arguments.get("target") == "package_search"
        and arguments.get("query") == query
        and page_size_value is not None
        and page_size_value <= 25
    )
    response_job_id = _response_job_id(call_response)
    durable_artifacts = {
        str(artifact.get("kind")): artifact
        for artifact in artifacts
        if isinstance(artifact.get("kind"), str)
    }
    required_artifacts = {"stdout", "stderr", "mcp_result", "provenance"}
    provenance_job = _mapping(provenance.get("job")) if provenance else None
    job_passed = bool(
        response_job_id == call_job_id
        and job.get("job_id") == call_job_id
        and job.get("cluster") == cluster
        and job.get("kind") == "mcp_call"
        and job.get("state") == "succeeded"
        and call_status.get("terminal") is True
        and spec.get("operation") == "tools/call"
        and spec.get("tool") == "jarvis_describe"
        and request_bounded
        and required_artifacts.issubset(durable_artifacts)
        and provenance_job is not None
        and provenance_job.get("job_id") == call_job_id
    )

    expected_server_artifact_digest = (
        remote_mcp_server_artifact_digest(expected_server_artifact)
        if expected_server_artifact is not None
        else None
    )
    result_server_artifact = _mapping(mcp_result.get("server_artifact")) if mcp_result else None
    expected_jarvis_cd_lock_binding = jarvis_cd_lock_binding_expectation()
    server_binding_passed = bool(
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
    raw_packages = structured.get("packages") if structured else None
    packages = cast(list[object], raw_packages) if isinstance(raw_packages, list) else []
    summaries_valid = bool(packages) and all(
        _valid_package_search_summary(package) for package in packages
    )
    total_matches = cast(object, structured.get("total_matches")) if structured else None
    returned_count = cast(object, structured.get("returned_count")) if structured else None
    total_matches_value: int | None = total_matches if _positive_int(total_matches) else None
    returned_count_value: int | None = returned_count if _positive_int(returned_count) else None
    next_cursor = cast(object, structured.get("next_cursor")) if structured else None
    encoded_bytes = (
        len(
            json.dumps(
                structured,
                allow_nan=False,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8")
        )
        if structured is not None
        else None
    )
    result_bounded = bool(
        structured is not None
        and set(structured)
        == {
            "schema_version",
            "target",
            "query",
            "inventory_revision",
            "packages",
            "total_matches",
            "returned_count",
            "next_cursor",
        }
        and structured.get("schema_version") == "jarvis.package-search.v1"
        and structured.get("target") == "package_search"
        and structured.get("query") == query
        and _is_sha256(structured.get("inventory_revision"))
        and summaries_valid
        and returned_count_value is not None
        and total_matches_value is not None
        and page_size_value is not None
        and returned_count_value == len(packages)
        and returned_count_value <= page_size_value
        and total_matches_value >= returned_count_value
        and (
            next_cursor is None or (isinstance(next_cursor, str) and 1 <= len(next_cursor) <= 1024)
        )
        and encoded_bytes is not None
        and encoded_bytes <= 64 * 1024
    )
    protocol_passed = bool(
        mcp_result is not None
        and mcp_result.get("returncode") == 0
        and mcp_result.get("operation") == "tools/call"
        and mcp_result.get("tool") == "jarvis_describe"
        and mcp_result.get("protocol_error") is None
    )
    assertions = {
        "local_surface": local_surface_passed,
        "durable_call": job_passed,
        "server_artifact_binding": server_binding_passed,
        "protocol_result": protocol_passed,
        "bounded_summary_page": result_bounded,
    }
    return (
        {
            "query": query,
            "page_size": page_size_value,
            "response_job_id": response_job_id,
            "job_id": job.get("job_id"),
            "artifact_kinds": sorted(durable_artifacts),
            "required_artifact_kinds": sorted(required_artifacts),
            "expected_server_artifact_digest": expected_server_artifact_digest,
            "expected_jarvis_cd_lock_binding": expected_jarvis_cd_lock_binding,
            "spec_jarvis_cd_lock_binding": spec.get("expected_jarvis_cd_lock_binding"),
            "result_jarvis_cd_lock_binding": (
                mcp_result.get("expected_jarvis_cd_lock_binding") if mcp_result else None
            ),
            "returned_count": returned_count_value,
            "total_matches": total_matches_value,
            "next_cursor_present": isinstance(next_cursor, str),
            "serialized_result_bytes": encoded_bytes,
            "result": structured or {},
            "local_contract": local_contract,
            "packaged_stdio": stdio_evidence or {},
            "assertions": assertions,
        },
        all(assertions.values()),
    )


def _valid_package_search_summary(value: object) -> bool:
    """Return whether one package-search item is summary-only and bounded."""
    summary = _mapping(value)
    if summary is None:
        return False
    if not {"name", "short_name", "repository"}.issubset(summary):
        return False
    if not set(summary).issubset({"name", "short_name", "repository", "description"}):
        return False
    for key in ("name", "short_name", "repository"):
        item = summary.get(key)
        if not isinstance(item, str) or not item:
            return False
    description = summary.get("description")
    return description is None or (
        isinstance(description, str) and len(description.encode("utf-8")) <= 4096
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


def _valid_query_integrity_marker(value: object) -> bool:
    """Recognize only the relay's bounded fail-closed integrity accumulator."""
    marker = _mapping(value)
    if marker is None:
        return False
    return bool(
        set(marker) == {"schema_version", "valid", "reason", "previous_state", "current_state"}
        and marker.get("schema_version") == "clio-relay.jarvis-query-integrity.v1"
        and marker.get("valid") is False
        and isinstance(marker.get("reason"), str)
        and marker.get("reason")
        and (marker.get("previous_state") is None or isinstance(marker.get("previous_state"), str))
        and (marker.get("current_state") is None or isinstance(marker.get("current_state"), str))
    )


def _valid_query_verified_gap(
    value: object,
    *,
    previous: JSON,
    current: JSON,
) -> bool:
    """Validate one relay-trusted local summary for a sampled observation span."""
    marker = _mapping(value)
    if marker is None:
        return False
    return bool(
        set(marker)
        == {
            "schema_version",
            "verified",
            "discarded_observation_count",
            "discarded_observations_sha256",
            "previous_query_job_id",
            "current_query_job_id",
        }
        and marker.get("schema_version") == "clio-relay.jarvis-query-verified-gap.v1"
        and marker.get("verified") is True
        and _positive_int(marker.get("discarded_observation_count"))
        and _is_sha256(marker.get("discarded_observations_sha256"))
        and marker.get("previous_query_job_id") == previous.get("query_job_id")
        and marker.get("current_query_job_id") == current.get("query_job_id")
    )


def _jarvis_query_lifecycle_progress_evidence(
    *,
    observations: list[JSON],
    pipeline_id: object,
    execution_id: object,
    scheduler_cluster: object = _UNBOUND_JARVIS_IDENTITY,
    scheduler_provider: object = _UNBOUND_JARVIS_IDENTITY,
) -> tuple[JSON, bool, JSON | None]:
    """Validate in-flight and terminal progress obtained through execution queries."""
    state_rank = {
        "preparing": 0,
        "scripted": 1,
        "submitting": 2,
        "submitted": 3,
        "running": 4,
        "completed": 5,
        "failed": 5,
        "canceled": 5,
    }
    normalized: list[tuple[JSON, JSON, JSON, JSON]] = []
    identities_valid = isinstance(pipeline_id, str) and bool(pipeline_id)
    identities_valid = identities_valid and isinstance(execution_id, str) and bool(execution_id)
    expected_mode = "scheduler" if scheduler_provider is not None else "direct"
    bounded = 0 < len(observations) <= 512
    for observation in observations:
        handle = _mapping(observation.get("execution_handle"))
        record = _mapping(observation.get("execution_record"))
        progress = _mapping(observation.get("progress"))
        if handle is None or record is None or progress is None:
            identities_valid = False
            continue
        if not (
            observation.get("pipeline_id") == pipeline_id
            and observation.get("execution_id") == execution_id
            and record.get("pipeline_id") == pipeline_id
            and record.get("execution_id") == execution_id
            and handle.get("pipeline_id") == pipeline_id
            and handle.get("execution_id") == execution_id
            and progress.get("pipeline_id") == pipeline_id
            and progress.get("execution_id") == execution_id
            and (
                scheduler_provider is _UNBOUND_JARVIS_IDENTITY
                or (
                    handle.get("mode") == expected_mode
                    and record.get("mode") == expected_mode
                    and handle.get("scheduler_provider") == scheduler_provider
                    and record.get("scheduler_provider") == scheduler_provider
                )
            )
            and handle.get("schema_version") == "jarvis.execution.handle.v1"
            and record.get("state") == observation.get("state")
            and record.get("terminal") is observation.get("terminal")
            and progress.get("execution_state") == observation.get("state")
            and progress.get("terminal") is observation.get("terminal")
            and record.get("schema_version") == "jarvis.execution.record.v1"
            and progress.get("schema_version") == "jarvis.execution.progress.v1"
            and isinstance(observation.get("query_job_id"), str)
            and bool(observation.get("query_job_id"))
        ):
            identities_valid = False
        normalized.append((observation, handle, record, progress))

    base_identity_fields = (
        "execution_id",
        "pipeline_id",
        "mode",
        "scheduler_provider",
    )
    snapshot_identity_fields = (*base_identity_fields, "scheduler_native_id", "cluster")
    stable_identity: tuple[object, ...] | None = None
    assigned_scheduler_native_id: str | None = None
    assigned_scheduler_cluster: str | None = None
    scheduler_identity_valid = bool(
        scheduler_cluster is _UNBOUND_JARVIS_IDENTITY
        or scheduler_cluster is None
        or (isinstance(scheduler_cluster, str) and bool(scheduler_cluster))
    )
    for observation, handle, record, _progress in normalized:
        identity = tuple(handle.get(field) for field in base_identity_fields)
        if stable_identity is None:
            stable_identity = identity
        elif identity != stable_identity:
            scheduler_identity_valid = False
        if any(handle.get(field) != record.get(field) for field in snapshot_identity_fields):
            scheduler_identity_valid = False
        mode = handle.get("mode")
        provider = handle.get("scheduler_provider")
        native_id = handle.get("scheduler_native_id")
        native_cluster = handle.get("cluster")
        if mode == "direct":
            if provider is not None or native_id is not None or native_cluster is not None:
                scheduler_identity_valid = False
            continue
        if mode != "scheduler" or not isinstance(provider, str) or not provider:
            scheduler_identity_valid = False
            continue
        if native_id is None:
            if assigned_scheduler_native_id is not None or observation.get("terminal") is True:
                scheduler_identity_valid = False
        elif not isinstance(native_id, str) or not native_id:
            scheduler_identity_valid = False
        elif assigned_scheduler_native_id is None:
            assigned_scheduler_native_id = native_id
        elif native_id != assigned_scheduler_native_id:
            scheduler_identity_valid = False
        if native_cluster is None:
            if assigned_scheduler_cluster is not None:
                scheduler_identity_valid = False
        elif not isinstance(native_cluster, str) or not native_cluster:
            scheduler_identity_valid = False
        elif assigned_scheduler_cluster is None:
            assigned_scheduler_cluster = native_cluster
        elif native_cluster != assigned_scheduler_cluster:
            scheduler_identity_valid = False
    if scheduler_cluster is not _UNBOUND_JARVIS_IDENTITY:
        if scheduler_cluster is None:
            if assigned_scheduler_cluster is not None:
                scheduler_identity_valid = False
        elif assigned_scheduler_cluster != scheduler_cluster:
            scheduler_identity_valid = False
    integrity_violations: list[JSON] = []
    for observation, _handle, _record, _progress in normalized:
        marker = observation.get("relay_query_integrity")
        if marker is None:
            continue
        if _valid_query_integrity_marker(marker):
            integrity_violations.append(cast(JSON, marker))
        else:
            integrity_violations.append(
                {
                    "schema_version": "clio-relay.jarvis-query-integrity.v1",
                    "valid": False,
                    "reason": "integrity_marker_invalid",
                    "previous_state": None,
                    "current_state": observation.get("state"),
                }
            )
    verified_gap_counts: list[int] = []
    invalid_verified_gaps: list[JSON] = []
    verified_gap_count = 0
    for index, (observation, _handle, _record, _progress) in enumerate(normalized):
        marker = observation.get("relay_query_verified_gap")
        if marker is not None:
            if index == 0 or not _valid_query_verified_gap(
                marker,
                previous=normalized[index - 1][0],
                current=observation,
            ):
                invalid_verified_gaps.append(
                    {
                        "index": index,
                        "query_job_id": observation.get("query_job_id"),
                    }
                )
            else:
                verified_gap_count += 1
        verified_gap_counts.append(verified_gap_count)
    lifecycle_prefix_valid = bool(
        normalized
        and identities_valid
        and scheduler_identity_valid
        and not integrity_violations
        and not invalid_verified_gaps
    )
    last_known_rank = -1
    terminal_snapshot: tuple[object, object, object] | None = None
    terminal_seen = False
    for index, (observation, _handle, record, _progress) in enumerate(normalized):
        state = observation.get("state")
        if state == "unknown":
            if (
                observation.get("terminal") is not False
                or index == len(normalized) - 1
                or terminal_seen
            ):
                lifecycle_prefix_valid = False
            continue
        rank = state_rank.get(state) if isinstance(state, str) else None
        if rank is None:
            lifecycle_prefix_valid = False
            continue
        if rank < last_known_rank:
            lifecycle_prefix_valid = False
        last_known_rank = max(last_known_rank, rank)
        terminal = observation.get("terminal")
        if terminal is True:
            if state not in {"completed", "failed", "canceled"}:
                lifecycle_prefix_valid = False
            snapshot = (state, record.get("return_code"), record.get("error"))
            if terminal_snapshot is None:
                terminal_snapshot = snapshot
            elif snapshot != terminal_snapshot:
                lifecycle_prefix_valid = False
            terminal_seen = True
        elif terminal is False:
            if (
                terminal_seen
                or state in {"completed", "failed", "canceled"}
                or record.get("return_code") is not None
                or record.get("error") is not None
            ):
                lifecycle_prefix_valid = False
        else:
            lifecycle_prefix_valid = False
    terminal_success_valid = bool(
        lifecycle_prefix_valid
        and normalized[-1][0].get("state") == "completed"
        and normalized[-1][0].get("terminal") is True
        and normalized[-1][2].get("return_code") == 0
        and normalized[-1][2].get("error") is None
    )

    live_package: tuple[int, JSON, JSON, JSON] | None = None
    progress_monotonic = True
    package_counters: dict[tuple[object, object], tuple[int, int, JSON | None, int]] = {}
    for observation_index, (
        observation,
        _handle,
        _record,
        execution_progress,
    ) in enumerate(normalized):
        packages = execution_progress.get("packages")
        if not isinstance(packages, list):
            progress_monotonic = False
            continue
        for raw_package in cast(list[object], packages):
            package = _mapping(raw_package)
            if package is None:
                progress_monotonic = False
                continue
            key = (package.get("package_id"), package.get("package_name"))
            event_count = package.get("event_count")
            latest = _mapping(package.get("latest"))
            base_valid = bool(
                isinstance(key[0], str)
                and bool(key[0])
                and isinstance(key[1], str)
                and bool(key[1])
                and _nonnegative_int(event_count)
            )
            if not base_valid:
                progress_monotonic = False
                continue
            if event_count == 0 and latest is None:
                counters = (0, -1)
            elif latest is not None and bool(
                _nonnegative_int(latest.get("sequence"))
                and latest.get("schema_version") == "jarvis.progress.v1"
                and latest.get("execution_id") == execution_id
                and latest.get("package_id") == key[0]
                and latest.get("package_name") == key[1]
                and _valid_jarvis_progress_semantics(latest)
            ):
                counters = (cast(int, event_count), cast(int, latest["sequence"]))
            else:
                progress_monotonic = False
                continue
            prior = package_counters.get(key)
            if prior is not None:
                if counters[0] < prior[0] or counters[1] < prior[1]:
                    progress_monotonic = False
                prior_latest = prior[2]
                crossed_verified_gap = verified_gap_counts[observation_index] > prior[3]
                if prior_latest is not None and latest is not None:
                    if counters[1] == prior[1]:
                        if counters[0] != prior[0] or _jarvis_progress_semantic_signature(
                            latest
                        ) != _jarvis_progress_semantic_signature(prior_latest):
                            progress_monotonic = False
                    elif counters[1] > prior[1] and (
                        counters[0] <= prior[0]
                        or (
                            not crossed_verified_gap
                            and not _jarvis_progress_transition_nonregressing(
                                prior_latest,
                                latest,
                            )
                        )
                    ):
                        progress_monotonic = False
            package_counters[key] = (
                counters[0],
                counters[1],
                latest,
                verified_gap_counts[observation_index],
            )
        if observation.get("state") != "running" or observation.get("terminal") is not False:
            continue
        for raw_package in cast(list[object], packages):
            package = _mapping(raw_package)
            latest = _mapping(package.get("latest")) if package else None
            if (
                package is not None
                and latest is not None
                and _positive_int(package.get("event_count"))
                and latest.get("schema_version") == "jarvis.progress.v1"
                and latest.get("execution_id") == execution_id
                and latest.get("package_id") == package.get("package_id")
                and latest.get("package_name") == package.get("package_name")
                and _valid_jarvis_progress_semantics(latest)
                and _nonnegative_int(latest.get("sequence"))
            ):
                live_package = (observation_index, observation, package, latest)

    terminal_package: tuple[JSON, JSON] | None = None
    if live_package is not None and normalized:
        live_summary = live_package[2]
        terminal_packages = normalized[-1][3].get("packages")
        if isinstance(terminal_packages, list):
            for raw_package in cast(list[object], terminal_packages):
                package = _mapping(raw_package)
                latest = _mapping(package.get("latest")) if package else None
                if (
                    package is not None
                    and latest is not None
                    and package.get("package_id") == live_summary.get("package_id")
                    and package.get("package_name") == live_summary.get("package_name")
                ):
                    terminal_package = (package, latest)
                    break

    progress_valid = False
    if live_package is not None and terminal_package is not None:
        live_index, _live_observation, live_summary, live_latest = live_package
        terminal_summary, terminal_latest = terminal_package
        progress_valid = bool(
            terminal_latest.get("schema_version") == "jarvis.progress.v1"
            and terminal_latest.get("execution_id") == execution_id
            and terminal_latest.get("package_id") == live_latest.get("package_id")
            and terminal_latest.get("package_name") == live_latest.get("package_name")
            and _nonnegative_int(terminal_summary.get("event_count"))
            and cast(int, terminal_summary["event_count"]) >= cast(int, live_summary["event_count"])
            and _nonnegative_int(terminal_latest.get("sequence"))
            and cast(int, terminal_latest["sequence"]) >= cast(int, live_latest["sequence"])
            and _valid_jarvis_progress_semantics(terminal_latest)
            and (
                verified_gap_counts[-1] > verified_gap_counts[live_index]
                or _jarvis_progress_transition_nonregressing(live_latest, terminal_latest)
            )
        )

    compact_observations = [
        {
            "query_job_id": observation.get("query_job_id"),
            "state": observation.get("state"),
            "terminal": observation.get("terminal"),
            "query_integrity": observation.get("relay_query_integrity"),
            "verified_gap": observation.get("relay_query_verified_gap"),
            "package_count": (
                len(cast(list[object], progress.get("packages")))
                if isinstance(progress.get("packages"), list)
                else None
            ),
        }
        for observation, _handle, _record, progress in normalized
    ]
    observations_truncated = len(compact_observations) > 32
    if observations_truncated:
        compact_observations = [*compact_observations[:31], compact_observations[-1]]
    assertions = {
        "observation_count_bounded": bounded,
        "query_identities_coherent": identities_valid,
        "scheduler_identity_optional_coherent_and_stable": scheduler_identity_valid,
        "lifecycle_prefix_coherent": lifecycle_prefix_valid,
        "terminal_success_verified": terminal_success_valid,
        "in_flight_package_progress_observed": live_package is not None,
        "package_progress_nonregressing": progress_monotonic,
        "terminal_package_progress_bound": progress_valid,
    }
    evidence: JSON = {
        "pipeline_id": pipeline_id,
        "execution_id": execution_id,
        "observation_count": len(observations),
        "observations": compact_observations,
        "observations_truncated": observations_truncated,
        "query_integrity_violations": integrity_violations,
        "verified_gap_count": verified_gap_count,
        "invalid_verified_gaps": invalid_verified_gaps,
        "live_progress": (
            _compact_package_progress(live_package[3]) if live_package is not None else {}
        ),
        "terminal_progress": (
            _compact_package_progress(terminal_package[1]) if terminal_package is not None else {}
        ),
        "assertions": assertions,
    }
    passed = all(assertions.values())
    if live_package is None or terminal_package is None:
        return evidence, passed, None
    _live_index, live_observation, _live_summary, live_latest = live_package
    terminal_summary, terminal_latest = terminal_package
    resource: JSON = {
        "resource_id": f"{execution_id}:{live_latest.get('package_id', 'package')}",
        "provider": "jarvis-cd",
        "metadata": {
            "source": "jarvis_get_execution",
            "pipeline_id": pipeline_id,
            "execution_id": execution_id,
            "package_id": live_latest.get("package_id"),
            "package_name": live_latest.get("package_name"),
            "progress_schema_version": live_latest.get("schema_version"),
            "progress_determinate": live_latest.get("determinate"),
            "progress_event_count": terminal_summary.get("event_count"),
            "progress_sequence": terminal_latest.get("sequence"),
            "provider_source_authority": "jarvis_get_execution",
            "native_documents_validated": identities_valid,
            "query_identity_validated": identities_valid,
            "live_observed_while_running": True,
            "lifecycle_prefix_validated": lifecycle_prefix_valid,
            "terminal_success_validated": terminal_success_valid,
            "terminal_query_bound": progress_valid,
            "live_query_job_id": live_observation.get("query_job_id"),
            "terminal_query_job_id": normalized[-1][0].get("query_job_id"),
        },
    }
    return evidence, passed, resource


def _valid_jarvis_progress_semantics(progress: JSON) -> bool:
    """Validate the quantitative and phase semantics of one native progress event."""
    state = progress.get("state")
    label = progress.get("label")
    if (
        not isinstance(state, str)
        or state not in _JARVIS_PROGRESS_STATES
        or not isinstance(label, str)
        or not label.strip()
        or len(label) > _MAX_JARVIS_PROGRESS_IDENTITY_TEXT
    ):
        return False
    current = progress.get("current")
    total = progress.get("total")
    if current is not None and (not _finite_progress_number(current) or current < 0):
        return False
    if total is not None and (
        not _finite_progress_number(total)
        or total <= 0
        or current is None
        or not _finite_progress_number(current)
        or current > total
    ):
        return False
    unit = progress.get("unit")
    if unit is not None and (
        not isinstance(unit, str)
        or not unit.strip()
        or len(unit) > _MAX_JARVIS_PROGRESS_IDENTITY_TEXT
    ):
        return False
    determinate = progress.get("determinate")
    return isinstance(determinate, bool) and determinate is (
        current is not None and total is not None
    )


def _finite_progress_number(value: object) -> TypeGuard[int | float]:
    """Return whether a value is a finite, non-boolean JSON number."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return False
    try:
        return math.isfinite(value)
    except (OverflowError, TypeError, ValueError):
        return False


def _jarvis_progress_semantic_signature(progress: JSON) -> tuple[object, ...]:
    """Return fields that must not change without a new progress sequence."""
    return tuple(
        progress.get(field)
        for field in (
            "state",
            "label",
            "determinate",
            "current",
            "total",
            "unit",
        )
    )


def _jarvis_progress_transition_nonregressing(previous: JSON, current: JSON) -> bool:
    """Reject quantitative regression within a phase while allowing explicit phase changes."""
    previous_phase = (previous.get("state"), previous.get("label"))
    current_phase = (current.get("state"), current.get("label"))
    if current_phase != previous_phase:
        return True
    previous_unit = previous.get("unit")
    current_unit = current.get("unit")
    if previous_unit is not None and current_unit != previous_unit:
        return False
    previous_total = previous.get("total")
    current_total = current.get("total")
    if previous_total is not None and current_total != previous_total:
        return False
    previous_value = previous.get("current")
    current_value = current.get("current")
    return previous_value is None or bool(
        current_value is not None
        and cast(int | float, current_value) >= cast(int | float, previous_value)
    )


def _compact_package_progress(progress: JSON) -> JSON:
    """Keep validation reports bounded while retaining progress semantics."""
    return {
        field: progress.get(field)
        for field in (
            "schema_version",
            "execution_id",
            "package_id",
            "package_name",
            "state",
            "label",
            "sequence",
            "determinate",
            "current",
            "total",
            "unit",
        )
    }


def _jarvis_live_progress_evidence(
    *,
    progress: list[JSON],
    live_observation: JSON | None,
    call_job_id: str,
    pipeline_id: object,
    expected_server_artifact_digest: object,
    mcp_result: JSON | None,
    runtime_metadata: JSON | None,
) -> tuple[JSON, bool, JSON | None]:
    """Validate live and final progress owned by one native JARVIS execution."""
    candidates: list[tuple[int, JSON, JSON]] = []
    for index, record in enumerate(progress):
        metadata = _mapping(record.get("metadata"))
        if metadata is None or record.get("job_id") != call_job_id:
            continue
        if (
            metadata.get("source") != "jarvis_execution"
            or metadata.get("provider_source_authority") != "jarvis_mcp_progress_notification"
            or metadata.get("producer_validated") is not True
            or metadata.get("relay_job_id") != call_job_id
            or metadata.get("run_id") != metadata.get("execution_id")
            or metadata.get("progress_schema_version") != "jarvis.progress.v1"
        ):
            continue
        candidates.append((index, record, metadata))

    warming: tuple[int, JSON, JSON] | None = None
    accepted: tuple[int, JSON, JSON] | None = None
    for warming_candidate in candidates:
        warming_index, warming_record, warming_metadata = warming_candidate
        if warming_metadata.get("execution_binding_validated") is not False or not _positive_int(
            warming_metadata.get("progress_transport_sequence")
        ):
            continue
        for accepted_candidate in candidates:
            accepted_index, accepted_record, accepted_metadata = accepted_candidate
            if accepted_index <= warming_index:
                continue
            if (
                accepted_metadata.get("execution_binding_validated") is not True
                or not _same_native_progress_execution(
                    warming_metadata,
                    accepted_metadata,
                )
                or not _nondecreasing_native_progress(warming_metadata, accepted_metadata)
            ):
                continue
            warming = warming_candidate
            accepted = accepted_candidate
            break
        if warming is not None:
            break

    warming_record = warming[1] if warming is not None else None
    warming_metadata = warming[2] if warming is not None else None
    accepted_record = accepted[1] if accepted is not None else None
    accepted_metadata = accepted[2] if accepted is not None else None
    live_observed = (
        live_observation is not None
        and warming_record is not None
        and live_observation.get("progress_id") == warming_record.get("progress_id")
        and live_observation.get("job_state") == "running"
        and live_observation.get("terminal") is False
    )
    progress_binding_valid = (
        accepted_metadata is not None
        and isinstance(pipeline_id, str)
        and accepted_metadata.get("pipeline_id") == pipeline_id
        and _is_sha256(expected_server_artifact_digest)
        and accepted_metadata.get("server_artifact_digest") == expected_server_artifact_digest
        and all(
            isinstance(accepted_metadata.get(key), str) and bool(accepted_metadata.get(key))
            for key in _NATIVE_PROGRESS_IDENTITY_KEYS
        )
        and isinstance(accepted_metadata.get("progress_determinate"), bool)
        and _nonnegative_int(accepted_metadata.get("progress_event_count"))
        and _nonnegative_int(accepted_metadata.get("progress_sequence"))
        and _nonnegative_int(accepted_metadata.get("progress_transport_sequence"))
    )

    result_bridge = _mapping(mcp_result.get("package_progress_bridge")) if mcp_result else None
    bridge_valid = (
        result_bridge is not None
        and accepted_metadata is not None
        and result_bridge.get("schema_version") == "clio-relay.mcp-jarvis-progress-bridge.v1"
        and result_bridge.get("execution_validated") is True
        and _positive_int(result_bridge.get("notification_count"))
        and result_bridge.get("execution_id") == accepted_metadata.get("execution_id")
        and result_bridge.get("pipeline_id") == pipeline_id
        and result_bridge.get("expected_server_artifact_digest") == expected_server_artifact_digest
        and result_bridge.get("observed_server_artifact_digest") == expected_server_artifact_digest
        and isinstance(result_bridge.get("package_sequences"), dict)
    )

    runtime_details = _mapping(runtime_metadata.get("details")) if runtime_metadata else None
    native_execution = (
        _mapping(runtime_details.get("native_execution")) if runtime_details else None
    )
    native_progress = _mapping(native_execution.get("progress")) if native_execution else None
    native_packages = native_progress.get("packages") if native_progress else None
    runtime_package_bound = _native_runtime_package_bound(
        native_packages,
        accepted_metadata,
    )
    runtime_bound = (
        runtime_metadata is not None
        and accepted_metadata is not None
        and runtime_metadata.get("execution_id") == accepted_metadata.get("execution_id")
        and runtime_metadata.get("pipeline_id") == pipeline_id
        and native_progress is not None
        and native_progress.get("schema_version") == "jarvis.execution.progress.v1"
        and native_progress.get("execution_id") == accepted_metadata.get("execution_id")
        and runtime_package_bound
    )
    passed = bool(
        warming is not None
        and accepted is not None
        and live_observed
        and progress_binding_valid
        and bridge_valid
        and runtime_bound
    )
    evidence: JSON = {
        "execution_id": (accepted_metadata.get("execution_id") if accepted_metadata else None),
        "progress_record_count": len(progress),
        "warming_progress_id": warming_record.get("progress_id") if warming_record else None,
        "accepted_progress_id": accepted_record.get("progress_id") if accepted_record else None,
        "notification_sequence": (
            accepted_metadata.get("progress_transport_sequence") if accepted_metadata else None
        ),
        "live_observation": live_observation or {},
        "live_observed_while_running": live_observed,
        "expected_pipeline_id": pipeline_id,
        "expected_server_artifact_digest": expected_server_artifact_digest,
        "progress_binding_valid": progress_binding_valid,
        "bridge_valid": bridge_valid,
        "runtime_bound": runtime_bound,
        "bridge": result_bridge or {},
        "native_progress": (
            {key: accepted_metadata.get(key) for key in _NATIVE_PROGRESS_IDENTITY_KEYS}
            if accepted_metadata
            else {}
        ),
    }
    if accepted_record is None or accepted_metadata is None:
        return evidence, passed, None
    resource = {
        "resource_id": (
            f"{accepted_metadata.get('execution_id', 'execution')}:"
            f"{accepted_metadata.get('package_id', 'package')}"
        ),
        "provider": "jarvis-cd",
        "metadata": {
            **accepted_metadata,
            "warming_progress_id": warming_record.get("progress_id") if warming_record else None,
            "accepted_progress_id": accepted_record.get("progress_id"),
            "live_observed_while_running": live_observed,
            "bridge_validated": bridge_valid,
            "runtime_bound": runtime_bound,
        },
    }
    return evidence, passed, resource


def _same_native_progress_execution(
    warming_metadata: JSON,
    accepted_metadata: JSON,
) -> bool:
    """Return whether two observations belong to one native package execution."""
    return all(
        warming_metadata.get(key) == accepted_metadata.get(key)
        for key in _NATIVE_PROGRESS_IDENTITY_KEYS
    )


def _nondecreasing_native_progress(warming: JSON, accepted: JSON) -> bool:
    """Require final native progress counters not to regress from the live event."""
    return all(
        _nonnegative_int(warming.get(key))
        and _nonnegative_int(accepted.get(key))
        and cast(int, accepted[key]) >= cast(int, warming[key])
        for key in (
            "progress_sequence",
            "progress_event_count",
            "progress_transport_sequence",
        )
    )


def _native_runtime_package_bound(
    packages: object,
    metadata: JSON | None,
) -> bool:
    """Bind an accepted progress observation to the final native snapshot."""
    if not isinstance(packages, list) or metadata is None:
        return False
    for item in cast(list[object], packages):
        package = _mapping(item)
        if package is None:
            continue
        if (
            package.get("package_id") == metadata.get("package_id")
            and package.get("package_name") == metadata.get("package_name")
            and _nonnegative_int(package.get("event_count"))
            and cast(int, package["event_count"]) >= cast(int, metadata["progress_event_count"])
        ):
            return True
    return False


def _nonnegative_int(value: object) -> TypeGuard[int]:
    """Return whether a value is a non-boolean, nonnegative integer."""
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def _positive_int(value: object) -> TypeGuard[int]:
    """Return whether a value is a non-boolean, positive integer."""
    return _nonnegative_int(value) and value > 0


def _remote_jarvis_contract(document: JSON | None) -> tuple[JSON, bool]:
    protocol = _mapping(document.get("protocol_result")) if document else None
    raw_tools = protocol.get("tools") if protocol else None
    tools = (
        [cast(JSON, item) for item in cast(list[object], raw_tools) if isinstance(item, dict)]
        if isinstance(raw_tools, list)
        else []
    )
    by_name = {str(tool["name"]): tool for tool in tools if isinstance(tool.get("name"), str)}
    expected = set(jarvis_user_contract())
    edit_schema = _mapping(by_name.get("jarvis_edit_step", {}).get("inputSchema"))
    edit_properties = _mapping(edit_schema.get("properties")) if edit_schema else None
    operation = _mapping(edit_properties.get("operation")) if edit_properties else None
    run_schema = _mapping(by_name.get("jarvis_run", {}).get("inputSchema"))
    run_properties = _mapping(run_schema.get("properties")) if run_schema else None
    spack_specs = _mapping(run_properties.get("spack_specs")) if run_properties else None
    handle_first_run = run_properties is not None and "wait" not in run_properties
    query_evidence, query_passed = _execution_query_contract_evidence(
        by_name.get("jarvis_get_execution")
    )
    package_search_evidence, package_search_passed = _package_search_contract_evidence(
        by_name.get("jarvis_describe")
    )
    observed_digest: str | None = None
    contract_error: str | None = None
    try:
        typed_tools = [_remote_contract_tool(tool) for tool in tools]
        observed_digest = remote_mcp_schema_digest(typed_tools)
    except (TypeError, ValueError, ValidationError) as exc:
        contract_error = str(exc)
    passed = (
        document is not None
        and document.get("returncode") == 0
        and set(by_name) == expected
        and operation is not None
        and operation.get("enum") == ["edit", "remove"]
        and spack_specs is not None
        and handle_first_run
        and query_passed
        and package_search_passed
        and observed_digest == CLIO_KIT_JARVIS_USER_CONTRACT_SHA256
    )
    return (
        {
            "remote_tool_names": sorted(by_name),
            "expected_tool_names": sorted(expected),
            "edit_operation_schema": operation or {},
            "spack_specs_schema": spack_specs or {},
            "jarvis_run_input_fields": sorted(run_properties) if run_properties else [],
            "handle_first_run": handle_first_run,
            "internal_wait_exposed": bool(run_properties and "wait" in run_properties),
            "package_search": package_search_evidence,
            "execution_query": query_evidence,
            "expected_contract_sha256": CLIO_KIT_JARVIS_USER_CONTRACT_SHA256,
            "expected_clio_kit_version": CLIO_KIT_JARVIS_MCP_VERSION,
            "observed_contract_sha256": observed_digest,
            "contract_error": contract_error,
        },
        passed,
    )


def _package_search_contract_evidence(tool: JSON | None) -> tuple[JSON, bool]:
    """Summarize the bounded package-discovery surface from live tools/list."""
    input_schema = _mapping(tool.get("inputSchema")) if tool else None
    properties = _mapping(input_schema.get("properties")) if input_schema else None
    required = input_schema.get("required") if input_schema else None
    target = _mapping(properties.get("target")) if properties else None
    query_selector = _mapping(properties.get("query")) if properties else None
    query = _schema_option(query_selector, expected_type="string")
    page_size = _mapping(properties.get("page_size")) if properties else None
    cursor_selector = _mapping(properties.get("cursor")) if properties else None
    cursor = _schema_option(cursor_selector, expected_type="string")
    expected_fields = {
        "target",
        "package_name",
        "query",
        "page_size",
        "cursor",
        "pipeline_id",
        "step_id",
        "include_yaml",
    }
    target_values = (
        cast(list[object], target.get("enum"))
        if target is not None and isinstance(target.get("enum"), list)
        else []
    )
    passed = (
        input_schema is not None
        and input_schema.get("additionalProperties") is False
        and properties is not None
        and set(properties) == expected_fields
        and required == ["target"]
        and target_values == ["packages", "package_search", "package", "pipeline", "step"]
        and query == {"maxLength": 256, "minLength": 1, "type": "string"}
        and page_size is not None
        and page_size.get("default") == 10
        and page_size.get("minimum") == 1
        and page_size.get("maximum") == 25
        and page_size.get("type") == "integer"
        and cursor == {"maxLength": 1024, "minLength": 1, "type": "string"}
    )
    return (
        {
            "input_fields": sorted(properties) if properties is not None else [],
            "required": required if isinstance(required, list) else [],
            "target_values": target_values,
            "query_schema": query or {},
            "page_size_schema": page_size or {},
            "cursor_schema": cursor or {},
            "bounded": passed,
        },
        passed,
    )


def _execution_query_contract_evidence(tool: JSON | None) -> tuple[JSON, bool]:
    """Summarize the unified progress/artifact query without copying its full schema."""
    input_schema = _mapping(tool.get("inputSchema")) if tool else None
    input_properties = _mapping(input_schema.get("properties")) if input_schema else None
    required = input_schema.get("required") if input_schema else None
    include_progress = (
        _mapping(input_properties.get("include_progress")) if input_properties else None
    )
    include_service_runtimes = (
        _mapping(input_properties.get("include_service_runtimes")) if input_properties else None
    )
    artifact_selector = _mapping(input_properties.get("artifacts")) if input_properties else None
    artifact_query = _schema_option(artifact_selector, expected_type="object")
    artifact_filters = _mapping(artifact_query.get("properties")) if artifact_query else None
    page_size = _mapping(artifact_filters.get("page_size")) if artifact_filters else None
    output_schema = _mapping(tool.get("outputSchema")) if tool else None
    output_properties = _mapping(output_schema.get("properties")) if output_schema else None
    output_required = output_schema.get("required") if output_schema else None
    progress_selector = _mapping(output_properties.get("progress")) if output_properties else None
    progress = _schema_option(progress_selector, expected_type="object")
    progress_properties = _mapping(progress.get("properties")) if progress else None
    page_selector = _mapping(output_properties.get("artifact_page")) if output_properties else None
    artifact_page = _schema_option(page_selector, expected_type="object")
    artifact_page_properties = _mapping(artifact_page.get("properties")) if artifact_page else None
    artifacts_schema = (
        _mapping(artifact_page_properties.get("artifacts")) if artifact_page_properties else None
    )
    artifact_item = _mapping(artifacts_schema.get("items")) if artifacts_schema else None
    artifact_item_properties = _mapping(artifact_item.get("properties")) if artifact_item else None
    service_selector = (
        _mapping(output_properties.get("service_runtimes")) if output_properties else None
    )
    service_runtimes = _schema_option(service_selector, expected_type="object")
    service_runtime_properties = (
        _mapping(service_runtimes.get("properties")) if service_runtimes else None
    )
    expected_inputs = {
        "pipeline_id",
        "execution_id",
        "include_progress",
        "include_service_runtimes",
        "artifacts",
    }
    expected_filters = {
        "package_id",
        "role",
        "state",
        "artifact_id",
        "page_size",
        "cursor",
    }
    expected_outputs = {
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
    passed = bool(
        input_schema is not None
        and input_schema.get("additionalProperties") is False
        and input_properties is not None
        and set(input_properties) == expected_inputs
        and isinstance(required, list)
        and set(cast(list[object], required)) == {"pipeline_id", "execution_id"}
        and include_progress == {"default": True, "type": "boolean"}
        and include_service_runtimes == {"default": False, "type": "boolean"}
        and artifact_selector is not None
        and artifact_selector.get("default") is None
        and artifact_query is not None
        and artifact_query.get("additionalProperties") is False
        and artifact_filters is not None
        and set(artifact_filters) == expected_filters
        and page_size
        == {
            "default": 50,
            "description": "Maximum artifacts to return in this page.",
            "maximum": 100,
            "minimum": 1,
            "type": "integer",
        }
        and output_schema is not None
        and output_schema.get("additionalProperties") is False
        and output_properties is not None
        and set(output_properties) == expected_outputs
        and isinstance(output_required, list)
        and set(cast(list[object], output_required)) == expected_outputs
        and output_properties.get("schema_version")
        == {"const": CLIO_KIT_JARVIS_EXECUTION_SCHEMA, "type": "string"}
        and progress_properties is not None
        and progress_properties.get("schema_version")
        == {"const": "jarvis.execution.progress.v1", "type": "string"}
        and artifact_page_properties is not None
        and artifact_page_properties.get("producer_schema_version")
        == {"const": "jarvis.execution.artifacts.v1", "type": "string"}
        and artifact_item_properties is not None
        and artifact_item_properties.get("schema_version")
        == {"const": "jarvis.artifact.v1", "type": "string"}
        and service_runtime_properties is not None
        and service_runtime_properties.get("schema_version")
        == {"const": JARVIS_EXECUTION_SERVICE_RUNTIMES_SCHEMA, "type": "string"}
    )
    return (
        {
            "input_fields": sorted(input_properties) if input_properties else [],
            "required_identity_fields": sorted(str(item) for item in cast(list[object], required))
            if isinstance(required, list)
            else [],
            "include_progress_schema": include_progress or {},
            "include_service_runtimes_schema": include_service_runtimes or {},
            "artifact_filter_fields": sorted(artifact_filters) if artifact_filters else [],
            "artifact_page_size_schema": page_size or {},
            "output_fields": sorted(output_properties) if output_properties else [],
            "progress_schema_version": (
                progress_properties.get("schema_version") if progress_properties else None
            ),
            "artifact_page_schema_version": (
                artifact_page_properties.get("producer_schema_version")
                if artifact_page_properties
                else None
            ),
            "artifact_schema_version": (
                artifact_item_properties.get("schema_version") if artifact_item_properties else None
            ),
            "service_runtimes_schema_version": (
                service_runtime_properties.get("schema_version")
                if service_runtime_properties
                else None
            ),
        },
        passed,
    )


def _schema_option(schema: JSON | None, *, expected_type: str) -> JSON | None:
    """Return one non-null branch from a nullable JSON Schema property."""
    raw_options = schema.get("anyOf") if schema else None
    if not isinstance(raw_options, list):
        return None
    options = [
        cast(JSON, item) for item in cast(list[object], raw_options) if isinstance(item, dict)
    ]
    non_null = [item for item in options if item.get("type") == expected_type]
    nulls = [item for item in options if item == {"type": "null"}]
    if len(options) != 2 or len(non_null) != 1 or len(nulls) != 1:
        return None
    return non_null[0]


def _remote_contract_tool(tool: JSON) -> RemoteMcpToolSchema:
    name = tool.get("name")
    input_schema = _mapping(tool.get("inputSchema"))
    if not isinstance(name, str) or input_schema is None:
        raise ValueError("remote JARVIS MCP returned an invalid tool contract")
    title = _optional_contract_string(tool, "title")
    description = _optional_contract_string(tool, "description")
    output_schema = _optional_contract_mapping(tool, "outputSchema")
    annotations = _optional_contract_mapping(tool, "annotations")
    return RemoteMcpToolSchema(
        name=name,
        title=title,
        description=description,
        input_schema=input_schema,
        output_schema=output_schema,
        annotations=annotations,
    )


def _optional_contract_string(tool: JSON, key: str) -> str | None:
    value = tool.get(key)
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"remote JARVIS MCP tool {key} must be a string")
    return value


def _optional_contract_mapping(tool: JSON, key: str) -> JSON | None:
    value = tool.get(key)
    if value is None:
        return None
    if not isinstance(value, dict):
        raise ValueError(f"remote JARVIS MCP tool {key} must be an object")
    return cast(JSON, value)


def _local_jarvis_contract(tool: JSON | None, name: str) -> tuple[JSON, bool]:
    expected = jarvis_user_contract().get(name)
    if tool is None or expected is None:
        return ({"tool": name, "error": "tool is not part of the pinned contract"}, False)
    actual_input = _mapping(tool.get("inputSchema"))
    actual_output = _mapping(tool.get("outputSchema"))
    actual_annotations = _mapping(tool.get("annotations"))
    if actual_input is None:
        return ({"tool": name, "error": "tool has no input schema"}, False)

    remote_input = deepcopy(actual_input)
    properties = _mapping(remote_input.get("properties"))
    if properties is None:
        return ({"tool": name, "error": "tool has no property map"}, False)
    cluster_property = _mapping(properties.get("cluster"))
    cluster_values = cluster_property.get("enum") if cluster_property is not None else None
    clusters = cluster_values if _is_string_list(cluster_values) else None
    for key in (
        "cluster",
        "timeout_seconds",
        "idempotency_key",
        "wait_for_terminal",
        "wait_timeout_seconds",
        "poll_seconds",
    ):
        properties.pop(key, None)
    required = remote_input.get("required")
    if isinstance(required, list):
        remote_input["required"] = [
            item for item in cast(list[object], required) if item != "cluster"
        ]

    expected_description = expected.get("description")
    actual_description = tool.get("description")
    input_matches = remote_input == expected.get("inputSchema")
    annotations_match = actual_annotations == expected.get("annotations")
    output_matches = actual_output == virtual_jarvis_job_output_schema(name, clusters=clusters)
    description_matches = (
        isinstance(expected_description, str)
        and isinstance(actual_description, str)
        and actual_description.startswith(expected_description)
    )
    return (
        {
            "tool": name,
            "input_matches_pinned_contract": input_matches,
            "annotations_match_pinned_contract": annotations_match,
            "async_output_contract": output_matches,
            "description_derived_from_pinned_contract": description_matches,
        },
        input_matches and annotations_match and output_matches and description_matches,
    )


def _spack_environment_metadata(runtime_metadata: JSON | None) -> JSON | None:
    details = _mapping(runtime_metadata.get("details")) if runtime_metadata else None
    runtime = _mapping(details.get("runtime_metadata")) if details else None
    runtime_details = _mapping(runtime.get("details")) if runtime else None
    return _mapping(runtime_details.get("environment")) if runtime_details else None


def _jarvis_runtime_scheduler_cluster(runtime_metadata: JSON | None) -> object:
    """Return JARVIS's scheduler-native cluster, which is not the relay route alias."""
    details = _mapping(runtime_metadata.get("details")) if runtime_metadata else None
    native_execution = _mapping(details.get("native_execution")) if details else None
    handle = _mapping(native_execution.get("execution_handle")) if native_execution else None
    return handle.get("cluster") if handle is not None else _UNBOUND_JARVIS_IDENTITY


def _is_sha256(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value.lower())
    )


def _is_string_list(value: object) -> TypeGuard[list[str]]:
    """Return whether a JSON value is a list containing only strings."""
    items = cast(list[object], value)
    return isinstance(value, list) and all(isinstance(item, str) for item in items)


def _listed_tool(response: JSON | None, tool: str) -> JSON | None:
    if response is None or "error" in response:
        return None
    result = _mapping(response.get("result"))
    tools = result.get("tools") if result else None
    if not isinstance(tools, list):
        return None
    typed_tools = cast(list[object], tools)
    return next(
        (
            cast(JSON, item)
            for item in typed_tools
            if isinstance(item, dict) and cast(JSON, item).get("name") == tool
        ),
        None,
    )


def _response_job_id(response: JSON | None) -> str | None:
    if response is None or "error" in response:
        return None
    result = _mapping(response.get("result"))
    structured = _mapping(result.get("structuredContent")) if result else None
    if structured is not None and isinstance(structured.get("job_id"), str):
        return cast(str, structured["job_id"])
    content = result.get("content") if result else None
    if not isinstance(content, list):
        return None
    for item in cast(list[object], content):
        typed = _mapping(item)
        if typed is None or typed.get("type") != "text" or not isinstance(typed.get("text"), str):
            continue
        try:
            payload = cast(object, json.loads(cast(str, typed["text"])))
        except (TypeError, ValueError):
            continue
        typed_payload = _mapping(payload)
        if typed_payload is not None and isinstance(typed_payload.get("job_id"), str):
            return cast(str, typed_payload["job_id"])
    return None


def _check(
    check_id: str,
    summary: str,
    passed: bool,
    started_at: datetime,
    completed_at: datetime,
    metadata: JSON,
) -> ValidationCheck:
    return ValidationCheck(
        check_id=check_id,
        summary=summary,
        status=ValidationStatus.PASSED if passed else ValidationStatus.FAILED,
        started_at=started_at,
        completed_at=completed_at,
        evidence=[
            EvidenceReference(
                kind="jarvis_mcp_acceptance",
                excerpt=summary,
                metadata=metadata,
            )
        ],
        error=None if passed else summary,
    )


def _mapping(value: object) -> JSON | None:
    return cast(JSON, value) if isinstance(value, dict) else None


def _stdio_initialize_passed(*, initialize_response: JSON | None, evidence: JSON | None) -> bool:
    if evidence is None:
        return True
    if (
        evidence.get("boundary") != "packaged_clio_relay_mcp_server_stdio"
        or evidence.get("returncode") != 0
        or initialize_response is None
        or initialize_response.get("error") is not None
    ):
        return False
    result = _mapping(initialize_response.get("result"))
    server_info = _mapping(result.get("serverInfo")) if result else None
    return (
        result is not None
        and isinstance(result.get("protocolVersion"), str)
        and server_info is not None
        and server_info.get("name") == "clio-relay"
    )

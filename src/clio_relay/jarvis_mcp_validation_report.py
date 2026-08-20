"""Assemble the full acceptance report for one virtual JARVIS MCP call.

Owner module for the ``jarvis_mcp_validation.py`` split (clio-relay split/
jarvis-mcp-validation): ``build_jarvis_mcp_validation_report`` is the
top-level orchestrator every other evidence builder in this split feeds
into, assembling the discovery/contract/call/artifact/progress/runtime-
metadata checks and the resulting ``report.resources``/``report.artifacts``
graph. Three single-caller leaf lookups moved to
``jarvis_mcp_validation_report_support.py`` to keep this file under the cap.
The orchestrator itself stays one function (under the 800-line cap, over the
150-500 sweet spot): its checks and the resource graph all read the same
locals computed once at the top, so splitting further would mean
recomputing those locals per fragment or threading a wide parameter list
between fragments -- a restructuring, not a mechanical extraction (the
``session_start_execution.py`` precedent in ``check_file_size.py``'s
``RATCHET_BASELINE`` makes the same call on a sibling split). The facade
re-exports this function under its original name as the split's sole public
entry point.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import cast

from clio_relay.jarvis_mcp import (
    jarvis_cd_lock_binding_expectation,
    jarvis_mcp_server_artifact_verified,
)
from clio_relay.jarvis_mcp_validation_contract import (
    _local_jarvis_contract,
    _remote_jarvis_contract,
)
from clio_relay.jarvis_mcp_validation_core import (
    _UNBOUND_JARVIS_IDENTITY,
    JSON,
    _check,
    _is_sha256,
    _is_string_list,
    _listed_tool,
    _mapping,
    _response_job_id,
    _stdio_initialize_passed,
)
from clio_relay.jarvis_mcp_validation_execution_query import _jarvis_execution_query_evidence
from clio_relay.jarvis_mcp_validation_lifecycle_progress import (
    _jarvis_query_lifecycle_progress_evidence,
)
from clio_relay.jarvis_mcp_validation_live_progress import _jarvis_live_progress_evidence
from clio_relay.jarvis_mcp_validation_package_search import _jarvis_package_search_evidence
from clio_relay.jarvis_mcp_validation_report_support import (
    _artifact_location_references,
    _jarvis_runtime_scheduler_cluster,
    _spack_environment_metadata,
)
from clio_relay.remote_mcp import remote_mcp_server_artifact_digest
from clio_relay.runtime_metadata import RUNTIME_METADATA_SCHEMA, RuntimeMetadataSource
from clio_relay.validation_report import (
    EvidenceReference,
    LiveValidationReport,
    ValidationResource,
    ValidationStatus,
    new_live_validation_report,
)


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

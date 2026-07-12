"""Acceptance evidence for the built-in virtual JARVIS MCP tools."""

from __future__ import annotations

import json
from copy import deepcopy
from datetime import UTC, datetime
from typing import Any, TypeGuard, cast

from pydantic import ValidationError

from clio_relay.jarvis_mcp import (
    CLIO_KIT_JARVIS_MCP_VERSION,
    CLIO_KIT_JARVIS_USER_CONTRACT_SHA256,
    jarvis_user_contract,
)
from clio_relay.remote_mcp import (
    VIRTUAL_REMOTE_MCP_JOB_OUTPUT_SCHEMA,
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
_PROVIDER_IDENTITY_KEYS = (
    "provider_entry_point",
    "provider_entry_point_value",
    "provider_distribution",
    "provider_distribution_version",
    "adapter",
    "package_name",
    "package_version",
    "application_profile",
)
_PROGRESS_REPLAY_KEYS = (
    "provider_execution_id",
    "provider_pipeline_id",
    "provider_server_artifact_digest",
    "provider_notification_sequence",
    *_PROVIDER_IDENTITY_KEYS,
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

    tool_definition = _listed_tool(tools_list_response, tool)
    input_schema = _mapping(tool_definition.get("inputSchema")) if tool_definition else None
    properties = _mapping(input_schema.get("properties")) if input_schema else None
    required = input_schema.get("required") if input_schema else None
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
        and isinstance(required, list)
        and "cluster" in required
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
            "remote JARVIS MCP exposes the five-tool merged user contract",
            remote_contract_passed,
            report.started_at,
            observed_at,
            remote_contract_evidence,
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
    server_install_spec = _server_install_spec(cast(list[object], spec.get("server_args", [])))
    expected_server_artifact_digest = spec.get("expected_server_artifact_digest")
    computed_server_artifact_digest = (
        remote_mcp_server_artifact_digest(server_artifact) if server_artifact is not None else None
    )
    server_artifact_passed = (
        server_artifact is not None
        and server_artifact.get("verified") is True
        and server_artifact.get("server_process_artifact_verified") is True
        and bool(server_artifact.get("executable"))
        and server_artifact.get("install_source") == "wheel"
        and _is_sha256(server_artifact.get("install_artifact_sha256"))
        and server_artifact.get("requested_command") == spec.get("server")
        and server_artifact.get("install_spec") == server_install_spec
        and server_artifact == discovery_server_artifact
        and _is_sha256(expected_server_artifact_digest)
        and expected_server_artifact_digest == computed_server_artifact_digest
        and mcp_result is not None
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

    progress_evidence, progress_passed, progress_resource = _jarvis_live_progress_evidence(
        progress=progress,
        live_observation=live_progress_observation,
        call_job_id=call_job_id,
        pipeline_id=spec.get("arguments", {}).get("pipeline_id")
        if isinstance(spec.get("arguments"), dict)
        else None,
        expected_server_artifact_digest=expected_server_artifact_digest,
        mcp_result=mcp_result,
        runtime_metadata=runtime_metadata,
    )
    report.checks.append(
        _check(
            "remote-mcp.jarvis-live-progress",
            "jarvis_run exposed provider-valid progress before completion and replayed it only "
            "after execution binding",
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
    environment = _spack_environment_metadata(runtime_metadata)
    spack_runtime_passed = (
        spack_specs is not None
        and len(spack_specs) > 0
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
    runtime_passed = (
        tool == "jarvis_run"
        and runtime_metadata is not None
        and runtime_metadata.get("schema_version") == RUNTIME_METADATA_SCHEMA
        and source == RuntimeMetadataSource.JARVIS_MCP.value
        and runtime_metadata.get("pipeline_id") is not None
        and isinstance(scheduler_provider, str)
        and bool(scheduler_provider)
        and isinstance(scheduler_job_id, str)
        and bool(scheduler_job_id)
        and bool(field_sources)
        and RuntimeMetadataSource.LEGACY_STDOUT.value not in set(field_sources.values())
        and scheduler_provider_source in authoritative_runtime_sources
        and scheduler_job_id_source in authoritative_runtime_sources
        and producer_contract is not None
        and producer_contract.get("trusted") is True
        and producer_contract.get("producer_schema_version") == "jarvis.runtime.v1"
        and terminal is not None
        and terminal.get("terminal") is True
        and terminal.get("returncode") == 0
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
                "source": source,
                "pipeline_id": runtime_metadata.get("pipeline_id") if runtime_metadata else None,
                "scheduler_provider": scheduler_provider,
                "scheduler_job_id": scheduler_job_id,
                "scheduler_provider_source": scheduler_provider_source,
                "scheduler_job_id_source": scheduler_job_id_source,
                "field_sources": field_sources or {},
                "producer_contract": producer_contract or {},
                "terminal": terminal or {},
                "runtime_artifact_id": (
                    artifacts_by_kind.get("runtime_metadata", {}).get("artifact_id")
                ),
            },
        )
    )

    if isinstance(job.get("job_id"), str):
        report.resources.append(
            ValidationResource(
                kind="relay_job",
                resource_id=cast(str, job["job_id"]),
                role="virtual_jarvis_mcp_call",
                cluster=cluster,
                state=str(job.get("state")) if job.get("state") is not None else None,
                metadata=job,
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
    if isinstance(scheduler_job_id, str):
        report.resources.append(
            ValidationResource(
                kind="scheduler_job",
                resource_id=scheduler_job_id,
                role="jarvis_owned_execution",
                cluster=cluster,
                state=(
                    str(runtime_metadata.get("scheduler_phase"))
                    if runtime_metadata and runtime_metadata.get("scheduler_phase") is not None
                    else None
                ),
                provider=(
                    str(runtime_metadata.get("scheduler_provider"))
                    if runtime_metadata and runtime_metadata.get("scheduler_provider") is not None
                    else None
                ),
                metadata=runtime_metadata or {},
            )
        )
    if progress_resource is not None:
        report.resources.append(
            ValidationResource(
                kind="package_progress_provider",
                resource_id=str(progress_resource["resource_id"]),
                role="jarvis_mcp_package_progress",
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
    """Validate one warming-to-accepted virtual JARVIS progress replay."""
    candidates: list[tuple[int, JSON, JSON]] = []
    for index, record in enumerate(progress):
        metadata = _mapping(record.get("metadata"))
        if metadata is None or record.get("job_id") != call_job_id:
            continue
        if (
            metadata.get("source") != "jarvis_package"
            or metadata.get("provider_source_authority") != "mcp_progress_notification"
            or metadata.get("provider_validated") is not True
            or metadata.get("run_id") != call_job_id
            or metadata.get("execution_id") != call_job_id
        ):
            continue
        candidates.append((index, record, metadata))

    warming: tuple[int, JSON, JSON] | None = None
    accepted: tuple[int, JSON, JSON] | None = None
    for warming_candidate in candidates:
        warming_index, warming_record, warming_metadata = warming_candidate
        if (
            warming_metadata.get("acceptance_validated") is not False
            or warming_metadata.get("provider_execution_validated") is not False
        ):
            continue
        for accepted_candidate in candidates:
            accepted_index, accepted_record, accepted_metadata = accepted_candidate
            if accepted_index <= warming_index:
                continue
            if (
                accepted_metadata.get("acceptance_validated") is not True
                or accepted_metadata.get("provider_execution_validated") is not True
                or not _same_progress_replay(
                    warming_record,
                    warming_metadata,
                    accepted_record,
                    accepted_metadata,
                )
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
        and accepted_metadata.get("provider_pipeline_id") == pipeline_id
        and _is_sha256(expected_server_artifact_digest)
        and accepted_metadata.get("provider_server_artifact_digest")
        == expected_server_artifact_digest
        and all(
            isinstance(accepted_metadata.get(key), str) and bool(accepted_metadata.get(key))
            for key in _PROVIDER_IDENTITY_KEYS
            if key != "application_profile"
        )
    )

    result_bridge = _mapping(mcp_result.get("package_progress_bridge")) if mcp_result else None
    result_provider = _mapping(result_bridge.get("provider")) if result_bridge else None
    bridge_identity = (
        {
            "provider_entry_point": result_provider.get("entry_point"),
            "provider_entry_point_value": result_provider.get("entry_point_value"),
            "provider_distribution": result_provider.get("distribution"),
            "provider_distribution_version": result_provider.get("distribution_version"),
            "adapter": result_provider.get("adapter"),
            "package_name": result_provider.get("package_name"),
            "package_version": result_provider.get("package_version"),
            "application_profile": result_provider.get("application_profile"),
        }
        if result_provider is not None
        else None
    )
    bridge_valid = (
        result_bridge is not None
        and accepted_metadata is not None
        and result_bridge.get("schema_version") == "clio-relay.mcp-package-progress-bridge.v1"
        and result_bridge.get("execution_validated") is True
        and isinstance(result_bridge.get("notification_count"), int)
        and not isinstance(result_bridge.get("notification_count"), bool)
        and cast(int, result_bridge["notification_count"]) >= 1
        and result_bridge.get("execution_id") == accepted_metadata.get("provider_execution_id")
        and result_bridge.get("pipeline_id") == pipeline_id
        and result_bridge.get("expected_server_artifact_digest") == expected_server_artifact_digest
        and result_bridge.get("observed_server_artifact_digest") == expected_server_artifact_digest
        and bridge_identity == {key: accepted_metadata.get(key) for key in _PROVIDER_IDENTITY_KEYS}
    )

    runtime_packages = runtime_metadata.get("packages") if runtime_metadata else None
    provider_package = accepted_metadata.get("package_name") if accepted_metadata else None
    runtime_package_bound = (
        isinstance(runtime_packages, list)
        and isinstance(provider_package, str)
        and any(
            isinstance(item, dict) and cast(JSON, item).get("name") == provider_package
            for item in cast(list[object], runtime_packages)
        )
    )
    runtime_bound = (
        runtime_metadata is not None
        and accepted_metadata is not None
        and runtime_metadata.get("execution_id") == accepted_metadata.get("provider_execution_id")
        and runtime_metadata.get("pipeline_id") == pipeline_id
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
        "progress_record_count": len(progress),
        "warming_progress_id": warming_record.get("progress_id") if warming_record else None,
        "accepted_progress_id": accepted_record.get("progress_id") if accepted_record else None,
        "notification_sequence": (
            accepted_metadata.get("provider_notification_sequence") if accepted_metadata else None
        ),
        "live_observation": live_observation or {},
        "live_observed_while_running": live_observed,
        "expected_pipeline_id": pipeline_id,
        "expected_server_artifact_digest": expected_server_artifact_digest,
        "progress_binding_valid": progress_binding_valid,
        "bridge_valid": bridge_valid,
        "runtime_bound": runtime_bound,
        "bridge": result_bridge or {},
        "provider": (
            {key: accepted_metadata.get(key) for key in _PROVIDER_IDENTITY_KEYS}
            if accepted_metadata
            else {}
        ),
    }
    if accepted_record is None or accepted_metadata is None:
        return evidence, passed, None
    provider_distribution = accepted_metadata.get("provider_distribution")
    resource = {
        "resource_id": (
            f"{accepted_metadata.get('provider_entry_point', 'provider')}:"
            f"{accepted_metadata.get('provider_execution_id', 'execution')}"
        ),
        "provider": (
            str(provider_distribution) if provider_distribution is not None else "unknown"
        ),
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


def _same_progress_replay(
    warming_record: JSON,
    warming_metadata: JSON,
    accepted_record: JSON,
    accepted_metadata: JSON,
) -> bool:
    """Return whether two durable records are the warming/final replay pair."""
    record_keys = ("label", "current", "total", "unit", "message", "source_event_seq")
    return all(
        warming_metadata.get(key) == accepted_metadata.get(key) for key in _PROGRESS_REPLAY_KEYS
    ) and all(warming_record.get(key) == accepted_record.get(key) for key in record_keys)


def _server_install_spec(server_args: list[object]) -> str | None:
    for index, argument in enumerate(server_args[:-1]):
        if argument == "--from" and isinstance(server_args[index + 1], str):
            return cast(str, server_args[index + 1])
    return None


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
        and observed_digest == CLIO_KIT_JARVIS_USER_CONTRACT_SHA256
    )
    return (
        {
            "remote_tool_names": sorted(by_name),
            "expected_tool_names": sorted(expected),
            "edit_operation_schema": operation or {},
            "spack_specs_schema": spack_specs or {},
            "expected_contract_sha256": CLIO_KIT_JARVIS_USER_CONTRACT_SHA256,
            "expected_clio_kit_version": CLIO_KIT_JARVIS_MCP_VERSION,
            "observed_contract_sha256": observed_digest,
            "contract_error": contract_error,
        },
        passed,
    )


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
    output_matches = actual_output == VIRTUAL_REMOTE_MCP_JOB_OUTPUT_SCHEMA
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

"""Validates a durable JARVIS MCP result against the relay job that produced it.

Extracted from ``jarvis_service_runtime.py`` (clio-relay file-size ratchet,
scripts/check_file_size.py): the source-job/MCP-result provenance checks and
ready-service selection/package-identity checks shared by both the full
binding resolution path (still facade-resident, since it also drives the
externally-patched collaborators -- ``read_artifact_bytes``,
``should_execute_on_cluster``, ``OwnedSessionApiClient``, ``run_remote_clio``
-- the queue-reading half of that flow depends on) and the standalone
agent-facing handoff derivation (``derive_jarvis_service_runtime_handoffs``)
below, which never touches the durable queue at all -- its caller has
already read and SHA-verified the MCP result document.
"""

from __future__ import annotations

from typing import cast

from clio_relay.jarvis_mcp import (
    jarvis_cd_lock_binding_expectation,
    jarvis_mcp_server_artifact_binding_verified,
)
from clio_relay.jarvis_service_runtime_models import (
    JARVIS_SERVICE_RUNTIME_SCHEMA_V2,
    JSON,
    ClioKitJarvisExecutionQuery,
    JarvisExecutionServiceRuntimes,
    JarvisServiceRuntime,
    JarvisServiceRuntimeHandoff,
    _canonical_sha256,
)
from clio_relay.models import (
    REGISTERED_JARVIS_USER_CONTRACT,
    ArtifactRef,
    JobKind,
    JobState,
    McpCallSpec,
    McpOperation,
    RelayJob,
)
from clio_relay.remote_mcp import remote_mcp_server_artifact_binding_verified
from clio_relay.runtime_metadata import JarvisNativeExecutionDocuments, native_execution_documents


def _json_object(value: object, label: str) -> JSON:
    if not isinstance(value, dict):
        raise ValueError(f"{label} did not return a JSON object")
    return cast(JSON, value)


def _validate_source_job(job: RelayJob, *, cluster: str) -> McpCallSpec:
    if job.cluster != cluster:
        raise ValueError("JARVIS service source job belongs to a different cluster")
    if job.state is not JobState.SUCCEEDED:
        raise ValueError("JARVIS service source job must have completed successfully")
    if job.kind is not JobKind.MCP_CALL or not isinstance(job.spec, McpCallSpec):
        raise ValueError("JARVIS service source job is not an MCP call")
    if job.spec.operation is not McpOperation.TOOLS_CALL or job.spec.tool != "jarvis_get_execution":
        raise ValueError("JARVIS service source must be jarvis_get_execution")
    if job.spec.arguments.get("include_service_runtimes") is not True:
        raise ValueError(
            "jarvis_get_execution service source must set include_service_runtimes=true"
        )
    if job.spec.expected_registered_contract is not None:
        if job.spec.expected_registered_contract != REGISTERED_JARVIS_USER_CONTRACT:
            raise ValueError(
                "registered JARVIS service source does not use the supported JARVIS contract"
            )
        if job.spec.expected_jarvis_cd_lock_binding is not None:
            raise ValueError(
                "registered JARVIS service source also supplied a built-in JARVIS-CD lock pin"
            )
    else:
        server_name = job.spec.server.replace("\\", "/").rsplit("/", maxsplit=1)[-1].casefold()
        if server_name not in {"clio-kit", "clio-kit.exe"} or job.spec.server_args != [
            "mcp-server",
            "jarvis",
        ]:
            raise ValueError(
                "JARVIS service source does not use the configured clio-kit JARVIS MCP"
            )
        if job.spec.expected_jarvis_cd_lock_binding != jarvis_cd_lock_binding_expectation():
            raise ValueError("JARVIS service source did not enforce the relay JARVIS-CD lock pin")
    if job.spec.expected_server_artifact_digest is None:
        raise ValueError("JARVIS service source is not bound to a discovered server artifact")
    return job.spec


def _validate_mcp_result(
    document: JSON,
    *,
    job: RelayJob,
    spec: McpCallSpec,
) -> ClioKitJarvisExecutionQuery:
    if document.get("server") != spec.server or document.get("server_args") != spec.server_args:
        raise ValueError("JARVIS MCP result command did not match its durable relay job")
    if (
        document.get("operation") != McpOperation.TOOLS_CALL.value
        or document.get("tool") != spec.tool
        or document.get("arguments") != spec.arguments
        or document.get("env_from") != spec.env_from
    ):
        raise ValueError("JARVIS MCP result route did not match its durable relay job")
    if document.get("expected_registered_contract") != spec.expected_registered_contract:
        raise ValueError("JARVIS MCP result registered contract did not match")
    if document.get("expected_jarvis_cd_lock_binding") != spec.expected_jarvis_cd_lock_binding:
        raise ValueError("JARVIS MCP result JARVIS-CD lock pin did not match")
    if (
        document.get("expected_server_artifact_digest") != spec.expected_server_artifact_digest
        or document.get("observed_server_artifact_digest") != spec.expected_server_artifact_digest
    ):
        raise ValueError("JARVIS MCP result server artifact binding did not match")
    if spec.expected_registered_contract is not None:
        artifact_verified = remote_mcp_server_artifact_binding_verified(
            document.get("server_artifact"),
            expected_digest=spec.expected_server_artifact_digest,
        )
        artifact_failure = (
            "JARVIS MCP result server artifact identity is not the immutable registered route"
        )
    else:
        artifact_verified = jarvis_mcp_server_artifact_binding_verified(
            document.get("server_artifact"),
            expected_digest=spec.expected_server_artifact_digest,
        )
        artifact_failure = "JARVIS MCP result server artifact identity is not the exact release pin"
    if not artifact_verified:
        raise ValueError(artifact_failure)
    if (
        document.get("returncode") != 0
        or document.get("timed_out") is True
        or document.get("protocol_error") is not None
    ):
        raise ValueError("JARVIS MCP source call did not complete successfully")
    protocol = document.get("protocol_result")
    if not isinstance(protocol, dict):
        raise ValueError("JARVIS MCP source omitted its protocol result")
    typed_protocol = cast(JSON, protocol)
    if typed_protocol.get("isError") is True:
        raise ValueError("JARVIS MCP source tool returned isError")
    structured = document.get("structured_result")
    if not isinstance(structured, dict):
        raise ValueError("JARVIS MCP source omitted structuredContent")
    typed_structured = cast(JSON, structured)
    protocol_structured = typed_protocol.get("structuredContent")
    if protocol_structured != typed_structured:
        raise ValueError("JARVIS MCP persisted structured results disagreed")
    expected_schema = "clio-kit.jarvis-execution.v2"
    if typed_structured.get("schema_version") != expected_schema:
        raise ValueError(f"JARVIS MCP source schema must be {expected_schema} for {spec.tool}")
    return ClioKitJarvisExecutionQuery.model_validate(typed_structured)


def _validate_snapshot_execution(
    snapshot: JarvisExecutionServiceRuntimes,
    *,
    native: JarvisNativeExecutionDocuments,
) -> None:
    record = native.execution_record
    if (
        snapshot.execution_id != record.execution_id
        or snapshot.pipeline_id != record.pipeline_id
        or snapshot.execution_state != record.state
        or snapshot.terminal is not record.terminal
    ):
        raise ValueError("JARVIS service snapshot did not match native execution lifecycle")


def _select_ready_runtime(
    snapshot: JarvisExecutionServiceRuntimes,
    *,
    package_id: str,
    package_name: str,
    service_instance_id: str | None = None,
) -> JarvisServiceRuntime:
    matches = [
        runtime
        for runtime in snapshot.service_runtimes
        if runtime.package_id == package_id
        and runtime.package_name == package_name
        and (service_instance_id is None or runtime.service_instance_id == service_instance_id)
    ]
    if len(matches) != 1:
        raise ValueError(
            "JARVIS service package selector must resolve exactly one service instance"
        )
    runtime = matches[0]
    if runtime.lifecycle != "ready":
        raise ValueError("JARVIS service runtime must be ready before relay binding")
    return runtime


def _validate_runtime_package(
    native: JarvisNativeExecutionDocuments,
    *,
    runtime: JarvisServiceRuntime,
) -> None:
    packages = [
        package
        for package in native.progress.packages
        if package.package_id == runtime.package_id and package.package_name == runtime.package_name
    ]
    if len(packages) != 1:
        raise ValueError("JARVIS service package did not match native execution progress")


def derive_jarvis_service_runtime_handoffs(
    *,
    cluster: str,
    source_job: RelayJob,
    source_artifact: ArtifactRef,
    document: JSON,
) -> list[JarvisServiceRuntimeHandoff]:
    """Derive ready-service selectors from one SHA-verified durable MCP artifact.

    The caller verifies the artifact envelope and payload digest before passing
    the decoded document. The same route, release, execution, and package checks
    used by the eventual bind operation are then applied here.
    """
    if source_artifact.job_id != source_job.job_id or source_artifact.kind != "mcp_result":
        raise ValueError("JARVIS service handoff artifact identity did not match its source job")
    if source_artifact.sha256 is None:
        raise ValueError("JARVIS service handoff artifact has no durable SHA-256")
    _canonical_sha256(source_artifact.sha256, "handoff artifact digest")
    spec = _validate_source_job(source_job, cluster=cluster)
    query = _validate_mcp_result(document, job=source_job, spec=spec)
    native = native_execution_documents(query.model_dump(mode="json"))
    if native is None:
        raise ValueError("JARVIS service runtime result omitted native execution documents")
    snapshot = query.service_runtimes
    _validate_snapshot_execution(snapshot, native=native)
    handoffs: list[JarvisServiceRuntimeHandoff] = []
    for runtime in snapshot.service_runtimes:
        _validate_runtime_package(native, runtime=runtime)
        if (
            runtime.lifecycle != "ready"
            or runtime.schema_version != JARVIS_SERVICE_RUNTIME_SCHEMA_V2
        ):
            continue
        handoffs.append(
            JarvisServiceRuntimeHandoff(
                cluster=cluster,
                source_job_id=source_job.job_id,
                source_artifact_id=source_artifact.artifact_id,
                package_id=runtime.package_id,
                package_name=runtime.package_name,
                service_instance_id=runtime.service_instance_id,
            )
        )
    return handoffs

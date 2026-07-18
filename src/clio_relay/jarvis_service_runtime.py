"""Verified binding from durable JARVIS MCP results to service runtimes."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import math
from pathlib import PurePosixPath
from typing import Any, Literal, cast

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from clio_relay.cluster_config import ClusterDefinition
from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.identifiers import DurableRecordId
from clio_relay.jarvis_mcp import (
    jarvis_cd_lock_binding_expectation,
    jarvis_mcp_server_artifact_binding_verified,
)
from clio_relay.models import (
    ArtifactRef,
    JobKind,
    JobState,
    McpCallSpec,
    McpOperation,
    RelayJob,
)
from clio_relay.relay_ops import read_artifact_bytes
from clio_relay.remote_cli import run_remote_clio, should_execute_on_cluster
from clio_relay.runtime_metadata import JarvisNativeExecutionDocuments, native_execution_documents
from clio_relay.session_api import OwnedSessionApiClient

JSON = dict[str, Any]
JARVIS_SERVICE_RUNTIME_SCHEMA = "jarvis.service-runtime.v1"
JARVIS_SERVICE_RUNTIME_SNAPSHOT_SCHEMA = "jarvis.execution.service-runtimes.v1"
JARVIS_DATASET_DESCRIPTOR_SCHEMA = "jarvis.dataset-descriptor.v1"
RELAY_JARVIS_RUNTIME_BINDING_SCHEMA = "clio-relay.jarvis-service-runtime-binding.v1"
_HEX_DIGITS = frozenset("0123456789abcdef")


class JarvisArtifactIdentity(BaseModel):
    """Optional exact JARVIS artifact identity attached to a dataset."""

    model_config = ConfigDict(extra="forbid", strict=True)

    artifact_id: str = Field(min_length=1, max_length=512)
    sha256: str

    @field_validator("sha256")
    @classmethod
    def validate_sha256(cls, value: str) -> str:
        """Require a canonical SHA-256 digest."""
        return _canonical_sha256(value, "source_artifact.sha256")


class JarvisDatasetFingerprint(BaseModel):
    """Content identity for a JARVIS dataset descriptor."""

    model_config = ConfigDict(extra="forbid", strict=True)

    algorithm: Literal["sha256"]
    digest: str

    @field_validator("digest")
    @classmethod
    def validate_digest(cls, value: str) -> str:
        """Require the dataset fingerprint to be a canonical SHA-256."""
        return _canonical_sha256(value, "fingerprint.digest")


class JarvisDatasetMember(BaseModel):
    """One ordered member of a dataset collection."""

    model_config = ConfigDict(extra="forbid", strict=True)

    index: int = Field(ge=0)
    location: str
    timestep: float | int | None = Field(default=None, exclude_if=lambda value: value is None)

    @model_validator(mode="after")
    def validate_member(self) -> JarvisDatasetMember:
        """Require one normalized absolute location and a finite optional timestep."""
        _validate_cluster_path(self.location)
        if self.timestep is not None and not math.isfinite(float(self.timestep)):
            raise ValueError("dataset member timestep must be finite")
        return self


class JarvisDatasetArray(BaseModel):
    """Array metadata advertised by a dataset descriptor."""

    model_config = ConfigDict(extra="forbid", strict=True)

    name: str
    association: Literal["point", "cell", "field"]
    components: int = Field(ge=1, le=64)
    units: str | None = Field(default=None, exclude_if=lambda value: value is None)

    @model_validator(mode="after")
    def validate_array(self) -> JarvisDatasetArray:
        """Require printable bounded array labels."""
        _validate_printable_utf8(self.name, "dataset array name", maximum=512)
        if self.units is not None:
            _validate_printable_utf8(self.units, "dataset array units", maximum=256)
        return self


class JarvisDatasetDescriptor(BaseModel):
    """Strict, transport-neutral dataset identity reported by JARVIS."""

    model_config = ConfigDict(extra="forbid", strict=True)

    schema_version: Literal["jarvis.dataset-descriptor.v1"]
    dataset_id: str = Field(min_length=1, max_length=512)
    kind: str = Field(min_length=1, max_length=256)
    format: str = Field(min_length=1, max_length=256)
    members: list[JarvisDatasetMember] = Field(min_length=1, max_length=512)
    arrays: list[JarvisDatasetArray] = Field(max_length=256)
    bounds: list[float | int] | None
    fingerprint: JarvisDatasetFingerprint
    source_artifact: JarvisArtifactIdentity | None

    @model_validator(mode="after")
    def validate_descriptor(self) -> JarvisDatasetDescriptor:
        """Require canonical members, arrays, bounds, and content fingerprint."""
        if [member.index for member in self.members] != list(range(len(self.members))):
            raise ValueError("dataset member indexes must be contiguous and ordered")
        locations = [member.location for member in self.members]
        if len(locations) != len(set(locations)):
            raise ValueError("dataset member locations must be unique")
        array_keys = [(array.association, array.name) for array in self.arrays]
        if len(array_keys) != len(set(array_keys)):
            raise ValueError("dataset arrays repeated an association/name identity")
        if self.bounds is not None:
            if len(self.bounds) != 6 or any(
                not math.isfinite(float(value)) for value in self.bounds
            ):
                raise ValueError("dataset bounds must contain exactly six finite numbers")
            if any(
                float(self.bounds[index]) > float(self.bounds[index + 1]) for index in (0, 2, 4)
            ):
                raise ValueError("dataset bounds minimum exceeded its paired maximum")
        payload = self.model_dump(mode="json")
        payload.pop("fingerprint")
        observed = _canonical_json_sha256(payload)
        if not hmac.compare_digest(observed, self.fingerprint.digest):
            raise ValueError("dataset descriptor fingerprint did not match canonical content")
        return self


class JarvisServiceRuntime(BaseModel):
    """Latest exact service report for one JARVIS package instance."""

    model_config = ConfigDict(extra="forbid", strict=True)

    schema_version: Literal["jarvis.service-runtime.v1"]
    execution_id: str = Field(min_length=1, max_length=512)
    package_name: str = Field(min_length=1, max_length=256)
    package_id: str = Field(min_length=1, max_length=256)
    service_instance_id: str = Field(min_length=1, max_length=512)
    revision: int = Field(ge=1)
    lifecycle: Literal["starting", "ready", "degraded", "stopping", "stopped", "failed"]
    host: str = Field(min_length=1, max_length=1_024)
    port: int = Field(gt=0, le=65_535)
    protocol: Literal["http", "https"]
    health_path: str
    live_data_path: str
    events_path: str
    state_path: str
    command_path: str
    delivery_mode: Literal["push"]
    dataset_descriptor: JarvisDatasetDescriptor
    message: str | None = Field(default=None, max_length=16_384)
    observed_at_epoch: float = Field(ge=0)

    @field_validator("host")
    @classmethod
    def validate_host(cls, value: str) -> str:
        """Reject host strings that cannot safely identify one connector target."""
        if (
            value != value.strip()
            or value.startswith("-")
            or any(
                character.isspace() or ord(character) < 32 or ord(character) == 127
                for character in value
            )
            or any(character in value for character in "/\\?#@")
        ):
            raise ValueError("service runtime host is invalid")
        return value

    @field_validator(
        "health_path",
        "live_data_path",
        "events_path",
        "state_path",
        "command_path",
    )
    @classmethod
    def validate_http_path(cls, value: str) -> str:
        """Require one normalized absolute HTTP path without query or fragment data."""
        if (
            not value.startswith("/")
            or len(value) > 2_048
            or "\\" in value
            or "?" in value
            or "#" in value
            or "//" in value
            or str(PurePosixPath(value)) != value
        ):
            raise ValueError("service runtime paths must be normalized absolute HTTP paths")
        return value

    @field_validator("observed_at_epoch")
    @classmethod
    def validate_observed_at_epoch(cls, value: float) -> float:
        """Require JARVIS's exact finite, nonnegative epoch observation."""
        if not math.isfinite(value):
            raise ValueError("service runtime observed_at_epoch must be finite")
        return value


class JarvisExecutionServiceRuntimes(BaseModel):
    """Strict execution-scoped snapshot returned by JARVIS."""

    model_config = ConfigDict(extra="forbid", strict=True)

    schema_version: Literal["jarvis.execution.service-runtimes.v1"]
    execution_id: str = Field(min_length=1, max_length=512)
    pipeline_id: str = Field(min_length=1, max_length=512)
    execution_state: str = Field(min_length=1, max_length=64)
    terminal: bool
    service_runtimes: list[JarvisServiceRuntime] = Field(max_length=4_096)

    @model_validator(mode="after")
    def validate_runtime_identities(self) -> JarvisExecutionServiceRuntimes:
        """Require one latest report per service instance and stable execution identity."""
        instances: set[str] = set()
        expected_order: list[tuple[str, str]] = []
        for runtime in self.service_runtimes:
            if runtime.execution_id != self.execution_id:
                raise ValueError("service runtime execution identity did not match snapshot")
            if runtime.service_instance_id in instances:
                raise ValueError("service runtime snapshot repeated a service_instance_id")
            instances.add(runtime.service_instance_id)
            expected_order.append((runtime.package_id, runtime.service_instance_id))
        if expected_order != sorted(expected_order):
            raise ValueError("service runtime snapshot order is not canonical")
        return self


class ClioKitJarvisExecutionQuery(BaseModel):
    """Exact clio-kit execution-v2 view required for service binding."""

    model_config = ConfigDict(extra="forbid", strict=True)

    schema_version: Literal["clio-kit.jarvis-execution.v2"]
    pipeline_id: str = Field(min_length=1, max_length=256)
    execution_id: str = Field(min_length=1, max_length=256)
    execution_handle: JSON
    execution_record: JSON
    runtime_metadata: JSON
    progress: JSON
    artifact_page: JSON | None
    service_runtimes: JarvisExecutionServiceRuntimes

    @model_validator(mode="after")
    def validate_execution_identity(self) -> ClioKitJarvisExecutionQuery:
        """Bind the requested execution identity to its service snapshot."""
        if (
            self.execution_id != self.service_runtimes.execution_id
            or self.pipeline_id != self.service_runtimes.pipeline_id
        ):
            raise ValueError("clio-kit execution query identity did not match service snapshot")
        return self


class JarvisServiceRuntimeBinding(BaseModel):
    """Immutable provenance persisted by clio-relay for a bound service."""

    model_config = ConfigDict(extra="forbid", strict=True)

    schema_version: Literal["clio-relay.jarvis-service-runtime-binding.v1"] = (
        RELAY_JARVIS_RUNTIME_BINDING_SCHEMA
    )
    source_relay_job_id: str = Field(min_length=1, max_length=512)
    source_relay_artifact_id: str = Field(min_length=1, max_length=512)
    source_relay_artifact_sha256: str
    source_tool: Literal["jarvis_get_execution"]
    jarvis_execution_id: str = Field(min_length=1, max_length=512)
    scheduler_provider: str | None = Field(default=None, max_length=256)
    scheduler_native_id: str | None = Field(default=None, max_length=256)
    package_id: str = Field(min_length=1, max_length=256)
    package_name: str = Field(min_length=1, max_length=256)
    service_instance_id: str = Field(min_length=1, max_length=512)
    service_revision: int = Field(ge=1)
    service_report_sha256: str
    dataset_descriptor_sha256: str
    dataset_descriptor: JarvisDatasetDescriptor

    @field_validator(
        "source_relay_artifact_sha256",
        "service_report_sha256",
        "dataset_descriptor_sha256",
    )
    @classmethod
    def validate_digests(cls, value: str) -> str:
        """Require canonical SHA-256 values for every persisted evidence digest."""
        return _canonical_sha256(value, "binding digest")


class JarvisServiceRuntimeHandoff(BaseModel):
    """Agent-facing selectors copied unchanged into a relay runtime bind call."""

    model_config = ConfigDict(extra="forbid", strict=True)

    cluster: str = Field(min_length=1, max_length=256)
    source_job_id: DurableRecordId
    source_artifact_id: DurableRecordId
    package_id: str = Field(min_length=1, max_length=256)
    package_name: str = Field(min_length=1, max_length=256)
    service_instance_id: str = Field(min_length=1, max_length=512)


class VerifiedJarvisServiceRuntime(BaseModel):
    """Validated runtime and its immutable relay provenance."""

    model_config = ConfigDict(extra="forbid", strict=True, arbitrary_types_allowed=True)

    binding: JarvisServiceRuntimeBinding
    runtime: JarvisServiceRuntime
    native_execution: JarvisNativeExecutionDocuments


def resolve_jarvis_service_runtime(
    *,
    queue: ClioCoreQueue,
    definition: ClusterDefinition,
    settings: RelaySettings | None = None,
    source_job_id: str,
    source_artifact_id: str,
    package_id: str,
    package_name: str,
    service_instance_id: str | None = None,
) -> VerifiedJarvisServiceRuntime:
    """Resolve one ready service solely from a verified durable JARVIS MCP result."""
    job, artifact, document = _load_source(
        queue=queue,
        definition=definition,
        settings=settings,
        source_job_id=source_job_id,
        source_artifact_id=source_artifact_id,
    )
    spec = _validate_source_job(job, cluster=definition.name)
    query = _validate_mcp_result(document, job=job, spec=spec)
    native = native_execution_documents(query.model_dump(mode="json"))
    if native is None:
        raise ValueError("JARVIS service runtime result omitted native execution documents")
    snapshot = query.service_runtimes
    _validate_snapshot_execution(snapshot, native=native)
    runtime = _select_ready_runtime(
        snapshot,
        package_id=package_id,
        package_name=package_name,
        service_instance_id=service_instance_id,
    )
    _validate_runtime_package(native, runtime=runtime)
    scheduler_provider = native.execution_handle.scheduler_provider
    scheduler_native_id = native.execution_handle.scheduler_native_id
    if native.execution_handle.mode == "scheduler":
        if scheduler_native_id is None:
            raise ValueError("ready scheduler service has no scheduler-native identity")
        if scheduler_provider != definition.scheduler_provider:
            raise ValueError(
                "JARVIS scheduler provider does not match the configured cluster provider"
            )
    descriptor_payload = runtime.dataset_descriptor.model_dump(mode="json")
    runtime_payload = runtime.model_dump(mode="json")
    binding = JarvisServiceRuntimeBinding(
        source_relay_job_id=job.job_id,
        source_relay_artifact_id=artifact.artifact_id,
        source_relay_artifact_sha256=cast(str, artifact.sha256),
        source_tool=cast(Literal["jarvis_get_execution"], spec.tool),
        jarvis_execution_id=native.execution_handle.execution_id,
        scheduler_provider=scheduler_provider,
        scheduler_native_id=scheduler_native_id,
        package_id=runtime.package_id,
        package_name=runtime.package_name,
        service_instance_id=runtime.service_instance_id,
        service_revision=runtime.revision,
        service_report_sha256=_canonical_json_sha256(runtime_payload),
        dataset_descriptor_sha256=_canonical_json_sha256(descriptor_payload),
        dataset_descriptor=runtime.dataset_descriptor,
    )
    return VerifiedJarvisServiceRuntime(binding=binding, runtime=runtime, native_execution=native)


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
        if runtime.lifecycle != "ready":
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


def reverify_jarvis_service_runtime(
    *,
    queue: ClioCoreQueue,
    definition: ClusterDefinition,
    settings: RelaySettings | None = None,
    binding_document: object,
) -> VerifiedJarvisServiceRuntime:
    """Re-read an exact source artifact and require its persisted binding to remain unchanged."""
    expected = JarvisServiceRuntimeBinding.model_validate(binding_document)
    observed = resolve_jarvis_service_runtime(
        queue=queue,
        definition=definition,
        settings=settings,
        source_job_id=expected.source_relay_job_id,
        source_artifact_id=expected.source_relay_artifact_id,
        package_id=expected.package_id,
        package_name=expected.package_name,
        service_instance_id=expected.service_instance_id,
    )
    if not hmac.compare_digest(
        _canonical_json_bytes(observed.binding.model_dump(mode="json")),
        _canonical_json_bytes(expected.model_dump(mode="json")),
    ):
        raise ValueError("bound JARVIS service runtime no longer matches its durable source")
    return observed


def _load_source(
    *,
    queue: ClioCoreQueue,
    definition: ClusterDefinition,
    settings: RelaySettings | None,
    source_job_id: str,
    source_artifact_id: str,
) -> tuple[RelayJob, ArtifactRef, JSON]:
    # The source receipt belongs to its exact owner-session generation, regardless of
    # where the current operation executes.  In particular, browser attachment is a
    # desktop-local operation, but it must re-verify the remote JARVIS receipt through
    # the authenticated owner-session API rather than looking for that job in the
    # desktop queue.  CLI locality controls command placement, not provenance storage.
    if settings is not None and settings.owner_session_id is not None:
        with OwnedSessionApiClient(definition=definition, settings=settings) as client:
            status = _json_object(
                client.request_json(
                    method="GET",
                    path=f"/jobs/{source_job_id}/status",
                ),
                "JARVIS service source job",
            )
            envelope = _json_object(
                client.request_json(
                    method="GET",
                    path=f"/artifacts/{source_artifact_id}/content",
                ),
                "JARVIS service source artifact",
            )
        raw_job = status.get("job")
    elif should_execute_on_cluster(definition):
        status = _remote_json(
            definition,
            ["job", "status", source_job_id],
            "JARVIS service source job",
        )
        raw_job = status.get("job")
        envelope = _remote_json(
            definition,
            ["job", "read-artifact", source_artifact_id],
            "JARVIS service source artifact",
        )
    else:
        raw_job = queue.get_job(source_job_id).model_dump(mode="json")
        envelope = cast(JSON, read_artifact_bytes(queue, source_artifact_id))
    job = RelayJob.model_validate(raw_job)
    if job.job_id != source_job_id:
        raise ValueError("JARVIS service source returned a different relay job")
    raw_artifact = envelope.get("artifact")
    artifact = ArtifactRef.model_validate(raw_artifact)
    if (
        artifact.artifact_id != source_artifact_id
        or artifact.job_id != source_job_id
        or artifact.kind != "mcp_result"
    ):
        raise ValueError("JARVIS service source artifact identity did not match the request")
    if artifact.sha256 is None:
        raise ValueError("JARVIS service source artifact has no durable SHA-256")
    encoded = envelope.get("data")
    if envelope.get("encoding") != "base64" or not isinstance(encoded, str):
        raise ValueError("JARVIS service source artifact is not a base64 envelope")
    try:
        payload = base64.b64decode(encoded, validate=True)
    except ValueError as exc:
        raise ValueError("JARVIS service source artifact contains invalid base64") from exc
    digest = hashlib.sha256(payload).hexdigest()
    if not hmac.compare_digest(digest, artifact.sha256):
        raise ValueError("JARVIS service source artifact digest did not match durable metadata")
    try:
        document = json.loads(payload.decode("utf-8-sig"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("JARVIS service source artifact must contain UTF-8 JSON") from exc
    if not isinstance(document, dict):
        raise ValueError("JARVIS service source artifact must contain a JSON object")
    return job, artifact, cast(JSON, document)


def _validate_source_job(job: RelayJob, *, cluster: str) -> McpCallSpec:
    if job.cluster != cluster:
        raise ValueError("JARVIS service source job belongs to a different cluster")
    if job.state is not JobState.SUCCEEDED:
        raise ValueError("JARVIS service source job must have completed successfully")
    if job.kind is not JobKind.MCP_CALL or not isinstance(job.spec, McpCallSpec):
        raise ValueError("JARVIS service source job is not an MCP call")
    if job.spec.operation is not McpOperation.TOOLS_CALL or job.spec.tool != "jarvis_get_execution":
        raise ValueError("JARVIS service source must be jarvis_get_execution")
    server_name = job.spec.server.replace("\\", "/").rsplit("/", maxsplit=1)[-1].casefold()
    if server_name not in {"clio-kit", "clio-kit.exe"} or job.spec.server_args != [
        "mcp-server",
        "jarvis",
    ]:
        raise ValueError("JARVIS service source does not use the configured clio-kit JARVIS MCP")
    if job.spec.arguments.get("include_service_runtimes") is not True:
        raise ValueError(
            "jarvis_get_execution service source must set include_service_runtimes=true"
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
    if document.get("expected_jarvis_cd_lock_binding") != spec.expected_jarvis_cd_lock_binding:
        raise ValueError("JARVIS MCP result JARVIS-CD lock pin did not match")
    if (
        document.get("expected_server_artifact_digest") != spec.expected_server_artifact_digest
        or document.get("observed_server_artifact_digest") != spec.expected_server_artifact_digest
    ):
        raise ValueError("JARVIS MCP result server artifact binding did not match")
    if not jarvis_mcp_server_artifact_binding_verified(
        document.get("server_artifact"),
        expected_digest=spec.expected_server_artifact_digest,
    ):
        raise ValueError("JARVIS MCP result server artifact identity is not the exact release pin")
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


def _remote_json(
    definition: ClusterDefinition,
    arguments: list[str],
    label: str,
) -> JSON:
    output = run_remote_clio(definition, arguments)
    try:
        value = json.loads(output)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{label} returned invalid JSON") from exc
    if not isinstance(value, dict):
        raise ValueError(f"{label} did not return a JSON object")
    return cast(JSON, value)


def _json_object(value: object, label: str) -> JSON:
    if not isinstance(value, dict):
        raise ValueError(f"{label} did not return a JSON object")
    return cast(JSON, value)


def _validate_cluster_path(value: str) -> None:
    path = PurePosixPath(value)
    if (
        not value.startswith("/")
        or str(path) != value
        or ".." in path.parts
        or any(ord(character) < 32 or ord(character) == 127 for character in value)
    ):
        raise ValueError("dataset members must use normalized absolute cluster paths")


def _validate_printable_utf8(value: str, label: str, *, maximum: int) -> None:
    if (
        not value
        or len(value.encode("utf-8")) > maximum
        or any(ord(character) < 32 or ord(character) == 127 for character in value)
    ):
        raise ValueError(f"{label} must be non-empty printable UTF-8 within {maximum} bytes")


def _canonical_sha256(value: str, label: str) -> str:
    if len(value) != 64 or any(character not in _HEX_DIGITS for character in value):
        raise ValueError(f"{label} must be a canonical SHA-256")
    return value


def _canonical_json_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def _canonical_json_sha256(value: object) -> str:
    return hashlib.sha256(_canonical_json_bytes(value)).hexdigest()

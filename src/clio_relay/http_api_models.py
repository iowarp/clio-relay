"""HTTP request models for the ``http_api`` FastAPI routes.

split/http-api-w3 (iowarp/clio-relay#231): moved verbatim out of
``http_api.py``. None of these models or their validators referenced any of
``create_app()``'s local closures, so this is an unmodified, atomic move.
``http_api.py`` re-exports every model class under its original name so
external imports (e.g. ``from clio_relay.http_api import
JarvisMcpCallSubmitRequest``, used by ``tests/test_jarvis_handle_first_
admission.py``) keep resolving it.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import secrets
from datetime import datetime
from typing import Literal, cast

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from clio_relay.config import MAX_INPUT_FILE_MAX_BYTES
from clio_relay.identifiers import DurableRecordId
from clio_relay.jarvis_service_runtime import JarvisServiceRuntimeBinding
from clio_relay.models import (
    REGISTERED_JARVIS_USER_CONTRACT,
    ArtifactUse,
    GatewaySessionState,
    JarvisRunInputManifest,
    McpControlQueryEvidence,
    McpOperation,
    TaskEventStatus,
    validate_mcp_env_from,
)


def _empty_artifact_uses() -> list[ArtifactUse]:
    """Return a typed empty artifact dependency collection."""
    return []


class JarvisSubmitRequest(BaseModel):
    """HTTP request to submit a JARVIS pipeline YAML document."""

    model_config = ConfigDict(extra="forbid")

    cluster: str
    pipeline_yaml: str
    idempotency_key: str
    used_artifact_refs: list[ArtifactUse] = Field(
        default_factory=_empty_artifact_uses,
        max_length=1_000,
    )


MAX_INPUT_ARTIFACT_BASE64_CHARS = 4 * ((MAX_INPUT_FILE_MAX_BYTES + 2) // 3)


class InputArtifactIngestRequest(BaseModel):
    """Private owned-session request for one bounded regular-file input."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["clio-relay.input-artifact-ingest.v1"] = (
        "clio-relay.input-artifact-ingest.v1"
    )
    cluster: str = Field(min_length=1, max_length=256)
    logical_name: str = Field(min_length=1, max_length=255)
    size_bytes: int = Field(ge=0)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    data_base64: str = Field(max_length=MAX_INPUT_ARTIFACT_BASE64_CHARS)
    idempotency_key: str = Field(min_length=1, max_length=1_024)


def _decode_input_artifact_payload(
    request: InputArtifactIngestRequest,
    *,
    max_bytes: int,
) -> bytes:
    """Decode one canonical base64 payload without crossing the configured cap."""
    if request.size_bytes > max_bytes:
        raise ValueError(f"input artifact exceeds the {max_bytes}-byte per-file limit")
    expected_encoded_bytes = 4 * ((request.size_bytes + 2) // 3)
    if len(request.data_base64) != expected_encoded_bytes:
        raise ValueError("input artifact base64 length does not match its declared size")
    try:
        payload = base64.b64decode(request.data_base64, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise ValueError("input artifact data_base64 is not canonical base64") from exc
    if len(payload) != request.size_bytes:
        raise ValueError("input artifact decoded size does not match its declaration")
    digest = hashlib.sha256(payload).hexdigest()
    if not secrets.compare_digest(digest, request.sha256):
        raise ValueError("input artifact SHA-256 does not match its payload")
    return payload


class JarvisPipelineSubmitRequest(BaseModel):
    """HTTP request to submit an existing JARVIS pipeline by name."""

    model_config = ConfigDict(extra="forbid")

    cluster: str
    pipeline_name: str
    idempotency_key: str
    used_artifact_refs: list[ArtifactUse] = Field(
        default_factory=_empty_artifact_uses,
        max_length=1_000,
    )


class RemoteAgentSubmitRequest(BaseModel):
    """HTTP request to submit a remote-agent task."""

    model_config = ConfigDict(extra="forbid")

    cluster: str
    prompt_path: str
    mcp_config_path: str | None = None
    model: str | None = None
    workdir: str | None = None
    timeout_seconds: int | None = Field(default=None, gt=0)
    idempotency_key: str
    used_artifact_refs: list[ArtifactUse] = Field(
        default_factory=_empty_artifact_uses,
        max_length=1_000,
    )


class McpCallSubmitRequest(BaseModel):
    """HTTP request to submit a remote MCP tool call."""

    model_config = ConfigDict(extra="forbid")

    cluster: str
    server: str
    server_args: list[str] = Field(default_factory=list)
    env_from: dict[str, str] = Field(default_factory=dict)
    expected_server_artifact_digest: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{64}$",
    )
    expected_registered_contract: str | None = Field(default=None, min_length=1, max_length=256)
    operation: McpOperation = McpOperation.TOOLS_CALL
    tool: str | None = None
    arguments: dict[str, object] = Field(default_factory=dict)
    jarvis_input_manifest: JarvisRunInputManifest | None = None
    control_query_evidence: McpControlQueryEvidence | None = None
    timeout_seconds: int | None = Field(default=None, gt=0)
    idempotency_key: str
    used_artifact_refs: list[ArtifactUse] = Field(
        default_factory=_empty_artifact_uses,
        max_length=1_000,
    )

    @field_validator("env_from")
    @classmethod
    def validate_environment_references(cls, value: dict[str, str]) -> dict[str, str]:
        """Reject invalid names and relay-owned credential references."""
        return validate_mcp_env_from(value)

    @model_validator(mode="after")
    def validate_operation_contract(self) -> McpCallSubmitRequest:
        """Keep call and discovery payloads unambiguous before admission."""
        if self.operation is McpOperation.TOOLS_CALL:
            if not self.tool:
                raise ValueError("tool is required for tools/call")
            if self.jarvis_input_manifest is not None and (
                self.tool != "jarvis_run"
                or self.expected_registered_contract != REGISTERED_JARVIS_USER_CONTRACT
                or self.arguments.get("pipeline_id") != self.jarvis_input_manifest.route.pipeline_id
            ):
                raise ValueError(
                    "JARVIS input manifests require the exact registered jarvis_run pipeline"
                )
            return self
        if self.tool is not None:
            raise ValueError("tool must be omitted for tools/list")
        if self.arguments:
            raise ValueError("arguments must be empty for tools/list")
        if self.expected_server_artifact_digest is not None:
            raise ValueError("tools/list must not carry an expected server artifact digest")
        if self.expected_registered_contract is not None:
            raise ValueError("tools/list must not carry a registered semantic contract binding")
        if self.control_query_evidence is not None:
            raise ValueError("tools/list must not carry control-query route evidence")
        if self.jarvis_input_manifest is not None:
            raise ValueError("tools/list must not carry a JARVIS input manifest")
        return self


class JarvisMcpCallSubmitRequest(BaseModel):
    """HTTP request to submit a remote JARVIS MCP tool call."""

    model_config = ConfigDict(extra="forbid")

    cluster: str
    operation: McpOperation = McpOperation.TOOLS_CALL
    tool: str | None = None
    arguments: dict[str, object] = Field(default_factory=dict)
    expected_server_artifact_digest: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{64}$",
    )
    timeout_seconds: int | None = Field(default=None, gt=0)
    idempotency_key: str
    used_artifact_refs: list[ArtifactUse] = Field(
        default_factory=_empty_artifact_uses,
        max_length=1_000,
    )

    @model_validator(mode="after")
    def reject_internal_jarvis_run_wait(self) -> JarvisMcpCallSubmitRequest:
        """Keep workload waiting out of the trusted handle-first HTTP ingress."""
        if self.operation is McpOperation.TOOLS_CALL and not self.tool:
            raise ValueError("tool is required for tools/call")
        if self.operation is McpOperation.TOOLS_LIST:
            if self.tool is not None:
                raise ValueError("tool must be omitted for tools/list")
            if self.arguments:
                raise ValueError("arguments must be empty for tools/list")
            if self.expected_server_artifact_digest is not None:
                raise ValueError("tools/list must not carry an expected server artifact digest")
            return self
        if self.tool == "jarvis_run" and "wait" in self.arguments:
            raise ValueError("jarvis_run does not accept internal wait; use jarvis_get_execution")
        return self


class QueueCancelRequest(BaseModel):
    """HTTP request to cancel a relay job with explicit scheduler policy."""

    model_config = ConfigDict(extra="forbid")

    cluster: str | None = None
    cancel_scheduler_job: bool = False


class RetentionCollectRequest(BaseModel):
    """HTTP request to preview or advance bounded terminal retention."""

    model_config = ConfigDict(extra="forbid")

    execute: bool = False
    batch_size: int = Field(default=100, ge=1, le=100)
    expected_updated_at: datetime | None = None


class OwnerSessionQuiesceIntakeRequest(BaseModel):
    """HTTP request to quiesce this owned session's intake for teardown.

    clio-relay#179: replaces the per-operation ``ssh ... session
    quiesce-intake`` dial with a plain request over the held channel.
    """

    model_config = ConfigDict(extra="forbid")

    cleanup_operation_id: str
    stop_worker: bool
    cancel_jobs: bool
    cancel_scheduler_jobs: bool


class SchedulerStatusBatchRequest(BaseModel):
    """HTTP request to read a bounded batch of exact scheduler job statuses."""

    model_config = ConfigDict(extra="forbid")

    provider: str
    scheduler_job_ids: list[str] = Field(min_length=1)


class SchedulerCancelRequest(BaseModel):
    """HTTP request to cancel one scheduler job through its provider."""

    model_config = ConfigDict(extra="forbid")

    provider: str


class ProgressUpdateRequest(BaseModel):
    """HTTP request to record a job progress observation."""

    model_config = ConfigDict(extra="forbid")

    label: str = "progress"
    current: float | None = None
    total: float | None = Field(default=None, gt=0)
    unit: str | None = None
    message: str | None = None
    source_event_seq: int | None = Field(default=None, ge=1)
    metadata: dict[str, object] = Field(default_factory=dict)


class TaskTimelineEventRequest(BaseModel):
    """HTTP request to append a structured task timeline event."""

    model_config = ConfigDict(extra="forbid")

    event_type: str
    label: str
    status: TaskEventStatus = TaskEventStatus.RUNNING
    summary: str
    detail: str | None = None
    artifact_refs: list[DurableRecordId] = Field(default_factory=list)
    path_refs: list[str] = Field(default_factory=list)
    metadata: dict[str, object] = Field(default_factory=dict)


_RELAY_RUNTIME_GATEWAY_KEYS = frozenset(
    {
        "runtime_spec",
        "jarvis_runtime_binding",
        "browser_attachment",
        "ownership_intents",
        "teardown_intent",
        "teardown",
        "detach",
        "scheduler_provider",
        "scheduler_job_id",
        "scheduler_native_id",
    }
)
_RELAY_RUNTIME_CONNECTOR_KEYS = frozenset(
    {"browser_proxy", "desktop_connector", "remote_connector"}
)
_RELAY_OWNERSHIP_METADATA_KEYS = frozenset(
    {
        "owner",
        "owner_session_id",
        "owner_session_generation_id",
        "owner_session_admission_id",
        "runtime_kind",
        "binding_source",
        "source_relay_job_id",
        "source_relay_artifact_id",
        "jarvis_execution_id",
        "scheduler_provider",
        "scheduler_job_id",
        "scheduler_native_id",
    }
)


def _validate_generic_gateway_payload(
    value: dict[str, object] | None,
) -> dict[str, object] | None:
    """Reject fields whose identity is written only by the runtime supervisor."""
    if value is None:
        return None
    protected = sorted(_RELAY_RUNTIME_GATEWAY_KEYS.intersection(value))
    transport = value.get("transport")
    if isinstance(transport, dict):
        typed_transport = cast(dict[str, object], transport)
        protected.extend(
            f"transport.{key}"
            for key in sorted(_RELAY_RUNTIME_CONNECTOR_KEYS.intersection(typed_transport))
        )
    if protected:
        raise ValueError(
            "generic gateway requests cannot write relay-managed runtime fields: "
            + ", ".join(protected)
        )
    return value


def _validate_generic_gateway_metadata(value: dict[str, object]) -> dict[str, object]:
    """Reject client-provided relay ownership identity; the server stamps it."""
    protected = sorted(_RELAY_OWNERSHIP_METADATA_KEYS.intersection(value))
    if protected:
        raise ValueError(
            "generic gateway requests cannot write relay ownership metadata: "
            + ", ".join(protected)
        )
    return value


def _has_relay_managed_gateway_state(gateway: dict[str, object]) -> bool:
    """Return whether replacing this gateway payload could erase runtime ownership."""
    if _RELAY_RUNTIME_GATEWAY_KEYS.intersection(gateway):
        return True
    transport = gateway.get("transport")
    if not isinstance(transport, dict):
        return False
    return bool(_RELAY_RUNTIME_CONNECTOR_KEYS.intersection(cast(dict[str, object], transport)))


class GatewaySessionCreateRequest(BaseModel):
    """HTTP request to create a scheduler-backed gateway session."""

    model_config = ConfigDict(extra="forbid")

    cluster: str
    name: str
    state: GatewaySessionState = GatewaySessionState.CREATED
    queue_state: str | None = None
    node: str | None = None
    requested_resources: dict[str, object] = Field(default_factory=dict)
    stdout_uri: str | None = None
    stderr_uri: str | None = None
    log_uris: list[str] = Field(default_factory=list)
    gateway: dict[str, object] = Field(default_factory=dict)
    metadata: dict[str, object] = Field(default_factory=dict)

    _gateway_is_not_runtime_owned = field_validator("gateway")(_validate_generic_gateway_payload)
    _metadata_is_not_runtime_owned = field_validator("metadata")(_validate_generic_gateway_metadata)


class GatewaySessionUpdateRequest(BaseModel):
    """HTTP request to update scheduler-backed gateway session state."""

    model_config = ConfigDict(extra="forbid")

    state: GatewaySessionState | None = None
    queue_state: str | None = None
    node: str | None = None
    requested_resources: dict[str, object] | None = None
    stdout_uri: str | None = None
    stderr_uri: str | None = None
    log_uris: list[str] | None = None
    gateway: dict[str, object] | None = None
    artifacts: list[str] | None = None
    metadata: dict[str, object] = Field(default_factory=dict)

    _gateway_is_not_runtime_owned = field_validator("gateway")(_validate_generic_gateway_payload)
    _metadata_is_not_runtime_owned = field_validator("metadata")(_validate_generic_gateway_metadata)


class JarvisRuntimeAuthorityRequest(BaseModel):
    """Private exact-binding request accepted only by an owned session API."""

    model_config = ConfigDict(extra="forbid", strict=True)

    binding: JarvisServiceRuntimeBinding

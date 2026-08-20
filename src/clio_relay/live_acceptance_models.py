"""Live acceptance wire models and the tiny primitives their validators need.

Extracted from ``live_acceptance.py`` (#231 rework): every Pydantic/dataclass
model the live acceptance runner passes across its own function boundaries
(options, mutable run state, the resumable checkpoint, and the secure
runtime probe/evidence family), plus the handful of dependency-free
primitives those models call from inside their own validators -- the
canonical-JSON checkpoint hash and the RFC 6901 JSON-pointer helpers. Every
other extracted owner module, and ``live_acceptance.py`` itself, imports its
shared vocabulary from here so extraction order never has to fight a
circular import.
"""

from __future__ import annotations

import hashlib
import json
import re
import subprocess
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal, Protocol, cast
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, model_validator

from clio_relay.cluster_config import ClusterDefinition
from clio_relay.errors import RelayError
from clio_relay.identifiers import DurableRecordId
from clio_relay.jarvis_service_runtime import JarvisServiceRuntimeHandoff
from clio_relay.validation_report import ValidationRecorder


class CommandRunner(Protocol):
    """Protocol for command execution used by the live acceptance runner."""

    def __call__(
        self,
        command: list[str],
        *,
        input: bytes | None = None,
    ) -> subprocess.CompletedProcess[bytes]:
        """Run a command and return the completed process."""
        ...


MAX_ACCEPTANCE_COLLECTION_RECORDS = 10_000
MAX_SECURE_RUNTIME_RESPONSE_BYTES = 1024 * 1024
MAX_SECURE_RUNTIME_SSE_EVENT_BYTES = 256 * 1024
SECURE_RUNTIME_ACCEPTANCE_SCHEMA = "clio-relay.secure-runtime-acceptance.v1"
SECURE_RUNTIME_HTTP_EVIDENCE_SCHEMA = "clio-relay.secure-runtime-http-evidence.v1"
LIVE_ACCEPTANCE_CHECKPOINT_SCHEMA = "clio-relay.live-acceptance-checkpoint.v1"
LIVE_ACCEPTANCE_CHECKPOINT_RESOURCE_KIND = "live_acceptance_checkpoint"
LiveAcceptancePendingPhase = Literal[
    "primary_job_wait",
    "secure_runtime_metadata",
    "secure_runtime_query",
    "secure_runtime_bind",
    "agent_job_wait",
    "agent_child_job_wait",
]


def _live_acceptance_checkpoint_sha256(value: object) -> str:
    """Hash one finite checkpoint payload using its canonical wire representation."""
    return hashlib.sha256(
        json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()


def _secure_runtime_canonical_json_sha256(value: object) -> str:
    """Hash canonical finite JSON using the JARVIS runtime binding contract."""
    payload = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _validate_secure_runtime_json_pointer(pointer: str) -> None:
    """Validate one bounded RFC 6901 pointer used by an acceptance adapter."""
    if not pointer.startswith("/") or len(pointer.split("/")) > 65:
        raise ValueError("secure runtime adapter selector must be a bounded JSON pointer")
    if re.search(r"~(?:[^01]|$)", pointer) is not None:
        raise ValueError("secure runtime adapter selector used an invalid JSON pointer escape")


def _secure_runtime_json_pointer_value(
    document: object,
    pointer: str,
    *,
    label: str,
) -> object:
    """Resolve a validated RFC 6901 pointer without inference or fallback paths."""
    try:
        _validate_secure_runtime_json_pointer(pointer)
    except ValueError as exc:
        raise RelayError(f"secure runtime {label} selector was invalid") from exc
    current = document
    for encoded_token in pointer[1:].split("/"):
        token = encoded_token.replace("~1", "/").replace("~0", "~")
        if isinstance(current, dict):
            mapping = cast(dict[object, object], current)
            if token not in mapping:
                raise RelayError(f"secure runtime {label} selector did not resolve")
            current = mapping[token]
            continue
        if isinstance(current, list):
            if re.fullmatch(r"0|[1-9][0-9]*", token) is None:
                raise RelayError(f"secure runtime {label} selector did not resolve")
            index = int(token)
            sequence = cast(list[object], current)
            if index >= len(sequence):
                raise RelayError(f"secure runtime {label} selector did not resolve")
            current = sequence[index]
            continue
        raise RelayError(f"secure runtime {label} selector did not resolve")
    return current


def _configured_path(value: str | None) -> Path | None:
    if value is None:
        return None
    return Path(value).expanduser()


def _acceptance_run_id(path: Path) -> str:
    digest = hashlib.sha256(path.read_bytes()).hexdigest()[:16]
    return f"{path.stem}-{digest}-{uuid4().hex[:8]}"


class LiveAcceptanceCheckpoint(BaseModel):
    """Strict, non-expiring selector for resuming one exact live acceptance run."""

    model_config = ConfigDict(extra="forbid", strict=True)

    schema_version: Literal["clio-relay.live-acceptance-checkpoint.v1"] = (
        LIVE_ACCEPTANCE_CHECKPOINT_SCHEMA
    )
    source_report_id: DurableRecordId
    cluster: str = Field(min_length=1, max_length=256)
    scenario: str = Field(min_length=1, max_length=256)
    run_id: str = Field(min_length=1, max_length=512)
    phase: LiveAcceptancePendingPhase
    intent_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    pipeline_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    remote_pipeline_path: str = Field(min_length=1, max_length=4096)
    primary_job_id: str = Field(min_length=1, max_length=256)
    primary_idempotency_key: str = Field(min_length=1, max_length=512)
    agent_prompt: str | None = Field(default=None, max_length=4096)
    agent_job_id: str | None = Field(default=None, min_length=1, max_length=256)
    agent_child_job_id: str | None = Field(default=None, min_length=1, max_length=256)
    pipeline_id: str | None = Field(default=None, min_length=1, max_length=512)
    execution_id: str | None = Field(default=None, min_length=1, max_length=512)
    source_job_id: str | None = Field(default=None, min_length=1, max_length=256)
    source_artifact_id: str | None = Field(default=None, min_length=1, max_length=256)
    service_instance_id: str | None = Field(default=None, min_length=1, max_length=512)
    gateway_session_id: str | None = Field(default=None, min_length=1, max_length=256)
    scheduler_action: Literal["none"] = "none"
    relay_action: Literal["observe_existing"] = "observe_existing"
    integrity_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def validate_phase_and_integrity(self) -> LiveAcceptanceCheckpoint:
        """Reject incomplete selectors and any altered checkpoint field."""
        if not self.primary_job_id.startswith("job_"):
            raise ValueError("live acceptance checkpoint primary job id is invalid")
        expected_idempotency = f"live-test:{self.cluster}:{self.run_id}:jarvis"
        if self.primary_idempotency_key != expected_idempotency:
            raise ValueError("live acceptance checkpoint idempotency identity changed")
        if self.phase in {"secure_runtime_query", "secure_runtime_bind"} and (
            self.pipeline_id is None or self.execution_id is None
        ):
            raise ValueError("secure runtime checkpoint omitted pipeline or execution identity")
        if self.phase == "secure_runtime_bind" and (
            self.source_job_id is None
            or self.source_artifact_id is None
            or self.service_instance_id is None
        ):
            raise ValueError("secure runtime bind checkpoint omitted its exact source identity")
        if self.phase in {"agent_job_wait", "agent_child_job_wait"} and self.agent_job_id is None:
            raise ValueError("agent checkpoint omitted its exact relay job identity")
        if self.phase == "agent_child_job_wait" and self.agent_child_job_id is None:
            raise ValueError("agent child checkpoint omitted its exact relay job identity")
        expected_integrity = _live_acceptance_checkpoint_sha256(
            self.model_dump(mode="json", exclude={"integrity_sha256"})
        )
        if self.integrity_sha256 != expected_integrity:
            raise ValueError("live acceptance checkpoint integrity digest changed")
        return self

    def retry_selector(self) -> dict[str, object]:
        """Return the exact non-mutating selector to observe on the next invocation."""
        selector: dict[str, object] = {
            "cluster": self.cluster,
            "run_id": self.run_id,
            "phase": self.phase,
            "primary_job_id": self.primary_job_id,
            "primary_idempotency_key": self.primary_idempotency_key,
        }
        for name in (
            "agent_job_id",
            "agent_child_job_id",
            "pipeline_id",
            "execution_id",
            "source_job_id",
            "source_artifact_id",
            "service_instance_id",
            "gateway_session_id",
        ):
            value = getattr(self, name)
            if value is not None:
                selector[name] = value
        return selector


class _AcceptanceObservationPending(RelayError):
    """A bounded observation expired while the durable workload remains nonterminal."""

    def __init__(
        self,
        message: str,
        *,
        phase: LiveAcceptancePendingPhase,
        identifiers: dict[str, str] | None = None,
    ) -> None:
        super().__init__(message)
        self.phase = phase
        self.identifiers = dict(identifiers or {})


class _LiveAcceptancePending(RelayError):
    """Internal control result carrying one fully bound resumable checkpoint."""

    def __init__(self, message: str, *, checkpoint: LiveAcceptanceCheckpoint) -> None:
        super().__init__(message)
        self.checkpoint = checkpoint


class SecureRuntimeEndpointAdapter(BaseModel):
    """Application-owned JSON selectors for one runtime HTTP response."""

    model_config = ConfigDict(extra="forbid", strict=True)

    assertions: dict[str, str | int | bool | None] = Field(default_factory=dict)
    service_instance_id_pointer: str = Field(min_length=1, max_length=512)
    revision_pointer: str = Field(min_length=1, max_length=512)
    execution_id_pointer: str | None = Field(default=None, min_length=1, max_length=512)
    dataset_descriptor_pointer: str | None = Field(default=None, min_length=1, max_length=512)
    command_id_pointer: str | None = Field(default=None, min_length=1, max_length=512)
    event_name: str | None = Field(default=None, min_length=1, max_length=128)

    @model_validator(mode="after")
    def validate_pointers(self) -> SecureRuntimeEndpointAdapter:
        """Require bounded RFC 6901 pointers without embedding application semantics."""
        pointers = [
            *self.assertions,
            self.service_instance_id_pointer,
            self.revision_pointer,
            self.execution_id_pointer,
            self.dataset_descriptor_pointer,
            self.command_id_pointer,
        ]
        for pointer in pointers:
            if pointer is not None:
                _validate_secure_runtime_json_pointer(pointer)
        if len(self.assertions) > 16:
            raise ValueError("secure runtime endpoint assertions exceed 16 entries")
        return self


class SecureRuntimeProtocolAdapter(BaseModel):
    """Declarative package protocol used only by live acceptance."""

    model_config = ConfigDict(extra="forbid", strict=True)

    command_request_id_pointer: str = Field(min_length=1, max_length=512)
    health: SecureRuntimeEndpointAdapter
    state: SecureRuntimeEndpointAdapter
    command: SecureRuntimeEndpointAdapter
    events: SecureRuntimeEndpointAdapter

    @model_validator(mode="after")
    def validate_protocol(self) -> SecureRuntimeProtocolAdapter:
        """Require enough selectors to correlate every durable runtime surface."""
        _validate_secure_runtime_json_pointer(self.command_request_id_pointer)
        if self.health.event_name is not None:
            raise ValueError("secure runtime health adapter cannot declare an SSE event name")
        for name, adapter in (
            ("state", self.state),
            ("command", self.command),
            ("events", self.events),
        ):
            if adapter.execution_id_pointer is None or adapter.dataset_descriptor_pointer is None:
                raise ValueError(
                    f"secure runtime {name} adapter requires execution and dataset selectors"
                )
        if self.command.command_id_pointer is None:
            raise ValueError("secure runtime command adapter requires a command identity selector")
        if self.events.event_name is None:
            raise ValueError("secure runtime events adapter requires an SSE event name")
        if self.state.event_name is not None or self.command.event_name is not None:
            raise ValueError("only the secure runtime events adapter may declare an SSE event name")
        return self


class SecureRuntimeProbeConfig(BaseModel):
    """Application-configured selectors and command for one secure runtime probe."""

    model_config = ConfigDict(extra="forbid", strict=True)

    package_name: str = Field(min_length=1, max_length=256)
    package_id: str | None = Field(default=None, min_length=1, max_length=256)
    service_instance_id: str | None = Field(default=None, min_length=1, max_length=512)
    command: dict[str, Any]
    protocol_adapter: SecureRuntimeProtocolAdapter
    browser_attachment_ttl_seconds: int = Field(default=300, ge=60, le=28_800)
    require_state_change: bool = True
    require_sse_change: bool = True

    @model_validator(mode="after")
    def validate_command(self) -> SecureRuntimeProbeConfig:
        """Require one bounded finite JSON command supplied by the owning package demo."""
        if not self.command:
            raise ValueError("secure runtime probe command must not be empty")
        try:
            encoded = json.dumps(
                self.command,
                allow_nan=False,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8")
        except (TypeError, ValueError) as exc:
            raise ValueError("secure runtime probe command must be finite JSON") from exc
        if len(encoded) > 64 * 1024:
            raise ValueError("secure runtime probe command exceeds 65536 bytes")
        command_id = _secure_runtime_json_pointer_value(
            self.command,
            self.protocol_adapter.command_request_id_pointer,
            label="command request identity",
        )
        if (
            not isinstance(command_id, str)
            or not command_id
            or len(command_id) > 256
            or any(character in command_id for character in "\r\n\x00")
        ):
            raise ValueError("secure runtime probe command requires one bounded command identity")
        return self


class SecureRuntimeHttpEvidence(BaseModel):
    """Secret-free digest evidence for one browser-capability request."""

    model_config = ConfigDict(extra="forbid", strict=True)

    schema_version: Literal["clio-relay.secure-runtime-http-evidence.v1"] = (
        SECURE_RUNTIME_HTTP_EVIDENCE_SCHEMA
    )
    endpoint: Literal["health", "state", "command", "events"]
    method: Literal["GET", "POST"]
    status_code: int = Field(ge=200, le=299)
    content_type: str = Field(min_length=1, max_length=256)
    body_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    body_bytes: int = Field(ge=1, le=MAX_SECURE_RUNTIME_RESPONSE_BYTES)
    service_instance_id: str | None = Field(default=None, max_length=512)
    execution_id: str | None = Field(default=None, max_length=512)
    dataset_descriptor_sha256: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{64}$",
    )
    command_id: str | None = Field(default=None, min_length=1, max_length=256)
    revision: int | None = Field(default=None, ge=0)


class PackagedMcpAcceptanceEvidence(BaseModel):
    """Observed identity and contract digests from one installed MCP process."""

    model_config = ConfigDict(extra="forbid", strict=True)

    schema_version: Literal["clio-relay.packaged-mcp-stdio-evidence.v1"] = (
        "clio-relay.packaged-mcp-stdio-evidence.v1"
    )
    command: list[str] = Field(min_length=1, max_length=16)
    configured_executable: str = Field(min_length=1, max_length=4096)
    canonical_executable: str = Field(min_length=1, max_length=4096)
    executable_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    server_name: Literal["clio-relay"]
    server_version: str = Field(min_length=1, max_length=256)
    server_info_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    tools_list_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    called_tool_schema_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    jarvis_virtual_tools_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    containment_mode: Literal["windows_job_object", "linux_systemd_scope"]
    containment_enforceable: Literal[True]


class SecureRuntimeAcceptanceEvidence(BaseModel):
    """Complete secret-free proof for one v3.6 secure runtime lifecycle."""

    model_config = ConfigDict(extra="forbid", strict=True)

    schema_version: Literal["clio-relay.secure-runtime-acceptance.v1"] = (
        SECURE_RUNTIME_ACCEPTANCE_SCHEMA
    )
    claim_scope: Literal["clio-relay-core-lifecycle-and-public-evidence"] = (
        "clio-relay-core-lifecycle-and-public-evidence"
    )
    cluster: str = Field(min_length=1, max_length=256)
    query_mcp_session: PackagedMcpAcceptanceEvidence
    bind_mcp_session: PackagedMcpAcceptanceEvidence
    handoff: JarvisServiceRuntimeHandoff
    source_artifact_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    gateway_session_id: DurableRecordId
    binding_schema_version: Literal["clio-relay.jarvis-service-runtime-binding.v2"]
    service_runtime_schema_version: Literal["jarvis.service-runtime.v2"]
    service_revision: int = Field(ge=1)
    authorization_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    dataset_descriptor_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    browser_attachment_ids: list[str] = Field(min_length=2, max_length=2)
    browser_observations: list[SecureRuntimeHttpEvidence] = Field(min_length=8)
    lifecycle_states: list[Literal["ready", "degraded", "closed"]]
    scheduler_cancel_requested: Literal[False]
    browser_capability_in_public_evidence: Literal[False]
    raw_authority_material_in_public_evidence: Literal[False]
    secret_values_absent_from_public_evidence: Literal[True]

    @model_validator(mode="after")
    def validate_lifecycle(self) -> SecureRuntimeAcceptanceEvidence:
        """Require bind, detach, reconnect, and final teardown in exact order."""
        if self.lifecycle_states != ["ready", "degraded", "ready", "closed"]:
            raise ValueError("secure runtime lifecycle evidence is incomplete")
        if len(set(self.browser_attachment_ids)) != 2:
            raise ValueError("secure runtime reconnect requires two distinct attachments")
        endpoints = {observation.endpoint for observation in self.browser_observations}
        if endpoints != {"health", "state", "command", "events"}:
            raise ValueError("secure runtime browser evidence omitted a required endpoint")
        return self


@dataclass(frozen=True)
class RuntimeMetadataAcceptance:
    """Decoded runtime metadata and its structured-source trust decision."""

    document: dict[str, Any]
    structured: bool


@dataclass(frozen=True)
class _BrowserHttpResponse:
    """One bounded response read directly from the loopback browser proxy."""

    status_code: int
    content_type: str
    payload: bytes


class _BrowserHttpRequestError(RelayError):
    """Classified loopback transport failure used by revocation checks."""

    def __init__(self, message: str, *, kind: str) -> None:
        super().__init__(message)
        self.kind = kind


class _ValidationLines(list[str]):
    """List that mirrors every emitted acceptance fact into a report."""

    def __init__(self, recorder: ValidationRecorder | None) -> None:
        super().__init__()
        self._recorder = recorder

    def append(self, item: str) -> None:
        super().append(item)
        if self._recorder is not None:
            self._recorder.observe_line(item)

    def extend(self, items: Iterable[str]) -> None:
        for item in items:
            self.append(item)


def _empty_progress_payload() -> dict[str, object]:
    return {}


@dataclass(frozen=True)
class LiveAcceptanceOptions:
    """Inputs for a full live acceptance run."""

    cluster: str
    definition: ClusterDefinition
    jarvis_yaml: Path | None = None
    monitor_pattern: str | None = None
    progress_pattern: str | None = None
    progress_action_payload: dict[str, object] = field(default_factory=_empty_progress_payload)
    agent_prompt: str | None = None
    agent_mcp_config: str | None = None
    require_agent_child_job: bool | None = None
    verify_transport: bool | None = None
    verify_direct_transport: bool | None = None
    verify_ssh_transport: bool = False
    allow_direct_transport_fallback: bool | None = None
    transport_token: str | None = None
    transport_secret_key: str | None = None
    transport_frpc_bin: str = "frpc"
    transport_local_bind_port: int | None = None
    transport_remote_api_port: int | None = None
    transport_proxy_name: str | None = None
    ssh_transport_local_bind_port: int | None = None
    ssh_transport_remote_api_port: int | None = None
    ssh_transport_session_id: str | None = None
    api_token: str | None = None
    agent_child_jarvis_yaml: Path | None = None
    timeout_seconds: float = 600
    poll_seconds: float = 2
    report_path: Path | None = None
    markdown_report_path: Path | None = None
    validation_launcher: str | None = None
    validation_install_source: str | None = None
    validation_artifact_sha256: str | None = None
    require_structured_runtime_metadata: bool = False
    validation_scenario: str = "live-test"
    verify_cluster_deployment: bool = False
    report_id: DurableRecordId | None = None
    resume_report_path: Path | None = None


@dataclass
class _LiveAcceptanceState:
    """Mutable identities accumulated before a bounded observation can expire."""

    run_id: str
    intent_sha256: str
    pipeline_sha256: str
    remote_pipeline_path: str
    primary_idempotency_key: str
    primary_job_id: str | None = None
    agent_prompt: str | None = None
    agent_job_id: str | None = None
    agent_child_job_id: str | None = None
    pipeline_id: str | None = None
    execution_id: str | None = None
    source_job_id: str | None = None
    source_artifact_id: str | None = None
    service_instance_id: str | None = None
    gateway_session_id: str | None = None

    @classmethod
    def from_checkpoint(cls, checkpoint: LiveAcceptanceCheckpoint) -> _LiveAcceptanceState:
        """Restore every durable identity without inventing a new run or submission."""
        return cls(
            run_id=checkpoint.run_id,
            intent_sha256=checkpoint.intent_sha256,
            pipeline_sha256=checkpoint.pipeline_sha256,
            remote_pipeline_path=checkpoint.remote_pipeline_path,
            primary_idempotency_key=checkpoint.primary_idempotency_key,
            primary_job_id=checkpoint.primary_job_id,
            agent_prompt=checkpoint.agent_prompt,
            agent_job_id=checkpoint.agent_job_id,
            agent_child_job_id=checkpoint.agent_child_job_id,
            pipeline_id=checkpoint.pipeline_id,
            execution_id=checkpoint.execution_id,
            source_job_id=checkpoint.source_job_id,
            source_artifact_id=checkpoint.source_artifact_id,
            service_instance_id=checkpoint.service_instance_id,
            gateway_session_id=checkpoint.gateway_session_id,
        )

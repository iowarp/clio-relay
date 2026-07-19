"""Configurable live acceptance runner for cluster relay deployments."""

from __future__ import annotations

import hashlib
import http.client
import json
import math
import os
import posixpath
import re
import shlex
import socket
import subprocess
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from base64 import b64decode
from collections.abc import Generator, Iterable
from contextlib import contextmanager, suppress
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Literal, Protocol, cast
from uuid import uuid4

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator

from clio_relay import __version__
from clio_relay.browser_gateway import BrowserAttachmentGrant
from clio_relay.cluster_config import ClusterDefinition
from clio_relay.config import RelaySettings
from clio_relay.doctor import run_cluster_doctor
from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.identifiers import DurableRecordId, validate_durable_record_id
from clio_relay.installation import (
    verify_remote_clio_kit_native_execution_component,
    verify_remote_native_jarvis_component,
    verify_remote_worker_info,
)
from clio_relay.jarvis_service_runtime import (
    JARVIS_SERVICE_RUNTIME_SCHEMA_V2,
    RELAY_JARVIS_RUNTIME_BINDING_SCHEMA_V2,
    JarvisServiceRuntimeBinding,
    JarvisServiceRuntimeHandoff,
)
from clio_relay.mcp_stdio_validation import (
    PackagedMcpStdioSession,
    decode_strict_json,
    run_packaged_mcp_stdio_session,
)
from clio_relay.models import (
    TERMINAL_STATES,
    GatewaySession,
    GatewaySessionState,
    JobState,
    RelayJob,
)
from clio_relay.pagination import MAX_RESPONSE_PAGE_RECORDS
from clio_relay.progress_provenance import (
    validate_package_progress_acceptance_metadata,
)
from clio_relay.public_records import public_gateway_session
from clio_relay.remote_values import render_remote_shell_path, render_remote_shell_value
from clio_relay.runtime_metadata import (
    RUNTIME_METADATA_SCHEMA,
    JarvisRuntimeMetadata,
    RuntimeMetadataSource,
)
from clio_relay.service_runtime import ServiceRuntimeStopResult, ServiceRuntimeSupervisor
from clio_relay.session_api import (
    OWNER_SESSION_ID_HEADER,
    SESSION_GENERATION_ID_HEADER,
)
from clio_relay.storage_runtime import StorageManagedQueue, storage_managed_queue
from clio_relay.transport_probe import (
    run_frp_direct_http_probe,
    run_frp_http_probe,
    run_ssh_forward_http_probe,
    transport_evidence_lines_from_error,
)
from clio_relay.validation_report import (
    CleanupEvidence,
    EvidenceReference,
    ValidationRecorder,
    ValidationResource,
    detect_software_identity,
    new_live_validation_report,
    redact_sensitive_values,
)


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
    """Complete secret-free proof for one v3.5 secure runtime lifecycle."""

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


def run_live_acceptance(
    options: LiveAcceptanceOptions,
    *,
    runner: CommandRunner | None = None,
) -> list[str]:
    """Run live checks and persist a report even when acceptance fails."""
    command_runner = runner or _run_command
    recorder: ValidationRecorder | None = None
    if options.report_path is not None:
        transport_modes: list[str] = []
        verify_transport = (
            options.definition.live_test.verify_transport
            if options.verify_transport is None
            else options.verify_transport
        )
        verify_direct = (
            options.definition.live_test.verify_direct_transport
            if options.verify_direct_transport is None
            else options.verify_direct_transport
        )
        if verify_transport:
            transport_modes.append("frp-relay")
        if verify_direct:
            transport_modes.append("frp-direct")
        if options.verify_ssh_transport:
            transport_modes.append("ssh-forward")
        recorder = ValidationRecorder(
            new_live_validation_report(
                scenario=options.validation_scenario,
                cluster=options.cluster,
                transport_modes=transport_modes,
                launcher=options.validation_launcher,
                install_source=options.validation_install_source,
                artifact_sha256=options.validation_artifact_sha256,
                report_id=options.report_id,
            )
        )
        if transport_modes:
            recorder.report.cleanup = CleanupEvidence(
                requested=True,
                mode="transport_probe_teardown",
                cancel_scheduler_jobs=False,
            )
    try:
        lines = _run_live_acceptance(options, runner=command_runner, recorder=recorder)
    except BaseException as exc:
        if recorder is not None:
            for evidence_line in transport_evidence_lines_from_error(exc):
                try:
                    recorder.observe_line(evidence_line)
                except Exception as evidence_error:
                    recorder.record_failure(
                        "transport.structured-evidence",
                        "ingest structured transport cleanup evidence",
                        evidence_error,
                    )
            recorder.record_failure(
                "live-test.completed", "complete configured live acceptance", exc
            )
            recorder.finish(exc)
            assert options.report_path is not None
            recorder.write(options.report_path, options.markdown_report_path)
        raise
    if recorder is not None:
        recorder.finish()
        assert options.report_path is not None
        recorder.write(options.report_path, options.markdown_report_path)
        lines.append(f"validation.report={options.report_path.resolve()}")
    return lines


def _run_live_acceptance(
    options: LiveAcceptanceOptions,
    *,
    runner: CommandRunner,
    recorder: ValidationRecorder | None,
) -> list[str]:
    """Execute the acceptance workflow while emitting structured facts."""
    command_runner = runner
    jarvis_yaml = options.jarvis_yaml or _configured_path(options.definition.live_test.jarvis_yaml)
    monitor_pattern = options.monitor_pattern or options.definition.live_test.monitor_pattern
    progress_pattern = options.progress_pattern or options.definition.live_test.progress_pattern
    progress_action_payload = (
        options.progress_action_payload
        if options.progress_action_payload
        else options.definition.live_test.progress_action_payload
    )
    agent_prompt = options.agent_prompt or options.definition.live_test.agent_prompt
    agent_child_jarvis_yaml = options.agent_child_jarvis_yaml or _configured_path(
        options.definition.live_test.agent_child_jarvis_yaml
    )
    agent_mcp_config = options.agent_mcp_config or options.definition.live_test.agent_mcp_config
    require_agent_child_job = (
        agent_mcp_config is not None
        if options.require_agent_child_job is None
        else options.require_agent_child_job
    )
    verify_transport = (
        options.definition.live_test.verify_transport
        if options.verify_transport is None
        else options.verify_transport
    )
    if jarvis_yaml is None:
        raise ConfigurationError(
            "live-test requires --jarvis-yaml or cluster live_test.jarvis_yaml"
        )
    if not jarvis_yaml.exists():
        raise ConfigurationError(f"live-test JARVIS YAML does not exist: {jarvis_yaml}")
    if agent_child_jarvis_yaml is not None and not agent_child_jarvis_yaml.exists():
        raise ConfigurationError(
            f"live-test agent child JARVIS YAML does not exist: {agent_child_jarvis_yaml}"
        )
    if agent_child_jarvis_yaml is not None and agent_mcp_config is None:
        raise ConfigurationError(
            "live-test --agent-child-jarvis-yaml requires --agent-mcp-config "
            "or cluster live_test.agent_mcp_config"
        )
    if agent_child_jarvis_yaml is not None and agent_prompt is not None:
        raise ConfigurationError(
            "live-test cannot use both an explicit agent prompt and agent_child_jarvis_yaml"
        )
    transport_token: str | None = None
    transport_secret_key: str | None = None
    verify_direct_transport = (
        options.definition.live_test.verify_direct_transport
        if options.verify_direct_transport is None
        else options.verify_direct_transport
    )
    allow_direct_transport_fallback = (
        options.definition.live_test.allow_direct_transport_fallback
        if options.allow_direct_transport_fallback is None
        else options.allow_direct_transport_fallback
    )
    if verify_transport or verify_direct_transport:
        transport_token, transport_secret_key = _require_transport_secrets(
            token=options.transport_token,
            secret_key=options.transport_secret_key,
        )
    run_id = _acceptance_run_id(jarvis_yaml)
    pipeline_yaml_text = jarvis_yaml.read_text(encoding="utf-8")
    secure_runtime_probe = _secure_runtime_probe_config(pipeline_yaml_text)
    pipeline_yaml_text = _stage_acceptance_files(
        options.definition,
        jarvis_yaml=jarvis_yaml,
        pipeline_yaml_text=pipeline_yaml_text,
        run_id=run_id,
        runner=command_runner,
    )
    expected_progress_adapter = _expected_progress_adapter(pipeline_yaml_text)
    expected_progress_package = _expected_progress_package(pipeline_yaml_text)
    lines: list[str] = _ValidationLines(recorder)
    if expected_progress_adapter is not None:
        if expected_progress_package is None:
            raise ConfigurationError(
                "an explicit package progress adapter requires exactly one non-empty pkg_type"
            )
        lines.append("acceptance.application_boundary=package_progress_provider")
        lines.append(f"acceptance.package_adapter={expected_progress_adapter}")
        lines.append(f"acceptance.package_owner={expected_progress_package}")

    lines.extend(run_cluster_doctor(options.definition))
    lines.append("acceptance.cluster_doctor=passed")
    if options.verify_cluster_deployment:
        lines.extend(
            _verify_cluster_deployment(
                options.definition,
                runner=command_runner,
                expected_artifact_sha256=options.validation_artifact_sha256,
                expected_install_source=(
                    recorder.report.install_source.kind.value if recorder is not None else None
                ),
            )
        )
    if verify_transport:
        assert transport_token is not None
        assert transport_secret_key is not None
        lines.extend(
            _verify_transport(
                options,
                token=transport_token,
                secret_key=transport_secret_key,
                pipeline_yaml=pipeline_yaml_text,
                expected_progress_adapter=expected_progress_adapter,
                expected_progress_package=expected_progress_package,
            )
        )
    if verify_direct_transport:
        assert transport_token is not None
        assert transport_secret_key is not None
        direct_lines = _verify_direct_transport(
            options,
            token=transport_token,
            secret_key=transport_secret_key,
            allow_stcp_fallback=allow_direct_transport_fallback,
            pipeline_yaml=pipeline_yaml_text,
            expected_progress_adapter=expected_progress_adapter,
            expected_progress_package=expected_progress_package,
        )
        if not allow_direct_transport_fallback:
            _assert_direct_xtcp_acceptance(direct_lines)
        lines.extend(direct_lines)
    if options.verify_ssh_transport:
        lines.extend(_verify_ssh_transport(options, pipeline_yaml=pipeline_yaml_text))
    remote_yaml = f".local/share/clio-relay/live-tests/{run_id}/pipeline.yaml"
    _remote_write_file(
        options.definition.ssh_host,
        remote_yaml,
        pipeline_yaml_text.encode("utf-8"),
        runner=command_runner,
    )
    lines.append(f"acceptance.pipeline={remote_yaml}")
    if agent_child_jarvis_yaml is not None:
        agent_prompt = _write_generated_agent_prompt(
            options.definition,
            cluster=options.cluster,
            run_id=run_id,
            child_yaml=agent_child_jarvis_yaml,
            runner=command_runner,
        )
        lines.append(f"acceptance.agent_prompt={agent_prompt}")

    submit = _remote_clio_json(
        options.definition,
        [
            "job",
            "submit",
            "--cluster",
            options.cluster,
            "--jarvis-yaml",
            remote_yaml,
            "--idempotency-key",
            f"live-test:{options.cluster}:{run_id}:jarvis",
        ],
        runner=command_runner,
        raw_text=True,
    )
    job_id = submit.strip().splitlines()[-1]
    if not job_id.startswith("job_"):
        raise RelayError(f"live-test submit did not return a job id: {submit}")
    lines.append(f"acceptance.job_id={job_id}")

    if expected_progress_adapter is not None:
        _verify_live_package_progress(
            options.definition,
            job_id,
            expected_progress_adapter,
            package_name=expected_progress_package,
            timeout_seconds=options.timeout_seconds,
            poll_seconds=options.poll_seconds,
            runner=command_runner,
        )
        lines.append(f"acceptance.live_progress_adapter={expected_progress_adapter}")

    secure_runtime_forbidden_values: set[str] = set()
    if secure_runtime_probe is None:
        _wait_for_success(
            options.definition,
            job_id,
            timeout_seconds=options.timeout_seconds,
            poll_seconds=options.poll_seconds,
            runner=command_runner,
        )
        lines.append("acceptance.job_state=succeeded")
        if options.verify_cluster_deployment:
            lines.append("worker.execute=passed")

        _verify_completed_job(
            options.definition,
            job_id,
            line_prefix="acceptance",
            lines=lines,
            runner=command_runner,
            expected_progress_adapter=expected_progress_adapter,
            expected_progress_package=expected_progress_package,
            recorder=recorder,
            require_structured_runtime_metadata=options.require_structured_runtime_metadata,
        )
    else:
        if recorder is None:
            raise ConfigurationError(
                "secure runtime acceptance requires a machine-readable report path"
            )
        with _validation_check(
            recorder,
            "secure-runtime.source-live-metadata",
            "observe trusted runtime metadata while retaining the running source job",
            forbidden_values=set(),
        ) as evidence:
            runtime_metadata = _wait_for_live_structured_runtime_metadata(
                options.definition,
                job_id,
                line_prefix="acceptance",
                lines=lines,
                timeout_seconds=options.timeout_seconds,
                poll_seconds=options.poll_seconds,
                runner=command_runner,
            )
            runtime_document = runtime_metadata.document
            runtime_source = str(runtime_document["source"])
            evidence.append(
                EvidenceReference(
                    kind="relay_job_status",
                    reference=f"relay-job://{options.cluster}/{job_id}",
                    metadata={
                        "state": JobState.RUNNING.value,
                        "runtime_metadata_source": runtime_source,
                        "source_job_retained": True,
                        "cancel_scheduler_job": False,
                    },
                )
            )
            recorder.add_resource(
                ValidationResource(
                    kind="relay_job",
                    resource_id=job_id,
                    role="secure_runtime_source",
                    cluster=options.cluster,
                    state=JobState.RUNNING.value,
                    metadata={
                        "runtime_metadata_source": runtime_source,
                        "retained": True,
                        "cancel_scheduler_job": False,
                    },
                )
            )
        secure_runtime_forbidden_values = _verify_secure_runtime_acceptance(
            options,
            config=secure_runtime_probe,
            runtime_metadata=runtime_metadata.document,
            recorder=recorder,
        )
        lines.append("secure-runtime.acceptance=ok")

    if monitor_pattern is not None:
        _remote_clio_json(
            options.definition,
            [
                "monitor",
                "add-regex",
                job_id,
                "--pattern",
                monitor_pattern,
                "--event-type",
                "stdout.delta",
            ],
            runner=command_runner,
        )
        actions = _remote_clio_json(
            options.definition,
            ["monitor", "run-once", "--limit", "250"],
            runner=command_runner,
        )
        if not actions:
            raise RelayError(f"acceptance monitor pattern did not match: {monitor_pattern}")
        lines.append("acceptance.monitor=ok")

    if progress_pattern is not None:
        _verify_progress_monitor(
            options.definition,
            job_id,
            pattern=progress_pattern,
            action_payload=progress_action_payload,
            lines=lines,
            runner=command_runner,
        )

    if agent_prompt is not None:
        agent_args = [
            "agent",
            "run",
            "--cluster",
            options.cluster,
            "--prompt",
            agent_prompt,
            "--idempotency-key",
            f"live-test:{options.cluster}:{run_id}:agent",
        ]
        if agent_mcp_config is not None:
            agent_args.extend(["--mcp-config", agent_mcp_config])
        agent_submit = _remote_clio_json(
            options.definition,
            agent_args,
            runner=command_runner,
            raw_text=True,
        )
        agent_job_id = agent_submit.strip().splitlines()[-1]
        agent_job = _wait_for_success(
            options.definition,
            agent_job_id,
            timeout_seconds=options.timeout_seconds,
            poll_seconds=options.poll_seconds,
            runner=command_runner,
        )
        lines.append(f"acceptance.agent_job_id={agent_job_id}")
        lines.append("acceptance.agent_state=succeeded")
        if require_agent_child_job:
            child_job_id = _find_agent_child_job(
                options.definition,
                agent_job_id,
                agent_created_at=str(agent_job["created_at"]),
                runner=command_runner,
            )
            _wait_for_success(
                options.definition,
                child_job_id,
                timeout_seconds=options.timeout_seconds,
                poll_seconds=options.poll_seconds,
                runner=command_runner,
            )
            lines.append(f"acceptance.agent_child_job_id={child_job_id}")
            _verify_completed_job(
                options.definition,
                child_job_id,
                line_prefix="acceptance.agent_child",
                lines=lines,
                runner=command_runner,
                expected_progress_adapter=expected_progress_adapter,
                expected_progress_package=expected_progress_package,
                recorder=recorder,
                require_structured_runtime_metadata=options.require_structured_runtime_metadata,
            )

    lines.append("live acceptance passed")
    expected_transport_cleanups = sum(
        [verify_transport, verify_direct_transport, options.verify_ssh_transport]
    )
    observed_transport_cleanups = lines.count("transport.cleanup=passed")
    if observed_transport_cleanups < expected_transport_cleanups:
        raise RelayError(
            "transport cleanup evidence is incomplete: "
            f"expected={expected_transport_cleanups} observed={observed_transport_cleanups}"
        )
    if recorder is not None and recorder.transport_probe_count < expected_transport_cleanups:
        raise RelayError(
            "structured transport cleanup evidence is incomplete: "
            f"expected={expected_transport_cleanups} observed={recorder.transport_probe_count}"
        )
    if recorder is not None and recorder.report.cleanup.remaining_resources:
        raise RelayError(
            "transport cleanup left structured residual resources: "
            f"count={len(recorder.report.cleanup.remaining_resources)}"
        )
    if recorder is not None and secure_runtime_probe is not None:
        _assert_secret_free_document(
            recorder.report.model_dump(mode="json"),
            forbidden_values=secure_runtime_forbidden_values,
            label="secure runtime validation report",
        )
    return lines


def _verify_transport(
    options: LiveAcceptanceOptions,
    *,
    token: str,
    secret_key: str,
    pipeline_yaml: str,
    expected_progress_adapter: str | None,
    expected_progress_package: str | None,
) -> list[str]:
    run_suffix = uuid4().hex[:12]
    return run_frp_http_probe(
        cluster=options.cluster,
        definition=options.definition,
        frpc_bin=options.transport_frpc_bin,
        token=token,
        secret_key=secret_key,
        local_bind_port=(
            options.definition.live_test.transport_local_bind_port
            if options.transport_local_bind_port is None
            else options.transport_local_bind_port
        ),
        remote_api_port=(
            options.definition.live_test.transport_remote_api_port
            if options.transport_remote_api_port is None
            else options.transport_remote_api_port
        )
        or _unique_transport_port(run_suffix),
        proxy_name=(
            options.transport_proxy_name
            or options.definition.live_test.transport_proxy_name
            or f"relay-http-live-test-{run_suffix}"
        ),
        api_token=options.api_token,
        timeout_seconds=options.timeout_seconds,
        http_check=lambda local_url: _verify_transport_http_api(
            local_url,
            cluster=options.cluster,
            pipeline_yaml=pipeline_yaml,
            api_token=options.api_token,
            timeout_seconds=options.timeout_seconds,
            poll_seconds=options.poll_seconds,
            expected_progress_adapter=expected_progress_adapter,
            expected_progress_package=expected_progress_package,
        ),
    )


def _verify_cluster_deployment(
    definition: ClusterDefinition,
    *,
    runner: CommandRunner,
    expected_artifact_sha256: str | None,
    expected_install_source: str | None,
) -> list[str]:
    service = f"clio-relay-worker-{definition.name}.service"
    script = (
        'export PATH="$HOME/.local/bin:$PATH"\n'
        'relay_user="${USER:-$(id -un)}"\n'
        'linger="$(loginctl show-user "$relay_user" -p Linger --value 2>/dev/null || true)"\n'
        'test "$linger" = yes || { '
        'echo "persistent worker requires systemd user lingering (Linger=yes)" >&2; exit 78; }\n'
        f'test "$(systemctl --user is-enabled {shlex.quote(service)})" = enabled || {{ '
        f'echo "persistent worker service is not enabled: {shlex.quote(service)}" >&2; '
        "exit 1; }\n"
        f'test "$(systemctl --user is-active {shlex.quote(service)})" = active || {{ '
        f'echo "persistent worker service is not active: {shlex.quote(service)}" >&2; '
        "exit 1; }\n"
        f"clio-relay endpoint worker-info --cluster {shlex.quote(definition.name)}\n"
    )
    raw_info = _remote_shell(definition.ssh_host, script, runner=runner)
    try:
        loaded = json.loads(raw_info)
    except json.JSONDecodeError as exc:
        raise RelayError(f"remote installation info was not valid JSON: {exc}") from exc
    if not isinstance(loaded, dict):
        raise RelayError("remote installation info was not an object")
    info = cast(dict[str, Any], loaded)
    try:
        receipt = verify_remote_worker_info(
            info,
            expected_cluster=definition.name,
            expected_version=__version__,
            expected_software=detect_software_identity(),
            expected_artifact_sha256=expected_artifact_sha256,
            expected_source=expected_install_source,
            require_target_identity=False,
        )
    except ConfigurationError as exc:
        raise RelayError(str(exc)) from exc
    try:
        clio_kit_runtime = verify_remote_clio_kit_native_execution_component(info, receipt)
        native_jarvis_runtime = verify_remote_native_jarvis_component(info, receipt)
    except ConfigurationError as exc:
        raise RelayError(str(exc)) from exc
    software = receipt.software
    return [
        "worker.running=passed",
        "worker.service-enabled=verified",
        "worker.service-persistence=verified",
        f"worker.artifact-version={receipt.distribution_version}",
        f"worker.artifact-sha256={receipt.artifact_sha256 or 'none'}",
        "worker.source-identity="
        f"{software.commit or 'none'}:{software.tag or 'none'}:{software.dirty}",
        f"worker.scheduler-provider={info.get('scheduler_provider')}",
        "worker.components=" + json.dumps(receipt.components, sort_keys=True),
        "worker.component-artifacts="
        + json.dumps(
            {
                name: identity.model_dump(mode="json")
                for name, identity in receipt.component_artifacts.items()
            },
            sort_keys=True,
        ),
        "worker.component-runtime="
        + json.dumps(
            {
                "clio-kit": clio_kit_runtime,
                "jarvis-cd": native_jarvis_runtime,
            },
            sort_keys=True,
        ),
        "worker.component-clio-kit-native-jarvis-contract=passed",
        "worker.component-jarvis-native-execution=passed",
    ]


def _verify_direct_transport(
    options: LiveAcceptanceOptions,
    *,
    token: str,
    secret_key: str,
    allow_stcp_fallback: bool,
    pipeline_yaml: str,
    expected_progress_adapter: str | None,
    expected_progress_package: str | None,
) -> list[str]:
    run_suffix = uuid4().hex[:12]
    return run_frp_direct_http_probe(
        cluster=options.cluster,
        definition=options.definition,
        frpc_bin=options.transport_frpc_bin,
        token=token,
        secret_key=secret_key,
        local_bind_port=(
            options.definition.live_test.transport_local_bind_port
            if options.transport_local_bind_port is None
            else options.transport_local_bind_port
        ),
        remote_api_port=(
            options.definition.live_test.transport_remote_api_port
            if options.transport_remote_api_port is None
            else options.transport_remote_api_port
        )
        or _unique_transport_port(run_suffix),
        proxy_name=(
            options.transport_proxy_name
            or options.definition.live_test.transport_proxy_name
            or f"relay-http-direct-live-test-{run_suffix}"
        ),
        api_token=options.api_token,
        timeout_seconds=options.timeout_seconds,
        allow_stcp_fallback=allow_stcp_fallback,
        http_check=lambda local_url: _verify_transport_http_api(
            local_url,
            cluster=options.cluster,
            pipeline_yaml=pipeline_yaml,
            api_token=options.api_token,
            timeout_seconds=options.timeout_seconds,
            poll_seconds=options.poll_seconds,
            expected_progress_adapter=expected_progress_adapter,
            expected_progress_package=expected_progress_package,
        ),
    )


def _assert_direct_xtcp_acceptance(lines: list[str]) -> None:
    required = {
        "direct_transport.result=xtcp",
        "transport.proxy_type=xtcp",
        "transport.healthz=ok",
        "transport.http_wait=succeeded",
    }
    missing = required - set(lines)
    if missing:
        raise RelayError(f"direct transport acceptance did not prove XTCP: {sorted(missing)}")


def _verify_ssh_transport(
    options: LiveAcceptanceOptions,
    *,
    pipeline_yaml: str,
) -> list[str]:
    run_suffix = uuid4().hex[:12]
    return run_ssh_forward_http_probe(
        cluster=options.cluster,
        definition=options.definition,
        local_bind_port=options.ssh_transport_local_bind_port or _unique_transport_port(run_suffix),
        remote_api_port=options.ssh_transport_remote_api_port
        or _unique_transport_port(run_suffix[::-1]),
        session_id=options.ssh_transport_session_id or f"relay-ssh-live-test-{run_suffix}",
        api_token=options.api_token,
        timeout_seconds=options.timeout_seconds,
        http_check=lambda local_url, session_id, generation_id: _verify_transport_http_api(
            local_url,
            cluster=options.cluster,
            pipeline_yaml=pipeline_yaml,
            api_token=options.api_token,
            owner_session_id=session_id,
            session_generation_id=generation_id,
            timeout_seconds=options.timeout_seconds,
            poll_seconds=options.poll_seconds,
            expected_progress_adapter=_expected_progress_adapter(pipeline_yaml),
            expected_progress_package=_expected_progress_package(pipeline_yaml),
        ),
    )


def _unique_transport_port(run_suffix: str) -> int:
    return 20000 + (int(run_suffix[:6], 16) % 20000)


def _verify_transport_http_api(
    local_url: str,
    *,
    cluster: str,
    pipeline_yaml: str,
    api_token: str | None,
    owner_session_id: str | None = None,
    session_generation_id: str | None = None,
    timeout_seconds: float,
    poll_seconds: float,
    expected_progress_adapter: str | None,
    expected_progress_package: str | None,
) -> list[str]:
    run_digest = hashlib.sha256(pipeline_yaml.encode("utf-8")).hexdigest()[:16]
    idempotency_key = f"live-test:http-transport:{cluster}:{run_digest}:{uuid4().hex}"
    submitted = cast(
        dict[str, Any],
        _http_json(
            local_url,
            "POST",
            "/jobs/jarvis",
            api_token=api_token,
            body={
                "cluster": cluster,
                "pipeline_yaml": pipeline_yaml,
                "idempotency_key": idempotency_key,
            },
            owner_session_id=owner_session_id,
            session_generation_id=session_generation_id,
            timeout_seconds=10,
        ),
    )
    job_id = str(submitted["job_id"])
    _wait_for_transport_http_success(
        local_url,
        job_id,
        api_token=api_token,
        timeout_seconds=timeout_seconds,
        poll_seconds=poll_seconds,
    )
    monitor = cast(
        dict[str, Any],
        _http_json(
            local_url,
            "GET",
            f"/jobs/{job_id}/monitor",
            api_token=api_token,
            query={"cursor": "1", "limit": "250"},
            timeout_seconds=10,
        ),
    )
    event_types = {event["event_type"] for event in cast(list[dict[str, Any]], monitor["events"])}
    required_events = {"job.queued", "job.running", "jarvis.started", "job.succeeded"}
    missing_events = required_events - event_types
    if missing_events:
        raise RelayError(f"transport HTTP job missing events: {sorted(missing_events)}")
    stdout = cast(
        dict[str, Any],
        _http_json(
            local_url,
            "GET",
            f"/jobs/{job_id}/logs/stdout",
            api_token=api_token,
            query={"offset": "0", "limit": "65536"},
            timeout_seconds=10,
        ),
    )
    if int(stdout["next_offset"]) <= 0:
        raise RelayError("transport HTTP stdout log was empty")
    artifacts = cast(
        list[dict[str, Any]],
        _http_json(
            local_url,
            "GET",
            f"/jobs/{job_id}/artifacts",
            api_token=api_token,
            timeout_seconds=10,
        ),
    )
    artifact_kinds = {artifact["kind"] for artifact in artifacts}
    if not {"jarvis_pipeline", "stdout", "stderr", "provenance"}.issubset(artifact_kinds):
        raise RelayError(
            f"transport HTTP artifacts missing required kinds: {sorted(artifact_kinds)}"
        )
    provenance_id = next(
        str(artifact["artifact_id"]) for artifact in artifacts if artifact["kind"] == "provenance"
    )
    provenance = cast(
        dict[str, Any],
        _http_json(
            local_url,
            "GET",
            f"/artifacts/{provenance_id}/content",
            api_token=api_token,
            timeout_seconds=10,
        ),
    )
    if provenance["artifact"]["artifact_id"] != provenance_id:
        raise RelayError("transport HTTP provenance artifact id mismatch")
    if provenance["encoding"] != "base64" or str(provenance["data"]) == "":
        raise RelayError("transport HTTP provenance artifact was empty")
    runtime_facts: list[str] = []
    runtime_artifact = next(
        (artifact for artifact in artifacts if artifact.get("kind") == "runtime_metadata"),
        None,
    )
    if runtime_artifact is not None:
        runtime_artifact_id = runtime_artifact.get("artifact_id")
        if not isinstance(runtime_artifact_id, str) or not runtime_artifact_id:
            raise RelayError("transport HTTP runtime metadata artifact has no artifact id")
        runtime_payload = cast(
            dict[str, Any],
            _http_json(
                local_url,
                "GET",
                f"/artifacts/{runtime_artifact_id}/content",
                api_token=api_token,
                timeout_seconds=10,
            ),
        )
        runtime_facts = _runtime_metadata_facts(
            runtime_payload,
            artifact_id=runtime_artifact_id,
            line_prefix="transport.http",
        )
    lines = [
        f"transport.http_job_id={job_id}",
        "transport.http_wait=succeeded",
        "transport.http_events=ok",
        f"transport.http_stdout_bytes={stdout['next_offset']}",
        "transport.http_artifacts=ok",
        "transport.http_provenance=ok",
    ]
    lines.extend(runtime_facts)
    if expected_progress_adapter is not None:
        progress = cast(
            list[dict[str, Any]],
            _http_json(
                local_url,
                "GET",
                f"/jobs/{job_id}/progress",
                api_token=api_token,
                timeout_seconds=10,
            ),
        )
        _assert_progress_adapter(
            progress,
            expected_progress_adapter,
            job_id=job_id,
            package_name=expected_progress_package,
        )
        lines.append(f"transport.http_progress_adapter={expected_progress_adapter}")
    return lines


def _http_json(
    base_url: str,
    method: str,
    path: str,
    *,
    api_token: str | None,
    owner_session_id: str | None = None,
    session_generation_id: str | None = None,
    body: dict[str, object] | None = None,
    query: dict[str, str] | None = None,
    timeout_seconds: float,
) -> dict[str, Any] | list[dict[str, Any]]:
    if (owner_session_id is None) != (session_generation_id is None):
        raise ValueError("owner session and generation HTTP bindings must be provided together")
    encoded_query = "" if not query else "?" + urllib.parse.urlencode(query)
    data = None if body is None else json.dumps(body).encode("utf-8")
    request = urllib.request.Request(
        base_url + path + encoded_query,
        data=data,
        method=method,
        headers={"Content-Type": "application/json"} if data is not None else {},
    )
    if api_token is not None:
        request.add_header("Authorization", f"Bearer {api_token}")
    if owner_session_id is not None and session_generation_id is not None:
        request.add_header(OWNER_SESSION_ID_HEADER, owner_session_id)
        request.add_header(SESSION_GENERATION_ID_HEADER, session_generation_id)
    attempts = 3
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
                payload = response.read().decode("utf-8")
            return cast(dict[str, Any] | list[dict[str, Any]], json.loads(payload))
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise RelayError(f"transport HTTP request failed: {method} {path}: {detail}") from exc
        except (OSError, urllib.error.URLError) as exc:
            last_error = exc
            if attempt == attempts:
                break
            time.sleep(min(2.0, 0.5 * attempt))
    assert last_error is not None
    raise RelayError(
        f"transport HTTP request failed: {method} {path}: {last_error}"
    ) from last_error


def _wait_for_transport_http_success(
    local_url: str,
    job_id: str,
    *,
    api_token: str | None,
    timeout_seconds: float,
    poll_seconds: float,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    while True:
        job = cast(
            dict[str, Any],
            _http_json(
                local_url,
                "GET",
                f"/jobs/{job_id}",
                api_token=api_token,
                timeout_seconds=10,
            ),
        )
        if job["state"] == "succeeded":
            return job
        if job["state"] in {"failed", "canceled"}:
            raise RelayError(f"transport HTTP job did not succeed: {job['state']}")
        if time.monotonic() >= deadline:
            raise RelayError(f"transport HTTP job did not reach terminal state: {job_id}")
        time.sleep(poll_seconds)


def _require_transport_secrets(
    *,
    token: str | None,
    secret_key: str | None,
) -> tuple[str, str]:
    if token is None:
        raise ConfigurationError("live transport acceptance requires a frp token")
    if secret_key is None:
        raise ConfigurationError("live transport acceptance requires an stcp secret")
    return token, secret_key


def _verify_secure_runtime_acceptance(
    options: LiveAcceptanceOptions,
    *,
    config: SecureRuntimeProbeConfig,
    runtime_metadata: dict[str, Any],
    recorder: ValidationRecorder,
) -> set[str]:
    """Exercise one authenticated JARVIS service through bind, browser, and cleanup."""
    pipeline_id = runtime_metadata.get("pipeline_id")
    execution_id = runtime_metadata.get("execution_id")
    if not isinstance(pipeline_id, str) or not pipeline_id:
        raise RelayError("secure runtime metadata omitted pipeline_id")
    if not isinstance(execution_id, str) or not execution_id:
        raise RelayError("secure runtime metadata omitted execution_id")

    token = _configured_runtime_secret(
        explicit=options.transport_token,
        environment_name=options.definition.frp_transport.token_env,
        label="frp token",
    )
    secret_key = _configured_runtime_secret(
        explicit=options.transport_secret_key,
        environment_name=options.definition.frp_transport.stcp_secret_env,
        label="stcp secret",
    )
    forbidden_values = {token, secret_key}
    public_documents: list[object] = []
    gateway_session_id: str | None = None
    active_attachment: BrowserAttachmentGrant | None = None
    teardown_complete = False
    browser_observations: list[SecureRuntimeHttpEvidence] = []
    attachment_ids: list[str] = []
    revoked_grants: list[tuple[BrowserAttachmentGrant, bool]] = []
    lifecycle_states: list[Literal["ready", "degraded", "closed"]] = []
    supervisor: ServiceRuntimeSupervisor | None = None
    runtime_queue: StorageManagedQueue | None = None
    baseline_gateway_session_ids: set[str] | None = None
    handoff: JarvisServiceRuntimeHandoff | None = None
    teardown_result: ServiceRuntimeStopResult | None = None

    primary_error: Exception | None = None
    try:
        with _validation_check(
            recorder,
            "secure-runtime.jarvis-v3.5-query",
            "query one execution-owned service through the pinned JARVIS v3.5 contract",
            forbidden_values=forbidden_values,
        ) as evidence:
            query_deadline = time.monotonic() + options.timeout_seconds
            query_attempt = 0
            first_query_identity: PackagedMcpAcceptanceEvidence | None = None
            handoff: JarvisServiceRuntimeHandoff | None = None
            while True:
                remaining = query_deadline - time.monotonic()
                if remaining <= 0:
                    raise RelayError(
                        "timed out waiting for one ready JARVIS service runtime binding: "
                        f"{execution_id}"
                    )
                query_attempt += 1
                query_session = run_packaged_mcp_stdio_session(
                    profile="user",
                    tool="jarvis_get_execution",
                    arguments={
                        "cluster": options.cluster,
                        "pipeline_id": pipeline_id,
                        "execution_id": execution_id,
                        "include_service_runtimes": True,
                        "wait_for_terminal": True,
                        "wait_timeout_seconds": remaining,
                        "poll_seconds": options.poll_seconds,
                    },
                    timeout_seconds=remaining + 30.0,
                    require_enforceable_containment=True,
                )
                if time.monotonic() >= query_deadline:
                    raise RelayError(
                        "timed out waiting for one ready JARVIS service runtime binding: "
                        f"{execution_id}"
                    )
                query_result = _packaged_mcp_structured_result(
                    query_session,
                    expected_tool="jarvis_get_execution",
                )
                query_mcp_evidence = _packaged_mcp_acceptance_evidence(
                    query_session,
                    expected_tool="jarvis_get_execution",
                )
                if first_query_identity is None:
                    first_query_identity = query_mcp_evidence
                elif query_mcp_evidence != first_query_identity:
                    raise RelayError(
                        "packaged MCP identity changed while waiting for service readiness"
                    )
                public_documents.append(query_result)
                candidate_handoff = _select_secure_runtime_handoff(
                    query_result,
                    cluster=options.cluster,
                    config=config,
                )
                if candidate_handoff is not None:
                    handoff = candidate_handoff
                    break
                evidence.append(
                    EvidenceReference(
                        kind="packaged_mcp_stdio",
                        reference=(
                            f"packaged-mcp://jarvis_get_execution/readiness-attempt/{query_attempt}"
                        ),
                        excerpt="execution query returned no ready service runtime binding",
                        metadata={
                            **query_mcp_evidence.model_dump(mode="json"),
                            "pipeline_id": pipeline_id,
                            "execution_id": execution_id,
                            "ready_binding_count": 0,
                        },
                    )
                )
                remaining = query_deadline - time.monotonic()
                if remaining <= 0:
                    raise RelayError(
                        "timed out waiting for one ready JARVIS service runtime binding: "
                        f"{execution_id}"
                    )
                time.sleep(min(options.poll_seconds, remaining))

            assert handoff is not None
            source_artifact_sha256 = _query_source_artifact_sha256(
                query_result,
                handoff=handoff,
            )
            evidence.append(
                EvidenceReference(
                    kind="packaged_mcp_stdio",
                    reference=(
                        f"relay-job://{handoff.cluster}/{handoff.source_job_id}/"
                        f"{handoff.source_artifact_id}"
                    ),
                    sha256=source_artifact_sha256,
                    metadata={
                        **query_mcp_evidence.model_dump(mode="json"),
                        "pipeline_id": pipeline_id,
                        "execution_id": execution_id,
                    },
                )
            )
            recorder.add_resource(
                ValidationResource(
                    kind="relay_job",
                    resource_id=handoff.source_job_id,
                    role="secure_runtime_query",
                    cluster=options.cluster,
                    state="succeeded",
                )
            )
            recorder.add_resource(
                ValidationResource(
                    kind="artifact",
                    resource_id=handoff.source_artifact_id,
                    role="private_mcp_result",
                    cluster=options.cluster,
                    metadata={"sha256": source_artifact_sha256, "model_readable": False},
                )
            )

        with _isolated_runtime_child_environment(
            token_name=options.definition.frp_transport.token_env,
            token=token,
            secret_name=options.definition.frp_transport.stcp_secret_env,
            secret=secret_key,
        ) as runtime_child_environment:
            settings = RelaySettings.from_env()
            runtime_queue = storage_managed_queue(settings)
            baseline_gateway_session_ids = {
                session.session_id
                for session in _gateway_sessions_for_acceptance(
                    runtime_queue,
                    cluster=options.cluster,
                )
            }
            supervisor = ServiceRuntimeSupervisor(
                settings=settings,
                queue=runtime_queue,
                cluster=options.cluster,
                definition=options.definition,
                token=token,
                secret_key=secret_key,
            )
            with _validation_check(
                recorder,
                "secure-runtime.private-authority-bind",
                "resolve exact private authority and bind authenticated relay connectors",
                forbidden_values=forbidden_values,
            ) as evidence:
                bind_session = run_packaged_mcp_stdio_session(
                    profile="user",
                    tool="relay_bind_jarvis_runtime",
                    arguments={
                        "binding": handoff.model_dump(mode="json"),
                        "readiness_timeout_seconds": options.timeout_seconds,
                        "poll_seconds": options.poll_seconds,
                    },
                    timeout_seconds=options.timeout_seconds + 30.0,
                    extra_environment=runtime_child_environment,
                    require_enforceable_containment=True,
                )
                bind_result = _packaged_mcp_structured_result(
                    bind_session,
                    expected_tool="relay_bind_jarvis_runtime",
                )
                bind_mcp_evidence = _packaged_mcp_acceptance_evidence(
                    bind_session,
                    expected_tool="relay_bind_jarvis_runtime",
                )
                if (
                    bind_mcp_evidence.canonical_executable
                    != query_mcp_evidence.canonical_executable
                    or bind_mcp_evidence.executable_sha256 != query_mcp_evidence.executable_sha256
                    or bind_mcp_evidence.jarvis_virtual_tools_sha256
                    != query_mcp_evidence.jarvis_virtual_tools_sha256
                ):
                    raise RelayError("packaged MCP identity changed between query and bind")
                public_documents.append(bind_result)
                gateway_session_id = _secure_runtime_cleanup_candidate(
                    bind_result,
                    handoff=handoff,
                )
                validated_session_id, binding = _validated_secure_runtime_bind(
                    bind_result,
                    handoff=handoff,
                    expected_execution_id=execution_id,
                    expected_source_artifact_sha256=source_artifact_sha256,
                )
                if validated_session_id != gateway_session_id:
                    raise RelayError("secure runtime bind changed its cleanup identity")
                gateway = cast(dict[str, Any], bind_result["gateway_session"])
                public_documents.append(gateway)
                lifecycle_states.append("ready")
                evidence.append(
                    EvidenceReference(
                        kind="private_authority_resolution",
                        reference=f"gateway-runtime://{options.cluster}/{gateway_session_id}",
                        sha256=cast(str, binding.authorization_sha256),
                        metadata={
                            "resolver_identity_complete": True,
                            "pipeline_id": pipeline_id,
                            "execution_id": binding.jarvis_execution_id,
                            "package_id": binding.package_id,
                            "service_instance_id": binding.service_instance_id,
                            "service_revision": binding.service_revision,
                            "raw_authority_material_in_public_evidence": False,
                        },
                    )
                )
                recorder.add_resource(
                    ValidationResource(
                        kind="secure_runtime_binding",
                        resource_id=(f"{gateway_session_id}:revision:{binding.service_revision}"),
                        role="private_authority_bind",
                        cluster=options.cluster,
                        state="ready",
                        metadata={
                            "binding_schema_version": binding.schema_version,
                            "evidence_scope": ("clio-relay-core-lifecycle-and-public-evidence"),
                            "service_runtime_schema_version": (
                                binding.service_runtime_schema_version
                            ),
                            "source_relay_job_id": binding.source_relay_job_id,
                            "source_relay_artifact_id": binding.source_relay_artifact_id,
                            "source_relay_artifact_sha256": (binding.source_relay_artifact_sha256),
                            "jarvis_execution_id": binding.jarvis_execution_id,
                            "package_id": binding.package_id,
                            "package_name": binding.package_name,
                            "service_instance_id": binding.service_instance_id,
                            "service_revision": binding.service_revision,
                            "authorization_sha256": binding.authorization_sha256,
                            "dataset_descriptor_sha256": (binding.dataset_descriptor_sha256),
                            "query_mcp_containment_mode": query_mcp_evidence.containment_mode,
                            "query_mcp_containment_enforceable": (
                                query_mcp_evidence.containment_enforceable
                            ),
                            "bind_mcp_containment_mode": bind_mcp_evidence.containment_mode,
                            "bind_mcp_containment_enforceable": (
                                bind_mcp_evidence.containment_enforceable
                            ),
                        },
                    )
                )

            with _validation_check(
                recorder,
                "secure-runtime.browser-protocol",
                "exercise authenticated health, state, command, and SSE browser surfaces",
                forbidden_values=forbidden_values,
            ) as evidence:
                command_id = cast(
                    str,
                    _secure_runtime_json_pointer_value(
                        config.command,
                        config.protocol_adapter.command_request_id_pointer,
                        label="command request identity",
                    ),
                )
                event_name = cast(str, config.protocol_adapter.events.event_name)
                active_attachment = supervisor.browser_attach(
                    session_id=gateway_session_id,
                    ttl_seconds=config.browser_attachment_ttl_seconds,
                )
                attachment_ids.append(active_attachment.attachment_id)
                browser_capability = _browser_attachment_capability(active_attachment)
                forbidden_values.add(browser_capability)
                initial_health, initial_health_document = _browser_json_observation(
                    active_attachment.health_url,
                    endpoint="health",
                    method="GET",
                    body=None,
                    timeout_seconds=min(options.timeout_seconds, 60.0),
                )
                initial_health, initial_health_revision = (
                    _correlate_secure_runtime_browser_document(
                        initial_health_document,
                        initial_health,
                        endpoint="health",
                        adapter=config.protocol_adapter.health,
                        expected_service_instance_id=binding.service_instance_id,
                        expected_execution_id=binding.jarvis_execution_id,
                        expected_dataset_descriptor_sha256=binding.dataset_descriptor_sha256,
                        expected_command_id=None,
                    )
                )
                initial_state, initial_state_document = _browser_json_observation(
                    active_attachment.state_url,
                    endpoint="state",
                    method="GET",
                    body=None,
                    timeout_seconds=min(options.timeout_seconds, 60.0),
                )
                initial_state, initial_state_revision = _correlate_secure_runtime_browser_document(
                    initial_state_document,
                    initial_state,
                    endpoint="state",
                    adapter=config.protocol_adapter.state,
                    expected_service_instance_id=binding.service_instance_id,
                    expected_execution_id=binding.jarvis_execution_id,
                    expected_dataset_descriptor_sha256=binding.dataset_descriptor_sha256,
                    expected_command_id=None,
                )
                initial_event, initial_event_document = _browser_sse_observation(
                    active_attachment.events_url,
                    timeout_seconds=min(options.timeout_seconds, 60.0),
                    expected_event_name=event_name,
                )
                initial_event, initial_event_revision = _correlate_secure_runtime_browser_document(
                    initial_event_document,
                    initial_event,
                    endpoint="events",
                    adapter=config.protocol_adapter.events,
                    expected_service_instance_id=binding.service_instance_id,
                    expected_execution_id=binding.jarvis_execution_id,
                    expected_dataset_descriptor_sha256=binding.dataset_descriptor_sha256,
                    expected_command_id=None,
                )
                if {
                    initial_health_revision,
                    initial_state_revision,
                    initial_event_revision,
                } != {binding.service_revision}:
                    raise RelayError("secure runtime initial surfaces changed binding revision")
                command_observation, command_response = _browser_json_observation(
                    active_attachment.command_url,
                    endpoint="command",
                    method="POST",
                    body=config.command,
                    timeout_seconds=min(options.timeout_seconds, 60.0),
                )
                command_observation, command_revision = _correlate_secure_runtime_browser_document(
                    command_response,
                    command_observation,
                    endpoint="command",
                    adapter=config.protocol_adapter.command,
                    expected_service_instance_id=binding.service_instance_id,
                    expected_execution_id=binding.jarvis_execution_id,
                    expected_dataset_descriptor_sha256=binding.dataset_descriptor_sha256,
                    expected_command_id=command_id,
                )
                if command_revision <= initial_state_revision:
                    raise RelayError("secure runtime command did not advance service revision")
                changed_event, changed_event_document = _wait_for_changed_sse_event(
                    active_attachment.events_url,
                    previous=initial_event,
                    require_change=config.require_sse_change,
                    timeout_seconds=min(options.timeout_seconds, 60.0),
                    poll_seconds=options.poll_seconds,
                    expected_event_name=event_name,
                )
                changed_event, changed_event_revision = _correlate_secure_runtime_browser_document(
                    changed_event_document,
                    changed_event,
                    endpoint="events",
                    adapter=config.protocol_adapter.events,
                    expected_service_instance_id=binding.service_instance_id,
                    expected_execution_id=binding.jarvis_execution_id,
                    expected_dataset_descriptor_sha256=binding.dataset_descriptor_sha256,
                    expected_command_id=command_id,
                )
                changed_state, changed_state_document = _wait_for_changed_browser_state(
                    active_attachment.state_url,
                    previous=initial_state,
                    require_change=config.require_state_change,
                    timeout_seconds=min(options.timeout_seconds, 60.0),
                    poll_seconds=options.poll_seconds,
                )
                changed_state, changed_state_revision = _correlate_secure_runtime_browser_document(
                    changed_state_document,
                    changed_state,
                    endpoint="state",
                    adapter=config.protocol_adapter.state,
                    expected_service_instance_id=binding.service_instance_id,
                    expected_execution_id=binding.jarvis_execution_id,
                    expected_dataset_descriptor_sha256=binding.dataset_descriptor_sha256,
                    expected_command_id=command_id,
                )
                if {changed_event_revision, changed_state_revision} != {command_revision}:
                    raise RelayError("secure runtime command correlation changed its revision")
                first_observations = [
                    initial_health,
                    initial_state,
                    initial_event,
                    command_observation,
                    changed_event,
                    changed_state,
                ]
                browser_observations.extend(first_observations)
                evidence.extend(
                    _browser_evidence_reference(
                        active_attachment.attachment_id,
                        observation,
                    )
                    for observation in first_observations
                )

            with _validation_check(
                recorder,
                "secure-runtime.browser-revocation",
                "revoke the one-time browser capability before runtime detach",
                forbidden_values=forbidden_values,
            ) as evidence:
                revoked_grant = active_attachment
                detached_browser = supervisor.browser_detach(
                    session_id=gateway_session_id,
                    attachment_id=revoked_grant.attachment_id,
                )
                active_attachment = None
                if detached_browser.attachment_id != revoked_grant.attachment_id:
                    raise RelayError("browser detach returned a different attachment identity")
                if not detached_browser.capability_revoked or not detached_browser.proxy_stopped:
                    raise RelayError("browser detach did not revoke and stop its exact proxy")
                revoked_grants.append((revoked_grant, detached_browser.proxy_stopped))
                _assert_browser_capability_revoked(
                    revoked_grant.health_url,
                    timeout_seconds=min(options.poll_seconds, 2.0),
                    proxy_stopped=detached_browser.proxy_stopped,
                )
                evidence.append(
                    EvidenceReference(
                        kind="browser_capability_revocation",
                        reference=(
                            f"browser-attachment://{gateway_session_id}/"
                            f"{revoked_grant.attachment_id}"
                        ),
                        excerpt="revocation observed before runtime detach",
                    )
                )

            with _validation_check(
                recorder,
                "secure-runtime.detach",
                "detach desktop connector while retaining remote and scheduler resources",
                forbidden_values=forbidden_values,
            ) as evidence:
                detached = supervisor.detach(session_id=gateway_session_id)
                _validate_secure_runtime_cleanup(
                    detached,
                    expected_mode="detach",
                    expected_session_id=gateway_session_id,
                )
                lifecycle_states.append("degraded")
                public_detach = cast(
                    dict[str, Any], redact_sensitive_values(detached.json_payload())
                )
                public_documents.append(public_detach)
                _record_runtime_cleanup(
                    recorder,
                    detached,
                    role="secure_runtime_detach",
                )
                evidence.append(
                    EvidenceReference(
                        kind="gateway_cleanup",
                        reference=f"gateway-runtime://{options.cluster}/{gateway_session_id}",
                        excerpt="desktop detached; remote runtime and scheduler work retained",
                        metadata={"mode": "detach", "scheduler_cancel_requested": False},
                    )
                )

            with _validation_check(
                recorder,
                "secure-runtime.reconnect",
                "reattach relay connector and issue a fresh browser capability",
                forbidden_values=forbidden_values,
            ) as evidence:
                reattached = supervisor.attach(session_id=gateway_session_id)
                if (
                    reattached.session.session_id != gateway_session_id
                    or reattached.session.state is not GatewaySessionState.READY
                ):
                    raise RelayError("secure runtime reattachment did not restore the gateway")
                lifecycle_states.append("ready")
                public_documents.append(public_gateway_session(reattached.session))
                active_attachment = supervisor.browser_attach(
                    session_id=gateway_session_id,
                    ttl_seconds=config.browser_attachment_ttl_seconds,
                )
                attachment_ids.append(active_attachment.attachment_id)
                browser_capability = _browser_attachment_capability(active_attachment)
                if browser_capability in forbidden_values:
                    raise RelayError("secure runtime reconnect reused a browser capability")
                forbidden_values.add(browser_capability)
                for old_grant, proxy_stopped in revoked_grants:
                    _assert_browser_capability_revoked(
                        old_grant.health_url,
                        timeout_seconds=min(options.poll_seconds, 2.0),
                        proxy_stopped=proxy_stopped,
                    )
                reconnected_health, reconnected_health_document = _browser_json_observation(
                    active_attachment.health_url,
                    endpoint="health",
                    method="GET",
                    body=None,
                    timeout_seconds=min(options.timeout_seconds, 60.0),
                )
                reconnected_health, reconnected_health_revision = (
                    _correlate_secure_runtime_browser_document(
                        reconnected_health_document,
                        reconnected_health,
                        endpoint="health",
                        adapter=config.protocol_adapter.health,
                        expected_service_instance_id=binding.service_instance_id,
                        expected_execution_id=binding.jarvis_execution_id,
                        expected_dataset_descriptor_sha256=binding.dataset_descriptor_sha256,
                        expected_command_id=None,
                    )
                )
                reconnected_state, reconnected_state_document = _browser_json_observation(
                    active_attachment.state_url,
                    endpoint="state",
                    method="GET",
                    body=None,
                    timeout_seconds=min(options.timeout_seconds, 60.0),
                )
                reconnected_state, reconnected_state_revision = (
                    _correlate_secure_runtime_browser_document(
                        reconnected_state_document,
                        reconnected_state,
                        endpoint="state",
                        adapter=config.protocol_adapter.state,
                        expected_service_instance_id=binding.service_instance_id,
                        expected_execution_id=binding.jarvis_execution_id,
                        expected_dataset_descriptor_sha256=binding.dataset_descriptor_sha256,
                        expected_command_id=command_id,
                    )
                )
                reconnected_event, reconnected_event_document = _browser_sse_observation(
                    active_attachment.events_url,
                    timeout_seconds=min(options.timeout_seconds, 60.0),
                    expected_event_name=event_name,
                )
                reconnected_event, reconnected_event_revision = (
                    _correlate_secure_runtime_browser_document(
                        reconnected_event_document,
                        reconnected_event,
                        endpoint="events",
                        adapter=config.protocol_adapter.events,
                        expected_service_instance_id=binding.service_instance_id,
                        expected_execution_id=binding.jarvis_execution_id,
                        expected_dataset_descriptor_sha256=binding.dataset_descriptor_sha256,
                        expected_command_id=command_id,
                    )
                )
                if {
                    reconnected_health_revision,
                    reconnected_state_revision,
                    reconnected_event_revision,
                } != {command_revision}:
                    raise RelayError("secure runtime reconnect changed command revision")
                reconnected_observations = [
                    reconnected_health,
                    reconnected_state,
                    reconnected_event,
                ]
                browser_observations.extend(reconnected_observations)
                evidence.extend(
                    _browser_evidence_reference(
                        active_attachment.attachment_id,
                        observation,
                    )
                    for observation in reconnected_observations
                )

            with _validation_check(
                recorder,
                "secure-runtime.teardown",
                "revoke browser access and close relay resources without scheduler cancellation",
                forbidden_values=forbidden_values,
            ) as evidence:
                assert active_attachment is not None
                final_grant = active_attachment
                final_detachment = supervisor.browser_detach(
                    session_id=gateway_session_id,
                    attachment_id=final_grant.attachment_id,
                )
                active_attachment = None
                if (
                    final_detachment.attachment_id != final_grant.attachment_id
                    or not final_detachment.capability_revoked
                    or not final_detachment.proxy_stopped
                ):
                    raise RelayError("final browser detach did not revoke and stop its exact proxy")
                revoked_grants.append((final_grant, final_detachment.proxy_stopped))
                _assert_browser_capability_revoked(
                    final_grant.health_url,
                    timeout_seconds=min(options.poll_seconds, 2.0),
                    proxy_stopped=final_detachment.proxy_stopped,
                )
                teardown_result = supervisor.stop(
                    session_id=gateway_session_id,
                    cancel_scheduler_job=False,
                )
                _validate_secure_runtime_cleanup(
                    teardown_result,
                    expected_mode="teardown",
                    expected_session_id=gateway_session_id,
                )
                teardown_complete = True
                lifecycle_states.append("closed")
                public_teardown = cast(
                    dict[str, Any],
                    redact_sensitive_values(teardown_result.json_payload()),
                )
                public_documents.append(public_teardown)
                _record_runtime_cleanup(
                    recorder,
                    teardown_result,
                    role="secure_runtime_teardown",
                )
                for old_grant, proxy_stopped in revoked_grants:
                    _assert_browser_capability_revoked(
                        old_grant.health_url,
                        timeout_seconds=min(options.poll_seconds, 2.0),
                        proxy_stopped=proxy_stopped,
                    )
                evidence.append(
                    EvidenceReference(
                        kind="gateway_cleanup",
                        reference=f"gateway-runtime://{options.cluster}/{gateway_session_id}",
                        excerpt="gateway closed; scheduler cancellation not requested",
                        metadata={
                            "mode": "teardown",
                            "scheduler_cancel_requested": False,
                            "remaining_resources": 0,
                        },
                    )
                )

        assert gateway_session_id is not None
        assert teardown_result is not None
        secure_evidence = SecureRuntimeAcceptanceEvidence(
            cluster=options.cluster,
            query_mcp_session=query_mcp_evidence,
            bind_mcp_session=bind_mcp_evidence,
            handoff=handoff,
            source_artifact_sha256=source_artifact_sha256,
            gateway_session_id=gateway_session_id,
            binding_schema_version=cast(
                Literal["clio-relay.jarvis-service-runtime-binding.v2"],
                binding.schema_version,
            ),
            service_runtime_schema_version=cast(
                Literal["jarvis.service-runtime.v2"],
                binding.service_runtime_schema_version,
            ),
            service_revision=binding.service_revision,
            authorization_sha256=cast(str, binding.authorization_sha256),
            dataset_descriptor_sha256=binding.dataset_descriptor_sha256,
            browser_attachment_ids=attachment_ids,
            browser_observations=browser_observations,
            lifecycle_states=lifecycle_states,
            scheduler_cancel_requested=False,
            browser_capability_in_public_evidence=False,
            raw_authority_material_in_public_evidence=False,
            secret_values_absent_from_public_evidence=True,
        )
        public_documents.append(secure_evidence.model_dump(mode="json"))
        with _validation_check(
            recorder,
            "secure-runtime.secrets-absent",
            "prove private authority, browser capabilities, and connector secrets are absent",
            forbidden_values=forbidden_values,
        ) as evidence:
            for index, document in enumerate(public_documents):
                _assert_secret_free_document(
                    document,
                    forbidden_values=forbidden_values,
                    label=f"secure runtime public document {index}",
                )
            _assert_secret_free_document(
                recorder.report.model_dump(mode="json"),
                forbidden_values=forbidden_values,
                label="secure runtime report before final evidence",
            )
            evidence.append(
                EvidenceReference(
                    kind="secure_runtime_acceptance",
                    reference=f"gateway-runtime://{options.cluster}/{gateway_session_id}",
                    sha256=_secure_runtime_canonical_json_sha256(
                        secure_evidence.model_dump(mode="json")
                    ),
                    metadata=secure_evidence.model_dump(mode="json"),
                )
            )
        return forbidden_values
    except Exception as exc:
        primary_error = exc
        _redact_exception_values(exc, forbidden_values)
        raise
    finally:
        cleanup_session_ids: list[str] = []
        if gateway_session_id is not None:
            cleanup_session_ids.append(gateway_session_id)
        elif (
            supervisor is not None
            and runtime_queue is not None
            and baseline_gateway_session_ids is not None
            and handoff is not None
        ):
            try:
                cleanup_session_ids.extend(
                    session.session_id
                    for session in _gateway_sessions_for_acceptance(
                        runtime_queue,
                        cluster=options.cluster,
                    )
                    if session.session_id not in baseline_gateway_session_ids
                    and _gateway_session_matches_handoff(session, handoff=handoff)
                )
            except Exception as cleanup_discovery_exc:
                if primary_error is not None:
                    primary_error.add_note(
                        "secure runtime cleanup discovery: "
                        + _redacted_error_text(cleanup_discovery_exc, forbidden_values)
                    )
        if supervisor is not None and cleanup_session_ids and not teardown_complete:
            cleanup_errors: list[str] = []
            if active_attachment is not None and gateway_session_id is not None:
                try:
                    supervisor.browser_detach(
                        session_id=gateway_session_id,
                        attachment_id=active_attachment.attachment_id,
                    )
                except Exception as cleanup_exc:
                    cleanup_errors.append(_redacted_error_text(cleanup_exc, forbidden_values))
            for cleanup_session_id in cleanup_session_ids:
                try:
                    cleanup = supervisor.stop(
                        session_id=cleanup_session_id,
                        cancel_scheduler_job=False,
                    )
                    _record_runtime_cleanup(
                        recorder,
                        cleanup,
                        role="secure_runtime_failure_cleanup",
                    )
                    if cleanup.errors or cleanup.residual_resources:
                        cleanup_errors.extend(
                            _redacted_text(item, forbidden_values) for item in cleanup.errors
                        )
                except Exception as cleanup_exc:
                    cleanup_errors.append(_redacted_error_text(cleanup_exc, forbidden_values))
            if cleanup_errors and primary_error is not None:
                primary_error.add_note("secure runtime cleanup: " + "; ".join(cleanup_errors))


@contextmanager
def _validation_check(
    recorder: ValidationRecorder,
    check_id: str,
    summary: str,
    *,
    forbidden_values: set[str],
) -> Generator[list[EvidenceReference]]:
    """Expose a typed check while redacting private values before failure recording."""
    with recorder.check(check_id, summary) as evidence:
        try:
            yield evidence
        except Exception as exc:
            original = str(exc)
            redacted = _redacted_text(original, forbidden_values)
            if redacted == original:
                raise
            raise RelayError(f"secure runtime operation failed: {redacted}") from None


def _configured_runtime_secret(
    *,
    explicit: str | None,
    environment_name: str,
    label: str,
) -> str:
    """Resolve one required runtime transport secret without echoing its value."""
    value = explicit if explicit is not None else os.environ.get(environment_name)
    if not value:
        raise ConfigurationError(
            f"secure runtime acceptance requires {label} in {environment_name}"
        )
    return value


@contextmanager
def _isolated_runtime_child_environment(
    *,
    token_name: str,
    token: str,
    secret_name: str,
    secret: str,
) -> Generator[dict[str, str]]:
    """Yield explicit transport values for one packaged child without parent mutation."""
    yield {token_name: token, secret_name: secret}


def _packaged_mcp_structured_result(
    session: PackagedMcpStdioSession,
    *,
    expected_tool: str,
) -> dict[str, Any]:
    """Validate one packaged MCP call and return its exact structured content."""
    tools_result = session.tools_list_response.get("result")
    if not isinstance(tools_result, dict):
        raise RelayError("packaged MCP tools/list omitted its result")
    tools_value = cast(dict[str, object], tools_result).get("tools")
    tools = cast(list[object], tools_value) if isinstance(tools_value, list) else []
    advertised_names = {
        cast(dict[str, object], tool).get("name") for tool in tools if isinstance(tool, dict)
    }
    if expected_tool not in advertised_names:
        raise RelayError(f"packaged MCP did not advertise required tool {expected_tool}")
    if "error" in session.tools_call_response:
        error_value = session.tools_call_response.get("error")
        error = cast(dict[str, object], error_value) if isinstance(error_value, dict) else {}
        message = error.get("message")
        raise RelayError(
            f"packaged MCP {expected_tool} failed: "
            f"{message if isinstance(message, str) else 'unknown error'}"
        )
    raw_result = session.tools_call_response.get("result")
    if not isinstance(raw_result, dict):
        raise RelayError(f"packaged MCP {expected_tool} omitted its result")
    result = cast(dict[str, Any], raw_result)
    if result.get("isError") is True:
        raise RelayError(f"packaged MCP {expected_tool} returned isError=true")
    structured = result.get("structuredContent")
    if not isinstance(structured, dict):
        raise RelayError(f"packaged MCP {expected_tool} omitted structuredContent")
    content_value = result.get("content")
    content = cast(list[object], content_value) if isinstance(content_value, list) else []
    if len(content) != 1:
        raise RelayError(f"packaged MCP {expected_tool} returned invalid text content")
    item = content[0]
    text = cast(dict[str, object], item).get("text") if isinstance(item, dict) else None
    if not isinstance(text, str):
        raise RelayError(f"packaged MCP {expected_tool} returned invalid text content")
    text_document = decode_strict_json(
        text,
        label=f"packaged MCP {expected_tool} text",
    )
    if text_document != structured:
        raise RelayError(f"packaged MCP {expected_tool} text and structured content differ")
    return {str(key): value for key, value in cast(dict[object, object], structured).items()}


def _packaged_mcp_acceptance_evidence(
    session: PackagedMcpStdioSession,
    *,
    expected_tool: str,
) -> PackagedMcpAcceptanceEvidence:
    """Recheck and copy identities observed by the installed MCP child process."""
    initialize_result = session.initialize_response.get("result")
    if not isinstance(initialize_result, dict):
        raise RelayError("packaged MCP initialize omitted its result")
    raw_server_info = cast(dict[str, object], initialize_result).get("serverInfo")
    if not isinstance(raw_server_info, dict):
        raise RelayError("packaged MCP initialize omitted observed serverInfo")
    server_info = cast(dict[str, object], raw_server_info)
    server_name = server_info.get("name")
    server_version = server_info.get("version")
    if server_name != "clio-relay" or not isinstance(server_version, str):
        raise RelayError("packaged MCP initialize returned invalid observed serverInfo")
    tools_result = session.tools_list_response.get("result")
    if not isinstance(tools_result, dict):
        raise RelayError("packaged MCP tools/list omitted its result")
    raw_tools = cast(dict[str, object], tools_result).get("tools")
    tools = cast(list[object], raw_tools) if isinstance(raw_tools, list) else []
    typed_tools = [cast(dict[str, Any], item) for item in tools if isinstance(item, dict)]
    selected = [tool for tool in typed_tools if tool.get("name") == expected_tool]
    if len(selected) != 1:
        raise RelayError("packaged MCP observed tool schema was not unique")
    configured = session.configured_executable
    canonical = session.canonical_executable
    digests = {
        "executable_sha256": session.executable_sha256,
        "server_info_sha256": session.server_info_sha256,
        "tools_list_sha256": session.tools_list_sha256,
        "called_tool_schema_sha256": session.called_tool_schema_sha256,
        "jarvis_virtual_tools_sha256": session.jarvis_virtual_tools_sha256,
    }
    if not configured or not canonical:
        raise RelayError("packaged MCP omitted its observed executable identity")
    containment_mode = session.containment_mode
    if (
        containment_mode not in {"windows_job_object", "linux_systemd_scope"}
        or not session.containment_enforceable
    ):
        raise RelayError("packaged MCP process containment was not enforceable")
    if not session.command or session.command[0] != canonical:
        raise RelayError("packaged MCP command did not use its observed canonical executable")
    if any(
        not isinstance(digest, str) or re.fullmatch(r"[0-9a-f]{64}", digest) is None
        for digest in digests.values()
    ):
        raise RelayError("packaged MCP omitted an observed contract digest")
    if session.server_info_sha256 != _packaged_mcp_canonical_sha256(server_info):
        raise RelayError("packaged MCP observed serverInfo digest changed")
    if session.tools_list_sha256 != _packaged_mcp_tools_sha256(typed_tools):
        raise RelayError("packaged MCP observed tools/list digest changed")
    if session.called_tool_schema_sha256 != _packaged_mcp_canonical_sha256(selected[0]):
        raise RelayError("packaged MCP observed called-tool schema digest changed")
    return PackagedMcpAcceptanceEvidence(
        command=list(session.command),
        configured_executable=configured,
        canonical_executable=canonical,
        executable_sha256=cast(str, session.executable_sha256),
        server_name="clio-relay",
        server_version=server_version,
        server_info_sha256=cast(str, session.server_info_sha256),
        tools_list_sha256=cast(str, session.tools_list_sha256),
        called_tool_schema_sha256=cast(str, session.called_tool_schema_sha256),
        jarvis_virtual_tools_sha256=cast(str, session.jarvis_virtual_tools_sha256),
        containment_mode=cast(
            Literal["windows_job_object", "linux_systemd_scope"],
            containment_mode,
        ),
        containment_enforceable=True,
    )


def _packaged_mcp_canonical_sha256(value: object) -> str:
    """Reproduce the packaged MCP helper's canonical contract digest."""
    payload = json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _packaged_mcp_tools_sha256(tools: list[dict[str, Any]]) -> str:
    """Digest the exact sorted tools/list contract observed from stdio."""
    ordered = sorted(tools, key=lambda definition: cast(str, definition.get("name")))
    return _packaged_mcp_canonical_sha256({"tools": ordered})


def _select_secure_runtime_handoff(
    query_result: dict[str, Any],
    *,
    cluster: str,
    config: SecureRuntimeProbeConfig,
) -> JarvisServiceRuntimeHandoff | None:
    """Select exactly one artifact-bound service handoff using configured identities."""
    if query_result.get("terminal") is not True or query_result.get("state") != "succeeded":
        raise RelayError("JARVIS execution query did not complete successfully")
    if query_result.get("cluster") != cluster:
        raise RelayError("JARVIS execution query changed cluster identity")
    _query_receipt_artifact_identity(query_result)
    raw_bindings = query_result.get("service_runtime_bindings")
    if not isinstance(raw_bindings, list):
        raise RelayError("JARVIS v3.5 execution query omitted service_runtime_bindings")
    if not raw_bindings:
        return None
    bindings: list[JarvisServiceRuntimeHandoff] = []
    for raw in cast(list[object], raw_bindings):
        try:
            binding = JarvisServiceRuntimeHandoff.model_validate(raw)
        except ValueError as exc:
            raise RelayError(f"JARVIS service runtime handoff was invalid: {exc}") from exc
        if binding.cluster != cluster:
            raise RelayError("JARVIS service runtime handoff changed cluster identity")
        if binding.package_name != config.package_name:
            continue
        if config.package_id is not None and binding.package_id != config.package_id:
            continue
        if (
            config.service_instance_id is not None
            and binding.service_instance_id != config.service_instance_id
        ):
            continue
        bindings.append(binding)
    if len(bindings) != 1:
        raise RelayError(
            "secure runtime selectors must identify exactly one ready service; "
            f"matched={len(bindings)}"
        )
    return bindings[0]


def _query_source_artifact_sha256(
    query_result: dict[str, Any],
    *,
    handoff: JarvisServiceRuntimeHandoff,
) -> str:
    """Bind the compact handoff to the same immutable private MCP artifact."""
    job_id, artifact_id, digest = _query_receipt_artifact_identity(query_result)
    if job_id != handoff.source_job_id:
        raise RelayError("service runtime handoff source job differs from its query receipt")
    if artifact_id != handoff.source_artifact_id:
        raise RelayError("service runtime handoff source artifact differs from its query receipt")
    return digest


def _query_receipt_artifact_identity(
    query_result: dict[str, Any],
) -> tuple[str, str, str]:
    """Validate one durable query receipt and its private result-artifact identity."""
    try:
        job_id = validate_durable_record_id(query_result.get("job_id"))
    except (TypeError, ValueError) as exc:
        raise RelayError("JARVIS execution query omitted a durable relay job identity") from exc
    raw_artifact = query_result.get("mcp_result_artifact")
    if not isinstance(raw_artifact, dict):
        raise RelayError("JARVIS execution query omitted mcp_result_artifact")
    artifact = cast(dict[str, object], raw_artifact)
    try:
        artifact_id = validate_durable_record_id(artifact.get("artifact_id"))
        artifact_job_id = validate_durable_record_id(artifact.get("job_id"))
    except (TypeError, ValueError) as exc:
        raise RelayError("JARVIS execution query artifact identity was invalid") from exc
    if artifact_job_id != job_id or artifact.get("kind") != "mcp_result":
        raise RelayError("JARVIS execution query artifact does not match its receipt")
    digest = artifact.get("sha256")
    if not isinstance(digest, str) or re.fullmatch(r"[0-9a-f]{64}", digest) is None:
        raise RelayError("service runtime source artifact omitted a canonical SHA-256")
    return job_id, artifact_id, digest


def _secure_runtime_cleanup_candidate(
    bind_result: dict[str, Any],
    *,
    handoff: JarvisServiceRuntimeHandoff,
) -> str:
    """Recover only the exact owned session identity safe to tear down after bind failure."""
    session_id = bind_result.get("gateway_session_id")
    gateway_value = bind_result.get("gateway_session")
    if not isinstance(session_id, str) or not isinstance(gateway_value, dict):
        raise RelayError("secure runtime bind omitted its cleanup identity")
    gateway = cast(dict[str, object], gateway_value)
    metadata_value = gateway.get("metadata")
    metadata = cast(dict[str, object], metadata_value) if isinstance(metadata_value, dict) else {}
    gateway_data_value = gateway.get("gateway")
    gateway_data = (
        cast(dict[str, object], gateway_data_value) if isinstance(gateway_data_value, dict) else {}
    )
    binding_value = gateway_data.get("jarvis_runtime_binding")
    binding = cast(dict[str, object], binding_value) if isinstance(binding_value, dict) else {}
    if (
        gateway.get("session_id") != session_id
        or gateway.get("cluster") != handoff.cluster
        or metadata.get("owner") != "clio-relay"
        or binding.get("source_relay_job_id") != handoff.source_job_id
        or binding.get("source_relay_artifact_id") != handoff.source_artifact_id
        or binding.get("package_id") != handoff.package_id
        or binding.get("package_name") != handoff.package_name
        or binding.get("service_instance_id") != handoff.service_instance_id
    ):
        raise RelayError("secure runtime bind did not prove an exact owned cleanup identity")
    return session_id


def _gateway_session_matches_handoff(
    session: GatewaySession,
    *,
    handoff: JarvisServiceRuntimeHandoff,
) -> bool:
    """Identify only a newly created owned gateway for the exact requested service."""
    binding_value = session.gateway.get("jarvis_runtime_binding")
    binding = cast(dict[str, object], binding_value) if isinstance(binding_value, dict) else {}
    return (
        session.cluster == handoff.cluster
        and session.metadata.get("owner") == "clio-relay"
        and binding.get("source_relay_job_id") == handoff.source_job_id
        and binding.get("source_relay_artifact_id") == handoff.source_artifact_id
        and binding.get("package_id") == handoff.package_id
        and binding.get("package_name") == handoff.package_name
        and binding.get("service_instance_id") == handoff.service_instance_id
    )


def _gateway_sessions_for_acceptance(
    queue: StorageManagedQueue,
    *,
    cluster: str,
) -> list[GatewaySession]:
    """Read one target's gateway records through bounded canonical pagination."""
    sessions: list[GatewaySession] = []
    cursor = 1
    while True:
        page, next_cursor, total = queue.list_gateway_sessions_page(
            cursor=cursor,
            limit=MAX_RESPONSE_PAGE_RECORDS,
            cluster=cluster,
        )
        sessions.extend(page)
        if total > MAX_ACCEPTANCE_COLLECTION_RECORDS or len(sessions) > total:
            raise RelayError(
                "secure runtime acceptance gateway inventory exceeded "
                f"{MAX_ACCEPTANCE_COLLECTION_RECORDS} records"
            )
        if next_cursor is None:
            return sessions
        if next_cursor <= cursor:
            raise RelayError("secure runtime acceptance gateway pagination did not advance")
        cursor = next_cursor


def _validated_secure_runtime_bind(
    bind_result: dict[str, Any],
    *,
    handoff: JarvisServiceRuntimeHandoff,
    expected_execution_id: str,
    expected_source_artifact_sha256: str,
) -> tuple[str, JarvisServiceRuntimeBinding]:
    """Validate the public v2 bind result without accepting caller-owned runtime data."""
    if bind_result.get("scheduler_cancel_requested") is not False:
        raise RelayError("secure runtime bind unexpectedly requested scheduler cancellation")
    gateway_session_id = bind_result.get("gateway_session_id")
    gateway = bind_result.get("gateway_session")
    if not isinstance(gateway_session_id, str) or not isinstance(gateway, dict):
        raise RelayError("secure runtime bind omitted its gateway identity")
    typed_gateway = cast(dict[str, Any], gateway)
    if (
        typed_gateway.get("session_id") != gateway_session_id
        or typed_gateway.get("cluster") != handoff.cluster
        or typed_gateway.get("state") != "ready"
    ):
        raise RelayError("secure runtime bind returned an inconsistent gateway")
    gateway_data = typed_gateway.get("gateway")
    if not isinstance(gateway_data, dict):
        raise RelayError("secure runtime gateway omitted its public binding")
    try:
        binding = JarvisServiceRuntimeBinding.model_validate(
            cast(dict[str, Any], gateway_data).get("jarvis_runtime_binding")
        )
    except ValueError as exc:
        raise RelayError(f"secure runtime public binding was invalid: {exc}") from exc
    if (
        binding.schema_version != RELAY_JARVIS_RUNTIME_BINDING_SCHEMA_V2
        or binding.service_runtime_schema_version != JARVIS_SERVICE_RUNTIME_SCHEMA_V2
        or binding.authorization_sha256 is None
        or binding.source_relay_job_id != handoff.source_job_id
        or binding.source_relay_artifact_id != handoff.source_artifact_id
        or binding.source_relay_artifact_sha256 != expected_source_artifact_sha256
        or binding.jarvis_execution_id != expected_execution_id
        or binding.package_id != handoff.package_id
        or binding.package_name != handoff.package_name
        or binding.service_instance_id != handoff.service_instance_id
        or binding.dataset_descriptor_sha256
        != _secure_runtime_canonical_json_sha256(binding.dataset_descriptor.model_dump(mode="json"))
    ):
        raise RelayError("secure runtime public binding changed its exact handoff identity")
    for key in (
        "connect_url",
        "health_url",
        "stream_url",
        "events_url",
        "state_url",
        "command_url",
    ):
        value = bind_result.get(key)
        if not isinstance(value, str):
            raise RelayError(f"secure runtime bind omitted {key}")
        parsed = urllib.parse.urlsplit(value)
        if (
            parsed.scheme not in {"http", "https"}
            or parsed.hostname != "127.0.0.1"
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
        ):
            raise RelayError(f"secure runtime public {key} is not a clean loopback URL")
    _assert_secret_free_document(bind_result, forbidden_values=set(), label="secure runtime bind")
    return gateway_session_id, binding


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


def _correlate_secure_runtime_browser_document(
    document: dict[str, Any],
    observation: SecureRuntimeHttpEvidence,
    *,
    endpoint: Literal["health", "state", "command", "events"],
    adapter: SecureRuntimeEndpointAdapter,
    expected_service_instance_id: str,
    expected_execution_id: str,
    expected_dataset_descriptor_sha256: str,
    expected_command_id: str | None,
) -> tuple[SecureRuntimeHttpEvidence, int]:
    """Apply an application-owned adapter and bind selected values to relay identity."""
    for pointer, expected in adapter.assertions.items():
        observed = _secure_runtime_json_pointer_value(
            document,
            pointer,
            label=f"browser {endpoint} assertion",
        )
        if type(observed) is not type(expected) or observed != expected:
            raise RelayError(f"secure runtime browser {endpoint} assertion did not match")
    service_instance_id = _secure_runtime_json_pointer_value(
        document,
        adapter.service_instance_id_pointer,
        label=f"browser {endpoint} service identity",
    )
    if service_instance_id != expected_service_instance_id:
        raise RelayError(f"secure runtime browser {endpoint} changed service identity")
    revision = _secure_runtime_json_pointer_value(
        document,
        adapter.revision_pointer,
        label=f"browser {endpoint} revision",
    )
    if not isinstance(revision, int) or isinstance(revision, bool) or revision < 0:
        raise RelayError(f"secure runtime browser {endpoint} omitted a valid revision")
    execution_id: str | None = None
    if adapter.execution_id_pointer is not None:
        selected_execution_id = _secure_runtime_json_pointer_value(
            document,
            adapter.execution_id_pointer,
            label=f"browser {endpoint} execution identity",
        )
        if selected_execution_id != expected_execution_id:
            raise RelayError(f"secure runtime browser {endpoint} changed execution identity")
        execution_id = expected_execution_id
    dataset_descriptor_sha256: str | None = None
    if adapter.dataset_descriptor_pointer is not None:
        descriptor = _secure_runtime_json_pointer_value(
            document,
            adapter.dataset_descriptor_pointer,
            label=f"browser {endpoint} dataset descriptor",
        )
        try:
            dataset_descriptor_sha256 = _secure_runtime_canonical_json_sha256(descriptor)
        except (TypeError, ValueError) as exc:
            raise RelayError(
                f"secure runtime browser {endpoint} dataset descriptor was not finite JSON"
            ) from exc
        if dataset_descriptor_sha256 != expected_dataset_descriptor_sha256:
            raise RelayError(f"secure runtime browser {endpoint} changed dataset identity")
    command_id: str | None = None
    if adapter.command_id_pointer is not None:
        selected_command_id = _secure_runtime_json_pointer_value(
            document,
            adapter.command_id_pointer,
            label=f"browser {endpoint} command identity",
        )
        if not isinstance(selected_command_id, str) or not selected_command_id:
            raise RelayError(f"secure runtime browser {endpoint} omitted command identity")
        command_id = selected_command_id
        if expected_command_id is not None and command_id != expected_command_id:
            raise RelayError(f"secure runtime browser {endpoint} changed command identity")
    return (
        observation.model_copy(
            update={
                "service_instance_id": expected_service_instance_id,
                "execution_id": execution_id,
                "dataset_descriptor_sha256": dataset_descriptor_sha256,
                "command_id": command_id,
                "revision": revision,
            }
        ),
        revision,
    )


def _browser_attachment_capability(grant: BrowserAttachmentGrant) -> str:
    """Require one identical one-time capability across every loopback attachment URL."""
    capabilities: set[str] = set()
    for value in (
        grant.connect_url,
        grant.health_url,
        grant.stream_url,
        grant.events_url,
        grant.state_url,
        grant.command_url,
    ):
        parsed = urllib.parse.urlsplit(value)
        query = urllib.parse.parse_qs(parsed.query, strict_parsing=True)
        if (
            parsed.scheme != "http"
            or parsed.hostname != "127.0.0.1"
            or parsed.username is not None
            or parsed.password is not None
            or parsed.fragment
            or set(query) != {"capability"}
            or len(query["capability"]) != 1
            or not query["capability"][0]
        ):
            raise RelayError("browser attachment returned an invalid capability URL")
        capabilities.add(query["capability"][0])
    if len(capabilities) != 1:
        raise RelayError("browser attachment URLs did not share one exact capability")
    capability = next(iter(capabilities))
    if re.fullmatch(r"[0-9a-f]{64}", capability) is None:
        raise RelayError("browser attachment did not return one 256-bit capability")
    return capability


def _browser_json_observation(
    url: str,
    *,
    endpoint: Literal["health", "state", "command"],
    method: Literal["GET", "POST"],
    body: dict[str, Any] | None,
    timeout_seconds: float,
) -> tuple[SecureRuntimeHttpEvidence, dict[str, Any]]:
    """Call one sandbox-browser JSON surface without persisting its capability URL."""
    encoded: bytes | None = None
    headers = {"Accept": "application/json", "Origin": "null"}
    if body is not None:
        encoded = _canonical_finite_json_bytes(body)
        headers["Content-Type"] = "application/json"
    response = _direct_browser_http_request(
        url,
        method=method,
        headers=headers,
        body=encoded,
        timeout_seconds=timeout_seconds,
        maximum_bytes=MAX_SECURE_RUNTIME_RESPONSE_BYTES,
        stop_after_sse_event=False,
    )
    _require_media_type(response.content_type, expected="application/json")
    decoded = _strict_finite_json(response.payload, label=f"browser {endpoint} response")
    if not isinstance(decoded, dict):
        raise RelayError(f"secure runtime browser {endpoint} response was not an object")
    document = {str(key): value for key, value in cast(dict[object, object], decoded).items()}
    observation = SecureRuntimeHttpEvidence(
        endpoint=endpoint,
        method=method,
        status_code=response.status_code,
        content_type=response.content_type[:256],
        body_sha256=hashlib.sha256(response.payload).hexdigest(),
        body_bytes=len(response.payload),
    )
    return observation, document


def _browser_sse_observation(
    url: str,
    *,
    timeout_seconds: float,
    expected_event_name: str,
) -> tuple[SecureRuntimeHttpEvidence, dict[str, Any]]:
    """Read exactly one bounded SSE event over a fresh browser-capability connection."""
    response = _direct_browser_http_request(
        url,
        method="GET",
        headers={"Accept": "text/event-stream", "Origin": "null"},
        body=None,
        timeout_seconds=timeout_seconds,
        maximum_bytes=MAX_SECURE_RUNTIME_SSE_EVENT_BYTES,
        stop_after_sse_event=True,
    )
    _require_media_type(response.content_type, expected="text/event-stream")
    document = _strict_sse_data_document(
        response.payload,
        expected_event_name=expected_event_name,
    )
    return (
        SecureRuntimeHttpEvidence(
            endpoint="events",
            method="GET",
            status_code=response.status_code,
            content_type=response.content_type[:256],
            body_sha256=hashlib.sha256(response.payload).hexdigest(),
            body_bytes=len(response.payload),
        ),
        document,
    )


def _direct_browser_http_request(
    url: str,
    *,
    method: Literal["GET", "POST"],
    headers: dict[str, str],
    body: bytes | None,
    timeout_seconds: float,
    maximum_bytes: int,
    stop_after_sse_event: bool,
) -> _BrowserHttpResponse:
    """Issue one direct loopback request with an absolute wall-clock deadline."""
    if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
        raise RelayError("secure runtime browser timeout must be positive and finite")
    target, port = _direct_browser_http_target(url)
    deadline = time.monotonic() + timeout_seconds
    expired = threading.Event()
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=timeout_seconds)

    def abort_at_deadline() -> None:
        expired.set()
        active_socket = connection.sock
        if active_socket is not None:
            with suppress(OSError):
                active_socket.shutdown(socket.SHUT_RDWR)
        connection.close()

    timer = threading.Timer(timeout_seconds, abort_at_deadline)
    timer.daemon = True
    timer.start()
    try:
        connection.request(
            method,
            target,
            body=body,
            headers={**headers, "Connection": "close"},
        )
        response = connection.getresponse()
        if response.status < 200 or response.status > 299:
            raise _BrowserHttpRequestError(
                f"secure runtime browser request returned HTTP {response.status}",
                kind=f"http_{response.status}",
            )
        content_type_values = response.headers.get_all("Content-Type", failobj=[])
        if len(content_type_values) != 1:
            raise _BrowserHttpRequestError(
                "secure runtime browser response requires one Content-Type header",
                kind="protocol",
            )
        content_length_values = response.headers.get_all("Content-Length", failobj=[])
        if len(content_length_values) > 1:
            raise _BrowserHttpRequestError(
                "secure runtime browser response repeated Content-Length",
                kind="protocol",
            )
        transfer_encoding_values = response.headers.get_all("Transfer-Encoding", failobj=[])
        if (
            len(transfer_encoding_values) > 1
            or (transfer_encoding_values and transfer_encoding_values[0].casefold() != "chunked")
            or (transfer_encoding_values and content_length_values)
        ):
            raise _BrowserHttpRequestError(
                "secure runtime browser response had ambiguous transfer framing",
                kind="protocol",
            )
        if content_length_values:
            try:
                content_length = int(content_length_values[0])
            except ValueError as exc:
                raise _BrowserHttpRequestError(
                    "secure runtime browser response had invalid Content-Length",
                    kind="protocol",
                ) from exc
            if content_length < 0 or content_length > maximum_bytes:
                raise _BrowserHttpRequestError(
                    "secure runtime browser response exceeded its byte limit",
                    kind="flood",
                )
        payload = _read_browser_http_body(
            connection,
            response,
            deadline=deadline,
            maximum_bytes=maximum_bytes,
            stop_after_sse_event=stop_after_sse_event,
        )
        return _BrowserHttpResponse(
            status_code=int(response.status),
            content_type=str(content_type_values[0]),
            payload=payload,
        )
    except _BrowserHttpRequestError:
        raise
    except TimeoutError as exc:
        raise _BrowserHttpRequestError(
            "secure runtime browser request exceeded its absolute deadline",
            kind="deadline",
        ) from exc
    except (ConnectionRefusedError, ConnectionResetError, BrokenPipeError) as exc:
        kind = (
            "connection_refused" if isinstance(exc, ConnectionRefusedError) else "connection_reset"
        )
        raise _BrowserHttpRequestError(
            "secure runtime browser loopback proxy was unavailable",
            kind=kind,
        ) from exc
    except (OSError, http.client.HTTPException) as exc:
        if expired.is_set() or time.monotonic() >= deadline:
            raise _BrowserHttpRequestError(
                "secure runtime browser request exceeded its absolute deadline",
                kind="deadline",
            ) from exc
        error_number = getattr(exc, "winerror", None) or getattr(exc, "errno", None)
        if error_number in {61, 104, 111, 10054, 10061}:
            kind = "connection_refused" if error_number in {61, 111, 10061} else "connection_reset"
            raise _BrowserHttpRequestError(
                "secure runtime browser loopback proxy was unavailable",
                kind=kind,
            ) from exc
        raise _BrowserHttpRequestError(
            "secure runtime browser request failed at its direct loopback transport",
            kind="transport",
        ) from exc
    finally:
        timer.cancel()
        connection.close()


def _direct_browser_http_target(url: str) -> tuple[str, int]:
    """Return a direct HTTP request target without consulting redirect or proxy settings."""
    parsed = urllib.parse.urlsplit(url)
    try:
        port = parsed.port
    except ValueError as exc:
        raise RelayError("secure runtime browser URL had an invalid port") from exc
    if (
        parsed.scheme != "http"
        or parsed.hostname != "127.0.0.1"
        or port is None
        or parsed.username is not None
        or parsed.password is not None
        or parsed.fragment
        or any(character in url for character in "\r\n\x00")
    ):
        raise RelayError("secure runtime browser request requires one clean loopback HTTP URL")
    path = parsed.path or "/"
    return path + (f"?{parsed.query}" if parsed.query else ""), port


def _read_browser_http_body(
    connection: http.client.HTTPConnection,
    response: http.client.HTTPResponse,
    *,
    deadline: float,
    maximum_bytes: int,
    stop_after_sse_event: bool,
) -> bytes:
    """Read bounded decoded HTTP bytes while recomputing the absolute deadline."""
    payload = bytearray()
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise _BrowserHttpRequestError(
                "secure runtime browser request exceeded its absolute deadline",
                kind="deadline",
            )
        if connection.sock is not None:
            connection.sock.settimeout(remaining)
        chunk = response.read1(min(8192, maximum_bytes + 1 - len(payload)))
        if not chunk:
            if time.monotonic() >= deadline:
                raise _BrowserHttpRequestError(
                    "secure runtime browser request exceeded its absolute deadline",
                    kind="deadline",
                )
            break
        payload.extend(chunk)
        if len(payload) > maximum_bytes:
            raise _BrowserHttpRequestError(
                "secure runtime browser response exceeded its byte limit",
                kind="flood",
            )
        if stop_after_sse_event:
            frame_end = _first_sse_frame_end(payload)
            if frame_end is not None:
                return bytes(payload[:frame_end])
    if stop_after_sse_event:
        raise _BrowserHttpRequestError(
            "secure runtime browser events response omitted a complete event",
            kind="protocol",
        )
    if not payload:
        raise _BrowserHttpRequestError(
            "secure runtime browser response body was empty",
            kind="protocol",
        )
    return bytes(payload)


def _first_sse_frame_end(payload: bytes | bytearray) -> int | None:
    endings = [
        index + len(marker)
        for marker in (b"\n\n", b"\r\n\r\n")
        if (index := payload.find(marker)) >= 0
    ]
    return min(endings) if endings else None


def _require_media_type(content_type: str, *, expected: str) -> None:
    """Require one exact media type with at most a UTF-8 charset parameter."""
    parts = [part.strip() for part in content_type.split(";")]
    if not parts or parts[0].casefold() != expected:
        raise RelayError(f"secure runtime browser response was not {expected}")
    parameters: dict[str, str] = {}
    for raw in parts[1:]:
        name, separator, value = raw.partition("=")
        normalized_name = name.strip().casefold()
        normalized_value = value.strip().strip('"').casefold()
        if (
            separator != "="
            or not normalized_name
            or normalized_name in parameters
            or normalized_name != "charset"
            or normalized_value != "utf-8"
        ):
            raise RelayError("secure runtime browser response had invalid media-type parameters")
        parameters[normalized_name] = normalized_value


def _canonical_finite_json_bytes(value: object) -> bytes:
    try:
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise RelayError("secure runtime browser request body was not finite JSON") from exc


def _strict_finite_json(payload: bytes, *, label: str) -> object:
    """Decode UTF-8 JSON while rejecting duplicate keys and non-finite numbers."""
    try:
        return decode_strict_json(payload, label=f"secure runtime {label}")
    except RelayError:
        raise RelayError(f"secure runtime {label} was not strict finite JSON") from None


def _strict_sse_data_document(
    frame: bytes,
    *,
    expected_event_name: str,
) -> dict[str, Any]:
    """Require one complete SSE frame whose data field is a strict JSON object."""
    try:
        text = frame.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise RelayError("secure runtime browser SSE frame was not UTF-8") from exc
    normalized = text.replace("\r\n", "\n")
    if not normalized.endswith("\n\n"):
        raise RelayError("secure runtime browser SSE frame was incomplete")
    lines = normalized[:-2].split("\n")
    event_lines = [line[6:].lstrip(" ") for line in lines if line.startswith("event:")]
    if event_lines != [expected_event_name]:
        raise RelayError("secure runtime browser SSE event name did not match its adapter")
    data_lines = [line[5:].lstrip(" ") for line in lines if line.startswith("data:")]
    if not data_lines:
        raise RelayError("secure runtime browser SSE frame omitted data")
    decoded = _strict_finite_json("\n".join(data_lines).encode("utf-8"), label="SSE data")
    if not isinstance(decoded, dict):
        raise RelayError("secure runtime browser SSE data was not an object")
    return {str(key): value for key, value in cast(dict[object, object], decoded).items()}


def _wait_for_changed_sse_event(
    url: str,
    *,
    previous: SecureRuntimeHttpEvidence,
    require_change: bool,
    timeout_seconds: float,
    poll_seconds: float,
    expected_event_name: str,
) -> tuple[SecureRuntimeHttpEvidence, dict[str, Any]]:
    """Reconnect to SSE until the configured command produces a new event digest."""
    deadline = time.monotonic() + timeout_seconds
    while True:
        remaining = max(0.001, deadline - time.monotonic())
        observed, document = _browser_sse_observation(
            url,
            timeout_seconds=min(remaining, 10.0),
            expected_event_name=expected_event_name,
        )
        if not require_change or observed.body_sha256 != previous.body_sha256:
            return observed, document
        if time.monotonic() >= deadline:
            raise RelayError("secure runtime SSE did not change after its command")
        time.sleep(min(poll_seconds, max(0.001, deadline - time.monotonic())))


def _wait_for_changed_browser_state(
    url: str,
    *,
    previous: SecureRuntimeHttpEvidence,
    require_change: bool,
    timeout_seconds: float,
    poll_seconds: float,
) -> tuple[SecureRuntimeHttpEvidence, dict[str, Any]]:
    """Poll browser state until the configured command is durably observable."""
    deadline = time.monotonic() + timeout_seconds
    while True:
        remaining = max(0.001, deadline - time.monotonic())
        observed, document = _browser_json_observation(
            url,
            endpoint="state",
            method="GET",
            body=None,
            timeout_seconds=min(remaining, 10.0),
        )
        if not require_change or observed.body_sha256 != previous.body_sha256:
            return observed, document
        if time.monotonic() >= deadline:
            raise RelayError("secure runtime state did not change after its command")
        time.sleep(min(poll_seconds, max(0.001, deadline - time.monotonic())))


def _browser_evidence_reference(
    attachment_id: str,
    observation: SecureRuntimeHttpEvidence,
) -> EvidenceReference:
    """Project a browser observation without including its one-time capability URL."""
    return EvidenceReference(
        kind="secure_runtime_browser_http",
        reference=f"browser-attachment://{attachment_id}/{observation.endpoint}",
        sha256=observation.body_sha256,
        metadata=observation.model_dump(mode="json"),
    )


def _assert_browser_capability_revoked(
    url: str,
    *,
    timeout_seconds: float,
    proxy_stopped: bool,
) -> None:
    """Require explicit denial, or a proven-stopped loopback proxy, for an old grant."""
    try:
        _direct_browser_http_request(
            url,
            method="GET",
            headers={"Accept": "application/json", "Origin": "null"},
            body=None,
            timeout_seconds=max(timeout_seconds, 0.1),
            maximum_bytes=1,
            stop_after_sse_event=False,
        )
    except _BrowserHttpRequestError as exc:
        if exc.kind in {"http_401", "http_403", "http_410"}:
            return
        if proxy_stopped and exc.kind in {"connection_refused", "connection_reset"}:
            return
        raise RelayError(
            f"revoked browser capability failed with non-revocation cause {exc.kind}"
        ) from exc
    raise RelayError("revoked browser capability remained usable")


def _validate_secure_runtime_cleanup(
    result: ServiceRuntimeStopResult,
    *,
    expected_mode: Literal["detach", "teardown"],
    expected_session_id: str,
) -> None:
    """Require exact owned cleanup with scheduler preservation as the default."""
    expected_state = (
        GatewaySessionState.DEGRADED if expected_mode == "detach" else GatewaySessionState.CLOSED
    )
    if (
        result.mode != expected_mode
        or result.session.session_id != expected_session_id
        or result.session.state is not expected_state
        or result.errors
        or result.residual_resources
        or result.canceled_scheduler_job is not None
    ):
        raise RelayError(f"secure runtime {expected_mode} did not complete cleanly")
    if any(resource.action == "cancel" for resource in result.resources):
        raise RelayError(f"secure runtime {expected_mode} requested cancellation")
    report = result.to_live_validation_report()
    if report.status.value != "passed" or report.cleanup.cancel_scheduler_jobs:
        raise RelayError(f"secure runtime {expected_mode} evidence did not pass")


def _record_runtime_cleanup(
    recorder: ValidationRecorder,
    result: ServiceRuntimeStopResult,
    *,
    role: str,
) -> None:
    """Merge one secret-free runtime cleanup operation into the canonical report."""
    recorder.report.cleanup.requested = True
    recorder.report.cleanup.mode = "secure_runtime_detach_reconnect_teardown"
    recorder.report.cleanup.cancel_scheduler_jobs = False
    for resource in result.resources:
        raw = resource.to_validation_resource(cluster=result.session.cluster).model_dump(
            mode="json"
        )
        public = redact_sensitive_values(raw)
        if not isinstance(public, dict):
            raise RelayError("secure runtime cleanup projection was invalid")
        parsed_resource = ValidationResource.model_validate(public)
        validation_resource = parsed_resource.model_copy(
            update={
                "role": role,
                "metadata": {
                    **parsed_resource.metadata,
                    "cancel_scheduler_job": False,
                    "cleanup_action": resource.action,
                    "cleanup_outcome": resource.outcome,
                    "evidence_scope": "clio-relay-core-lifecycle-and-public-evidence",
                },
            }
        )
        recorder.add_resource(validation_resource)
        action = cast(dict[str, Any], redact_sensitive_values(resource.model_dump(mode="json")))
        action["phase"] = role
        if action not in recorder.report.cleanup.actions:
            recorder.report.cleanup.actions.append(action)
        if resource.residual:
            recorder.report.cleanup.remaining_resources.append(validation_resource)


def _assert_secret_free_document(
    document: object,
    *,
    forbidden_values: set[str],
    label: str,
) -> None:
    """Reject raw credentials, browser capabilities, or bearer material in public evidence."""
    rendered = json.dumps(document, ensure_ascii=False, sort_keys=True, default=str)
    if re.search(r"(?i)authorization\s*:\s*bearer\s+(?!<redacted>)", rendered):
        raise RelayError(f"{label} retained a bearer authorization value")
    if "?capability=" in rendered or "&capability=" in rendered:
        raise RelayError(f"{label} retained a browser capability URL")

    def visit(value: object, *, parent_key: str | None = None) -> None:
        if isinstance(value, dict):
            for raw_key, nested in cast(dict[object, object], value).items():
                key = str(raw_key)
                normalized = key.casefold().replace("-", "_").replace(".", "_")
                digest_key = normalized.endswith("_sha256") or normalized.endswith("_digest")
                sensitive_key = normalized in {
                    "authorization",
                    "capability",
                    "credential",
                    "credentials",
                    "password",
                    "private_key",
                    "secret",
                    "secret_key",
                    "token",
                } or normalized.endswith(
                    (
                        "_credential",
                        "_credentials",
                        "_authorization",
                        "_capability",
                        "_password",
                        "_private_key",
                        "_secret",
                        "_secret_key",
                        "_token",
                    )
                )
                if sensitive_key and not digest_key and nested != "<redacted>":
                    raise RelayError(f"{label} retained sensitive field {key}")
                visit(nested, parent_key=key)
        elif isinstance(value, list):
            for nested in cast(list[object], value):
                visit(nested, parent_key=parent_key)
        elif isinstance(value, str):
            if any(secret and secret in value for secret in forbidden_values):
                raise RelayError(f"{label} retained a private capability value")
            if re.search(r"(?i)\bbearer\s+(?!<redacted>)(?:\S+)", value):
                raise RelayError(f"{label} retained bearer authorization material")
            if parent_key == "authorization" and value not in {"<redacted>", "bearer"}:
                raise RelayError(f"{label} retained raw authorization material")

    visit(document)


def _redacted_text(value: str, forbidden_values: set[str]) -> str:
    """Remove known private values from a diagnostic before it reaches a report."""
    result = value
    for secret in sorted(forbidden_values, key=len, reverse=True):
        if secret:
            result = result.replace(secret, "<redacted>")
    return result


def _redacted_error_text(error: BaseException, forbidden_values: set[str]) -> str:
    return _redacted_text(f"{type(error).__name__}: {error}", forbidden_values)


def _redact_exception_values(error: BaseException, forbidden_values: set[str]) -> None:
    """Attach a safe diagnostic when an upstream exception may contain known capabilities."""
    safe = _redacted_error_text(error, forbidden_values)
    if safe != f"{type(error).__name__}: {error}":
        error.add_note(f"redacted secure runtime diagnostic: {safe}")


def _write_generated_agent_prompt(
    definition: ClusterDefinition,
    *,
    cluster: str,
    run_id: str,
    child_yaml: Path,
    runner: CommandRunner,
) -> str:
    remote_home = _remote_home(definition.ssh_host, runner=runner)
    remote_prompt = f"{remote_home}/.local/share/clio-relay/live-tests/{run_id}/agent-prompt.md"
    idempotency_key = f"live-test:{cluster}:{run_id}:agent-child"
    child_pipeline_yaml = _stage_acceptance_files(
        definition,
        jarvis_yaml=child_yaml,
        pipeline_yaml_text=child_yaml.read_text(encoding="utf-8"),
        run_id=f"{run_id}-agent-child",
        runner=runner,
    )
    prompt = _generated_agent_prompt(
        cluster=cluster,
        idempotency_key=idempotency_key,
        pipeline_yaml=child_pipeline_yaml,
    )
    _remote_write_file(
        definition.ssh_host,
        remote_prompt,
        prompt.encode("utf-8"),
        runner=runner,
    )
    return remote_prompt


def _remote_home(ssh_host: str, *, runner: CommandRunner) -> str:
    home = _remote_shell(ssh_host, 'printf "%s" "$HOME"', runner=runner).strip()
    if not home.startswith("/"):
        raise RelayError(f"remote HOME did not resolve to an absolute path: {home}")
    return home


def _generated_agent_prompt(
    *,
    cluster: str,
    idempotency_key: str,
    pipeline_yaml: str,
) -> str:
    return (
        "Use only the MCP tool named relay_submit_jarvis_pipeline. "
        "Do not use shell commands.\n\n"
        "Call relay_submit_jarvis_pipeline with:\n"
        f"- cluster: {cluster}\n"
        f"- idempotency_key: {idempotency_key}\n"
        "- pipeline_yaml: the exact YAML below\n\n"
        "After the tool returns, respond with only the relay job id.\n\n"
        "```yaml\n"
        f"{pipeline_yaml.rstrip()}\n"
        "```\n"
    )


def _wait_for_live_structured_runtime_metadata(
    definition: ClusterDefinition,
    job_id: str,
    *,
    line_prefix: str,
    lines: list[str],
    timeout_seconds: float,
    poll_seconds: float,
    runner: CommandRunner,
) -> RuntimeMetadataAcceptance:
    """Wait for trusted runtime metadata without waiting for its source job to finish."""
    deadline = time.monotonic() + timeout_seconds
    structured_sources = {
        RuntimeMetadataSource.JARVIS_MCP,
        RuntimeMetadataSource.JARVIS_SIDECAR,
    }
    while True:
        raw_status = _remote_clio_json(
            definition,
            ["job", "status", job_id],
            runner=runner,
        )
        if not isinstance(raw_status, dict):
            raise RelayError("secure runtime source job status was not a JSON object")
        status = cast(dict[str, Any], raw_status)
        raw_job = status.get("job")
        try:
            job = RelayJob.model_validate(raw_job)
        except ValueError as exc:
            raise RelayError(f"secure runtime source RelayJob was invalid: {exc}") from exc
        if job.job_id != job_id:
            raise RelayError(
                "secure runtime source job status changed identity: "
                f"expected={job_id} observed={job.job_id}"
            )
        reported_terminal = status.get("terminal")
        actual_terminal = job.state in TERMINAL_STATES
        if not isinstance(reported_terminal, bool) or reported_terminal is not actual_terminal:
            raise RelayError("secure runtime source job status had inconsistent terminal state")
        if actual_terminal:
            lines.append(f"{line_prefix}.job_state={job.state.value}")
            if job.state.value in {"failed", "canceled"}:
                raise RelayError(
                    "secure runtime source job "
                    f"{job.state.value} before structured runtime metadata was usable"
                )
            raise RelayError(
                "secure runtime source job succeeded before a live structured runtime was available"
            )

        raw_runtime = job.metadata.get("runtime_metadata")
        if raw_runtime is not None:
            if not isinstance(raw_runtime, dict):
                raise RelayError("secure runtime source metadata was not a JSON object")
            try:
                validated = JarvisRuntimeMetadata.model_validate(raw_runtime)
            except ValueError as exc:
                raise RelayError(f"secure runtime source metadata was invalid: {exc}") from exc
            if validated.schema_version != RUNTIME_METADATA_SCHEMA:
                raise RelayError(
                    "secure runtime source metadata used an unsupported schema version: "
                    f"{validated.schema_version}"
                )
            if validated.source in structured_sources:
                if not validated.pipeline_id or not validated.execution_id:
                    raise RelayError(
                        "secure runtime source metadata omitted pipeline_id or execution_id"
                    )
                if job.state is not JobState.RUNNING:
                    if time.monotonic() >= deadline:
                        lines.append(f"{line_prefix}.job_state={job.state.value}")
                        raise RelayError(
                            f"timed out waiting for the secure runtime source job to run: {job_id}"
                        )
                    time.sleep(poll_seconds)
                    continue
                document = validated.model_dump(mode="json")
                lines.append(f"{line_prefix}.job_state={job.state.value}")
                lines.extend(
                    _runtime_metadata_document_facts(
                        document,
                        line_prefix=line_prefix,
                    )
                )
                lines.append(f"{line_prefix}.source_job_retained=ok")
                return RuntimeMetadataAcceptance(document=document, structured=True)

        if time.monotonic() >= deadline:
            lines.append(f"{line_prefix}.job_state={job.state.value}")
            raise RelayError(
                "timed out waiting for structured runtime metadata from secure runtime "
                f"source job: {job_id}"
            )
        time.sleep(poll_seconds)


def _wait_for_success(
    definition: ClusterDefinition,
    job_id: str,
    *,
    timeout_seconds: float,
    poll_seconds: float,
    runner: CommandRunner,
) -> dict[str, Any]:
    job = _remote_clio_json(
        definition,
        [
            "job",
            "wait",
            job_id,
            "--timeout-seconds",
            str(timeout_seconds),
            "--poll-seconds",
            str(poll_seconds),
        ],
        runner=runner,
    )
    typed = cast(dict[str, Any], job)
    if typed["state"] != "succeeded":
        raise RelayError(f"acceptance job did not succeed: {typed['state']}")
    return typed


def _verify_live_package_progress(
    definition: ClusterDefinition,
    job_id: str,
    expected_adapter: str,
    *,
    package_name: str | None,
    timeout_seconds: float,
    poll_seconds: float,
    runner: CommandRunner,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    saw_running = False
    while time.monotonic() < deadline:
        monitor = _remote_clio_json(
            definition,
            ["job", "monitor", job_id, "--cursor", "1", "--limit", "500"],
            runner=runner,
        )
        events = cast(list[dict[str, Any]], monitor["events"])
        event_types = {str(event.get("event_type")) for event in events}
        saw_running = saw_running or "job.running" in event_types
        progress = _remote_job_collection(
            definition,
            ["job", "progress", job_id],
            record_key="progress",
            label=f"live package progress for {job_id}",
            runner=runner,
        )
        if _has_progress_adapter(
            progress,
            expected_adapter,
            job_id=job_id,
            package_name=package_name,
        ):
            if not saw_running and not _remote_job_has_event(
                definition,
                job_id,
                "job.running",
                runner=runner,
            ):
                raise RelayError("package progress was recorded before job.running")
            return
        if event_types & {"job.succeeded", "job.failed", "job.canceled"}:
            break
        time.sleep(poll_seconds)
    raise RelayError(
        f"expected live package progress before terminal job state: {expected_adapter}"
    )


def _remote_job_has_event(
    definition: ClusterDefinition,
    job_id: str,
    event_type: str,
    *,
    runner: CommandRunner,
) -> bool:
    monitor = _remote_clio_json(
        definition,
        ["job", "monitor", job_id, "--cursor", "1", "--limit", "1000"],
        runner=runner,
    )
    events = cast(list[dict[str, Any]], monitor["events"])
    return any(str(event.get("event_type")) == event_type for event in events)


def _verify_completed_job(
    definition: ClusterDefinition,
    job_id: str,
    *,
    line_prefix: str,
    lines: list[str],
    runner: CommandRunner,
    expected_progress_adapter: str | None = None,
    expected_progress_package: str | None = None,
    recorder: ValidationRecorder | None = None,
    require_structured_runtime_metadata: bool = False,
) -> RuntimeMetadataAcceptance | None:
    monitor = _remote_clio_json(
        definition,
        ["job", "monitor", job_id, "--cursor", "1", "--limit", "250"],
        runner=runner,
    )
    event_types = {event["event_type"] for event in cast(list[dict[str, Any]], monitor["events"])}
    required_events = {"job.queued", "job.running", "jarvis.started", "job.succeeded"}
    missing_events = required_events - event_types
    if missing_events:
        raise RelayError(f"acceptance job missing events: {sorted(missing_events)}")
    lines.append(f"{line_prefix}.events=ok")
    for scheduler_phase in ("pending", "allocated", "running", "completed"):
        if f"scheduler.{scheduler_phase}" in event_types:
            lines.append(f"scheduler.{scheduler_phase}=observed")

    task_items = _remote_job_collection(
        definition,
        ["job", "tasks", job_id],
        record_key="tasks",
        label=f"completed-job tasks for {job_id}",
        runner=runner,
    )
    if not task_items or not any(task["state"] == "succeeded" for task in task_items):
        raise RelayError("acceptance job missing succeeded task record")
    lines.append(f"{line_prefix}.tasks={len(task_items)}")
    if recorder is not None:
        for task in task_items:
            task_id = task.get("task_id")
            if isinstance(task_id, str):
                recorder.add_resource(
                    ValidationResource(
                        kind="relay_task",
                        resource_id=task_id,
                        role=line_prefix,
                        cluster=definition.name,
                        state=str(task.get("state")) if task.get("state") is not None else None,
                        metadata=(
                            cast(dict[str, Any], task["metadata"])
                            if isinstance(task.get("metadata"), dict)
                            else {}
                        ),
                    )
                )
        scheduler_items = monitor.get("scheduler", [])
        if isinstance(scheduler_items, list):
            for item in cast(list[object], scheduler_items):
                if not isinstance(item, dict):
                    continue
                scheduler = cast(dict[str, Any], item)
                scheduler_job_id = scheduler.get("scheduler_job_id")
                if not isinstance(scheduler_job_id, str):
                    continue
                recorder.add_resource(
                    ValidationResource(
                        kind="scheduler_job",
                        resource_id=scheduler_job_id,
                        role=line_prefix,
                        cluster=definition.name,
                        state=(
                            str(scheduler["phase"]) if scheduler.get("phase") is not None else None
                        ),
                        provider=(
                            str(scheduler["scheduler"])
                            if scheduler.get("scheduler") is not None
                            else None
                        ),
                        metadata=scheduler,
                    )
                )

    stdout = _remote_clio_json(
        definition,
        ["job", "read-log", job_id, "--stream", "stdout", "--offset", "0", "--limit", "200000"],
        runner=runner,
    )
    stderr = _remote_clio_json(
        definition,
        ["job", "read-log", job_id, "--stream", "stderr", "--offset", "0", "--limit", "200000"],
        runner=runner,
    )
    if int(stdout["next_offset"]) <= 0:
        raise RelayError("acceptance stdout log is empty")
    lines.append(f"{line_prefix}.stdout_bytes={stdout['next_offset']}")
    lines.append(f"{line_prefix}.stderr_bytes={stderr['next_offset']}")

    artifact_items = _remote_job_collection(
        definition,
        ["job", "list-artifacts", job_id],
        record_key="artifacts",
        label=f"completed-job artifacts for {job_id}",
        runner=runner,
    )
    artifact_kinds = {str(artifact["kind"]) for artifact in artifact_items}
    if not {"jarvis_pipeline", "stdout", "stderr", "provenance"}.issubset(artifact_kinds):
        raise RelayError(f"acceptance artifacts incomplete: {sorted(artifact_kinds)}")
    lines.append(f"{line_prefix}.artifacts={','.join(sorted(artifact_kinds))}")
    if recorder is not None:
        for artifact in artifact_items:
            artifact_id = artifact.get("artifact_id")
            if not isinstance(artifact_id, str):
                continue
            uri = artifact.get("uri")
            references = [str(uri)] if isinstance(uri, str) else []
            recorder.add_resource(
                ValidationResource(
                    kind="artifact",
                    resource_id=artifact_id,
                    role=str(artifact.get("kind", "unknown")),
                    cluster=definition.name,
                    references=references,
                    metadata=artifact,
                )
            )
            recorder.report.artifacts.append(
                EvidenceReference(
                    kind=str(artifact.get("kind", "artifact")),
                    reference=(
                        str(uri)
                        if isinstance(uri, str)
                        else f"relay-artifact://{definition.name}/{job_id}/{artifact_id}"
                    ),
                    sha256=(
                        str(artifact["sha256"]) if isinstance(artifact.get("sha256"), str) else None
                    ),
                )
            )

    stdout_artifact = next(artifact for artifact in artifact_items if artifact["kind"] == "stdout")
    artifact_payload = _remote_clio_json(
        definition,
        ["job", "read-artifact", str(stdout_artifact["artifact_id"])],
        runner=runner,
    )
    if artifact_payload.get("encoding") != "base64":
        raise RelayError("acceptance artifact payload was not base64 encoded")
    lines.append(f"{line_prefix}.artifact_read=ok")

    provenance_artifact = next(
        artifact for artifact in artifact_items if artifact["kind"] == "provenance"
    )
    provenance_payload = _remote_clio_json(
        definition,
        ["job", "read-artifact", str(provenance_artifact["artifact_id"])],
        runner=runner,
    )
    if provenance_payload.get("encoding") != "base64":
        raise RelayError("acceptance provenance payload was not base64 encoded")
    lines.append(f"{line_prefix}.provenance=ok")
    runtime_metadata = _verify_runtime_metadata_artifact(
        definition,
        artifact_items,
        line_prefix=line_prefix,
        lines=lines,
        runner=runner,
    )
    if require_structured_runtime_metadata and (
        runtime_metadata is None or not runtime_metadata.structured
    ):
        raise RelayError(
            "acceptance requires structured JARVIS runtime metadata, not a missing or "
            "legacy stdout-derived runtime artifact"
        )
    if expected_progress_adapter is not None:
        progress = _remote_job_collection(
            definition,
            ["job", "progress", job_id],
            record_key="progress",
            label=f"completed-job progress for {job_id}",
            runner=runner,
        )
        provider_metadata = _progress_provider_attestation(
            progress,
            expected_progress_adapter,
            job_id=job_id,
            package_name=expected_progress_package,
        )
        if provider_metadata is None:
            raise RelayError(
                f"expected package progress adapter was not recorded: {expected_progress_adapter}"
            )
        lines.append(f"{line_prefix}.progress_adapter={expected_progress_adapter}")
        lines.append("package-progress.provider=verified")
        lines.append("package-progress.acceptance=verified")
        lines.append(
            "package-progress.identity="
            f"{provider_metadata['provider_distribution']}:"
            f"{provider_metadata['provider_distribution_version']}:"
            f"{provider_metadata['provider_entry_point']}:"
            f"{provider_metadata['adapter']}"
        )
        if recorder is not None:
            recorder.add_resource(
                ValidationResource(
                    kind="package_progress_provider",
                    resource_id=(
                        f"{provider_metadata['provider_distribution']}:"
                        f"{provider_metadata['provider_distribution_version']}:"
                        f"{provider_metadata['provider_entry_point']}:"
                        f"{provider_metadata['adapter']}"
                    ),
                    role="jarvis_package_progress",
                    cluster=definition.name,
                    state="verified",
                    provider=str(provider_metadata["provider_distribution"]),
                    metadata=dict(provider_metadata),
                )
            )
    return runtime_metadata


def _verify_runtime_metadata_artifact(
    definition: ClusterDefinition,
    artifacts: list[dict[str, Any]],
    *,
    line_prefix: str,
    lines: list[str],
    runner: CommandRunner,
) -> RuntimeMetadataAcceptance | None:
    """Validate and report a normalized runtime metadata artifact when present."""
    runtime_artifact = next(
        (artifact for artifact in artifacts if artifact.get("kind") == "runtime_metadata"),
        None,
    )
    if runtime_artifact is None:
        return None
    artifact_id = runtime_artifact.get("artifact_id")
    if not isinstance(artifact_id, str) or not artifact_id:
        raise RelayError("runtime metadata artifact has no artifact id")
    payload = _remote_clio_json(
        definition,
        ["job", "read-artifact", artifact_id],
        runner=runner,
    )
    facts = _runtime_metadata_facts(
        payload,
        artifact_id=artifact_id,
        line_prefix=line_prefix,
    )
    lines.extend(facts)
    runtime = _decode_runtime_metadata_payload(payload)
    return RuntimeMetadataAcceptance(
        document=runtime,
        structured=f"{line_prefix}.structured_runtime_metadata=ok" in facts,
    )


def _runtime_metadata_facts(
    payload: dict[str, Any],
    *,
    artifact_id: str,
    line_prefix: str,
) -> list[str]:
    """Validate a runtime metadata payload and return report-ready facts."""
    runtime = _decode_runtime_metadata_payload(payload)
    return [
        f"{line_prefix}.runtime_metadata_artifact={artifact_id}",
        *_runtime_metadata_document_facts(runtime, line_prefix=line_prefix),
    ]


def _runtime_metadata_document_facts(
    runtime: dict[str, Any],
    *,
    line_prefix: str,
) -> list[str]:
    """Return report-ready facts for one already validated runtime document."""
    source = str(runtime["source"])
    facts = [f"{line_prefix}.runtime_metadata_source={source}"]
    structured_sources = {
        RuntimeMetadataSource.JARVIS_MCP.value,
        RuntimeMetadataSource.JARVIS_SIDECAR.value,
    }
    structured = source in structured_sources
    if structured:
        facts.append(f"{line_prefix}.structured_runtime_metadata=ok")
    else:
        compatibility_kind = (
            "legacy_fallback"
            if source == RuntimeMetadataSource.LEGACY_STDOUT.value
            else "untrusted_compatibility"
        )
        facts.append(f"runtime_metadata.compatibility={line_prefix}:{compatibility_kind}")
    raw_field_sources = runtime.get("field_sources")
    field_sources = (
        cast(dict[str, object], raw_field_sources) if isinstance(raw_field_sources, dict) else {}
    )
    provider = runtime.get("scheduler_provider")
    if isinstance(provider, str) and provider:
        facts.append(f"{line_prefix}.runtime_scheduler_provider={provider}")
    scheduler_job_id = runtime.get("scheduler_job_id")
    if isinstance(scheduler_job_id, str) and scheduler_job_id:
        facts.append(f"{line_prefix}.runtime_scheduler_job_id={scheduler_job_id}")
        scheduler_id_source = field_sources.get("scheduler_job_id")
        if isinstance(scheduler_id_source, str):
            facts.append(f"{line_prefix}.runtime_scheduler_job_id_source={scheduler_id_source}")
        provider_source = field_sources.get("scheduler_provider")
        if (
            structured
            and isinstance(provider, str)
            and provider
            and provider_source in structured_sources
            and scheduler_id_source in structured_sources
        ):
            facts.append(f"{line_prefix}.structured_runtime_scheduler_identity=ok")
    return facts


def _decode_runtime_metadata_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Decode and strictly validate one normalized runtime metadata artifact."""
    if payload.get("encoding") != "base64" or not isinstance(payload.get("data"), str):
        raise RelayError("runtime metadata artifact payload was not base64 encoded")
    try:
        decoded = json.loads(b64decode(cast(str, payload["data"])).decode("utf-8"))
    except (ValueError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RelayError(f"runtime metadata artifact was not valid JSON: {exc}") from exc
    if not isinstance(decoded, dict):
        raise RelayError("runtime metadata artifact was not an object")
    runtime = cast(dict[str, Any], decoded)
    if runtime.get("schema_version") != RUNTIME_METADATA_SCHEMA:
        raise RelayError("runtime metadata artifact has an unsupported schema")
    try:
        validated = JarvisRuntimeMetadata.model_validate(runtime)
    except ValueError as exc:
        raise RelayError(f"runtime metadata artifact was invalid: {exc}") from exc
    return validated.model_dump(mode="json")


def _expected_progress_adapter(pipeline_yaml: str) -> str | None:
    declaration = _expected_progress_declaration(pipeline_yaml)
    return declaration[0] if declaration is not None else None


def _secure_runtime_probe_config(pipeline_yaml: str) -> SecureRuntimeProbeConfig | None:
    """Read an acceptance-only secure runtime probe without forwarding it to JARVIS."""
    try:
        loaded = cast(object, yaml.safe_load(pipeline_yaml))
    except yaml.YAMLError as exc:
        raise ConfigurationError(f"live-test JARVIS YAML is invalid: {exc}") from exc
    if not isinstance(loaded, dict):
        return None
    extension = cast(dict[str, object], loaded).get("x_clio_relay")
    if extension is None:
        return None
    if not isinstance(extension, dict):
        raise ConfigurationError("x_clio_relay must be an object")
    raw_probe = cast(dict[str, object], extension).get("secure_runtime_probe")
    if raw_probe is None:
        return None
    try:
        return SecureRuntimeProbeConfig.model_validate(raw_probe)
    except ValueError as exc:
        raise ConfigurationError(f"x_clio_relay.secure_runtime_probe is invalid: {exc}") from exc


def _expected_progress_package(pipeline_yaml: str) -> str | None:
    declaration = _expected_progress_declaration(pipeline_yaml)
    return declaration[1] if declaration is not None else None


def _expected_progress_declaration(pipeline_yaml: str) -> tuple[str, str | None] | None:
    """Return the one explicitly selected package progress source, if any."""
    loaded = yaml.safe_load(pipeline_yaml)
    typed_document = cast(dict[str, Any], loaded) if isinstance(loaded, dict) else {}
    packages = typed_document.get("pkgs")
    if not isinstance(packages, list):
        return None
    typed_packages = cast(list[object], packages)
    declarations: list[tuple[str, str | None]] = []
    for package in typed_packages:
        if not isinstance(package, dict):
            continue
        typed_package = cast(dict[str, Any], package)
        progress = typed_package.get("progress")
        if not isinstance(progress, dict):
            continue
        typed_progress = cast(dict[str, Any], progress)
        adapter = typed_progress.get("adapter")
        if adapter is None or adapter == "none":
            continue
        if not isinstance(adapter, str) or not adapter:
            raise ConfigurationError("package progress.adapter must be a non-empty string")
        package_name = typed_package.get("pkg_type")
        declarations.append(
            (
                adapter,
                package_name if isinstance(package_name, str) and package_name else None,
            )
        )
    if len(declarations) > 1:
        raise ConfigurationError(
            "multiple pipeline packages declare progress; select exactly one package-owned "
            "progress source"
        )
    return declarations[0] if declarations else None


def _assert_progress_adapter(
    progress: list[dict[str, Any]],
    expected_adapter: str,
    *,
    job_id: str,
    package_name: str | None = None,
) -> None:
    if _has_progress_adapter(progress, expected_adapter, job_id=job_id, package_name=package_name):
        return
    raise RelayError(f"expected package progress adapter was not recorded: {expected_adapter}")


def _has_progress_adapter(
    progress: list[dict[str, Any]],
    expected_adapter: str,
    *,
    job_id: str,
    package_name: str | None = None,
) -> bool:
    return (
        _progress_provider_attestation(
            progress,
            expected_adapter,
            job_id=job_id,
            package_name=package_name,
        )
        is not None
    )


def _progress_provider_attestation(
    progress: list[dict[str, Any]],
    expected_adapter: str,
    *,
    job_id: str,
    package_name: str | None = None,
) -> dict[str, Any] | None:
    """Return one worker-stamped, provider-approved durable progress record."""
    for item in progress:
        current = item.get("current")
        if not isinstance(current, int | float) or isinstance(current, bool):
            continue
        numeric_current = float(current)
        if not math.isfinite(numeric_current) or numeric_current < 0:
            continue
        metadata = item.get("metadata")
        if not isinstance(metadata, dict):
            continue
        typed_metadata = cast(dict[str, Any], metadata)
        if (
            typed_metadata.get("adapter") == expected_adapter
            and typed_metadata.get("source") == "jarvis_package"
            and isinstance(typed_metadata.get("package_name"), str)
            and (package_name is None or typed_metadata.get("package_name") == package_name)
            and isinstance(typed_metadata.get("package_version"), str)
            and typed_metadata.get("run_id") == job_id
            and typed_metadata.get("execution_id") == job_id
        ):
            try:
                validate_package_progress_acceptance_metadata(typed_metadata)
            except ConfigurationError:
                continue
            return dict(typed_metadata)
    return None


def _verify_progress_monitor(
    definition: ClusterDefinition,
    job_id: str,
    *,
    pattern: str,
    action_payload: dict[str, object],
    lines: list[str],
    runner: CommandRunner,
) -> None:
    _remote_clio_json(
        definition,
        [
            "monitor",
            "add-regex",
            job_id,
            "--pattern",
            pattern,
            "--action",
            "record_progress",
            "--event-type",
            "stdout.delta",
            "--action-payload-json",
            json.dumps(action_payload, sort_keys=True, separators=(",", ":")),
        ],
        runner=runner,
    )
    actions = _remote_clio_json(
        definition,
        ["monitor", "run-once", "--limit", "250"],
        runner=runner,
    )
    action_items = cast(list[dict[str, Any]], actions)
    progress_actions = [
        action for action in action_items if action.get("action") == "record_progress"
    ]
    if not progress_actions:
        raise RelayError(f"acceptance progress pattern did not record progress: {pattern}")
    progress_items = _remote_job_collection(
        definition,
        ["job", "progress", job_id],
        record_key="progress",
        label=f"monitor progress for {job_id}",
        runner=runner,
    )
    if not progress_items:
        raise RelayError("acceptance progress records missing after monitor evaluation")
    lines.append(f"acceptance.progress={len(progress_items)}")


def _find_agent_child_job(
    definition: ClusterDefinition,
    agent_job_id: str,
    *,
    agent_created_at: str,
    runner: CommandRunner,
) -> str:
    artifact_items = _remote_job_collection(
        definition,
        ["job", "list-artifacts", agent_job_id],
        record_key="artifacts",
        label=f"agent artifacts for {agent_job_id}",
        runner=runner,
    )
    artifact_kinds = {str(artifact["kind"]) for artifact in artifact_items}
    if "agent_result" not in artifact_kinds:
        raise RelayError("acceptance agent job missing agent_result artifact")
    candidate_texts: list[str] = []
    for artifact in artifact_items:
        if str(artifact["kind"]) not in {"agent_last_message", "stdout", "agent_result"}:
            continue
        payload = _remote_clio_json(
            definition,
            ["job", "read-artifact", str(artifact["artifact_id"])],
            runner=runner,
        )
        candidate_texts.append(_decode_artifact_text(payload))
    stdout = _remote_clio_json(
        definition,
        [
            "job",
            "read-log",
            agent_job_id,
            "--stream",
            "stdout",
            "--offset",
            "0",
            "--limit",
            "200000",
        ],
        runner=runner,
    )
    candidate_texts.append(str(stdout.get("text", "")))
    child_job_ids = sorted(
        {
            match
            for text in candidate_texts
            for match in re.findall(r"\bjob_[0-9a-f]{32}\b", text)
            if match != agent_job_id
        }
    )
    if not child_job_ids:
        raise RelayError("acceptance agent did not report a child relay job id")
    agent_created = _parse_datetime(agent_created_at)
    stale_child_ids: list[str] = []
    for child_job_id in reversed(child_job_ids):
        child_created = _child_job_created_at(
            definition,
            child_job_id,
            runner=runner,
        )
        if child_created >= agent_created:
            return child_job_id
        stale_child_ids.append(child_job_id)
    raise RelayError(
        "acceptance agent only reported stale child relay jobs created before "
        f"the agent run: {stale_child_ids}"
    )


def _child_job_created_at(
    definition: ClusterDefinition,
    child_job_id: str,
    *,
    runner: CommandRunner,
) -> datetime:
    monitor = _remote_clio_json(
        definition,
        ["job", "monitor", child_job_id, "--cursor", "1", "--limit", "1"],
        runner=runner,
    )
    return _parse_datetime(str(monitor["job"]["created_at"]))


def _parse_datetime(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _decode_artifact_text(payload: dict[str, Any]) -> str:
    if payload.get("encoding") != "base64":
        raise RelayError("acceptance artifact payload was not base64 encoded")
    data = payload.get("data")
    if not isinstance(data, str):
        raise RelayError("acceptance artifact payload missing base64 data")
    return b64decode(data.encode("ascii")).decode("utf-8", errors="replace")


def _configured_path(value: str | None) -> Path | None:
    if value is None:
        return None
    return Path(value).expanduser()


def _acceptance_run_id(path: Path) -> str:
    digest = hashlib.sha256(path.read_bytes()).hexdigest()[:16]
    return f"{path.stem}-{digest}-{uuid4().hex[:8]}"


def _stage_acceptance_files(
    definition: ClusterDefinition,
    *,
    jarvis_yaml: Path,
    pipeline_yaml_text: str,
    run_id: str,
    runner: CommandRunner,
) -> str:
    loaded = cast(object, yaml.safe_load(pipeline_yaml_text))
    if not isinstance(loaded, dict):
        return pipeline_yaml_text
    document = cast(dict[str, object], loaded)
    relay_extension = document.pop("x_clio_relay", None)
    if relay_extension is None:
        return yaml.safe_dump(document, sort_keys=False)
    if not isinstance(relay_extension, dict):
        raise ConfigurationError("x_clio_relay must be an object")
    typed_extension = cast(dict[str, object], relay_extension)
    stage_files = typed_extension.get("stage_files", [])
    if not isinstance(stage_files, list):
        raise ConfigurationError("x_clio_relay.stage_files must be a list")
    for item in cast(list[object], stage_files):
        if not isinstance(item, dict):
            raise ConfigurationError("x_clio_relay.stage_files entries must be objects")
        typed_item = cast(dict[str, object], item)
        local_path_value = typed_item.get("local_path")
        remote_path_value = typed_item.get("remote_path")
        if not isinstance(local_path_value, str) or not isinstance(remote_path_value, str):
            raise ConfigurationError(
                "x_clio_relay.stage_files entries require local_path and remote_path strings"
            )
        local_path = Path(local_path_value)
        if not local_path.is_absolute():
            local_path = jarvis_yaml.parent / local_path
        if not local_path.exists():
            raise ConfigurationError(f"staged acceptance file does not exist: {local_path}")
        remote_path = remote_path_value.format(run_id=run_id)
        _remote_write_file(
            definition.ssh_host,
            remote_path,
            local_path.read_bytes(),
            runner=runner,
        )
    formatted_document = _format_run_id(document, run_id)
    return yaml.safe_dump(formatted_document, sort_keys=False)


def _format_run_id(value: object, run_id: str) -> object:
    if isinstance(value, str):
        return value.format(run_id=run_id)
    if isinstance(value, list):
        return [_format_run_id(item, run_id) for item in cast(list[object], value)]
    if isinstance(value, dict):
        typed = cast(dict[object, object], value)
        return {str(key): _format_run_id(item, run_id) for key, item in typed.items()}
    return value


def _remote_write_file(
    ssh_host: str,
    remote_path: str,
    data: bytes,
    *,
    runner: CommandRunner,
) -> None:
    mkdir_command = f"mkdir -p {shlex.quote(posixpath.dirname(remote_path))}"
    _remote_shell(ssh_host, mkdir_command, runner=runner)
    result = runner(["ssh", ssh_host, f"cat > {shlex.quote(remote_path)}"], input=data)
    if result.returncode != 0:
        raise RelayError(_command_error("remote file write failed", result))


def _remote_clio_json(
    definition: ClusterDefinition,
    args: list[str],
    *,
    runner: CommandRunner,
    raw_text: bool = False,
) -> Any:
    rendered_args = " ".join(shlex.quote(arg) for arg in args)
    output = _remote_shell(
        definition.ssh_host,
        f"{_remote_env(definition)} clio-relay {rendered_args}",
        runner=runner,
    )
    if raw_text:
        return output
    return json.loads(output)


def _remote_job_collection(
    definition: ClusterDefinition,
    command: list[str],
    *,
    record_key: str,
    label: str,
    runner: CommandRunner,
) -> list[dict[str, Any]]:
    """Drain an exact job-family page chain or reject incomplete acceptance evidence."""
    cursor = 1
    expected_total: int | None = None
    records: list[dict[str, Any]] = []
    while True:
        raw_payload = _remote_clio_json(
            definition,
            [
                *command,
                "--cursor",
                str(cursor),
                "--limit",
                str(MAX_RESPONSE_PAGE_RECORDS),
            ],
            runner=runner,
        )
        if not isinstance(raw_payload, dict):
            raise RelayError(f"{label} did not return a JSON object")
        payload = cast(dict[str, Any], raw_payload)
        raw_records = payload.get(record_key)
        if not isinstance(raw_records, list):
            raise RelayError(f"{label} did not return a {record_key} array")
        page: list[dict[str, Any]] = []
        for item in cast(list[object], raw_records):
            if not isinstance(item, dict):
                raise RelayError(f"{label} returned a non-object {record_key} entry")
            page.append(
                {str(key): value for key, value in cast(dict[object, object], item).items()}
            )
        total = payload.get("total")
        returned_cursor = payload.get("cursor")
        returned_limit = payload.get("limit")
        next_cursor = payload.get("next_cursor")
        if isinstance(total, bool) or not isinstance(total, int) or total < 0:
            raise RelayError(f"{label} returned an invalid total")
        if total > MAX_ACCEPTANCE_COLLECTION_RECORDS:
            raise RelayError(
                f"{label} exceeds the bounded completeness limit "
                f"{MAX_ACCEPTANCE_COLLECTION_RECORDS}"
            )
        if expected_total is not None and total != expected_total:
            raise RelayError(f"{label} changed during bounded discovery")
        expected_total = total
        if returned_cursor != cursor or returned_limit != MAX_RESPONSE_PAGE_RECORDS:
            raise RelayError(f"{label} returned inconsistent page metadata")
        if len(records) + len(page) > total:
            raise RelayError(f"{label} returned more records than its total")
        if next_cursor is not None and (
            isinstance(next_cursor, bool)
            or not isinstance(next_cursor, int)
            or not page
            or next_cursor != cursor + len(page)
            or next_cursor > total
        ):
            raise RelayError(f"{label} returned a non-contiguous next cursor")
        records.extend(page)
        if next_cursor is None:
            if len(records) != total:
                raise RelayError(f"{label} ended before its declared total")
            return records
        cursor = next_cursor


def _remote_shell(ssh_host: str, script: str, *, runner: CommandRunner) -> str:
    result = runner(["ssh", ssh_host, f"bash -lc {shlex.quote(script)}"])
    if result.returncode != 0:
        raise RelayError(_command_error("remote command failed", result))
    return result.stdout.decode("utf-8", errors="replace")


def _remote_env(definition: ClusterDefinition) -> str:
    jarvis_bin = definition.jarvis_bin or "$HOME/.local/bin/jarvis"
    frpc_bin = definition.frpc_bin or "$HOME/.local/bin/frpc"
    agent_bin = _cluster_agent_bin(definition)
    rendered_core_dir = render_remote_shell_path(definition.core_dir, field="core_dir")
    rendered_spool_dir = render_remote_shell_path(definition.spool_dir, field="spool_dir")
    rendered_jarvis_bin = render_remote_shell_value(jarvis_bin, field="jarvis_bin")
    rendered_frpc_bin = render_remote_shell_value(frpc_bin, field="frpc_bin")
    rendered_agent_bin = render_remote_shell_value(agent_bin, field="agent_bin")
    return " ".join(
        [
            'export PATH="$HOME/.local/bin:$PATH";',
            f"export CLIO_RELAY_CORE_DIR={rendered_core_dir};",
            f"export CLIO_RELAY_SPOOL_DIR={rendered_spool_dir};",
            f"export CLIO_RELAY_JARVIS_BIN={rendered_jarvis_bin};",
            f"export CLIO_RELAY_FRPC_BIN={rendered_frpc_bin};",
            f"export CLIO_RELAY_AGENT_BIN={rendered_agent_bin};",
            f"export CLIO_RELAY_AGENT_ADAPTER={shlex.quote(definition.agent_adapter)};",
        ]
    )


def _cluster_agent_bin(definition: ClusterDefinition) -> str:
    if definition.agent_bin is not None:
        return definition.agent_bin
    if definition.agent_npm_bin is not None:
        return f"$HOME/.local/bin/{definition.agent_npm_bin}"
    return "agent"


def _run_command(
    command: list[str],
    *,
    input: bytes | None = None,
) -> subprocess.CompletedProcess[bytes]:
    return subprocess.run(command, input=input, capture_output=True, check=False)


def _command_error(prefix: str, result: subprocess.CompletedProcess[bytes]) -> str:
    stdout = result.stdout.decode("utf-8", errors="replace").strip()
    stderr = result.stderr.decode("utf-8", errors="replace").strip()
    detail = stderr or stdout
    return f"{prefix}: {detail}"

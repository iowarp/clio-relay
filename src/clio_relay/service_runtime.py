"""Generic supervisor for scheduler-backed streaming service sessions."""

from __future__ import annotations

import base64
import hashlib
import json
import math
import os
import secrets
import shlex
import signal
import socket
import subprocess
import sys
import threading
import time
import urllib.parse
from collections.abc import Callable, Generator, Sequence
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Literal, Protocol, cast

import httpx
from filelock import FileLock
from filelock import Timeout as FileLockTimeout

from clio_relay.browser_gateway import (
    CAPABILITY_ENV,
    UPSTREAM_AUTHORIZATION_ENV,
    BrowserAttachmentGrant,
    BrowserAttachmentRecord,
    BrowserDetachmentResult,
    BrowserGatewayBootstrap,
    BrowserGatewayConfig,
)
from clio_relay.cluster_config import (
    ClusterDefinition,
    ensure_private_configuration_directory,
)
from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.errors import ConfigurationError, QueueConflictError, RelayError
from clio_relay.filesystem_paths import internal_filesystem_path
from clio_relay.jarvis_service_runtime import (
    JARVIS_SERVICE_RUNTIME_SCHEMA_V1,
    JARVIS_SERVICE_RUNTIME_SCHEMA_V2,
    JarvisServiceRuntimeBinding,
    VerifiedJarvisServiceRuntime,
    resolve_jarvis_service_runtime_authorization,
    reverify_jarvis_service_runtime,
)
from clio_relay.models import (
    GatewaySession,
    GatewaySessionState,
    SchedulerConnectorPlacement,
    SchedulerConnectorStepIdentity,
    SchedulerConnectorStepStatus,
    SchedulerPhase,
    SchedulerStatus,
    ServiceRuntimeSpec,
    utc_now,
)
from clio_relay.owner_session_admission import desktop_owner_session_admission_id
from clio_relay.public_records import public_gateway_payload
from clio_relay.relay_host import (
    FrpcConfig,
    FrpcVisitorConfig,
    FrpTransportProtocol,
    render_frpc_config,
    render_frpc_visitor_config,
)
from clio_relay.remote_cli import remote_env
from clio_relay.remote_values import render_remote_shell_value
from clio_relay.scheduler_providers import (
    SchedulerAllocationConnectorProvider,
    provider_for_scheduler,
)
from clio_relay.session_lifecycle import CleanupResource

if TYPE_CHECKING:
    from clio_relay.validation_report import (
        CleanupEvidence,
        LiveValidationReport,
        ValidationResource,
    )

RUNTIME_SUBMIT_CHECK_ID = "gateway.submit"
RUNTIME_ALLOCATED_CHECK_ID = "gateway.allocated"
RUNTIME_READY_CHECK_ID = "gateway.ready"
RUNTIME_CONNECT_CHECK_ID = "gateway.connect"
RUNTIME_DETACH_CHECK_ID = "gateway.detach-connectors"
RUNTIME_DETACHED_RECORD_CHECK_ID = "gateway.detached-record"
RUNTIME_TEARDOWN_CHECK_ID = "gateway.stop-connectors"
RUNTIME_SCHEDULER_RETAINED_CHECK_ID = "gateway.jobs-preserved-default"
RUNTIME_SCHEDULER_CANCELED_CHECK_ID = "gateway.scheduler-canceled"
RUNTIME_CLOSED_CHECK_ID = "gateway.closed-record"
_LOCAL_CONNECTOR_WRAPPER_CODE = (
    "import subprocess,sys; "
    "_owner_token=sys.argv[1]; "
    "_generation_id=sys.argv[2]; "
    "child=subprocess.Popen(sys.argv[3:]); "
    "raise SystemExit(child.wait())"
)
_OWNERSHIP_INTENT_SCHEMA = "clio-relay.gateway-ownership-intent.v1"
_MAX_SUBMISSION_OUTPUT_BYTES = 262_144
_MAX_LOCAL_HEALTH_BYTES = 64 * 1024
_GATEWAY_TEARDOWN_LOCK_TIMEOUT_SECONDS = 60.0
_GATEWAY_DETACH_INTENT_SCHEMA = "clio-relay.gateway-detach-intent.v1"
_GATEWAY_DETACH_RESULT_SCHEMA = "clio-relay.gateway-detach-result.v1"
_GATEWAY_TEARDOWN_POLICY_SCHEMA = "clio-relay.gateway-teardown-policy.v1"
_GATEWAY_TEARDOWN_RESULT_SCHEMA = "clio-relay.gateway-teardown-result.v1"
_REMOTE_RUNTIME_COMMAND_TIMEOUT_SECONDS = 120.0
_LOCAL_CLEANUP_COMMAND_TIMEOUT_SECONDS = 30.0
_CONNECTOR_STEP_CLEANUP_TIMEOUT_SECONDS = 30.0
_CONNECTOR_STEP_CLEANUP_POLL_SECONDS = 0.25
_TERMINAL_RUNTIME_STATES = {
    "canceled",
    "cancelled",
    "completed",
    "failed",
    "terminated",
    "timeout",
}
_ACTIVE_RUNTIME_STATES = {
    "submitted",
    "pending",
    "queued",
    "allocated",
    "starting",
    "ready",
    "running",
}
_CANCELED_RUNTIME_STATES = {"canceled", "cancelled"}


@dataclass(frozen=True)
class LocalConnectorIdentity:
    """Immutable identity captured for an owned desktop connector process group."""

    pid: int
    process_group_id: int
    process_start_marker: str
    owner_token: str


@dataclass(frozen=True)
class _BoundedHttpResponse:
    """Response metadata plus an optional fully consumed, caller-bounded body."""

    status_code: int
    headers: httpx.Headers
    content: bytes


@dataclass
class _BoundedHttpReadState:
    """Cross-thread state for one absolute-deadline HTTP response read."""

    response: httpx.Response | None = None
    result: _BoundedHttpResponse | None = None
    error: BaseException | None = None


@dataclass(frozen=True)
class _ObservedLocalProcess:
    pid: int
    process_group_id: int
    process_start_marker: str
    command_line: str
    environment: bytes | None


@dataclass(frozen=True)
class _VerifiedSchedulerSubmission:
    """Scheduler identity proven against the relay-created remote sidecar."""

    provider: str
    scheduler_job_id: str
    spec: ServiceRuntimeSpec


@dataclass(frozen=True)
class _DurableSchedulerContract:
    """Scheduler identity or explicit absence proven by durable gateway state."""

    provider: str
    scheduler_job_id: str | None
    unresolved_submission: bool = False


class CommandRunner(Protocol):
    """Protocol for local command execution used by the supervisor."""

    def run(
        self,
        command: Sequence[str],
        *,
        input_text: str | None = None,
        timeout_seconds: float | None = None,
    ) -> subprocess.CompletedProcess[str]:
        """Run a command and return the completed process."""
        ...

    def popen(
        self,
        command: Sequence[str],
        *,
        stdout_path: Path,
        stderr_path: Path,
        env: dict[str, str] | None = None,
        isolate_process_group: bool = False,
        input_bytes: bytes | None = None,
    ) -> subprocess.Popen[bytes]:
        """Start a long-running local process."""
        ...

    def local_process_identity(
        self,
        *,
        pid: int,
        owner_token: str,
        expected_config: str,
    ) -> LocalConnectorIdentity:
        """Capture and verify immutable process identity after launch."""
        ...


class SubprocessCommandRunner:
    """Command runner backed by subprocess."""

    def run(
        self,
        command: Sequence[str],
        *,
        input_text: str | None = None,
        timeout_seconds: float | None = None,
    ) -> subprocess.CompletedProcess[str]:
        """Run a local subprocess with text output."""
        input_bytes = input_text.encode("utf-8") if input_text is not None else None
        result = subprocess.run(
            list(command),
            input=input_bytes,
            capture_output=True,
            check=False,
            timeout=timeout_seconds,
        )
        return subprocess.CompletedProcess(
            args=result.args,
            returncode=result.returncode,
            stdout=result.stdout.decode("utf-8", errors="replace"),
            stderr=result.stderr.decode("utf-8", errors="replace"),
        )

    def popen(
        self,
        command: Sequence[str],
        *,
        stdout_path: Path,
        stderr_path: Path,
        env: dict[str, str] | None = None,
        isolate_process_group: bool = False,
        input_bytes: bytes | None = None,
    ) -> subprocess.Popen[bytes]:
        """Start a local subprocess with owned log files."""
        stdout_handle = stdout_path.open("ab")
        stderr_handle = stderr_path.open("ab")
        creationflags = 0
        start_new_session = False
        if isolate_process_group:
            if os.name == "nt":
                creationflags = subprocess.CREATE_NEW_PROCESS_GROUP
            else:
                start_new_session = True
        try:
            process = subprocess.Popen(
                list(command),
                stdin=subprocess.PIPE if input_bytes is not None else None,
                stdout=stdout_handle,
                stderr=stderr_handle,
                # The launched connector outlives this CLI process.  In particular, a relay
                # command may itself be invoked with captured stdout/stderr by an MCP surface.
                # Closing inherited descriptors on Windows prevents the connector grandchild
                # from retaining those capture pipes and blocking the short-lived CLI forever.
                close_fds=True,
                env=env,
                creationflags=creationflags,
                start_new_session=start_new_session,
            )
            if input_bytes is not None:
                _deliver_process_input(
                    process,
                    input_bytes=input_bytes,
                    isolate_process_group=isolate_process_group,
                )
            return process
        finally:
            stdout_handle.close()
            stderr_handle.close()

    def local_process_identity(
        self,
        *,
        pid: int,
        owner_token: str,
        expected_config: str,
    ) -> LocalConnectorIdentity:
        """Capture and verify immutable process identity after launch."""
        return _capture_local_connector_identity(
            pid=pid,
            owner_token=owner_token,
            expected_config=expected_config,
        )


def _deliver_process_input(
    process: subprocess.Popen[bytes],
    *,
    input_bytes: bytes,
    isolate_process_group: bool,
) -> None:
    """Write one private bootstrap document and close its anonymous pipe promptly."""
    input_pipe = process.stdin
    delivery_error: Exception | None = None
    if input_pipe is None:
        delivery_error = RuntimeError("subprocess stdin pipe was not created")
    else:
        try:
            written = input_pipe.write(input_bytes)
            if written != len(input_bytes):
                raise OSError("subprocess stdin accepted only a partial bootstrap document")
            input_pipe.flush()
        except Exception as exc:
            delivery_error = exc
        finally:
            try:
                input_pipe.close()
            except Exception as exc:
                if delivery_error is None:
                    delivery_error = exc
    if delivery_error is None:
        return
    if isolate_process_group:
        _terminate_just_started_process_group(process.pid)
    else:
        with suppress(OSError):
            process.terminate()
        try:
            process.wait(timeout=1.0)
        except subprocess.TimeoutExpired:
            with suppress(OSError):
                process.kill()
    raise RelayError("failed to deliver private process bootstrap over stdin") from delivery_error


@dataclass(frozen=True)
class ServiceRuntimeStartResult:
    """Result of a started service runtime session."""

    session: GatewaySession
    connect_url: str
    health_url: str
    stream_url: str | None
    compatibility_urls: dict[str, str]
    events_url: str | None
    state_url: str | None = None
    command_url: str | None = None

    def to_live_validation_report(
        self,
        *,
        launcher: str | None = None,
        install_source: str | None = None,
        artifact_sha256: str | None = None,
    ) -> LiveValidationReport:
        """Convert a proven-ready runtime to canonical release evidence."""
        from clio_relay.validation_report import (
            EvidenceReference,
            ValidationCheck,
            ValidationResource,
            ValidationStatus,
            new_live_validation_report,
        )

        report = new_live_validation_report(
            scenario="gateway-runtime",
            cluster=self.session.cluster,
            launcher=launcher,
            install_source=install_source,
            artifact_sha256=artifact_sha256,
        )
        completed_at = utc_now()
        checks = [
            (RUNTIME_SUBMIT_CHECK_ID, "scheduler runtime submitted"),
            (RUNTIME_ALLOCATED_CHECK_ID, "runtime received an allocated service node"),
            (RUNTIME_READY_CHECK_ID, "runtime reached ready state"),
            (RUNTIME_CONNECT_CHECK_ID, "desktop health connection succeeded"),
        ]
        report.checks = [
            ValidationCheck(
                check_id=check_id,
                summary=summary,
                status=ValidationStatus.PASSED,
                started_at=report.started_at,
                completed_at=completed_at,
                evidence=[
                    EvidenceReference(
                        kind="gateway_runtime",
                        reference=self.health_url,
                        excerpt=summary,
                        metadata={"session_id": self.session.session_id},
                    )
                ],
            )
            for check_id, summary in checks
        ]
        report.resources.append(
            ValidationResource(
                kind="gateway_session",
                resource_id=self.session.session_id,
                role="service_runtime",
                cluster=self.session.cluster,
                state=self.session.state.value,
                metadata=self.session.model_dump(mode="json"),
            )
        )
        if self.session.scheduler_job_id is not None:
            report.resources.append(
                ValidationResource(
                    kind="scheduler_job",
                    resource_id=self.session.scheduler_job_id,
                    role="service_runtime",
                    cluster=self.session.cluster,
                    state=self.session.queue_state,
                    provider=self.session.scheduler,
                )
            )
        transport = _object(self.session.gateway.get("transport", {}))
        for connector_role in ("remote_connector", "desktop_connector"):
            connector = _object(transport.get(connector_role, {}))
            pid = _optional_int(connector.get("pid"))
            scheduler_step_id = _optional_str(connector.get("scheduler_step_id"))
            resource_id = str(pid) if pid is not None else scheduler_step_id
            if resource_id is None:
                continue
            report.resources.append(
                ValidationResource(
                    kind="connector",
                    resource_id=resource_id,
                    role=connector_role,
                    cluster=self.session.cluster,
                    state="running",
                    references=[
                        str(connector["config_path"])
                        if isinstance(connector.get("config_path"), str)
                        else self.connect_url
                    ],
                    metadata=connector,
                )
            )
        report.completed_at = completed_at
        report.status = ValidationStatus.PASSED
        return report


@dataclass(frozen=True)
class ServiceRuntimeStopResult:
    """Result of stopping owned runtime connector processes."""

    session: GatewaySession
    mode: Literal["detach", "teardown"]
    stopped_local_pid: int | None
    stopped_remote_pid: int | None
    canceled_scheduler_job: str | None
    resources: list[CleanupResource]
    errors: list[str]

    @property
    def residual_resources(self) -> list[CleanupResource]:
        """Return requested cleanup actions that left a resource running."""
        return [resource for resource in self.resources if resource.residual]

    def json_payload(self) -> dict[str, object]:
        """Return a machine-readable cleanup report."""
        return public_gateway_payload(
            {
                "session": self.session.model_dump(mode="json"),
                "resources": [resource.model_dump(mode="json") for resource in self.resources],
                "residual_resources": [
                    resource.model_dump(mode="json") for resource in self.residual_resources
                ],
                "validation_resources": [
                    resource.model_dump(mode="json") for resource in self.validation_resources()
                ],
                "cleanup_evidence": self.to_cleanup_evidence().model_dump(mode="json"),
                "errors": self.errors,
                "ok": not self.errors and not self.residual_resources,
            }
        )

    def validation_resources(self) -> list[ValidationResource]:
        """Return cleanup resources in the shared validation-report shape."""
        return [
            resource.to_validation_resource(cluster=self.session.cluster)
            for resource in self.resources
        ]

    def to_cleanup_evidence(self) -> CleanupEvidence:
        """Convert this stop result to shared cleanup evidence."""
        from clio_relay.validation_report import CleanupEvidence

        operation_intent_name = "detach_intent" if self.mode == "detach" else "teardown_intent"
        operation_intent = _object(self.session.gateway.get(operation_intent_name, {}))
        raw_cancel_scheduler_jobs: object = (
            False if self.mode == "detach" else operation_intent.get("cancel_scheduler_job")
        )
        if not isinstance(raw_cancel_scheduler_jobs, bool):
            raise RelayError("gateway cleanup operation policy is invalid")
        return CleanupEvidence(
            requested=True,
            mode=self.mode,
            operation_id=_optional_str(operation_intent.get("operation_id")),
            cancel_scheduler_jobs=raw_cancel_scheduler_jobs,
            actions=[resource.model_dump(mode="json") for resource in self.resources],
            remaining_resources=[
                resource.to_validation_resource(cluster=self.session.cluster)
                for resource in self.residual_resources
            ],
        )

    def to_live_validation_report(
        self,
        *,
        launcher: str | None = None,
        install_source: str | None = None,
        artifact_sha256: str | None = None,
    ) -> LiveValidationReport:
        """Convert runtime teardown to canonical release evidence."""
        from clio_relay.validation_report import (
            EvidenceReference,
            ValidationCheck,
            ValidationResource,
            ValidationStatus,
            new_live_validation_report,
        )

        report = new_live_validation_report(
            scenario="gateway-runtime",
            cluster=self.session.cluster,
            launcher=launcher,
            install_source=install_source,
            artifact_sha256=artifact_sha256,
        )
        completed_at = utc_now()
        desktop_connectors = [
            resource for resource in self.resources if resource.kind == "desktop_connector"
        ]
        remote_connectors = [
            resource for resource in self.resources if resource.kind == "remote_connector"
        ]
        scheduler_resources = [
            resource for resource in self.resources if resource.kind == "scheduler_job"
        ]
        gateway_resources = [
            resource for resource in self.resources if resource.kind == "gateway_record"
        ]
        cancellation_requested = any(
            resource.action == "cancel" for resource in scheduler_resources
        )
        scheduler_identity_exact = (
            not scheduler_resources
            if self.session.scheduler_job_id is None
            else len(scheduler_resources) == 1
            and scheduler_resources[0].resource_id == self.session.scheduler_job_id
            and scheduler_resources[0].provider == self.session.scheduler
        )
        if cancellation_requested:
            scheduler_check = (
                RUNTIME_SCHEDULER_CANCELED_CHECK_ID,
                "scheduler cancellation reached an observed canceled state",
                scheduler_identity_exact
                and all(
                    resource.action == "cancel"
                    and resource.outcome == "canceled"
                    and resource.ownership_verified
                    and resource.verified_after_operation
                    and resource.observed_state in _CANCELED_RUNTIME_STATES
                    and not resource.residual
                    for resource in scheduler_resources
                ),
            )
        else:
            allowed_retention_outcomes = (
                {"retained"} if self.mode == "detach" else {"retained", "terminal", "missing"}
            )
            scheduler_check = (
                RUNTIME_SCHEDULER_RETAINED_CHECK_ID,
                "scheduler job preserved by default and its disposition observed",
                scheduler_identity_exact
                and (
                    self.session.scheduler_job_id is None
                    or all(
                        resource.action == "retain"
                        and resource.outcome in allowed_retention_outcomes
                        and resource.ownership_verified
                        and resource.verified_after_operation
                        and resource.observed_state is not None
                        and (
                            resource.observed_state in _ACTIVE_RUNTIME_STATES
                            if self.mode == "detach"
                            else resource.observed_state
                            not in {"not-found", "not_found", "unknown"}
                        )
                        and not resource.residual
                        for resource in scheduler_resources
                    )
                ),
            )
        if self.mode == "detach":
            desktop_stopped = len(desktop_connectors) == 1 and all(
                resource.metadata.get("gateway_session_id") == self.session.session_id
                and resource.action == "stop"
                and resource.outcome in {"stopped", "missing"}
                and resource.ownership_verified
                and resource.verified_after_operation
                and not resource.residual
                for resource in desktop_connectors
            )
            remote_retained = len(remote_connectors) == 1 and all(
                resource.metadata.get("gateway_session_id") == self.session.session_id
                and resource.action == "retain"
                and resource.outcome == "retained"
                and resource.ownership_verified
                and resource.verified_after_operation
                and not resource.residual
                for resource in remote_connectors
            )
            check_values = [
                (
                    RUNTIME_DETACH_CHECK_ID,
                    "desktop connector stopped and remote connector retained",
                    desktop_stopped and remote_retained,
                ),
                scheduler_check,
                (
                    RUNTIME_DETACHED_RECORD_CHECK_ID,
                    "gateway record remains available for reattachment",
                    self.session.state == GatewaySessionState.DEGRADED
                    and len(gateway_resources) == 1
                    and all(
                        resource.resource_id == self.session.session_id
                        and resource.action == "retain"
                        and resource.outcome == "retained"
                        and resource.ownership_verified
                        and resource.verified_after_operation
                        and not resource.residual
                        for resource in gateway_resources
                    ),
                ),
            ]
        else:
            connector_resources = [*desktop_connectors, *remote_connectors]
            connectors_stopped = (
                len(desktop_connectors) == 1
                and len(remote_connectors) == 1
                and all(
                    resource.metadata.get("gateway_session_id") == self.session.session_id
                    and resource.action == "stop"
                    and resource.outcome in {"stopped", "missing"}
                    and resource.ownership_verified
                    and resource.verified_after_operation
                    and not resource.residual
                    for resource in connector_resources
                )
            )
            gateway_closed = (
                self.session.state == GatewaySessionState.CLOSED
                and len(gateway_resources) == 1
                and gateway_resources[0].resource_id == self.session.session_id
                and gateway_resources[0].action == "close"
                and gateway_resources[0].outcome == "closed"
                and gateway_resources[0].ownership_verified
                and gateway_resources[0].verified_after_operation
                and not gateway_resources[0].residual
            )
            check_values = [
                (RUNTIME_TEARDOWN_CHECK_ID, "owned runtime connectors stopped", connectors_stopped),
                scheduler_check,
                (
                    RUNTIME_CLOSED_CHECK_ID,
                    "gateway record closed",
                    gateway_closed,
                ),
            ]
        report.checks = [
            ValidationCheck(
                check_id=check_id,
                summary=summary,
                status=ValidationStatus.PASSED if passed else ValidationStatus.FAILED,
                started_at=report.started_at,
                completed_at=completed_at,
                evidence=[
                    EvidenceReference(
                        kind="gateway_cleanup",
                        excerpt=summary,
                        metadata=self.json_payload(),
                    )
                ],
                error=None if passed else summary,
            )
            for check_id, summary, passed in check_values
        ]
        report.resources = self.validation_resources()
        report.resources.append(
            ValidationResource(
                kind="gateway_session",
                resource_id=self.session.session_id,
                role="service_runtime",
                cluster=self.session.cluster,
                state=self.session.state.value,
                metadata=self.session.model_dump(mode="json"),
            )
        )
        if self.session.scheduler_job_id is not None:
            scheduler_observation = next(
                (
                    resource
                    for resource in scheduler_resources
                    if resource.resource_id == self.session.scheduler_job_id
                ),
                None,
            )
            report.resources.append(
                ValidationResource(
                    kind="scheduler_job",
                    resource_id=self.session.scheduler_job_id,
                    role="service_runtime",
                    cluster=self.session.cluster,
                    state=(
                        "canceled"
                        if self.canceled_scheduler_job is not None
                        else (
                            scheduler_observation.observed_state
                            if scheduler_observation is not None
                            else self.session.queue_state
                        )
                    ),
                    provider=self.session.scheduler,
                )
            )
        report.cleanup = self.to_cleanup_evidence()
        report.completed_at = completed_at
        report.status = (
            ValidationStatus.PASSED
            if all(check.status is ValidationStatus.PASSED for check in report.checks)
            else ValidationStatus.FAILED
        )
        report.error = (
            None if report.status is ValidationStatus.PASSED else "gateway cleanup failed"
        )
        return report


class ServiceRuntimeSupervisor:
    """Start, bind, probe, and tear down scheduler-backed remote service sessions."""

    def __init__(
        self,
        *,
        settings: RelaySettings,
        queue: ClioCoreQueue,
        cluster: str,
        definition: ClusterDefinition,
        token: str,
        secret_key: str,
        runner: CommandRunner | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.settings = settings
        self.queue = queue
        self.cluster = cluster
        self.definition = definition
        self.token = token
        self.secret_key = secret_key
        self.runner = runner or SubprocessCommandRunner()
        self.sleep = sleep

    def _jarvis_runtime_authorization(
        self,
        verified: VerifiedJarvisServiceRuntime,
    ) -> str | None:
        """Resolve per operation; callers may stdin-transfer only to the owned memory proxy."""
        return resolve_jarvis_service_runtime_authorization(
            definition=self.definition,
            settings=self.settings,
            verified=verified,
        )

    def start(
        self,
        *,
        name: str,
        spec: ServiceRuntimeSpec,
        owner_session_id: str | None = None,
        owner_session_generation_id: str | None = None,
        owner_session_admission_id: str | None = None,
    ) -> ServiceRuntimeStartResult:
        """Start a scheduler-backed remote service and bind it to a desktop port."""
        if spec.deployment_driver == "jarvis-bound":
            raise ConfigurationError("jarvis-bound runtimes must use bind_verified_jarvis_runtime")
        if spec.submit_command is None:
            raise ConfigurationError("submitted runtimes require a submit command")
        submit_command = spec.submit_command
        if (owner_session_id is None) != (owner_session_generation_id is None):
            raise ConfigurationError(
                "owner_session_id and owner_session_generation_id must be provided together"
            )
        if owner_session_admission_id is not None and owner_session_id is None:
            raise ConfigurationError(
                "owner_session_admission_id requires owner_session_id and generation"
            )
        scheduler_provider = provider_for_scheduler(spec.scheduler)
        if scheduler_provider.name != spec.scheduler:
            spec = spec.model_copy(update={"scheduler": scheduler_provider.name})
        self.queue.initialize()
        owner_metadata: dict[str, object] = {
            "owner": "clio-relay",
            "runtime_kind": spec.kind,
        }
        if owner_session_id is not None and owner_session_generation_id is not None:
            owner_metadata.update(
                {
                    "owner_session_id": owner_session_id,
                    "owner_session_generation_id": owner_session_generation_id,
                }
            )
            if owner_session_admission_id is not None:
                owner_metadata["owner_session_admission_id"] = owner_session_admission_id
        session = self.queue.create_gateway_session(
            GatewaySession(
                cluster=self.cluster,
                name=name,
                state=GatewaySessionState.CREATED,
                scheduler=spec.scheduler,
                requested_resources={"service_port": spec.service_port},
                gateway={
                    "runtime_spec": spec.model_dump(mode="json"),
                    "transport": {"mode": spec.transport_mode},
                    "ownership_intents": {
                        role: _new_ownership_intent("not_started")
                        for role in (
                            "scheduler_submission",
                            "remote_connector",
                            "desktop_connector",
                        )
                    },
                },
                metadata=owner_metadata,
            )
        )
        transition_lock = self._acquire_gateway_transition_lock(session.session_id)
        try:
            session = self._runtime_start_session_after_lock(session.session_id)
        except BaseException:
            transition_lock.release()
            raise
        remote_connector: dict[str, object] | None = None
        local_connector: dict[str, object] | None = None
        try:
            session = self._update(
                session,
                state=GatewaySessionState.SUBMITTED,
                metadata={"submitted_at": utc_now().isoformat()},
            )
            submission_id = secrets.token_hex(16)
            submission_marker = secrets.token_hex(32)
            session = self._set_ownership_intent(
                session,
                "scheduler_submission",
                _new_ownership_intent(
                    "starting",
                    submission_id=submission_id,
                    scheduler_provider=spec.scheduler,
                    submission_marker=submission_marker,
                ),
            )
            submit_output = self._ssh(
                _submit_script(
                    submit_command,
                    session_id=session.session_id,
                    submission_id=submission_id,
                    scheduler_provider=spec.scheduler,
                    submission_marker=submission_marker,
                )
            )
            submission = _parse_runtime_submission(submit_output)
            scheduler_job_id = submission.scheduler_job_id
            session = self._update(
                session,
                scheduler_job_id=scheduler_job_id,
                queue_state="submitted",
                gateway=self._gateway_with_ownership_intent(
                    session,
                    "scheduler_submission",
                    _new_ownership_intent(
                        "recorded",
                        submission_id=submission_id,
                        scheduler_provider=spec.scheduler,
                        submission_marker=submission_marker,
                        scheduler_job_id=scheduler_job_id,
                    ),
                    submit_output=submit_output.strip(),
                ),
            )
            node = self._wait_for_allocation_and_health(
                session,
                spec,
                scheduler_job_id,
                initial_service_host=submission.service_host,
            )
            session = self.queue.get_gateway_session(session.session_id)
            proxy_name = spec.proxy_name or f"{session.session_id}-service"
            remote_intent = _new_ownership_intent(
                "starting",
                owner_token=secrets.token_hex(32),
                connector_generation_id=secrets.token_hex(16),
            )
            session = self._set_ownership_intent(
                session,
                "remote_connector",
                remote_intent,
            )
            remote_connector = self._start_remote_connector(
                session=session,
                spec=spec,
                node=node,
                proxy_name=proxy_name,
                ownership_intent=remote_intent,
            )
            # Allocation-scoped connector startup may enrich and durably persist
            # the ownership intent before launching a scheduler step. Reload the
            # exact revision before publishing the returned connector identity.
            session = self.queue.get_gateway_session(session.session_id)
            session = self._update(
                session,
                gateway=self._gateway_with_ownership_intent(
                    session,
                    "remote_connector",
                    _new_ownership_intent("recorded", **remote_connector),
                    transport={
                        **_object(session.gateway.get("transport", {})),
                        "remote_connector": remote_connector,
                    },
                ),
            )
            local_intent = self._local_connector_intent(session)
            session = self._set_ownership_intent(
                session,
                "desktop_connector",
                local_intent,
            )
            local_connector = self._start_local_visitor(
                session=session,
                spec=spec,
                proxy_name=proxy_name,
                ownership_intent=local_intent,
            )
            session = self._update(
                session,
                gateway=self._gateway_with_ownership_intent(
                    session,
                    "desktop_connector",
                    _new_ownership_intent("recorded", **local_connector),
                    transport={
                        **_object(session.gateway.get("transport", {})),
                        "remote_connector": remote_connector,
                        "desktop_connector": local_connector,
                    },
                ),
            )
            connect_url = spec.connect_url_template.format(
                bind_addr=spec.desktop_bind_addr,
                bind_port=spec.desktop_bind_port,
                session_id=session.session_id,
            )
            health_url = (
                f"{spec.protocol}://{spec.desktop_bind_addr}:"
                f"{spec.desktop_bind_port}{spec.health_path}"
            )
            self._wait_for_local_health(
                health_url,
                spec.readiness_timeout_seconds,
                spec.poll_seconds,
                expected_body=spec.health_expected_body,
            )
            events_url = (
                f"{spec.protocol}://{spec.desktop_bind_addr}:"
                f"{spec.desktop_bind_port}{spec.event_stream_path}"
                if spec.event_stream_path is not None
                else None
            )
            stream_url = (
                f"{spec.protocol}://{spec.desktop_bind_addr}:"
                f"{spec.desktop_bind_port}{spec.stream_path}"
                if spec.stream_path is not None
                else None
            )
            state_url = (
                f"{spec.protocol}://{spec.desktop_bind_addr}:"
                f"{spec.desktop_bind_port}{spec.state_path}"
                if spec.state_path is not None
                else None
            )
            command_url = (
                f"{spec.protocol}://{spec.desktop_bind_addr}:"
                f"{spec.desktop_bind_port}{spec.command_path}"
                if spec.command_path is not None
                else None
            )
            compatibility_urls = {
                name: (f"{spec.protocol}://{spec.desktop_bind_addr}:{spec.desktop_bind_port}{path}")
                for name, path in spec.compatibility_paths.items()
            }
            session = self._update(
                session,
                state=GatewaySessionState.READY,
                queue_state="running",
                node=node,
                gateway={
                    **session.gateway,
                    "connect_url": connect_url,
                    "health_url": health_url,
                    "stream_url": stream_url,
                    "compatibility_urls": compatibility_urls,
                    "events_url": events_url,
                    "state_url": state_url,
                    "command_url": command_url,
                    "service": {
                        "host": node,
                        "port": spec.service_port,
                        "health_path": spec.health_path,
                        "stream_mode": spec.stream_mode,
                        "stream_path": spec.stream_path,
                        "compatibility_paths": spec.compatibility_paths,
                        "state_path": spec.state_path,
                        "event_stream_path": spec.event_stream_path,
                        "command_path": spec.command_path,
                        "protocol": spec.protocol,
                        "deployment_driver": spec.deployment_driver,
                    },
                    "transport": {
                        "mode": spec.transport_mode,
                        "proxy_name": proxy_name,
                        "remote_connector": remote_connector,
                        "desktop_connector": local_connector,
                        "remote_target": f"{node}:{spec.service_port}",
                        "desktop_bind": f"{spec.desktop_bind_addr}:{spec.desktop_bind_port}",
                    },
                },
                metadata={"ready_at": utc_now().isoformat()},
            )
            return ServiceRuntimeStartResult(
                session=session,
                connect_url=connect_url,
                health_url=health_url,
                stream_url=stream_url,
                compatibility_urls=compatibility_urls,
                events_url=events_url,
                state_url=state_url,
                command_url=command_url,
            )
        except Exception as exc:
            cleanup_errors: list[str] = []
            if remote_connector is None:
                try:
                    recovered = self._reconcile_ownership_intents(
                        self.queue.get_gateway_session(session.session_id)
                    )
                    recovered_remote = _object(
                        _object(recovered.gateway.get("transport", {})).get(
                            "remote_connector",
                            {},
                        )
                    )
                    if recovered_remote:
                        remote_connector = recovered_remote
                except (ConfigurationError, RelayError) as recovery_exc:
                    cleanup_errors.append(
                        f"remote connector rollback reconciliation failed: {recovery_exc}"
                    )
            if local_connector is not None:
                _, local_rollback = self._stop_local_connector(
                    session_id=session.session_id,
                    connector=local_connector,
                    require_record=True,
                )
                if local_rollback.residual or not local_rollback.verified_after_operation:
                    cleanup_errors.append(
                        local_rollback.detail or "desktop connector rollback was not proven"
                    )
            if remote_connector is not None:
                remote_pid = _optional_int(remote_connector.get("pid"))
                if remote_pid is None:
                    cleanup_errors.append("remote connector rollback has no recorded pid")
                else:
                    try:
                        remote_result = _last_json_object(
                            self._ssh(
                                _remote_stop_script(
                                    session_id=session.session_id,
                                    pid=remote_pid,
                                )
                            )
                        )
                        if not _remote_cleanup_proven(remote_result):
                            cleanup_errors.append(
                                "remote connector rollback did not prove full process-group absence"
                            )
                    except RelayError as rollback_exc:
                        cleanup_errors.append(str(rollback_exc))
            try:
                stop_result = self._stop_serialized(
                    session_id=session.session_id,
                    cancel_scheduler_job=False,
                    final_state=GatewaySessionState.FAILED,
                )
                cleanup_errors.extend(stop_result.errors)
            except Exception as cleanup_exc:
                cleanup_errors.append(str(cleanup_exc))
            try:
                self._record_runtime_start_failure(
                    session_id=session.session_id,
                    error=exc,
                    cleanup_errors=cleanup_errors,
                )
            except Exception as record_exc:
                exc.add_note(
                    f"runtime failure handling could not persist its final record: {record_exc}"
                )
            raise
        finally:
            transition_lock.release()

    def bind_verified_jarvis_runtime(
        self,
        *,
        name: str,
        verified: VerifiedJarvisServiceRuntime,
        desktop_bind_port: int | None = None,
        owner_session_id: str | None = None,
        owner_session_generation_id: str | None = None,
        owner_session_admission_id: str | None = None,
        transport_mode: str = "frp-stcp-wss",
        readiness_timeout_seconds: float = 300.0,
        poll_seconds: float = 2.0,
    ) -> ServiceRuntimeStartResult:
        """Bind connectors to a ready JARVIS-owned service without submitting work.

        ``desktop_bind_port`` is an internal operator override. Agent-facing calls
        omit it so the relay allocates a distinct free loopback port.
        """
        runtime = verified.runtime
        binding = verified.binding
        if runtime.lifecycle != "ready":
            raise ConfigurationError("only a ready JARVIS service runtime can be bound")
        if (owner_session_id is None) != (owner_session_generation_id is None):
            raise ConfigurationError(
                "owner_session_id and owner_session_generation_id must be provided together"
            )
        if owner_session_admission_id is not None and owner_session_id is None:
            raise ConfigurationError(
                "owner_session_admission_id requires owner_session_id and generation"
            )
        if owner_session_id is not None and owner_session_admission_id is None:
            raise ConfigurationError(
                "owned JARVIS runtime binding requires owner_session_admission_id"
            )
        if owner_session_id is not None and owner_session_admission_id != (
            desktop_owner_session_admission_id(
                cluster=self.cluster,
                session_id=owner_session_id,
            )
        ):
            raise ConfigurationError(
                "owned JARVIS runtime binding admission id does not match its "
                "cluster/session identity"
            )
        if readiness_timeout_seconds <= 0 or poll_seconds <= 0:
            raise ConfigurationError("runtime readiness intervals must be positive")
        allocation_provider = binding.scheduler_provider
        allocation_job_id = binding.scheduler_native_id
        if allocation_job_id is not None:
            if allocation_provider is None:
                raise ConfigurationError(
                    "scheduler-backed JARVIS runtime omitted its scheduler provider"
                )
            if runtime.host not in {"127.0.0.1", "::1", "localhost"}:
                raise ConfigurationError(
                    "scheduler-backed JARVIS services must advertise a loopback-only endpoint"
                )
        local_port = (
            _available_loopback_port(exclude={runtime.port})
            if desktop_bind_port is None
            else _validated_available_loopback_port(desktop_bind_port)
        )
        scheduler = binding.scheduler_provider or "external"
        spec = ServiceRuntimeSpec(
            kind="jarvis-service-runtime",
            submit_command=None,
            deployment_driver="jarvis-bound",
            service_port=runtime.port,
            protocol=runtime.protocol,
            health_path=runtime.health_path,
            stream_mode="push",
            stream_path=runtime.live_data_path,
            event_stream_path=runtime.events_path,
            state_path=runtime.state_path,
            command_path=runtime.command_path,
            desktop_bind_addr="127.0.0.1",
            desktop_bind_port=local_port,
            transport_mode=transport_mode,
            readiness_timeout_seconds=readiness_timeout_seconds,
            poll_seconds=poll_seconds,
            scheduler=scheduler,
            connect_url_template=f"{runtime.protocol}://{{bind_addr}}:{{bind_port}}",
            metadata={
                "source": "verified_jarvis_service_runtime",
                "service_instance_id": runtime.service_instance_id,
                "service_revision": runtime.revision,
            },
        )
        owner_metadata: dict[str, object] = {
            "owner": "clio-relay",
            "runtime_kind": spec.kind,
            "binding_source": "jarvis_mcp_result",
            "source_relay_job_id": binding.source_relay_job_id,
            "source_relay_artifact_id": binding.source_relay_artifact_id,
            "jarvis_execution_id": binding.jarvis_execution_id,
        }
        if owner_session_id is not None and owner_session_generation_id is not None:
            owner_metadata.update(
                {
                    "owner_session_id": owner_session_id,
                    "owner_session_generation_id": owner_session_generation_id,
                }
            )
            if owner_session_admission_id is not None:
                owner_metadata["owner_session_admission_id"] = owner_session_admission_id
        self.queue.initialize()
        session = self.queue.create_gateway_session(
            GatewaySession(
                cluster=self.cluster,
                name=name,
                state=GatewaySessionState.CREATED,
                scheduler=scheduler,
                scheduler_job_id=binding.scheduler_native_id,
                requested_resources={"service_port": runtime.port},
                gateway={
                    "runtime_spec": spec.model_dump(mode="json"),
                    "jarvis_runtime_binding": binding.model_dump(mode="json"),
                    "transport": {"mode": transport_mode},
                    "ownership_intents": {
                        "scheduler_submission": _new_ownership_intent(
                            "absent_verified",
                            source="verified_jarvis_runtime_binding",
                        ),
                        "remote_connector": _new_ownership_intent("not_started"),
                        "desktop_connector": _new_ownership_intent("not_started"),
                    },
                },
                metadata=owner_metadata,
            )
        )
        transition_lock = self._acquire_gateway_transition_lock(session.session_id)
        try:
            session = self._runtime_start_session_after_lock(session.session_id)
        except BaseException:
            transition_lock.release()
            raise
        remote_connector: dict[str, object] | None = None
        local_connector: dict[str, object] | None = None
        try:
            session = self._update(
                session,
                state=GatewaySessionState.STARTING,
                queue_state=runtime.lifecycle,
                node=runtime.host,
                metadata={"binding_started_at": utc_now().isoformat()},
            )
            service_authorization = self._jarvis_runtime_authorization(verified)
            proxy_name = f"{session.session_id}-service"
            remote_intent = _new_ownership_intent(
                "starting",
                owner_token=secrets.token_hex(32),
                connector_generation_id=secrets.token_hex(16),
            )
            session = self._set_ownership_intent(session, "remote_connector", remote_intent)
            remote_connector = self._start_remote_connector(
                session=session,
                spec=spec,
                node=runtime.host,
                proxy_name=proxy_name,
                ownership_intent=remote_intent,
                allocation_provider=allocation_provider,
                allocation_job_id=allocation_job_id,
            )
            # ``_start_remote_connector`` persists scheduler placement and its
            # unique step marker before starting an allocation-scoped process.
            # Continue from that newer durable revision instead of the stale
            # pre-placement snapshot.
            session = self.queue.get_gateway_session(session.session_id)
            session = self._update(
                session,
                gateway=self._gateway_with_ownership_intent(
                    session,
                    "remote_connector",
                    _new_ownership_intent("recorded", **remote_connector),
                    transport={
                        **_object(session.gateway.get("transport", {})),
                        "remote_connector": remote_connector,
                    },
                ),
            )
            local_intent = self._local_connector_intent(session)
            session = self._set_ownership_intent(session, "desktop_connector", local_intent)
            local_connector = self._start_local_visitor(
                session=session,
                spec=spec,
                proxy_name=proxy_name,
                ownership_intent=local_intent,
            )
            session = self._update(
                session,
                gateway=self._gateway_with_ownership_intent(
                    session,
                    "desktop_connector",
                    _new_ownership_intent("recorded", **local_connector),
                    transport={
                        **_object(session.gateway.get("transport", {})),
                        "remote_connector": remote_connector,
                        "desktop_connector": local_connector,
                    },
                ),
            )
            base_url = f"{runtime.protocol}://127.0.0.1:{local_port}"
            connect_url = base_url
            health_url = f"{base_url}{runtime.health_path}"
            stream_url = f"{base_url}{runtime.live_data_path}"
            events_url = f"{base_url}{runtime.events_path}"
            state_url = f"{base_url}{runtime.state_path}"
            command_url = f"{base_url}{runtime.command_path}"
            self._wait_for_jarvis_health(
                health_url,
                timeout_seconds=readiness_timeout_seconds,
                poll_seconds=poll_seconds,
                runtime_schema_version=runtime.schema_version,
                authorization=service_authorization,
            )
            session = self._update(
                session,
                state=GatewaySessionState.READY,
                queue_state=runtime.lifecycle,
                node=runtime.host,
                gateway={
                    **session.gateway,
                    "connect_url": connect_url,
                    "health_url": health_url,
                    "stream_url": stream_url,
                    "events_url": events_url,
                    "state_url": state_url,
                    "command_url": command_url,
                    "compatibility_urls": {},
                    "service": {
                        "host": runtime.host,
                        "port": runtime.port,
                        "protocol": runtime.protocol,
                        "health_path": runtime.health_path,
                        "stream_mode": runtime.delivery_mode,
                        "stream_path": runtime.live_data_path,
                        "event_stream_path": runtime.events_path,
                        "state_path": runtime.state_path,
                        "command_path": runtime.command_path,
                        "deployment_driver": "jarvis-bound",
                        "placement": remote_connector.get("placement"),
                    },
                    "transport": {
                        "mode": transport_mode,
                        "proxy_name": proxy_name,
                        "remote_connector": remote_connector,
                        "desktop_connector": local_connector,
                        "remote_target": f"{runtime.host}:{runtime.port}",
                        "desktop_bind": f"127.0.0.1:{local_port}",
                    },
                },
                metadata={"ready_at": utc_now().isoformat()},
            )
            return ServiceRuntimeStartResult(
                session=session,
                connect_url=connect_url,
                health_url=health_url,
                stream_url=stream_url,
                compatibility_urls={},
                events_url=events_url,
                state_url=state_url,
                command_url=command_url,
            )
        except Exception as exc:
            cleanup_errors: list[str] = []
            if remote_connector is None or local_connector is None:
                try:
                    recovered = self._reconcile_ownership_intents(
                        self.queue.get_gateway_session(session.session_id)
                    )
                    recovered_transport = _object(recovered.gateway.get("transport", {}))
                    if remote_connector is None:
                        recovered_remote = _object(recovered_transport.get("remote_connector", {}))
                        if recovered_remote:
                            remote_connector = recovered_remote
                    if local_connector is None:
                        recovered_local = _object(recovered_transport.get("desktop_connector", {}))
                        if recovered_local:
                            local_connector = recovered_local
                except (ConfigurationError, RelayError) as recovery_exc:
                    cleanup_errors.append(
                        f"connector rollback reconciliation failed: {recovery_exc}"
                    )
            if local_connector is not None:
                _, local_rollback = self._stop_local_connector(
                    session_id=session.session_id,
                    connector=local_connector,
                    require_record=True,
                )
                if local_rollback.residual or not local_rollback.verified_after_operation:
                    cleanup_errors.append(
                        local_rollback.detail or "desktop connector rollback was not proven"
                    )
            if remote_connector is not None:
                if remote_connector.get("execution_scope") == "scheduler_allocation":
                    try:
                        rollback = self._stop_allocation_connector(
                            session_id=session.session_id,
                            connector=remote_connector,
                        )
                        if rollback.residual or not rollback.verified_after_operation:
                            cleanup_errors.append(
                                rollback.detail
                                or "allocation connector rollback absence was not proven"
                            )
                    except (ConfigurationError, RelayError) as rollback_exc:
                        cleanup_errors.append(str(rollback_exc))
                else:
                    remote_pid = _optional_int(remote_connector.get("pid"))
                    if remote_pid is None:
                        cleanup_errors.append("remote connector rollback has no recorded pid")
                    else:
                        try:
                            result = _last_json_object(
                                self._ssh(
                                    _remote_stop_script(
                                        session_id=session.session_id,
                                        pid=remote_pid,
                                    )
                                )
                            )
                            if not _remote_cleanup_proven(result):
                                cleanup_errors.append(
                                    "remote connector rollback did not prove process-group absence"
                                )
                        except RelayError as rollback_exc:
                            cleanup_errors.append(str(rollback_exc))
            try:
                self._record_runtime_start_failure(
                    session_id=session.session_id,
                    error=exc,
                    cleanup_errors=cleanup_errors,
                )
            except Exception as record_exc:
                exc.add_note(
                    f"runtime failure handling could not persist its final record: {record_exc}"
                )
            raise
        finally:
            transition_lock.release()

    def browser_attach(
        self,
        *,
        session_id: str,
        ttl_seconds: int = 1_800,
        bind_port: int | None = None,
    ) -> BrowserAttachmentGrant:
        """Serialize browser capability creation against all gateway transitions."""
        with self._gateway_transition_lock(session_id):
            return self._browser_attach_serialized(
                session_id=session_id,
                ttl_seconds=ttl_seconds,
                bind_port=bind_port,
            )

    def _browser_attach_serialized(
        self,
        *,
        session_id: str,
        ttl_seconds: int = 1_800,
        bind_port: int | None = None,
    ) -> BrowserAttachmentGrant:
        """Issue one short-lived sandbox capability through an owned loopback proxy."""
        if ttl_seconds < 60 or ttl_seconds > 28_800:
            raise ConfigurationError("browser attachment TTL must be between 60 and 28800 seconds")
        session = self.queue.get_gateway_session(session_id)
        if session.cluster != self.cluster:
            raise ConfigurationError(
                f"gateway session {session_id} belongs to cluster {session.cluster}, "
                f"not {self.cluster}"
            )
        if session.metadata.get("owner") != "clio-relay":
            raise ConfigurationError("browser attachment requires an owned clio-relay runtime")
        if session.state is not GatewaySessionState.READY:
            raise ConfigurationError("browser attachment requires a ready gateway session")
        if session.gateway.get("teardown_intent") is not None:
            raise ConfigurationError("a gateway committed to teardown cannot issue attachments")
        binding_document = session.gateway.get("jarvis_runtime_binding")
        if binding_document is None:
            raise ConfigurationError("browser attachment requires a verified JARVIS binding")
        try:
            verified_runtime = reverify_jarvis_service_runtime(
                queue=self.queue,
                definition=self.definition,
                settings=self.settings,
                binding_document=binding_document,
            )
        except ValueError as exc:
            raise RelayError(
                f"JARVIS service runtime binding re-verification failed: {exc}"
            ) from exc
        try:
            spec = ServiceRuntimeSpec.model_validate(session.gateway.get("runtime_spec"))
        except ValueError as exc:
            raise RelayError("owned runtime has no valid service runtime specification") from exc
        if spec.deployment_driver != "jarvis-bound" or spec.command_path is None:
            raise ConfigurationError("browser attachment requires a JARVIS-bound command contract")
        existing_document = session.gateway.get("browser_attachment")
        if existing_document is not None:
            try:
                existing = BrowserAttachmentRecord.model_validate(existing_document)
            except ValueError as exc:
                raise RelayError("gateway contains an invalid browser attachment record") from exc
            if existing.state != "revoked":
                expiry = _utc_timestamp(existing.expires_at)
                if expiry > utc_now() and not Path(existing.revocation_path).exists():
                    raise ConfigurationError(
                        "gateway already has an active browser attachment; "
                        "detach it before rotating"
                    )
                session, _result, cleanup = self._revoke_browser_attachment(
                    session,
                    attachment_id=existing.attachment_id,
                )
                if cleanup.residual:
                    raise RelayError(cleanup.detail or "expired browser proxy cleanup failed")

        public_port = bind_port or _available_loopback_port(exclude={spec.desktop_bind_port})
        if public_port < 1 or public_port > 65_535:
            raise ConfigurationError("browser attachment bind port must be between 1 and 65535")
        if public_port == spec.desktop_bind_port:
            raise ConfigurationError("browser attachment port must differ from the direct port")
        attachment_id = f"browser-{secrets.token_hex(16)}"
        capability = secrets.token_urlsafe(32)
        issued_at = utc_now()
        expires_at = issued_at + timedelta(seconds=ttl_seconds)
        runtime_dir = (
            self.settings.core_dir.parent / "runtime-sessions" / session.session_id
        ).resolve()
        runtime_dir.mkdir(parents=True, exist_ok=True)
        config_path = runtime_dir / f"{attachment_id}.browser-gateway.json"
        revocation_path = runtime_dir / f"{attachment_id}.revoked"
        stdout_path = runtime_dir / f"{attachment_id}.browser-gateway.out"
        stderr_path = runtime_dir / f"{attachment_id}.browser-gateway.err"
        metadata_path = runtime_dir / f"{attachment_id}.browser-gateway-owner.json"
        token_sha256 = hashlib.sha256(capability.encode("utf-8")).hexdigest()
        paths = list(
            dict.fromkeys(
                [
                    "/",
                    spec.health_path,
                    spec.stream_path,
                    spec.event_stream_path,
                    spec.state_path,
                    spec.command_path,
                ]
            )
        )
        if any(path is None for path in paths):
            raise ConfigurationError("JARVIS browser attachment requires all six endpoint paths")
        config = BrowserGatewayConfig(
            attachment_id=attachment_id,
            token_sha256=token_sha256,
            bind_port=public_port,
            upstream_protocol=spec.protocol,
            upstream_port=spec.desktop_bind_port,
            allowed_paths=cast(list[str], paths),
            command_path=spec.command_path,
            expires_at=expires_at.isoformat(),
            revocation_path=str(revocation_path),
        )
        intent = _new_ownership_intent(
            "starting",
            owner_token=secrets.token_hex(32),
            connector_generation_id=secrets.token_hex(16),
            config_path=str(config_path),
            stdout_path=str(stdout_path),
            stderr_path=str(stderr_path),
            metadata_path=str(metadata_path),
            attachment_id=attachment_id,
        )
        record = BrowserAttachmentRecord(
            attachment_id=attachment_id,
            state="starting",
            issued_at=issued_at.isoformat(),
            expires_at=expires_at.isoformat(),
            token_sha256=token_sha256,
            bind_port=public_port,
            revocation_path=str(revocation_path),
        )
        session = self.queue.prepare_gateway_browser_attachment(
            session.session_id,
            attachment=record,
            browser_proxy_intent=intent,
        )
        proxy: dict[str, object] | None = None
        try:
            proxy = self._start_browser_proxy(
                session=session,
                config=config,
                capability=capability,
                upstream_authorization=self._jarvis_runtime_authorization(verified_runtime),
                ownership_intent=intent,
            )
            active = record.model_copy(update={"state": "active", "proxy_process_id": proxy["pid"]})
            session = self.queue.complete_gateway_browser_attachment(
                session.session_id,
                attachment=active,
                browser_proxy=proxy,
                browser_proxy_intent=_new_ownership_intent("recorded", **proxy),
            )
            grant = _browser_attachment_grant(
                record=active,
                capability=capability,
                spec=spec,
            )
            self._wait_for_browser_health(
                grant.health_url,
                timeout_seconds=min(spec.readiness_timeout_seconds, 60.0),
                poll_seconds=min(spec.poll_seconds, 1.0),
            )
            return grant
        except Exception as exc:
            cleanup_detail: str | None = None
            try:
                latest = self.queue.get_gateway_session(session.session_id)
                _latest, _result, cleanup = self._revoke_browser_attachment(
                    latest,
                    attachment_id=attachment_id,
                )
                if cleanup.residual:
                    cleanup_detail = cleanup.detail
            except RelayError as cleanup_exc:
                cleanup_detail = str(cleanup_exc)
            if proxy is not None:
                _stopped_pid, direct_cleanup = self._stop_local_connector(
                    session_id=session.session_id,
                    connector=proxy,
                    require_record=True,
                )
                if direct_cleanup.residual:
                    cleanup_detail = direct_cleanup.detail or cleanup_detail
            if cleanup_detail is not None:
                latest = self.queue.get_gateway_session(session.session_id)
                self.queue.update_gateway_session(
                    latest.session_id,
                    metadata={
                        "browser_attachment_error": str(exc),
                        "browser_attachment_cleanup_error": cleanup_detail,
                    },
                )
            raise

    def browser_detach(
        self,
        *,
        session_id: str,
        attachment_id: str,
    ) -> BrowserDetachmentResult:
        """Serialize browser capability revocation against gateway transitions."""
        with self._gateway_transition_lock(session_id):
            return self._browser_detach_serialized(
                session_id=session_id,
                attachment_id=attachment_id,
            )

    def _browser_detach_serialized(
        self,
        *,
        session_id: str,
        attachment_id: str,
    ) -> BrowserDetachmentResult:
        """Revoke one exact browser capability and stop its owned loopback proxy."""
        session = self.queue.get_gateway_session(session_id)
        if session.cluster != self.cluster:
            raise ConfigurationError(
                f"gateway session {session_id} belongs to cluster {session.cluster}, "
                f"not {self.cluster}"
            )
        session, result, cleanup = self._revoke_browser_attachment(
            session,
            attachment_id=attachment_id,
        )
        del session
        if cleanup.residual:
            raise RelayError(cleanup.detail or "browser attachment proxy cleanup failed")
        return result

    def _revoke_browser_attachment(
        self,
        session: GatewaySession,
        *,
        attachment_id: str,
    ) -> tuple[GatewaySession, BrowserDetachmentResult, CleanupResource]:
        try:
            session = self.queue.begin_gateway_browser_attachment_revoke(
                session.session_id,
                attachment_id=attachment_id,
            )
        except QueueConflictError as exc:
            if "changed before revocation" in str(exc):
                raise ConfigurationError(
                    "browser attachment id does not match the gateway record"
                ) from exc
            raise
        raw_record = session.gateway.get("browser_attachment")
        try:
            record = BrowserAttachmentRecord.model_validate(raw_record)
        except ValueError as exc:
            raise RelayError("gateway contains an invalid browser attachment record") from exc
        if record.state == "revoked":
            result = BrowserDetachmentResult(
                attachment_id=record.attachment_id,
                revoked_at=cast(str, record.revoked_at),
                already_revoked=True,
                proxy_process_id=record.proxy_process_id,
                proxy_stopped=False,
            )
            return (
                session,
                result,
                CleanupResource(
                    kind="browser_proxy",
                    resource_id=str(record.proxy_process_id or record.attachment_id),
                    location="desktop",
                    action="stop",
                    ownership_verified=True,
                    outcome="missing",
                    verified_after_operation=True,
                    metadata={"gateway_session_id": session.session_id},
                ),
            )
        revocation_path = _owned_browser_runtime_path(
            self.settings,
            session.session_id,
            record.revocation_path,
        )
        _write_browser_revocation_marker(revocation_path, record.attachment_id)
        transport = _object(session.gateway.get("transport", {}))
        proxy = _object(transport.get("browser_proxy", {}))
        intents = _object(session.gateway.get("ownership_intents", {}))
        intent = _object(intents.get("browser_proxy", {}))
        absence_verified = False
        if not proxy:
            proxy, absence_verified = _discover_local_connector(
                intent,
                session_id=session.session_id,
            )
            proxy = proxy or {}
        stopped_pid, cleanup = self._stop_local_connector(
            session_id=session.session_id,
            connector=proxy,
            require_record=True,
            absence_verified=absence_verified,
        )
        cleanup = cleanup.model_copy(
            update={
                "kind": "browser_proxy",
                "metadata": {
                    **cleanup.metadata,
                    "gateway_session_id": session.session_id,
                    "attachment_id": attachment_id,
                },
            }
        )
        revoked_at = utc_now().isoformat()
        if cleanup.residual:
            failed = record.model_copy(update={"state": "failed"})
            session = self.queue.finish_gateway_browser_attachment_revoke(
                session.session_id,
                attachment=failed,
                metadata={"browser_detach_error": cleanup.detail},
            )
            result = BrowserDetachmentResult(
                attachment_id=attachment_id,
                revoked_at=revoked_at,
                already_revoked=False,
                proxy_process_id=record.proxy_process_id,
                proxy_stopped=False,
            )
            return session, result, cleanup
        revoked = record.model_copy(update={"state": "revoked", "revoked_at": revoked_at})
        intents["browser_proxy"] = _new_ownership_intent(
            "absent_verified",
            attachment_id=attachment_id,
            owner_token=intent.get("owner_token"),
            connector_generation_id=intent.get("connector_generation_id"),
            config_path=intent.get("config_path"),
        )
        session = self.queue.finish_gateway_browser_attachment_revoke(
            session.session_id,
            attachment=revoked,
            browser_proxy_absent_intent=_object(intents["browser_proxy"]),
            metadata={"browser_detached_at": revoked_at},
        )
        persisted_revoked = BrowserAttachmentRecord.model_validate(
            session.gateway.get("browser_attachment")
        )
        effective_revoked_at = cast(str, persisted_revoked.revoked_at)
        return (
            session,
            BrowserDetachmentResult(
                attachment_id=attachment_id,
                revoked_at=effective_revoked_at,
                already_revoked=effective_revoked_at != revoked_at,
                proxy_process_id=record.proxy_process_id,
                proxy_stopped=stopped_pid is not None,
            ),
            cleanup,
        )

    def _revoke_browser_for_runtime_cleanup(
        self,
        session: GatewaySession,
    ) -> tuple[GatewaySession, CleanupResource | None, str | None]:
        """Revoke any active browser attachment as part of detach or teardown."""
        raw_record = session.gateway.get("browser_attachment")
        if raw_record is None:
            return session, None, None
        try:
            record = BrowserAttachmentRecord.model_validate(raw_record)
        except ValueError as exc:
            detail = f"browser attachment record is invalid: {exc}"
            return (
                session,
                CleanupResource(
                    kind="browser_proxy",
                    resource_id=session.session_id,
                    location="desktop",
                    action="stop",
                    ownership_verified=False,
                    outcome="refused",
                    residual=True,
                    detail=detail,
                    metadata={"gateway_session_id": session.session_id},
                ),
                detail,
            )
        if record.state == "revoked":
            return session, None, None
        try:
            session, _result, cleanup = self._revoke_browser_attachment(
                session,
                attachment_id=record.attachment_id,
            )
        except (ConfigurationError, RelayError) as exc:
            detail = str(exc)
            return (
                session,
                CleanupResource(
                    kind="browser_proxy",
                    resource_id=str(record.proxy_process_id or record.attachment_id),
                    location="desktop",
                    action="stop",
                    ownership_verified=False,
                    outcome="failed",
                    residual=True,
                    detail=detail,
                    metadata={"gateway_session_id": session.session_id},
                ),
                detail,
            )
        return session, cleanup, cleanup.detail if cleanup.residual else None

    def stop(
        self,
        *,
        session_id: str,
        cancel_scheduler_job: bool = False,
        final_state: GatewaySessionState = GatewaySessionState.CLOSED,
    ) -> ServiceRuntimeStopResult:
        """Serialize and durably replay one owned runtime teardown operation."""
        session = self.queue.get_gateway_session(session_id)
        self._validate_gateway_transition_session(session)
        if final_state not in {GatewaySessionState.CLOSED, GatewaySessionState.FAILED}:
            raise ConfigurationError("gateway teardown final state must be closed or failed")
        with self._gateway_transition_lock(session_id):
            return self._stop_serialized(
                session_id=session_id,
                cancel_scheduler_job=cancel_scheduler_job,
                final_state=final_state,
            )

    def _stop_serialized(
        self,
        *,
        session_id: str,
        cancel_scheduler_job: bool,
        final_state: GatewaySessionState,
    ) -> ServiceRuntimeStopResult:
        """Execute teardown while holding the exact cluster/session transition lock."""
        session = self.queue.get_gateway_session(session_id)
        self._validate_gateway_transition_session(session)
        session = self._prepare_teardown_intent(
            session,
            cancel_scheduler_job=cancel_scheduler_job,
        )
        session = self._prepare_teardown_policy(
            session,
            cancel_scheduler_job=cancel_scheduler_job,
            final_state=final_state,
        )
        replay = self._completed_teardown_result(
            session,
            cancel_scheduler_job=cancel_scheduler_job,
            final_state=final_state,
        )
        if replay is not None:
            return replay
        session = self._reconcile_ownership_intents(session)
        scheduler_contract = _validated_durable_scheduler_contract(session, strict=False)

        # Reconciliation may refresh durable connector identities, but cannot alter
        # the teardown policy that was committed before any cleanup side effect.
        self._validate_teardown_policy(
            session,
            cancel_scheduler_job=cancel_scheduler_job,
            final_state=final_state,
        )

        session, browser_resource, browser_error = self._revoke_browser_for_runtime_cleanup(session)
        teardown_intent = _object(session.gateway.get("teardown_intent", {}))
        ownership_intents = _object(session.gateway.get("ownership_intents", {}))
        transport = _object(session.gateway.get("transport", {}))
        desktop_connector = _object(transport.get("desktop_connector", {}))
        remote_connector = _object(transport.get("remote_connector", {}))
        resources: list[CleanupResource] = []
        errors: list[str] = []
        if browser_resource is not None:
            resources.append(browser_resource)
        if browser_error is not None:
            errors.append(browser_error)
        stopped_local_pid, local_resource = self._stop_local_connector(
            session_id=session.session_id,
            connector=desktop_connector,
            require_record=True,
            absence_verified=_intent_proves_absence(
                ownership_intents,
                "desktop_connector",
            ),
        )
        local_resource = _bind_cleanup_resource_to_gateway(local_resource, session.session_id)
        resources.append(local_resource)
        if local_resource.residual:
            errors.append(local_resource.detail or "desktop connector cleanup failed")
        stopped_remote_pid = None
        remote_pid = _optional_int(remote_connector.get("pid"))
        allocation_scoped = remote_connector.get("execution_scope") == "scheduler_allocation"
        remote_owned = (
            remote_connector.get("owner") == "clio-relay"
            and remote_connector.get("session_id") == session.session_id
        )
        if allocation_scoped:
            try:
                remote_resource = self._stop_allocation_connector(
                    session_id=session.session_id,
                    connector=remote_connector,
                )
            except (ConfigurationError, RelayError) as exc:
                remote_resource = CleanupResource(
                    kind="remote_connector",
                    resource_id=(
                        _optional_str(remote_connector.get("scheduler_step_id"))
                        or session.session_id
                    ),
                    location=self.definition.ssh_host,
                    provider=_optional_str(remote_connector.get("scheduler_provider")),
                    action="stop",
                    ownership_verified=False,
                    outcome="refused",
                    residual=True,
                    detail=str(exc),
                )
                errors.append(str(exc))
        elif remote_pid is None:
            absence_verified = _intent_proves_absence(
                ownership_intents,
                "remote_connector",
            )
            remote_resource = CleanupResource(
                kind="remote_connector",
                resource_id=session.session_id,
                location=self.definition.ssh_host,
                action="stop",
                ownership_verified=absence_verified,
                outcome="missing" if absence_verified else "refused",
                verified_after_operation=absence_verified,
                residual=not absence_verified,
                detail=(
                    "no remote connector side effect was proven by its durable intent"
                    if absence_verified
                    else "owned remote connector record is missing or unverified"
                ),
            )
            if remote_resource.residual:
                errors.append(remote_resource.detail or "remote connector record is missing")
        elif not remote_owned:
            remote_resource = CleanupResource(
                kind="remote_connector",
                resource_id=str(remote_pid),
                location=self.definition.ssh_host,
                action="stop",
                ownership_verified=False,
                outcome="refused",
                residual=True,
                detail="connector record does not prove clio-relay session ownership",
            )
            errors.append(remote_resource.detail or "remote connector ownership failed")
        else:
            try:
                remote_output = self._ssh(
                    _remote_stop_script(session_id=session.session_id, pid=remote_pid)
                )
                remote_result = _last_json_object(remote_output)
                remote_outcome = remote_result.get("outcome")
                if not _remote_cleanup_proven(remote_result):
                    raise RelayError(
                        "remote connector cleanup did not prove full process-group absence: "
                        f"{remote_result!r}"
                    )
                if remote_outcome == "stopped":
                    stopped_remote_pid = remote_pid
                remote_resource = CleanupResource(
                    kind="remote_connector",
                    resource_id=str(remote_pid),
                    location=self.definition.ssh_host,
                    action="stop",
                    ownership_verified=True,
                    outcome=cast(Literal["stopped", "missing"], remote_outcome),
                    verified_after_operation=True,
                )
            except RelayError as exc:
                remote_resource = CleanupResource(
                    kind="remote_connector",
                    resource_id=str(remote_pid),
                    location=self.definition.ssh_host,
                    action="stop",
                    ownership_verified=False,
                    outcome="refused",
                    residual=True,
                    detail=str(exc),
                )
                errors.append(str(exc))
        resources.append(_bind_cleanup_resource_to_gateway(remote_resource, session.session_id))
        canceled_scheduler_job = None
        scheduler_intent = _object(ownership_intents.get("scheduler_submission", {}))
        if scheduler_contract.unresolved_submission:
            unresolved_scheduler = CleanupResource(
                kind="scheduler_job",
                resource_id=str(scheduler_intent.get("submission_id") or session.session_id),
                location=self.definition.ssh_host,
                provider=session.scheduler,
                action="cancel" if cancel_scheduler_job else "retain",
                metadata={"gateway_session_id": session.session_id},
                ownership_verified=False,
                outcome="failed",
                verified_after_operation=False,
                residual=True,
                detail=(
                    "scheduler submission side effect could not be reconciled to an exact job id"
                ),
            )
            resources.append(unresolved_scheduler)
            errors.append(unresolved_scheduler.detail or "scheduler submission is unresolved")
        if session.scheduler_job_id is not None:
            try:
                verified_submission = self._verified_scheduler_submission(
                    session,
                    allow_quiesced_owner_source_recovery=not cancel_scheduler_job,
                )
            except (ConfigurationError, RelayError) as exc:
                scheduler_resource = CleanupResource(
                    kind="scheduler_job",
                    resource_id=session.scheduler_job_id,
                    location=self.definition.ssh_host,
                    provider=session.scheduler,
                    action="cancel" if cancel_scheduler_job else "retain",
                    metadata={"gateway_session_id": session.session_id},
                    ownership_verified=False,
                    outcome="refused",
                    verified_after_operation=False,
                    residual=True,
                    detail=f"scheduler ownership verification failed: {exc}",
                )
            else:
                scheduler_job_id = verified_submission.scheduler_job_id
                spec = verified_submission.spec
                if cancel_scheduler_job:
                    cancel_request_error: str | None = None
                    try:
                        if verified_submission.provider == "external":
                            if spec.cancel_command is None:
                                raise RelayError(
                                    "externally managed runtime has no deployment-driver "
                                    "cancel command"
                                )
                            if spec.status_command is None:
                                raise RelayError(
                                    "externally managed runtime has no deployment-driver "
                                    "status command for terminal cancellation confirmation"
                                )
                            self._ssh(
                                _template_command_script(spec.cancel_command, scheduler_job_id)
                            )
                        else:
                            self._request_scheduler_provider_cancel(
                                provider=verified_submission.provider,
                                scheduler_job_id=scheduler_job_id,
                            )
                    except (ConfigurationError, RelayError) as exc:
                        cancel_request_error = str(exc)
                    try:
                        terminal_state = self._wait_for_scheduler_terminal(
                            scheduler=verified_submission.provider,
                            spec=spec,
                            scheduler_job_id=scheduler_job_id,
                        )
                        if terminal_state in _CANCELED_RUNTIME_STATES:
                            canceled_scheduler_job = scheduler_job_id
                            scheduler_resource = CleanupResource(
                                kind="scheduler_job",
                                resource_id=scheduler_job_id,
                                location=self.definition.ssh_host,
                                provider=verified_submission.provider,
                                action="cancel",
                                metadata={"gateway_session_id": session.session_id},
                                ownership_verified=True,
                                outcome="canceled",
                                verified_after_operation=True,
                                observed_state=terminal_state,
                                detail=(
                                    f"canceled scheduler state confirmed: {terminal_state}"
                                    + (
                                        "; the repeated cancel request returned an error: "
                                        f"{cancel_request_error}"
                                        if cancel_request_error is not None
                                        else ""
                                    )
                                ),
                            )
                        else:
                            scheduler_resource = CleanupResource(
                                kind="scheduler_job",
                                resource_id=scheduler_job_id,
                                location=self.definition.ssh_host,
                                provider=verified_submission.provider,
                                action="cancel",
                                metadata={"gateway_session_id": session.session_id},
                                ownership_verified=True,
                                outcome="terminal",
                                verified_after_operation=True,
                                observed_state=terminal_state,
                                detail=(
                                    "cancel was requested, but the observed terminal scheduler "
                                    f"state was {terminal_state}; cancellation is not claimed"
                                    + (
                                        "; the repeated cancel request returned an error: "
                                        f"{cancel_request_error}"
                                        if cancel_request_error is not None
                                        else ""
                                    )
                                ),
                            )
                    except (ConfigurationError, RelayError) as exc:
                        detail = str(exc)
                        if cancel_request_error is not None:
                            detail = (
                                f"scheduler cancel request failed: {cancel_request_error}; "
                                f"terminal-state verification failed: {detail}"
                            )
                        scheduler_resource = CleanupResource(
                            kind="scheduler_job",
                            resource_id=scheduler_job_id,
                            location=self.definition.ssh_host,
                            provider=verified_submission.provider,
                            action="cancel",
                            metadata={"gateway_session_id": session.session_id},
                            ownership_verified=True,
                            outcome="failed",
                            residual=True,
                            detail=detail,
                        )
                        errors.append(detail)
                else:
                    scheduler_resource = self._retained_scheduler_resource(
                        session=session,
                        spec=spec,
                    )
            resources.append(scheduler_resource)
            if scheduler_resource.residual:
                errors.append(
                    scheduler_resource.detail or "scheduler lifecycle verification failed"
                )
        cleanup_operation_id = _required_intent_str(teardown_intent, "operation_id")
        resources = [
            resource.model_copy(
                update={
                    "metadata": {
                        **resource.metadata,
                        "cleanup_operation_id": cleanup_operation_id,
                        "cancel_scheduler_job": cancel_scheduler_job,
                    }
                }
            )
            for resource in resources
        ]
        cleanup_succeeded = not errors and not any(resource.residual for resource in resources)
        effective_state = (
            final_state
            if cleanup_succeeded
            else (
                GatewaySessionState.FAILED
                if final_state == GatewaySessionState.FAILED
                else GatewaySessionState.DEGRADED
            )
        )
        gateway_resource = CleanupResource(
            kind="gateway_record",
            resource_id=session_id,
            location=str(self.settings.core_dir),
            action="close",
            ownership_verified=True,
            outcome="closed" if cleanup_succeeded else "failed",
            verified_after_operation=cleanup_succeeded,
            residual=not cleanup_succeeded,
            detail=None if cleanup_succeeded else "gateway remains retryable after cleanup failure",
            metadata={
                "cleanup_operation_id": cleanup_operation_id,
                "cancel_scheduler_job": cancel_scheduler_job,
                "gateway_session_id": session_id,
            },
        )
        resources.append(gateway_resource)
        cleanup_completed_at = utc_now().isoformat()
        updated = self.queue.update_gateway_session(
            session_id,
            state=effective_state,
            expected_updated_at=session.updated_at,
            allow_owned_runtime_close=effective_state == GatewaySessionState.CLOSED,
            metadata={
                "cleanup_at": cleanup_completed_at,
                "closed_at": (
                    cleanup_completed_at if effective_state == GatewaySessionState.CLOSED else None
                ),
                "cancel_scheduler_job": cancel_scheduler_job,
                "cleanup_retryable": not cleanup_succeeded,
                "cleanup_errors": errors,
                "cleanup_operation_id": cleanup_operation_id,
            },
            gateway={
                **session.gateway,
                "teardown": {
                    "schema_version": _GATEWAY_TEARDOWN_RESULT_SCHEMA,
                    "operation_id": cleanup_operation_id,
                    "gateway_session_id": session_id,
                    "mode": "teardown",
                    "cancel_scheduler_job": cancel_scheduler_job,
                    "requested_final_state": final_state.value,
                    "effective_state": effective_state.value,
                    "completed_at": cleanup_completed_at,
                    "retryable": not cleanup_succeeded,
                    "stopped_local_pid": stopped_local_pid,
                    "stopped_remote_pid": stopped_remote_pid,
                    "canceled_scheduler_job": canceled_scheduler_job,
                    "resources": [resource.model_dump(mode="json") for resource in resources],
                    "errors": errors,
                },
            },
        )
        return ServiceRuntimeStopResult(
            session=updated,
            mode="teardown",
            stopped_local_pid=stopped_local_pid,
            stopped_remote_pid=stopped_remote_pid,
            canceled_scheduler_job=canceled_scheduler_job,
            resources=resources,
            errors=errors,
        )

    def detach(self, *, session_id: str) -> ServiceRuntimeStopResult:
        """Serialize detachment against attach and teardown for this gateway."""
        session = self.queue.get_gateway_session(session_id)
        self._validate_gateway_transition_session(session)
        with self._gateway_transition_lock(session_id):
            return self._detach_serialized(session_id=session_id)

    def _detach_serialized(self, *, session_id: str) -> ServiceRuntimeStopResult:
        """Stop only the desktop connector while holding the gateway transition lock."""
        session = self.queue.get_gateway_session(session_id)
        self._validate_gateway_transition_session(session)
        if session.state is GatewaySessionState.CLOSED:
            raise ConfigurationError(f"gateway session {session_id} is closed")
        if session.gateway.get("teardown_intent") is not None:
            raise ConfigurationError(
                f"gateway session {session_id} is committed to teardown and cannot detach"
            )
        session = self._prepare_detach_intent(session)
        replay = self._completed_detach_result(session)
        if replay is not None:
            return replay
        session = self._reconcile_ownership_intents(session)
        scheduler_contract = _validated_durable_scheduler_contract(session, strict=False)
        session, browser_resource, browser_error = self._revoke_browser_for_runtime_cleanup(session)
        transport = _object(session.gateway.get("transport", {}))
        desktop_connector = _object(transport.get("desktop_connector", {}))
        stopped_local_pid, local_resource = self._stop_local_connector(
            session_id=session.session_id,
            connector=desktop_connector,
            require_record=True,
        )
        local_resource = _bind_cleanup_resource_to_gateway(local_resource, session.session_id)
        resources = [local_resource]
        if browser_resource is not None:
            resources.insert(0, browser_resource)
        errors = (
            [local_resource.detail] if local_resource.residual and local_resource.detail else []
        )
        if browser_error is not None:
            errors.append(browser_error)
        remote_connector = _object(transport.get("remote_connector", {}))
        remote_pid = _optional_int(remote_connector.get("pid"))
        if remote_connector.get("execution_scope") == "scheduler_allocation":
            try:
                allocation_resource = self._retained_allocation_connector_resource(
                    session_id=session.session_id,
                    connector=remote_connector,
                )
            except (ConfigurationError, RelayError) as exc:
                allocation_resource = CleanupResource(
                    kind="remote_connector",
                    resource_id=(
                        _optional_str(remote_connector.get("scheduler_step_id"))
                        or session.session_id
                    ),
                    location=self.definition.ssh_host,
                    provider=_optional_str(remote_connector.get("scheduler_provider")),
                    action="retain",
                    ownership_verified=False,
                    outcome="failed",
                    residual=True,
                    detail=str(exc),
                )
            resources.append(
                _bind_cleanup_resource_to_gateway(
                    allocation_resource,
                    session.session_id,
                )
            )
            if allocation_resource.residual:
                errors.append(
                    allocation_resource.detail
                    or "allocation connector retention could not be proven"
                )
        elif remote_pid is not None:
            remote_owned = (
                remote_connector.get("owner") == "clio-relay"
                and remote_connector.get("session_id") == session.session_id
            )
            remote_verified = False
            remote_detail = "remote connector ownership record is incomplete"
            if remote_owned:
                try:
                    remote_status = _last_json_object(
                        self._ssh(
                            _remote_connector_status_script(
                                session_id=session.session_id,
                                pid=remote_pid,
                            )
                        )
                    )
                    remote_verified = (
                        remote_status.get("ownership_verified") is True
                        and remote_status.get("running") is True
                        and isinstance(remote_status.get("matching_pids"), list)
                        and bool(remote_status["matching_pids"])
                    )
                    remote_detail = (
                        "remote connector intentionally retained for reattachment"
                        if remote_verified
                        else "remote connector retention could not be proven live"
                    )
                except RelayError as exc:
                    remote_detail = str(exc)
            resources.append(
                _bind_cleanup_resource_to_gateway(
                    CleanupResource(
                        kind="remote_connector",
                        resource_id=str(remote_pid),
                        location=self.definition.ssh_host,
                        action="retain",
                        ownership_verified=remote_verified,
                        outcome="retained" if remote_verified else "failed",
                        verified_after_operation=remote_verified,
                        residual=not remote_verified,
                        detail=remote_detail,
                    ),
                    session.session_id,
                )
            )
            if not remote_verified:
                errors.append(remote_detail)
        else:
            errors.append("owned remote connector record is missing during detach")
            resources.append(
                _bind_cleanup_resource_to_gateway(
                    CleanupResource(
                        kind="remote_connector",
                        resource_id=session.session_id,
                        location=self.definition.ssh_host,
                        action="retain",
                        ownership_verified=False,
                        outcome="failed",
                        residual=True,
                        detail="owned remote connector record is missing during detach",
                    ),
                    session.session_id,
                )
            )
        if scheduler_contract.unresolved_submission:
            scheduler_intent = _object(
                _object(session.gateway.get("ownership_intents", {})).get(
                    "scheduler_submission",
                    {},
                )
            )
            scheduler_resource = CleanupResource(
                kind="scheduler_job",
                resource_id=str(scheduler_intent.get("submission_id") or session.session_id),
                location=self.definition.ssh_host,
                provider=scheduler_contract.provider,
                action="retain",
                metadata={"gateway_session_id": session.session_id},
                ownership_verified=False,
                outcome="failed",
                verified_after_operation=False,
                residual=True,
                detail=(
                    "scheduler submission side effect could not be reconciled to an exact job id"
                ),
            )
            resources.append(scheduler_resource)
            errors.append(scheduler_resource.detail or "scheduler submission is unresolved")
        elif session.scheduler_job_id is not None:
            try:
                verified_submission = self._verified_scheduler_submission(session)
            except (ConfigurationError, RelayError) as exc:
                scheduler_resource = CleanupResource(
                    kind="scheduler_job",
                    resource_id=session.scheduler_job_id,
                    location=self.definition.ssh_host,
                    provider=session.scheduler,
                    action="retain",
                    metadata={"gateway_session_id": session.session_id},
                    ownership_verified=False,
                    outcome="refused",
                    verified_after_operation=False,
                    residual=True,
                    detail=f"scheduler ownership verification failed: {exc}",
                )
            else:
                scheduler_resource = self._retained_scheduler_resource(
                    session=session,
                    spec=verified_submission.spec,
                )
            resources.append(scheduler_resource)
            if scheduler_resource.residual:
                errors.append(
                    scheduler_resource.detail or "scheduler retention verification failed"
                )
            elif scheduler_resource.outcome in {"terminal", "missing"}:
                errors.append(
                    f"scheduler job is {scheduler_resource.outcome}; detached runtime cannot "
                    "be proven reattachable"
                )
        resources.append(
            CleanupResource(
                kind="gateway_record",
                resource_id=session.session_id,
                location=str(self.settings.core_dir),
                action="retain",
                ownership_verified=True,
                outcome="retained",
                verified_after_operation=True,
                observed_state=GatewaySessionState.DEGRADED.value,
                detail="gateway record retained for an explicit later reattachment or teardown",
                metadata={"gateway_session_id": session.session_id},
            )
        )
        detach_intent = _validated_gateway_detach_intent(session)
        detach_operation_id = cast(str, detach_intent["operation_id"])
        resources = [
            resource.model_copy(
                update={
                    "metadata": {
                        **resource.metadata,
                        "cleanup_operation_id": detach_operation_id,
                        "cancel_scheduler_job": False,
                    }
                }
            )
            for resource in resources
        ]
        detach_retryable = any(item.residual for item in resources)
        detached_at = utc_now().isoformat()
        updated = self.queue.update_gateway_session(
            session_id,
            state=GatewaySessionState.DEGRADED,
            expected_updated_at=session.updated_at,
            metadata={
                "detached_at": detached_at,
                "cleanup_retryable": detach_retryable,
                "cleanup_errors": errors,
                "detach_operation_id": detach_operation_id,
                "detach_retryable": detach_retryable,
                "detach_errors": errors,
            },
            gateway={
                **session.gateway,
                "detach": {
                    "schema_version": _GATEWAY_DETACH_RESULT_SCHEMA,
                    "operation_id": detach_operation_id,
                    "gateway_session_id": session_id,
                    "mode": "detach",
                    "completed_at": detached_at,
                    "retryable": detach_retryable,
                    "stopped_local_pid": stopped_local_pid,
                    "resources": [resource.model_dump(mode="json") for resource in resources],
                    "errors": errors,
                },
            },
        )
        return ServiceRuntimeStopResult(
            session=updated,
            mode="detach",
            stopped_local_pid=stopped_local_pid,
            stopped_remote_pid=None,
            canceled_scheduler_job=None,
            resources=resources,
            errors=errors,
        )

    def attach(self, *, session_id: str) -> ServiceRuntimeStartResult:
        """Serialize attachment against detach and teardown for this gateway."""
        session = self.queue.get_gateway_session(session_id)
        self._validate_gateway_transition_session(session)
        with self._gateway_transition_lock(session_id):
            return self._attach_serialized(session_id=session_id)

    def _attach_serialized(self, *, session_id: str) -> ServiceRuntimeStartResult:
        """Recreate the desktop connector while holding the gateway transition lock."""
        session = self.queue.get_gateway_session(session_id)
        self._validate_gateway_transition_session(session)
        if session.state == GatewaySessionState.CLOSED:
            raise ConfigurationError(f"gateway session {session_id} is closed")
        if session.gateway.get("teardown_intent") is not None:
            raise ConfigurationError(
                f"gateway session {session_id} is committed to teardown and cannot attach"
            )
        if session.gateway.get("detach_intent") is not None:
            completed_detach = self._completed_detach_result(session)
            if completed_detach is None:
                raise ConfigurationError(
                    f"gateway session {session_id} has an incomplete detach; retry detach or "
                    "tear down the runtime"
                )
            session = self._consume_completed_detach_for_attach(session)
        session = self._reconcile_ownership_intents(session)
        spec = ServiceRuntimeSpec.model_validate(session.gateway["runtime_spec"])
        verified_runtime: VerifiedJarvisServiceRuntime | None = None
        service_authorization: str | None = None
        binding_document = session.gateway.get("jarvis_runtime_binding")
        if binding_document is not None:
            try:
                verified_runtime = reverify_jarvis_service_runtime(
                    queue=self.queue,
                    definition=self.definition,
                    settings=self.settings,
                    binding_document=binding_document,
                )
            except ValueError as exc:
                raise RelayError(
                    f"JARVIS service runtime binding re-verification failed: {exc}"
                ) from exc
            runtime = verified_runtime.runtime
            if runtime.lifecycle != "ready":
                raise ConfigurationError("detached JARVIS service runtime is no longer ready")
            if (
                spec.deployment_driver != "jarvis-bound"
                or runtime.port != spec.service_port
                or runtime.protocol != spec.protocol
                or runtime.health_path != spec.health_path
                or runtime.live_data_path != spec.stream_path
                or runtime.events_path != spec.event_stream_path
                or runtime.state_path != spec.state_path
                or runtime.command_path != spec.command_path
            ):
                raise RelayError("detached JARVIS runtime endpoints changed before reattachment")
            service_authorization = self._jarvis_runtime_authorization(verified_runtime)
        transport = _object(session.gateway.get("transport", {}))
        proxy_name = _optional_str(transport.get("proxy_name"))
        if proxy_name is None:
            raise ConfigurationError("gateway session has no recorded transport proxy name")
        existing = _object(transport.get("desktop_connector", {}))
        existing_pid = _optional_int(existing.get("pid"))
        existing_config = _optional_str(existing.get("config_path"))
        existing_owned = (
            existing.get("owner") == "clio-relay"
            and existing.get("session_id") == session_id
            and existing_config is not None
        )
        created_connector = False
        local_connector: dict[str, object] | None = None
        try:
            if (
                existing_pid is not None
                and existing_owned
                and _local_connector_identity_status(existing)[0] == "owned"
            ):
                local_connector = existing
            else:
                local_intent = self._local_connector_intent(session)
                session = self._set_ownership_intent(
                    session,
                    "desktop_connector",
                    local_intent,
                )
                local_connector = self._start_local_visitor(
                    session=session,
                    spec=spec,
                    proxy_name=proxy_name,
                    ownership_intent=local_intent,
                )
                created_connector = True
                session = self._update(
                    session,
                    gateway=self._gateway_with_ownership_intent(
                        session,
                        "desktop_connector",
                        _new_ownership_intent("recorded", **local_connector),
                        transport={
                            **_object(session.gateway.get("transport", {})),
                            "desktop_connector": local_connector,
                        },
                    ),
                )
            connect_url = spec.connect_url_template.format(
                bind_addr=spec.desktop_bind_addr,
                bind_port=spec.desktop_bind_port,
                session_id=session.session_id,
            )
            health_url = (
                f"{spec.protocol}://{spec.desktop_bind_addr}:"
                f"{spec.desktop_bind_port}{spec.health_path}"
            )
            if verified_runtime is None:
                self._wait_for_local_health(
                    health_url,
                    spec.readiness_timeout_seconds,
                    spec.poll_seconds,
                    expected_body=spec.health_expected_body,
                )
            else:
                self._wait_for_jarvis_health(
                    health_url,
                    timeout_seconds=spec.readiness_timeout_seconds,
                    poll_seconds=spec.poll_seconds,
                    runtime_schema_version=verified_runtime.runtime.schema_version,
                    authorization=service_authorization,
                )
        except Exception as exc:
            cleanup_error: str | None = None
            if not created_connector:
                try:
                    recovered = self._reconcile_ownership_intents(
                        self.queue.get_gateway_session(session.session_id)
                    )
                    recovered_local = _object(
                        _object(recovered.gateway.get("transport", {})).get(
                            "desktop_connector",
                            {},
                        )
                    )
                    if recovered_local:
                        session = recovered
                        local_connector = recovered_local
                        created_connector = True
                except (ConfigurationError, RelayError) as recovery_exc:
                    cleanup_error = (
                        f"desktop connector rollback reconciliation failed: {recovery_exc}"
                    )
            if created_connector and local_connector is not None:
                _, rollback = self._stop_local_connector(
                    session_id=session.session_id,
                    connector=local_connector,
                    require_record=True,
                )
                if rollback.residual or not rollback.verified_after_operation:
                    cleanup_error = rollback.detail or "desktop connector rollback was not proven"
                else:
                    try:
                        self._remove_unpublished_local_connector_files(
                            session_id=session.session_id,
                            connector=local_connector,
                        )
                    except RelayError as cleanup_exc:
                        cleanup_error = str(cleanup_exc)
            self._record_attach_failure(
                session_id=session_id,
                error=exc,
                cleanup_error=cleanup_error,
            )
            raise
        try:
            stream_url = (
                f"{spec.protocol}://{spec.desktop_bind_addr}:"
                f"{spec.desktop_bind_port}{spec.stream_path}"
                if spec.stream_path is not None
                else None
            )
            events_url = (
                f"{spec.protocol}://{spec.desktop_bind_addr}:"
                f"{spec.desktop_bind_port}{spec.event_stream_path}"
                if spec.event_stream_path is not None
                else None
            )
            state_url = (
                f"{spec.protocol}://{spec.desktop_bind_addr}:"
                f"{spec.desktop_bind_port}{spec.state_path}"
                if spec.state_path is not None
                else None
            )
            command_url = (
                f"{spec.protocol}://{spec.desktop_bind_addr}:"
                f"{spec.desktop_bind_port}{spec.command_path}"
                if spec.command_path is not None
                else None
            )
            compatibility_urls = {
                name: (f"{spec.protocol}://{spec.desktop_bind_addr}:{spec.desktop_bind_port}{path}")
                for name, path in spec.compatibility_paths.items()
            }
            updated = self.queue.update_gateway_session(
                session_id,
                state=GatewaySessionState.READY,
                expected_updated_at=session.updated_at,
                metadata={"attached_at": utc_now().isoformat()},
                gateway={
                    **session.gateway,
                    "transport": {
                        **_object(session.gateway.get("transport", {})),
                        "desktop_connector": local_connector,
                    },
                },
            )
        except Exception as exc:
            cleanup_error: str | None = None
            if created_connector:
                _, rollback = self._stop_local_connector(
                    session_id=session.session_id,
                    connector=local_connector,
                    require_record=True,
                )
                if rollback.residual or not rollback.verified_after_operation:
                    cleanup_error = rollback.detail or "desktop connector rollback was not proven"
                else:
                    try:
                        self._remove_unpublished_local_connector_files(
                            session_id=session.session_id,
                            connector=local_connector,
                        )
                    except RelayError as cleanup_exc:
                        cleanup_error = str(cleanup_exc)
            self._record_attach_failure(
                session_id=session_id,
                error=exc,
                cleanup_error=cleanup_error,
            )
            raise
        return ServiceRuntimeStartResult(
            session=updated,
            connect_url=connect_url,
            health_url=health_url,
            stream_url=stream_url,
            compatibility_urls=compatibility_urls,
            events_url=events_url,
            state_url=state_url,
            command_url=command_url,
        )

    def _set_ownership_intent(
        self,
        session: GatewaySession,
        role: str,
        intent: dict[str, object],
    ) -> GatewaySession:
        """Durably record one resource intent before or after its side effect."""
        gateway = self._gateway_with_ownership_intent(session, role, intent)
        return self._update(session, gateway=gateway)

    def _prepare_detach_intent(self, session: GatewaySession) -> GatewaySession:
        """Persist or validate one detach operation before destructive cleanup."""
        raw_intent = session.gateway.get("detach_intent")
        if raw_intent is not None:
            _validated_gateway_detach_intent(session)
            return session
        raw_result = session.gateway.get("detach")
        versioned_result = (
            cast(dict[str, object], raw_result).get("schema_version")
            == _GATEWAY_DETACH_RESULT_SCHEMA
            if isinstance(raw_result, dict)
            else False
        )
        if versioned_result or session.metadata.get("detach_operation_id") is not None:
            raise RelayError("gateway detach evidence is invalid")
        operation_id = f"gateway_detach_{secrets.token_hex(16)}"
        created_at = utc_now().isoformat()
        gateway = dict(session.gateway)
        # A legacy, unversioned detach observation cannot be replayed as durable
        # evidence. A new operation supersedes it and proves the current state.
        gateway.pop("detach", None)
        gateway["detach_intent"] = {
            "schema_version": _GATEWAY_DETACH_INTENT_SCHEMA,
            "operation_id": operation_id,
            "gateway_session_id": session.session_id,
            "created_at": created_at,
        }
        return self.queue.update_gateway_session(
            session.session_id,
            expected_updated_at=session.updated_at,
            metadata={
                "detach_operation_id": operation_id,
                "detach_retryable": True,
                "detach_errors": [],
            },
            gateway=gateway,
        )

    def _completed_detach_result(
        self,
        session: GatewaySession,
    ) -> ServiceRuntimeStopResult | None:
        """Rehydrate exact completed detach evidence without repeating side effects."""
        intent = _validated_gateway_detach_intent(session)
        raw_result = session.gateway.get("detach")
        retryable = session.metadata.get("detach_retryable")
        result = cast(dict[str, object], raw_result) if isinstance(raw_result, dict) else None
        result_marks_completed = bool(
            result is not None
            and result.get("schema_version") == _GATEWAY_DETACH_RESULT_SCHEMA
            and result.get("retryable") is False
        )
        if retryable is True:
            if result_marks_completed:
                raise RelayError("gateway detach evidence is invalid")
            return None
        if retryable is not False:
            if result_marks_completed:
                raise RelayError("gateway detach evidence is invalid")
            return None
        if result is None or set(result) != {
            "schema_version",
            "operation_id",
            "gateway_session_id",
            "mode",
            "completed_at",
            "retryable",
            "stopped_local_pid",
            "resources",
            "errors",
        }:
            raise RelayError("gateway detach evidence is invalid")
        completed_at = result.get("completed_at")
        operation_id = cast(str, intent["operation_id"])
        if (
            result.get("schema_version") != _GATEWAY_DETACH_RESULT_SCHEMA
            or result.get("operation_id") != operation_id
            or result.get("gateway_session_id") != session.session_id
            or result.get("mode") != "detach"
            or result.get("retryable") is not False
            or not isinstance(completed_at, str)
            or session.state is not GatewaySessionState.DEGRADED
        ):
            raise RelayError("gateway detach evidence is invalid")
        _gateway_teardown_timestamp(completed_at)
        stopped_local_pid = _strict_optional_positive_int(result.get("stopped_local_pid"))
        resources, errors = _validated_completed_resource_lists(
            result,
            error="gateway detach evidence is invalid",
        )
        _validate_completed_detach_resources(
            session,
            resources=resources,
            stopped_local_pid=stopped_local_pid,
            operation_id=operation_id,
        )
        if not _completed_detach_metadata_matches(
            session,
            operation_id=operation_id,
            completed_at=completed_at,
            errors=errors,
        ):
            raise RelayError("gateway detach evidence is invalid")
        return ServiceRuntimeStopResult(
            session=session,
            mode="detach",
            stopped_local_pid=stopped_local_pid,
            stopped_remote_pid=None,
            canceled_scheduler_job=None,
            resources=resources,
            errors=errors,
        )

    def _consume_completed_detach_for_attach(self, session: GatewaySession) -> GatewaySession:
        """Retire one validated detach generation before creating its replacement connector."""
        gateway = dict(session.gateway)
        gateway.pop("detach", None)
        gateway.pop("detach_intent", None)
        return self.queue.update_gateway_session(
            session.session_id,
            expected_updated_at=session.updated_at,
            metadata={
                "detached_at": None,
                "detach_operation_id": None,
                "detach_retryable": None,
                "detach_errors": [],
            },
            gateway=gateway,
        )

    def _prepare_teardown_intent(
        self,
        session: GatewaySession,
        *,
        cancel_scheduler_job: bool,
    ) -> GatewaySession:
        """Persist an immutable cleanup policy before any teardown side effect."""
        return self.queue.prepare_gateway_teardown_intent(
            session.session_id,
            cancel_scheduler_job=cancel_scheduler_job,
        )

    def _validate_gateway_transition_session(self, session: GatewaySession) -> None:
        """Require one exact relay-owned session before and after lock acquisition."""
        if session.cluster != self.cluster:
            raise ConfigurationError(
                f"gateway session {session.session_id} belongs to cluster {session.cluster}, "
                f"not {self.cluster}"
            )
        if session.metadata.get("owner") != "clio-relay":
            raise ConfigurationError(
                f"gateway session {session.session_id} is not an owned clio-relay runtime"
            )

    def _gateway_transition_lock_path(self, session_id: str) -> Path:
        """Return a private lock path keyed by the exact cluster and gateway session."""
        directory = self.queue.root / ".gateway-transition-locks"
        try:
            ensure_private_configuration_directory(directory)
        except (ConfigurationError, OSError) as exc:
            raise RelayError(
                "could not prepare the trusted gateway transition lock directory"
            ) from exc
        identity = hashlib.sha256(f"{self.cluster}\0{session_id}".encode()).hexdigest()
        return directory / f"{identity}.lock"

    def _acquire_gateway_transition_lock(self, session_id: str) -> FileLock:
        """Acquire and return the exact bounded cross-process transition lock."""
        lock_path = self._gateway_transition_lock_path(session_id)
        lock = FileLock(
            str(internal_filesystem_path(lock_path, force_extended=True)),
            timeout=_GATEWAY_TEARDOWN_LOCK_TIMEOUT_SECONDS,
        )
        try:
            lock.acquire()
        except FileLockTimeout as exc:
            raise RelayError("timed out acquiring the gateway transition lock") from exc
        except OSError as exc:
            raise RelayError("could not acquire the gateway transition lock") from exc
        return lock

    @contextmanager
    def _gateway_transition_lock(self, session_id: str) -> Generator[None, None, None]:
        """Hold the bounded cross-process lock for one gateway state transition."""
        lock = self._acquire_gateway_transition_lock(session_id)
        try:
            yield
        finally:
            lock.release()

    def _runtime_start_session_after_lock(self, session_id: str) -> GatewaySession:
        """Reread and admit one newly created gateway before any runtime side effect."""
        session = self.queue.get_gateway_session(session_id)
        self._validate_gateway_transition_session(session)
        if session.state is not GatewaySessionState.CREATED:
            raise ConfigurationError(
                f"gateway session {session_id} changed before runtime start acquired its lock"
            )
        if session.gateway.get("teardown_intent") is not None:
            raise ConfigurationError(
                f"gateway session {session_id} is committed to teardown and cannot start"
            )
        return session

    def _prepare_teardown_policy(
        self,
        session: GatewaySession,
        *,
        cancel_scheduler_job: bool,
        final_state: GatewaySessionState,
    ) -> GatewaySession:
        """Persist or validate immutable cleanup policy before cleanup side effects."""
        intent = _validated_gateway_teardown_intent(
            session,
            cancel_scheduler_job=cancel_scheduler_job,
        )
        raw_policy = session.gateway.get("teardown_policy")
        if raw_policy is not None:
            self._validate_teardown_policy(
                session,
                cancel_scheduler_job=cancel_scheduler_job,
                final_state=final_state,
            )
            return session
        if session.state is GatewaySessionState.CLOSED or (
            session.metadata.get("cleanup_retryable") is False
            and session.gateway.get("teardown") is not None
        ):
            raise RelayError("completed gateway teardown evidence is invalid")
        policy: dict[str, object] = {
            "schema_version": _GATEWAY_TEARDOWN_POLICY_SCHEMA,
            "operation_id": intent["operation_id"],
            "gateway_session_id": session.session_id,
            "cancel_scheduler_job": cancel_scheduler_job,
            "final_state": final_state.value,
            "committed_at": utc_now().isoformat(),
        }
        return self.queue.update_gateway_session(
            session.session_id,
            expected_updated_at=session.updated_at,
            metadata={
                "cleanup_at": None,
                "closed_at": None,
                "cancel_scheduler_job": cancel_scheduler_job,
                "cleanup_retryable": True,
                "cleanup_errors": [],
                "cleanup_operation_id": intent["operation_id"],
            },
            gateway={**session.gateway, "teardown_policy": policy},
        )

    def _validate_teardown_policy(
        self,
        session: GatewaySession,
        *,
        cancel_scheduler_job: bool,
        final_state: GatewaySessionState,
    ) -> dict[str, object]:
        """Validate the exact immutable cleanup policy committed for this operation."""
        intent = _validated_gateway_teardown_intent(
            session,
            cancel_scheduler_job=cancel_scheduler_job,
        )
        raw_policy = session.gateway.get("teardown_policy")
        if not isinstance(raw_policy, dict):
            raise RelayError("gateway teardown policy is invalid")
        policy = cast(dict[str, object], raw_policy)
        if set(policy) != {
            "schema_version",
            "operation_id",
            "gateway_session_id",
            "cancel_scheduler_job",
            "final_state",
            "committed_at",
        }:
            raise RelayError("gateway teardown policy is invalid")
        committed_at = policy.get("committed_at")
        if (
            policy.get("schema_version") != _GATEWAY_TEARDOWN_POLICY_SCHEMA
            or policy.get("operation_id") != intent["operation_id"]
            or policy.get("gateway_session_id") != session.session_id
            or not isinstance(committed_at, str)
        ):
            raise RelayError("gateway teardown policy is invalid")
        _gateway_teardown_timestamp(committed_at)
        if policy.get("cancel_scheduler_job") is not cancel_scheduler_job:
            raise RelayError(
                "gateway cleanup policy changed during retry; resume with the original "
                f"cancel_scheduler_job={policy.get('cancel_scheduler_job')} policy"
            )
        if policy.get("final_state") != final_state.value:
            raise RelayError(
                "gateway cleanup final-state policy changed during retry; resume with the "
                f"original final_state={policy.get('final_state')} policy"
            )
        return policy

    def _completed_teardown_result(
        self,
        session: GatewaySession,
        *,
        cancel_scheduler_job: bool,
        final_state: GatewaySessionState,
    ) -> ServiceRuntimeStopResult | None:
        """Rehydrate exact non-retryable teardown evidence without repeating side effects."""
        raw_result = session.gateway.get("teardown")
        retryable = session.metadata.get("cleanup_retryable")
        typed_result = cast(dict[str, object], raw_result) if isinstance(raw_result, dict) else None
        result_marks_completed = bool(
            typed_result is not None
            and typed_result.get("schema_version") == _GATEWAY_TEARDOWN_RESULT_SCHEMA
            and typed_result.get("retryable") is False
        )
        if retryable is True:
            if result_marks_completed or session.state is GatewaySessionState.CLOSED:
                raise RelayError("completed gateway teardown evidence is invalid")
            return None
        if retryable is not False:
            if result_marks_completed or session.state is GatewaySessionState.CLOSED:
                raise RelayError("completed gateway teardown evidence is invalid")
            return None
        policy = self._validate_teardown_policy(
            session,
            cancel_scheduler_job=cancel_scheduler_job,
            final_state=final_state,
        )
        if typed_result is None:
            raise RelayError("completed gateway teardown evidence is invalid")
        result = typed_result
        expected_fields = {
            "schema_version",
            "operation_id",
            "gateway_session_id",
            "mode",
            "cancel_scheduler_job",
            "requested_final_state",
            "effective_state",
            "completed_at",
            "retryable",
            "stopped_local_pid",
            "stopped_remote_pid",
            "canceled_scheduler_job",
            "resources",
            "errors",
        }
        if set(result) != expected_fields:
            raise RelayError("completed gateway teardown evidence is invalid")
        operation_id = cast(str, policy["operation_id"])
        completed_at = result.get("completed_at")
        if (
            result.get("schema_version") != _GATEWAY_TEARDOWN_RESULT_SCHEMA
            or result.get("operation_id") != operation_id
            or result.get("gateway_session_id") != session.session_id
            or result.get("mode") != "teardown"
            or result.get("cancel_scheduler_job") is not cancel_scheduler_job
            or result.get("requested_final_state") != final_state.value
            or result.get("effective_state") != final_state.value
            or result.get("retryable") is not False
            or not isinstance(completed_at, str)
            or session.state.value != result.get("effective_state")
        ):
            raise RelayError("completed gateway teardown evidence is invalid")
        _gateway_teardown_timestamp(completed_at)
        stopped_local_pid = _strict_optional_positive_int(result.get("stopped_local_pid"))
        stopped_remote_pid = _strict_optional_positive_int(result.get("stopped_remote_pid"))
        canceled_scheduler_job = _strict_optional_nonempty_str(result.get("canceled_scheduler_job"))
        resources, errors = _validated_completed_resource_lists(
            result,
            error="completed gateway teardown evidence is invalid",
        )
        if errors or any(resource.residual for resource in resources):
            raise RelayError("completed gateway teardown evidence is invalid")
        _validate_completed_teardown_resources(
            session,
            resources=resources,
            stopped_local_pid=stopped_local_pid,
            stopped_remote_pid=stopped_remote_pid,
            canceled_scheduler_job=canceled_scheduler_job,
            operation_id=operation_id,
            cancel_scheduler_job=cancel_scheduler_job,
        )
        if not _completed_teardown_metadata_matches(
            session,
            operation_id=operation_id,
            cancel_scheduler_job=cancel_scheduler_job,
            completed_at=completed_at,
            final_state=final_state,
            errors=errors,
        ):
            raise RelayError("completed gateway teardown evidence is invalid")
        return ServiceRuntimeStopResult(
            session=session,
            mode="teardown",
            stopped_local_pid=stopped_local_pid,
            stopped_remote_pid=stopped_remote_pid,
            canceled_scheduler_job=canceled_scheduler_job,
            resources=resources,
            errors=errors,
        )

    def _gateway_with_ownership_intent(
        self,
        session: GatewaySession,
        role: str,
        intent: dict[str, object],
        **gateway_updates: object,
    ) -> dict[str, object]:
        """Return a gateway payload containing an atomically paired intent update."""
        gateway = dict(session.gateway)
        intents = _object(gateway.get("ownership_intents", {}))
        intents[role] = intent
        gateway["ownership_intents"] = intents
        gateway.update(gateway_updates)
        return gateway

    def _local_connector_intent(self, session: GatewaySession) -> dict[str, object]:
        """Build the exact durable identity needed to rediscover a local connector."""
        runtime_dir = (
            self.settings.core_dir.parent / "runtime-sessions" / session.session_id
        ).resolve()
        return _new_ownership_intent(
            "starting",
            owner_token=secrets.token_hex(32),
            connector_generation_id=secrets.token_hex(16),
            config_path=str(runtime_dir / "desktop-frpc.toml"),
            stdout_path=str(runtime_dir / "desktop-frpc.out"),
            stderr_path=str(runtime_dir / "desktop-frpc.err"),
            metadata_path=str(runtime_dir / "desktop-frpc-owner.json"),
        )

    def _reconcile_ownership_intents(self, session: GatewaySession) -> GatewaySession:
        """Recover scheduler and connector identities written before a hard exit."""
        gateway = dict(session.gateway)
        intents = _object(gateway.get("ownership_intents", {}))
        if not intents:
            return session
        transport = _object(gateway.get("transport", {}))
        changed = False
        scheduler_job_id = session.scheduler_job_id

        scheduler_intent = _object(intents.get("scheduler_submission", {}))
        if scheduler_job_id is None and scheduler_intent.get("state") == "recorded":
            recorded_scheduler_job_id = _optional_str(scheduler_intent.get("scheduler_job_id"))
            if recorded_scheduler_job_id is not None:
                scheduler_job_id = recorded_scheduler_job_id
                changed = True
        if scheduler_job_id is None and scheduler_intent.get("state") == "starting":
            submission_id = _optional_str(scheduler_intent.get("submission_id"))
            scheduler_provider = _optional_str(scheduler_intent.get("scheduler_provider"))
            submission_marker = _optional_str(scheduler_intent.get("submission_marker"))
            if (
                submission_id is not None
                and scheduler_provider is not None
                and submission_marker is not None
            ):
                try:
                    record = _last_json_object(
                        self._ssh(
                            _remote_submission_record_script(
                                session_id=session.session_id,
                                submission_id=submission_id,
                                scheduler_provider=scheduler_provider,
                                submission_marker=submission_marker,
                            )
                        )
                    )
                    if record.get("present") is True:
                        output = record.get("output")
                        if (
                            record.get("session_id") != session.session_id
                            or record.get("submission_id") != submission_id
                            or record.get("scheduler_provider") != scheduler_provider
                            or record.get("submission_marker") != submission_marker
                            or record.get("returncode") != 0
                            or not isinstance(output, str)
                        ):
                            raise RelayError("scheduler submission sidecar identity is invalid")
                        submission = _parse_runtime_submission(output)
                        scheduler_job_id = submission.scheduler_job_id
                        intents["scheduler_submission"] = _new_ownership_intent(
                            "recorded",
                            submission_id=submission_id,
                            scheduler_provider=scheduler_provider,
                            submission_marker=submission_marker,
                            scheduler_job_id=scheduler_job_id,
                            reconciled=True,
                        )
                        gateway["submit_output"] = output.strip()
                        changed = True
                except RelayError as exc:
                    scheduler_intent["reconciliation_error"] = str(exc)
                    intents["scheduler_submission"] = scheduler_intent
                    changed = True

        remote_intent = _object(intents.get("remote_connector", {}))
        if not _object(transport.get("remote_connector", {})) and remote_intent.get("state") in {
            "starting",
            "recorded",
        }:
            owner_token = _optional_str(remote_intent.get("owner_token"))
            generation_id = _optional_str(remote_intent.get("connector_generation_id"))
            if owner_token is not None and generation_id is not None:
                try:
                    allocation_placement = _object(remote_intent.get("placement", {}))
                    result = _last_json_object(
                        self._ssh(
                            _remote_connector_discovery_script(
                                session_id=session.session_id,
                                owner_token=owner_token,
                                connector_generation_id=generation_id,
                                allocation_provider=_optional_str(
                                    remote_intent.get("scheduler_provider")
                                ),
                                allocation_job_id=_optional_str(
                                    remote_intent.get("scheduler_native_id")
                                ),
                                allocation_step_marker=_optional_str(
                                    remote_intent.get("scheduler_step_marker")
                                ),
                                allocation_placement_host=_optional_str(
                                    allocation_placement.get("placement_host")
                                ),
                            )
                        )
                    )
                    connector = result.get("connector")
                    if remote_intent.get("execution_scope") == "scheduler_allocation":
                        if result.get("ownership_verified") is not True:
                            detail = result.get("error")
                            raise RelayError(
                                detail
                                if isinstance(detail, str)
                                else "allocation connector sidecar could not be verified"
                            )
                        typed_connector, absence_verified = (
                            self._reconcile_allocation_connector_intent(
                                session_id=session.session_id,
                                intent=remote_intent,
                                connector_base=(
                                    cast(dict[str, object], connector)
                                    if isinstance(connector, dict)
                                    else None
                                ),
                            )
                        )
                        if typed_connector is not None:
                            transport["remote_connector"] = typed_connector
                            intents["remote_connector"] = _new_ownership_intent(
                                "recorded",
                                reconciled=True,
                                **typed_connector,
                            )
                            changed = True
                        elif absence_verified:
                            intents["remote_connector"] = _new_ownership_intent(
                                "absent_verified",
                                owner_token=owner_token,
                                connector_generation_id=generation_id,
                                execution_scope="scheduler_allocation",
                                scheduler_provider=remote_intent.get("scheduler_provider"),
                                scheduler_native_id=remote_intent.get("scheduler_native_id"),
                                scheduler_step_marker=remote_intent.get("scheduler_step_marker"),
                                placement=remote_intent.get("placement"),
                                reconciled=True,
                            )
                            changed = True
                    elif (
                        result.get("ownership_verified") is True
                        and result.get("present") is True
                        and isinstance(connector, dict)
                    ):
                        typed_connector = cast(dict[str, object], connector)
                        transport["remote_connector"] = typed_connector
                        intents["remote_connector"] = _new_ownership_intent(
                            "recorded",
                            reconciled=True,
                            **typed_connector,
                        )
                        changed = True
                    elif (
                        result.get("ownership_verified") is True
                        and result.get("present") is False
                        and result.get("matching_pids") == []
                    ):
                        intents["remote_connector"] = _new_ownership_intent(
                            "absent_verified",
                            owner_token=owner_token,
                            connector_generation_id=generation_id,
                            reconciled=True,
                        )
                        changed = True
                    elif result.get("ownership_verified") is False:
                        detail = result.get("error")
                        remote_intent["reconciliation_error"] = (
                            detail
                            if isinstance(detail, str)
                            else "remote connector ownership observation was incomplete"
                        )
                        intents["remote_connector"] = remote_intent
                        changed = True
                except RelayError as exc:
                    remote_intent["reconciliation_error"] = str(exc)
                    intents["remote_connector"] = remote_intent
                    changed = True

        local_intent = _object(intents.get("desktop_connector", {}))
        if not _object(transport.get("desktop_connector", {})) and local_intent.get("state") in {
            "starting",
            "recorded",
        }:
            try:
                connector, absence_verified = _discover_local_connector(
                    local_intent,
                    session_id=session.session_id,
                )
                if connector is not None:
                    transport["desktop_connector"] = connector
                    intents["desktop_connector"] = _new_ownership_intent(
                        "recorded",
                        reconciled=True,
                        **connector,
                    )
                    changed = True
                elif absence_verified:
                    intents["desktop_connector"] = _new_ownership_intent(
                        "absent_verified",
                        owner_token=local_intent.get("owner_token"),
                        connector_generation_id=local_intent.get("connector_generation_id"),
                        config_path=local_intent.get("config_path"),
                        reconciled=True,
                    )
                    changed = True
            except RelayError as exc:
                local_intent["reconciliation_error"] = str(exc)
                intents["desktop_connector"] = local_intent
                changed = True

        if not changed:
            return session
        gateway["ownership_intents"] = intents
        gateway["transport"] = transport
        if scheduler_job_id is not None:
            return self._update(
                session,
                gateway=gateway,
                scheduler_job_id=scheduler_job_id,
                queue_state=session.queue_state or "submitted",
            )
        return self._update(session, gateway=gateway)

    def _reconcile_allocation_connector_intent(
        self,
        *,
        session_id: str,
        intent: dict[str, object],
        connector_base: dict[str, object] | None,
    ) -> tuple[dict[str, object] | None, bool]:
        """Recover or disprove an allocation connector by its provider marker."""
        provider_name = _required_intent_str(intent, "scheduler_provider")
        scheduler_job_id = _required_intent_str(intent, "scheduler_native_id")
        step_marker = _required_intent_str(intent, "scheduler_step_marker")
        generation_id = _required_intent_str(intent, "connector_generation_id")
        try:
            placement = SchedulerConnectorPlacement.model_validate_json(
                json.dumps(intent.get("placement"), separators=(",", ":"), allow_nan=False)
            )
        except (TypeError, ValueError) as exc:
            raise RelayError("allocation connector intent has invalid placement") from exc
        if (
            intent.get("execution_scope") != "scheduler_allocation"
            or placement.scheduler != provider_name
            or placement.scheduler_job_id != scheduler_job_id
            or step_marker != _connector_step_marker(session_id, generation_id)
        ):
            raise RelayError("allocation connector recovery identity does not match its intent")
        if connector_base is not None and (
            connector_base.get("owner") != "clio-relay"
            or connector_base.get("session_id") != session_id
            or connector_base.get("execution_scope") != "scheduler_allocation"
            or connector_base.get("scheduler_provider") != provider_name
            or connector_base.get("scheduler_native_id") != scheduler_job_id
            or connector_base.get("scheduler_step_marker") != step_marker
            or connector_base.get("connector_generation_id") != generation_id
            or connector_base.get("owner_token") != intent.get("owner_token")
            or connector_base.get("placement") != intent.get("placement")
            or _optional_str(connector_base.get("config_path")) is None
            or _optional_str(connector_base.get("log_path")) is None
            or connector_base.get("pid") is not None
        ):
            raise RelayError("allocation connector sidecar identity does not match its intent")
        record = _last_json_object(
            self._ssh(
                _remote_connector_step_reconcile_script(
                    definition=self.definition,
                    provider=provider_name,
                    scheduler_job_id=scheduler_job_id,
                    step_marker=step_marker,
                    placement_host=placement.placement_host,
                )
            )
        )
        if (
            record.get("schema_version") != "clio-relay.scheduler-connector-step-reconciliation.v1"
            or record.get("scheduler") != provider_name
            or record.get("scheduler_job_id") != scheduler_job_id
            or record.get("step_marker") != step_marker
            or record.get("placement_host") != placement.placement_host
            or not isinstance(record.get("found"), bool)
        ):
            raise RelayError("scheduler step reconciliation returned mismatched identity")
        if record.get("found") is False:
            if record.get("step") is not None:
                raise RelayError("scheduler step reconciliation contradicted step absence")
            return None, True
        if connector_base is None:
            raise RelayError("active scheduler connector step has no durable allocation sidecar")
        try:
            step = SchedulerConnectorStepIdentity.model_validate_json(
                json.dumps(record.get("step"), separators=(",", ":"), allow_nan=False)
            )
        except (TypeError, ValueError) as exc:
            raise RelayError("scheduler step reconciliation returned invalid identity") from exc
        connector = {
            **connector_base,
            "scheduler_step_id": step.scheduler_step_id,
            "scheduler_step": step.model_dump(mode="json"),
        }
        self._allocation_connector_identity(
            session_id=session_id,
            connector=connector,
        )
        status = self._poll_allocation_connector_step(step)
        if status.state == "absent":
            return None, True
        return connector, False

    def _verified_scheduler_submission(
        self,
        session: GatewaySession,
        *,
        allow_quiesced_owner_source_recovery: bool = False,
    ) -> _VerifiedSchedulerSubmission:
        """Prove the exact provider and job ID from the relay-created remote sidecar."""
        scheduler_job_id = _optional_str(session.scheduler_job_id)
        if scheduler_job_id is None:
            raise RelayError("scheduler ownership verification requires an exact job id")
        try:
            spec = ServiceRuntimeSpec.model_validate(session.gateway.get("runtime_spec"))
        except ValueError as exc:
            raise RelayError("owned runtime has no valid service runtime specification") from exc
        binding_document = session.gateway.get("jarvis_runtime_binding")
        if binding_document is not None:
            try:
                verified = reverify_jarvis_service_runtime(
                    queue=self.queue,
                    definition=self.definition,
                    settings=self.settings,
                    binding_document=binding_document,
                )
            except (ConfigurationError, RelayError):
                if not (
                    allow_quiesced_owner_source_recovery
                    and self._quiesced_owner_source_recovery_is_authorized(session)
                ):
                    raise
                try:
                    verified = reverify_jarvis_service_runtime(
                        queue=self.queue,
                        definition=self.definition,
                        settings=None,
                        binding_document=binding_document,
                    )
                except ValueError as exc:
                    raise RelayError(
                        f"JARVIS service runtime binding re-verification failed: {exc}"
                    ) from exc
            except ValueError as exc:
                raise RelayError(
                    f"JARVIS service runtime binding re-verification failed: {exc}"
                ) from exc
            binding = verified.binding
            if (
                binding.scheduler_provider is None
                or binding.scheduler_native_id is None
                or binding.scheduler_provider != session.scheduler
                or binding.scheduler_native_id != scheduler_job_id
                or spec.scheduler != session.scheduler
            ):
                raise RelayError(
                    "scheduler identity disagrees with the verified JARVIS runtime binding"
                )
            return _VerifiedSchedulerSubmission(
                provider=binding.scheduler_provider,
                scheduler_job_id=binding.scheduler_native_id,
                spec=spec,
            )
        intents = _object(session.gateway.get("ownership_intents", {}))
        scheduler_intent = _object(intents.get("scheduler_submission", {}))
        if (
            scheduler_intent.get("schema_version") != _OWNERSHIP_INTENT_SCHEMA
            or scheduler_intent.get("state") != "recorded"
        ):
            raise RelayError(
                "scheduler ownership is not backed by a recorded relay submission intent"
            )
        submission_id = _optional_str(scheduler_intent.get("submission_id"))
        intent_provider = _optional_str(scheduler_intent.get("scheduler_provider"))
        submission_marker = _optional_str(scheduler_intent.get("submission_marker"))
        intent_job_id = _optional_str(scheduler_intent.get("scheduler_job_id"))
        if None in {
            submission_id,
            intent_provider,
            submission_marker,
            intent_job_id,
        }:
            raise RelayError("recorded scheduler ownership intent has incomplete identity")
        assert submission_id is not None
        assert intent_provider is not None
        assert submission_marker is not None
        assert intent_job_id is not None
        try:
            canonical_provider = provider_for_scheduler(session.scheduler).name
        except ConfigurationError as exc:
            raise RelayError(f"scheduler provider identity is invalid: {exc}") from exc
        if (
            session.scheduler != canonical_provider
            or intent_provider != canonical_provider
            or spec.scheduler != canonical_provider
        ):
            raise RelayError(
                "scheduler provider identity disagrees between the runtime, "
                "submission intent, and runtime specification"
            )
        if intent_job_id != scheduler_job_id:
            raise RelayError(
                "scheduler job identity disagrees between the gateway and submission intent"
            )
        record = _last_json_object(
            self._ssh(
                _remote_submission_record_script(
                    session_id=session.session_id,
                    submission_id=submission_id,
                    scheduler_provider=intent_provider,
                    submission_marker=submission_marker,
                )
            )
        )
        output = record.get("output")
        if (
            record.get("schema_version") != "clio-relay.gateway-submission-sidecar.v1"
            or record.get("present") is not True
            or record.get("session_id") != session.session_id
            or record.get("submission_id") != submission_id
            or record.get("scheduler_provider") != canonical_provider
            or record.get("submission_marker") != submission_marker
            or record.get("returncode") != 0
            or record.get("output_truncated") is True
            or not isinstance(output, str)
        ):
            raise RelayError("scheduler submission sidecar identity is invalid")
        submission = _parse_runtime_submission(output)
        if submission.scheduler_job_id != scheduler_job_id:
            raise RelayError("scheduler job identity disagrees with the anchored submission output")
        return _VerifiedSchedulerSubmission(
            provider=canonical_provider,
            scheduler_job_id=scheduler_job_id,
            spec=spec,
        )

    def _quiesced_owner_source_recovery_is_authorized(
        self,
        session: GatewaySession,
    ) -> bool:
        """Authorize a non-canceling direct source read for an exact closing owner."""
        teardown_intent = _object(session.gateway.get("teardown_intent", {}))
        owner_session_id = _optional_str(session.metadata.get("owner_session_id"))
        generation_id = _optional_str(session.metadata.get("owner_session_generation_id"))
        admission_id = _optional_str(session.metadata.get("owner_session_admission_id"))
        if owner_session_id is None or generation_id is None or admission_id is None:
            return False
        try:
            expected_admission_id = desktop_owner_session_admission_id(
                cluster=self.cluster,
                session_id=owner_session_id,
            )
        except ValueError:
            return False
        if (
            teardown_intent.get("schema_version") != "clio-relay.gateway-teardown-intent.v1"
            or teardown_intent.get("gateway_session_id") != session.session_id
            or teardown_intent.get("cancel_scheduler_job") is not False
            or self.settings.owner_session_id != owner_session_id
            or self.settings.owner_session_generation_id != generation_id
            or self.settings.resolved_owner_session_cluster() != self.cluster
            or admission_id != expected_admission_id
        ):
            return False
        try:
            cleanup_intent = self.queue.get_owner_session_cleanup_intent(
                admission_id,
                session_generation_id=generation_id,
            )
        except (OSError, QueueConflictError, ValueError):
            return False
        return bool(
            cleanup_intent is not None
            and cleanup_intent.get("schema_version") == "clio-relay.owner-session-cleanup-intent.v1"
            and cleanup_intent.get("owner_session_id") == admission_id
            and cleanup_intent.get("session_generation_id") == generation_id
            and cleanup_intent.get("cancel_scheduler_jobs") is False
            and isinstance(cleanup_intent.get("operation_id"), str)
            and bool(cleanup_intent.get("operation_id"))
        )

    def _stop_local_connector(
        self,
        *,
        session_id: str,
        connector: dict[str, object],
        require_record: bool = False,
        absence_verified: bool = False,
    ) -> tuple[int | None, CleanupResource]:
        pid = _optional_int(connector.get("pid"))
        config_path = _optional_str(connector.get("config_path"))
        expected_directory = (
            self.settings.core_dir.parent / "runtime-sessions" / session_id
        ).resolve()
        config_owned = False
        if config_path is not None:
            try:
                config_owned = Path(config_path).resolve().parent == expected_directory
            except OSError:
                config_owned = False
        owned = (
            connector.get("owner") == "clio-relay"
            and connector.get("session_id") == session_id
            and config_owned
        )
        resource_id = str(pid) if pid is not None else session_id
        identity_status, identity_detail = _local_connector_identity_status(connector)
        if pid is None:
            residual = require_record and not absence_verified
            return None, CleanupResource(
                kind="desktop_connector",
                resource_id=resource_id,
                location="desktop",
                action="stop",
                ownership_verified=absence_verified,
                outcome="refused" if residual else "missing",
                verified_after_operation=absence_verified,
                residual=residual,
                detail=(
                    "owned desktop connector record is missing"
                    if residual
                    else "no desktop connector was recorded"
                ),
            )
        if identity_status in {"missing", "replaced"}:
            try:
                no_group_members = not _local_connector_group_members(connector)
            except RelayError as exc:
                return None, CleanupResource(
                    kind="desktop_connector",
                    resource_id=resource_id,
                    location="desktop",
                    action="stop",
                    ownership_verified=False,
                    outcome="failed",
                    residual=True,
                    detail=str(exc),
                )
            durable_identity = (
                owned
                and _optional_str(connector.get("owner_token")) is not None
                and _optional_int(connector.get("process_group_id")) is not None
                and _optional_str(connector.get("process_start_marker")) is not None
                and no_group_members
            )
            return None, CleanupResource(
                kind="desktop_connector",
                resource_id=resource_id,
                location="desktop",
                action="stop",
                ownership_verified=durable_identity,
                outcome="missing" if durable_identity else "refused",
                verified_after_operation=durable_identity,
                residual=not durable_identity,
                detail=identity_detail,
            )
        if not owned or identity_status != "owned":
            return None, CleanupResource(
                kind="desktop_connector",
                resource_id=resource_id,
                location="desktop",
                action="stop",
                ownership_verified=False,
                outcome="refused",
                residual=True,
                detail=identity_detail
                or "connector process does not match the owned session record",
            )
        try:
            stopped = _terminate_local_connector(connector)
            residual = bool(_local_connector_group_members(connector))
        except RelayError as exc:
            return None, CleanupResource(
                kind="desktop_connector",
                resource_id=resource_id,
                location="desktop",
                action="stop",
                ownership_verified=False,
                outcome="failed",
                residual=True,
                detail=str(exc),
            )
        return stopped, CleanupResource(
            kind="desktop_connector",
            resource_id=resource_id,
            location="desktop",
            action="stop",
            ownership_verified=True,
            outcome="failed" if residual else "stopped",
            verified_after_operation=not residual,
            residual=residual,
            detail="connector still running after termination" if residual else None,
        )

    def _remove_unpublished_local_connector_files(
        self,
        *,
        session_id: str,
        connector: dict[str, object],
    ) -> None:
        """Remove private files for a connector that failed before durable publication."""

        expected_directory = (
            self.settings.core_dir.parent / "runtime-sessions" / session_id
        ).resolve()
        paths: list[Path] = []
        for field in ("config_path", "stdout_path", "stderr_path", "metadata_path"):
            raw_path = _optional_str(connector.get(field))
            if raw_path is None:
                raise RelayError(f"unpublished desktop connector omitted {field}")
            path = Path(raw_path).resolve()
            if path.parent != expected_directory:
                raise RelayError("unpublished desktop connector path escaped its runtime directory")
            paths.append(path)
        try:
            for path in paths:
                path.unlink(missing_ok=True)
        except OSError as exc:
            raise RelayError("could not remove unpublished desktop connector files") from exc

    def _wait_for_allocation_and_health(
        self,
        session: GatewaySession,
        spec: ServiceRuntimeSpec,
        scheduler_job_id: str,
        initial_service_host: str | None = None,
    ) -> str:
        deadline = time.time() + spec.readiness_timeout_seconds
        last_status = ""
        current_session = session
        while time.time() < deadline:
            provider_status = (
                self._poll_scheduler_provider(
                    provider=spec.scheduler,
                    scheduler_job_id=scheduler_job_id,
                )
                if provider_for_scheduler(spec.scheduler).name != "external"
                else None
            )
            if initial_service_host is not None:
                scheduler_state = (
                    provider_status.phase.value if provider_status is not None else "allocated"
                )
                node = initial_service_host
                reason = provider_status.reason if provider_status is not None else None
                runtime_events: list[dict[str, object]] | None = None
                status_text = json.dumps(
                    {
                        "scheduler_job_id": scheduler_job_id,
                        "service_host": initial_service_host,
                    },
                    sort_keys=True,
                )
            else:
                if spec.status_command is None:
                    raise ConfigurationError(
                        "service host was not reported by submission output; "
                        "ServiceRuntimeSpec.status_command is required"
                    )
                status_text = self._ssh(
                    _template_command_script(spec.status_command, scheduler_job_id)
                )
                status = _parse_runtime_status(status_text)
                scheduler_state = (
                    provider_status.phase.value
                    if provider_status is not None
                    else status.state or "unknown"
                )
                node = status.service_host
                reason = (
                    provider_status.reason
                    if provider_status is not None and provider_status.reason is not None
                    else status.reason
                )
                runtime_events = status.events
            last_status = status_text.strip()
            state = (
                GatewaySessionState.ALLOCATED if node is not None else GatewaySessionState.PENDING
            )
            current_session = self._update(
                current_session,
                state=state,
                queue_state=scheduler_state.lower() if scheduler_state else None,
                node=node,
                gateway={
                    **current_session.gateway,
                    "scheduler_status": {
                        "raw": last_status,
                        "state": scheduler_state,
                        "reason": reason,
                        "provider": (
                            provider_status.model_dump(mode="json")
                            if provider_status is not None
                            else None
                        ),
                    },
                    "runtime_events": runtime_events or [],
                },
            )
            if node is not None:
                health = self._ssh(
                    _remote_http_probe_script(
                        node,
                        spec.service_port,
                        spec.health_path,
                        expected_body=spec.health_expected_body,
                    )
                )
                if "service_health=ok" in health:
                    return node
            self.sleep(spec.poll_seconds)
        raise RelayError(
            f"service did not become healthy before timeout; job={scheduler_job_id} "
            f"last_status={last_status!r}"
        )

    def _retained_scheduler_resource(
        self,
        *,
        session: GatewaySession,
        spec: ServiceRuntimeSpec,
    ) -> CleanupResource:
        scheduler_job_id = session.scheduler_job_id
        if scheduler_job_id is None:
            raise ConfigurationError("scheduler retention requires a scheduler job id")
        try:
            provider = provider_for_scheduler(session.scheduler)
            if provider.name == "external":
                observed_state = self._observe_runtime_state(
                    spec=spec,
                    scheduler_job_id=scheduler_job_id,
                )
            else:
                provider_status = self._poll_scheduler_provider(
                    provider=provider.name,
                    scheduler_job_id=scheduler_job_id,
                )
                if (
                    provider_status.phase is SchedulerPhase.UNKNOWN
                    and provider_status.active_record_found is False
                ):
                    return CleanupResource(
                        kind="scheduler_job",
                        resource_id=scheduler_job_id,
                        location=self.definition.ssh_host,
                        provider=session.scheduler,
                        action="retain",
                        metadata={
                            "gateway_session_id": session.session_id,
                            "scheduler_status": provider_status.model_dump(mode="json"),
                        },
                        ownership_verified=True,
                        outcome="missing",
                        verified_after_operation=True,
                        observed_state="missing",
                        residual=False,
                        detail=(
                            "scheduler cancellation was not requested; the provider proved "
                            "that no active scheduler record remained; no completed or "
                            "canceled state is claimed"
                        ),
                    )
                observed_state = provider_status.phase.value
        except RelayError as exc:
            return CleanupResource(
                kind="scheduler_job",
                resource_id=scheduler_job_id,
                location=self.definition.ssh_host,
                provider=session.scheduler,
                action="retain",
                metadata={"gateway_session_id": session.session_id},
                ownership_verified=True,
                outcome="failed",
                verified_after_operation=False,
                residual=True,
                detail=(
                    "scheduler cancellation was not requested, but retained-state "
                    f"verification failed: {exc}"
                ),
            )
        if observed_state in {"missing", "not-found", "not_found", "unknown"}:
            return CleanupResource(
                kind="scheduler_job",
                resource_id=scheduler_job_id,
                location=self.definition.ssh_host,
                provider=session.scheduler,
                action="retain",
                metadata={"gateway_session_id": session.session_id},
                ownership_verified=True,
                outcome="failed",
                verified_after_operation=False,
                observed_state=observed_state,
                residual=True,
                detail=(
                    "scheduler cancellation was not requested, but retained-state "
                    f"verification remained unresolved: {observed_state}"
                ),
            )
        scheduler_terminal = observed_state in _TERMINAL_RUNTIME_STATES
        return CleanupResource(
            kind="scheduler_job",
            resource_id=scheduler_job_id,
            location=self.definition.ssh_host,
            provider=session.scheduler,
            action="retain",
            metadata={"gateway_session_id": session.session_id},
            ownership_verified=True,
            outcome="terminal" if scheduler_terminal else "retained",
            verified_after_operation=True,
            observed_state=observed_state,
            detail=(
                "scheduler cancellation was not requested; observed "
                f"{'terminal' if scheduler_terminal else 'active'} runtime state: "
                f"{observed_state}"
            ),
        )

    def _observe_runtime_state(
        self,
        *,
        spec: ServiceRuntimeSpec,
        scheduler_job_id: str,
    ) -> str:
        if spec.status_command is None:
            raise RelayError("runtime status command is required for retained-state verification")
        status_text = self._ssh(_template_command_script(spec.status_command, scheduler_job_id))
        status = _parse_runtime_status(status_text)
        if status.state is None or not status.state.strip():
            raise RelayError(
                f"runtime status did not report a state for scheduler job {scheduler_job_id}"
            )
        normalized = status.state.strip().lower()
        if normalized not in _ACTIVE_RUNTIME_STATES | _TERMINAL_RUNTIME_STATES:
            raise RelayError(
                "runtime status reported an unsupported state for scheduler job "
                f"{scheduler_job_id}: {normalized}"
            )
        return normalized

    def _observe_scheduler_state(
        self,
        *,
        scheduler: str,
        spec: ServiceRuntimeSpec,
        scheduler_job_id: str,
    ) -> str:
        provider = provider_for_scheduler(scheduler)
        if provider.name == "external":
            return self._observe_runtime_state(
                spec=spec,
                scheduler_job_id=scheduler_job_id,
            )
        return self._poll_scheduler_provider(
            provider=provider.name,
            scheduler_job_id=scheduler_job_id,
        ).phase.value

    def _wait_for_scheduler_terminal(
        self,
        *,
        scheduler: str,
        spec: ServiceRuntimeSpec,
        scheduler_job_id: str,
    ) -> str:
        deadline = time.time() + spec.readiness_timeout_seconds
        last_state = "unknown"
        while time.time() < deadline:
            last_state = self._observe_scheduler_state(
                scheduler=scheduler,
                spec=spec,
                scheduler_job_id=scheduler_job_id,
            )
            if last_state in _TERMINAL_RUNTIME_STATES:
                return last_state
            self.sleep(spec.poll_seconds)
        raise RelayError(
            "runtime cancellation was not confirmed terminal before timeout: "
            f"job={scheduler_job_id} last_state={last_state}"
        )

    def _poll_scheduler_provider(
        self,
        *,
        provider: str,
        scheduler_job_id: str,
    ) -> SchedulerStatus:
        output = self._ssh(
            _remote_scheduler_script(
                definition=self.definition,
                operation="status",
                provider=provider,
                scheduler_job_id=scheduler_job_id,
            )
        )
        try:
            status = SchedulerStatus.model_validate(_last_json_object(output))
        except (ValueError, TypeError) as exc:
            raise RelayError("scheduler provider returned invalid structured status") from exc
        expected_provider = provider_for_scheduler(provider).name
        if status.scheduler != expected_provider:
            raise RelayError(
                "scheduler provider identity mismatch: "
                f"{status.scheduler!r} != {expected_provider!r}"
            )
        if status.scheduler_job_id != scheduler_job_id:
            raise RelayError(
                "scheduler provider job identity mismatch: "
                f"{status.scheduler_job_id!r} != {scheduler_job_id!r}"
            )
        return status

    def _request_scheduler_provider_cancel(
        self,
        *,
        provider: str,
        scheduler_job_id: str,
    ) -> None:
        output = self._ssh(
            _remote_scheduler_script(
                definition=self.definition,
                operation="cancel",
                provider=provider,
                scheduler_job_id=scheduler_job_id,
            )
        )
        result = _last_json_object(output)
        if (
            result.get("scheduler") != provider_for_scheduler(provider).name
            or result.get("scheduler_job_id") != scheduler_job_id
            or result.get("cancel_requested") is not True
            or result.get("accepted") is not True
            or result.get("returncode") != 0
        ):
            raise RelayError("scheduler provider did not accept exact-job cancellation")

    def _start_remote_connector(
        self,
        *,
        session: GatewaySession,
        spec: ServiceRuntimeSpec,
        node: str,
        proxy_name: str,
        ownership_intent: dict[str, object],
        allocation_provider: str | None = None,
        allocation_job_id: str | None = None,
    ) -> dict[str, object]:
        if (allocation_provider is None) != (allocation_job_id is None):
            raise ConfigurationError(
                "allocation_provider and allocation_job_id must be provided together"
            )
        placement: SchedulerConnectorPlacement | None = None
        step_marker: str | None = None
        if allocation_provider is not None and allocation_job_id is not None:
            provider = provider_for_scheduler(allocation_provider)
            if not isinstance(provider, SchedulerAllocationConnectorProvider):
                raise ConfigurationError(
                    f"scheduler provider {allocation_provider!r} cannot launch an "
                    "allocation-scoped connector"
                )
            raw_placement = _last_json_object(
                self._ssh(
                    _remote_scheduler_script(
                        definition=self.definition,
                        operation="connector-placement",
                        provider=allocation_provider,
                        scheduler_job_id=allocation_job_id,
                    )
                )
            )
            try:
                placement = SchedulerConnectorPlacement.model_validate_json(
                    json.dumps(raw_placement, separators=(",", ":"), allow_nan=False)
                )
            except ValueError as exc:
                raise RelayError(
                    "scheduler provider returned invalid connector placement evidence"
                ) from exc
            if (
                placement.scheduler != allocation_provider
                or placement.scheduler_job_id != allocation_job_id
                or placement.allocation_node_count != 1
                or placement.verified is not True
            ):
                raise RelayError("scheduler connector placement identity did not match binding")
            step_marker = _connector_step_marker(
                session.session_id,
                _required_intent_str(
                    ownership_intent,
                    "connector_generation_id",
                ),
            )
            ownership_intent = _new_ownership_intent(
                "starting",
                owner_token=_required_intent_str(ownership_intent, "owner_token"),
                connector_generation_id=_required_intent_str(
                    ownership_intent,
                    "connector_generation_id",
                ),
                execution_scope="scheduler_allocation",
                scheduler_provider=allocation_provider,
                scheduler_native_id=allocation_job_id,
                scheduler_step_marker=step_marker,
                placement=placement.model_dump(mode="json"),
            )
            # Persist the allocation, placement, and unique step marker before
            # A detached ``srun`` can create a scheduler-side process.
            self._set_ownership_intent(
                session,
                "remote_connector",
                ownership_intent,
            )
        transport = self.definition.frp_transport
        server_addr = _require_server_addr(transport.server_addr, self.cluster)
        config = render_frpc_config(
            FrpcConfig(
                server_addr=server_addr,
                server_port=transport.server_port,
                token=self.token,
                transport_protocol=FrpTransportProtocol(transport.protocol),
                proxy_name=proxy_name,
                proxy_type=_frp_proxy_type(spec.transport_mode),
                local_ip=node,
                local_port=spec.service_port,
                secret_key=self.secret_key,
            )
        )
        owner_token = _required_intent_str(ownership_intent, "owner_token")
        connector_generation_id = _required_intent_str(
            ownership_intent,
            "connector_generation_id",
        )
        if allocation_provider is not None and allocation_job_id is not None:
            if placement is None or step_marker is None:
                raise AssertionError("allocation placement and step marker were not resolved")
            output = self._ssh(
                _remote_allocation_frpc_start_script(
                    definition=self.definition,
                    session_id=session.session_id,
                    config_text=config,
                    owner_token=owner_token,
                    connector_generation_id=connector_generation_id,
                    allocation_provider=allocation_provider,
                    allocation_job_id=allocation_job_id,
                    placement=placement,
                    step_marker=step_marker,
                )
            )
            start_result = _last_json_object(output)
            if start_result.get("schema_version") != "clio-relay.allocation-connector-start.v1":
                raise RelayError("allocation connector start returned the wrong schema")
            if (
                start_result.get("session_id") != session.session_id
                or start_result.get("connector_generation_id") != connector_generation_id
            ):
                raise RelayError("allocation connector start identity did not match its intent")
            raw_step = start_result.get("step_identity")
            try:
                step_identity = SchedulerConnectorStepIdentity.model_validate_json(
                    json.dumps(raw_step, separators=(",", ":"), allow_nan=False)
                )
            except (TypeError, ValueError) as exc:
                raise RelayError(
                    "allocation connector start returned invalid scheduler step identity"
                ) from exc
            if (
                step_identity.scheduler != allocation_provider
                or step_identity.scheduler_job_id != allocation_job_id
                or step_identity.placement_host != placement.placement_host
                or step_identity.step_marker != step_marker
                or step_identity.verified is not True
            ):
                raise RelayError("allocation connector scheduler step identity did not match")
            config_path = _optional_str(start_result.get("config_path"))
            log_path = _optional_str(start_result.get("log_path"))
            if config_path is None or log_path is None:
                raise RelayError("allocation connector start omitted its owned paths")
            return {
                "owner": "clio-relay",
                "session_id": session.session_id,
                "execution_scope": "scheduler_allocation",
                "scheduler_provider": allocation_provider,
                "scheduler_native_id": allocation_job_id,
                "scheduler_step_id": step_identity.scheduler_step_id,
                "scheduler_step_marker": step_marker,
                "scheduler_step": step_identity.model_dump(mode="json"),
                "connector_generation_id": connector_generation_id,
                "owner_token": owner_token,
                "config_path": config_path,
                "log_path": log_path,
                "placement": placement.model_dump(mode="json"),
            }
        output = self._ssh(
            _remote_frpc_start_script(
                definition=self.definition,
                session_id=session.session_id,
                config_text=config,
                owner_token=owner_token,
                connector_generation_id=connector_generation_id,
            )
        )
        metadata = _key_value_output(output)
        expected_fields = {
            "remote_frpc_pid",
            "remote_frpc_pgid",
            "connector_generation_id",
            "remote_frpc_config",
            "remote_frpc_log",
        }
        if set(metadata) != expected_fields:
            raise RelayError("remote connector start returned an invalid response shape")
        try:
            pid = int(metadata["remote_frpc_pid"])
            process_group_id = int(metadata["remote_frpc_pgid"])
        except ValueError as exc:
            raise RelayError("remote connector start returned an invalid process identity") from exc
        if pid <= 0 or process_group_id != pid:
            raise RelayError("remote connector start returned an invalid process group identity")
        if metadata["connector_generation_id"] != connector_generation_id:
            raise RelayError("remote connector start identity did not match its durable intent")
        config_path = _validated_remote_session_file(
            metadata["remote_frpc_config"],
            session_id=session.session_id,
            filename="remote-frpc.toml",
        )
        log_path = _validated_remote_session_file(
            metadata["remote_frpc_log"],
            session_id=session.session_id,
            filename="remote-frpc.log",
        )
        if config_path.parent != log_path.parent:
            raise RelayError("remote connector start returned paths from different sessions")
        connector: dict[str, object] = {
            "owner": "clio-relay",
            "session_id": session.session_id,
            "pid": pid,
            "process_group_id": process_group_id,
            "connector_generation_id": connector_generation_id,
            "owner_token": owner_token,
            "config_path": config_path.as_posix(),
            "log_path": log_path.as_posix(),
        }
        return connector

    def _allocation_connector_identity(
        self,
        *,
        session_id: str,
        connector: dict[str, object],
    ) -> SchedulerConnectorStepIdentity:
        """Validate exact provider, allocation, placement, and step ownership."""
        if (
            connector.get("owner") != "clio-relay"
            or connector.get("session_id") != session_id
            or connector.get("execution_scope") != "scheduler_allocation"
            or connector.get("pid") is not None
            or connector.get("process_group_id") is not None
        ):
            raise RelayError("allocation connector ownership record is invalid")
        try:
            step = SchedulerConnectorStepIdentity.model_validate_json(
                json.dumps(
                    connector.get("scheduler_step"),
                    separators=(",", ":"),
                    allow_nan=False,
                )
            )
            placement = SchedulerConnectorPlacement.model_validate_json(
                json.dumps(
                    connector.get("placement"),
                    separators=(",", ":"),
                    allow_nan=False,
                )
            )
        except (TypeError, ValueError) as exc:
            raise RelayError("allocation connector has invalid provider-native identity") from exc
        generation_id = _optional_str(connector.get("connector_generation_id"))
        provider_name = _optional_str(connector.get("scheduler_provider"))
        scheduler_job_id = _optional_str(connector.get("scheduler_native_id"))
        scheduler_step_id = _optional_str(connector.get("scheduler_step_id"))
        step_marker = _optional_str(connector.get("scheduler_step_marker"))
        config_path = _optional_str(connector.get("config_path"))
        log_path = _optional_str(connector.get("log_path"))
        if None in {
            generation_id,
            provider_name,
            scheduler_job_id,
            scheduler_step_id,
            step_marker,
            config_path,
            log_path,
        }:
            raise RelayError("allocation connector ownership record is incomplete")
        assert generation_id is not None
        assert provider_name is not None
        assert scheduler_job_id is not None
        assert scheduler_step_id is not None
        assert step_marker is not None
        try:
            provider = provider_for_scheduler(provider_name)
        except ConfigurationError as exc:
            raise RelayError(f"allocation connector provider is invalid: {exc}") from exc
        if not isinstance(provider, SchedulerAllocationConnectorProvider):
            raise RelayError("allocation connector provider lacks step lifecycle semantics")
        if (
            provider.name != provider_name
            or step.scheduler != provider_name
            or step.scheduler_job_id != scheduler_job_id
            or step.scheduler_step_id != scheduler_step_id
            or step.step_marker != step_marker
            or step_marker != _connector_step_marker(session_id, generation_id)
            or placement.scheduler != provider_name
            or placement.scheduler_job_id != scheduler_job_id
            or placement.placement_host != step.placement_host
            or placement.allocation_node_count != 1
            or step.verified is not True
            or placement.verified is not True
        ):
            raise RelayError("allocation connector identities disagree")
        return step

    def _poll_allocation_connector_step(
        self,
        identity: SchedulerConnectorStepIdentity,
    ) -> SchedulerConnectorStepStatus:
        """Poll one exact provider-native connector step over the cluster boundary."""
        output = self._ssh(
            _remote_connector_step_status_script(
                definition=self.definition,
                provider=identity.scheduler,
                scheduler_job_id=identity.scheduler_job_id,
                scheduler_step_id=identity.scheduler_step_id,
                placement_host=identity.placement_host,
            )
        )
        try:
            status = SchedulerConnectorStepStatus.model_validate_json(
                json.dumps(
                    _last_json_object(output),
                    separators=(",", ":"),
                    allow_nan=False,
                )
            )
        except (TypeError, ValueError) as exc:
            raise RelayError("scheduler returned invalid connector step status") from exc
        if (
            status.scheduler != identity.scheduler
            or status.scheduler_job_id != identity.scheduler_job_id
            or status.scheduler_step_id != identity.scheduler_step_id
            or status.placement_host != identity.placement_host
            or status.verified is not True
        ):
            raise RelayError("scheduler connector step status identity did not match")
        return status

    def _stop_allocation_connector(
        self,
        *,
        session_id: str,
        connector: dict[str, object],
    ) -> CleanupResource:
        """Cancel one exact scheduler step and prove its compute-node absence."""
        identity = self._allocation_connector_identity(
            session_id=session_id,
            connector=connector,
        )
        status = self._poll_allocation_connector_step(identity)
        cancel_error: str | None = None
        canceled = False
        if status.state == "active":
            try:
                result = _last_json_object(
                    self._ssh(
                        _remote_connector_step_cancel_script(
                            definition=self.definition,
                            provider=identity.scheduler,
                            scheduler_job_id=identity.scheduler_job_id,
                            scheduler_step_id=identity.scheduler_step_id,
                        )
                    )
                )
                if (
                    result.get("scheduler") != identity.scheduler
                    or result.get("scheduler_job_id") != identity.scheduler_job_id
                    or result.get("scheduler_step_id") != identity.scheduler_step_id
                    or result.get("cancel_requested") is not True
                    or result.get("accepted") is not True
                    or result.get("returncode") != 0
                ):
                    raise RelayError("scheduler did not accept exact connector-step cancellation")
                canceled = True
            except RelayError as exc:
                cancel_error = str(exc)
            attempts = max(
                1,
                math.ceil(
                    _CONNECTOR_STEP_CLEANUP_TIMEOUT_SECONDS / _CONNECTOR_STEP_CLEANUP_POLL_SECONDS
                ),
            )
            for attempt in range(attempts):
                status = self._poll_allocation_connector_step(identity)
                if status.state == "absent":
                    break
                if attempt + 1 < attempts:
                    self.sleep(_CONNECTOR_STEP_CLEANUP_POLL_SECONDS)
        if status.state != "absent":
            detail = "scheduler connector step remains active after exact-step cancellation"
            if cancel_error is not None:
                detail = f"{detail}: {cancel_error}"
            raise RelayError(detail)
        return CleanupResource(
            kind="remote_connector",
            resource_id=identity.scheduler_step_id,
            location=identity.placement_host,
            provider=identity.scheduler,
            action="stop",
            ownership_verified=True,
            outcome="stopped" if canceled else "missing",
            verified_after_operation=True,
            observed_state="absent",
            detail=(
                "exact scheduler connector step absence confirmed"
                + (f" after cancellation error: {cancel_error}" if cancel_error else "")
            ),
            metadata={
                "scheduler_job_id": identity.scheduler_job_id,
                "scheduler_step_id": identity.scheduler_step_id,
                "scheduler_step_marker": identity.step_marker,
                "placement_host": identity.placement_host,
                "parent_scheduler_job_retained": True,
            },
        )

    def _retained_allocation_connector_resource(
        self,
        *,
        session_id: str,
        connector: dict[str, object],
    ) -> CleanupResource:
        """Prove that a detached allocation-scoped connector remains active."""
        identity = self._allocation_connector_identity(
            session_id=session_id,
            connector=connector,
        )
        status = self._poll_allocation_connector_step(identity)
        retained = status.state == "active"
        return CleanupResource(
            kind="remote_connector",
            resource_id=identity.scheduler_step_id,
            location=identity.placement_host,
            provider=identity.scheduler,
            action="retain",
            ownership_verified=True,
            outcome="retained" if retained else "failed",
            verified_after_operation=True,
            observed_state=status.state,
            residual=not retained,
            detail=(
                "exact scheduler connector step retained for reattachment"
                if retained
                else "scheduler confirms the allocation connector step is absent"
            ),
            metadata={
                "scheduler_job_id": identity.scheduler_job_id,
                "scheduler_step_id": identity.scheduler_step_id,
                "scheduler_step_marker": identity.step_marker,
                "placement_host": identity.placement_host,
                "parent_scheduler_job_retained": True,
            },
        )

    def _start_local_visitor(
        self,
        *,
        session: GatewaySession,
        spec: ServiceRuntimeSpec,
        proxy_name: str,
        ownership_intent: dict[str, object],
    ) -> dict[str, object]:
        transport = self.definition.frp_transport
        server_addr = _require_server_addr(transport.server_addr, self.cluster)
        runtime_dir = self.settings.core_dir.parent / "runtime-sessions" / session.session_id
        runtime_dir.mkdir(parents=True, exist_ok=True)
        config_path = Path(_required_intent_str(ownership_intent, "config_path")).resolve()
        stdout_path = Path(_required_intent_str(ownership_intent, "stdout_path")).resolve()
        stderr_path = Path(_required_intent_str(ownership_intent, "stderr_path")).resolve()
        metadata_path = Path(_required_intent_str(ownership_intent, "metadata_path")).resolve()
        owned_paths = (config_path, stdout_path, stderr_path, metadata_path)
        if any(path.parent != runtime_dir.resolve() for path in owned_paths):
            raise RelayError("desktop connector ownership intent escaped its runtime directory")
        config_path.write_text(
            render_frpc_visitor_config(
                FrpcVisitorConfig(
                    server_addr=server_addr,
                    server_port=transport.server_port,
                    token=self.token,
                    transport_protocol=FrpTransportProtocol(transport.protocol),
                    visitor_name=f"{proxy_name}-visitor",
                    visitor_type=_frp_proxy_type(spec.transport_mode),
                    server_name=proxy_name,
                    bind_addr=spec.desktop_bind_addr,
                    bind_port=spec.desktop_bind_port,
                    secret_key=self.secret_key,
                    keep_tunnel_open=_frp_proxy_type(spec.transport_mode) == "xtcp",
                )
            ),
            encoding="utf-8",
        )
        config_path.chmod(0o600)
        owner_token = _required_intent_str(ownership_intent, "owner_token")
        connector_generation_id = _required_intent_str(
            ownership_intent,
            "connector_generation_id",
        )
        environment = os.environ.copy()
        environment["CLIO_RELAY_CONNECTOR_OWNER_TOKEN"] = owner_token
        environment["CLIO_RELAY_CONNECTOR_GENERATION_ID"] = connector_generation_id
        connector_command = [self.settings.frpc_bin, "-c", str(config_path)]
        process = self.runner.popen(
            [
                sys.executable,
                "-c",
                _LOCAL_CONNECTOR_WRAPPER_CODE,
                owner_token,
                connector_generation_id,
                *connector_command,
            ],
            stdout_path=stdout_path,
            stderr_path=stderr_path,
            env=environment,
            isolate_process_group=True,
        )
        try:
            identity = self.runner.local_process_identity(
                pid=process.pid,
                owner_token=owner_token,
                expected_config=str(config_path),
            )
        except BaseException:
            _terminate_just_started_process_group(process.pid)
            raise
        connector: dict[str, object] = {
            "owner": "clio-relay",
            "session_id": session.session_id,
            "pid": process.pid,
            "process_group_id": identity.process_group_id,
            "process_start_marker": identity.process_start_marker,
            "owner_token": identity.owner_token,
            "connector_generation_id": connector_generation_id,
            "config_path": str(config_path),
            "stdout_path": str(stdout_path),
            "stderr_path": str(stderr_path),
            "metadata_path": str(metadata_path),
        }
        _write_local_connector_sidecar(metadata_path, connector)
        return connector

    def _start_browser_proxy(
        self,
        *,
        session: GatewaySession,
        config: BrowserGatewayConfig,
        capability: str,
        upstream_authorization: str | None,
        ownership_intent: dict[str, object],
    ) -> dict[str, object]:
        """Start one owned capability proxy without placing either secret on disk."""
        runtime_dir = (
            self.settings.core_dir.parent / "runtime-sessions" / session.session_id
        ).resolve()
        config_path = Path(_required_intent_str(ownership_intent, "config_path")).resolve()
        stdout_path = Path(_required_intent_str(ownership_intent, "stdout_path")).resolve()
        stderr_path = Path(_required_intent_str(ownership_intent, "stderr_path")).resolve()
        metadata_path = Path(_required_intent_str(ownership_intent, "metadata_path")).resolve()
        if any(
            path.parent != runtime_dir
            for path in (config_path, stdout_path, stderr_path, metadata_path)
        ):
            raise RelayError("browser proxy ownership intent escaped its runtime directory")
        temporary = config_path.with_suffix(f"{config_path.suffix}.{os.getpid()}.tmp")
        temporary.write_text(config.model_dump_json(indent=2) + "\n", encoding="utf-8")
        temporary.chmod(0o600)
        os.replace(temporary, config_path)
        owner_token = _required_intent_str(ownership_intent, "owner_token")
        generation_id = _required_intent_str(ownership_intent, "connector_generation_id")
        environment = os.environ.copy()
        environment.pop(CAPABILITY_ENV, None)
        environment.pop(UPSTREAM_AUTHORIZATION_ENV, None)
        environment["CLIO_RELAY_CONNECTOR_OWNER_TOKEN"] = owner_token
        environment["CLIO_RELAY_CONNECTOR_GENERATION_ID"] = generation_id
        bootstrap = (
            BrowserGatewayBootstrap(
                capability=capability,
                upstream_authorization=upstream_authorization,
            )
            .model_dump_json()
            .encode("utf-8")
        )
        process = self.runner.popen(
            [
                sys.executable,
                "-c",
                _LOCAL_CONNECTOR_WRAPPER_CODE,
                owner_token,
                generation_id,
                sys.executable,
                "-m",
                "clio_relay.browser_gateway",
                "--config",
                str(config_path),
                "--process-label",
                "clio-relay-browser-frpc-proxy",
            ],
            stdout_path=stdout_path,
            stderr_path=stderr_path,
            env=environment,
            isolate_process_group=True,
            input_bytes=bootstrap,
        )
        try:
            identity = self.runner.local_process_identity(
                pid=process.pid,
                owner_token=owner_token,
                expected_config=str(config_path),
            )
        except BaseException:
            _terminate_just_started_process_group(process.pid)
            raise
        proxy: dict[str, object] = {
            "owner": "clio-relay",
            "session_id": session.session_id,
            "attachment_id": config.attachment_id,
            "pid": process.pid,
            "process_group_id": identity.process_group_id,
            "process_start_marker": identity.process_start_marker,
            "owner_token": identity.owner_token,
            "connector_generation_id": generation_id,
            "config_path": str(config_path),
            "stdout_path": str(stdout_path),
            "stderr_path": str(stderr_path),
            "metadata_path": str(metadata_path),
        }
        _write_local_connector_sidecar(metadata_path, proxy)
        return proxy

    def _wait_for_jarvis_health(
        self,
        health_url: str,
        *,
        timeout_seconds: float,
        poll_seconds: float,
        runtime_schema_version: Literal["jarvis.service-runtime.v1", "jarvis.service-runtime.v2"],
        authorization: str | None,
    ) -> None:
        """Prove the versioned JARVIS HTTP authorization boundary is live."""
        if runtime_schema_version == JARVIS_SERVICE_RUNTIME_SCHEMA_V1:
            if authorization is not None:
                raise RelayError(
                    "legacy JARVIS service runtime unexpectedly resolved authorization"
                )
        elif runtime_schema_version == JARVIS_SERVICE_RUNTIME_SCHEMA_V2:
            if authorization is None:
                raise RelayError("authenticated JARVIS service runtime omitted authorization")
        else:
            raise RelayError("JARVIS service runtime schema is unsupported")
        deadline = time.monotonic() + timeout_seconds
        last_error = "no response"
        while time.monotonic() < deadline:
            try:
                anonymous = _read_bounded_http_response(
                    health_url,
                    headers=None,
                    maximum_bytes=None,
                    deadline=deadline,
                )
                if runtime_schema_version == JARVIS_SERVICE_RUNTIME_SCHEMA_V1:
                    if 200 <= anonymous.status_code < 300:
                        return
                    last_error = f"legacy anonymous health status={anonymous.status_code}"
                else:
                    if 200 <= anonymous.status_code < 300:
                        raise RelayError(
                            "authenticated JARVIS service health accepted an anonymous request"
                        )
                    if anonymous.status_code != 401:
                        last_error = f"anonymous health status={anonymous.status_code}"
                    else:
                        authenticated = _read_bounded_http_response(
                            health_url,
                            headers={"Authorization": cast(str, authorization)},
                            maximum_bytes=None,
                            deadline=deadline,
                        )
                        if 200 <= authenticated.status_code < 300:
                            return
                        last_error = f"authenticated health status={authenticated.status_code}"
            except httpx.HTTPError:
                last_error = "HTTP transport failed"
            _sleep_before_deadline(self.sleep, poll_seconds, deadline)
        raise RelayError(f"JARVIS service health boundary was not ready: {last_error}")

    def _wait_for_browser_health(
        self,
        health_url: str,
        *,
        timeout_seconds: float,
        poll_seconds: float,
    ) -> None:
        """Prove the capability proxy forwards health with exact sandbox-origin CORS."""
        deadline = time.monotonic() + timeout_seconds
        last_error = "no response"
        while time.monotonic() < deadline:
            try:
                response = _read_bounded_http_response(
                    health_url,
                    headers={"Origin": "null"},
                    maximum_bytes=None,
                    deadline=deadline,
                )
                if (
                    200 <= response.status_code < 300
                    and response.headers.get("access-control-allow-origin") == "null"
                ):
                    return
                last_error = (
                    f"status={response.status_code}; "
                    "access-control-allow-origin was not exactly null"
                )
            except httpx.HTTPError:
                last_error = "HTTP transport failed"
            _sleep_before_deadline(self.sleep, poll_seconds, deadline)
        raise RelayError(f"browser capability gateway did not become ready: {last_error}")

    def _wait_for_local_health(
        self,
        health_url: str,
        timeout_seconds: float,
        poll_seconds: float,
        *,
        expected_body: str | None = None,
    ) -> None:
        deadline = time.monotonic() + timeout_seconds
        last_error: str | None = None
        while time.monotonic() < deadline:
            try:
                response = _read_bounded_http_response(
                    health_url,
                    headers=None,
                    maximum_bytes=_MAX_LOCAL_HEALTH_BYTES,
                    deadline=deadline,
                )
                if 200 <= response.status_code < 300:
                    if expected_body is None or response.content == expected_body.encode("utf-8"):
                        return
                    last_error = "HTTP response body did not match the runtime identity"
                else:
                    last_error = f"HTTP {response.status_code}"
            except httpx.HTTPError as exc:
                last_error = str(exc)
            _sleep_before_deadline(self.sleep, poll_seconds, deadline)
        raise RelayError(f"local service health probe failed: {health_url}: {last_error}")

    def _update(
        self,
        session: GatewaySession,
        *,
        state: GatewaySessionState | None = None,
        metadata: dict[str, object] | None = None,
        **updates: object,
    ) -> GatewaySession:
        return self.queue.update_gateway_session(
            session.session_id,
            state=state,
            metadata=metadata,
            expected_updated_at=session.updated_at,
            **updates,
        )

    def _record_runtime_start_failure(
        self,
        *,
        session_id: str,
        error: BaseException,
        cleanup_errors: Sequence[str],
    ) -> None:
        """Persist a start failure against the latest post-cleanup session revision."""

        last_conflict: QueueConflictError | None = None
        for _attempt in range(3):
            current = self.queue.get_gateway_session(session_id)
            if current.state is GatewaySessionState.READY:
                return
            target_state = (
                GatewaySessionState.CLOSED
                if current.state is GatewaySessionState.CLOSED
                else GatewaySessionState.FAILED
            )
            try:
                self.queue.update_gateway_session(
                    session_id,
                    state=target_state,
                    expected_updated_at=current.updated_at,
                    metadata={
                        "failed_at": utc_now().isoformat(),
                        "last_error": str(error),
                        "cleanup_error": ("; ".join(dict.fromkeys(cleanup_errors)) or None),
                    },
                )
                return
            except QueueConflictError as exc:
                last_conflict = exc
        if last_conflict is not None:
            raise last_conflict

    def _record_attach_failure(
        self,
        *,
        session_id: str,
        error: BaseException,
        cleanup_error: str | None,
    ) -> None:
        """Record an attach failure only while the same gateway remains mutable."""

        if isinstance(error, QueueConflictError):
            return
        current = self.queue.get_gateway_session(session_id)
        if (
            current.state in {GatewaySessionState.READY, GatewaySessionState.CLOSED}
            or current.gateway.get("teardown_intent") is not None
        ):
            return
        try:
            self.queue.update_gateway_session(
                session_id,
                state=GatewaySessionState.DEGRADED,
                expected_updated_at=current.updated_at,
                metadata={
                    "attach_failed_at": utc_now().isoformat(),
                    "attach_error": str(error),
                    "attach_cleanup_error": cleanup_error,
                },
            )
        except QueueConflictError:
            return

    def _ssh(self, script: str) -> str:
        try:
            result = self.runner.run(
                ["ssh", self.definition.ssh_host, "bash", "-s"],
                input_text=script,
                timeout_seconds=_REMOTE_RUNTIME_COMMAND_TIMEOUT_SECONDS,
            )
        except subprocess.TimeoutExpired as exc:
            raise RelayError(
                "remote service runtime command timed out after "
                f"{_REMOTE_RUNTIME_COMMAND_TIMEOUT_SECONDS:g} seconds"
            ) from exc
        if result.returncode != 0:
            detail = result.stderr.strip() or result.stdout.strip()
            raise RelayError(f"remote service runtime command failed: {detail}")
        return result.stdout


def _read_bounded_http_response(
    url: str,
    *,
    headers: dict[str, str] | None,
    maximum_bytes: int | None,
    deadline: float | None = None,
) -> _BoundedHttpResponse:
    """Read headers and, when requested, a bounded body by one absolute deadline."""

    if maximum_bytes is not None and maximum_bytes <= 0:
        raise ValueError("HTTP response byte limit must be positive")
    effective_deadline = time.monotonic() + 5.0 if deadline is None else deadline
    remaining = effective_deadline - time.monotonic()
    if remaining <= 0:
        raise httpx.TimeoutException("HTTP response total deadline expired before connection")

    state = _BoundedHttpReadState()
    state_lock = threading.Lock()
    completed = threading.Event()
    client = _new_readiness_http_client(remaining)

    def read_response() -> None:
        try:
            with client.stream("GET", url, headers=headers) as response:
                with state_lock:
                    state.response = response
                if maximum_bytes is None:
                    state.result = _BoundedHttpResponse(
                        status_code=response.status_code,
                        headers=httpx.Headers(response.headers),
                        content=b"",
                    )
                    return
                raw_length = response.headers.get("content-length")
                if raw_length is not None:
                    try:
                        content_length = int(raw_length)
                    except ValueError as exc:
                        raise ValueError("HTTP response Content-Length is invalid") from exc
                    if content_length < 0 or content_length > maximum_bytes:
                        raise ValueError(
                            f"HTTP response exceeds the {maximum_bytes}-byte decompressed limit"
                        )
                content = bytearray()
                for chunk in response.iter_bytes(chunk_size=64 * 1024):
                    if len(content) + len(chunk) > maximum_bytes:
                        raise ValueError(
                            f"HTTP response exceeds the {maximum_bytes}-byte decompressed limit"
                        )
                    content.extend(chunk)
                state.result = _BoundedHttpResponse(
                    status_code=response.status_code,
                    headers=httpx.Headers(response.headers),
                    content=bytes(content),
                )
        except BaseException as exc:
            state.error = exc
        finally:
            with suppress(Exception):
                client.close()
            completed.set()

    reader = threading.Thread(
        target=read_response,
        name="clio-relay-readiness-http",
        daemon=True,
    )
    reader.start()
    completed_before_deadline = completed.wait(max(0.0, effective_deadline - time.monotonic()))
    if not completed_before_deadline or time.monotonic() > effective_deadline:
        with state_lock:
            active_response = state.response
        if active_response is not None:
            with suppress(Exception):
                active_response.close()
        with suppress(Exception):
            client.close()
        raise httpx.TimeoutException("HTTP response exceeded its total monotonic deadline")
    if state.error is not None:
        raise state.error
    if state.result is None:
        raise RuntimeError("HTTP response reader completed without a result or error")
    return state.result


def _new_readiness_http_client(timeout_seconds: float) -> httpx.Client:
    """Create one operation-owned client whose pool can be closed at the deadline."""

    return httpx.Client(timeout=timeout_seconds)


def _sleep_before_deadline(
    sleep: Callable[[float], None],
    poll_seconds: float,
    deadline: float,
) -> None:
    """Sleep for at most the remaining monotonic readiness budget."""

    remaining = deadline - time.monotonic()
    if remaining > 0:
        sleep(min(poll_seconds, remaining))


def _available_loopback_port(*, exclude: set[int] | None = None) -> int:
    """Select one currently free loopback TCP port outside an explicit exclusion set."""
    excluded = exclude or set()
    for _ in range(20):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
            listener.bind(("127.0.0.1", 0))
            port = cast(int, listener.getsockname()[1])
        if port not in excluded:
            return port
    raise RelayError("could not select a distinct loopback port")


def _validated_available_loopback_port(port: object) -> int:
    """Validate and availability-test an explicit operator-selected loopback port."""
    if isinstance(port, bool) or not isinstance(port, int):
        raise ConfigurationError("desktop bind port must be an integer")
    if port < 1 or port > 65_535:
        raise ConfigurationError("desktop bind port must be between 1 and 65535")
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
            listener.bind(("127.0.0.1", port))
    except OSError as exc:
        raise ConfigurationError(f"desktop bind port is already occupied: {port}") from exc
    return port


def _browser_attachment_grant(
    *,
    record: BrowserAttachmentRecord,
    capability: str,
    spec: ServiceRuntimeSpec,
) -> BrowserAttachmentGrant:
    """Build the one-time capability URLs without copying them into gateway state."""
    if spec.command_path is None or spec.stream_path is None or spec.event_stream_path is None:
        raise ConfigurationError("browser attachment requires stream, events, and command paths")
    if spec.state_path is None:
        raise ConfigurationError("browser attachment requires a state path")
    base = f"http://{record.bind_addr}:{record.bind_port}"

    def capability_url(path: str) -> str:
        encoded = urllib.parse.urlencode({"capability": capability})
        return f"{base}{path}?{encoded}"

    return BrowserAttachmentGrant(
        attachment_id=record.attachment_id,
        expires_at=record.expires_at,
        connect_url=capability_url("/"),
        health_url=capability_url(spec.health_path),
        stream_url=capability_url(spec.stream_path),
        events_url=capability_url(spec.event_stream_path),
        state_url=capability_url(spec.state_path),
        command_url=capability_url(spec.command_path),
    )


def _utc_timestamp(value: str) -> datetime:
    """Parse one explicitly UTC persisted timestamp."""
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise RelayError("browser attachment timestamp is invalid") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != UTC.utcoffset(parsed):
        raise RelayError("browser attachment timestamp is not UTC")
    return parsed


def _owned_browser_runtime_path(
    settings: RelaySettings,
    session_id: str,
    raw_path: str,
) -> Path:
    """Resolve a browser attachment path only inside its owned runtime directory."""
    expected = (settings.core_dir.parent / "runtime-sessions" / session_id).resolve()
    path = Path(raw_path).resolve()
    if path.parent != expected:
        raise RelayError("browser attachment revocation path escaped its runtime directory")
    return path


def _write_browser_revocation_marker(path: Path, attachment_id: str) -> None:
    """Durably revoke a browser capability before process cleanup begins."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{secrets.token_hex(8)}.tmp")
    try:
        with temporary.open("w", encoding="utf-8") as handle:
            handle.write(
                json.dumps(
                    {
                        "schema_version": "clio-relay.browser-capability-revocation.v1",
                        "attachment_id": attachment_id,
                        "revoked_at": utc_now().isoformat(),
                    },
                    sort_keys=True,
                )
                + "\n"
            )
            handle.flush()
            os.fsync(handle.fileno())
        temporary.chmod(0o600)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _submit_script(
    command: Sequence[str],
    *,
    session_id: str,
    submission_id: str,
    scheduler_provider: str,
    submission_marker: str,
) -> str:
    """Run a submission behind an exact durable intent and bounded output anchor."""
    encoded_command = base64.b64encode(
        json.dumps(list(command), separators=(",", ":")).encode("utf-8")
    ).decode("ascii")
    return f"""set -euo pipefail
umask 077
session_id={shlex.quote(session_id)}
submission_id={shlex.quote(submission_id)}
scheduler_provider={shlex.quote(scheduler_provider)}
submission_marker={shlex.quote(submission_marker)}
session_dir="$HOME/.local/share/clio-relay/service-sessions/$session_id"
mkdir -p "$session_dir/submissions"
record_file="$session_dir/submissions/$submission_id.json"
output_file="$session_dir/submissions/$submission_id.out"
output_meta="$session_dir/submissions/$submission_id.out.json"
intent_file="$session_dir/submissions/$submission_id.intent.json"
python3 - "$intent_file" "$session_id" "$submission_id" "$scheduler_provider" \
  "$submission_marker" <<'__CLIO_RESERVE_SUBMISSION__'
import json
import os
import stat
import sys
from pathlib import Path

path_raw, session_id, submission_id, provider, marker = sys.argv[1:]
path = Path(path_raw)
expected = {{
    "schema_version": "clio-relay.gateway-submission-intent.v1",
    "session_id": session_id,
    "submission_id": submission_id,
    "scheduler_provider": provider,
    "submission_marker": marker,
}}
if path.exists():
    before = os.lstat(path)
    if (
        not stat.S_ISREG(before.st_mode)
        or before.st_nlink != 1
        or before.st_mode & 0o077
        or before.st_size > 65536
    ):
        raise RuntimeError("scheduler submission intent is not a private bounded file")
    payload = path.read_bytes()
    after = os.lstat(path)
    if (before.st_ino, before.st_size, before.st_mtime_ns) != (
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
    ):
        raise RuntimeError("scheduler submission intent changed while reading")
    if json.loads(payload) != expected:
        raise RuntimeError("scheduler submission intent identity mismatch")
    raise SystemExit(0)
temporary = path.with_name(f".{{path.name}}.{{os.getpid()}}.tmp")
with temporary.open("w", encoding="utf-8") as handle:
    json.dump(expected, handle, sort_keys=True)
    handle.flush()
    os.fsync(handle.fileno())
os.chmod(temporary, 0o600)
os.replace(temporary, path)
directory = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
try:
    os.fsync(directory)
finally:
    os.close(directory)
__CLIO_RESERVE_SUBMISSION__
python3 - "$record_file" "$output_file" "$output_meta" "$intent_file" \
  "$session_id" "$submission_id" "$scheduler_provider" "$submission_marker" \
  {shlex.quote(encoded_command)} {int(_MAX_SUBMISSION_OUTPUT_BYTES)} \
  <<'__CLIO_CAPTURE_SUBMISSION__'
import base64
import hashlib
import json
import os
import stat
import subprocess
import sys
from pathlib import Path

(
    record_raw,
    output_raw,
    meta_raw,
    intent_raw,
    session_id,
    submission_id,
    provider,
    marker,
    encoded_command,
    maximum_raw,
) = sys.argv[1:]
maximum = int(maximum_raw)
expected_intent = {{
    "schema_version": "clio-relay.gateway-submission-intent.v1",
    "session_id": session_id,
    "submission_id": submission_id,
    "scheduler_provider": provider,
    "submission_marker": marker,
}}
intent_path = Path(intent_raw)
before = os.lstat(intent_path)
if (
    not stat.S_ISREG(before.st_mode)
    or before.st_nlink != 1
    or before.st_mode & 0o077
    or before.st_size > 65536
):
    raise RuntimeError("scheduler submission intent is not a private bounded file")
intent_payload = intent_path.read_bytes()
after = os.lstat(intent_path)
if (before.st_ino, before.st_size, before.st_mtime_ns) != (
    after.st_ino,
    after.st_size,
    after.st_mtime_ns,
):
    raise RuntimeError("scheduler submission intent changed while reading")
intent = json.loads(intent_payload)
if intent != expected_intent:
    raise RuntimeError("scheduler submission intent changed before execution")
record_exists = os.path.lexists(record_raw)
output_exists = os.path.lexists(output_raw)
meta_exists = os.path.lexists(meta_raw)
if output_exists != meta_exists:
    raise RuntimeError("scheduler submission output anchor is incomplete")
if record_exists and not (output_exists and meta_exists):
    raise RuntimeError("scheduler submission record is missing its output anchor")
if output_exists and meta_exists:
    raise SystemExit(0)
command = json.loads(base64.b64decode(encoded_command).decode("utf-8"))
if (
    not isinstance(command, list)
    or not command
    or not all(isinstance(item, str) for item in command)
):
    raise RuntimeError("scheduler submission command is invalid")
process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
temporary_output = Path(output_raw).with_name(f".{{Path(output_raw).name}}.{{os.getpid()}}.tmp")
observed = 0
persisted = 0
with temporary_output.open("wb") as handle:
    assert process.stdout is not None
    while True:
        chunk = process.stdout.read(65536)
        if not chunk:
            break
        observed += len(chunk)
        if persisted < maximum + 1:
            selected = chunk[: maximum + 1 - persisted]
            handle.write(selected)
            persisted += len(selected)
    handle.flush()
    os.fsync(handle.fileno())
returncode = process.wait()
os.chmod(temporary_output, 0o600)
os.replace(temporary_output, output_raw)
output = Path(output_raw).read_bytes()
truncated = observed > maximum
effective_returncode = returncode if returncode != 0 else (75 if truncated else 0)
meta = {{
    **expected_intent,
    "schema_version": "clio-relay.gateway-submission-output.v1",
    "returncode": effective_returncode,
    "output_sha256": hashlib.sha256(output).hexdigest(),
    "output_size": len(output),
    "observed_output_size": observed,
    "output_truncated": truncated,
}}
meta_path = Path(meta_raw)
temporary_meta = meta_path.with_name(f".{{meta_path.name}}.{{os.getpid()}}.tmp")
with temporary_meta.open("w", encoding="utf-8") as handle:
    json.dump(meta, handle, sort_keys=True)
    handle.flush()
    os.fsync(handle.fileno())
os.chmod(temporary_meta, 0o600)
os.replace(temporary_meta, meta_path)
directory = os.open(meta_path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
try:
    os.fsync(directory)
finally:
    os.close(directory)
__CLIO_CAPTURE_SUBMISSION__
python3 - "$record_file" "$output_file" "$output_meta" "$intent_file" \
  "$session_id" "$submission_id" "$scheduler_provider" "$submission_marker" \
  {int(_MAX_SUBMISSION_OUTPUT_BYTES)} <<'__CLIO_RECORD_SUBMISSION__'
import hashlib
import json
import os
import stat
import sys
from datetime import datetime, timezone
from pathlib import Path

(
    record_raw,
    output_raw,
    meta_raw,
    intent_raw,
    session_id,
    submission_id,
    provider,
    marker,
    maximum_raw,
) = sys.argv[1:]
maximum = int(maximum_raw)

def read_private(path_raw, maximum_bytes):
    path = Path(path_raw)
    before = os.lstat(path)
    if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1 or before.st_mode & 0o077:
        raise RuntimeError(f"submission sidecar is not a private regular file: {{path}}")
    if before.st_size > maximum_bytes:
        raise RuntimeError(f"submission sidecar exceeds its bound: {{path}}")
    data = path.read_bytes()
    after = os.lstat(path)
    if (before.st_ino, before.st_size, before.st_mtime_ns) != (
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
    ):
        raise RuntimeError(f"submission sidecar changed while reading: {{path}}")
    return data

expected = {{
    "session_id": session_id,
    "submission_id": submission_id,
    "scheduler_provider": provider,
    "submission_marker": marker,
}}
intent = json.loads(read_private(intent_raw, 65536))
if (
    intent.get("schema_version") != "clio-relay.gateway-submission-intent.v1"
    or any(intent.get(k) != v for k, v in expected.items())
):
    raise RuntimeError("scheduler submission intent identity mismatch")
output = read_private(output_raw, maximum + 1)
meta = json.loads(read_private(meta_raw, 65536))
if (
    meta.get("schema_version") != "clio-relay.gateway-submission-output.v1"
    or any(meta.get(k) != v for k, v in expected.items())
):
    raise RuntimeError("scheduler submission output identity mismatch")
if (
    meta.get("output_sha256") != hashlib.sha256(output).hexdigest()
    or meta.get("output_size") != len(output)
):
    raise RuntimeError("scheduler submission output digest mismatch")
record = {{
    "schema_version": "clio-relay.gateway-submission-sidecar.v1",
    **expected,
    "returncode": int(meta["returncode"]),
    "output": output.decode("utf-8"),
    "output_sha256": meta["output_sha256"],
    "output_size": len(output),
    "output_truncated": meta.get("output_truncated") is True,
    "recorded_at": datetime.now(timezone.utc).isoformat(),
}}
record_path = Path(record_raw)
if record_path.exists():
    existing = json.loads(read_private(record_raw, maximum + 65536))
    if any(existing.get(k) != v for k, v in record.items() if k != "recorded_at"):
        raise RuntimeError("scheduler submission record conflicts with anchored output")
else:
    temporary = record_path.with_name(f".{{record_path.name}}.{{os.getpid()}}.tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        json.dump(record, handle, sort_keys=True)
        handle.flush()
        os.fsync(handle.fileno())
    os.chmod(temporary, 0o600)
    os.replace(temporary, record_path)
    directory = os.open(record_path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(directory)
    finally:
        os.close(directory)
print(record["output"], end="")
raise SystemExit(record["returncode"])
__CLIO_RECORD_SUBMISSION__
"""


def _remote_submission_record_script(
    *,
    session_id: str,
    submission_id: str,
    scheduler_provider: str,
    submission_marker: str,
) -> str:
    """Validate and promote one exact anchored scheduler-submission output."""
    return f"""set -euo pipefail
session_id={shlex.quote(session_id)}
submission_id={shlex.quote(submission_id)}
scheduler_provider={shlex.quote(scheduler_provider)}
submission_marker={shlex.quote(submission_marker)}
root="$HOME/.local/share/clio-relay/service-sessions/$session_id/submissions"
record_file="$root/$submission_id.json"
output_file="$root/$submission_id.out"
output_meta="$root/$submission_id.out.json"
intent_file="$root/$submission_id.intent.json"
python3 - "$record_file" "$output_file" "$output_meta" "$intent_file" \
  "$session_id" "$submission_id" "$scheduler_provider" "$submission_marker" \
  {int(_MAX_SUBMISSION_OUTPUT_BYTES)} <<'__CLIO_READ_SUBMISSION__'
import hashlib
import json
import os
import stat
import sys
from datetime import datetime, timezone
from pathlib import Path

(
    record_raw,
    output_raw,
    meta_raw,
    intent_raw,
    session_id,
    submission_id,
    provider,
    marker,
    maximum_raw,
) = sys.argv[1:]
maximum = int(maximum_raw)

def read_private(path_raw, maximum_bytes):
    path = Path(path_raw)
    before = os.lstat(path)
    if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1 or before.st_mode & 0o077:
        raise RuntimeError(f"submission sidecar is not a private regular file: {{path}}")
    if before.st_size > maximum_bytes:
        raise RuntimeError(f"submission sidecar exceeds its bound: {{path}}")
    data = path.read_bytes()
    after = os.lstat(path)
    if (before.st_ino, before.st_size, before.st_mtime_ns) != (
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
    ):
        raise RuntimeError(f"submission sidecar changed while reading: {{path}}")
    return data

expected = {{
    "session_id": session_id,
    "submission_id": submission_id,
    "scheduler_provider": provider,
    "submission_marker": marker,
}}
intent_path = Path(intent_raw)
if not intent_path.exists():
    print(json.dumps({{"present": False}}))
    raise SystemExit(0)
intent = json.loads(read_private(intent_raw, 65536))
if (
    intent.get("schema_version") != "clio-relay.gateway-submission-intent.v1"
    or any(intent.get(k) != v for k, v in expected.items())
):
    raise RuntimeError("scheduler submission intent identity mismatch")
record_path = Path(record_raw)
if record_path.exists():
    record = json.loads(read_private(record_raw, maximum + 65536))
else:
    output_exists = Path(output_raw).exists()
    meta_exists = Path(meta_raw).exists()
    if not output_exists and not meta_exists:
        print(json.dumps({{"present": False, "anchored": True}}))
        raise SystemExit(0)
    if output_exists != meta_exists:
        raise RuntimeError("scheduler submission output is incomplete or ambiguous")
    output = read_private(output_raw, maximum + 1)
    meta = json.loads(read_private(meta_raw, 65536))
    if (
        meta.get("schema_version") != "clio-relay.gateway-submission-output.v1"
        or any(meta.get(k) != v for k, v in expected.items())
    ):
        raise RuntimeError("scheduler submission output identity mismatch")
    if (
        meta.get("output_sha256") != hashlib.sha256(output).hexdigest()
        or meta.get("output_size") != len(output)
    ):
        raise RuntimeError("scheduler submission output digest mismatch")
    record = {{
        "schema_version": "clio-relay.gateway-submission-sidecar.v1",
        **expected,
        "returncode": int(meta["returncode"]),
        "output": output.decode("utf-8"),
        "output_sha256": meta["output_sha256"],
        "output_size": len(output),
        "output_truncated": meta.get("output_truncated") is True,
        "recorded_at": datetime.now(timezone.utc).isoformat(),
    }}
    temporary = record_path.with_name(f".{{record_path.name}}.{{os.getpid()}}.tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        json.dump(record, handle, sort_keys=True)
        handle.flush()
        os.fsync(handle.fileno())
    os.chmod(temporary, 0o600)
    os.replace(temporary, record_path)
    directory = os.open(record_path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(directory)
    finally:
        os.close(directory)
if (
    record.get("schema_version") != "clio-relay.gateway-submission-sidecar.v1"
    or any(record.get(k) != v for k, v in expected.items())
):
    raise RuntimeError("scheduler submission record identity mismatch")
output = record.get("output")
if not isinstance(output, str) or len(output.encode("utf-8")) > maximum + 1:
    raise RuntimeError("scheduler submission record output is invalid")
if record.get("output_sha256") != hashlib.sha256(output.encode("utf-8")).hexdigest():
    raise RuntimeError("scheduler submission record output digest mismatch")
record["present"] = True
print(json.dumps(record))
__CLIO_READ_SUBMISSION__
"""


def _template_command_script(command: Sequence[str], scheduler_job_id: str) -> str:
    templated = [part.format(scheduler_job_id=scheduler_job_id) for part in command]
    return "set -euo pipefail\n" + shlex.join(templated) + "\n"


def _remote_scheduler_script(
    *,
    definition: ClusterDefinition,
    operation: Literal["status", "cancel", "connector-placement"],
    provider: str,
    scheduler_job_id: str,
) -> str:
    command = [
        "clio-relay",
        "scheduler",
        operation,
        scheduler_job_id,
        "--cluster",
        definition.name,
        "--provider",
        provider,
    ]
    return f"set -euo pipefail\n{remote_env(definition)} {shlex.join(command)}\n"


def _remote_connector_step_status_script(
    *,
    definition: ClusterDefinition,
    provider: str,
    scheduler_job_id: str,
    scheduler_step_id: str,
    placement_host: str,
) -> str:
    command = [
        "clio-relay",
        "scheduler",
        "connector-step-status",
        scheduler_step_id,
        "--scheduler-job-id",
        scheduler_job_id,
        "--cluster",
        definition.name,
        "--provider",
        provider,
        "--placement-host",
        placement_host,
    ]
    return f"set -euo pipefail\n{remote_env(definition)} {shlex.join(command)}\n"


def _remote_connector_step_cancel_script(
    *,
    definition: ClusterDefinition,
    provider: str,
    scheduler_job_id: str,
    scheduler_step_id: str,
) -> str:
    command = [
        "clio-relay",
        "scheduler",
        "connector-step-cancel",
        scheduler_step_id,
        "--scheduler-job-id",
        scheduler_job_id,
        "--cluster",
        definition.name,
        "--provider",
        provider,
    ]
    return f"set -euo pipefail\n{remote_env(definition)} {shlex.join(command)}\n"


def _remote_connector_step_reconcile_script(
    *,
    definition: ClusterDefinition,
    provider: str,
    scheduler_job_id: str,
    step_marker: str,
    placement_host: str,
) -> str:
    command = [
        "clio-relay",
        "scheduler",
        "connector-step-reconcile",
        scheduler_job_id,
        "--cluster",
        definition.name,
        "--provider",
        provider,
        "--placement-host",
        placement_host,
        "--step-marker",
        step_marker,
    ]
    return f"set -euo pipefail\n{remote_env(definition)} {shlex.join(command)}\n"


def _remote_http_probe_script(
    host: str,
    port: int,
    path: str,
    *,
    expected_body: str | None = None,
) -> str:
    encoded_body = (
        ""
        if expected_body is None
        else base64.b64encode(expected_body.encode("utf-8")).decode("ascii")
    )
    probe_arguments = shlex.join((host, str(port), path, encoded_body))
    return f"""set -euo pipefail
python3 - {probe_arguments} <<'__CLIO_SERVICE_HEALTH__'
import base64
import http.client
import sys
host, port, path, encoded_body = sys.argv[1], int(sys.argv[2]), sys.argv[3], sys.argv[4]
expected_body = base64.b64decode(encoded_body) if encoded_body else None
try:
    conn = http.client.HTTPConnection(host, port, timeout=5)
    conn.request("GET", path)
    response = conn.getresponse()
    body = response.read(4097)
    healthy = (
        200 <= response.status < 300
        and len(body) <= 4096
        and (expected_body is None or body == expected_body)
    )
    print(f"service_health={{'ok' if healthy else 'bad'}}")
    print(f"service_status={{response.status}}")
    conn.close()
except (OSError, http.client.HTTPException) as exc:
    print(f"service_health=unreachable")
    print(f"service_error={{exc}}")
__CLIO_SERVICE_HEALTH__
"""


def _remote_allocation_frpc_start_script(
    *,
    definition: ClusterDefinition,
    session_id: str,
    config_text: str,
    owner_token: str,
    connector_generation_id: str,
    allocation_provider: str,
    allocation_job_id: str,
    placement: SchedulerConnectorPlacement,
    step_marker: str,
) -> str:
    """Launch frpc as a durable scheduler step, never as a login-node child PID."""
    encoded = base64.b64encode(config_text.encode("utf-8")).decode("ascii")
    frpc_bin = definition.frpc_bin or "$HOME/.local/bin/frpc"
    if (
        placement.scheduler != allocation_provider
        or placement.scheduler_job_id != allocation_job_id
    ):
        raise ConfigurationError("connector placement does not match its allocation")
    placement_host = placement.placement_host
    placement_json = placement.model_dump_json()
    return f"""set -euo pipefail
umask 077
{remote_env(definition)}
session_id={shlex.quote(session_id)}
session_dir="$HOME/.local/share/clio-relay/service-sessions/$session_id"
mkdir -p "$session_dir"
chmod 700 "$session_dir"
exec 9>"$session_dir/transition.lock"
flock -w 10 -x 9 || {{ echo "connector start lock timed out" >&2; exit 75; }}
config_file="$session_dir/remote-frpc.toml"
log_file="$session_dir/remote-frpc.log"
metadata_file="$session_dir/metadata.json"
step_file="$session_dir/scheduler-connector-step.json"
pending_file="$session_dir/scheduler-connector-step.pending.json"
reconcile_file="$session_dir/scheduler-connector-step.reconcile.json"
reconcile_state_file="$session_dir/scheduler-connector-step.reconcile-state"
python3 - "$config_file" <<'__CLIO_WRITE_ALLOCATION_FRPC__'
import base64
import os
import sys
from pathlib import Path

path = Path(sys.argv[1])
temporary = path.with_name(f".{{path.name}}.{{os.getpid()}}.tmp")
with temporary.open("w", encoding="utf-8") as handle:
    handle.write(base64.b64decode({encoded!r}).decode("utf-8"))
    handle.flush()
    os.fsync(handle.fileno())
os.chmod(temporary, 0o600)
os.replace(temporary, path)
directory = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
try:
    os.fsync(directory)
finally:
    os.close(directory)
__CLIO_WRITE_ALLOCATION_FRPC__
python3 - "$metadata_file" "$session_id" "$config_file" "$log_file" \
  {shlex.quote(owner_token)} {shlex.quote(connector_generation_id)} \
  {shlex.quote(allocation_provider)} {shlex.quote(allocation_job_id)} \
  {shlex.quote(placement_host)} {shlex.quote(step_marker)} \
  {shlex.quote(placement_json)} <<'__CLIO_ALLOCATION_INTENT__'
import json
import os
import stat
import sys
from datetime import datetime, timezone
from pathlib import Path

(
    metadata_raw,
    session_id,
    config_path,
    log_path,
    owner_token,
    generation_id,
    provider,
    job_id,
    placement_host,
    step_marker,
    placement_raw,
) = sys.argv[1:]
path = Path(metadata_raw)
expected = {{
    "schema_version": "clio-relay.allocation-connector-sidecar.v1",
    "owner": "clio-relay",
    "session_id": session_id,
    "owner_token": owner_token,
    "connector_generation_id": generation_id,
    "execution_scope": "scheduler_allocation",
    "scheduler_provider": provider,
    "scheduler_native_id": job_id,
    "scheduler_step_marker": step_marker,
    "placement": json.loads(placement_raw),
    "remote_frpc_config": config_path,
    "remote_frpc_log": log_path,
}}
try:
    before = os.lstat(path)
except FileNotFoundError:
    current = None
else:
    if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1 or before.st_mode & 0o077:
        raise RuntimeError("allocation connector sidecar is not a private regular file")
    if before.st_size > 65536:
        raise RuntimeError("allocation connector sidecar exceeds its size bound")
    current = json.loads(path.read_text(encoding="utf-8"))
    after = os.lstat(path)
    if (before.st_ino, before.st_size, before.st_mtime_ns) != (
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
    ):
        raise RuntimeError("allocation connector sidecar changed while reading")
    if not isinstance(current, dict) or any(
        current.get(key) != value for key, value in expected.items()
    ):
        raise RuntimeError("allocation connector sidecar identity does not match launch intent")
payload = {{
    **expected,
    "state": "starting",
    "scheduler_step": current.get("scheduler_step") if isinstance(current, dict) else None,
    "updated_at": datetime.now(timezone.utc).isoformat(),
}}
temporary = path.with_name(f".{{path.name}}.{{os.getpid()}}.tmp")
with temporary.open("w", encoding="utf-8") as handle:
    json.dump(payload, handle, sort_keys=True)
    handle.flush()
    os.fsync(handle.fileno())
os.chmod(temporary, 0o600)
os.replace(temporary, path)
directory = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
try:
    os.fsync(directory)
finally:
    os.close(directory)
__CLIO_ALLOCATION_INTENT__
clio-relay scheduler connector-step-reconcile \
  {shlex.quote(allocation_job_id)} \
  --cluster {shlex.quote(definition.name)} \
  --provider {shlex.quote(allocation_provider)} \
  --placement-host {shlex.quote(placement_host)} \
  --step-marker {shlex.quote(step_marker)} >"$reconcile_file"
python3 - "$reconcile_file" \
  {shlex.quote(allocation_provider)} {shlex.quote(allocation_job_id)} \
  {shlex.quote(placement_host)} {shlex.quote(step_marker)} \
  >"$reconcile_state_file" \
  <<'__CLIO_RECONCILE_STATE__'
import json
import sys
from pathlib import Path

path, provider, job_id, placement_host, step_marker = sys.argv[1:]
record = json.loads(Path(path).read_text(encoding="utf-8"))
valid = (
    isinstance(record, dict)
    and record.get("schema_version")
    == "clio-relay.scheduler-connector-step-reconciliation.v1"
    and record.get("scheduler") == provider
    and record.get("scheduler_job_id") == job_id
    and record.get("placement_host") == placement_host
    and record.get("step_marker") == step_marker
    and isinstance(record.get("found"), bool)
)
if not valid:
    raise RuntimeError("connector step reconciliation identity is invalid")
print("found" if record["found"] else "absent")
__CLIO_RECONCILE_STATE__
reconcile_state=""
IFS= read -r reconcile_state <"$reconcile_state_file"
if [ "$reconcile_state" != "found" ] && [ "$reconcile_state" != "absent" ]; then
  echo "connector step reconciliation returned an invalid state" >&2
  exit 75
fi
candidate_file="$reconcile_file"
candidate_kind="reconciliation"
if [ "$reconcile_state" = "absent" ]; then
  frpc_bin={render_remote_shell_value(frpc_bin, field="frpc_bin")}
  clio-relay scheduler connector-step-start \
    {shlex.quote(allocation_job_id)} \
    --cluster {shlex.quote(definition.name)} \
    --provider {shlex.quote(allocation_provider)} \
    --placement-host {shlex.quote(placement_host)} \
    --step-marker {shlex.quote(step_marker)} \
    --output-path "$log_file" -- \
    "$frpc_bin" -c "$config_file" >"$pending_file"
  candidate_file="$pending_file"
  candidate_kind="launch"
fi
python3 - "$candidate_file" "$candidate_kind" "$metadata_file" "$step_file" \
  "$session_id" "$config_file" "$log_file" \
  {shlex.quote(owner_token)} {shlex.quote(connector_generation_id)} \
  {shlex.quote(allocation_provider)} {shlex.quote(allocation_job_id)} \
  {shlex.quote(placement_host)} {shlex.quote(step_marker)} \
  <<'__CLIO_RECORD_ALLOCATION_STEP__'
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

(
    candidate_raw,
    candidate_kind,
    metadata_raw,
    step_raw,
    session_id,
    config_path,
    log_path,
    owner_token,
    generation_id,
    provider,
    job_id,
    placement_host,
    step_marker,
) = sys.argv[1:]
candidate = json.loads(Path(candidate_raw).read_text(encoding="utf-8"))
step = candidate.get("step") if candidate_kind == "reconciliation" else candidate
if not isinstance(step, dict):
    raise RuntimeError("scheduler connector launch omitted its step identity")
expected_step_prefix = f"{{job_id}}."
step_id = step.get("scheduler_step_id")
valid_step = (
    step.get("schema_version") == "clio-relay.scheduler-connector-step.v1"
    and step.get("scheduler") == provider
    and step.get("scheduler_job_id") == job_id
    and isinstance(step_id, str)
    and step_id.startswith(expected_step_prefix)
    and step_id[len(expected_step_prefix):].isdecimal()
    and step.get("step_marker") == step_marker
    and step.get("placement_host") == placement_host
    and step.get("source")
    in {{"slurm-srun-detached-marker", "slurm-squeue-step-marker"}}
    and step.get("verified") is True
)
if not valid_step:
    raise RuntimeError("scheduler connector step identity does not match launch intent")
metadata_path = Path(metadata_raw)
metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
expected_metadata = {{
    "schema_version": "clio-relay.allocation-connector-sidecar.v1",
    "owner": "clio-relay",
    "session_id": session_id,
    "owner_token": owner_token,
    "connector_generation_id": generation_id,
    "execution_scope": "scheduler_allocation",
    "scheduler_provider": provider,
    "scheduler_native_id": job_id,
    "scheduler_step_marker": step_marker,
    "remote_frpc_config": config_path,
    "remote_frpc_log": log_path,
}}
if not isinstance(metadata, dict) or any(
    metadata.get(key) != value for key, value in expected_metadata.items()
):
    raise RuntimeError("allocation connector sidecar changed before step recording")

def atomic_json(path, payload):
    temporary = path.with_name(f".{{path.name}}.{{os.getpid()}}.tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, sort_keys=True)
        handle.flush()
        os.fsync(handle.fileno())
    os.chmod(temporary, 0o600)
    os.replace(temporary, path)

atomic_json(Path(step_raw), step)
metadata["state"] = "recorded"
metadata["scheduler_step"] = step
metadata["updated_at"] = datetime.now(timezone.utc).isoformat()
atomic_json(metadata_path, metadata)
directory = os.open(metadata_path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
try:
    os.fsync(directory)
finally:
    os.close(directory)
print(json.dumps({{
    "schema_version": "clio-relay.allocation-connector-start.v1",
    "session_id": session_id,
    "connector_generation_id": generation_id,
    "config_path": config_path,
    "log_path": log_path,
    "step_identity": step,
}}))
__CLIO_RECORD_ALLOCATION_STEP__
rm -f -- "$pending_file" "$reconcile_file" "$reconcile_state_file"
"""


def _remote_frpc_start_script(
    *,
    definition: ClusterDefinition,
    session_id: str,
    config_text: str,
    owner_token: str,
    connector_generation_id: str,
) -> str:
    encoded = base64.b64encode(config_text.encode("utf-8")).decode("ascii")
    frpc_bin = definition.frpc_bin or "$HOME/.local/bin/frpc"
    return f"""set -euo pipefail
umask 077
{remote_env(definition)}
session_id={shlex.quote(session_id)}
session_dir="$HOME/.local/share/clio-relay/service-sessions/$session_id"
mkdir -p "$session_dir"
exec 9>"$session_dir/transition.lock"
flock -w 10 -x 9 || {{ echo "connector start lock timed out" >&2; exit 75; }}
config_file="$session_dir/remote-frpc.toml"
log_file="$session_dir/remote-frpc.log"
pid_file="$session_dir/remote-frpc.pid"
metadata_file="$session_dir/metadata.json"
python3 - "$metadata_file" "$pid_file" "$session_id" <<'__CLIO_CONNECTOR_PREFLIGHT__'
import json
import os
import sys
from pathlib import Path

metadata_path, pid_path, session_id = sys.argv[1:]
try:
    metadata = json.loads(Path(metadata_path).read_text(encoding="utf-8"))
except (FileNotFoundError, json.JSONDecodeError):
    metadata = {{}}
token = metadata.get("owner_token")
generation_id = metadata.get("connector_generation_id")
pgid = metadata.get("remote_frpc_pgid")
recorded_pid = metadata.get("remote_frpc_pid")
if not isinstance(recorded_pid, int):
    try:
        recorded_pid = int(Path(pid_path).read_text(encoding="utf-8").strip())
    except (OSError, ValueError):
        recorded_pid = None
active_recorded_pid = False
if isinstance(recorded_pid, int):
    try:
        state = (Path("/proc") / str(recorded_pid) / "stat").read_text(
            encoding="utf-8"
        ).rsplit(")", 1)[1].split()[0]
        active_recorded_pid = state != "Z"
    except (OSError, IndexError):
        pass
matches = []
active_group_pids = []
complete_identity = (
    metadata.get("owner") == "clio-relay"
    and metadata.get("session_id") == session_id
    and isinstance(token, str)
    and token
    and isinstance(generation_id, str)
    and generation_id
    and isinstance(pgid, int)
)
if isinstance(pgid, int):
    token_marker = (
        f"CLIO_RELAY_CONNECTOR_OWNER_TOKEN={{token}}".encode()
        if complete_identity
        else None
    )
    generation_marker = (
        f"CLIO_RELAY_CONNECTOR_GENERATION_ID={{generation_id}}".encode()
        if complete_identity
        else None
    )
    for proc in Path("/proc").iterdir():
        if not proc.name.isdigit():
            continue
        member_pid = int(proc.name)
        try:
            state = (proc / "stat").read_text(encoding="utf-8").rsplit(")", 1)[1].split()[0]
            process_group = os.getpgid(member_pid)
        except (FileNotFoundError, ProcessLookupError):
            continue
        except (OSError, IndexError, ValueError) as exc:
            raise RuntimeError(
                f"cannot inspect prior connector candidate {{member_pid}}: {{exc}}"
            ) from exc
        if state == "Z" or process_group != pgid:
            continue
        active_group_pids.append(member_pid)
        try:
            environment = (proc / "environ").read_bytes().split(bytes([0]))
        except (FileNotFoundError, ProcessLookupError):
            continue
        except OSError as exc:
            raise RuntimeError(
                f"cannot verify prior connector candidate {{member_pid}}: {{exc}}"
            ) from exc
        if (
            token_marker is not None
            and generation_marker is not None
            and token_marker in environment
            and generation_marker in environment
        ):
            matches.append(member_pid)
if matches:
    raise RuntimeError(f"owned remote connector is already active: pids={{matches}}")
if active_recorded_pid or active_group_pids:
    raise RuntimeError(
        "refusing to replace an active remote connector without complete ownership proof: "
        f"pid={{recorded_pid}} group_pids={{active_group_pids}}"
    )
Path(pid_path).unlink(missing_ok=True)
__CLIO_CONNECTOR_PREFLIGHT__
python3 - "$config_file" <<'__CLIO_WRITE_FRPC__'
import base64
import sys
path = sys.argv[1]
data = base64.b64decode({encoded!r}).decode("utf-8")
with open(path, "w", encoding="utf-8") as handle:
    handle.write(data)
__CLIO_WRITE_FRPC__
frpc_bin={render_remote_shell_value(frpc_bin, field="frpc_bin")}
owner_token={shlex.quote(owner_token)}
connector_generation_id={shlex.quote(connector_generation_id)}
pid=""
start_complete=0
cleanup_incomplete_start() {{
  if [ "$start_complete" = "1" ] || [ -z "$pid" ]; then return; fi
  kill -- "-$pid" 2>/dev/null || kill "$pid" 2>/dev/null || true
  for _ in 1 2 3 4 5; do
    if ! kill -0 -- "-$pid" 2>/dev/null; then break; fi
    sleep 0.2
  done
  if kill -0 -- "-$pid" 2>/dev/null; then
    kill -9 -- "-$pid" 2>/dev/null || kill -9 "$pid" 2>/dev/null || true
  fi
  for _ in 1 2 3 4 5; do
    if ! kill -0 -- "-$pid" 2>/dev/null; then break; fi
    sleep 0.1
  done
  if kill -0 -- "-$pid" 2>/dev/null; then
    echo "incomplete remote connector process group cleanup: $pid" >&2
    return 1
  fi
  python3 - \
    "$metadata_file" "$pid_file" "$pid" "$connector_generation_id" \
    <<'__CLIO_CONNECTOR_ROLLBACK__'
import json
import sys
from pathlib import Path

metadata_path, pid_path, pid_raw, generation_id = sys.argv[1:]
metadata_file = Path(metadata_path)
try:
    metadata = json.loads(metadata_file.read_text(encoding="utf-8"))
except (FileNotFoundError, json.JSONDecodeError):
    metadata = None
if (
    isinstance(metadata, dict)
    and str(metadata.get("remote_frpc_pid")) == pid_raw
    and metadata.get("connector_generation_id") == generation_id
):
    metadata_file.unlink(missing_ok=True)
try:
    recorded_pid = Path(pid_path).read_text(encoding="utf-8").strip()
except OSError:
    recorded_pid = None
if recorded_pid == pid_raw:
    Path(pid_path).unlink(missing_ok=True)
__CLIO_CONNECTOR_ROLLBACK__
}}
trap cleanup_incomplete_start EXIT
nohup setsid env \
  "CLIO_RELAY_CONNECTOR_OWNER_TOKEN=$owner_token" \
  "CLIO_RELAY_CONNECTOR_GENERATION_ID=$connector_generation_id" \
  "$frpc_bin" -c "$config_file" >"$log_file" 2>&1 9>&- &
pid="$!"
echo "$pid" > "$pid_file"
python3 - "$metadata_file" "$pid" "$config_file" "$log_file" \
  "$owner_token" "$connector_generation_id" <<'__CLIO_METADATA__'
import json
import os
import sys
import time
from datetime import datetime, timezone
metadata_file, pid, config_file, log_file, owner_token, generation_id = sys.argv[1:]
pid_value = int(pid)
for _ in range(40):
    try:
        process_group_id = os.getpgid(pid_value)
        with open(f"/proc/{{pid}}/environ", "rb") as handle:
            environment = handle.read().split(bytes([0]))
    except OSError:
        time.sleep(0.05)
        continue
    if (
        process_group_id == pid_value
        and f"CLIO_RELAY_CONNECTOR_OWNER_TOKEN={{owner_token}}".encode() in environment
        and f"CLIO_RELAY_CONNECTOR_GENERATION_ID={{generation_id}}".encode() in environment
    ):
        break
    time.sleep(0.05)
else:
    raise RuntimeError("owned connector did not establish its isolated process group")
with open(f"/proc/{{pid}}/stat", encoding="utf-8") as handle:
    process_start_ticks = handle.read().rsplit(")", 1)[1].split()[19]
temporary = f"{{metadata_file}}.{{os.getpid()}}.tmp"
with open(temporary, "w", encoding="utf-8") as handle:
    json.dump({{
        "owner": "clio-relay",
        "session_id": {session_id!r},
        "remote_frpc_pid": pid_value,
        "remote_frpc_pgid": process_group_id,
        "remote_frpc_config": config_file,
        "remote_frpc_log": log_file,
        "owner_token": owner_token,
        "connector_generation_id": generation_id,
        "process_start_ticks": process_start_ticks,
        "started_at": datetime.now(timezone.utc).isoformat(),
    }}, handle, indent=2)
os.chmod(temporary, 0o600)
os.replace(temporary, metadata_file)
__CLIO_METADATA__
sleep 1
if ! kill -0 "$pid" 2>/dev/null; then
  cat "$log_file" >&2
  exit 1
fi
start_complete=1
trap - EXIT
echo "remote_frpc_pid=$pid"
echo "remote_frpc_config=$config_file"
echo "remote_frpc_log=$log_file"
echo "remote_frpc_pgid=$pid"
echo "connector_generation_id=$connector_generation_id"
"""


def _remote_connector_discovery_script(
    *,
    session_id: str,
    owner_token: str,
    connector_generation_id: str,
    allocation_provider: str | None = None,
    allocation_job_id: str | None = None,
    allocation_step_marker: str | None = None,
    allocation_placement_host: str | None = None,
) -> str:
    """Discover one remote connector by its pre-recorded unforgeable identity."""
    return f"""set -euo pipefail
session_id={shlex.quote(session_id)}
owner_token={shlex.quote(owner_token)}
generation_id={shlex.quote(connector_generation_id)}
session_dir="$HOME/.local/share/clio-relay/service-sessions/$session_id"
python3 - "$session_dir" "$session_id" "$owner_token" "$generation_id" \
  {shlex.quote(allocation_provider or "")} {shlex.quote(allocation_job_id or "")} \
  {shlex.quote(allocation_step_marker or "")} \
  {shlex.quote(allocation_placement_host or "")} \
  <<'__CLIO_DISCOVER_CONNECTOR__'
import json
import os
import stat
import sys
from pathlib import Path

(
    session_dir,
    session_id,
    owner_token,
    generation_id,
    expected_provider,
    expected_job_id,
    expected_step_marker,
    expected_placement_host,
) = sys.argv[1:]
directory = Path(session_dir)
metadata_path = directory / "metadata.json"
try:
    metadata_before = os.lstat(metadata_path)
    if (
        not stat.S_ISREG(metadata_before.st_mode)
        or metadata_before.st_nlink != 1
        or metadata_before.st_mode & 0o077
        or metadata_before.st_size > 65536
    ):
        raise RuntimeError("remote connector sidecar is not a private bounded file")
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    metadata_after = os.lstat(metadata_path)
    if (
        metadata_before.st_ino,
        metadata_before.st_size,
        metadata_before.st_mtime_ns,
    ) != (
        metadata_after.st_ino,
        metadata_after.st_size,
        metadata_after.st_mtime_ns,
    ):
        raise RuntimeError("remote connector sidecar changed while reading")
except (FileNotFoundError, json.JSONDecodeError, OSError):
    metadata = None
if isinstance(metadata, dict) and metadata.get("schema_version") == (
    "clio-relay.allocation-connector-sidecar.v1"
):
    placement = metadata.get("placement")
    expected_identity = (
        bool(expected_provider)
        and bool(expected_job_id)
        and bool(expected_step_marker)
        and bool(expected_placement_host)
        and metadata.get("owner") == "clio-relay"
        and metadata.get("session_id") == session_id
        and metadata.get("owner_token") == owner_token
        and metadata.get("connector_generation_id") == generation_id
        and metadata.get("execution_scope") == "scheduler_allocation"
        and metadata.get("scheduler_provider") == expected_provider
        and metadata.get("scheduler_native_id") == expected_job_id
        and metadata.get("scheduler_step_marker") == expected_step_marker
        and isinstance(placement, dict)
        and placement.get("scheduler") == expected_provider
        and placement.get("scheduler_job_id") == expected_job_id
        and placement.get("placement_host") == expected_placement_host
        and placement.get("allocation_node_count") == 1
        and placement.get("verified") is True
        and isinstance(metadata.get("remote_frpc_config"), str)
        and isinstance(metadata.get("remote_frpc_log"), str)
    )
    if not expected_identity:
        print(json.dumps({{
            "present": False,
            "ownership_verified": False,
            "error": "allocation connector sidecar identity does not match its intent",
        }}))
        raise SystemExit(0)
    step = metadata.get("scheduler_step")
    if not isinstance(step, dict):
        for candidate_path in (
            directory / "scheduler-connector-step.json",
            directory / "scheduler-connector-step.pending.json",
            directory / "scheduler-connector-step.reconcile.json",
        ):
            try:
                candidate = json.loads(candidate_path.read_text(encoding="utf-8"))
            except (FileNotFoundError, json.JSONDecodeError, OSError):
                continue
            if isinstance(candidate, dict) and isinstance(candidate.get("step"), dict):
                candidate = candidate["step"]
            if isinstance(candidate, dict):
                step = candidate
                break
    connector = {{
        "owner": "clio-relay",
        "session_id": session_id,
        "execution_scope": "scheduler_allocation",
        "scheduler_provider": expected_provider,
        "scheduler_native_id": expected_job_id,
        "scheduler_step_marker": expected_step_marker,
        "connector_generation_id": generation_id,
        "owner_token": owner_token,
        "config_path": metadata["remote_frpc_config"],
        "log_path": metadata["remote_frpc_log"],
        "placement": placement,
    }}
    if isinstance(step, dict):
        connector["scheduler_step"] = step
        connector["scheduler_step_id"] = step.get("scheduler_step_id")
        print(json.dumps({{
            "present": True,
            "ownership_verified": True,
            "connector": connector,
        }}))
    else:
        print(json.dumps({{
            "present": False,
            "ownership_verified": True,
            "reconciliation_required": True,
            "connector": connector,
        }}))
    raise SystemExit(0)
token_marker = f"CLIO_RELAY_CONNECTOR_OWNER_TOKEN={{owner_token}}".encode()
generation_marker = f"CLIO_RELAY_CONNECTOR_GENERATION_ID={{generation_id}}".encode()
matches = []
observation_errors = []
for proc in Path("/proc").iterdir():
    if not proc.name.isdigit():
        continue
    try:
        if proc.stat().st_uid != os.geteuid():
            continue
    except FileNotFoundError:
        continue
    except OSError as exc:
        observation_errors.append(f"{{proc.name}}: owner lookup failed: {{exc}}")
        continue
    try:
        environment = (proc / "environ").read_bytes().split(bytes([0]))
        state = (proc / "stat").read_text(encoding="utf-8").rsplit(")", 1)[1].split()[0]
    except FileNotFoundError:
        continue
    except (OSError, IndexError) as exc:
        observation_errors.append(f"{{proc.name}}: identity read failed: {{exc}}")
        continue
    if state != "Z" and token_marker in environment and generation_marker in environment:
        matches.append(int(proc.name))
matches.sort()
if observation_errors:
    print(json.dumps({{
        "present": bool(matches),
        "ownership_verified": False,
        "matching_pids": matches,
        "error": "remote connector observation was incomplete: " + "; ".join(
            observation_errors[:20]
        ),
    }}))
    raise SystemExit(0)
if len(matches) > 1:
    print(json.dumps({{
        "present": True,
        "ownership_verified": False,
        "matching_pids": matches,
        "error": "multiple processes matched one connector intent",
    }}))
    raise SystemExit(0)
if len(matches) == 0:
    print(json.dumps({{
        "present": False,
        "ownership_verified": True,
        "matching_pids": [],
    }}))
    raise SystemExit(0)
pid = matches[0]
pgid = os.getpgid(pid)
if not isinstance(metadata, dict):
    metadata = {{
        "owner": "clio-relay",
        "session_id": session_id,
        "remote_frpc_pid": pid,
        "remote_frpc_pgid": pgid,
        "remote_frpc_config": str(directory / "remote-frpc.toml"),
        "remote_frpc_log": str(directory / "remote-frpc.log"),
        "owner_token": owner_token,
        "connector_generation_id": generation_id,
    }}
identity_valid = (
    metadata.get("owner") == "clio-relay"
    and metadata.get("session_id") == session_id
    and metadata.get("owner_token") == owner_token
    and metadata.get("connector_generation_id") == generation_id
    and metadata.get("remote_frpc_pid") == pid
    and metadata.get("remote_frpc_pgid") == pgid
)
connector = {{
    "owner": "clio-relay",
    "session_id": session_id,
    "pid": pid,
    "process_group_id": pgid,
    "connector_generation_id": generation_id,
    "owner_token": owner_token,
    "config_path": metadata.get("remote_frpc_config"),
    "log_path": metadata.get("remote_frpc_log"),
}}
print(json.dumps({{
    "present": True,
    "ownership_verified": identity_valid,
    "matching_pids": matches,
    "connector": connector,
}}))
__CLIO_DISCOVER_CONNECTOR__
"""


def _remote_stop_script(*, session_id: str, pid: int) -> str:
    return f"""set -euo pipefail
session_id={shlex.quote(session_id)}
pid={pid}
session_dir="$HOME/.local/share/clio-relay/service-sessions/$session_id"
metadata_file="$session_dir/metadata.json"
pid_file="$session_dir/remote-frpc.pid"
mkdir -p "$session_dir"
exec 9>"$session_dir/transition.lock"
flock -w 10 -x 9 || {{ echo "connector stop lock timed out" >&2; exit 75; }}
python3 - "$metadata_file" "$pid_file" "$pid" "$session_id" <<'__CLIO_STOP_CONNECTOR__'
import json
import os
import signal
import sys
import time
from pathlib import Path

metadata_file, pid_file, pid_raw, session_id = sys.argv[1:]
pid = int(pid_raw)
try:
    with open(metadata_file, encoding="utf-8") as handle:
        metadata = json.load(handle)
except (FileNotFoundError, json.JSONDecodeError) as exc:
    raise RuntimeError("durable connector ownership metadata is unavailable") from exc
if metadata.get("owner") != "clio-relay" or metadata.get("session_id") != session_id:
    raise RuntimeError("connector metadata owner/session mismatch")
if metadata.get("remote_frpc_pid") != pid:
    raise RuntimeError("connector pid does not match metadata")

token = metadata.get("owner_token")
generation_id = metadata.get("connector_generation_id")
pgid = metadata.get("remote_frpc_pgid")
durable_identity = (
    isinstance(token, str) and bool(token)
    and isinstance(generation_id, str) and bool(generation_id)
    and isinstance(pgid, int)
    and isinstance(metadata.get("process_start_ticks"), str)
    and isinstance(metadata.get("remote_frpc_config"), str)
)
if not durable_identity:
    raise RuntimeError("durable connector ownership identity is incomplete")


def owned_group_processes():
    token_marker = f"CLIO_RELAY_CONNECTOR_OWNER_TOKEN={{token}}".encode()
    generation_marker = f"CLIO_RELAY_CONNECTOR_GENERATION_ID={{generation_id}}".encode()
    matches = []
    for proc in Path("/proc").iterdir():
        if not proc.name.isdigit():
            continue
        member_pid = int(proc.name)
        try:
            if proc.stat().st_uid != os.geteuid():
                continue
            fields = (proc / "stat").read_text(encoding="utf-8").rsplit(")", 1)[1].split()
        except (FileNotFoundError, ProcessLookupError):
            continue
        except (OSError, IndexError, ValueError) as exc:
            raise RuntimeError(
                f"cannot inspect remote connector candidate {{member_pid}}: {{exc}}"
            ) from exc
        if fields[0] == "Z":
            continue
        try:
            environment = (proc / "environ").read_bytes().split(bytes([0]))
        except (FileNotFoundError, ProcessLookupError):
            continue
        except OSError as exc:
            raise RuntimeError(
                f"cannot verify remote connector candidate {{member_pid}}: {{exc}}"
            ) from exc
        if (
            token_marker in environment
            and generation_marker in environment
        ):
            matches.append(member_pid)
    return sorted(matches)


def signal_owned_processes(sig):
    if not hasattr(os, "pidfd_open") or not hasattr(signal, "pidfd_send_signal"):
        raise RuntimeError("race-safe pidfd connector cleanup is unavailable")
    signaled = []
    for member_pid in owned_group_processes():
        try:
            process_fd = os.pidfd_open(member_pid, 0)
        except ProcessLookupError:
            continue
        except OSError as exc:
            raise RuntimeError(f"cannot open connector pidfd for {{member_pid}}: {{exc}}") from exc
        try:
            if member_pid not in owned_group_processes():
                continue
            try:
                signal.pidfd_send_signal(process_fd, sig, None, 0)
            except ProcessLookupError:
                continue
            except OSError as exc:
                raise RuntimeError(
                    f"cannot signal owned connector pid {{member_pid}}: {{exc}}"
                ) from exc
            signaled.append(member_pid)
        finally:
            os.close(process_fd)
    return signaled


proc = Path("/proc") / str(pid)
matches = owned_group_processes()
if proc.exists():
    try:
        command = (proc / "cmdline").read_bytes().replace(bytes([0]), b" ").decode(
            "utf-8", errors="replace"
        )
        environment = (proc / "environ").read_bytes().split(bytes([0]))
        fields = (proc / "stat").read_text(encoding="utf-8").rsplit(")", 1)[1].split()
        leader_owned = (
            fields[0] != "Z"
            and fields[19] == metadata["process_start_ticks"]
            and os.getpgid(pid) == pgid
            and f"CLIO_RELAY_CONNECTOR_OWNER_TOKEN={{token}}".encode() in environment
            and f"CLIO_RELAY_CONNECTOR_GENERATION_ID={{generation_id}}".encode()
            in environment
            and "frpc" in command
            and metadata["remote_frpc_config"] in command
        )
    except FileNotFoundError:
        leader_owned = False
    except (OSError, IndexError) as exc:
        raise RuntimeError(f"connector leader ownership observation failed: {{exc}}") from exc
    if not leader_owned and not matches:
        raise RuntimeError("connector leader PID ownership proof failed")
if not matches:
    Path(pid_file).unlink(missing_ok=True)
    print(json.dumps({{
        "pid": pid,
        "outcome": "missing",
        "ownership_verified": True,
        "verified_after_operation": True,
        "residual": False,
        "remaining_pids": [],
    }}))
    raise SystemExit(0)

signal_owned_processes(signal.SIGTERM)
for _ in range(25):
    if not owned_group_processes():
        break
    time.sleep(0.2)
remaining = owned_group_processes()
if remaining:
    signal_owned_processes(signal.SIGKILL)
    time.sleep(0.2)
remaining = owned_group_processes()
if remaining:
    raise RuntimeError("connector process group remains after SIGKILL")
Path(pid_file).unlink(missing_ok=True)
print(json.dumps({{
    "pid": pid,
    "outcome": "stopped",
    "ownership_verified": True,
    "verified_after_operation": True,
    "residual": False,
    "remaining_pids": remaining,
}}))
__CLIO_STOP_CONNECTOR__
"""


def _remote_connector_status_script(*, session_id: str, pid: int) -> str:
    return f"""set -euo pipefail
session_id={shlex.quote(session_id)}
pid={pid}
metadata_file="$HOME/.local/share/clio-relay/service-sessions/$session_id/metadata.json"
python3 - "$metadata_file" "$pid" "$session_id" <<'__CLIO_CONNECTOR_STATUS__'
import json
import os
import sys
from pathlib import Path

metadata_path, pid_raw, session_id = sys.argv[1:]
pid = int(pid_raw)
try:
    metadata = json.loads(Path(metadata_path).read_text(encoding="utf-8"))
except (FileNotFoundError, json.JSONDecodeError) as exc:
    raise RuntimeError("durable connector ownership metadata is unavailable") from exc
token = metadata.get("owner_token")
generation_id = metadata.get("connector_generation_id")
pgid = metadata.get("remote_frpc_pgid")
config_path = metadata.get("remote_frpc_config")
durable = (
    metadata.get("owner") == "clio-relay"
    and metadata.get("session_id") == session_id
    and metadata.get("remote_frpc_pid") == pid
    and isinstance(token, str) and bool(token)
    and isinstance(generation_id, str) and bool(generation_id)
    and isinstance(pgid, int)
    and isinstance(config_path, str)
)
matches = []
if durable:
    token_marker = f"CLIO_RELAY_CONNECTOR_OWNER_TOKEN={{token}}".encode()
    generation_marker = f"CLIO_RELAY_CONNECTOR_GENERATION_ID={{generation_id}}".encode()
    for proc in Path("/proc").iterdir():
        if not proc.name.isdigit():
            continue
        member_pid = int(proc.name)
        try:
            if proc.stat().st_uid != os.geteuid():
                continue
            fields = (proc / "stat").read_text(encoding="utf-8").rsplit(")", 1)[1].split()
            process_group = os.getpgid(member_pid)
        except (FileNotFoundError, ProcessLookupError):
            continue
        except (OSError, IndexError, ValueError) as exc:
            raise RuntimeError(
                f"cannot inspect remote connector status candidate {{member_pid}}: {{exc}}"
            ) from exc
        if fields[0] == "Z" or process_group != pgid:
            continue
        try:
            environment = (proc / "environ").read_bytes().split(bytes([0]))
            command = (proc / "cmdline").read_bytes().replace(bytes([0]), b" ").decode(
                "utf-8", errors="replace"
            )
        except (FileNotFoundError, ProcessLookupError):
            continue
        except OSError as exc:
            raise RuntimeError(
                f"cannot verify remote connector status candidate {{member_pid}}: {{exc}}"
            ) from exc
        if (
            token_marker in environment
            and generation_marker in environment
            and "frpc" in command
            and config_path in command
        ):
            matches.append(member_pid)
print(json.dumps({{
    "pid": pid,
    "ownership_verified": durable and bool(matches),
    "running": bool(matches),
    "matching_pids": sorted(matches),
}}))
__CLIO_CONNECTOR_STATUS__
"""


@dataclass(frozen=True)
class RuntimeSubmission:
    """Structured submission result emitted by a deployment driver."""

    scheduler_job_id: str
    service_host: str | None = None


@dataclass(frozen=True)
class RuntimeStatus:
    """Structured status emitted by a deployment driver."""

    state: str | None = None
    service_host: str | None = None
    reason: str | None = None
    events: list[dict[str, object]] | None = None


def _parse_runtime_submission(output: str) -> RuntimeSubmission:
    """Parse structured JSON submission output from a deployment driver."""
    record = _last_json_object(output)
    scheduler_job_id = record.get("scheduler_job_id")
    if not isinstance(scheduler_job_id, str) or scheduler_job_id == "":
        raise RelayError(
            f"deployment output must include JSON field scheduler_job_id; received: {output!r}"
        )
    service_host = record.get("service_host")
    if service_host is not None and not isinstance(service_host, str):
        raise RelayError("deployment output JSON field service_host must be a string")
    return RuntimeSubmission(scheduler_job_id=scheduler_job_id, service_host=service_host)


def _parse_runtime_status(output: str) -> RuntimeStatus:
    """Parse structured JSON status output from a deployment driver."""
    record = _last_json_object(output)
    state = record.get("state")
    service_host = record.get("service_host")
    reason = record.get("reason")
    events = _runtime_events(record.get("events"))
    return RuntimeStatus(
        state=state if isinstance(state, str) else None,
        service_host=service_host if isinstance(service_host, str) else None,
        reason=reason if isinstance(reason, str) else None,
        events=events,
    )


def _last_json_object(output: str) -> dict[str, object]:
    stripped_output = output.strip()
    if stripped_output:
        try:
            loaded_output = json.loads(stripped_output)
        except json.JSONDecodeError:
            pass
        else:
            if isinstance(loaded_output, dict):
                return cast(dict[str, object], loaded_output)
    for line in reversed(output.splitlines()):
        stripped = line.strip()
        if not stripped.startswith("{"):
            continue
        try:
            loaded = json.loads(stripped)
        except json.JSONDecodeError:
            continue
        if isinstance(loaded, dict):
            return cast(dict[str, object], loaded)
    raise RelayError(f"deployment output must include a JSON object: {output!r}")


def _runtime_events(value: object) -> list[dict[str, object]] | None:
    if not isinstance(value, list):
        return None
    raw_items = cast(list[object], value)
    events: list[dict[str, object]] = []
    for item in raw_items:
        if not isinstance(item, dict):
            return None
        events.append(cast(dict[str, object], item))
    return events


def _key_value_output(output: str) -> dict[str, str]:
    if len(output.encode("utf-8")) > 16_384:
        raise RelayError("remote connector start response exceeded its size limit")
    lines = output.splitlines()
    if not lines or len(lines) > 16:
        raise RelayError("remote connector start returned an invalid response")
    values: dict[str, str] = {}
    for line in lines:
        key, separator, value = line.partition("=")
        key = key.strip()
        value = value.strip()
        if not separator or not key or not value or key in values:
            raise RelayError("remote connector start returned an invalid key/value response")
        values[key] = value
    return values


def _validated_remote_session_file(
    value: str,
    *,
    session_id: str,
    filename: str,
) -> PurePosixPath:
    """Validate an exact remote session-owned file path without trusting SSH output."""
    if len(value) > 4_096 or any(ord(character) < 32 for character in value):
        raise RelayError("remote connector start returned an invalid owned path")
    path = PurePosixPath(value)
    expected_tail = (
        ".local",
        "share",
        "clio-relay",
        "service-sessions",
        session_id,
        filename,
    )
    if (
        not path.is_absolute()
        or path.as_posix() != value
        or ".." in path.parts
        or tuple(path.parts[-len(expected_tail) :]) != expected_tail
    ):
        raise RelayError("remote connector start returned a path outside its owned session")
    return path


def _connector_step_marker(session_id: str, connector_generation_id: str) -> str:
    """Derive one bounded provider marker from durable connector ownership."""
    digest = hashlib.sha256(f"{session_id}\x00{connector_generation_id}".encode()).hexdigest()[:32]
    return f"clio-relay-connector-{digest}"


def _new_ownership_intent(state: str, **identity: object) -> dict[str, object]:
    """Return one versioned gateway ownership transition record."""
    return {
        "schema_version": _OWNERSHIP_INTENT_SCHEMA,
        "state": state,
        "updated_at": utc_now().isoformat(),
        **identity,
    }


def _validated_durable_scheduler_contract(
    session: GatewaySession,
    *,
    strict: bool = True,
) -> _DurableSchedulerContract:
    """Cross-check scheduler identity or explicit absence across durable records."""
    try:
        spec = ServiceRuntimeSpec.model_validate(session.gateway.get("runtime_spec"))
    except ValueError as exc:
        raise RelayError("owned runtime has no valid service runtime specification") from exc

    binding_document = session.gateway.get("jarvis_runtime_binding")
    if binding_document is not None:
        try:
            binding = JarvisServiceRuntimeBinding.model_validate(binding_document)
        except ValueError as exc:
            raise RelayError("owned runtime has an invalid JARVIS runtime binding") from exc
        provider = binding.scheduler_provider
        scheduler_job_id = binding.scheduler_native_id
        if (provider is None) != (scheduler_job_id is None):
            raise RelayError("JARVIS runtime binding has incomplete scheduler identity")
        expected_provider = provider or "external"
        if session.scheduler != expected_provider or spec.scheduler != expected_provider:
            raise RelayError(
                "scheduler provider disagrees between the gateway, runtime specification, "
                "and JARVIS runtime binding"
            )
        if session.scheduler_job_id != scheduler_job_id:
            raise RelayError(
                "scheduler job identity disagrees between the gateway and JARVIS runtime binding"
            )
        return _DurableSchedulerContract(
            provider=expected_provider,
            scheduler_job_id=scheduler_job_id,
        )

    def unresolved_or_known() -> _DurableSchedulerContract:
        scheduler_job_id = _optional_str(session.scheduler_job_id)
        return _DurableSchedulerContract(
            provider=session.scheduler,
            scheduler_job_id=scheduler_job_id,
            unresolved_submission=scheduler_job_id is None,
        )

    intents = session.gateway.get("ownership_intents")
    if not isinstance(intents, dict):
        if not strict:
            return unresolved_or_known()
        raise RelayError("gateway has no durable scheduler ownership contract")
    typed_intents = cast(dict[str, object], intents)
    scheduler_intent = typed_intents.get("scheduler_submission")
    if not isinstance(scheduler_intent, dict):
        if not strict:
            return unresolved_or_known()
        raise RelayError("gateway has no durable scheduler submission intent")
    typed_scheduler_intent = cast(dict[str, object], scheduler_intent)
    if typed_scheduler_intent.get("schema_version") != _OWNERSHIP_INTENT_SCHEMA:
        if not strict:
            return unresolved_or_known()
        raise RelayError("gateway scheduler submission intent has the wrong schema")
    if session.scheduler != spec.scheduler:
        if not strict:
            return unresolved_or_known()
        raise RelayError(
            "scheduler provider disagrees between the gateway and runtime specification"
        )

    state = typed_scheduler_intent.get("state")
    if state in {"not_started", "absent_verified"}:
        if session.scheduler_job_id is not None:
            if not strict:
                return unresolved_or_known()
            raise RelayError(
                "gateway scheduler job identity contradicts an explicit absence intent"
            )
        return _DurableSchedulerContract(
            provider=session.scheduler,
            scheduler_job_id=None,
        )

    intent_provider = _optional_str(typed_scheduler_intent.get("scheduler_provider"))
    if intent_provider != session.scheduler:
        if not strict:
            return unresolved_or_known()
        raise RelayError("scheduler provider disagrees between the gateway and submission intent")
    if state == "starting":
        if (
            session.scheduler_job_id is not None
            or _optional_str(typed_scheduler_intent.get("submission_id")) is None
            or _optional_str(typed_scheduler_intent.get("submission_marker")) is None
        ):
            if not strict:
                return unresolved_or_known()
            raise RelayError("starting scheduler submission intent has inconsistent identity")
        return _DurableSchedulerContract(
            provider=session.scheduler,
            scheduler_job_id=None,
            unresolved_submission=True,
        )
    if state == "recorded":
        intent_job_id = _optional_str(typed_scheduler_intent.get("scheduler_job_id"))
        if intent_job_id is None or intent_job_id != session.scheduler_job_id:
            if not strict:
                return unresolved_or_known()
            raise RelayError(
                "scheduler job identity disagrees between the gateway and submission intent"
            )
        return _DurableSchedulerContract(
            provider=session.scheduler,
            scheduler_job_id=intent_job_id,
        )
    if not strict:
        return unresolved_or_known()
    raise RelayError("gateway scheduler submission intent has an invalid state")


def _intent_proves_absence(intents: dict[str, object], role: str) -> bool:
    """Return whether a durable intent proves a connector never started or is absent."""
    intent = _object(intents.get(role, {}))
    return intent.get("schema_version") == _OWNERSHIP_INTENT_SCHEMA and intent.get("state") in {
        "not_started",
        "absent_verified",
    }


def _required_intent_str(intent: dict[str, object], field: str) -> str:
    value = _optional_str(intent.get(field))
    if value is None:
        raise RelayError(f"connector ownership intent has no {field}")
    return value


def _validated_gateway_teardown_intent(
    session: GatewaySession,
    *,
    cancel_scheduler_job: bool,
) -> dict[str, object]:
    """Validate the immutable queue-authored teardown operation identity."""
    raw_intent = session.gateway.get("teardown_intent")
    if not isinstance(raw_intent, dict):
        raise RelayError("gateway teardown intent is invalid")
    intent = cast(dict[str, object], raw_intent)
    if set(intent) != {
        "schema_version",
        "operation_id",
        "gateway_session_id",
        "cancel_scheduler_job",
        "created_at",
    }:
        raise RelayError("gateway teardown intent is invalid")
    operation_id = intent.get("operation_id")
    created_at = intent.get("created_at")
    if (
        intent.get("schema_version") != "clio-relay.gateway-teardown-intent.v1"
        or intent.get("gateway_session_id") != session.session_id
        or not isinstance(operation_id, str)
        or not operation_id.startswith("gateway_cleanup_")
        or not isinstance(created_at, str)
        or not isinstance(intent.get("cancel_scheduler_job"), bool)
    ):
        raise RelayError("gateway teardown intent is invalid")
    _gateway_teardown_timestamp(created_at)
    if intent.get("cancel_scheduler_job") is not cancel_scheduler_job:
        raise RelayError(
            "gateway cleanup policy changed during retry; resume with the original "
            f"cancel_scheduler_job={intent.get('cancel_scheduler_job')} policy"
        )
    return intent


def _validated_gateway_detach_intent(session: GatewaySession) -> dict[str, object]:
    """Validate one immutable relay-authored detach operation identity."""
    raw_intent = session.gateway.get("detach_intent")
    if not isinstance(raw_intent, dict):
        raise RelayError("gateway detach intent is invalid")
    intent = cast(dict[str, object], raw_intent)
    if set(intent) != {
        "schema_version",
        "operation_id",
        "gateway_session_id",
        "created_at",
    }:
        raise RelayError("gateway detach intent is invalid")
    operation_id = intent.get("operation_id")
    created_at = intent.get("created_at")
    if (
        intent.get("schema_version") != _GATEWAY_DETACH_INTENT_SCHEMA
        or intent.get("gateway_session_id") != session.session_id
        or not isinstance(operation_id, str)
        or not operation_id.startswith("gateway_detach_")
        or not isinstance(created_at, str)
    ):
        raise RelayError("gateway detach intent is invalid")
    _gateway_teardown_timestamp(created_at)
    return intent


def _validated_completed_resource_lists(
    result: dict[str, object],
    *,
    error: str,
) -> tuple[list[CleanupResource], list[str]]:
    """Strictly parse bounded completed lifecycle resources and errors."""
    raw_resources = result.get("resources")
    raw_errors = result.get("errors")
    if not isinstance(raw_resources, list) or not isinstance(raw_errors, list):
        raise RelayError(error)
    typed_resources = cast(list[object], raw_resources)
    typed_errors = cast(list[object], raw_errors)
    if not 3 <= len(typed_resources) <= 5 or any(
        not isinstance(item, str) or not item for item in typed_errors
    ):
        raise RelayError(error)
    try:
        resources = [
            CleanupResource.model_validate(resource, strict=True) for resource in typed_resources
        ]
    except ValueError as exc:
        raise RelayError(error) from exc
    return resources, cast(list[str], typed_errors)


def _validate_completed_detach_resources(
    session: GatewaySession,
    *,
    resources: list[CleanupResource],
    stopped_local_pid: int | None,
    operation_id: str,
) -> None:
    """Require complete ownership and disposition proof for a finished detach."""
    error = "gateway detach evidence is invalid"
    scheduler_contract = _validated_durable_scheduler_contract(session)
    if scheduler_contract.unresolved_submission:
        raise RelayError(error)
    allowed_kinds = {
        "browser_proxy",
        "desktop_connector",
        "remote_connector",
        "scheduler_job",
        "gateway_record",
    }
    counts = {kind: sum(item.kind == kind for item in resources) for kind in allowed_kinds}
    expected_scheduler_count = 1 if scheduler_contract.scheduler_job_id is not None else 0
    if (
        any(item.kind not in allowed_kinds for item in resources)
        or counts["desktop_connector"] != 1
        or counts["remote_connector"] != 1
        or counts["gateway_record"] != 1
        or counts["browser_proxy"] > 1
        or counts["scheduler_job"] != expected_scheduler_count
        or any(
            not item.resource_id
            or not item.location
            or item.residual
            or item.metadata.get("gateway_session_id") != session.session_id
            or item.metadata.get("cleanup_operation_id") != operation_id
            or item.metadata.get("cancel_scheduler_job") is not False
            for item in resources
        )
    ):
        raise RelayError(error)
    desktop = next(item for item in resources if item.kind == "desktop_connector")
    remote = next(item for item in resources if item.kind == "remote_connector")
    gateway = next(item for item in resources if item.kind == "gateway_record")
    if (
        desktop.action != "stop"
        or desktop.outcome not in {"stopped", "missing"}
        or not desktop.ownership_verified
        or not desktop.verified_after_operation
        or (desktop.outcome == "stopped") != (stopped_local_pid is not None)
        or (stopped_local_pid is not None and desktop.resource_id != str(stopped_local_pid))
        or remote.action != "retain"
        or remote.outcome != "retained"
        or not remote.ownership_verified
        or not remote.verified_after_operation
        or gateway.resource_id != session.session_id
        or gateway.action != "retain"
        or gateway.outcome != "retained"
        or not gateway.ownership_verified
        or not gateway.verified_after_operation
        or gateway.observed_state != GatewaySessionState.DEGRADED.value
    ):
        raise RelayError(error)
    browser = [item for item in resources if item.kind == "browser_proxy"]
    if browser and (
        browser[0].action != "stop"
        or browser[0].outcome not in {"stopped", "missing"}
        or not browser[0].ownership_verified
        or not browser[0].verified_after_operation
    ):
        raise RelayError(error)
    scheduler = [item for item in resources if item.kind == "scheduler_job"]
    if scheduler:
        item = scheduler[0]
        outcome_state_valid = (
            (item.outcome == "retained" and item.observed_state in _ACTIVE_RUNTIME_STATES)
            or (item.outcome == "terminal" and item.observed_state in _TERMINAL_RUNTIME_STATES)
            or (item.outcome == "missing" and item.observed_state == "missing")
        )
        if (
            item.resource_id != scheduler_contract.scheduler_job_id
            or item.provider != scheduler_contract.provider
            or item.action != "retain"
            or not item.ownership_verified
            or not item.verified_after_operation
            or not outcome_state_valid
        ):
            raise RelayError(error)


def _validate_completed_teardown_resources(
    session: GatewaySession,
    *,
    resources: list[CleanupResource],
    stopped_local_pid: int | None,
    stopped_remote_pid: int | None,
    canceled_scheduler_job: str | None,
    operation_id: str,
    cancel_scheduler_job: bool,
) -> None:
    """Require complete ownership and disposition proof for a finished teardown."""
    error = "completed gateway teardown evidence is invalid"
    scheduler_contract = _validated_durable_scheduler_contract(session)
    if scheduler_contract.unresolved_submission:
        raise RelayError(error)
    allowed_kinds = {
        "browser_proxy",
        "desktop_connector",
        "remote_connector",
        "scheduler_job",
        "gateway_record",
    }
    counts = {kind: sum(item.kind == kind for item in resources) for kind in allowed_kinds}
    expected_scheduler_count = 1 if scheduler_contract.scheduler_job_id is not None else 0
    if (
        any(item.kind not in allowed_kinds for item in resources)
        or counts["desktop_connector"] != 1
        or counts["remote_connector"] != 1
        or counts["gateway_record"] != 1
        or counts["browser_proxy"] > 1
        or counts["scheduler_job"] != expected_scheduler_count
        or any(
            not item.resource_id
            or not item.location
            or item.residual
            or item.metadata.get("gateway_session_id") != session.session_id
            or item.metadata.get("cleanup_operation_id") != operation_id
            or item.metadata.get("cancel_scheduler_job") is not cancel_scheduler_job
            for item in resources
        )
    ):
        raise RelayError(error)
    desktop = next(item for item in resources if item.kind == "desktop_connector")
    remote = next(item for item in resources if item.kind == "remote_connector")
    gateway = next(item for item in resources if item.kind == "gateway_record")
    if (
        desktop.action != "stop"
        or desktop.outcome not in {"stopped", "missing"}
        or not desktop.ownership_verified
        or not desktop.verified_after_operation
        or (desktop.outcome == "stopped") != (stopped_local_pid is not None)
        or (stopped_local_pid is not None and desktop.resource_id != str(stopped_local_pid))
        or remote.action != "stop"
        or remote.outcome not in {"stopped", "missing"}
        or not remote.ownership_verified
        or not remote.verified_after_operation
        or gateway.resource_id != session.session_id
        or gateway.action != "close"
        or gateway.outcome != "closed"
        or not gateway.ownership_verified
        or not gateway.verified_after_operation
    ):
        raise RelayError(error)
    remote_connector = _object(
        _object(session.gateway.get("transport", {})).get("remote_connector", {})
    )
    if remote_connector.get("execution_scope") == "scheduler_allocation":
        if stopped_remote_pid is not None or remote.resource_id != _optional_str(
            remote_connector.get("scheduler_step_id")
        ):
            raise RelayError(error)
    elif (remote.outcome == "stopped") != (stopped_remote_pid is not None) or (
        stopped_remote_pid is not None and remote.resource_id != str(stopped_remote_pid)
    ):
        raise RelayError(error)
    browser = [item for item in resources if item.kind == "browser_proxy"]
    if browser and (
        browser[0].action != "stop"
        or browser[0].outcome not in {"stopped", "missing"}
        or not browser[0].ownership_verified
        or not browser[0].verified_after_operation
    ):
        raise RelayError(error)
    scheduler = [item for item in resources if item.kind == "scheduler_job"]
    if not scheduler:
        if canceled_scheduler_job is not None:
            raise RelayError(error)
        return
    item = scheduler[0]
    if (
        item.resource_id != scheduler_contract.scheduler_job_id
        or item.provider != scheduler_contract.provider
        or not item.ownership_verified
        or not item.verified_after_operation
    ):
        raise RelayError(error)
    if cancel_scheduler_job:
        canceled = item.outcome == "canceled" and item.observed_state in (_CANCELED_RUNTIME_STATES)
        naturally_terminal = (
            item.outcome == "terminal"
            and item.observed_state in _TERMINAL_RUNTIME_STATES - _CANCELED_RUNTIME_STATES
        )
        if (
            item.action != "cancel"
            or not (canceled or naturally_terminal)
            or (canceled_scheduler_job is not None) != canceled
            or (canceled and canceled_scheduler_job != item.resource_id)
        ):
            raise RelayError(error)
        return
    retained_state_valid = (
        (item.outcome == "retained" and item.observed_state in _ACTIVE_RUNTIME_STATES)
        or (item.outcome == "terminal" and item.observed_state in _TERMINAL_RUNTIME_STATES)
        or (item.outcome == "missing" and item.observed_state == "missing")
    )
    if item.action != "retain" or not retained_state_valid or canceled_scheduler_job is not None:
        raise RelayError(error)


def _gateway_teardown_timestamp(value: str) -> datetime:
    """Parse one timezone-aware teardown timestamp without accepting naive evidence."""
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise RelayError("gateway teardown timestamp is invalid") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise RelayError("gateway teardown timestamp is invalid")
    return parsed


def _strict_optional_positive_int(value: object) -> int | None:
    """Validate an optional positive process identity in completed teardown evidence."""
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise RelayError("completed gateway teardown evidence is invalid")
    return value


def _strict_optional_nonempty_str(value: object) -> str | None:
    """Validate an optional non-empty identity in completed teardown evidence."""
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise RelayError("completed gateway teardown evidence is invalid")
    return value


def _completed_teardown_metadata_matches(
    session: GatewaySession,
    *,
    operation_id: str,
    cancel_scheduler_job: bool,
    completed_at: str,
    final_state: GatewaySessionState,
    errors: list[str],
) -> bool:
    """Return whether public session metadata agrees exactly with completed evidence."""
    metadata = session.metadata
    expected_closed_at: str | None = (
        completed_at if final_state is GatewaySessionState.CLOSED else None
    )
    return bool(
        metadata.get("cleanup_at") == completed_at
        and metadata.get("closed_at") == expected_closed_at
        and metadata.get("cancel_scheduler_job") is cancel_scheduler_job
        and metadata.get("cleanup_retryable") is False
        and metadata.get("cleanup_errors") == errors
        and metadata.get("cleanup_operation_id") == operation_id
    )


def _completed_detach_metadata_matches(
    session: GatewaySession,
    *,
    operation_id: str,
    completed_at: str,
    errors: list[str],
) -> bool:
    """Return whether public session metadata agrees with completed detach evidence."""
    metadata = session.metadata
    return bool(
        metadata.get("detached_at") == completed_at
        and metadata.get("detach_operation_id") == operation_id
        and metadata.get("detach_retryable") is False
        and metadata.get("detach_errors") == errors
        and metadata.get("cleanup_retryable") is False
        and metadata.get("cleanup_errors") == errors
    )


def _write_local_connector_sidecar(path: Path, connector: dict[str, object]) -> None:
    """Atomically persist exact local process identity next to its connector config."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{secrets.token_hex(8)}.tmp")
    payload = {
        "schema_version": "clio-relay.desktop-connector-sidecar.v1",
        **connector,
    }
    try:
        with temporary.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle)
            handle.flush()
            os.fsync(handle.fileno())
        temporary.chmod(0o600)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _discover_local_connector(
    intent: dict[str, object],
    *,
    session_id: str,
) -> tuple[dict[str, object] | None, bool]:
    """Rediscover one local connector or prove its exact intent has no live process."""
    owner_token = _required_intent_str(intent, "owner_token")
    generation_id = _required_intent_str(intent, "connector_generation_id")
    config_path = _required_intent_str(intent, "config_path")
    metadata_path = Path(_required_intent_str(intent, "metadata_path"))
    sidecar: dict[str, object] | None = None
    try:
        loaded = json.loads(metadata_path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        loaded = None
    except (OSError, json.JSONDecodeError) as exc:
        raise RelayError(f"desktop connector sidecar is unreadable: {exc}") from exc
    if isinstance(loaded, dict):
        candidate = cast(dict[str, object], loaded)
        if (
            candidate.get("schema_version") != "clio-relay.desktop-connector-sidecar.v1"
            or candidate.get("owner") != "clio-relay"
            or candidate.get("session_id") != session_id
            or candidate.get("owner_token") != owner_token
            or candidate.get("connector_generation_id") != generation_id
            or candidate.get("config_path") != config_path
        ):
            raise RelayError("desktop connector sidecar identity does not match its intent")
        sidecar = {key: value for key, value in candidate.items() if key != "schema_version"}
        status, _detail = _local_connector_identity_status(sidecar)
        if status == "owned":
            return sidecar, False

    observed_matches: list[_ObservedLocalProcess] = []
    observation_errors: list[str] = []
    for pid in _local_process_ids(
        command_markers=(owner_token, generation_id, config_path),
    ):
        try:
            observed = _observe_local_process(pid)
        except RelayError as exc:
            observation_errors.append(f"pid {pid}: {exc}")
            continue
        if observed is None:
            continue
        owned, _detail = _observed_connector_matches(
            observed,
            owner_token=owner_token,
            expected_config=config_path,
            expected_process_group_id=observed.pid,
        )
        if not owned:
            continue
        generation_marker = f"CLIO_RELAY_CONNECTOR_GENERATION_ID={generation_id}".encode()
        if observed.environment is not None:
            if generation_marker not in observed.environment.split(bytes([0])):
                continue
        elif generation_id.casefold() not in observed.command_line.casefold():
            continue
        observed_matches.append(observed)
    if observation_errors:
        raise RelayError(
            "desktop connector process observation was incomplete: "
            + "; ".join(observation_errors[:20])
        )
    if len(observed_matches) > 1:
        raise RelayError("multiple local processes matched one connector ownership intent")
    if observed_matches:
        observed = observed_matches[0]
        connector: dict[str, object] = {
            "owner": "clio-relay",
            "session_id": session_id,
            "pid": observed.pid,
            "process_group_id": observed.process_group_id,
            "process_start_marker": observed.process_start_marker,
            "owner_token": owner_token,
            "connector_generation_id": generation_id,
            "config_path": config_path,
            "stdout_path": intent.get("stdout_path"),
            "stderr_path": intent.get("stderr_path"),
        }
        _write_local_connector_sidecar(metadata_path, connector)
        return connector, False
    if sidecar is not None and _local_connector_group_members(sidecar):
        raise RelayError("desktop connector descendants remain but the leader is unresolved")
    return None, True


def _local_process_ids(*, command_markers: tuple[str, ...] = ()) -> list[int]:
    """Enumerate same-owner or marker-matching connector candidate processes."""
    if os.name != "nt":
        try:
            candidates: list[int] = []
            for path in Path("/proc").iterdir():
                if not path.name.isdigit():
                    continue
                try:
                    owner = path.stat().st_uid
                except FileNotFoundError:
                    continue
                except OSError as exc:
                    raise RelayError(
                        f"cannot inspect local process owner {path.name}: {exc}"
                    ) from exc
                if owner == os.geteuid():
                    candidates.append(int(path.name))
            return sorted(candidates)
        except OSError as exc:
            raise RelayError(f"cannot enumerate local processes: {exc}") from exc
    result = _run_bounded_local_cleanup(
        [
            "powershell",
            "-NoProfile",
            "-Command",
            "@(Get-CimInstance Win32_Process | Select-Object ProcessId,CommandLine) "
            "| ConvertTo-Json -Compress",
        ],
    )
    if result.returncode != 0:
        raise RelayError("cannot enumerate local Windows processes")
    try:
        loaded: object = json.loads(result.stdout) if result.stdout.strip() else []
    except json.JSONDecodeError as exc:
        raise RelayError("local Windows process enumeration returned invalid JSON") from exc
    raw_ids = cast(list[object], loaded) if isinstance(loaded, list) else [loaded]
    folded_markers = tuple(marker.casefold() for marker in command_markers)
    process_ids: list[int] = []
    for item in raw_ids:
        if not isinstance(item, dict):
            raise RelayError("local Windows process enumeration returned an invalid record")
        record = cast(dict[str, object], item)
        raw_process_id = record.get("ProcessId")
        command_line = record.get("CommandLine")
        if (
            isinstance(raw_process_id, bool)
            or not isinstance(raw_process_id, int)
            or raw_process_id < 0
        ):
            raise RelayError("local Windows process enumeration returned an invalid process id")
        # Win32_Process includes the System Idle Process as PID 0. It cannot own
        # or be signaled as a connector, while every relay process identity is
        # strictly positive, so omit only this Windows sentinel from discovery.
        if raw_process_id == 0:
            continue
        process_id = raw_process_id
        if folded_markers:
            if not isinstance(command_line, str):
                continue
            folded_command = command_line.casefold()
            if not all(marker in folded_command for marker in folded_markers):
                continue
        process_ids.append(process_id)
    return sorted(process_ids)


def _remote_cleanup_proven(result: dict[str, object]) -> bool:
    """Return whether remote cleanup proved the exact owned group absent."""
    return (
        result.get("outcome") in {"stopped", "missing"}
        and result.get("ownership_verified") is True
        and result.get("verified_after_operation") is True
        and result.get("residual") is False
        and result.get("remaining_pids") == []
    )


def _capture_local_connector_identity(
    *,
    pid: int,
    owner_token: str,
    expected_config: str,
) -> LocalConnectorIdentity:
    deadline = time.time() + 5
    last_detail = "process did not appear"
    while time.time() < deadline:
        observed = _observe_local_process(pid)
        if observed is None:
            time.sleep(0.05)
            continue
        owned, last_detail = _observed_connector_matches(
            observed,
            owner_token=owner_token,
            expected_config=expected_config,
            expected_process_group_id=pid,
        )
        if owned:
            return LocalConnectorIdentity(
                pid=pid,
                process_group_id=observed.process_group_id,
                process_start_marker=observed.process_start_marker,
                owner_token=owner_token,
            )
        time.sleep(0.05)
    raise RelayError(f"desktop connector did not establish owned process identity: {last_detail}")


def _local_connector_identity_status(
    connector: dict[str, object],
) -> tuple[Literal["owned", "missing", "replaced", "unverified"], str | None]:
    pid = _optional_int(connector.get("pid"))
    if pid is None:
        return "missing", "connector record has no process id"
    owner_token = _optional_str(connector.get("owner_token"))
    config_path = _optional_str(connector.get("config_path"))
    process_group_id = _optional_int(connector.get("process_group_id"))
    start_marker = _optional_str(connector.get("process_start_marker"))
    if (
        owner_token is None
        or config_path is None
        or process_group_id is None
        or start_marker is None
    ):
        return "unverified", "connector record lacks token, start, or process-group identity"
    try:
        group_members = _local_connector_group_members(connector)
        observed = _observe_local_process(pid)
    except RelayError as exc:
        return "unverified", str(exc)
    if observed is None:
        if group_members:
            return "owned", "owned connector descendants remain after the group leader exited"
        return "missing", "recorded connector process is no longer running"
    if observed.process_start_marker != start_marker:
        if os.name == "nt":
            return "replaced", "recorded connector PID now belongs to a different process"
        if group_members:
            return "owned", "owned connector group remains after leader PID reuse"
        return "replaced", "recorded connector PID now belongs to a different process"
    owned, detail = _observed_connector_matches(
        observed,
        owner_token=owner_token,
        expected_config=config_path,
        expected_process_group_id=process_group_id,
    )
    return ("owned", None) if owned else ("unverified", detail)


def _local_connector_group_members(connector: dict[str, object]) -> list[int]:
    """Return all live processes carrying the connector's unforgeable identity."""
    pid = _optional_int(connector.get("pid"))
    process_group_id = _optional_int(connector.get("process_group_id"))
    owner_token = _optional_str(connector.get("owner_token"))
    generation_id = _optional_str(connector.get("connector_generation_id"))
    config_path = _optional_str(connector.get("config_path"))
    if (
        pid is None
        or process_group_id is None
        or owner_token is None
        or generation_id is None
        or config_path is None
    ):
        return []
    if os.name == "nt":
        return _windows_connector_descendants(pid=pid, expected_config=config_path)
    token_marker = f"CLIO_RELAY_CONNECTOR_OWNER_TOKEN={owner_token}".encode()
    generation_marker = f"CLIO_RELAY_CONNECTOR_GENERATION_ID={generation_id}".encode()
    matches: list[int] = []
    for proc in Path("/proc").iterdir():
        if not proc.name.isdigit():
            continue
        member_pid = int(proc.name)
        try:
            if proc.stat().st_uid != os.geteuid():
                continue
            fields = (proc / "stat").read_text(encoding="utf-8").rsplit(")", 1)[1].split()
        except (FileNotFoundError, ProcessLookupError):
            continue
        except (OSError, IndexError, ValueError) as exc:
            raise RelayError(
                f"cannot inspect local process group member {member_pid}: {exc}"
            ) from exc
        if fields[0] == "Z":
            continue
        try:
            command_line = (
                (proc / "cmdline")
                .read_bytes()
                .replace(bytes([0]), b" ")
                .decode("utf-8", errors="replace")
            )
        except (FileNotFoundError, ProcessLookupError):
            continue
        except OSError as exc:
            raise RelayError(
                f"cannot inspect local connector group member {member_pid}: {exc}"
            ) from exc
        if "frpc" not in command_line.casefold() or not _command_contains_path(
            command_line,
            config_path,
        ):
            continue
        try:
            environment = (proc / "environ").read_bytes().split(bytes([0]))
        except (FileNotFoundError, ProcessLookupError):
            continue
        except OSError as exc:
            raise RelayError(
                f"cannot verify local connector group member {member_pid}: {exc}"
            ) from exc
        if token_marker in environment and generation_marker in environment:
            matches.append(member_pid)
    return sorted(matches)


def _windows_connector_descendants(*, pid: int, expected_config: str) -> list[int]:
    command = (
        "$items = @(Get-CimInstance Win32_Process | "
        "Select-Object ProcessId,ParentProcessId,CommandLine); "
        "$items | ConvertTo-Json -Compress"
    )
    result = _run_bounded_local_cleanup(
        ["powershell", "-NoProfile", "-Command", command],
    )
    if result.returncode != 0:
        raise RelayError(
            "cannot enumerate local Windows connector descendants: "
            + (result.stderr.strip() or f"exit {result.returncode}")
        )
    try:
        loaded = cast(object, json.loads(result.stdout))
    except json.JSONDecodeError as exc:
        raise RelayError("local Windows connector descendant query returned invalid JSON") from exc
    raw_items = cast(list[object], loaded) if isinstance(loaded, list) else [loaded]
    processes = [cast(dict[str, object], item) for item in raw_items if isinstance(item, dict)]
    descendants = {pid}
    changed = True
    while changed:
        changed = False
        for item in processes:
            child = _optional_int(item.get("ProcessId"))
            parent = _optional_int(item.get("ParentProcessId"))
            if child is not None and parent in descendants and child not in descendants:
                descendants.add(child)
                changed = True
    matches: list[int] = []
    for item in processes:
        child = _optional_int(item.get("ProcessId"))
        command_line = _optional_str(item.get("CommandLine"))
        if (
            child is not None
            and child in descendants
            and command_line is not None
            and "frpc" in command_line.casefold()
            and _command_contains_path(command_line, expected_config)
        ):
            matches.append(child)
    return sorted(matches)


def _observed_connector_matches(
    observed: _ObservedLocalProcess,
    *,
    owner_token: str,
    expected_config: str,
    expected_process_group_id: int,
) -> tuple[bool, str]:
    if observed.process_group_id != expected_process_group_id:
        return False, "connector process-group identity does not match"
    command = observed.command_line.casefold()
    if "frpc" not in command:
        return False, "connector command does not contain frpc"
    if owner_token.casefold() not in command:
        return False, "connector command does not contain its owner token"
    if not _command_contains_path(observed.command_line, expected_config):
        return False, "connector command does not contain its owned config path"
    if observed.environment is not None:
        expected_environment = f"CLIO_RELAY_CONNECTOR_OWNER_TOKEN={owner_token}".encode()
        if expected_environment not in observed.environment.split(bytes([0])):
            return False, "connector environment does not contain its owner token"
    return True, "owned connector identity verified"


def _command_contains_path(command_line: str, expected_path: str) -> bool:
    normalized_command = command_line.replace("\\", "/").casefold()
    candidates = {expected_path}
    with suppress(OSError):
        candidates.add(str(Path(expected_path).resolve()))
    return any(
        candidate.replace("\\", "/").casefold() in normalized_command for candidate in candidates
    )


def _observe_local_process(pid: int) -> _ObservedLocalProcess | None:
    if pid <= 0:
        return None
    if os.name == "nt":
        return _observe_windows_process(pid)
    proc = Path("/proc") / str(pid)
    try:
        stat_fields = (proc / "stat").read_text(encoding="utf-8").rsplit(")", 1)[1].split()
        if stat_fields[0] == "Z":
            return None
        command_line = (
            (proc / "cmdline")
            .read_bytes()
            .replace(bytes([0]), b" ")
            .decode("utf-8", errors="replace")
        )
        environment = (proc / "environ").read_bytes()
        process_group_id = os.getpgid(pid)
    except (FileNotFoundError, ProcessLookupError):
        return None
    except (OSError, IndexError) as exc:
        raise RelayError(f"cannot observe local connector candidate {pid}: {exc}") from exc
    return _ObservedLocalProcess(
        pid=pid,
        process_group_id=process_group_id,
        process_start_marker=stat_fields[19],
        command_line=command_line,
        environment=environment,
    )


def _observe_windows_process(pid: int) -> _ObservedLocalProcess | None:
    command = (
        f"$cim = Get-CimInstance Win32_Process -Filter 'ProcessId = {pid}'; "
        "if ($null -eq $cim) { exit 3 }; "
        f"$process = Get-Process -Id {pid} -ErrorAction Stop; "
        "$value = [pscustomobject]@{"
        "command_line=$cim.CommandLine; "
        "start_marker=$process.StartTime.ToUniversalTime().Ticks.ToString()}; "
        "$value | ConvertTo-Json -Compress"
    )
    result = _run_bounded_local_cleanup(
        ["powershell", "-NoProfile", "-Command", command],
    )
    if result.returncode == 3:
        return None
    if result.returncode != 0:
        raise RelayError(
            f"cannot query local Windows connector candidate {pid}: "
            + (result.stderr.strip() or f"exit {result.returncode}")
        )
    try:
        loaded = cast(object, json.loads(result.stdout))
    except json.JSONDecodeError as exc:
        raise RelayError(f"local Windows connector candidate {pid} returned invalid JSON") from exc
    if not isinstance(loaded, dict):
        raise RelayError(f"local Windows connector candidate {pid} returned an invalid record")
    payload = cast(dict[str, object], loaded)
    command_line = payload.get("command_line")
    start_marker = payload.get("start_marker")
    if not isinstance(command_line, str) or not isinstance(start_marker, str):
        raise RelayError(f"local Windows connector candidate {pid} lacks identity fields")
    return _ObservedLocalProcess(
        pid=pid,
        process_group_id=pid,
        process_start_marker=start_marker,
        command_line=command_line,
        environment=None,
    )


def _signal_owned_posix_connector_processes(
    connector: dict[str, object],
    sig: int,
) -> list[int]:
    """Signal only revalidated connector identities through race-safe pidfds."""
    pidfd_open = getattr(os, "pidfd_open", None)
    pidfd_send_signal = getattr(signal, "pidfd_send_signal", None)
    if not callable(pidfd_open) or not callable(pidfd_send_signal):
        raise RelayError("race-safe pidfd connector cleanup is unavailable on this platform")
    signaled: list[int] = []
    for member_pid in _local_connector_group_members(connector):
        try:
            raw_process_fd = pidfd_open(member_pid, 0)
        except ProcessLookupError:
            continue
        except OSError as exc:
            raise RelayError(f"cannot open connector pidfd for {member_pid}: {exc}") from exc
        if not isinstance(raw_process_fd, int):
            raise RelayError(f"connector pidfd for {member_pid} is not an integer")
        process_fd = raw_process_fd
        try:
            if member_pid not in _local_connector_group_members(connector):
                continue
            try:
                pidfd_send_signal(process_fd, sig, None, 0)
            except ProcessLookupError:
                continue
            except OSError as exc:
                raise RelayError(f"cannot signal owned connector pid {member_pid}: {exc}") from exc
            signaled.append(member_pid)
        finally:
            os.close(process_fd)
    return signaled


def _terminate_local_connector(connector: dict[str, object]) -> int | None:
    pid = _optional_int(connector.get("pid"))
    process_group_id = _optional_int(connector.get("process_group_id"))
    if pid is None or process_group_id is None:
        return None
    if _local_connector_identity_status(connector)[0] != "owned":
        return None
    if os.name == "nt":
        result = _run_bounded_local_cleanup(["taskkill", "/PID", str(pid), "/T", "/F"])
        if result.returncode not in {0, 128}:
            return None
    else:
        _signal_owned_posix_connector_processes(connector, signal.SIGTERM)
        deadline = time.time() + 5
        while time.time() < deadline:
            if not _local_connector_group_members(connector):
                return pid
            time.sleep(0.2)
        _signal_owned_posix_connector_processes(connector, signal.SIGKILL)
    deadline = time.time() + 5
    while time.time() < deadline:
        if not _local_connector_group_members(connector):
            return pid
        time.sleep(0.2)
    return None


def _terminate_just_started_process_group(pid: int) -> None:
    """Best-effort rollback for a process whose durable identity capture failed."""
    if os.name == "nt":
        with suppress(subprocess.TimeoutExpired):
            subprocess.run(
                ["taskkill", "/PID", str(pid), "/T", "/F"],
                capture_output=True,
                text=True,
                check=False,
                timeout=_LOCAL_CLEANUP_COMMAND_TIMEOUT_SECONDS,
            )
        return
    with suppress(ProcessLookupError):
        os.killpg(pid, signal.SIGTERM)
    time.sleep(0.1)
    with suppress(ProcessLookupError):
        os.killpg(pid, signal.SIGKILL)


def _run_bounded_local_cleanup(command: Sequence[str]) -> subprocess.CompletedProcess[str]:
    """Run one local ownership/cleanup command with a strict wall-clock bound."""
    try:
        return subprocess.run(
            list(command),
            capture_output=True,
            text=True,
            check=False,
            timeout=_LOCAL_CLEANUP_COMMAND_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired as exc:
        raise RelayError(
            "local cleanup command timed out after "
            f"{_LOCAL_CLEANUP_COMMAND_TIMEOUT_SECONDS:g} seconds: {command[0]}"
        ) from exc


def _object(value: object) -> dict[str, object]:
    return cast(dict[str, object], value) if isinstance(value, dict) else {}


def _bind_cleanup_resource_to_gateway(
    resource: CleanupResource,
    gateway_session_id: str,
) -> CleanupResource:
    """Bind connector cleanup evidence to its exact durable gateway record."""
    return resource.model_copy(
        update={
            "metadata": {
                **resource.metadata,
                "gateway_session_id": gateway_session_id,
            }
        }
    )


def _optional_int(value: object) -> int | None:
    return value if isinstance(value, int) else None


def _optional_str(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _require_server_addr(server_addr: str, cluster: str) -> str:
    if server_addr.strip():
        return server_addr
    raise ConfigurationError(f"frp server address is not configured for cluster {cluster}")


def _frp_proxy_type(transport_mode: str) -> str:
    normalized = transport_mode.strip().lower().replace("_", "-")
    if normalized in {"frp-stcp", "frp-stcp-wss", "stcp", "relay"}:
        return "stcp"
    if normalized in {"frp-xtcp", "frp-xtcp-wss", "xtcp", "direct", "nat-bypass"}:
        return "xtcp"
    raise ConfigurationError(f"unsupported service runtime transport mode: {transport_mode}")

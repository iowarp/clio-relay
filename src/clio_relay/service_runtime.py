"""Generic supervisor for scheduler-backed streaming service sessions."""

from __future__ import annotations

import hashlib
import json
import math
import os
import secrets
import subprocess
import sys
import time
from collections.abc import Callable, Generator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Literal, cast

import httpx
from filelock import FileLock
from filelock import Timeout as FileLockTimeout

from clio_relay import service_runtime_command_runner as _command_runner
from clio_relay import service_runtime_connector_identity as _connector_identity
from clio_relay import service_runtime_connector_step_scripts as _connector_step_scripts
from clio_relay import service_runtime_primitives as _primitives
from clio_relay import service_runtime_readiness as _readiness
from clio_relay import service_runtime_scheduler_contracts as _scheduler_contracts
from clio_relay import service_runtime_submission_scripts as _submission_scripts
from clio_relay import service_runtime_types as _types
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
from clio_relay.errors import (
    BrowserAttachmentIdentityConflictError,
    ConfigurationError,
    NotFoundError,
    QueueConflictError,
    RelayError,
)
from clio_relay.filesystem_paths import internal_filesystem_path
from clio_relay.frp_link import FrpLinkConfig, render_proxy_config, start_owned_frp_visitor
from clio_relay.frp_remote_scripts import (
    remote_allocation_frpc_start_script as _remote_allocation_frpc_start_script,
)
from clio_relay.frp_remote_scripts import (
    remote_frpc_start_script as _remote_frpc_start_script,
)
from clio_relay.frp_remote_scripts import (
    remote_stop_script as _remote_stop_script,
)
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
from clio_relay.relay_host import FrpTransportProtocol
from clio_relay.scheduler_providers import (
    SchedulerAllocationConnectorProvider,
    provider_for_scheduler,
)
from clio_relay.session_wire_models import CleanupResource

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
_MAX_LOCAL_HEALTH_BYTES = 64 * 1024
_GATEWAY_TEARDOWN_LOCK_TIMEOUT_SECONDS = 60.0
_GATEWAY_DETACH_RESULT_SCHEMA = "clio-relay.gateway-detach-result.v1"
_GATEWAY_TEARDOWN_POLICY_SCHEMA = "clio-relay.gateway-teardown-policy.v1"
_GATEWAY_TEARDOWN_RESULT_SCHEMA = "clio-relay.gateway-teardown-result.v1"
_JARVIS_BIND_IDENTITY_SCHEMA = "clio-relay.jarvis-bind-identity.v1"
_JARVIS_BIND_POLICY_SCHEMA = "clio-relay.jarvis-bind-policy.v1"
_REMOTE_RUNTIME_COMMAND_TIMEOUT_SECONDS = 120.0
_CONNECTOR_STEP_CLEANUP_TIMEOUT_SECONDS = 30.0
_CONNECTOR_STEP_CLEANUP_POLL_SECONDS = 0.25
_RUNTIME_HEALTH_OBSERVATION_TIMEOUT_SECONDS = 5.0


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
        transport = _primitives._object(self.session.gateway.get("transport", {}))
        for connector_role in ("remote_connector", "desktop_connector"):
            connector = _primitives._object(transport.get(connector_role, {}))
            pid = _primitives._optional_int(connector.get("pid"))
            scheduler_step_id = _primitives._optional_str(connector.get("scheduler_step_id"))
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
class ServiceRuntimePendingResult:
    """Durable, resumable outcome for a submitted runtime not ready yet."""

    session: GatewaySession
    outcome: Literal["pending"] = "pending"
    scheduler_action: Literal["none"] = "none"
    relay_action: Literal["none"] = "none"

    def retry_selector(self) -> dict[str, object]:
        """Return the exact selector required to advance this submission in place."""
        scheduler_job_id = self.session.scheduler_job_id
        selector: dict[str, object] = {
            "cluster": self.session.cluster,
            "gateway_session_id": self.session.session_id,
            "scheduler_provider": self.session.scheduler,
            "scheduler_job_id": scheduler_job_id,
        }
        binding = _primitives._object(self.session.gateway.get("jarvis_runtime_binding", {}))
        if binding:
            required = {
                "source_relay_job_id",
                "source_relay_artifact_id",
                "package_id",
                "package_name",
                "service_instance_id",
            }
            if not required.issubset(binding):
                raise RelayError("pending JARVIS runtime omitted its durable binding identity")
            selector.update(
                {
                    "resume_tool": "relay_bind_jarvis_runtime",
                    "binding": {
                        "cluster": self.session.cluster,
                        "source_job_id": binding["source_relay_job_id"],
                        "source_artifact_id": binding["source_relay_artifact_id"],
                        "package_id": binding["package_id"],
                        "package_name": binding["package_name"],
                        "service_instance_id": binding["service_instance_id"],
                    },
                    "name": self.session.name,
                }
            )
            return selector
        if scheduler_job_id is not None:
            return selector
        intents = _primitives._object(self.session.gateway.get("ownership_intents", {}))
        intent = _primitives._object(intents.get("scheduler_submission", {}))
        submission_id = _primitives._optional_str(intent.get("submission_id"))
        submission_marker = _primitives._optional_str(intent.get("submission_marker"))
        if (
            intent.get("schema_version") != _primitives._OWNERSHIP_INTENT_SCHEMA
            or intent.get("state") != "starting"
            or intent.get("scheduler_provider") != self.session.scheduler
            or submission_id is None
            or submission_marker is None
        ):
            raise RelayError("pending runtime omitted its durable submission identity")
        selector.update(
            {
                "submission_id": submission_id,
                "submission_marker": submission_marker,
            }
        )
        return selector

    def to_live_validation_report(
        self,
        *,
        launcher: str | None = None,
        install_source: str | None = None,
        artifact_sha256: str | None = None,
    ) -> LiveValidationReport:
        """Record an honest nonterminal observation that cannot satisfy a release gate."""
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
        observed_at = utc_now()
        selector = self.retry_selector()
        scheduler_job_id = self.session.scheduler_job_id
        jarvis_binding = _primitives._object(self.session.gateway.get("jarvis_runtime_binding", {}))
        unresolved_submission = scheduler_job_id is None and not jarvis_binding
        summary = (
            "exact scheduler submission intent is durable; submission outcome is unresolved"
            if unresolved_submission
            else (
                "verified JARVIS runtime binding is durable and its local gateway is not ready yet"
                if jarvis_binding
                else "scheduler-backed gateway is durably submitted and not ready yet"
            )
        )
        report.checks.append(
            ValidationCheck(
                check_id=RUNTIME_ALLOCATED_CHECK_ID,
                summary=summary,
                status=ValidationStatus.PENDING,
                started_at=report.started_at,
                completed_at=observed_at,
                evidence=[
                    EvidenceReference(
                        kind="gateway_resume_selector",
                        excerpt=json.dumps(selector, sort_keys=True),
                        metadata={
                            **selector,
                            "scheduler_action": self.scheduler_action,
                            "relay_action": self.relay_action,
                        },
                    )
                ],
            )
        )
        report.resources.append(
            ValidationResource(
                kind="gateway_session",
                resource_id=self.session.session_id,
                cluster=self.session.cluster,
                state=self.session.state.value,
                metadata={"retry_selector": selector},
            )
        )
        if jarvis_binding:
            report.resources.append(
                ValidationResource(
                    kind="jarvis_service_runtime",
                    resource_id=cast(str, jarvis_binding["service_instance_id"]),
                    cluster=self.session.cluster,
                    provider=self.session.scheduler,
                    state=self.session.queue_state,
                    metadata={
                        "gateway_session_id": self.session.session_id,
                        "jarvis_execution_id": jarvis_binding.get("jarvis_execution_id"),
                        "scheduler_job_id": scheduler_job_id,
                        "cancel_requested": False,
                        "resubmit_requested": False,
                    },
                )
            )
            if scheduler_job_id is not None:
                report.resources.append(
                    ValidationResource(
                        kind="scheduler_job",
                        resource_id=scheduler_job_id,
                        cluster=self.session.cluster,
                        provider=self.session.scheduler,
                        state=self.session.queue_state,
                        metadata={
                            "gateway_session_id": self.session.session_id,
                            "retained": True,
                            "cancel_requested": False,
                            "resubmit_requested": False,
                        },
                    )
                )
        else:
            report.resources.append(
                ValidationResource(
                    kind=("scheduler_job" if not unresolved_submission else "scheduler_submission"),
                    resource_id=(scheduler_job_id or cast(str, selector["submission_id"])),
                    cluster=self.session.cluster,
                    provider=self.session.scheduler,
                    state=(
                        self.session.queue_state if not unresolved_submission else "intent_recorded"
                    ),
                    metadata=(
                        {
                            "gateway_session_id": self.session.session_id,
                            "retained": True,
                            "scheduler_job_id": scheduler_job_id,
                            "cancel_requested": False,
                            "resubmit_requested": False,
                        }
                        if not unresolved_submission
                        else {
                            "gateway_session_id": self.session.session_id,
                            "scheduler_job_id": None,
                            "submission_id": selector["submission_id"],
                            "submission_marker": selector["submission_marker"],
                            "submission_outcome": "unresolved",
                            "cancel_requested": False,
                            "resubmit_requested": False,
                        }
                    ),
                )
            )
        report.completed_at = observed_at
        report.status = ValidationStatus.PENDING
        report.error = None
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
        operation_intent = _primitives._object(self.session.gateway.get(operation_intent_name, {}))
        raw_cancel_scheduler_jobs: object = (
            False if self.mode == "detach" else operation_intent.get("cancel_scheduler_job")
        )
        if not isinstance(raw_cancel_scheduler_jobs, bool):
            raise RelayError("gateway cleanup operation policy is invalid")
        return CleanupEvidence(
            requested=True,
            mode=self.mode,
            operation_id=_primitives._optional_str(operation_intent.get("operation_id")),
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
        scheduler_submission_resources = [
            resource for resource in self.resources if resource.kind == "scheduler_submission"
        ]
        gateway_resources = [
            resource for resource in self.resources if resource.kind == "gateway_record"
        ]
        cancellation_requested = any(
            resource.action == "cancel" for resource in scheduler_resources
        )
        unresolved_submission = bool(
            self.session.scheduler_job_id is None
            and self.session.gateway.get("jarvis_runtime_binding") is None
            and _scheduler_contracts._validated_durable_scheduler_contract(
                self.session
            ).unresolved_submission
        )
        scheduler_identity_exact = bool(
            not scheduler_resources
            if self.session.scheduler_job_id is None
            else len(scheduler_resources) == 1
            and scheduler_resources[0].resource_id == self.session.scheduler_job_id
            and scheduler_resources[0].provider == self.session.scheduler
        )
        if unresolved_submission:
            scheduler_intent = _primitives._object(
                _primitives._object(self.session.gateway.get("ownership_intents", {})).get(
                    "scheduler_submission",
                    {},
                )
            )
            scheduler_submission_exact = (
                not scheduler_resources
                and len(scheduler_submission_resources) == 1
                and scheduler_submission_resources[0].resource_id
                == scheduler_intent.get("submission_id")
                and scheduler_submission_resources[0].provider == self.session.scheduler
                and scheduler_submission_resources[0].action == "retain"
                and scheduler_submission_resources[0].outcome == "retained"
                and scheduler_submission_resources[0].observed_state == "intent_recorded"
                and scheduler_submission_resources[0].ownership_verified
                and scheduler_submission_resources[0].verified_after_operation
                and not scheduler_submission_resources[0].residual
                and scheduler_submission_resources[0].metadata.get("submission_marker")
                == scheduler_intent.get("submission_marker")
                and scheduler_submission_resources[0].metadata.get("scheduler_job_id") is None
                and scheduler_submission_resources[0].metadata.get("cancel_requested") is False
                and scheduler_submission_resources[0].metadata.get("resubmit_requested") is False
            )
            scheduler_check = (
                RUNTIME_SCHEDULER_RETAINED_CHECK_ID,
                "exact scheduler submission intent retained; no job, cancellation, or "
                "resubmission is claimed",
                scheduler_submission_exact,
            )
        elif cancellation_requested:
            scheduler_check = (
                RUNTIME_SCHEDULER_CANCELED_CHECK_ID,
                "scheduler cancellation reached an observed canceled state",
                scheduler_identity_exact
                and all(
                    resource.action == "cancel"
                    and resource.outcome == "canceled"
                    and resource.ownership_verified
                    and resource.verified_after_operation
                    and resource.observed_state in _scheduler_contracts._CANCELED_RUNTIME_STATES
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
                            resource.observed_state in _scheduler_contracts._ACTIVE_RUNTIME_STATES
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
            no_connector_side_effects = (
                len(desktop_connectors) == 1
                and desktop_connectors[0].action == "stop"
                and desktop_connectors[0].outcome == "missing"
                and desktop_connectors[0].ownership_verified
                and desktop_connectors[0].verified_after_operation
                and not desktop_connectors[0].residual
                and len(remote_connectors) == 1
                and remote_connectors[0].action == "retain"
                and remote_connectors[0].outcome == "missing"
                and remote_connectors[0].observed_state == "not_created"
                and remote_connectors[0].ownership_verified
                and remote_connectors[0].verified_after_operation
                and not remote_connectors[0].residual
            )
            check_values = [
                (
                    RUNTIME_DETACH_CHECK_ID,
                    (
                        "connector intents prove no connector side effects were created"
                        if no_connector_side_effects
                        else "desktop connector stopped and remote connector retained"
                    ),
                    no_connector_side_effects
                    if no_connector_side_effects
                    else desktop_stopped and remote_retained,
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
        runner: _types.CommandRunner | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.settings = settings
        self.queue = queue
        self.cluster = cluster
        self.definition = definition
        self.token = token
        self.secret_key = secret_key
        self.runner = runner or _command_runner.SubprocessCommandRunner()
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
    ) -> ServiceRuntimeStartResult | ServiceRuntimePendingResult:
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
                        role: _scheduler_contracts._new_ownership_intent("not_started")
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
        completion_started = False
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
                _scheduler_contracts._new_ownership_intent(
                    "starting",
                    submission_id=submission_id,
                    scheduler_provider=spec.scheduler,
                    submission_marker=submission_marker,
                ),
            )
            try:
                submit_output = self._ssh(
                    _submission_scripts._submit_script(
                        submit_command,
                        session_id=session.session_id,
                        submission_id=submission_id,
                        scheduler_provider=spec.scheduler,
                        submission_marker=submission_marker,
                    )
                )
            except _types._AmbiguousRemoteSideEffectError as exc:
                pending = self._record_runtime_observation_pending(
                    self.queue.get_gateway_session(session.session_id),
                    node=None,
                    error=exc,
                    provider_status=None,
                    state=GatewaySessionState.PENDING,
                )
                return ServiceRuntimePendingResult(session=pending)
            submission = _scheduler_contracts._parse_runtime_submission(submit_output)
            scheduler_job_id = submission.scheduler_job_id
            session = self._update(
                session,
                scheduler_job_id=scheduler_job_id,
                queue_state="submitted",
                gateway=self._gateway_with_ownership_intent(
                    session,
                    "scheduler_submission",
                    _scheduler_contracts._new_ownership_intent(
                        "recorded",
                        submission_id=submission_id,
                        scheduler_provider=spec.scheduler,
                        submission_marker=submission_marker,
                        scheduler_job_id=scheduler_job_id,
                    ),
                    submit_output=submit_output.strip(),
                ),
            )
            node = self._observe_allocation_and_health_once(
                session,
                spec,
                scheduler_job_id,
                initial_service_host=submission.service_host,
            )
            if node is None:
                return ServiceRuntimePendingResult(
                    session=self.queue.get_gateway_session(session.session_id)
                )
            completion_started = True
            return self._complete_runtime_start_locked(
                session_id=session.session_id,
                spec=spec,
                node=node,
            )
        except Exception as exc:
            if not completion_started:
                self._rollback_runtime_start(
                    session_id=session.session_id,
                    error=exc,
                    remote_connector=None,
                    local_connector=None,
                )
            raise
        finally:
            transition_lock.release()

    def resume_start(
        self,
        *,
        session_id: str,
    ) -> ServiceRuntimeStartResult | ServiceRuntimePendingResult:
        """Advance one exact durable runtime submission without resubmitting it."""
        self.queue.initialize()
        with self._gateway_transition_lock(session_id):
            return self._resume_start_locked(session_id=session_id)

    def _resume_start_locked(
        self,
        *,
        session_id: str,
    ) -> ServiceRuntimeStartResult | ServiceRuntimePendingResult:
        """Advance one durable start while the caller holds its transition lock."""
        session = self.queue.get_gateway_session(session_id)
        self._validate_gateway_transition_session(session)
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
            spec = self._validate_jarvis_binding_session(
                session=session,
                verified=verified_runtime,
            )
            if session.gateway.get("teardown_intent") is not None:
                raise ConfigurationError(
                    f"gateway session {session_id} is committed to teardown and cannot resume"
                )
            if session.state is GatewaySessionState.READY:
                return self._ready_start_result(session)
            authorization = self._jarvis_runtime_authorization(verified_runtime)
            return self._resume_jarvis_binding_locked(
                session_id=session_id,
                verified=verified_runtime,
                authorization=authorization,
                readiness_timeout_seconds=spec.readiness_timeout_seconds,
                poll_seconds=spec.poll_seconds,
            )
        if session.state is GatewaySessionState.READY:
            return self._ready_start_result(session)
        if session.gateway.get("teardown_intent") is not None:
            raise ConfigurationError(
                f"gateway session {session_id} is committed to teardown and cannot resume"
            )
        session = self._reconcile_ownership_intents(session)
        if session.state is GatewaySessionState.FAILED:
            scheduler_intent = _primitives._object(
                _primitives._object(session.gateway.get("ownership_intents", {})).get(
                    "scheduler_submission",
                    {},
                )
            )
            if scheduler_intent.get("reconciliation_outcome") == "definitive_failure":
                raise RelayError(
                    _primitives._optional_str(scheduler_intent.get("reconciliation_error"))
                    or "scheduler submission reconciliation failed definitively"
                )
        if session.state is GatewaySessionState.DEGRADED:
            if not self._detached_pending_submission_can_resume(session):
                raise ConfigurationError(
                    f"gateway session {session_id} cannot resume start from {session.state.value}"
                )
            completed_detach = self._completed_detach_result(session)
            if completed_detach is None:
                raise ConfigurationError(
                    f"gateway session {session_id} has an incomplete detach; retry detach or "
                    "tear down the runtime"
                )
            session = self._consume_completed_detach_for_attach(session)
            session = self._update(
                session,
                state=(
                    GatewaySessionState.ALLOCATED
                    if session.node is not None
                    else GatewaySessionState.PENDING
                ),
                metadata={
                    "cleanup_retryable": None,
                    "cleanup_errors": [],
                },
            )
        if session.state not in {
            GatewaySessionState.SUBMITTED,
            GatewaySessionState.PENDING,
            GatewaySessionState.ALLOCATED,
            GatewaySessionState.STARTING,
        }:
            raise ConfigurationError(
                f"gateway session {session_id} cannot resume start from {session.state.value}"
            )
        unresolved_connector = self._first_unresolved_connector_role(session)
        if unresolved_connector is not None:
            return self._connector_recovery_pending(
                session,
                role=unresolved_connector,
            )
        try:
            submission = self._verified_scheduler_submission(session)
        except RelayError as exc:
            if (
                session.scheduler_job_id is None
                and not self._scheduler_submission_reconciliation_is_pending(session)
            ):
                raise
            pending_session = self._record_runtime_observation_pending(
                session,
                node=session.node,
                error=exc,
                provider_status=None,
            )
            return ServiceRuntimePendingResult(session=pending_session)
        try:
            node = self._observe_allocation_and_health_once(
                session,
                submission.spec,
                submission.scheduler_job_id,
                initial_service_host=session.node,
            )
        except _types._DefinitiveRuntimeObservationError as exc:
            self._rollback_runtime_start(
                session_id=session_id,
                error=exc,
                remote_connector=None,
                local_connector=None,
            )
            raise
        if node is None:
            return ServiceRuntimePendingResult(session=self.queue.get_gateway_session(session_id))
        return self._complete_runtime_start_locked(
            session_id=session_id,
            spec=submission.spec,
            node=node,
        )

    def _complete_runtime_start_locked(
        self,
        *,
        session_id: str,
        spec: ServiceRuntimeSpec,
        node: str,
    ) -> ServiceRuntimeStartResult | ServiceRuntimePendingResult:
        """Create connectors and publish readiness while holding the session transition lock."""
        session = self._reconcile_ownership_intents(self.queue.get_gateway_session(session_id))
        remote_connector: dict[str, object] | None = None
        local_connector: dict[str, object] | None = None
        try:
            proxy_name = spec.proxy_name or f"{session.session_id}-service"
            session = self._update(
                session,
                state=GatewaySessionState.STARTING,
                queue_state="running",
                node=node,
            )
            transport = _primitives._object(session.gateway.get("transport", {}))
            recovered_remote = _primitives._object(transport.get("remote_connector", {}))
            if recovered_remote:
                if not self._connector_reuse_is_verified(
                    session,
                    role="remote_connector",
                ):
                    return self._connector_recovery_pending(
                        session,
                        role="remote_connector",
                    )
                remote_connector = recovered_remote
            else:
                if not self._connector_launch_is_authorized(
                    session,
                    role="remote_connector",
                ):
                    return self._connector_recovery_pending(
                        session,
                        role="remote_connector",
                    )
                remote_intent = _scheduler_contracts._new_ownership_intent(
                    "starting",
                    owner_token=secrets.token_hex(32),
                    connector_generation_id=secrets.token_hex(16),
                )
                session = self._set_ownership_intent(
                    session,
                    "remote_connector",
                    remote_intent,
                )
                try:
                    remote_connector = self._start_remote_connector(
                        session=session,
                        spec=spec,
                        node=node,
                        proxy_name=proxy_name,
                        ownership_intent=remote_intent,
                    )
                except _types._AmbiguousRemoteSideEffectError as exc:
                    latest = self.queue.get_gateway_session(session.session_id)
                    pending = self._record_runtime_observation_pending(
                        latest,
                        node=node,
                        error=exc,
                        provider_status=None,
                        state=GatewaySessionState.STARTING,
                        queue_state=latest.queue_state or "running",
                        preserve_scheduler_status=True,
                    )
                    return ServiceRuntimePendingResult(session=pending)
                session = self.queue.get_gateway_session(session.session_id)
                session = self._update(
                    session,
                    gateway=self._gateway_with_ownership_intent(
                        session,
                        "remote_connector",
                        _scheduler_contracts._new_ownership_intent("recorded", **remote_connector),
                        transport={
                            **_primitives._object(session.gateway.get("transport", {})),
                            "proxy_name": proxy_name,
                            "remote_connector": remote_connector,
                        },
                    ),
                )
            transport = _primitives._object(session.gateway.get("transport", {}))
            recovered_local = _primitives._object(transport.get("desktop_connector", {}))
            if recovered_local:
                if not self._connector_reuse_is_verified(
                    session,
                    role="desktop_connector",
                ):
                    return self._connector_recovery_pending(
                        session,
                        role="desktop_connector",
                    )
                local_connector = recovered_local
            else:
                if not self._connector_launch_is_authorized(
                    session,
                    role="desktop_connector",
                ):
                    return self._connector_recovery_pending(
                        session,
                        role="desktop_connector",
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
                        _scheduler_contracts._new_ownership_intent("recorded", **local_connector),
                        transport={
                            **_primitives._object(session.gateway.get("transport", {})),
                            "proxy_name": proxy_name,
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
            try:
                self._wait_for_local_health(
                    health_url,
                    min(
                        spec.readiness_timeout_seconds,
                        _RUNTIME_HEALTH_OBSERVATION_TIMEOUT_SECONDS,
                    ),
                    spec.poll_seconds,
                    expected_body=spec.health_expected_body,
                    max_attempts=1,
                )
            except RelayError as exc:
                pending_session = self._record_runtime_observation_pending(
                    session,
                    node=node,
                    error=exc,
                    provider_status=None,
                    state=GatewaySessionState.STARTING,
                    queue_state=session.queue_state or "running",
                    preserve_scheduler_status=True,
                )
                return ServiceRuntimePendingResult(session=pending_session)
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
            self._rollback_runtime_start(
                session_id=session_id,
                error=exc,
                remote_connector=remote_connector,
                local_connector=local_connector,
            )
            raise

    def _ready_start_result(self, session: GatewaySession) -> ServiceRuntimeStartResult:
        """Rehydrate an idempotent ready result from one exact durable gateway record."""
        gateway = session.gateway
        connect_url = _primitives._optional_str(gateway.get("connect_url"))
        health_url = _primitives._optional_str(gateway.get("health_url"))
        if connect_url is None or health_url is None:
            raise RelayError("ready gateway session omitted its durable connection URLs")
        compatibility_raw = gateway.get("compatibility_urls")
        compatibility_urls = (
            {
                key: value
                for key, value in cast(dict[object, object], compatibility_raw).items()
                if isinstance(key, str) and isinstance(value, str)
            }
            if isinstance(compatibility_raw, dict)
            else {}
        )
        return ServiceRuntimeStartResult(
            session=session,
            connect_url=connect_url,
            health_url=health_url,
            stream_url=_primitives._optional_str(gateway.get("stream_url")),
            compatibility_urls=compatibility_urls,
            events_url=_primitives._optional_str(gateway.get("events_url")),
            state_url=_primitives._optional_str(gateway.get("state_url")),
            command_url=_primitives._optional_str(gateway.get("command_url")),
        )

    @staticmethod
    def _first_unresolved_connector_role(session: GatewaySession) -> str | None:
        """Return one connector whose exact durable identity remains ambiguous."""
        intents = _primitives._object(session.gateway.get("ownership_intents", {}))
        transport = _primitives._object(session.gateway.get("transport", {}))
        for role in ("remote_connector", "desktop_connector"):
            intent = _primitives._object(intents.get(role, {}))
            record = _primitives._object(transport.get(role, {}))
            if intent.get("reconciliation_error") is not None:
                return role
            if intent.get("state") in {"starting", "recorded"} and (
                not record or intent.get("live_identity_verified") is not True
            ):
                return role
            if record and intent.get("state") != "recorded":
                return role
        return None

    @staticmethod
    def _scheduler_submission_reconciliation_is_pending(session: GatewaySession) -> bool:
        """Return whether one exact pre-submit identity still awaits sidecar publication."""
        intents = _primitives._object(session.gateway.get("ownership_intents", {}))
        intent = _primitives._object(intents.get("scheduler_submission", {}))
        return bool(
            session.scheduler_job_id is None
            and intent.get("schema_version") == _primitives._OWNERSHIP_INTENT_SCHEMA
            and intent.get("state") == "starting"
            and _primitives._optional_str(intent.get("submission_id")) is not None
            and _primitives._optional_str(intent.get("submission_marker")) is not None
            and intent.get("scheduler_provider") == session.scheduler
        )

    @staticmethod
    def _connector_launch_is_authorized(
        session: GatewaySession,
        *,
        role: str,
    ) -> bool:
        """Allow a new generation only after durable non-start or exact absence proof."""
        intents = _primitives._object(session.gateway.get("ownership_intents", {}))
        intent = _primitives._object(intents.get(role, {}))
        transport = _primitives._object(session.gateway.get("transport", {}))
        return bool(
            intent.get("schema_version") == _primitives._OWNERSHIP_INTENT_SCHEMA
            and intent.get("state") in {"not_started", "absent_verified"}
            and not _primitives._object(transport.get(role, {}))
            and intent.get("reconciliation_error") is None
        )

    @staticmethod
    def _connector_reuse_is_verified(
        session: GatewaySession,
        *,
        role: str,
    ) -> bool:
        """Require fresh live reconciliation before adopting a durable connector record."""
        intents = _primitives._object(session.gateway.get("ownership_intents", {}))
        intent = _primitives._object(intents.get(role, {}))
        transport = _primitives._object(session.gateway.get("transport", {}))
        return bool(
            _primitives._object(transport.get(role, {}))
            and intent.get("schema_version") == _primitives._OWNERSHIP_INTENT_SCHEMA
            and intent.get("state") == "recorded"
            and intent.get("live_identity_verified") is True
            and intent.get("reconciliation_error") is None
        )

    def _connector_recovery_pending(
        self,
        session: GatewaySession,
        *,
        role: str,
    ) -> ServiceRuntimePendingResult:
        """Persist an ambiguous connector identity as resumable, without replacement."""
        intents = _primitives._object(session.gateway.get("ownership_intents", {}))
        intent = _primitives._object(intents.get(role, {}))
        detail = _primitives._optional_str(intent.get("reconciliation_error"))
        error = RelayError(
            detail or f"{role.replace('_', ' ')} identity has not been proven live for this intent"
        )
        pending = self._record_runtime_observation_pending(
            session,
            node=session.node,
            error=error,
            provider_status=None,
            state=GatewaySessionState.STARTING,
            queue_state=session.queue_state or "running",
            preserve_scheduler_status=True,
        )
        return ServiceRuntimePendingResult(session=pending)

    def _rollback_runtime_start(
        self,
        *,
        session_id: str,
        error: BaseException,
        remote_connector: dict[str, object] | None,
        local_connector: dict[str, object] | None,
    ) -> None:
        """Roll back owned connectors while retaining the submitted scheduler job."""
        cleanup_errors: list[str] = []
        if remote_connector is None:
            try:
                recovered = self._reconcile_ownership_intents(
                    self.queue.get_gateway_session(session_id)
                )
                recovered_remote = _primitives._object(
                    _primitives._object(recovered.gateway.get("transport", {})).get(
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
                session_id=session_id,
                connector=local_connector,
                require_record=True,
            )
            if local_rollback.residual or not local_rollback.verified_after_operation:
                cleanup_errors.append(
                    local_rollback.detail or "desktop connector rollback was not proven"
                )
        if remote_connector is not None:
            remote_pid = _primitives._optional_int(remote_connector.get("pid"))
            if remote_pid is None:
                cleanup_errors.append("remote connector rollback has no recorded pid")
            else:
                try:
                    remote_result = _scheduler_contracts._last_json_object(
                        self._ssh(
                            _remote_stop_script(
                                session_id=session_id,
                                pid=remote_pid,
                            )
                        )
                    )
                    if not _connector_identity._remote_cleanup_proven(remote_result):
                        cleanup_errors.append(
                            "remote connector rollback did not prove full process-group absence"
                        )
                except RelayError as rollback_exc:
                    cleanup_errors.append(str(rollback_exc))
        try:
            stop_result = self._stop_serialized(
                session_id=session_id,
                cancel_scheduler_job=False,
                final_state=GatewaySessionState.FAILED,
            )
            cleanup_errors.extend(stop_result.errors)
        except Exception as cleanup_exc:
            cleanup_errors.append(str(cleanup_exc))
        try:
            self._record_runtime_start_failure(
                session_id=session_id,
                error=error,
                cleanup_errors=cleanup_errors,
            )
        except Exception as record_exc:
            error.add_note(
                f"runtime failure handling could not persist its final record: {record_exc}"
            )

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
    ) -> ServiceRuntimeStartResult | ServiceRuntimePendingResult:
        """Bind or resume one exact JARVIS-owned service without submitting work.

        The immutable binding and owner identity derive the gateway ID. Reissuing
        an identical request therefore resumes the same connector intents; it
        cannot create a second gateway, scheduler job, or untracked connector.
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
        if binding.scheduler_native_id is not None:
            if binding.scheduler_provider is None:
                raise ConfigurationError(
                    "scheduler-backed JARVIS runtime omitted its scheduler provider"
                )
            if runtime.host not in {"127.0.0.1", "::1", "localhost"}:
                raise ConfigurationError(
                    "scheduler-backed JARVIS services must advertise a loopback-only endpoint"
                )

        owner_identity = self._jarvis_bind_owner_identity(
            owner_session_id=owner_session_id,
            owner_session_generation_id=owner_session_generation_id,
            owner_session_admission_id=owner_session_admission_id,
        )
        session_id, identity_sha256 = self._jarvis_bind_identity(
            binding=binding,
            owner_identity=owner_identity,
        )
        requested_policy = self._jarvis_bind_policy(
            name=name,
            transport_mode=transport_mode,
            requested_desktop_bind_port=desktop_bind_port,
        )
        self.queue.initialize()
        # The deterministic transition lock is also the binding-creation lock.
        # No process can race a lookup/create pair for this immutable identity.
        with self._gateway_transition_lock(session_id):
            try:
                session = self.queue.get_gateway_session(session_id)
            except NotFoundError:
                # Resolve authenticated authority before creating any durable or
                # process side effect. Authorization/integrity failures fail closed.
                authorization = self._jarvis_runtime_authorization(verified)
                local_port = (
                    _readiness._available_loopback_port(exclude={runtime.port})
                    if desktop_bind_port is None
                    else _readiness._validated_available_loopback_port(desktop_bind_port)
                )
                spec = self._jarvis_runtime_spec(
                    verified=verified,
                    local_port=local_port,
                    transport_mode=transport_mode,
                    readiness_timeout_seconds=readiness_timeout_seconds,
                    poll_seconds=poll_seconds,
                )
                policy = {
                    **requested_policy,
                    "actual_desktop_bind_port": local_port,
                }
                owner_metadata: dict[str, object] = {
                    "owner": "clio-relay",
                    "runtime_kind": spec.kind,
                    "binding_source": "jarvis_mcp_result",
                    "jarvis_bind_identity_sha256": identity_sha256,
                    "source_relay_job_id": binding.source_relay_job_id,
                    "source_relay_artifact_id": binding.source_relay_artifact_id,
                    "jarvis_execution_id": binding.jarvis_execution_id,
                    **{key: value for key, value in owner_identity.items() if value is not None},
                }
                session = self.queue.create_gateway_session(
                    GatewaySession(
                        session_id=session_id,
                        cluster=self.cluster,
                        name=name,
                        state=GatewaySessionState.CREATED,
                        scheduler=binding.scheduler_provider or "external",
                        scheduler_job_id=binding.scheduler_native_id,
                        requested_resources={"service_port": runtime.port},
                        gateway={
                            "runtime_spec": spec.model_dump(mode="json"),
                            "jarvis_runtime_binding": binding.model_dump(mode="json"),
                            "jarvis_bind_policy": policy,
                            "transport": {"mode": transport_mode},
                            "ownership_intents": {
                                "scheduler_submission": _scheduler_contracts._new_ownership_intent(
                                    "absent_verified",
                                    source="verified_jarvis_runtime_binding",
                                ),
                                "remote_connector": _scheduler_contracts._new_ownership_intent(
                                    "not_started"
                                ),
                                "desktop_connector": _scheduler_contracts._new_ownership_intent(
                                    "not_started"
                                ),
                            },
                        },
                        metadata=owner_metadata,
                    )
                )
                session = self._runtime_start_session_after_lock(session.session_id)
            else:
                self._validate_jarvis_binding_session(
                    session=session,
                    verified=verified,
                    expected_policy=requested_policy,
                    expected_owner_identity=owner_identity,
                )
                if session.gateway.get("teardown_intent") is not None:
                    raise ConfigurationError(
                        f"gateway session {session.session_id} is committed to teardown "
                        "and cannot resume"
                    )
                if session.state is GatewaySessionState.READY:
                    return self._ready_start_result(session)
                authorization = self._jarvis_runtime_authorization(verified)
            return self._resume_jarvis_binding_locked(
                session_id=session.session_id,
                verified=verified,
                authorization=authorization,
                readiness_timeout_seconds=readiness_timeout_seconds,
                poll_seconds=poll_seconds,
            )

    @staticmethod
    def _jarvis_bind_owner_identity(
        *,
        owner_session_id: str | None,
        owner_session_generation_id: str | None,
        owner_session_admission_id: str | None,
    ) -> dict[str, object]:
        """Return the complete owner identity used by deterministic JARVIS binds."""
        return {
            "owner_session_id": owner_session_id,
            "owner_session_generation_id": owner_session_generation_id,
            "owner_session_admission_id": owner_session_admission_id,
        }

    def _jarvis_bind_identity(
        self,
        *,
        binding: JarvisServiceRuntimeBinding,
        owner_identity: dict[str, object],
    ) -> tuple[str, str]:
        """Derive one portable gateway ID from immutable binding and owner identity."""
        document = {
            "schema_version": _JARVIS_BIND_IDENTITY_SCHEMA,
            "cluster": self.cluster,
            "binding": binding.model_dump(mode="json"),
            "owner_identity": owner_identity,
        }
        encoded = json.dumps(
            document,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
        digest = hashlib.sha256(encoded).hexdigest()
        return f"gateway_{digest[:32]}", digest

    @staticmethod
    def _jarvis_bind_policy(
        *,
        name: str,
        transport_mode: str,
        requested_desktop_bind_port: int | None,
    ) -> dict[str, object]:
        """Return side-effect policy that cannot change on an idempotent replay."""
        return {
            "schema_version": _JARVIS_BIND_POLICY_SCHEMA,
            "name": name,
            "transport_mode": transport_mode,
            "requested_desktop_bind_port": requested_desktop_bind_port,
        }

    @staticmethod
    def _jarvis_runtime_spec(
        *,
        verified: VerifiedJarvisServiceRuntime,
        local_port: int,
        transport_mode: str,
        readiness_timeout_seconds: float,
        poll_seconds: float,
    ) -> ServiceRuntimeSpec:
        """Build the generic connector spec from verified JARVIS endpoints only."""
        runtime = verified.runtime
        return ServiceRuntimeSpec(
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
            scheduler=verified.binding.scheduler_provider or "external",
            connect_url_template=f"{runtime.protocol}://{{bind_addr}}:{{bind_port}}",
            metadata={
                "source": "verified_jarvis_service_runtime",
                "service_instance_id": runtime.service_instance_id,
                "service_revision": runtime.revision,
            },
        )

    def _validate_jarvis_binding_session(
        self,
        *,
        session: GatewaySession,
        verified: VerifiedJarvisServiceRuntime,
        expected_policy: dict[str, object] | None = None,
        expected_owner_identity: dict[str, object] | None = None,
    ) -> ServiceRuntimeSpec:
        """Fail closed if a persisted JARVIS binding or immutable policy changed."""
        self._validate_gateway_transition_session(session)
        try:
            stored_binding = JarvisServiceRuntimeBinding.model_validate(
                session.gateway.get("jarvis_runtime_binding")
            )
            spec = ServiceRuntimeSpec.model_validate(session.gateway.get("runtime_spec"))
        except ValueError as exc:
            raise RelayError("JARVIS gateway binding evidence is invalid") from exc
        if stored_binding != verified.binding:
            raise ConfigurationError("JARVIS gateway binding identity changed")
        owner_identity = self._jarvis_bind_owner_identity(
            owner_session_id=_primitives._optional_str(session.metadata.get("owner_session_id")),
            owner_session_generation_id=_primitives._optional_str(
                session.metadata.get("owner_session_generation_id")
            ),
            owner_session_admission_id=_primitives._optional_str(
                session.metadata.get("owner_session_admission_id")
            ),
        )
        expected_session_id, identity_sha256 = self._jarvis_bind_identity(
            binding=stored_binding,
            owner_identity=owner_identity,
        )
        if (
            session.session_id != expected_session_id
            or session.metadata.get("jarvis_bind_identity_sha256") != identity_sha256
        ):
            raise RelayError("JARVIS gateway deterministic binding identity is invalid")
        if expected_owner_identity is not None and owner_identity != expected_owner_identity:
            raise ConfigurationError("JARVIS gateway owner identity changed")
        policy = _primitives._object(session.gateway.get("jarvis_bind_policy", {}))
        actual_port = policy.get("actual_desktop_bind_port")
        if (
            policy.get("schema_version") != _JARVIS_BIND_POLICY_SCHEMA
            or policy.get("name") != session.name
            or policy.get("transport_mode") != spec.transport_mode
            or isinstance(actual_port, bool)
            or not isinstance(actual_port, int)
            or actual_port != spec.desktop_bind_port
        ):
            raise RelayError("JARVIS gateway immutable bind policy is invalid")
        if expected_policy is not None and any(
            policy.get(key) != value for key, value in expected_policy.items()
        ):
            raise ConfigurationError(
                "JARVIS runtime is already bound with a different immutable policy"
            )
        runtime = verified.runtime
        expected_scheduler = stored_binding.scheduler_provider or "external"
        if (
            spec.deployment_driver != "jarvis-bound"
            or spec.kind != "jarvis-service-runtime"
            or session.scheduler != expected_scheduler
            or session.scheduler_job_id != stored_binding.scheduler_native_id
            or spec.scheduler != expected_scheduler
            or spec.service_port != runtime.port
            or spec.protocol != runtime.protocol
            or spec.health_path != runtime.health_path
            or spec.stream_path != runtime.live_data_path
            or spec.event_stream_path != runtime.events_path
            or spec.state_path != runtime.state_path
            or spec.command_path != runtime.command_path
            or spec.desktop_bind_addr != "127.0.0.1"
            or spec.transport_mode != policy.get("transport_mode")
        ):
            raise RelayError("JARVIS gateway endpoints or scheduler identity changed")
        return spec

    def _resume_jarvis_binding_locked(
        self,
        *,
        session_id: str,
        verified: VerifiedJarvisServiceRuntime,
        authorization: str | None,
        readiness_timeout_seconds: float,
        poll_seconds: float,
    ) -> ServiceRuntimeStartResult | ServiceRuntimePendingResult:
        """Advance one exact JARVIS binding while holding its transition lock."""
        session = self.queue.get_gateway_session(session_id)
        spec = self._validate_jarvis_binding_session(session=session, verified=verified)
        if session.state is GatewaySessionState.READY:
            return self._ready_start_result(session)
        if session.state in {GatewaySessionState.FAILED, GatewaySessionState.CLOSED}:
            raise ConfigurationError(
                f"gateway session {session_id} cannot resume from {session.state.value}"
            )
        if session.gateway.get("teardown_intent") is not None:
            raise ConfigurationError(
                f"gateway session {session_id} is committed to teardown and cannot resume"
            )
        if session.state is GatewaySessionState.DEGRADED:
            return self._attach_serialized(session_id=session_id)
        if session.state not in {
            GatewaySessionState.CREATED,
            GatewaySessionState.SUBMITTED,
            GatewaySessionState.PENDING,
            GatewaySessionState.ALLOCATED,
            GatewaySessionState.STARTING,
        }:
            raise ConfigurationError(
                f"gateway session {session_id} cannot resume from {session.state.value}"
            )
        runtime = verified.runtime
        if runtime.lifecycle != "ready":
            raise ConfigurationError("JARVIS service runtime is no longer ready")
        if session.state is GatewaySessionState.CREATED:
            session = self._update(
                session,
                state=GatewaySessionState.STARTING,
                queue_state=runtime.lifecycle,
                node=runtime.host,
                metadata={"binding_started_at": utc_now().isoformat()},
            )
        session = self._reconcile_ownership_intents(session)
        unresolved_connector = self._first_unresolved_connector_role(session)
        if unresolved_connector is not None:
            return self._connector_recovery_pending(session, role=unresolved_connector)

        proxy_name = f"{session.session_id}-service"
        transport = _primitives._object(session.gateway.get("transport", {}))
        remote_connector = _primitives._object(transport.get("remote_connector", {}))
        if remote_connector:
            if not self._connector_reuse_is_verified(session, role="remote_connector"):
                return self._connector_recovery_pending(session, role="remote_connector")
        else:
            if not self._connector_launch_is_authorized(session, role="remote_connector"):
                return self._connector_recovery_pending(session, role="remote_connector")
            remote_intent = self._jarvis_connector_start_intent(
                session,
                role="remote_connector",
            )
            session = self._set_ownership_intent(session, "remote_connector", remote_intent)
            try:
                remote_connector = self._start_remote_connector(
                    session=session,
                    spec=spec,
                    node=runtime.host,
                    proxy_name=proxy_name,
                    ownership_intent=remote_intent,
                    allocation_provider=verified.binding.scheduler_provider,
                    allocation_job_id=verified.binding.scheduler_native_id,
                )
            except _types._AmbiguousRemoteSideEffectError as exc:
                latest = self.queue.get_gateway_session(session.session_id)
                pending = self._record_runtime_observation_pending(
                    latest,
                    node=runtime.host,
                    error=exc,
                    provider_status=None,
                    state=GatewaySessionState.STARTING,
                    queue_state=runtime.lifecycle,
                    preserve_scheduler_status=True,
                )
                return ServiceRuntimePendingResult(session=pending)
            except RelayError as exc:
                provider_status: SchedulerStatus | None = None
                if (
                    verified.binding.scheduler_provider is not None
                    and verified.binding.scheduler_native_id is not None
                ):
                    try:
                        provider_status = self._poll_scheduler_provider(
                            provider=verified.binding.scheduler_provider,
                            scheduler_job_id=verified.binding.scheduler_native_id,
                        )
                    except RelayError:
                        provider_status = None
                if provider_status is not None and provider_status.phase in {
                    SchedulerPhase.COMPLETED,
                    SchedulerPhase.FAILED,
                    SchedulerPhase.CANCELED,
                }:
                    definitive = _types._DefinitiveRuntimeObservationError(
                        "scheduler job reached a terminal state before its verified JARVIS "
                        "service could be bound: "
                        f"job={verified.binding.scheduler_native_id} "
                        f"state={provider_status.phase.value}"
                    )
                    self._rollback_jarvis_binding(session_id=session_id, error=definitive)
                    raise definitive from exc
                latest = self.queue.get_gateway_session(session.session_id)
                pending = self._record_runtime_observation_pending(
                    latest,
                    node=runtime.host,
                    error=exc,
                    provider_status=provider_status,
                    state=GatewaySessionState.STARTING,
                    queue_state=(
                        provider_status.phase.value
                        if provider_status is not None
                        else runtime.lifecycle
                    ),
                    preserve_scheduler_status=provider_status is None,
                )
                return ServiceRuntimePendingResult(session=pending)
            # Allocation connector startup can publish placement intent first.
            session = self.queue.get_gateway_session(session.session_id)
            session = self._update(
                session,
                gateway=self._gateway_with_ownership_intent(
                    session,
                    "remote_connector",
                    _scheduler_contracts._new_ownership_intent("recorded", **remote_connector),
                    transport={
                        **_primitives._object(session.gateway.get("transport", {})),
                        "proxy_name": proxy_name,
                        "remote_connector": remote_connector,
                    },
                ),
            )

        transport = _primitives._object(session.gateway.get("transport", {}))
        local_connector = _primitives._object(transport.get("desktop_connector", {}))
        if local_connector:
            if not self._connector_reuse_is_verified(session, role="desktop_connector"):
                return self._connector_recovery_pending(session, role="desktop_connector")
        else:
            if not self._connector_launch_is_authorized(session, role="desktop_connector"):
                return self._connector_recovery_pending(session, role="desktop_connector")
            local_intent = self._jarvis_connector_start_intent(
                session,
                role="desktop_connector",
            )
            session = self._set_ownership_intent(session, "desktop_connector", local_intent)
            try:
                local_connector = self._start_local_visitor(
                    session=session,
                    spec=spec,
                    proxy_name=proxy_name,
                    ownership_intent=local_intent,
                )
            except (RelayError, OSError, subprocess.SubprocessError) as exc:
                pending = self._record_runtime_observation_pending(
                    self.queue.get_gateway_session(session.session_id),
                    node=runtime.host,
                    error=RelayError(str(exc)),
                    provider_status=None,
                    state=GatewaySessionState.STARTING,
                    queue_state=runtime.lifecycle,
                    preserve_scheduler_status=True,
                )
                return ServiceRuntimePendingResult(session=pending)
            session = self._update(
                session,
                gateway=self._gateway_with_ownership_intent(
                    session,
                    "desktop_connector",
                    _scheduler_contracts._new_ownership_intent("recorded", **local_connector),
                    transport={
                        **_primitives._object(session.gateway.get("transport", {})),
                        "proxy_name": proxy_name,
                        "remote_connector": remote_connector,
                        "desktop_connector": local_connector,
                    },
                ),
            )

        local_port = spec.desktop_bind_port
        base_url = f"{runtime.protocol}://127.0.0.1:{local_port}"
        connect_url = base_url
        health_url = f"{base_url}{runtime.health_path}"
        stream_url = f"{base_url}{runtime.live_data_path}"
        events_url = f"{base_url}{runtime.events_path}"
        state_url = f"{base_url}{runtime.state_path}"
        command_url = f"{base_url}{runtime.command_path}"
        try:
            self._wait_for_jarvis_health(
                health_url,
                timeout_seconds=min(
                    readiness_timeout_seconds,
                    _RUNTIME_HEALTH_OBSERVATION_TIMEOUT_SECONDS,
                ),
                poll_seconds=poll_seconds,
                runtime_schema_version=runtime.schema_version,
                authorization=authorization,
                max_attempts=1,
            )
        except _types._DefinitiveRuntimeObservationError as exc:
            self._rollback_jarvis_binding(session_id=session_id, error=exc)
            raise
        except RelayError as exc:
            pending = self._record_runtime_observation_pending(
                session,
                node=runtime.host,
                error=exc,
                provider_status=None,
                state=GatewaySessionState.STARTING,
                queue_state=runtime.lifecycle,
                preserve_scheduler_status=True,
            )
            return ServiceRuntimePendingResult(session=pending)

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
                    "mode": spec.transport_mode,
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

    def _jarvis_connector_start_intent(
        self,
        session: GatewaySession,
        *,
        role: Literal["remote_connector", "desktop_connector"],
    ) -> dict[str, object]:
        """Reuse an absence-proven generation instead of inventing a retry identity."""
        intents = _primitives._object(session.gateway.get("ownership_intents", {}))
        previous = _primitives._object(intents.get(role, {}))
        if previous.get("state") != "absent_verified":
            if role == "desktop_connector":
                return self._local_connector_intent(session)
            return _scheduler_contracts._new_ownership_intent(
                "starting",
                owner_token=secrets.token_hex(32),
                connector_generation_id=secrets.token_hex(16),
            )
        identity: dict[str, object] = {
            "owner_token": _scheduler_contracts._required_intent_str(previous, "owner_token"),
            "connector_generation_id": _scheduler_contracts._required_intent_str(
                previous,
                "connector_generation_id",
            ),
        }
        if role == "desktop_connector":
            for field in (
                "config_path",
                "stdout_path",
                "stderr_path",
                "metadata_path",
            ):
                identity[field] = _scheduler_contracts._required_intent_str(previous, field)
        return _scheduler_contracts._new_ownership_intent("starting", **identity)

    def _rollback_jarvis_binding(
        self,
        *,
        session_id: str,
        error: BaseException,
    ) -> None:
        """Fail closed and clean exact connectors after a definitive bind failure."""
        cleanup_errors: list[str] = []
        local_connector: dict[str, object] | None = None
        remote_connector: dict[str, object] | None = None
        try:
            recovered = self._reconcile_ownership_intents(
                self.queue.get_gateway_session(session_id)
            )
            transport = _primitives._object(recovered.gateway.get("transport", {}))
            local_connector = _primitives._object(transport.get("desktop_connector", {})) or None
            remote_connector = _primitives._object(transport.get("remote_connector", {})) or None
        except (ConfigurationError, RelayError) as exc:
            cleanup_errors.append(f"connector rollback reconciliation failed: {exc}")
        if local_connector is not None:
            try:
                _, result = self._stop_local_connector(
                    session_id=session_id,
                    connector=local_connector,
                    require_record=True,
                )
                if result.residual or not result.verified_after_operation:
                    cleanup_errors.append(
                        result.detail or "desktop connector rollback was not proven"
                    )
            except (ConfigurationError, RelayError) as exc:
                cleanup_errors.append(str(exc))
        if remote_connector is not None:
            try:
                if remote_connector.get("execution_scope") == "scheduler_allocation":
                    result = self._stop_allocation_connector(
                        session_id=session_id,
                        connector=remote_connector,
                    )
                    if result.residual or not result.verified_after_operation:
                        cleanup_errors.append(
                            result.detail or "allocation connector rollback was not proven"
                        )
                else:
                    remote_pid = _primitives._optional_int(remote_connector.get("pid"))
                    if remote_pid is None:
                        raise RelayError("remote connector rollback has no recorded pid")
                    result = _scheduler_contracts._last_json_object(
                        self._ssh(
                            _remote_stop_script(
                                session_id=session_id,
                                pid=remote_pid,
                            )
                        )
                    )
                    if not _connector_identity._remote_cleanup_proven(result):
                        cleanup_errors.append(
                            "remote connector rollback did not prove process-group absence"
                        )
            except (ConfigurationError, RelayError) as exc:
                cleanup_errors.append(str(exc))
        self._record_runtime_start_failure(
            session_id=session_id,
            error=error,
            cleanup_errors=cleanup_errors,
        )

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
                expiry = _readiness._utc_timestamp(existing.expires_at)
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

        public_port = bind_port or _readiness._available_loopback_port(
            exclude={spec.desktop_bind_port}
        )
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
        intent = _scheduler_contracts._new_ownership_intent(
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
                browser_proxy_intent=_scheduler_contracts._new_ownership_intent(
                    "recorded", **proxy
                ),
            )
            grant = _readiness._browser_attachment_grant(
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
        except BrowserAttachmentIdentityConflictError as exc:
            raise ConfigurationError(
                "browser attachment id does not match the gateway record"
            ) from exc
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
        revocation_path = _readiness._owned_browser_runtime_path(
            self.settings,
            session.session_id,
            record.revocation_path,
        )
        _readiness._write_browser_revocation_marker(revocation_path, record.attachment_id)
        transport = _primitives._object(session.gateway.get("transport", {}))
        proxy = _primitives._object(transport.get("browser_proxy", {}))
        intents = _primitives._object(session.gateway.get("ownership_intents", {}))
        intent = _primitives._object(intents.get("browser_proxy", {}))
        absence_verified = False
        if not proxy:
            proxy, absence_verified = _connector_identity._discover_local_connector(
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
        intents["browser_proxy"] = _scheduler_contracts._new_ownership_intent(
            "absent_verified",
            attachment_id=attachment_id,
            owner_token=intent.get("owner_token"),
            connector_generation_id=intent.get("connector_generation_id"),
            config_path=intent.get("config_path"),
        )
        session = self.queue.finish_gateway_browser_attachment_revoke(
            session.session_id,
            attachment=revoked,
            browser_proxy_absent_intent=_primitives._object(intents["browser_proxy"]),
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
        scheduler_contract = _scheduler_contracts._validated_durable_scheduler_contract(
            session, strict=False
        )

        # Reconciliation may refresh durable connector identities, but cannot alter
        # the teardown policy that was committed before any cleanup side effect.
        self._validate_teardown_policy(
            session,
            cancel_scheduler_job=cancel_scheduler_job,
            final_state=final_state,
        )

        session, browser_resource, browser_error = self._revoke_browser_for_runtime_cleanup(session)
        teardown_intent = _primitives._object(session.gateway.get("teardown_intent", {}))
        ownership_intents = _primitives._object(session.gateway.get("ownership_intents", {}))
        transport = _primitives._object(session.gateway.get("transport", {}))
        desktop_connector = _primitives._object(transport.get("desktop_connector", {}))
        remote_connector = _primitives._object(transport.get("remote_connector", {}))
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
            absence_verified=_scheduler_contracts._intent_proves_absence(
                ownership_intents,
                "desktop_connector",
            ),
        )
        local_resource = _primitives._bind_cleanup_resource_to_gateway(
            local_resource, session.session_id
        )
        resources.append(local_resource)
        if local_resource.residual:
            errors.append(local_resource.detail or "desktop connector cleanup failed")
        stopped_remote_pid = None
        remote_pid = _primitives._optional_int(remote_connector.get("pid"))
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
                        _primitives._optional_str(remote_connector.get("scheduler_step_id"))
                        or session.session_id
                    ),
                    location=self.definition.ssh_host,
                    provider=_primitives._optional_str(remote_connector.get("scheduler_provider")),
                    action="stop",
                    ownership_verified=False,
                    outcome="refused",
                    residual=True,
                    detail=str(exc),
                )
                errors.append(str(exc))
        elif remote_pid is None:
            absence_verified = _scheduler_contracts._intent_proves_absence(
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
                remote_result = _scheduler_contracts._last_json_object(remote_output)
                remote_outcome = remote_result.get("outcome")
                if not _connector_identity._remote_cleanup_proven(remote_result):
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
        resources.append(
            _primitives._bind_cleanup_resource_to_gateway(remote_resource, session.session_id)
        )
        canceled_scheduler_job = None
        scheduler_intent = _primitives._object(ownership_intents.get("scheduler_submission", {}))
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
                                _submission_scripts._template_command_script(
                                    spec.cancel_command, scheduler_job_id
                                )
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
                        if terminal_state in _scheduler_contracts._CANCELED_RUNTIME_STATES:
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
        cleanup_operation_id = _scheduler_contracts._required_intent_str(
            teardown_intent, "operation_id"
        )
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
        pending_without_connectors = self._pending_submission_has_no_connector_side_effects(session)
        scheduler_contract = _scheduler_contracts._validated_durable_scheduler_contract(
            session, strict=False
        )
        session, browser_resource, browser_error = self._revoke_browser_for_runtime_cleanup(session)
        transport = _primitives._object(session.gateway.get("transport", {}))
        desktop_connector = _primitives._object(transport.get("desktop_connector", {}))
        ownership_intents = _primitives._object(session.gateway.get("ownership_intents", {}))
        desktop_absence_verified = _scheduler_contracts._intent_proves_absence(
            ownership_intents,
            "desktop_connector",
        )
        stopped_local_pid, local_resource = self._stop_local_connector(
            session_id=session.session_id,
            connector=desktop_connector,
            require_record=not (pending_without_connectors or desktop_absence_verified),
            absence_verified=pending_without_connectors or desktop_absence_verified,
        )
        local_resource = _primitives._bind_cleanup_resource_to_gateway(
            local_resource, session.session_id
        )
        resources = [local_resource]
        if browser_resource is not None:
            resources.insert(0, browser_resource)
        errors = (
            [local_resource.detail] if local_resource.residual and local_resource.detail else []
        )
        if browser_error is not None:
            errors.append(browser_error)
        remote_connector = _primitives._object(transport.get("remote_connector", {}))
        remote_pid = _primitives._optional_int(remote_connector.get("pid"))
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
                        _primitives._optional_str(remote_connector.get("scheduler_step_id"))
                        or session.session_id
                    ),
                    location=self.definition.ssh_host,
                    provider=_primitives._optional_str(remote_connector.get("scheduler_provider")),
                    action="retain",
                    ownership_verified=False,
                    outcome="failed",
                    residual=True,
                    detail=str(exc),
                )
            resources.append(
                _primitives._bind_cleanup_resource_to_gateway(
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
                    remote_status = _scheduler_contracts._last_json_object(
                        self._ssh(
                            _connector_step_scripts._remote_connector_status_script(
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
                _primitives._bind_cleanup_resource_to_gateway(
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
        elif pending_without_connectors:
            resources.append(
                _primitives._bind_cleanup_resource_to_gateway(
                    CleanupResource(
                        kind="remote_connector",
                        resource_id=session.session_id,
                        location=self.definition.ssh_host,
                        action="retain",
                        ownership_verified=True,
                        outcome="missing",
                        verified_after_operation=True,
                        observed_state="not_created",
                        detail=(
                            "durable connector intent proves no remote connector side effect "
                            "was created"
                        ),
                    ),
                    session.session_id,
                )
            )
        else:
            errors.append("owned remote connector record is missing during detach")
            resources.append(
                _primitives._bind_cleanup_resource_to_gateway(
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
            scheduler_intent = _primitives._object(
                _primitives._object(session.gateway.get("ownership_intents", {})).get(
                    "scheduler_submission",
                    {},
                )
            )
            submission_id = _scheduler_contracts._required_intent_str(
                scheduler_intent, "submission_id"
            )
            submission_marker = _scheduler_contracts._required_intent_str(
                scheduler_intent,
                "submission_marker",
            )
            scheduler_resource = CleanupResource(
                kind="scheduler_submission",
                resource_id=submission_id,
                location=self.definition.ssh_host,
                provider=scheduler_contract.provider,
                action="retain",
                metadata={
                    "gateway_session_id": session.session_id,
                    "submission_id": submission_id,
                    "submission_marker": submission_marker,
                    "scheduler_job_id": None,
                    "submission_outcome": "unresolved",
                    "cancel_requested": False,
                    "resubmit_requested": False,
                },
                ownership_verified=True,
                outcome="retained",
                verified_after_operation=True,
                observed_state="intent_recorded",
                residual=False,
                detail=(
                    "exact scheduler submission intent retained without claiming a scheduler job"
                ),
            )
            resources.append(scheduler_resource)
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
        detach_intent = _scheduler_contracts._validated_gateway_detach_intent(session)
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

    def attach(
        self,
        *,
        session_id: str,
    ) -> ServiceRuntimeStartResult | ServiceRuntimePendingResult:
        """Serialize attachment against detach and teardown for this gateway."""
        session = self.queue.get_gateway_session(session_id)
        self._validate_gateway_transition_session(session)
        with self._gateway_transition_lock(session_id):
            return self._attach_serialized(session_id=session_id)

    def _attach_serialized(
        self,
        *,
        session_id: str,
    ) -> ServiceRuntimeStartResult | ServiceRuntimePendingResult:
        """Recreate the desktop connector while holding the gateway transition lock."""
        session = self.queue.get_gateway_session(session_id)
        self._validate_gateway_transition_session(session)
        if session.state == GatewaySessionState.CLOSED:
            raise ConfigurationError(f"gateway session {session_id} is closed")
        if session.gateway.get("teardown_intent") is not None:
            raise ConfigurationError(
                f"gateway session {session_id} is committed to teardown and cannot attach"
            )
        if self._detached_pending_submission_can_resume(
            session
        ) or self._pre_ready_submission_can_resume(session):
            return self._resume_start_locked(session_id=session_id)
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
        transport = _primitives._object(session.gateway.get("transport", {}))
        remote_connector = _primitives._object(transport.get("remote_connector", {}))
        if not remote_connector or not self._connector_reuse_is_verified(
            session,
            role="remote_connector",
        ):
            return self._connector_recovery_pending(
                session,
                role="remote_connector",
            )
        proxy_name = _primitives._optional_str(transport.get("proxy_name"))
        if proxy_name is None:
            raise ConfigurationError("gateway session has no recorded transport proxy name")
        existing = _primitives._object(transport.get("desktop_connector", {}))
        existing_pid = _primitives._optional_int(existing.get("pid"))
        existing_config = _primitives._optional_str(existing.get("config_path"))
        existing_owned = (
            existing.get("owner") == "clio-relay"
            and existing.get("session_id") == session_id
            and existing_config is not None
        )
        created_connector = False
        local_connector: dict[str, object] | None = None
        try:
            if existing_pid is not None and existing_owned:
                if not self._connector_reuse_is_verified(
                    session,
                    role="desktop_connector",
                ):
                    return self._connector_recovery_pending(
                        session,
                        role="desktop_connector",
                    )
                local_connector = existing
            else:
                if not self._connector_launch_is_authorized(
                    session,
                    role="desktop_connector",
                ):
                    return self._connector_recovery_pending(
                        session,
                        role="desktop_connector",
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
                created_connector = True
                session = self._update(
                    session,
                    gateway=self._gateway_with_ownership_intent(
                        session,
                        "desktop_connector",
                        _scheduler_contracts._new_ownership_intent("recorded", **local_connector),
                        transport={
                            **_primitives._object(session.gateway.get("transport", {})),
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
            try:
                if verified_runtime is None:
                    self._wait_for_local_health(
                        health_url,
                        min(
                            spec.readiness_timeout_seconds,
                            _RUNTIME_HEALTH_OBSERVATION_TIMEOUT_SECONDS,
                        ),
                        spec.poll_seconds,
                        expected_body=spec.health_expected_body,
                        max_attempts=1,
                    )
                else:
                    self._wait_for_jarvis_health(
                        health_url,
                        timeout_seconds=min(
                            spec.readiness_timeout_seconds,
                            _RUNTIME_HEALTH_OBSERVATION_TIMEOUT_SECONDS,
                        ),
                        poll_seconds=spec.poll_seconds,
                        runtime_schema_version=verified_runtime.runtime.schema_version,
                        authorization=service_authorization,
                        max_attempts=1,
                    )
            except _types._DefinitiveRuntimeObservationError as exc:
                self._rollback_jarvis_binding(session_id=session_id, error=exc)
                raise
            except RelayError as exc:
                pending = self._record_runtime_observation_pending(
                    session,
                    node=session.node,
                    error=exc,
                    provider_status=None,
                    state=GatewaySessionState.STARTING,
                    queue_state=session.queue_state or "running",
                    preserve_scheduler_status=True,
                )
                return ServiceRuntimePendingResult(session=pending)
        except Exception as exc:
            cleanup_error: str | None = None
            if not created_connector:
                try:
                    recovered = self._reconcile_ownership_intents(
                        self.queue.get_gateway_session(session.session_id)
                    )
                    recovered_local = _primitives._object(
                        _primitives._object(recovered.gateway.get("transport", {})).get(
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
                        **_primitives._object(session.gateway.get("transport", {})),
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
            _scheduler_contracts._validated_gateway_detach_intent(session)
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
            "schema_version": _scheduler_contracts._GATEWAY_DETACH_INTENT_SCHEMA,
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
        intent = _scheduler_contracts._validated_gateway_detach_intent(session)
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
        _scheduler_contracts._gateway_teardown_timestamp(completed_at)
        stopped_local_pid = _scheduler_contracts._strict_optional_positive_int(
            result.get("stopped_local_pid")
        )
        resources, errors = _scheduler_contracts._validated_completed_resource_lists(
            result,
            error="gateway detach evidence is invalid",
        )
        _scheduler_contracts._validate_completed_detach_resources(
            session,
            resources=resources,
            stopped_local_pid=stopped_local_pid,
            operation_id=operation_id,
        )
        if not _scheduler_contracts._completed_detach_metadata_matches(
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

    def _pending_submission_has_no_connector_side_effects(
        self,
        session: GatewaySession,
    ) -> bool:
        """Prove a not-yet-ready submission has never launched either connector."""

        if session.state not in {
            GatewaySessionState.SUBMITTED,
            GatewaySessionState.PENDING,
            GatewaySessionState.ALLOCATED,
            GatewaySessionState.STARTING,
            GatewaySessionState.DEGRADED,
        }:
            return False
        if "service" in session.gateway:
            return False
        transport = _primitives._object(session.gateway.get("transport", {}))
        if _primitives._object(transport.get("remote_connector", {})) or _primitives._object(
            transport.get("desktop_connector", {})
        ):
            return False
        intents = _primitives._object(session.gateway.get("ownership_intents", {}))
        scheduler_intent = _primitives._object(intents.get("scheduler_submission", {}))
        scheduler_identity_exact = (
            self._scheduler_submission_reconciliation_is_pending(session)
            if session.scheduler_job_id is None
            else scheduler_intent.get("schema_version") == _primitives._OWNERSHIP_INTENT_SCHEMA
            and scheduler_intent.get("state") == "recorded"
            and scheduler_intent.get("scheduler_provider") == session.scheduler
            and scheduler_intent.get("scheduler_job_id") == session.scheduler_job_id
        )
        return scheduler_identity_exact and all(
            _primitives._object(intents.get(role, {})).get("schema_version")
            == _primitives._OWNERSHIP_INTENT_SCHEMA
            and _primitives._object(intents.get(role, {})).get("state")
            in {"not_started", "absent_verified"}
            for role in ("remote_connector", "desktop_connector")
        )

    def _detached_pending_submission_can_resume(self, session: GatewaySession) -> bool:
        """Return whether a detached pre-ready submission can safely advance again."""

        if (
            session.state is not GatewaySessionState.DEGRADED
            or session.gateway.get("detach_intent") is None
            or "service" in session.gateway
        ):
            return False
        intents = _primitives._object(session.gateway.get("ownership_intents", {}))
        scheduler_intent = _primitives._object(intents.get("scheduler_submission", {}))
        return bool(
            self._scheduler_submission_reconciliation_is_pending(session)
            if session.scheduler_job_id is None
            else scheduler_intent.get("schema_version") == _primitives._OWNERSHIP_INTENT_SCHEMA
            and scheduler_intent.get("state") == "recorded"
            and scheduler_intent.get("scheduler_job_id") == session.scheduler_job_id
        )

    def _pre_ready_submission_can_resume(self, session: GatewaySession) -> bool:
        """Return whether attach should advance an existing pre-ready start in place."""
        return bool(
            session.state
            in {
                GatewaySessionState.SUBMITTED,
                GatewaySessionState.PENDING,
                GatewaySessionState.ALLOCATED,
                GatewaySessionState.STARTING,
            }
            and session.gateway.get("teardown_intent") is None
            and "service" not in session.gateway
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
        return cast(FileLock, lock)

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
        intent = _scheduler_contracts._validated_gateway_teardown_intent(
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
        intent = _scheduler_contracts._validated_gateway_teardown_intent(
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
        _scheduler_contracts._gateway_teardown_timestamp(committed_at)
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
        _scheduler_contracts._gateway_teardown_timestamp(completed_at)
        stopped_local_pid = _scheduler_contracts._strict_optional_positive_int(
            result.get("stopped_local_pid")
        )
        stopped_remote_pid = _scheduler_contracts._strict_optional_positive_int(
            result.get("stopped_remote_pid")
        )
        canceled_scheduler_job = _scheduler_contracts._strict_optional_nonempty_str(
            result.get("canceled_scheduler_job")
        )
        resources, errors = _scheduler_contracts._validated_completed_resource_lists(
            result,
            error="completed gateway teardown evidence is invalid",
        )
        if errors or any(resource.residual for resource in resources):
            raise RelayError("completed gateway teardown evidence is invalid")
        _scheduler_contracts._validate_completed_teardown_resources(
            session,
            resources=resources,
            stopped_local_pid=stopped_local_pid,
            stopped_remote_pid=stopped_remote_pid,
            canceled_scheduler_job=canceled_scheduler_job,
            operation_id=operation_id,
            cancel_scheduler_job=cancel_scheduler_job,
        )
        if not _scheduler_contracts._completed_teardown_metadata_matches(
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
        intents = _primitives._object(gateway.get("ownership_intents", {}))
        intents[role] = intent
        gateway["ownership_intents"] = intents
        gateway.update(gateway_updates)
        return gateway

    def _local_connector_intent(self, session: GatewaySession) -> dict[str, object]:
        """Build the exact durable identity needed to rediscover a local connector."""
        runtime_dir = (
            self.settings.core_dir.parent / "runtime-sessions" / session.session_id
        ).resolve()
        return _scheduler_contracts._new_ownership_intent(
            "starting",
            owner_token=secrets.token_hex(32),
            connector_generation_id=secrets.token_hex(16),
            config_path=str(runtime_dir / "desktop-frpc.toml"),
            stdout_path=str(runtime_dir / "desktop-frpc.out"),
            stderr_path=str(runtime_dir / "desktop-frpc.err"),
            metadata_path=str(runtime_dir / "desktop-frpc-owner.json"),
        )

    def _validate_remote_connector_intent_binding(
        self,
        *,
        session_id: str,
        intent: dict[str, object],
        connector: dict[str, object],
    ) -> None:
        """Require a complete remote connector identity bound to one durable intent."""
        if (
            intent.get("schema_version") != _primitives._OWNERSHIP_INTENT_SCHEMA
            or intent.get("state") not in {"starting", "recorded"}
            or connector.get("owner") != "clio-relay"
            or connector.get("session_id") != session_id
            or connector.get("owner_token")
            != _scheduler_contracts._required_intent_str(intent, "owner_token")
            or connector.get("connector_generation_id")
            != _scheduler_contracts._required_intent_str(intent, "connector_generation_id")
        ):
            raise RelayError("remote connector record does not match its durable intent")
        common_fields = (
            "owner",
            "session_id",
            "owner_token",
            "connector_generation_id",
        )
        if intent.get("state") == "recorded" and any(
            intent.get(field) != connector.get(field) for field in common_fields
        ):
            raise RelayError("recorded remote connector identity changed after publication")
        if connector.get("execution_scope") == "scheduler_allocation":
            self._allocation_connector_identity(
                session_id=session_id,
                connector=connector,
            )
            allocation_fields = (
                "execution_scope",
                "scheduler_provider",
                "scheduler_native_id",
                "scheduler_step_id",
                "scheduler_step_marker",
                "scheduler_step",
                "placement",
                "config_path",
                "log_path",
            )
            if intent.get("state") == "recorded" and any(
                intent.get(field) != connector.get(field) for field in allocation_fields
            ):
                raise RelayError("recorded allocation connector identity changed after publication")
            return
        pid = _primitives._optional_int(connector.get("pid"))
        process_group_id = _primitives._optional_int(connector.get("process_group_id"))
        config_path = _primitives._optional_str(connector.get("config_path"))
        log_path = _primitives._optional_str(connector.get("log_path"))
        if pid is None or process_group_id != pid or config_path is None or log_path is None:
            raise RelayError("remote connector record has incomplete process identity")
        validated_config = _scheduler_contracts._validated_remote_session_file(
            config_path,
            session_id=session_id,
            filename="remote-frpc.toml",
        )
        validated_log = _scheduler_contracts._validated_remote_session_file(
            log_path,
            session_id=session_id,
            filename="remote-frpc.log",
        )
        if validated_config.parent != validated_log.parent:
            raise RelayError("remote connector paths do not belong to one owned session")
        process_fields = (
            "pid",
            "process_group_id",
            "config_path",
            "log_path",
        )
        if intent.get("state") == "recorded" and any(
            intent.get(field) != connector.get(field) for field in process_fields
        ):
            raise RelayError("recorded remote connector process identity changed after publication")

    def _validate_local_connector_intent_binding(
        self,
        *,
        session_id: str,
        intent: dict[str, object],
        connector: dict[str, object],
    ) -> None:
        """Require a complete desktop connector identity bound to one durable intent."""
        if (
            intent.get("schema_version") != _primitives._OWNERSHIP_INTENT_SCHEMA
            or intent.get("state") not in {"starting", "recorded"}
            or connector.get("owner") != "clio-relay"
            or connector.get("session_id") != session_id
            or connector.get("owner_token")
            != _scheduler_contracts._required_intent_str(intent, "owner_token")
            or connector.get("connector_generation_id")
            != _scheduler_contracts._required_intent_str(intent, "connector_generation_id")
        ):
            raise RelayError("desktop connector record does not match its durable intent")
        pid = _primitives._optional_int(connector.get("pid"))
        process_group_id = _primitives._optional_int(connector.get("process_group_id"))
        start_marker = _primitives._optional_str(connector.get("process_start_marker"))
        if pid is None or process_group_id is None or start_marker is None:
            raise RelayError("desktop connector record has incomplete process identity")
        runtime_dir = (self.settings.core_dir.parent / "runtime-sessions" / session_id).resolve()
        path_fields = (
            "config_path",
            "stdout_path",
            "stderr_path",
            "metadata_path",
        )
        for field in path_fields:
            value = _primitives._optional_str(connector.get(field))
            if value is None or Path(value).resolve().parent != runtime_dir:
                raise RelayError("desktop connector record escaped its owned runtime directory")
        identity_fields = (
            "owner",
            "session_id",
            "pid",
            "process_group_id",
            "process_start_marker",
            "owner_token",
            "connector_generation_id",
            *path_fields,
        )
        if intent.get("state") == "recorded" and any(
            intent.get(field) != connector.get(field) for field in identity_fields
        ):
            raise RelayError("recorded desktop connector identity changed after publication")

    @staticmethod
    def _connector_records_match(
        first: dict[str, object],
        second: dict[str, object],
        *,
        fields: Sequence[str],
    ) -> bool:
        """Return whether two records name the same complete connector generation."""
        return all(first.get(field) == second.get(field) for field in fields)

    def _reconcile_ownership_intents(self, session: GatewaySession) -> GatewaySession:
        """Recover scheduler and connector identities written before a hard exit."""
        gateway = dict(session.gateway)
        intents = _primitives._object(gateway.get("ownership_intents", {}))
        if not intents:
            return session
        transport = _primitives._object(gateway.get("transport", {}))
        changed = False
        scheduler_job_id = session.scheduler_job_id
        definitive_submission_failure: _types._DefinitiveSubmissionReconciliationError | None = None

        scheduler_intent = _primitives._object(intents.get("scheduler_submission", {}))
        if scheduler_job_id is None and scheduler_intent.get("state") == "recorded":
            recorded_scheduler_job_id = _primitives._optional_str(
                scheduler_intent.get("scheduler_job_id")
            )
            if recorded_scheduler_job_id is not None:
                scheduler_job_id = recorded_scheduler_job_id
                changed = True
        if (
            scheduler_job_id is None
            and scheduler_intent.get("state") == "starting"
            and scheduler_intent.get("reconciliation_outcome") != "definitive_failure"
        ):
            submission_id = _primitives._optional_str(scheduler_intent.get("submission_id"))
            scheduler_provider = _primitives._optional_str(
                scheduler_intent.get("scheduler_provider")
            )
            submission_marker = _primitives._optional_str(scheduler_intent.get("submission_marker"))
            if (
                submission_id is not None
                and scheduler_provider is not None
                and submission_marker is not None
            ):
                try:
                    record = _scheduler_contracts._last_json_object(
                        self._ssh(
                            _submission_scripts._remote_submission_record_script(
                                session_id=session.session_id,
                                submission_id=submission_id,
                                scheduler_provider=scheduler_provider,
                                submission_marker=submission_marker,
                            )
                        )
                    )
                    if (
                        record.get("schema_version")
                        == _submission_scripts._REMOTE_SUBMISSION_VERIFICATION_SCHEMA
                        and record.get("verification_outcome") == "definitive_invalid"
                    ):
                        reported_error = _primitives._optional_str(record.get("error"))
                        message = (
                            reported_error[:1024]
                            if reported_error is not None
                            else "scheduler submission sidecar failed integrity verification"
                        )
                        failure_kind: Literal["integrity_failure"] = "integrity_failure"
                        raise _types._DefinitiveSubmissionReconciliationError(
                            message,
                            evidence=_scheduler_contracts._submission_reconciliation_failure_evidence(
                                session_id=session.session_id,
                                submission_id=submission_id,
                                scheduler_provider=scheduler_provider,
                                submission_marker=submission_marker,
                                record=record,
                                error=message,
                                failure_kind=failure_kind,
                            ),
                            failure_kind=failure_kind,
                        )
                    if record.get("present") is True:
                        output = record.get("output")
                        if (
                            record.get("session_id") != session.session_id
                            or record.get("submission_id") != submission_id
                            or record.get("scheduler_provider") != scheduler_provider
                            or record.get("submission_marker") != submission_marker
                        ):
                            message = "scheduler submission sidecar identity is invalid"
                            raise _types._DefinitiveSubmissionReconciliationError(
                                message,
                                evidence=_scheduler_contracts._submission_reconciliation_failure_evidence(
                                    session_id=session.session_id,
                                    submission_id=submission_id,
                                    scheduler_provider=scheduler_provider,
                                    submission_marker=submission_marker,
                                    record=record,
                                    error=message,
                                    failure_kind="integrity_failure",
                                ),
                                failure_kind="integrity_failure",
                            )
                        returncode = record.get("returncode")
                        if isinstance(returncode, bool) or not isinstance(returncode, int):
                            message = "scheduler submission sidecar return code is invalid"
                            raise _types._DefinitiveSubmissionReconciliationError(
                                message,
                                evidence=_scheduler_contracts._submission_reconciliation_failure_evidence(
                                    session_id=session.session_id,
                                    submission_id=submission_id,
                                    scheduler_provider=scheduler_provider,
                                    submission_marker=submission_marker,
                                    record=record,
                                    error=message,
                                    failure_kind="integrity_failure",
                                ),
                                failure_kind="integrity_failure",
                            )
                        if returncode != 0:
                            message = (
                                "scheduler submission command completed unsuccessfully: "
                                f"returncode={returncode}"
                            )
                            raise _types._DefinitiveSubmissionReconciliationError(
                                message,
                                evidence=_scheduler_contracts._submission_reconciliation_failure_evidence(
                                    session_id=session.session_id,
                                    submission_id=submission_id,
                                    scheduler_provider=scheduler_provider,
                                    submission_marker=submission_marker,
                                    record=record,
                                    error=message,
                                    failure_kind="command_failure",
                                ),
                                failure_kind="command_failure",
                            )
                        if not isinstance(output, str):
                            message = "scheduler submission sidecar output is invalid"
                            raise _types._DefinitiveSubmissionReconciliationError(
                                message,
                                evidence=_scheduler_contracts._submission_reconciliation_failure_evidence(
                                    session_id=session.session_id,
                                    submission_id=submission_id,
                                    scheduler_provider=scheduler_provider,
                                    submission_marker=submission_marker,
                                    record=record,
                                    error=message,
                                    failure_kind="integrity_failure",
                                ),
                                failure_kind="integrity_failure",
                            )
                        try:
                            submission = _scheduler_contracts._parse_runtime_submission(output)
                        except RelayError as exc:
                            message = f"scheduler submission sidecar output is invalid: {exc}"
                            raise _types._DefinitiveSubmissionReconciliationError(
                                message,
                                evidence=_scheduler_contracts._submission_reconciliation_failure_evidence(
                                    session_id=session.session_id,
                                    submission_id=submission_id,
                                    scheduler_provider=scheduler_provider,
                                    submission_marker=submission_marker,
                                    record=record,
                                    error=message,
                                    failure_kind="response_invalid",
                                ),
                                failure_kind="response_invalid",
                            ) from exc
                        scheduler_job_id = submission.scheduler_job_id
                        intents["scheduler_submission"] = (
                            _scheduler_contracts._new_ownership_intent(
                                "recorded",
                                submission_id=submission_id,
                                scheduler_provider=scheduler_provider,
                                submission_marker=submission_marker,
                                scheduler_job_id=scheduler_job_id,
                                reconciled=True,
                            )
                        )
                        gateway["submit_output"] = output.strip()
                        changed = True
                except _types._DefinitiveSubmissionReconciliationError as exc:
                    failed_intent = dict(scheduler_intent)
                    failed_intent["reconciliation_error"] = str(exc)
                    failed_intent["reconciliation_outcome"] = "definitive_failure"
                    failed_intent["reconciliation_failure_kind"] = exc.failure_kind
                    failed_intent["failure_evidence"] = exc.evidence
                    intents["scheduler_submission"] = failed_intent
                    definitive_submission_failure = exc
                    changed = True
                except RelayError as exc:
                    unresolved_intent = dict(scheduler_intent)
                    unresolved_intent["reconciliation_error"] = str(exc)
                    unresolved_intent["reconciliation_outcome"] = "observation_unknown"
                    intents["scheduler_submission"] = unresolved_intent
                    changed = True

        remote_intent = _primitives._object(intents.get("remote_connector", {}))
        remote_record = _primitives._object(transport.get("remote_connector", {}))
        if remote_intent.get("state") in {"starting", "recorded"}:
            try:
                if remote_record:
                    self._validate_remote_connector_intent_binding(
                        session_id=session.session_id,
                        intent=remote_intent,
                        connector=remote_record,
                    )
                owner_token = _scheduler_contracts._required_intent_str(
                    remote_intent, "owner_token"
                )
                generation_id = _scheduler_contracts._required_intent_str(
                    remote_intent,
                    "connector_generation_id",
                )
                allocation_placement = _primitives._object(remote_intent.get("placement", {}))
                result = _scheduler_contracts._last_json_object(
                    self._ssh(
                        _connector_step_scripts._remote_connector_discovery_script(
                            session_id=session.session_id,
                            owner_token=owner_token,
                            connector_generation_id=generation_id,
                            allocation_provider=_primitives._optional_str(
                                remote_intent.get("scheduler_provider")
                            ),
                            allocation_job_id=_primitives._optional_str(
                                remote_intent.get("scheduler_native_id")
                            ),
                            allocation_step_marker=_primitives._optional_str(
                                remote_intent.get("scheduler_step_marker")
                            ),
                            allocation_placement_host=_primitives._optional_str(
                                allocation_placement.get("placement_host")
                            ),
                        )
                    )
                )
                connector = result.get("connector")
                verified_connector: dict[str, object] | None = None
                absence_verified = False
                if remote_intent.get("execution_scope") == "scheduler_allocation":
                    if result.get("ownership_verified") is not True:
                        detail = result.get("error")
                        raise RelayError(
                            detail
                            if isinstance(detail, str)
                            else "allocation connector sidecar could not be verified"
                        )
                    verified_connector, absence_verified = (
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
                elif (
                    result.get("ownership_verified") is True
                    and result.get("present") is True
                    and isinstance(connector, dict)
                ):
                    verified_connector = cast(dict[str, object], connector)
                elif (
                    result.get("ownership_verified") is True
                    and result.get("present") is False
                    and result.get("matching_pids") == []
                ):
                    absence_verified = True
                else:
                    detail = result.get("error")
                    raise RelayError(
                        detail
                        if isinstance(detail, str)
                        else "remote connector ownership observation was incomplete"
                    )
                if verified_connector is not None:
                    self._validate_remote_connector_intent_binding(
                        session_id=session.session_id,
                        intent=remote_intent,
                        connector=verified_connector,
                    )
                    remote_fields = (
                        "owner",
                        "session_id",
                        "pid",
                        "process_group_id",
                        "execution_scope",
                        "scheduler_provider",
                        "scheduler_native_id",
                        "scheduler_step_id",
                        "scheduler_step_marker",
                        "scheduler_step",
                        "connector_generation_id",
                        "owner_token",
                        "config_path",
                        "log_path",
                        "placement",
                    )
                    if remote_record and not self._connector_records_match(
                        remote_record,
                        verified_connector,
                        fields=remote_fields,
                    ):
                        raise RelayError(
                            "remote connector record disagrees with its live sidecar identity"
                        )
                    transport["remote_connector"] = verified_connector
                    intents["remote_connector"] = _scheduler_contracts._new_ownership_intent(
                        "recorded",
                        reconciled=True,
                        live_identity_verified=True,
                        **verified_connector,
                    )
                    changed = True
                elif absence_verified:
                    transport.pop("remote_connector", None)
                    intents["remote_connector"] = _scheduler_contracts._new_ownership_intent(
                        "absent_verified",
                        owner_token=owner_token,
                        connector_generation_id=generation_id,
                        execution_scope=remote_intent.get("execution_scope"),
                        scheduler_provider=remote_intent.get("scheduler_provider"),
                        scheduler_native_id=remote_intent.get("scheduler_native_id"),
                        scheduler_step_marker=remote_intent.get("scheduler_step_marker"),
                        placement=remote_intent.get("placement"),
                        reconciled=True,
                    )
                    changed = True
            except RelayError as exc:
                unresolved_remote = dict(remote_intent)
                unresolved_remote.pop("live_identity_verified", None)
                unresolved_remote["reconciliation_error"] = str(exc)
                intents["remote_connector"] = unresolved_remote
                changed = True
        elif remote_record:
            unresolved_remote = dict(remote_intent)
            unresolved_remote["reconciliation_error"] = (
                "remote connector record has no matching starting or recorded durable intent"
            )
            intents["remote_connector"] = unresolved_remote
            changed = True

        local_intent = _primitives._object(intents.get("desktop_connector", {}))
        local_record = _primitives._object(transport.get("desktop_connector", {}))
        if local_intent.get("state") in {"starting", "recorded"}:
            try:
                if local_record:
                    self._validate_local_connector_intent_binding(
                        session_id=session.session_id,
                        intent=local_intent,
                        connector=local_record,
                    )
                connector, absence_verified = _connector_identity._discover_local_connector(
                    local_intent,
                    session_id=session.session_id,
                )
                if connector is not None:
                    self._validate_local_connector_intent_binding(
                        session_id=session.session_id,
                        intent=local_intent,
                        connector=connector,
                    )
                    local_fields = (
                        "owner",
                        "session_id",
                        "pid",
                        "process_group_id",
                        "process_start_marker",
                        "owner_token",
                        "connector_generation_id",
                        "config_path",
                        "stdout_path",
                        "stderr_path",
                        "metadata_path",
                    )
                    if local_record and not self._connector_records_match(
                        local_record,
                        connector,
                        fields=local_fields,
                    ):
                        raise RelayError(
                            "desktop connector record disagrees with its live sidecar identity"
                        )
                    transport["desktop_connector"] = connector
                    intents["desktop_connector"] = _scheduler_contracts._new_ownership_intent(
                        "recorded",
                        reconciled=True,
                        live_identity_verified=True,
                        **connector,
                    )
                    changed = True
                elif absence_verified:
                    transport.pop("desktop_connector", None)
                    intents["desktop_connector"] = _scheduler_contracts._new_ownership_intent(
                        "absent_verified",
                        owner_token=local_intent.get("owner_token"),
                        connector_generation_id=local_intent.get("connector_generation_id"),
                        config_path=local_intent.get("config_path"),
                        stdout_path=local_intent.get("stdout_path"),
                        stderr_path=local_intent.get("stderr_path"),
                        metadata_path=local_intent.get("metadata_path"),
                        reconciled=True,
                    )
                    changed = True
            except RelayError as exc:
                unresolved_local = dict(local_intent)
                unresolved_local.pop("live_identity_verified", None)
                unresolved_local["reconciliation_error"] = str(exc)
                intents["desktop_connector"] = unresolved_local
                changed = True
        elif local_record:
            unresolved_local = dict(local_intent)
            unresolved_local["reconciliation_error"] = (
                "desktop connector record has no matching starting or recorded durable intent"
            )
            intents["desktop_connector"] = unresolved_local
            changed = True

        if not changed:
            return session
        gateway["ownership_intents"] = intents
        gateway["transport"] = transport
        if definitive_submission_failure is not None:
            return self._update(
                session,
                state=GatewaySessionState.FAILED,
                queue_state=definitive_submission_failure.queue_state,
                gateway=gateway,
                metadata={
                    "failed_at": utc_now().isoformat(),
                    "last_error": str(definitive_submission_failure),
                    "runtime_observation_error": str(definitive_submission_failure),
                    "scheduler_submission_outcome": (
                        definitive_submission_failure.scheduler_submission_outcome
                    ),
                },
            )
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
        provider_name = _scheduler_contracts._required_intent_str(intent, "scheduler_provider")
        scheduler_job_id = _scheduler_contracts._required_intent_str(intent, "scheduler_native_id")
        step_marker = _scheduler_contracts._required_intent_str(intent, "scheduler_step_marker")
        generation_id = _scheduler_contracts._required_intent_str(intent, "connector_generation_id")
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
            or step_marker != _scheduler_contracts._connector_step_marker(session_id, generation_id)
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
            or _primitives._optional_str(connector_base.get("config_path")) is None
            or _primitives._optional_str(connector_base.get("log_path")) is None
            or connector_base.get("pid") is not None
        ):
            raise RelayError("allocation connector sidecar identity does not match its intent")
        record = _scheduler_contracts._last_json_object(
            self._ssh(
                _connector_step_scripts._remote_connector_step_reconcile_script(
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
    ) -> _types._VerifiedSchedulerSubmission:
        """Prove the exact provider and job ID from the relay-created remote sidecar."""
        scheduler_job_id = _primitives._optional_str(session.scheduler_job_id)
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
            return _types._VerifiedSchedulerSubmission(
                provider=binding.scheduler_provider,
                scheduler_job_id=binding.scheduler_native_id,
                spec=spec,
            )
        intents = _primitives._object(session.gateway.get("ownership_intents", {}))
        scheduler_intent = _primitives._object(intents.get("scheduler_submission", {}))
        if (
            scheduler_intent.get("schema_version") != _primitives._OWNERSHIP_INTENT_SCHEMA
            or scheduler_intent.get("state") != "recorded"
        ):
            raise RelayError(
                "scheduler ownership is not backed by a recorded relay submission intent"
            )
        submission_id = _primitives._optional_str(scheduler_intent.get("submission_id"))
        intent_provider = _primitives._optional_str(scheduler_intent.get("scheduler_provider"))
        submission_marker = _primitives._optional_str(scheduler_intent.get("submission_marker"))
        intent_job_id = _primitives._optional_str(scheduler_intent.get("scheduler_job_id"))
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
        record = _scheduler_contracts._last_json_object(
            self._ssh(
                _submission_scripts._remote_submission_record_script(
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
        submission = _scheduler_contracts._parse_runtime_submission(output)
        if submission.scheduler_job_id != scheduler_job_id:
            raise RelayError("scheduler job identity disagrees with the anchored submission output")
        return _types._VerifiedSchedulerSubmission(
            provider=canonical_provider,
            scheduler_job_id=scheduler_job_id,
            spec=spec,
        )

    def _quiesced_owner_source_recovery_is_authorized(
        self,
        session: GatewaySession,
    ) -> bool:
        """Authorize a non-canceling direct source read for an exact closing owner."""
        teardown_intent = _primitives._object(session.gateway.get("teardown_intent", {}))
        owner_session_id = _primitives._optional_str(session.metadata.get("owner_session_id"))
        generation_id = _primitives._optional_str(
            session.metadata.get("owner_session_generation_id")
        )
        admission_id = _primitives._optional_str(session.metadata.get("owner_session_admission_id"))
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
        pid = _primitives._optional_int(connector.get("pid"))
        config_path = _primitives._optional_str(connector.get("config_path"))
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
        identity_status, identity_detail = _connector_identity._local_connector_identity_status(
            connector
        )
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
                no_group_members = not _connector_identity._local_connector_group_members(connector)
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
                and _primitives._optional_str(connector.get("owner_token")) is not None
                and _primitives._optional_int(connector.get("process_group_id")) is not None
                and _primitives._optional_str(connector.get("process_start_marker")) is not None
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
            stopped = _connector_identity._terminate_local_connector(connector)
            residual = bool(_connector_identity._local_connector_group_members(connector))
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
            raw_path = _primitives._optional_str(connector.get(field))
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

    def _observe_allocation_and_health_once(
        self,
        session: GatewaySession,
        spec: ServiceRuntimeSpec,
        scheduler_job_id: str,
        initial_service_host: str | None = None,
    ) -> str | None:
        """Make one bounded scheduler/runtime/health observation without waiting."""

        current_session = session
        provider_status: SchedulerStatus | None = None
        try:
            provider_status = (
                self._poll_scheduler_provider(
                    provider=spec.scheduler,
                    scheduler_job_id=scheduler_job_id,
                )
                if provider_for_scheduler(spec.scheduler).name != "external"
                else None
            )
        except ConfigurationError:
            raise
        except RelayError as exc:
            self._record_runtime_observation_pending(
                current_session,
                node=initial_service_host or current_session.node,
                error=exc,
                provider_status=None,
            )
            return None
        if provider_status is not None:
            provider_state = provider_status.phase.value
            if provider_state in _scheduler_contracts._TERMINAL_RUNTIME_STATES:
                raise _types._DefinitiveRuntimeObservationError(
                    "scheduler job reached a terminal state before the service became ready: "
                    f"job={scheduler_job_id} state={provider_state}"
                )
            if provider_status.phase is SchedulerPhase.UNKNOWN:
                self._record_runtime_observation_pending(
                    current_session,
                    node=initial_service_host or current_session.node,
                    error=RelayError(
                        "scheduler provider could not observe a current or terminal record for "
                        f"submitted job {scheduler_job_id}; absence is not terminal proof"
                    ),
                    provider_status=provider_status,
                )
                return None
            if provider_status.phase in {SchedulerPhase.SUBMITTED, SchedulerPhase.PENDING}:
                observed_gateway = dict(current_session.gateway)
                observed_gateway.pop("runtime_observation", None)
                self._update(
                    current_session,
                    state=GatewaySessionState.PENDING,
                    queue_state=provider_state,
                    node=None,
                    metadata={
                        "runtime_observation_error": None,
                        "runtime_observed_at": utc_now().isoformat(),
                    },
                    gateway={
                        **observed_gateway,
                        "scheduler_status": {
                            "raw": provider_status.model_dump_json(),
                            "state": provider_state,
                            "reason": provider_status.reason,
                            "provider": provider_status.model_dump(mode="json"),
                        },
                    },
                )
                return None
        try:
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
                    _submission_scripts._template_command_script(
                        spec.status_command, scheduler_job_id
                    )
                )
                status = _scheduler_contracts._parse_runtime_status(status_text)
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
        except ConfigurationError:
            raise
        except RelayError as exc:
            self._record_runtime_observation_pending(
                current_session,
                node=initial_service_host or current_session.node,
                error=exc,
                provider_status=provider_status,
            )
            return None

        last_status = status_text.strip()
        normalized_scheduler_state = (
            scheduler_state.strip().lower() if scheduler_state else "unknown"
        )
        if normalized_scheduler_state in _scheduler_contracts._TERMINAL_RUNTIME_STATES:
            raise _types._DefinitiveRuntimeObservationError(
                "scheduler job reached a terminal state before the service became ready: "
                f"job={scheduler_job_id} state={normalized_scheduler_state}"
            )
        state = GatewaySessionState.ALLOCATED if node is not None else GatewaySessionState.PENDING
        observed_gateway = dict(current_session.gateway)
        observed_gateway.pop("runtime_observation", None)
        current_session = self._update(
            current_session,
            state=state,
            queue_state=normalized_scheduler_state,
            node=node,
            metadata={
                "runtime_observation_error": None,
                "runtime_observed_at": utc_now().isoformat(),
            },
            gateway={
                **observed_gateway,
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
        if node is None:
            return None
        try:
            health = self._ssh(
                _connector_step_scripts._remote_http_probe_script(
                    node,
                    spec.service_port,
                    spec.health_path,
                    expected_body=spec.health_expected_body,
                )
            )
        except RelayError as exc:
            self._record_runtime_observation_pending(
                current_session,
                node=node,
                error=exc,
                provider_status=provider_status,
            )
            return None
        if "service_health=ok" in health:
            return node
        self._record_runtime_observation_pending(
            current_session,
            node=node,
            error=RelayError(
                "service health observation was not ready: "
                f"job={scheduler_job_id} output={health.strip()!r}"
            ),
            provider_status=provider_status,
        )
        return None

    def _record_runtime_observation_pending(
        self,
        session: GatewaySession,
        *,
        node: str | None,
        error: RelayError,
        provider_status: SchedulerStatus | None,
        state: GatewaySessionState | None = None,
        queue_state: str = "observation_unknown",
        preserve_scheduler_status: bool = False,
    ) -> GatewaySession:
        """Persist an inconclusive observation without failing the owned submission."""

        previous_status = _primitives._object(session.gateway.get("scheduler_status", {}))
        observed_at = utc_now().isoformat()
        gateway = {
            **session.gateway,
            "runtime_observation": {
                "state": "not_ready",
                "error": str(error),
                "observed_at": observed_at,
            },
        }
        if not preserve_scheduler_status:
            gateway["scheduler_status"] = {
                "raw": previous_status.get("raw", ""),
                "state": "observation_unknown",
                "reason": str(error),
                "provider": (
                    provider_status.model_dump(mode="json")
                    if provider_status is not None
                    else previous_status.get("provider")
                ),
            }
        return self._update(
            session,
            state=state
            or (GatewaySessionState.ALLOCATED if node is not None else GatewaySessionState.PENDING),
            queue_state=queue_state,
            node=node,
            metadata={
                "runtime_observation_error": str(error),
                "runtime_observed_at": observed_at,
            },
            gateway=gateway,
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
        scheduler_terminal = observed_state in _scheduler_contracts._TERMINAL_RUNTIME_STATES
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
        status_text = self._ssh(
            _submission_scripts._template_command_script(spec.status_command, scheduler_job_id)
        )
        status = _scheduler_contracts._parse_runtime_status(status_text)
        if status.state is None or not status.state.strip():
            raise RelayError(
                f"runtime status did not report a state for scheduler job {scheduler_job_id}"
            )
        normalized = status.state.strip().lower()
        if (
            normalized
            not in _scheduler_contracts._ACTIVE_RUNTIME_STATES
            | _scheduler_contracts._TERMINAL_RUNTIME_STATES
        ):
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
            if last_state in _scheduler_contracts._TERMINAL_RUNTIME_STATES:
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
            _submission_scripts._remote_scheduler_script(
                definition=self.definition,
                operation="status",
                provider=provider,
                scheduler_job_id=scheduler_job_id,
            )
        )
        try:
            status = SchedulerStatus.model_validate(_scheduler_contracts._last_json_object(output))
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
            _submission_scripts._remote_scheduler_script(
                definition=self.definition,
                operation="cancel",
                provider=provider,
                scheduler_job_id=scheduler_job_id,
            )
        )
        result = _scheduler_contracts._last_json_object(output)
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
            raw_placement = _scheduler_contracts._last_json_object(
                self._ssh(
                    _submission_scripts._remote_scheduler_script(
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
            step_marker = _scheduler_contracts._connector_step_marker(
                session.session_id,
                _scheduler_contracts._required_intent_str(
                    ownership_intent,
                    "connector_generation_id",
                ),
            )
            ownership_intent = _scheduler_contracts._new_ownership_intent(
                "starting",
                owner_token=_scheduler_contracts._required_intent_str(
                    ownership_intent, "owner_token"
                ),
                connector_generation_id=_scheduler_contracts._required_intent_str(
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
        server_addr = _primitives._require_server_addr(transport.server_addr, self.cluster)
        config = render_proxy_config(
            FrpLinkConfig(
                server_addr=server_addr,
                server_port=transport.server_port,
                protocol=FrpTransportProtocol(transport.protocol),
                token=self.token,
                secret_key=self.secret_key,
                proxy_name=proxy_name,
            ),
            proxy_type=_primitives._frp_proxy_type(spec.transport_mode),
            local_ip=node,
            local_port=spec.service_port,
        )
        owner_token = _scheduler_contracts._required_intent_str(ownership_intent, "owner_token")
        connector_generation_id = _scheduler_contracts._required_intent_str(
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
            start_result = _scheduler_contracts._last_json_object(output)
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
            config_path = _primitives._optional_str(start_result.get("config_path"))
            log_path = _primitives._optional_str(start_result.get("log_path"))
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
        metadata = _scheduler_contracts._key_value_output(output)
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
        config_path = _scheduler_contracts._validated_remote_session_file(
            metadata["remote_frpc_config"],
            session_id=session.session_id,
            filename="remote-frpc.toml",
        )
        log_path = _scheduler_contracts._validated_remote_session_file(
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
        generation_id = _primitives._optional_str(connector.get("connector_generation_id"))
        provider_name = _primitives._optional_str(connector.get("scheduler_provider"))
        scheduler_job_id = _primitives._optional_str(connector.get("scheduler_native_id"))
        scheduler_step_id = _primitives._optional_str(connector.get("scheduler_step_id"))
        step_marker = _primitives._optional_str(connector.get("scheduler_step_marker"))
        config_path = _primitives._optional_str(connector.get("config_path"))
        log_path = _primitives._optional_str(connector.get("log_path"))
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
            or step_marker != _scheduler_contracts._connector_step_marker(session_id, generation_id)
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
            _connector_step_scripts._remote_connector_step_status_script(
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
                    _scheduler_contracts._last_json_object(output),
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
                result = _scheduler_contracts._last_json_object(
                    self._ssh(
                        _connector_step_scripts._remote_connector_step_cancel_script(
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
        server_addr = _primitives._require_server_addr(transport.server_addr, self.cluster)
        runtime_dir = self.settings.core_dir.parent / "runtime-sessions" / session.session_id
        runtime_dir.mkdir(parents=True, exist_ok=True)
        config_path = Path(
            _scheduler_contracts._required_intent_str(ownership_intent, "config_path")
        ).resolve()
        stdout_path = Path(
            _scheduler_contracts._required_intent_str(ownership_intent, "stdout_path")
        ).resolve()
        stderr_path = Path(
            _scheduler_contracts._required_intent_str(ownership_intent, "stderr_path")
        ).resolve()
        metadata_path = Path(
            _scheduler_contracts._required_intent_str(ownership_intent, "metadata_path")
        ).resolve()
        owned_paths = (config_path, stdout_path, stderr_path, metadata_path)
        if any(path.parent != runtime_dir.resolve() for path in owned_paths):
            raise RelayError("desktop connector ownership intent escaped its runtime directory")
        owner_token = _scheduler_contracts._required_intent_str(ownership_intent, "owner_token")
        connector_generation_id = _scheduler_contracts._required_intent_str(
            ownership_intent,
            "connector_generation_id",
        )
        visitor_type = _primitives._frp_proxy_type(spec.transport_mode)
        visitor = start_owned_frp_visitor(
            frpc_bin=self.settings.frpc_bin,
            config=FrpLinkConfig(
                server_addr=server_addr,
                server_port=transport.server_port,
                protocol=FrpTransportProtocol(transport.protocol),
                token=self.token,
                secret_key=self.secret_key,
                proxy_name=proxy_name,
            ),
            local_bind_addr=spec.desktop_bind_addr,
            local_bind_port=spec.desktop_bind_port,
            visitor_type=visitor_type,
            keep_tunnel_open=visitor_type == "xtcp",
            config_path=config_path,
            stdout_path=stdout_path,
            stderr_path=stderr_path,
            owner_token=owner_token,
            connector_generation_id=connector_generation_id,
            command_prefix=[
                sys.executable,
                "-c",
                _LOCAL_CONNECTOR_WRAPPER_CODE,
                owner_token,
                connector_generation_id,
            ],
            process_factory=self.runner.popen,
            identity_factory=self.runner.local_process_identity,
            rollback=_primitives._terminate_just_started_process_group,
        )
        connector: dict[str, object] = {
            "owner": "clio-relay",
            "session_id": session.session_id,
            "pid": visitor.pid,
            "process_group_id": visitor.process_group_id,
            "process_start_marker": visitor.process_start_marker,
            "owner_token": visitor.owner_token,
            "connector_generation_id": connector_generation_id,
            "config_path": str(visitor.config_path),
            "stdout_path": str(visitor.stdout_path),
            "stderr_path": str(visitor.stderr_path),
            "metadata_path": str(metadata_path),
        }
        _connector_identity._write_local_connector_sidecar(metadata_path, connector)
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
        config_path = Path(
            _scheduler_contracts._required_intent_str(ownership_intent, "config_path")
        ).resolve()
        stdout_path = Path(
            _scheduler_contracts._required_intent_str(ownership_intent, "stdout_path")
        ).resolve()
        stderr_path = Path(
            _scheduler_contracts._required_intent_str(ownership_intent, "stderr_path")
        ).resolve()
        metadata_path = Path(
            _scheduler_contracts._required_intent_str(ownership_intent, "metadata_path")
        ).resolve()
        if any(
            path.parent != runtime_dir
            for path in (config_path, stdout_path, stderr_path, metadata_path)
        ):
            raise RelayError("browser proxy ownership intent escaped its runtime directory")
        temporary = config_path.with_suffix(f"{config_path.suffix}.{os.getpid()}.tmp")
        temporary.write_text(config.model_dump_json(indent=2) + "\n", encoding="utf-8")
        temporary.chmod(0o600)
        os.replace(temporary, config_path)
        owner_token = _scheduler_contracts._required_intent_str(ownership_intent, "owner_token")
        generation_id = _scheduler_contracts._required_intent_str(
            ownership_intent, "connector_generation_id"
        )
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
            _primitives._terminate_just_started_process_group(process.pid)
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
        _connector_identity._write_local_connector_sidecar(metadata_path, proxy)
        return proxy

    def _wait_for_jarvis_health(
        self,
        health_url: str,
        *,
        timeout_seconds: float,
        poll_seconds: float,
        runtime_schema_version: Literal["jarvis.service-runtime.v1", "jarvis.service-runtime.v2"],
        authorization: str | None,
        max_attempts: int | None = None,
    ) -> None:
        """Prove the versioned JARVIS HTTP authorization boundary is live."""
        if runtime_schema_version == JARVIS_SERVICE_RUNTIME_SCHEMA_V1:
            if authorization is not None:
                raise _types._DefinitiveRuntimeObservationError(
                    "legacy JARVIS service runtime unexpectedly resolved authorization"
                )
        elif runtime_schema_version == JARVIS_SERVICE_RUNTIME_SCHEMA_V2:
            if authorization is None:
                raise _types._DefinitiveRuntimeObservationError(
                    "authenticated JARVIS service runtime omitted authorization"
                )
        else:
            raise _types._DefinitiveRuntimeObservationError(
                "JARVIS service runtime schema is unsupported"
            )
        deadline = time.monotonic() + timeout_seconds
        last_error = "no response"
        attempts = 0
        while time.monotonic() < deadline:
            attempts += 1
            try:
                anonymous = _readiness._read_bounded_http_response(
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
                        raise _types._DefinitiveRuntimeObservationError(
                            "authenticated JARVIS service health accepted an anonymous request"
                        )
                    if anonymous.status_code != 401:
                        last_error = f"anonymous health status={anonymous.status_code}"
                    else:
                        authenticated = _readiness._read_bounded_http_response(
                            health_url,
                            headers={"Authorization": cast(str, authorization)},
                            maximum_bytes=None,
                            deadline=deadline,
                        )
                        if 200 <= authenticated.status_code < 300:
                            return
                        if authenticated.status_code in {401, 403}:
                            raise _types._DefinitiveRuntimeObservationError(
                                "authenticated JARVIS service rejected its verified authority"
                            )
                        last_error = f"authenticated health status={authenticated.status_code}"
            except httpx.HTTPError:
                last_error = "HTTP transport failed"
            if max_attempts is not None and attempts >= max_attempts:
                break
            _readiness._sleep_before_deadline(self.sleep, poll_seconds, deadline)
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
                response = _readiness._read_bounded_http_response(
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
            _readiness._sleep_before_deadline(self.sleep, poll_seconds, deadline)
        raise RelayError(f"browser capability gateway did not become ready: {last_error}")

    def _wait_for_local_health(
        self,
        health_url: str,
        timeout_seconds: float,
        poll_seconds: float,
        *,
        expected_body: str | None = None,
        max_attempts: int | None = None,
    ) -> None:
        deadline = time.monotonic() + timeout_seconds
        last_error: str | None = None
        attempts = 0
        while time.monotonic() < deadline:
            attempts += 1
            try:
                response = _readiness._read_bounded_http_response(
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
            if max_attempts is not None and attempts >= max_attempts:
                break
            _readiness._sleep_before_deadline(self.sleep, poll_seconds, deadline)
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
            raise _types._AmbiguousRemoteSideEffectError(
                "remote service runtime command timed out after "
                f"{_REMOTE_RUNTIME_COMMAND_TIMEOUT_SECONDS:g} seconds"
            ) from exc
        if result.returncode != 0:
            detail = result.stderr.strip() or result.stdout.strip()
            if result.returncode == 255:
                raise _types._AmbiguousRemoteSideEffectError(
                    f"remote service runtime transport failed: {detail}"
                )
            raise RelayError(f"remote service runtime command failed: {detail}")
        return result.stdout

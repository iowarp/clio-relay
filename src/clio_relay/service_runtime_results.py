"""Frozen result types the supervisor returns from start/resume/stop.

Extracted from ``service_runtime.py`` (#231 rework slice): the three
outcome dataclasses ``ServiceRuntimeStartResult``, ``ServiceRuntimePendingResult``,
and ``ServiceRuntimeStopResult``, each carrying its own
``to_live_validation_report`` conversion into the shared release-evidence
shape (``clio_relay.validation_report``, imported at function scope in every
method here to avoid a load-order cycle -- that module imports back from
this package's models, not from ``service_runtime.py`` itself, but the
deferral is kept consistent with the sibling methods that do need it), plus
the ten ``RUNTIME_*_CHECK_ID`` validation-check identifiers those
conversions stamp.

Depends on ``service_runtime_primitives`` (coercion helpers, the shared
ownership-intent schema) and ``service_runtime_scheduler_contracts``
(``_validated_durable_scheduler_contract``, the runtime-state classification
sets) -- never on the supervisor class, which imports these result types
back qualified through this module instead.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, cast

from clio_relay import service_runtime_primitives as _primitives
from clio_relay import service_runtime_scheduler_contracts as _scheduler_contracts
from clio_relay.errors import RelayError
from clio_relay.models import GatewaySession, GatewaySessionState, utc_now
from clio_relay.public_records import public_gateway_payload
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

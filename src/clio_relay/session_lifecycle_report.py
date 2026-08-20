"""Owned-session lifecycle report: wire model, canonical encoding, sidecar I/O.

Extracted from ``session_lifecycle.py`` (#231 rework slice): the machine-readable
detach/teardown report (``SessionLifecycleReport``, including its live-validation
projection), its two stdin request wrappers, the canonical bytes/digest
encoders, and the immutable coordinator-report sidecar primitives (name
derivation, reference construction, orphan pruning, verified read, and the
"does the full report extend the remote prefix" check the teardown finalizer
relies on).
"""

from __future__ import annotations

import hashlib
import hmac
import json
import re
from collections import Counter
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Literal, cast

from pydantic import BaseModel, ConfigDict, Field

from clio_relay.errors import RelayError
from clio_relay.identifiers import DurableRecordId, validate_durable_record_id
from clio_relay.session_wire_models import (
    MAX_OWNED_SESSION_CLEANUP_REPORT_BYTES,
    CleanupResource,
    OwnedSessionCleanupReportReference,
    RemoteSessionStateEvidence,
)

if TYPE_CHECKING:
    from clio_relay.session_transaction import _OwnedSessionTransaction
    from clio_relay.validation_report import (
        CleanupEvidence,
        LiveValidationReport,
        ValidationResource,
    )

SESSION_DETACH_CHECK_ID = "cleanup.detach"
SESSION_TEARDOWN_CHECK_ID = "cleanup.relay-session"
SESSION_CONNECTORS_CHECK_ID = "cleanup.connectors"
SESSION_GATEWAY_CHECK_ID = "cleanup.gateway-record"
SESSION_WORKER_CHECK_ID = "cleanup.worker-service"
SESSION_NO_RESIDUALS_CHECK_ID = "cleanup.no-owned-resources"
SESSION_SCHEDULER_RETAINED_CHECK_ID = "cleanup.jobs-preserved-default"
SESSION_RELAY_CANCELED_CHECK_ID = "cleanup.relay-jobs-canceled"
SESSION_SCHEDULER_CANCELED_CHECK_ID = "cleanup.explicit-job-cancel"
_CLEANUP_REPORT_SIDECAR_PATTERN = re.compile(r"^coordinator-cleanup-report-[0-9a-f]{64}\.json$")
_CLEANUP_REPORT_PENDING_PATTERN = re.compile(
    r"^\.coordinator-cleanup-report-[0-9a-f]{64}\.json\.pending$"
)


def cleanup_connectors_cover_gateways(
    connector_resources: list[CleanupResource],
    gateway_resources: list[CleanupResource],
    *,
    mode: Literal["detach", "teardown"],
) -> bool:
    """Require exactly one desktop and remote connector disposition per gateway."""
    gateway_counts = Counter(resource.resource_id for resource in gateway_resources)
    if not gateway_counts or any(count != 1 for count in gateway_counts.values()):
        return False
    connector_counts: Counter[tuple[str, str]] = Counter()
    for resource in connector_resources:
        gateway_id = resource.metadata.get("gateway_session_id")
        if not isinstance(gateway_id, str) or gateway_id not in gateway_counts:
            return False
        connector_counts[(gateway_id, resource.kind)] += 1
        if not (
            resource.ownership_verified
            and resource.verified_after_operation
            and not resource.residual
        ):
            return False
        if resource.kind == "desktop_connector":
            if resource.action != "stop" or resource.outcome not in {"stopped", "missing"}:
                return False
        elif resource.kind == "remote_connector":
            if mode == "detach":
                if resource.action != "retain" or resource.outcome != "retained":
                    return False
            elif resource.action != "stop" or resource.outcome not in {"stopped", "missing"}:
                return False
        else:
            return False
    expected = {
        (gateway_id, connector_kind): 1
        for gateway_id in gateway_counts
        for connector_kind in ("desktop_connector", "remote_connector")
    }
    return connector_counts == Counter(expected)


class SessionLifecycleReport(BaseModel):
    """Machine-readable detach or teardown report for an owned relay session."""

    model_config = ConfigDict(extra="forbid")

    cluster: str | None = None
    session_id: str
    session_generation_id: DurableRecordId | None = None
    mode: Literal["detach", "teardown"]
    cleanup_operation_id: DurableRecordId | None = None
    cleanup_policy: dict[str, bool] = Field(default_factory=dict[str, bool])
    relay_cancel_requested: bool = False
    scheduler_cancel_requested: bool = False
    prior_session_status: RemoteSessionStateEvidence | None = None
    post_session_status: RemoteSessionStateEvidence | None = None
    resources: list[CleanupResource] = Field(default_factory=list[CleanupResource])
    errors: list[str] = Field(default_factory=list)

    @property
    def residual_resources(self) -> list[CleanupResource]:
        """Return resources that remain after a requested destructive action."""
        return [resource for resource in self.resources if resource.residual]

    def json_payload(self) -> dict[str, object]:
        """Return the report with an explicit residual-resource summary."""
        payload = self.model_dump(mode="json")
        payload["residual_resources"] = [
            resource.model_dump(mode="json") for resource in self.residual_resources
        ]
        payload["validation_resources"] = [
            resource.model_dump(mode="json") for resource in self.validation_resources()
        ]
        payload["cleanup_evidence"] = self.to_cleanup_evidence().model_dump(mode="json")
        payload["ok"] = not self.errors and not self.residual_resources
        return payload

    def validation_resources(self) -> list[ValidationResource]:
        """Return all lifecycle resources in the shared validation-report shape."""
        from clio_relay.validation_report import ValidationResource

        resources: list[ValidationResource] = []
        generation_id = self.session_generation_id
        stable_session_id = (
            f"{self.session_id}:{generation_id}" if generation_id is not None else self.session_id
        )
        for resource in self.resources:
            if resource.kind != "remote_relay_api":
                resources.append(resource.to_validation_resource(cluster=self.cluster))
                continue
            resources.append(
                ValidationResource(
                    kind="relay_session",
                    resource_id=stable_session_id,
                    role=f"{resource.kind}:{resource.action}",
                    cluster=self.cluster,
                    state=resource.outcome,
                    references=[resource.location],
                    metadata={
                        "session_id": self.session_id,
                        "session_generation_id": generation_id,
                        "api_pid": resource.resource_id,
                        "ownership_verified": resource.ownership_verified,
                        "verified_after_operation": resource.verified_after_operation,
                        "residual": resource.residual,
                        "detail": resource.detail,
                        **resource.metadata,
                    },
                )
            )
            resources.append(
                ValidationResource(
                    kind="relay_process",
                    resource_id=resource.resource_id,
                    role="remote_relay_api_process",
                    cluster=self.cluster,
                    state=resource.outcome,
                    references=[resource.location],
                    metadata={
                        "session_id": self.session_id,
                        "session_generation_id": generation_id,
                        "ownership_verified": resource.ownership_verified,
                        "verified_after_operation": resource.verified_after_operation,
                        "residual": resource.residual,
                        **resource.metadata,
                    },
                )
            )
        return resources

    def to_cleanup_evidence(self, *, stop_worker: bool | None = None) -> CleanupEvidence:
        """Convert this lifecycle result to shared cleanup evidence."""
        from clio_relay.validation_report import CleanupEvidence

        effective_stop_worker = (
            any(
                resource.kind == "worker_service" and resource.action == "stop"
                for resource in self.resources
            )
            if stop_worker is None
            else stop_worker
        )
        return CleanupEvidence(
            requested=True,
            mode=self.mode,
            operation_id=self.cleanup_operation_id,
            cancel_relay_jobs=self.relay_cancel_requested,
            cancel_scheduler_jobs=self.scheduler_cancel_requested,
            stop_worker=effective_stop_worker,
            actions=[resource.model_dump(mode="json") for resource in self.resources],
            remaining_resources=[
                resource.to_validation_resource(cluster=self.cluster)
                for resource in self.residual_resources
            ],
        )

    def to_live_validation_report(
        self,
        *,
        stop_worker: bool | None = None,
        cancel_jobs: bool | None = None,
        launcher: str | None = None,
        install_source: str | None = None,
        artifact_sha256: str | None = None,
    ) -> LiveValidationReport:
        """Convert one live lifecycle operation to canonical release evidence."""
        from clio_relay.validation_report import (
            EvidenceReference,
            ValidationCheck,
            ValidationStatus,
            new_live_validation_report,
        )

        cluster = self.cluster or "unknown"
        report = new_live_validation_report(
            scenario="cleanup",
            cluster=cluster,
            launcher=launcher,
            install_source=install_source,
            artifact_sha256=artifact_sha256,
        )
        effective_stop_worker = (
            any(
                resource.kind == "worker_service" and resource.action == "stop"
                for resource in self.resources
            )
            if stop_worker is None
            else stop_worker
        )
        effective_cancel_jobs = self.relay_cancel_requested if cancel_jobs is None else cancel_jobs
        completed_at = datetime.now(UTC)
        checks: list[tuple[str, str, bool]] = []
        relay_stopped = False
        if self.mode == "detach":
            relay_resources = [
                resource for resource in self.resources if resource.kind == "remote_relay_api"
            ]
            retained = len(relay_resources) == 1 and all(
                resource.action == "retain"
                and resource.outcome == "retained"
                and resource.ownership_verified
                and resource.verified_after_operation
                and not resource.residual
                for resource in relay_resources
            )
            checks.append(
                (
                    SESSION_DETACH_CHECK_ID,
                    "detach retained the owned session and removed desktop resources",
                    retained
                    and self.session_generation_id is not None
                    and not self.errors
                    and not self.residual_resources,
                )
            )
        else:
            relay_resources = [
                resource for resource in self.resources if resource.kind == "remote_relay_api"
            ]
            prior = self.prior_session_status
            post = self.post_session_status
            linked_pid = None if prior is None or prior.api_pid is None else str(prior.api_pid)
            failed_start_without_api_pid = bool(
                prior is not None
                and prior.api_pid is None
                and relay_resources
                and all(
                    resource.metadata.get("failed_start") is True
                    and resource.resource_id == "failed-start"
                    for resource in relay_resources
                )
            )
            relay_stopped = (
                prior is not None
                and prior.ownership_verified
                and post is not None
                and post.api_pid == prior.api_pid
                and not post.running
                and bool(relay_resources)
                and all(
                    resource.outcome in {"stopped", "missing"}
                    and resource.ownership_verified
                    and (resource.resource_id == linked_pid or failed_start_without_api_pid)
                    and resource.verified_after_operation
                    and not resource.residual
                    for resource in relay_resources
                )
            )
            checks.append((SESSION_TEARDOWN_CHECK_ID, "owned relay session stopped", relay_stopped))
        if effective_cancel_jobs:
            relay_cancel_resources = [
                resource for resource in self.resources if resource.kind == "relay_job"
            ]
            if relay_cancel_resources:
                checks.append(
                    (
                        SESSION_RELAY_CANCELED_CHECK_ID,
                        "owned relay jobs reached acknowledged cancellation or terminal state",
                        all(
                            resource.action in {"cancel", "retain"}
                            and resource.ownership_verified
                            and resource.outcome in {"canceled", "terminal"}
                            and resource.verified_after_operation
                            and not resource.residual
                            for resource in relay_cancel_resources
                        ),
                    )
                )
        retained_jobs = [
            resource
            for resource in self.resources
            if resource.action == "retain"
            and (
                resource.kind == "scheduler_job"
                or (resource.kind == "relay_job" and not effective_cancel_jobs)
            )
        ]
        if not self.scheduler_cancel_requested and retained_jobs:
            relay_resource_ids = {
                resource.resource_id for resource in self.resources if resource.kind == "relay_job"
            }
            gateway_resource_ids = {
                resource.resource_id
                for resource in self.resources
                if resource.kind == "gateway_record"
            }
            allowed_retention_outcomes = (
                {"retained"} if self.mode == "detach" else {"retained", "terminal", "missing"}
            )
            checks.append(
                (
                    SESSION_SCHEDULER_RETAINED_CHECK_ID,
                    (
                        "scheduler jobs were preserved while relay cancellation completed"
                        if effective_cancel_jobs
                        else "owned relay and scheduler jobs were preserved by default"
                    ),
                    all(
                        resource.ownership_verified
                        and (
                            resource.kind != "scheduler_job"
                            or (
                                resource.provider is not None
                                and (
                                    resource.metadata.get("relay_job_id") in relay_resource_ids
                                    or resource.metadata.get("gateway_session_id")
                                    in gateway_resource_ids
                                )
                            )
                        )
                        and resource.outcome in allowed_retention_outcomes
                        and (
                            self.mode != "detach"
                            or resource.observed_state
                            in {
                                "submitted",
                                "pending",
                                "queued",
                                "allocated",
                                "starting",
                                "ready",
                                "running",
                            }
                        )
                        and resource.verified_after_operation
                        and not resource.residual
                        for resource in retained_jobs
                    ),
                )
            )
        if self.scheduler_cancel_requested:
            relay_resources = {
                resource.resource_id: resource
                for resource in self.resources
                if resource.kind == "relay_job"
                and (
                    resource.action == "cancel"
                    or (resource.action == "retain" and resource.outcome == "terminal")
                )
            }
            scheduler_ids_by_relay: dict[str, list[object]] = {}
            for relay_id, resource in relay_resources.items():
                raw_scheduler_ids = resource.metadata.get("scheduler_job_ids")
                scheduler_ids_by_relay[relay_id] = (
                    cast(list[object], raw_scheduler_ids)
                    if isinstance(raw_scheduler_ids, list)
                    else []
                )
            expected_scheduler_links = {
                (relay_id, scheduler_id)
                for relay_id, scheduler_ids in scheduler_ids_by_relay.items()
                for scheduler_id in scheduler_ids
                if isinstance(scheduler_id, str)
            }
            canceled_scheduler_resources = [
                resource
                for resource in self.resources
                if resource.kind == "scheduler_job" and resource.action == "cancel"
            ]
            observed_scheduler_links = {
                (relay_id, resource.resource_id)
                for resource in canceled_scheduler_resources
                if isinstance((relay_id := resource.metadata.get("relay_job_id")), str)
                and resource.outcome == "canceled"
                and resource.ownership_verified
                and resource.verified_after_operation
                and not resource.residual
            }
            gateway_resource_ids = {
                resource.resource_id
                for resource in self.resources
                if resource.kind == "gateway_record"
            }
            every_scheduler_resource_linked = all(
                (
                    isinstance(resource.metadata.get("relay_job_id"), str)
                    and resource.metadata.get("relay_job_id") in relay_resources
                )
                or (
                    isinstance(resource.metadata.get("gateway_session_id"), str)
                    and resource.metadata.get("gateway_session_id") in gateway_resource_ids
                )
                for resource in canceled_scheduler_resources
            )
            scheduler_canceled = (
                every_scheduler_resource_linked
                and expected_scheduler_links == observed_scheduler_links
                and all(
                    resource.outcome == "canceled"
                    and resource.ownership_verified
                    and resource.verified_after_operation
                    and not resource.residual
                    for resource in canceled_scheduler_resources
                )
            )
            checks.append(
                (
                    SESSION_SCHEDULER_CANCELED_CHECK_ID,
                    "explicit scheduler cancellation completed",
                    scheduler_canceled,
                )
            )
        gateway_resources = [
            resource for resource in self.resources if resource.kind == "gateway_record"
        ]
        connector_resources = [
            resource
            for resource in self.resources
            if resource.kind in {"desktop_connector", "remote_connector"}
        ]
        if self.mode == "detach" and (connector_resources or gateway_resources):
            checks.append(
                (
                    SESSION_CONNECTORS_CHECK_ID,
                    "desktop connectors stopped and remote connectors retained",
                    cleanup_connectors_cover_gateways(
                        connector_resources,
                        gateway_resources,
                        mode="detach",
                    ),
                )
            )
        elif self.mode == "teardown" and (connector_resources or gateway_resources):
            checks.append(
                (
                    SESSION_CONNECTORS_CHECK_ID,
                    "owned connectors were cleaned",
                    cleanup_connectors_cover_gateways(
                        connector_resources,
                        gateway_resources,
                        mode="teardown",
                    ),
                )
            )
        if self.mode == "detach" and gateway_resources:
            checks.append(
                (
                    SESSION_GATEWAY_CHECK_ID,
                    "owned gateway records were retained for reattachment",
                    all(
                        resource.action == "retain"
                        and resource.outcome == "retained"
                        and resource.ownership_verified
                        and resource.verified_after_operation
                        and not resource.residual
                        for resource in gateway_resources
                    ),
                )
            )
        elif self.mode == "teardown" and gateway_resources:
            checks.append(
                (
                    SESSION_GATEWAY_CHECK_ID,
                    "owned gateway records were closed or detached",
                    all(
                        resource.action == "close"
                        and resource.outcome == "closed"
                        and resource.ownership_verified
                        and resource.verified_after_operation
                        and not resource.residual
                        for resource in gateway_resources
                    ),
                )
            )
        worker_resources = [
            resource for resource in self.resources if resource.kind == "worker_service"
        ]
        if self.mode == "teardown" and effective_stop_worker:
            checks.append(
                (
                    SESSION_WORKER_CHECK_ID,
                    "owned worker service reached a proven inactive state",
                    len(worker_resources) == 1
                    and all(
                        resource.action == "stop"
                        and resource.outcome in {"stopped", "missing"}
                        and resource.ownership_verified
                        and resource.verified_after_operation
                        and resource.observed_state in {"inactive", "not-found"}
                        and not resource.residual
                        for resource in worker_resources
                    ),
                )
            )
        if self.mode == "teardown":
            checks.append(
                (
                    SESSION_NO_RESIDUALS_CHECK_ID,
                    "no requested owned resources remain",
                    relay_stopped and not self.errors and not self.residual_resources,
                )
            )
        report.checks = [
            ValidationCheck(
                check_id=check_id,
                summary=summary,
                status=ValidationStatus.PASSED if passed else ValidationStatus.FAILED,
                started_at=report.started_at,
                completed_at=completed_at,
                evidence=[
                    EvidenceReference(
                        kind="cleanup",
                        excerpt=summary,
                        metadata=self.json_payload(),
                    )
                ],
                error=None if passed else summary,
            )
            for check_id, summary, passed in checks
        ]
        report.resources = self.validation_resources()
        report.cleanup = self.to_cleanup_evidence(stop_worker=effective_stop_worker)
        report.completed_at = completed_at
        report.status = (
            ValidationStatus.PASSED
            if report.checks
            and all(check.status is ValidationStatus.PASSED for check in report.checks)
            else ValidationStatus.FAILED
        )
        report.error = None if report.status is ValidationStatus.PASSED else "cleanup failed"
        return report


class OwnedSessionCleanupFinalizeRequest(BaseModel):
    """Exact stdin contract for binding a fully verified cleanup report."""

    model_config = ConfigDict(extra="forbid")

    cluster: str = Field(min_length=1)
    session_id: str = Field(min_length=1)
    expected_session_generation_id: DurableRecordId
    expected_cleanup_operation_id: DurableRecordId
    expected_cleanup_policy: dict[str, bool]
    coordinator_report: SessionLifecycleReport
    coordinator_report_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class OwnedSessionCleanupReportReadRequest(BaseModel):
    """Exact request for reading one finalized coordinator-report sidecar."""

    model_config = ConfigDict(extra="forbid")

    cluster: str = Field(min_length=1)
    session_id: str = Field(min_length=1)
    expected_session_generation_id: DurableRecordId
    coordinator_report_ref: OwnedSessionCleanupReportReference


def session_lifecycle_report_bytes(report: SessionLifecycleReport) -> bytes:
    """Return the canonical bounded sidecar encoding for one lifecycle report."""
    payload = json.dumps(
        report.model_dump(mode="json"),
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    if not payload or len(payload) > MAX_OWNED_SESSION_CLEANUP_REPORT_BYTES:
        raise RelayError("coordinator cleanup report exceeds its byte limit")
    return payload


def session_lifecycle_report_sha256(report: SessionLifecycleReport) -> str:
    """Return the canonical digest for an exact lifecycle report."""
    return hashlib.sha256(session_lifecycle_report_bytes(report)).hexdigest()


def _coordinator_report_sidecar_name(
    *,
    session_generation_id: str,
    cleanup_operation_id: str,
) -> str:
    """Derive a stable basename without exposing variable-length identifiers."""
    validate_durable_record_id(session_generation_id)
    validate_durable_record_id(cleanup_operation_id)
    identity = (
        "clio-relay.owner-session-cleanup-report.v1\0"
        f"{session_generation_id}\0{cleanup_operation_id}"
    ).encode("ascii")
    return f"coordinator-cleanup-report-{hashlib.sha256(identity).hexdigest()}.json"


def _coordinator_report_reference(
    report: SessionLifecycleReport,
) -> tuple[OwnedSessionCleanupReportReference, bytes]:
    """Build the exact immutable sidecar reference and canonical payload."""
    generation_id = report.session_generation_id
    operation_id = report.cleanup_operation_id
    if generation_id is None or operation_id is None:
        raise RelayError("coordinator cleanup report omitted its durable identity")
    payload = session_lifecycle_report_bytes(report)
    reference = OwnedSessionCleanupReportReference(
        name=_coordinator_report_sidecar_name(
            session_generation_id=generation_id,
            cleanup_operation_id=operation_id,
        ),
        size=len(payload),
        sha256=hashlib.sha256(payload).hexdigest(),
    )
    return reference, payload


def _prune_unreferenced_cleanup_report_sidecars(
    transaction: _OwnedSessionTransaction,
    *,
    preserve_names: set[str],
) -> None:
    """Remove at most one proven orphan while preserving the in-flight publication."""
    candidates = transaction.cleanup_report_candidate_names()
    unexpected_pending = [
        name
        for name in candidates
        if _CLEANUP_REPORT_PENDING_PATTERN.fullmatch(name) and name not in preserve_names
    ]
    if unexpected_pending:
        raise RelayError(
            "owned session has an unreferenced cleanup report pending file: "
            + ", ".join(unexpected_pending)
        )
    orphan_names = [
        name
        for name in candidates
        if _CLEANUP_REPORT_SIDECAR_PATTERN.fullmatch(name) and name not in preserve_names
    ]
    if len(orphan_names) > 1:
        raise RelayError("owned session has multiple unreferenced cleanup report sidecars")
    for name in orphan_names:
        linked = transaction.stat_regular(name)
        if linked is None:  # pragma: no cover - required stat
            raise RelayError(f"owned session cleanup report sidecar disappeared: {name}")
        if not 0 < linked.st_size <= MAX_OWNED_SESSION_CLEANUP_REPORT_BYTES:
            raise RelayError(f"owned session cleanup report sidecar has an invalid size: {name}")
        transaction.unlink_verified(
            name,
            expected_device=linked.st_dev,
            expected_inode=linked.st_ino,
            expected_size=linked.st_size,
            expected_sha256=None,
            maximum_bytes=None,
        )
    remaining = set(transaction.cleanup_report_candidate_names())
    if not remaining.issubset(preserve_names):
        raise RelayError("owned session cleanup report sidecar pruning was not exact")


def _read_coordinator_report_sidecar(
    transaction: _OwnedSessionTransaction,
    reference: OwnedSessionCleanupReportReference,
    *,
    expected_session_generation_id: str,
    expected_cleanup_operation_id: str,
) -> SessionLifecycleReport:
    """Read and verify one exact coordinator report through the pinned dirfd."""
    expected_name = _coordinator_report_sidecar_name(
        session_generation_id=expected_session_generation_id,
        cleanup_operation_id=expected_cleanup_operation_id,
    )
    if reference.name != expected_name:
        raise RelayError("coordinator cleanup report sidecar name does not match its identity")
    payload = transaction.read_bytes(
        reference.name,
        maximum_bytes=reference.size,
    )
    if payload is None:  # pragma: no cover - required read
        raise RelayError("coordinator cleanup report sidecar is unavailable")
    if len(payload) != reference.size:
        raise RelayError("coordinator cleanup report sidecar size does not match its reference")
    if not hmac.compare_digest(hashlib.sha256(payload).hexdigest(), reference.sha256):
        raise RelayError("coordinator cleanup report sidecar digest does not match its reference")
    try:
        report = SessionLifecycleReport.model_validate_json(payload)
    except ValueError as exc:
        raise RelayError(f"coordinator cleanup report sidecar is invalid: {exc}") from exc
    if not hmac.compare_digest(session_lifecycle_report_bytes(report), payload):
        raise RelayError("coordinator cleanup report sidecar is not canonically encoded")
    return report


def _coordinator_report_extends_remote_report(
    report: SessionLifecycleReport,
    remote_report: SessionLifecycleReport,
) -> bool:
    """Return whether the full coordinator report preserves the remote prefix exactly."""
    return bool(
        report.cluster == remote_report.cluster
        and report.session_id == remote_report.session_id
        and report.session_generation_id == remote_report.session_generation_id
        and report.mode == remote_report.mode
        and report.cleanup_operation_id == remote_report.cleanup_operation_id
        and report.cleanup_policy == remote_report.cleanup_policy
        and report.relay_cancel_requested == remote_report.relay_cancel_requested
        and report.scheduler_cancel_requested == remote_report.scheduler_cancel_requested
        and report.prior_session_status == remote_report.prior_session_status
        and report.post_session_status == remote_report.post_session_status
        and len(report.resources) >= len(remote_report.resources)
        and report.resources[: len(remote_report.resources)] == remote_report.resources
    )

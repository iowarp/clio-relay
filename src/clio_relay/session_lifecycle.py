"""Owned remote relay session lifecycle helpers."""

from __future__ import annotations

import json
import shlex
import subprocess
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Literal, cast

from pydantic import BaseModel, ConfigDict, Field

from clio_relay.cluster_config import ClusterDefinition
from clio_relay.errors import RelayError
from clio_relay.identifiers import DurableRecordId, validate_durable_record_id
from clio_relay.remote_cli import remote_env

if TYPE_CHECKING:
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
_REMOTE_SESSION_COMMAND_TIMEOUT_SECONDS = 120.0
_REMOTE_API_READINESS_TIMEOUT_SECONDS = 60.0


@dataclass(frozen=True)
class RemoteSession:
    """A remotely owned relay session."""

    session_id: str
    remote_api_port: int
    api_token: str | None


class CleanupResource(BaseModel):
    """Machine-readable result for one lifecycle-owned resource."""

    model_config = ConfigDict(extra="forbid")

    kind: str
    resource_id: str
    location: str
    action: Literal["retain", "stop", "close", "cancel"]
    ownership_verified: bool
    outcome: Literal[
        "retained",
        "stopped",
        "closed",
        "canceled",
        "terminal",
        "missing",
        "refused",
        "failed",
    ]
    provider: str | None = None
    verified_after_operation: bool = False
    observed_state: str | None = None
    residual: bool = False
    detail: str | None = None
    metadata: dict[str, object] = Field(default_factory=dict)

    def to_validation_resource(self, *, cluster: str | None) -> ValidationResource:
        """Convert this cleanup result to canonical live-validation resource evidence."""
        from clio_relay.validation_report import ValidationResource

        validation_kind = {
            "remote_relay_api": "relay_session",
            "desktop_connector": "connector",
            "remote_connector": "connector",
            "gateway_record": "gateway_session",
            "worker_service": "relay_worker",
            "scheduler_sentinel": "scheduler_job",
        }.get(self.kind, self.kind)
        return ValidationResource(
            kind=validation_kind,
            resource_id=self.resource_id,
            role=f"{self.kind}:{self.action}",
            cluster=cluster,
            state=self.outcome,
            provider=self.provider,
            references=[self.location],
            metadata={
                "ownership_verified": self.ownership_verified,
                "cleanup_kind": self.kind,
                "provider": self.provider,
                "verified_after_operation": self.verified_after_operation,
                "observed_state": self.observed_state,
                "residual": self.residual,
                "detail": self.detail,
                **self.metadata,
            },
        )


class RemoteSessionStateEvidence(BaseModel):
    """Observed state linked to a remote session API lifecycle operation."""

    model_config = ConfigDict(extra="forbid")

    api_pid: int | None = None
    session_generation_id: DurableRecordId | None = None
    process_start_marker: str | None = None
    running: bool
    ownership_verified: bool
    observed_at: datetime
    started_at: datetime | None = None


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
                    and resource.resource_id == linked_pid
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
                {"retained"} if self.mode == "detach" else {"retained", "terminal"}
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


def start_remote_session(
    *,
    cluster: str,
    definition: ClusterDefinition,
    session_id: str,
    remote_api_port: int,
    api_token: str | None,
    replace: bool = False,
) -> list[str]:
    """Start a cluster-side relay API owned by a session id."""
    _validate_session(session_id=session_id, remote_api_port=remote_api_port)
    result = _ssh_script(
        definition,
        _start_script(
            cluster=cluster,
            definition=definition,
            session_id=session_id,
            remote_api_port=remote_api_port,
            api_token=api_token,
            replace=replace,
        ),
    )
    return result.splitlines()


def status_remote_session(
    *,
    definition: ClusterDefinition,
    session_id: str,
) -> dict[str, object]:
    """Return status for a previously started remote relay session."""
    _validate_session(session_id=session_id, remote_api_port=1)
    output = _ssh_script(definition, _owned_status_script(session_id=session_id))
    return cast(dict[str, object], json.loads(output))


def challenge_remote_session_identity(
    *,
    definition: ClusterDefinition,
    session_id: str,
    session_generation_id: DurableRecordId,
    nonce: str,
) -> dict[str, object]:
    """Return an SSH-authenticated HMAC challenge for one live session API."""
    _validate_session(session_id=session_id, remote_api_port=1)
    validate_durable_record_id(session_generation_id)
    if len(nonce) != 64 or any(character not in "0123456789abcdef" for character in nonce):
        raise ValueError("session identity nonce must be a lowercase 256-bit hexadecimal value")
    output = _ssh_script(
        definition,
        _owned_identity_challenge_script(
            cluster=definition.name,
            session_id=session_id,
            session_generation_id=session_generation_id,
            nonce=nonce,
        ),
    )
    return cast(dict[str, object], json.loads(output))


def teardown_remote_session(
    *,
    definition: ClusterDefinition,
    session_id: str,
    expected_session_generation_id: str,
    expected_cleanup_operation_id: str | None = None,
    stop_worker: bool = False,
    cancel_jobs: bool = False,
    cancel_scheduler_jobs: bool = False,
    cluster: str | None = None,
) -> SessionLifecycleReport:
    """Stop processes owned by a remote relay session."""
    _validate_session(session_id=session_id, remote_api_port=1)
    _validate_durable_session_identity(
        expected_session_generation_id,
        field="expected_session_generation_id",
    )
    if expected_cleanup_operation_id is not None:
        _validate_durable_session_identity(
            expected_cleanup_operation_id,
            field="expected_cleanup_operation_id",
        )
    output = _ssh_script(
        definition,
        _owned_teardown_script(
            definition=definition,
            session_id=session_id,
            expected_session_generation_id=expected_session_generation_id,
            stop_worker=stop_worker,
            cancel_jobs=cancel_jobs,
            cancel_scheduler_jobs=cancel_scheduler_jobs,
            cluster=cluster,
        ),
    )
    report = SessionLifecycleReport.model_validate_json(output)
    if expected_cleanup_operation_id is not None:
        if report.cleanup_operation_id != expected_cleanup_operation_id:
            raise RelayError(
                "remote teardown cleanup operation does not match the durable owner-session intent"
            )
        expected_policy = {
            "stop_worker": stop_worker,
            "cancel_jobs": cancel_jobs,
            "cancel_scheduler_jobs": cancel_scheduler_jobs,
        }
        if report.cleanup_policy != expected_policy:
            raise RelayError(
                "remote teardown cleanup policy does not match the durable owner-session intent"
            )
        if (
            report.relay_cancel_requested is not cancel_jobs
            or report.scheduler_cancel_requested is not cancel_scheduler_jobs
        ):
            raise RelayError(
                "remote teardown cancellation evidence does not match the durable owner-session "
                "intent"
            )
    return report


def detach_remote_session(
    *,
    definition: ClusterDefinition,
    session_id: str,
    cluster: str | None = None,
) -> SessionLifecycleReport:
    """Detach the desktop while intentionally retaining the remote session."""
    status = status_remote_session(definition=definition, session_id=session_id)
    pid = status.get("api_pid")
    running = status.get("running") is True
    ownership_verified = status.get("ownership_verified") is True
    identity_verified = status.get("session_id") == session_id
    generation_id = status.get("session_generation_id")
    generation_verified = isinstance(generation_id, str) and bool(generation_id)
    retained = running and ownership_verified and identity_verified and generation_verified
    resource_id = str(pid) if isinstance(pid, int) else session_id
    if retained:
        outcome: Literal["retained", "missing", "refused"] = "retained"
        detail = "remote relay session intentionally retained for reattachment"
    elif not running:
        outcome = "missing"
        detail = "remote relay API was not running after detach"
    else:
        outcome = "refused"
        detail = "remote relay API retention could not be tied to the requested owned generation"
    return SessionLifecycleReport(
        cluster=cluster,
        session_id=session_id,
        session_generation_id=str(generation_id) if generation_verified else None,
        mode="detach",
        resources=[
            CleanupResource(
                kind="remote_relay_api",
                resource_id=resource_id,
                location=definition.ssh_host,
                action="retain",
                ownership_verified=ownership_verified and identity_verified,
                outcome=outcome,
                verified_after_operation=retained,
                residual=not retained,
                detail=detail,
            )
        ],
        errors=[] if retained else [detail],
    )


def _start_script(
    *,
    cluster: str,
    definition: ClusterDefinition,
    session_id: str,
    remote_api_port: int,
    api_token: str | None,
    replace: bool,
) -> str:
    token_export = ""
    require_token = ""
    if api_token is not None:
        token_export = f"export CLIO_RELAY_API_TOKEN={_shell_single_quote(api_token)}"
        require_token = " --require-token"
    replace_flag = "1" if replace else "0"
    return f"""set -euo pipefail
umask 077
{remote_env(definition)}
{token_export}
session_id={shlex.quote(session_id)}
session_dir="$HOME/.local/share/clio-relay/sessions/$session_id"
mkdir -p "$session_dir"
exec 9>"$session_dir/transition.lock"
flock -w 10 -x 9 || {{ echo "session transition lock timed out" >&2; exit 75; }}
pid_file="$session_dir/api.pid"
log_file="$session_dir/api.log"
metadata_file="$session_dir/metadata.json"
is_owned_api_pid() {{
  python3 - "$metadata_file" "$1" "$session_id" <<'__CLIO_RELAY_PID_OWNER__'
import json
import sys
metadata_path, pid, session_id = sys.argv[1:]
try:
    with open(metadata_path, encoding="utf-8") as handle:
        metadata = json.load(handle)
except (FileNotFoundError, json.JSONDecodeError):
    raise SystemExit(1)
if str(metadata.get("api_pid")) != pid:
    raise SystemExit(1)
if metadata.get("session_id") != session_id or metadata.get("owner") != "clio-relay":
    raise SystemExit(1)
owner_token = metadata.get("owner_token")
generation_id = metadata.get("session_generation_id")
start_ticks = metadata.get("process_start_ticks")
if (
    not isinstance(owner_token, str)
    or not owner_token
    or not isinstance(generation_id, str)
    or not generation_id
    or not isinstance(start_ticks, str)
):
    raise SystemExit(1)
try:
    with open(f"/proc/{{pid}}/cmdline", "rb") as handle:
        command = handle.read().replace(b"\\0", b" ").decode("utf-8", errors="replace")
    with open(f"/proc/{{pid}}/environ", "rb") as handle:
        environment = handle.read().split(b"\\0")
    with open(f"/proc/{{pid}}/stat", encoding="utf-8") as handle:
        observed_start_ticks = handle.read().rsplit(")", 1)[1].split()[19]
except OSError:
    raise SystemExit(1)
if "clio-relay" not in command or " api " not in f" {{command}} " or " start" not in command:
    raise SystemExit(1)
if f"CLIO_RELAY_SESSION_OWNER_TOKEN={{owner_token}}".encode() not in environment:
    raise SystemExit(1)
if f"CLIO_RELAY_SESSION_GENERATION_ID={{generation_id}}".encode() not in environment:
    raise SystemExit(1)
if observed_start_ticks != start_ticks:
    raise SystemExit(1)
__CLIO_RELAY_PID_OWNER__
}}
recorded_api_pgid="$(python3 - "$metadata_file" <<'__CLIO_RELAY_RECORDED_PGID__'
import json
import sys
try:
    with open(sys.argv[1], encoding="utf-8") as handle:
        pgid = json.load(handle).get("api_pgid")
except (FileNotFoundError, json.JSONDecodeError):
    pgid = None
print(pgid if isinstance(pgid, int) else "")
__CLIO_RELAY_RECORDED_PGID__
)"
existing_owned_pid=""
existing_owned_pgid=""
if [ -s "$pid_file" ] \
  && kill -0 "$(cat "$pid_file")" 2>/dev/null \
  && is_owned_api_pid "$(cat "$pid_file")"; then
  existing_owned_pid="$(cat "$pid_file")"
  existing_owned_pgid="$(python3 - "$metadata_file" <<'__CLIO_RELAY_EXISTING_PGID__'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    print(json.load(handle)["api_pgid"])
__CLIO_RELAY_EXISTING_PGID__
)"
elif [ -s "$pid_file" ]; then
  recorded_pid="$(cat "$pid_file")"
  if kill -0 "$recorded_pid" 2>/dev/null \
    || {{ [ -n "$recorded_api_pgid" ] && kill -0 -- "-$recorded_api_pgid" 2>/dev/null; }}; then
    echo "refusing to replace an active session API without ownership proof" >&2
    exit 1
  fi
  rm -f "$pid_file"
elif [ -n "$recorded_api_pgid" ] \
  && kill -0 -- "-$recorded_api_pgid" 2>/dev/null; then
  echo "refusing to replace an active session API group without a PID record" >&2
  exit 1
fi
recorded_generation_id="$(python3 - "$metadata_file" "$session_id" \
  <<'__CLIO_RELAY_RECORDED_GENERATION__'
import json
import sys
from pathlib import Path

metadata_path, session_id = sys.argv[1:]
try:
    metadata = json.loads(Path(metadata_path).read_text(encoding="utf-8"))
except FileNotFoundError:
    print("")
    raise SystemExit(0)
except json.JSONDecodeError as exc:
    raise SystemExit(f"owned session metadata is invalid: {{exc}}") from exc
if metadata.get("owner") != "clio-relay" or metadata.get("session_id") != session_id:
    raise SystemExit("owned session metadata identity is invalid")
generation_id = metadata.get("session_generation_id")
if generation_id is None:
    print("")
elif isinstance(generation_id, str) and generation_id:
    print(generation_id)
else:
    raise SystemExit("owned session metadata generation is invalid")
__CLIO_RELAY_RECORDED_GENERATION__
)"
candidate_generation_id="$(python3 -c 'import uuid; print(uuid.uuid4().hex)')"
prepare_args=(
  clio-relay session prepare-start
  --session-id "$session_id"
  --candidate-generation-id "$candidate_generation_id"
)
if [ -n "$recorded_generation_id" ]; then
  prepare_args+=(--recorded-generation-id "$recorded_generation_id")
fi
generation_transition="$("${{prepare_args[@]}}")"
session_generation_id="$(python3 - "$generation_transition" "$session_id" \
  <<'__CLIO_RELAY_SELECTED_GENERATION__'
import json
import sys

payload = json.loads(sys.argv[1])
session_id = sys.argv[2]
generation_id = payload.get("session_generation_id")
if payload.get("session_id") != session_id:
    raise SystemExit("owner session generation transition returned the wrong session")
if not isinstance(generation_id, str) or not generation_id:
    raise SystemExit("owner session generation transition returned no generation")
print(generation_id)
__CLIO_RELAY_SELECTED_GENERATION__
)"
if [ -n "$existing_owned_pid" ]; then
  clio-relay session resume-intake \
    --session-id "$session_id" \
    --session-generation-id "$session_generation_id" >/dev/null
  if [ "{replace_flag}" != "1" ]; then
    echo "session_already_running=$session_id"
    echo "api_pid=$existing_owned_pid"
    echo "session_generation_id=$session_generation_id"
    exit 0
  fi
  kill -- "-$existing_owned_pgid" 2>/dev/null || true
  for _ in 1 2 3 4 5 6 7 8 9 10; do
    if ! kill -0 -- "-$existing_owned_pgid" 2>/dev/null; then break; fi
    sleep 0.2
  done
  if kill -0 -- "-$existing_owned_pgid" 2>/dev/null; then
    kill -9 -- "-$existing_owned_pgid" 2>/dev/null || true
    sleep 0.2
  fi
  if kill -0 -- "-$existing_owned_pgid" 2>/dev/null; then
    echo "owned session API process group did not stop: $existing_owned_pgid" >&2
    exit 1
  fi
fi
if python3 - {remote_api_port} <<'__CLIO_RELAY_PORT_CHECK__'
import socket
import sys
port = int(sys.argv[1])
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        sock.bind(("127.0.0.1", port))
    except OSError:
        raise SystemExit(1)
__CLIO_RELAY_PORT_CHECK__
then
  :
else
  echo "remote API port is already occupied: {remote_api_port}" >&2
  exit 1
fi
api_command=(clio-relay api start --host 127.0.0.1 --port {remote_api_port}{require_token})
owner_token="$(python3 -c 'import secrets; print(secrets.token_hex(32))')"
api_pid=""
start_complete=0
cleanup_incomplete_start() {{
  if [ "$start_complete" = "1" ] || [ -z "$api_pid" ]; then return; fi
  kill -- "-$api_pid" 2>/dev/null || kill "$api_pid" 2>/dev/null || true
  for _ in 1 2 3 4 5; do
    if ! kill -0 -- "-$api_pid" 2>/dev/null; then break; fi
    sleep 0.2
  done
  if kill -0 -- "-$api_pid" 2>/dev/null; then
    kill -9 -- "-$api_pid" 2>/dev/null || kill -9 "$api_pid" 2>/dev/null || true
  fi
  for _ in 1 2 3 4 5; do
    if ! kill -0 -- "-$api_pid" 2>/dev/null; then break; fi
    sleep 0.1
  done
  if kill -0 -- "-$api_pid" 2>/dev/null; then
    echo "incomplete session API process group cleanup: $api_pid" >&2
    return 1
  fi
  python3 - \
    "$metadata_file" "$pid_file" "$api_pid" "$session_generation_id" \
    <<'__CLIO_RELAY_ROLLBACK_METADATA__'
import json
import sys
from pathlib import Path

metadata_path, pid_path, pid_raw, generation_id = sys.argv[1:]
metadata_file = Path(metadata_path)
pid_file = Path(pid_path)
try:
    metadata = json.loads(metadata_file.read_text(encoding="utf-8"))
except (FileNotFoundError, json.JSONDecodeError):
    metadata = None
if (
    isinstance(metadata, dict)
    and str(metadata.get("api_pid")) == pid_raw
    and metadata.get("session_generation_id") == generation_id
):
    metadata_file.unlink(missing_ok=True)
try:
    recorded_pid = pid_file.read_text(encoding="utf-8").strip()
except OSError:
    recorded_pid = None
if recorded_pid == pid_raw:
    pid_file.unlink(missing_ok=True)
__CLIO_RELAY_ROLLBACK_METADATA__
}}
trap cleanup_incomplete_start EXIT
nohup setsid env \\
  "CLIO_RELAY_SESSION_OWNER_TOKEN=$owner_token" \\
  "CLIO_RELAY_SESSION_GENERATION_ID=$session_generation_id" \\
  "CLIO_RELAY_OWNER_SESSION_ID=$session_id" \\
  "CLIO_RELAY_OWNER_SESSION_CLUSTER={shlex.quote(cluster)}" \\
  "${{api_command[@]}}" \\
  >"$log_file" 2>&1 9>&- &
api_pid="$!"
echo "$api_pid" > "$pid_file"
python3 - \
  "$metadata_file" "$api_pid" "$owner_token" "$session_generation_id" \
  <<'__CLIO_RELAY_METADATA__'
import json
import os
import sys
import time
from datetime import datetime, timezone
path = sys.argv[1]
api_pid = int(sys.argv[2])
owner_token = sys.argv[3]
session_generation_id = sys.argv[4]
for _ in range(40):
    try:
        api_pgid = os.getpgid(api_pid)
        with open(f"/proc/{{api_pid}}/environ", "rb") as handle:
            environment = handle.read().split(bytes([0]))
    except OSError:
        time.sleep(0.05)
        continue
    if (
        api_pgid == api_pid
        and f"CLIO_RELAY_SESSION_OWNER_TOKEN={{owner_token}}".encode() in environment
        and f"CLIO_RELAY_SESSION_GENERATION_ID={{session_generation_id}}".encode()
        in environment
    ):
        break
    time.sleep(0.05)
else:
    raise RuntimeError("owned API process did not establish its isolated process group")
with open(f"/proc/{{api_pid}}/stat", encoding="utf-8") as handle:
    process_start_ticks = handle.read().rsplit(")", 1)[1].split()[19]
metadata = {{
    "cluster": {cluster!r},
    "session_id": {session_id!r},
    "remote_api_port": {remote_api_port},
    "api_pid": api_pid,
    "api_pgid": api_pgid,
    "owner_token": owner_token,
    "session_generation_id": session_generation_id,
    "process_start_ticks": process_start_ticks,
    "started_at": datetime.now(timezone.utc).isoformat(),
    "owner": "clio-relay",
}}
temporary = f"{{path}}.{{os.getpid()}}.tmp"
with open(temporary, "w", encoding="utf-8") as handle:
    json.dump(metadata, handle, indent=2)
os.chmod(temporary, 0o600)
os.replace(temporary, path)
__CLIO_RELAY_METADATA__
python3 - "$api_pid" "{remote_api_port}" <<'__CLIO_RELAY_API_READY__'
import json
import os
import sys
import time
import urllib.error
import urllib.request

api_pid = int(sys.argv[1])
port = int(sys.argv[2])
url = f"http://127.0.0.1:{{port}}/healthz"
last_error = "API did not become ready"
readiness_timeout_seconds = {_REMOTE_API_READINESS_TIMEOUT_SECONDS!r}
readiness_started = time.monotonic()
readiness_deadline = readiness_started + readiness_timeout_seconds
attempts = 0
while time.monotonic() < readiness_deadline:
    attempts += 1
    try:
        os.kill(api_pid, 0)
    except OSError as exc:
        elapsed = time.monotonic() - readiness_started
        raise RuntimeError(
            f"owned API process exited before readiness after {{elapsed:.3f}} seconds"
        ) from exc
    try:
        with urllib.request.urlopen(url, timeout=0.25) as response:
            payload = json.load(response)
        if response.status == 200 and payload.get("ok") is True:
            break
        last_error = f"unexpected health response: {{payload!r}}"
    except (OSError, TimeoutError, urllib.error.URLError, json.JSONDecodeError) as exc:
        last_error = str(exc)
    time.sleep(0.1)
else:
    elapsed = time.monotonic() - readiness_started
    raise RuntimeError(
        "owned API did not become ready within "
        f"{{readiness_timeout_seconds:.1f}} seconds after {{attempts}} attempts: {{last_error}}"
    )
print(f"remote_api_ready_seconds={{time.monotonic() - readiness_started:.3f}}")
__CLIO_RELAY_API_READY__
clio-relay session resume-intake \
  --session-id "$session_id" \
  --session-generation-id "$session_generation_id" >/dev/null
start_complete=1
trap - EXIT
echo "session_started=$session_id"
echo "api_pid=$api_pid"
echo "session_generation_id=$session_generation_id"
echo "remote_api_port={remote_api_port}"
echo "metadata=$metadata_file"
"""


def _status_script(*, session_id: str) -> str:  # pyright: ignore[reportUnusedFunction]
    return f"""set -euo pipefail
session_id={shlex.quote(session_id)}
session_dir="$HOME/.local/share/clio-relay/sessions/$session_id"
pid_file="$session_dir/api.pid"
metadata_file="$session_dir/metadata.json"
running=false
api_pid=null
is_owned_api_pid() {{
  python3 - "$metadata_file" "$1" "$session_id" <<'__CLIO_RELAY_PID_OWNER__'
import json
import sys
metadata_path, pid, session_id = sys.argv[1:]
try:
    with open(metadata_path, encoding="utf-8") as handle:
        metadata = json.load(handle)
except (FileNotFoundError, json.JSONDecodeError):
    raise SystemExit(1)
if str(metadata.get("api_pid")) != pid:
    raise SystemExit(1)
if metadata.get("session_id") != session_id or metadata.get("owner") != "clio-relay":
    raise SystemExit(1)
try:
    with open(f"/proc/{{pid}}/cmdline", "rb") as handle:
        command = handle.read().replace(b"\\0", b" ").decode("utf-8", errors="replace")
except OSError:
    raise SystemExit(1)
if "clio-relay" not in command or " api " not in f" {{command}} " or " start" not in command:
    raise SystemExit(1)
__CLIO_RELAY_PID_OWNER__
}}
if [ -s "$pid_file" ]; then
  api_pid="$(cat "$pid_file")"
  if kill -0 "$api_pid" 2>/dev/null && is_owned_api_pid "$api_pid"; then running=true; fi
fi
python3 - "$metadata_file" "$running" "$api_pid" <<'__CLIO_RELAY_STATUS__'
import json
import sys
metadata_path, running, api_pid = sys.argv[1:]
metadata = {{}}
try:
    with open(metadata_path, encoding="utf-8") as handle:
        metadata = json.load(handle)
except FileNotFoundError:
    pass
metadata["running"] = running == "true"
metadata["api_pid"] = None if api_pid == "null" else int(api_pid)
print(json.dumps(metadata))
__CLIO_RELAY_STATUS__
"""


def _teardown_script(  # pyright: ignore[reportUnusedFunction]
    *, session_id: str, stop_worker: bool, cluster: str | None
) -> str:
    worker_command = ""
    if stop_worker:
        if cluster is None:
            raise RelayError("cluster is required when stopping the worker service")
        service = shlex.quote(f"clio-relay-worker-{cluster}.service")
        worker_command = f"systemctl --user stop {service} || true\necho worker_stopped={service}\n"
    return f"""set -euo pipefail
session_id={shlex.quote(session_id)}
session_dir="$HOME/.local/share/clio-relay/sessions/$session_id"
pid_file="$session_dir/api.pid"
metadata_file="$session_dir/metadata.json"
is_owned_api_pid() {{
  python3 - "$metadata_file" "$1" "$session_id" <<'__CLIO_RELAY_PID_OWNER__'
import json
import sys
metadata_path, pid, session_id = sys.argv[1:]
try:
    with open(metadata_path, encoding="utf-8") as handle:
        metadata = json.load(handle)
except (FileNotFoundError, json.JSONDecodeError):
    raise SystemExit(1)
if str(metadata.get("api_pid")) != pid:
    raise SystemExit(1)
if metadata.get("session_id") != session_id or metadata.get("owner") != "clio-relay":
    raise SystemExit(1)
try:
    with open(f"/proc/{{pid}}/cmdline", "rb") as handle:
        command = handle.read().replace(b"\\0", b" ").decode("utf-8", errors="replace")
except OSError:
    raise SystemExit(1)
if "clio-relay" not in command or " api " not in f" {{command}} " or " start" not in command:
    raise SystemExit(1)
__CLIO_RELAY_PID_OWNER__
}}
if [ -s "$pid_file" ]; then
  api_pid="$(cat "$pid_file")"
  if kill -0 "$api_pid" 2>/dev/null && is_owned_api_pid "$api_pid"; then
    kill "$api_pid" 2>/dev/null || true
    for _ in 1 2 3 4 5; do
      if ! kill -0 "$api_pid" 2>/dev/null; then break; fi
      sleep 1
    done
    if kill -0 "$api_pid" 2>/dev/null; then kill -9 "$api_pid" 2>/dev/null || true; fi
    echo "api_stopped=$api_pid"
  elif kill -0 "$api_pid" 2>/dev/null; then
    echo "api_pid_not_owned=$api_pid"
  else
    echo "api_not_running=$api_pid"
  fi
else
  echo "api_pid_missing=$session_id"
fi
rm -f "$pid_file"
{worker_command}echo "session_teardown=$session_id"
"""


def _owned_status_script(*, session_id: str) -> str:
    return f"""set -euo pipefail
session_id={shlex.quote(session_id)}
metadata_file="$HOME/.local/share/clio-relay/sessions/$session_id/metadata.json"
python3 - "$metadata_file" "$session_id" <<'__CLIO_RELAY_OWNED_STATUS__'
import json
import sys
from pathlib import Path

metadata_path, session_id = sys.argv[1:]
metadata = {{}}
try:
    metadata = json.loads(Path(metadata_path).read_text(encoding="utf-8"))
except (FileNotFoundError, json.JSONDecodeError):
    pass

pid = metadata.get("api_pid")
if not isinstance(pid, int):
    try:
        pid = int(Path(metadata_path).with_name("api.pid").read_text(encoding="utf-8").strip())
    except (OSError, ValueError):
        pid = None
running = False
ownership_verified = False
try:
    proc = Path("/proc") / str(pid)
    stat = (proc / "stat").read_text(encoding="utf-8")
    running = stat.rsplit(")", 1)[1].split()[0] != "Z"
    observed_start = stat.rsplit(")", 1)[1].split()[19]
    command = (proc / "cmdline").read_bytes().replace(bytes([0]), b" ").decode(
        "utf-8", errors="replace"
    )
    environment = (proc / "environ").read_bytes().split(bytes([0]))
    token = metadata.get("owner_token")
    generation_id = metadata.get("session_generation_id")
    ownership_verified = (
        metadata.get("owner") == "clio-relay"
        and metadata.get("session_id") == session_id
        and isinstance(pid, int)
        and isinstance(token, str)
        and bool(token)
        and isinstance(generation_id, str)
        and bool(generation_id)
        and metadata.get("process_start_ticks") == observed_start
        and f"CLIO_RELAY_SESSION_OWNER_TOKEN={{token}}".encode() in environment
        and f"CLIO_RELAY_SESSION_GENERATION_ID={{generation_id}}".encode() in environment
        and "clio-relay" in command
        and " api " in f" {{command}} "
        and " start" in command
    )
except (OSError, IndexError, TypeError):
    pass

metadata["running"] = running
metadata["ownership_verified"] = ownership_verified
metadata["api_pid"] = pid if isinstance(pid, int) else None
metadata["ownership_token_present"] = bool(metadata.pop("owner_token", None))
print(json.dumps(metadata))
__CLIO_RELAY_OWNED_STATUS__
"""


def _owned_identity_challenge_script(
    *,
    cluster: str,
    session_id: str,
    session_generation_id: str,
    nonce: str,
) -> str:
    return f"""set -euo pipefail
session_id={shlex.quote(session_id)}
metadata_file="$HOME/.local/share/clio-relay/sessions/$session_id/metadata.json"
python3 - "$metadata_file" {shlex.quote(cluster)} {shlex.quote(session_id)} \
  {shlex.quote(session_generation_id)} {shlex.quote(nonce)} <<'__CLIO_RELAY_IDENTITY_CHALLENGE__'
import hashlib
import hmac
import json
import os
import sys
from pathlib import Path

metadata_path, cluster, session_id, generation_id, nonce = sys.argv[1:]
metadata = json.loads(Path(metadata_path).read_text(encoding="utf-8"))
pid = metadata.get("api_pid")
token = metadata.get("owner_token")
if (
    metadata.get("owner") != "clio-relay"
    or metadata.get("cluster") != cluster
    or metadata.get("session_id") != session_id
    or metadata.get("session_generation_id") != generation_id
    or not isinstance(pid, int)
    or not isinstance(token, str)
    or not token
):
    raise SystemExit("owned session identity does not match the requested generation")
proc = Path("/proc") / str(pid)
stat_fields = (proc / "stat").read_text(encoding="utf-8").rsplit(")", 1)[1].split()
observed_start = stat_fields[19]
command = (proc / "cmdline").read_bytes().replace(bytes([0]), b" ").decode(
    "utf-8", errors="replace"
)
environment = (proc / "environ").read_bytes().split(bytes([0]))
owner_cluster_marker = f"CLIO_RELAY_OWNER_SESSION_CLUSTER={{cluster}}".encode()
owner_cluster_entries = [
    item for item in environment if item.startswith(b"CLIO_RELAY_OWNER_SESSION_CLUSTER=")
]
owner_cluster_verified = owner_cluster_marker in environment or not owner_cluster_entries
if (
    stat_fields[0] == "Z"
    or os.getpgid(pid) != pid
    or metadata.get("api_pgid") != pid
    or metadata.get("process_start_ticks") != observed_start
    or f"CLIO_RELAY_SESSION_OWNER_TOKEN={{token}}".encode() not in environment
    or f"CLIO_RELAY_SESSION_GENERATION_ID={{generation_id}}".encode() not in environment
    or f"CLIO_RELAY_OWNER_SESSION_ID={{session_id}}".encode() not in environment
    or f"CLIO_RELAY_REMOTE_CLUSTER={{cluster}}".encode() not in environment
    or not owner_cluster_verified
    or "clio-relay" not in command
    or " api " not in f" {{command}} "
    or " start" not in command
):
    raise SystemExit("owned session API process identity is not verified")
message = "\\n".join(
    (
        "clio-relay.session-identity.v1",
        cluster,
        session_id,
        generation_id,
        nonce,
    )
).encode("utf-8")
signature = hmac.new(token.encode("utf-8"), message, hashlib.sha256).hexdigest()
print(json.dumps({{
    "schema_version": "clio-relay.session-identity.v1",
    "cluster": cluster,
    "session_id": session_id,
    "session_generation_id": generation_id,
    "nonce": nonce,
    "hmac_sha256": signature,
}}, sort_keys=True, separators=(",", ":")))
__CLIO_RELAY_IDENTITY_CHALLENGE__
"""


def _owned_teardown_script(
    *,
    definition: ClusterDefinition,
    session_id: str,
    expected_session_generation_id: str,
    stop_worker: bool,
    cancel_jobs: bool,
    cancel_scheduler_jobs: bool,
    cluster: str | None,
) -> str:
    if stop_worker and cluster is None:
        raise RelayError("cluster is required when stopping the worker service")
    cluster_value = cluster or ""
    service = f"clio-relay-worker-{cluster_value}.service" if stop_worker else ""
    cleanup_policy_flags = ""
    if stop_worker:
        cleanup_policy_flags += " --cleanup-stop-worker"
    if cancel_jobs:
        cleanup_policy_flags += " --cleanup-cancel-jobs"
    if cancel_scheduler_jobs:
        cleanup_policy_flags += " --cleanup-cancel-scheduler-jobs"
    return f"""set -euo pipefail
{remote_env(definition)}
session_id={shlex.quote(session_id)}
session_dir="$HOME/.local/share/clio-relay/sessions/$session_id"
mkdir -p "$session_dir"
exec 9>"$session_dir/transition.lock"
flock -w 10 -x 9 || {{ echo "session teardown lock timed out" >&2; exit 75; }}
metadata_file="$session_dir/metadata.json"
expected_session_generation_id={shlex.quote(expected_session_generation_id)}
python3 - "$metadata_file" "$session_id" "$expected_session_generation_id" \
  <<'__CLIO_RELAY_EXPECTED_GENERATION__'
import json
import sys
from pathlib import Path

metadata_path, session_id, expected_generation_id = sys.argv[1:]
try:
    metadata = json.loads(Path(metadata_path).read_text(encoding="utf-8"))
except (FileNotFoundError, json.JSONDecodeError) as exc:
    raise SystemExit(f"owned session metadata is unavailable: {{exc}}") from exc
if metadata.get("owner") != "clio-relay" or metadata.get("session_id") != session_id:
    raise SystemExit("owned session metadata does not match the requested session")
if metadata.get("session_generation_id") != expected_generation_id:
    raise SystemExit("owned session generation changed before teardown")
__CLIO_RELAY_EXPECTED_GENERATION__
cleanup_intake_result="$(timeout --signal=TERM --kill-after=5s 10s \
  clio-relay session quiesce-intake \
  --session-id "$session_id" \
  --session-generation-id "$expected_session_generation_id"{cleanup_policy_flags})"
timeout --signal=TERM --kill-after=5s 90s \
  python3 - "$session_dir" "$session_id" {shlex.quote(cluster_value)} \
  {"1" if stop_worker else "0"} {shlex.quote(service)} "$cleanup_intake_result" \
  <<'__CLIO_RELAY_OWNED_TEARDOWN__'
import json
import os
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

session_dir = Path(sys.argv[1])
session_id, cluster, stop_worker_raw, service, cleanup_intake_raw = sys.argv[2:]
stop_worker = stop_worker_raw == "1"
cleanup_intake = json.loads(cleanup_intake_raw)
cleanup_intent = cleanup_intake.get("cleanup_intent")
if not isinstance(cleanup_intent, dict):
    raise RuntimeError("owner-session cleanup intent is missing")
metadata_path = session_dir / "metadata.json"
pid_path = session_dir / "api.pid"
errors = []
resources = []
metadata = {{}}
try:
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
except FileNotFoundError:
    pass
except json.JSONDecodeError as exc:
    errors.append(f"session metadata unavailable: {{exc}}")


def process_state(pid):
    try:
        stat = (Path("/proc") / str(pid) / "stat").read_text(encoding="utf-8")
    except OSError:
        return None, None
    fields = stat.rsplit(")", 1)[1].split()
    return fields[0], fields[19]


def token_group_processes():
    token = metadata.get("owner_token")
    generation_id = metadata.get("session_generation_id")
    if (
        not isinstance(token, str)
        or not token
        or not isinstance(generation_id, str)
        or not generation_id
    ):
        return []
    token_marker = f"CLIO_RELAY_SESSION_OWNER_TOKEN={{token}}".encode()
    generation_marker = f"CLIO_RELAY_SESSION_GENERATION_ID={{generation_id}}".encode()
    matches = []
    for proc in Path("/proc").iterdir():
        if not proc.name.isdigit():
            continue
        try:
            if proc.stat().st_uid != os.geteuid():
                continue
            fields = (proc / "stat").read_text(encoding="utf-8").rsplit(")", 1)[1].split()
            state = fields[0]
        except (FileNotFoundError, ProcessLookupError):
            continue
        except (OSError, IndexError, ValueError) as exc:
            raise RuntimeError(
                f"cannot verify owned session process {{proc.name}}: {{exc}}"
            ) from exc
        try:
            environment = (proc / "environ").read_bytes().split(bytes([0]))
        except (FileNotFoundError, ProcessLookupError):
            continue
        except PermissionError as exc:
            recorded_pgid = metadata.get("api_pgid")
            try:
                observed_pgid = int(fields[2])
            except (IndexError, ValueError) as parse_exc:
                raise RuntimeError(
                    f"cannot verify protected session process {{proc.name}}: {{parse_exc}}"
                ) from parse_exc
            if isinstance(recorded_pgid, int) and observed_pgid != recorded_pgid:
                continue
            raise RuntimeError(
                f"cannot verify protected session process {{proc.name}}: {{exc}}"
            ) from exc
        except OSError as exc:
            raise RuntimeError(
                f"cannot verify owned session process {{proc.name}}: {{exc}}"
            ) from exc
        if (
            state != "Z"
            and token_marker in environment
            and generation_marker in environment
        ):
            matches.append(int(proc.name))
    return sorted(matches)


def signal_token_processes(sig):
    if not hasattr(os, "pidfd_open") or not hasattr(signal, "pidfd_send_signal"):
        raise RuntimeError("race-safe pidfd session cleanup is unavailable")
    signaled = []
    for owned_pid in token_group_processes():
        try:
            process_fd = os.pidfd_open(owned_pid, 0)
        except ProcessLookupError:
            continue
        except OSError as exc:
            raise RuntimeError(f"cannot open session pidfd for {{owned_pid}}: {{exc}}") from exc
        try:
            if owned_pid not in token_group_processes():
                continue
            try:
                signal.pidfd_send_signal(process_fd, sig, None, 0)
            except ProcessLookupError:
                continue
            except OSError as exc:
                raise RuntimeError(
                    f"cannot signal owned session pid {{owned_pid}}: {{exc}}"
                ) from exc
            signaled.append(owned_pid)
        finally:
            os.close(process_fd)
    return signaled


pid = metadata.get("api_pid")
if not isinstance(pid, int):
    try:
        pid = int(pid_path.read_text(encoding="utf-8").strip())
    except (OSError, ValueError):
        pid = None
resource = {{
    "kind": "remote_relay_api",
    "resource_id": str(pid) if isinstance(pid, int) else session_id,
    "location": cluster or "remote",
    "action": "stop",
    "ownership_verified": False,
    "outcome": "missing",
    "residual": False,
    "detail": None,
}}
state, observed_start = process_state(pid)
owned_group_pids = token_group_processes()
running = bool(owned_group_pids)
prior_running = running
prior_observed_at = datetime.now(timezone.utc).isoformat()
durable_identity = (
    metadata.get("owner") == "clio-relay"
    and metadata.get("session_id") == session_id
    and isinstance(metadata.get("owner_token"), str)
    and bool(metadata.get("owner_token"))
    and isinstance(metadata.get("session_generation_id"), str)
    and bool(metadata.get("session_generation_id"))
    and isinstance(metadata.get("process_start_ticks"), str)
    and isinstance(metadata.get("api_pgid"), int)
)
leader_owned = False
if state is not None and state != "Z" and isinstance(pid, int):
    try:
        proc = Path("/proc") / str(pid)
        command = (proc / "cmdline").read_bytes().replace(bytes([0]), b" ").decode(
            "utf-8", errors="replace"
        )
        environment = (proc / "environ").read_bytes().split(bytes([0]))
        token = metadata.get("owner_token")
        generation_id = metadata.get("session_generation_id")
        recorded_pgid = metadata.get("api_pgid")
        leader_owned = (
            durable_identity
            and isinstance(token, str)
            and isinstance(generation_id, str)
            and metadata.get("process_start_ticks") == observed_start
            and isinstance(recorded_pgid, int)
            and os.getpgid(pid) == recorded_pgid
            and f"CLIO_RELAY_SESSION_OWNER_TOKEN={{token}}".encode() in environment
            and f"CLIO_RELAY_SESSION_GENERATION_ID={{generation_id}}".encode()
            in environment
            and "clio-relay" in command
            and " api " in f" {{command}} "
            and " start" in command
        )
    except (OSError, TypeError):
        leader_owned = False
ownership_verified = durable_identity and bool(owned_group_pids)
if running and not leader_owned:
    resource["detail"] = (
        "recorded API leader was absent or replaced; only exact token-generation "
        "processes were targeted"
    )
if running:
    resource["ownership_verified"] = ownership_verified
    if not ownership_verified:
        resource["outcome"] = "refused"
        resource["residual"] = True
        resource["detail"] = "ownership proof failed; process was not signaled"
        errors.append(f"refused to stop unverified API pid {{pid}}")
    else:
        try:
            signal_token_processes(signal.SIGTERM)
            for _ in range(25):
                if not token_group_processes():
                    break
                time.sleep(0.2)
            if token_group_processes():
                signal_token_processes(signal.SIGKILL)
                time.sleep(0.2)
            residual = bool(token_group_processes())
            resource["outcome"] = "failed" if residual else "stopped"
            resource["residual"] = residual
            if residual:
                errors.append(f"API process group still running for pid {{pid}}")
            else:
                pid_path.unlink(missing_ok=True)
        except (OSError, RuntimeError) as exc:
            resource["outcome"] = "failed"
            resource["residual"] = True
            resource["detail"] = str(exc)
            errors.append(f"failed to stop API pid {{pid}}: {{exc}}")
elif isinstance(pid, int):
    pid_path.unlink(missing_ok=True)
    resource["ownership_verified"] = durable_identity
    ownership_verified = durable_identity
    if not durable_identity:
        resource["outcome"] = "refused"
        resource["residual"] = True
        resource["detail"] = "durable ownership identity is incomplete; absence is unproven"
        errors.append(f"could not prove missing API identity for pid {{pid}}")
resource["verified_after_operation"] = (
    resource["outcome"] in {{"stopped", "missing"}}
    and resource["ownership_verified"]
    and not resource["residual"]
)
resources.append(resource)
post_running = bool(token_group_processes())
post_observed_at = datetime.now(timezone.utc).isoformat()


def cleanup_command(command):
    try:
        return subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=False,
            timeout=20,
        )
    except subprocess.TimeoutExpired:
        return subprocess.CompletedProcess(
            command,
            124,
            "",
            "cleanup command timed out after 20 seconds",
        )


if stop_worker:
    ownership = cleanup_command(
        [
            "systemctl",
            "--user",
            "show",
            service,
            "--property=LoadState",
            "--property=FragmentPath",
            "--property=ExecStart",
        ],
    )
    service_missing = "LoadState=not-found" in ownership.stdout
    worker_owned = (
        ownership.returncode == 0
        and not service_missing
        and "clio-relay" in ownership.stdout
        and "endpoint start" in ownership.stdout
    )
    stopped = None
    if worker_owned:
        stopped = cleanup_command(["systemctl", "--user", "stop", service])
    active = cleanup_command(["systemctl", "--user", "is-active", service])
    active_state = active.stdout.strip().lower() or "unknown"
    observed_state = "not-found" if service_missing else active_state
    verified_after_operation = service_missing or (
        worker_owned
        and stopped is not None
        and stopped.returncode == 0
        and active_state == "inactive"
    )
    residual = not verified_after_operation
    if worker_owned and stopped is not None:
        outcome = "stopped" if verified_after_operation else "failed"
        detail = stopped.stderr.strip() or active.stdout.strip() or None
    elif service_missing:
        outcome = "missing"
        detail = "worker service is not installed"
    else:
        outcome = "refused"
        detail = "worker service ownership proof failed; service was not stopped"
    resources.append({{
        "kind": "worker_service",
        "resource_id": service,
        "location": cluster or "remote",
        "action": "stop",
        "ownership_verified": worker_owned or service_missing,
        "outcome": outcome,
        "verified_after_operation": verified_after_operation,
        "observed_state": observed_state,
        "residual": residual,
        "detail": detail,
    }})
    if outcome in {{"failed", "refused"}}:
        errors.append(f"worker service cleanup {{outcome}}: {{service}}")

metadata["last_cleanup"] = {{
    "mode": "teardown",
    "completed_at": datetime.now(timezone.utc).isoformat(),
    "resources": resources,
    "errors": errors,
}}
session_dir.mkdir(parents=True, exist_ok=True)
metadata_path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")
print(json.dumps({{
    "cluster": cluster or None,
    "session_id": session_id,
    "session_generation_id": metadata.get("session_generation_id"),
    "mode": "teardown",
    "cleanup_operation_id": cleanup_intent.get("operation_id"),
    "cleanup_policy": {{
        "stop_worker": cleanup_intent.get("stop_worker"),
        "cancel_jobs": cleanup_intent.get("cancel_jobs"),
        "cancel_scheduler_jobs": cleanup_intent.get("cancel_scheduler_jobs"),
    }},
    "relay_cancel_requested": cleanup_intent.get("cancel_jobs") is True,
    "scheduler_cancel_requested": cleanup_intent.get("cancel_scheduler_jobs") is True,
    "prior_session_status": {{
        "api_pid": pid if isinstance(pid, int) else None,
        "session_generation_id": metadata.get("session_generation_id"),
        "process_start_marker": metadata.get("process_start_ticks"),
        "running": prior_running,
        "ownership_verified": ownership_verified,
        "observed_at": prior_observed_at,
        "started_at": metadata.get("started_at"),
    }},
    "post_session_status": {{
        "api_pid": pid if isinstance(pid, int) else None,
        "session_generation_id": metadata.get("session_generation_id"),
        "process_start_marker": metadata.get("process_start_ticks"),
        "running": post_running,
        "ownership_verified": ownership_verified,
        "observed_at": post_observed_at,
        "started_at": metadata.get("started_at"),
    }},
    "resources": resources,
    "errors": errors,
}}))
__CLIO_RELAY_OWNED_TEARDOWN__
"""


def _validate_session(*, session_id: str, remote_api_port: int) -> None:
    if not session_id or not all(item.isalnum() or item in {"-", "_"} for item in session_id):
        raise RelayError("session_id must contain only letters, numbers, hyphen, or underscore")
    if remote_api_port <= 0:
        raise RelayError("remote_api_port must be positive")


def _validate_durable_session_identity(value: str, *, field: str) -> str:
    """Validate an execution identity before any remote lifecycle I/O."""
    try:
        return validate_durable_record_id(value)
    except ValueError as error:
        raise RelayError(f"invalid {field}: {error}") from error


def _ssh_script(definition: ClusterDefinition, script: str) -> str:
    try:
        result = subprocess.run(
            ["ssh", definition.ssh_host, "bash", "-s"],
            input=script.encode("utf-8"),
            capture_output=True,
            check=False,
            timeout=_REMOTE_SESSION_COMMAND_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired as exc:
        raise RelayError(
            "remote session command timed out after "
            f"{_REMOTE_SESSION_COMMAND_TIMEOUT_SECONDS:g} seconds"
        ) from exc
    if result.returncode != 0:
        stdout = result.stdout.decode("utf-8", errors="replace").strip()
        stderr = result.stderr.decode("utf-8", errors="replace").strip()
        detail = stderr or stdout
        raise RelayError(f"remote session command failed: {detail}")
    return result.stdout.decode("utf-8", errors="replace")


def _shell_single_quote(value: str) -> str:
    return "'" + value.replace("'", "'\"'\"'") + "'"

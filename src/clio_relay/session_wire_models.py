"""Wire models for owned remote relay session lifecycle operations.

Extracted from :mod:`clio_relay.session_lifecycle` (iowarp/clio-relay#231,
R8(iii); see ``docs/design/relay-architecture-2026-08.md`` §4.4). This module
owns the wire-contract cluster: one frozen dataclass (:class:`RemoteSession`)
plus sixteen ``pydantic.BaseModel`` types that define the exact stdin/stdout
contracts, selectors, and status/result documents the owned-session state
machine passes across process and API boundaries.

The state-machine logic that constructs and interprets these types stays in
:mod:`clio_relay.session_lifecycle`, which re-exports every name here under
its original binding so existing callers, tests, and monkeypatch seams that
reference ``session_lifecycle.<Symbol>`` keep resolving unchanged (a pure
move, not a behavior change).
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from clio_relay.config import (
    DEFAULT_INPUT_FILE_MAX_BYTES,
    DEFAULT_INPUT_FILE_MAX_COUNT,
    DEFAULT_INPUT_TOTAL_MAX_BYTES,
    MAX_INPUT_FILE_MAX_BYTES,
    MAX_INPUT_TOTAL_MAX_BYTES,
)
from clio_relay.identifiers import DurableRecordId
from clio_relay.validation_report import SoftwareIdentity

if TYPE_CHECKING:
    from clio_relay.validation_report import ValidationResource

# Bound the two Field() constraints below. Both are wire-contract constants
# (a max_length/le on a model field), not general session_lifecycle.py
# constants -- session_lifecycle.py imports them back for the handful of
# business-logic sites (string trimming, byte-limit checks) that must agree
# with the same bound these models enforce on the wire.
MAX_SESSION_START_ERROR_CHARS = 8192
MAX_OWNED_SESSION_CLEANUP_REPORT_BYTES = 32 * 1024 * 1024


@dataclass(frozen=True)
class RemoteSession:
    """A remotely owned relay session."""

    session_id: str
    remote_api_port: int
    api_token: str | None


class SessionApiReleaseIdentity(BaseModel):
    """Exact released artifact identity bound to an owned session API process."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["clio-relay.session-api-release.v1"] = (
        "clio-relay.session-api-release.v1"
    )
    distribution_version: str = Field(min_length=1)
    artifact_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    software: SoftwareIdentity

    def canonical_json(self) -> str:
        """Return the canonical JSON representation used for process attestation."""
        return json.dumps(
            self.model_dump(mode="json"),
            sort_keys=True,
            separators=(",", ":"),
        )

    def sha256(self) -> str:
        """Return the canonical release-identity digest."""
        return hashlib.sha256(self.canonical_json().encode("utf-8")).hexdigest()


class OwnedSessionInputPolicy(BaseModel):
    """Bounded input-ingestion policy projected into one owned API generation."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["clio-relay.owner-session-input-policy.v1"] = (
        "clio-relay.owner-session-input-policy.v1"
    )
    file_max_bytes: int = Field(
        default=DEFAULT_INPUT_FILE_MAX_BYTES,
        ge=1,
        le=MAX_INPUT_FILE_MAX_BYTES,
    )
    total_max_bytes: int = Field(
        default=DEFAULT_INPUT_TOTAL_MAX_BYTES,
        ge=1,
        le=MAX_INPUT_TOTAL_MAX_BYTES,
    )
    file_max_count: int = Field(default=DEFAULT_INPUT_FILE_MAX_COUNT, ge=1, le=1_000)

    @model_validator(mode="after")
    def _validate_total(self) -> OwnedSessionInputPolicy:
        if self.total_max_bytes < self.file_max_bytes:
            raise ValueError("input policy total_max_bytes must cover file_max_bytes")
        return self

    def environment(self) -> dict[str, str]:
        """Return the exact child-process environment projection."""

        return {
            "CLIO_RELAY_INPUT_FILE_MAX_BYTES": str(self.file_max_bytes),
            "CLIO_RELAY_INPUT_TOTAL_MAX_BYTES": str(self.total_max_bytes),
            "CLIO_RELAY_INPUT_FILE_MAX_COUNT": str(self.file_max_count),
        }


class OwnedSessionStartRequest(BaseModel):
    """Exact stdin contract for one cluster-local owned-session start."""

    model_config = ConfigDict(extra="forbid")

    cluster: str = Field(min_length=1)
    session_id: str = Field(min_length=1)
    start_operation_id: DurableRecordId
    remote_api_port: int = Field(gt=0, le=65_535)
    replace: bool = False
    require_token: bool = True
    input_policy: OwnedSessionInputPolicy = Field(default_factory=OwnedSessionInputPolicy)
    expected_api_release_identity: SessionApiReleaseIdentity | None = None
    cluster_registry: dict[str, object]
    cluster_registry_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    cluster_route_revision: str = Field(min_length=1)


class OwnedSessionStartRejection(BaseModel):
    """Exact rejection of one invocation, not proof the durable operation failed."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["clio-relay.owner-session-start-rejection.v1"] = (
        "clio-relay.owner-session-start-rejection.v1"
    )
    cluster: str = Field(min_length=1)
    session_id: str = Field(min_length=1)
    start_operation_id: DurableRecordId
    cluster_route_revision: str = Field(min_length=1)
    invocation_rejected: Literal[True] = True
    error: str = Field(min_length=1, max_length=MAX_SESSION_START_ERROR_CHARS)


class OwnedSessionStartStatusSelector(BaseModel):
    """Selector for the current transition until a later start supersedes it."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    operation: Literal["session.start-status"] = "session.start-status"
    cluster: str = Field(min_length=1)
    session_id: str = Field(min_length=1)
    start_operation_id: DurableRecordId
    cluster_route_revision: str = Field(min_length=1)
    remote_api_port: int = Field(gt=0, le=65_535)
    replace: bool
    require_token: bool
    input_policy: OwnedSessionInputPolicy = Field(default_factory=OwnedSessionInputPolicy)
    expected_api_release_identity_sha256: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{64}$",
    )


class OwnedSessionStartRetrySelector(BaseModel):
    """Secret-free selector for safely retrying one owned-session start."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    operation: Literal["session.start"] = "session.start"
    cluster: str = Field(min_length=1)
    session_id: str = Field(min_length=1)
    start_operation_id: DurableRecordId
    cluster_route_revision: str = Field(min_length=1)
    remote_api_port: int = Field(gt=0, le=65_535)
    replace: bool
    require_token: bool
    input_policy: OwnedSessionInputPolicy = Field(default_factory=OwnedSessionInputPolicy)
    expected_api_release_identity_sha256: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{64}$",
    )


class OwnedSessionTeardownRequest(BaseModel):
    """Exact stdin contract for one cluster-local owned-session teardown."""

    model_config = ConfigDict(extra="forbid")

    cluster: str = Field(min_length=1)
    session_id: str = Field(min_length=1)
    expected_session_generation_id: DurableRecordId
    expected_cleanup_operation_id: DurableRecordId
    stop_worker: bool = False
    cancel_jobs: bool = False
    cancel_scheduler_jobs: bool = False


class OwnedSessionIdentityChallengeRequest(BaseModel):
    """Exact stdin contract for a bounded owned-session identity challenge."""

    model_config = ConfigDict(extra="forbid")

    cluster: str = Field(min_length=1)
    session_id: str = Field(min_length=1)
    session_generation_id: DurableRecordId
    nonce: str = Field(pattern=r"^[0-9a-f]{64}$")


class OwnedSessionCleanupTarget(BaseModel):
    """Pinned identity for one file authorized by a cleanup receipt."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(pattern=r"^[A-Za-z0-9_.-]+$")
    present: bool
    device: int | None = Field(default=None, ge=0)
    inode: int | None = Field(default=None, gt=0)
    size: int | None = Field(default=None, ge=0)
    sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    identity_mode: Literal["inode", "content_sha256"] = "content_sha256"

    def identity_is_complete(self) -> bool:
        """Return whether present/absent state has the exact permitted shape."""
        stat_identity = (self.device, self.inode, self.size)
        if not self.present:
            return all(value is None for value in (*stat_identity, self.sha256))
        if not all(value is not None for value in stat_identity):
            return False
        return self.sha256 is None if self.identity_mode == "inode" else self.sha256 is not None


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


class OwnedSessionCleanupReportReference(BaseModel):
    """Immutable owner-private sidecar identity for one coordinator report."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["clio-relay.owner-session-cleanup-report-ref.v1"] = (
        "clio-relay.owner-session-cleanup-report-ref.v1"
    )
    name: str = Field(
        min_length=1,
        max_length=128,
        pattern=r"^coordinator-cleanup-report-[0-9a-f]{64}\.json$",
    )
    size: int = Field(gt=0, le=MAX_OWNED_SESSION_CLEANUP_REPORT_BYTES)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class OwnedSessionRecoveryStatus(BaseModel):
    """Fail-closed recovery evidence for one exact owned session generation."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["clio-relay.owner-session-recovery-status.v1"] = (
        "clio-relay.owner-session-recovery-status.v1"
    )
    cluster: str
    session_id: str
    session_generation_id: DurableRecordId | None = None
    start_operation_id: DurableRecordId | None = None
    cluster_route_revision: str | None = None
    owner: str | None = None
    api_pid: int | None = None
    remote_api_port: int | None = None
    process_start_marker: str | None = None
    leader_process_state: Literal[
        "absent",
        "owned_running",
        "owned_terminal",
        "reused",
        "foreign",
        "unverified",
    ] = "unverified"
    process_state: Literal[
        "absent",
        "owned_running",
        "owned_terminal",
        "reused",
        "foreign",
        "cleanup_pending",
        "already_closed",
        "unverified",
    ] = "unverified"
    running: bool = False
    process_absence_verified: bool = False
    generation_process_pids: list[int] = Field(default_factory=list[int])
    generation_process_absence_verified: bool = False
    metadata_verified: bool = False
    cluster_registry_verified: bool = False
    durable_generation_verified: bool = False
    cleanup_receipt: bool = False
    cleanup_paths_pending: bool | None = None
    # Compatibility-null only. Full reports are never copied into status responses.
    coordinator_report: None = None
    coordinator_report_ref: OwnedSessionCleanupReportReference | None = None
    coordinator_report_sha256: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{64}$",
    )
    coordinator_report_bound: bool = False
    ownership_verified: bool = False
    recovery_verified: bool = False
    api_release_identity: SessionApiReleaseIdentity | None = None
    api_release_identity_verified: bool = False
    ownership_token_present: bool = False
    admission_status: dict[str, object] | None = None
    # iowarp/clio-relay#277: the owned-session client-liveness lease
    # projected as a plain dict (mirrors admission_status's own shape), so
    # `session recovery-status`'s stdout -- and therefore the SAME single-dial
    # channel bootstrap script that already embeds this whole document
    # (control_channel.py:owned_session_channel_bootstrap_script) -- carries
    # it with zero extra plumbing. Present only when a lease record exists;
    # see queue_owner_session_lease.owner_session_lease_status.
    owner_session_lease_status: dict[str, object] | None = None
    start_state: Literal[
        "unknown",
        "starting",
        "ready",
        "failed",
        "failed_cleaned",
        "not_current",
    ] = "unknown"
    start_phase: Literal["pending", "admitted", "scope_bound", "contained"] | None = None
    start_attempt_verified: bool = False
    start_retryable: bool = False
    start_replace: bool | None = None
    start_require_token: bool | None = None
    start_input_policy: OwnedSessionInputPolicy | None = None
    start_expected_api_release_identity_sha256: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{64}$",
    )
    start_error: str | None = Field(default=None, max_length=MAX_SESSION_START_ERROR_CHARS)
    errors: list[str] = Field(default_factory=list[str])

    @model_validator(mode="after")
    def _validate_coordinator_report_reference(self) -> OwnedSessionRecoveryStatus:
        """Keep the compatibility digest identical to the compact sidecar reference."""
        if (
            self.coordinator_report_ref is not None
            and self.coordinator_report_sha256 != self.coordinator_report_ref.sha256
        ):
            raise ValueError("coordinator report reference digest does not match status")
        return self


class OwnedSessionStartResult(BaseModel):
    """Desktop-visible outcome for a possibly asynchronous remote session start."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["clio-relay.owner-session-start-result.v1"] = (
        "clio-relay.owner-session-start-result.v1"
    )
    cluster: str = Field(min_length=1)
    session_id: str = Field(min_length=1)
    start_operation_id: DurableRecordId
    cluster_route_revision: str = Field(min_length=1)
    session_generation_id: DurableRecordId | None = None
    remote_api_port: int = Field(gt=0, le=65_535)
    state: Literal["ready", "starting", "ambiguous", "failed", "not_current"]
    terminal: bool
    retryable: bool
    usable: bool = False
    watch_deadline_exceeded: bool = False
    transition_accepted: bool | None = None
    transport_deadline_exceeded: bool = False
    running: bool = False
    ownership_verified: bool = False
    recovery_verified: bool = False
    start_phase: Literal["pending", "admitted", "scope_bound", "contained"] | None = None
    error: str | None = Field(default=None, max_length=MAX_SESSION_START_ERROR_CHARS)
    status_selector: OwnedSessionStartStatusSelector
    retry_selector: OwnedSessionStartRetrySelector

    @model_validator(mode="after")
    def _validate_start_result(self) -> OwnedSessionStartResult:
        """Keep state, identity, and the advertised recovery operations exact."""
        if not (
            self.status_selector.cluster == self.cluster
            and self.status_selector.session_id == self.session_id
            and self.status_selector.start_operation_id == self.start_operation_id
            and self.status_selector.cluster_route_revision == self.cluster_route_revision
            and self.status_selector.remote_api_port == self.remote_api_port
            and self.status_selector.replace == self.retry_selector.replace
            and self.status_selector.require_token == self.retry_selector.require_token
            and self.status_selector.input_policy == self.retry_selector.input_policy
            and self.status_selector.expected_api_release_identity_sha256
            == self.retry_selector.expected_api_release_identity_sha256
            and self.retry_selector.cluster == self.cluster
            and self.retry_selector.session_id == self.session_id
            and self.retry_selector.start_operation_id == self.start_operation_id
            and self.retry_selector.cluster_route_revision == self.cluster_route_revision
            and self.retry_selector.remote_api_port == self.remote_api_port
        ):
            raise ValueError("owned-session start selectors changed result identity")
        if self.usable is not (self.state == "ready"):
            raise ValueError("only a ready owned-session result is usable")
        if self.watch_deadline_exceeded and self.terminal:
            raise ValueError("a terminal start result cannot exceed a watch deadline")
        if self.state == "ready":
            if not (
                self.terminal
                and not self.retryable
                and self.transition_accepted is True
                and self.session_generation_id is not None
                and self.ownership_verified
                and self.recovery_verified
            ):
                raise ValueError("ready owned-session start result is incomplete")
        elif self.state == "starting":
            if not (
                not self.terminal
                and self.retryable
                and self.transition_accepted is True
                and self.session_generation_id is not None
                and self.start_phase is not None
            ):
                raise ValueError("starting owned-session result lacks a durable attempt")
        elif self.state == "ambiguous":
            if self.terminal or not self.retryable or self.transition_accepted is not None:
                raise ValueError("ambiguous owned-session result claimed a terminal transition")
        elif self.state == "not_current":
            if (
                not self.terminal
                or self.retryable
                or self.transition_accepted is not None
                or self.error is None
            ):
                raise ValueError("non-current owned-session selector is incomplete")
        elif not self.terminal or self.retryable or self.error is None:
            raise ValueError("failed owned-session start result is incomplete")
        return self


class OwnedSessionStartReceipt(BaseModel):
    """Typed cluster-local receipt for one committed owned-session start."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["clio-relay.owner-session-start-receipt.v1"] = (
        "clio-relay.owner-session-start-receipt.v1"
    )
    cluster: str = Field(min_length=1)
    session_id: str = Field(min_length=1)
    start_operation_id: DurableRecordId
    cluster_route_revision: str = Field(min_length=1)
    session_generation_id: DurableRecordId
    remote_api_port: int = Field(gt=0, le=65_535)
    api_pid: int = Field(gt=0)
    outcome: Literal["started", "already_running", "recovered"]
    ready_seconds: float | None = Field(default=None, ge=0)
    running: Literal[True] = True
    ownership_verified: Literal[True] = True
    recovery_verified: Literal[True] = True
    start_phase: Literal["contained"] = "contained"

    @model_validator(mode="after")
    def _validate_ready_observation(self) -> OwnedSessionStartReceipt:
        """Require a readiness duration only when this operation observed startup."""

        if self.outcome == "already_running" and self.ready_seconds is not None:
            raise ValueError("ready_seconds must be absent for an already-running session")
        if self.outcome != "already_running" and self.ready_seconds is None:
            raise ValueError("ready_seconds is required for a started or recovered session")
        return self


class OwnedSessionStartPlan(BaseModel):
    """Read-only, persistable selector set for one future session start."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["clio-relay.owner-session-start-plan.v1"] = (
        "clio-relay.owner-session-start-plan.v1"
    )
    cluster: str = Field(min_length=1)
    session_id: str = Field(min_length=1)
    start_operation_id: DurableRecordId
    cluster_route_revision: str = Field(min_length=1)
    remote_api_port: int = Field(gt=0, le=65_535)
    input_policy: OwnedSessionInputPolicy = Field(default_factory=OwnedSessionInputPolicy)
    expected_api_release_identity_sha256: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{64}$",
    )
    status_selector: OwnedSessionStartStatusSelector
    retry_selector: OwnedSessionStartRetrySelector

    @model_validator(mode="after")
    def _validate_plan_selectors(self) -> OwnedSessionStartPlan:
        """Require both plan selectors to bind the same immutable request identity."""
        if not (
            self.status_selector.cluster == self.cluster
            and self.status_selector.session_id == self.session_id
            and self.status_selector.start_operation_id == self.start_operation_id
            and self.status_selector.cluster_route_revision == self.cluster_route_revision
            and self.status_selector.remote_api_port == self.remote_api_port
            and self.status_selector.replace == self.retry_selector.replace
            and self.status_selector.require_token == self.retry_selector.require_token
            and self.status_selector.input_policy == self.retry_selector.input_policy
            and self.status_selector.input_policy == self.input_policy
            and self.retry_selector.input_policy == self.input_policy
            and self.status_selector.expected_api_release_identity_sha256
            == self.expected_api_release_identity_sha256
            and self.retry_selector.cluster == self.cluster
            and self.retry_selector.session_id == self.session_id
            and self.retry_selector.start_operation_id == self.start_operation_id
            and self.retry_selector.cluster_route_revision == self.cluster_route_revision
            and self.retry_selector.remote_api_port == self.remote_api_port
            and self.retry_selector.expected_api_release_identity_sha256
            == self.expected_api_release_identity_sha256
        ):
            raise ValueError("owned-session start plan selectors changed identity")
        return self

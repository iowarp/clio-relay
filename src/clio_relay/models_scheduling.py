"""Endpoint registration and cluster-scheduler durable observation records.

Covers a registered relay endpoint's heartbeat identity
(:class:`EndpointRegistration`), the crash-recoverable scheduler-cancellation
retry state (:class:`SchedulerCancelDisposition`, :class:`SchedulerCancelPending`),
and the read-only scheduler/connector observation models
(:class:`SchedulerStatus` and the ``SchedulerConnector*`` family).
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from clio_relay.identifiers import DurableRecordId
from clio_relay.models_enums import (
    EndpointRole,
    JobState,
    SchedulerCancelDispositionState,
    SchedulerPhase,
)
from clio_relay.models_shared import new_id, utc_now


class EndpointRegistration(BaseModel):
    """A registered relay endpoint."""

    model_config = ConfigDict(extra="forbid")

    endpoint_id: DurableRecordId = Field(default_factory=lambda: new_id("endpoint"))
    role: EndpointRole
    cluster: str | None = None
    hostname: str
    pid: int
    registered_at: datetime = Field(default_factory=utc_now)
    last_seen_at: datetime = Field(default_factory=utc_now)
    metadata: dict[str, Any] = Field(default_factory=dict)


class SchedulerCancelDisposition(BaseModel):
    """Retry and terminal evidence for one scheduler job identity."""

    model_config = ConfigDict(extra="forbid")

    scheduler_job_id: str = Field(min_length=1, max_length=256)
    provider: str | None = Field(default=None, min_length=1, max_length=128)
    state: SchedulerCancelDispositionState = SchedulerCancelDispositionState.PENDING
    attempts: int = Field(default=0, ge=0, le=100)
    confirmation_attempts: int = Field(default=0, ge=0, le=100)
    next_attempt_at: datetime | None = None
    last_error: str | None = Field(default=None, max_length=16_384)
    attempt_claim_id: DurableRecordId | None = None
    attempt_claimed_at: datetime | None = None
    attempt_claim_expires_at: datetime | None = None
    confirmation_claim_id: DurableRecordId | None = None
    confirmation_claimed_at: datetime | None = None
    confirmation_claim_expires_at: datetime | None = None
    updated_at: datetime = Field(default_factory=utc_now)

    @model_validator(mode="after")
    def validate_attempt_claim(self) -> SchedulerCancelDisposition:
        """Require one complete, bounded-lifecycle claim on retryable work."""
        claim_values = (
            self.attempt_claim_id,
            self.attempt_claimed_at,
            self.attempt_claim_expires_at,
        )
        populated = sum(value is not None for value in claim_values)
        if populated not in {0, len(claim_values)}:
            raise ValueError("scheduler cancellation attempt claim must be complete")
        if self.attempt_claim_id is None:
            return self
        if self.state not in {
            SchedulerCancelDispositionState.PENDING,
            SchedulerCancelDispositionState.RETRY_WAIT,
        }:
            raise ValueError("scheduler cancellation attempt claim requires retryable state")
        claimed_at = self.attempt_claimed_at
        expires_at = self.attempt_claim_expires_at
        if claimed_at is None or expires_at is None or expires_at <= claimed_at:
            raise ValueError("scheduler cancellation attempt claim must expire after acquisition")
        return self

    @model_validator(mode="after")
    def validate_confirmation_claim(self) -> SchedulerCancelDisposition:
        """Require one complete, bounded-lifecycle claim on confirmation work."""
        claim_values = (
            self.confirmation_claim_id,
            self.confirmation_claimed_at,
            self.confirmation_claim_expires_at,
        )
        populated = sum(value is not None for value in claim_values)
        if populated not in {0, len(claim_values)}:
            raise ValueError("scheduler cancellation confirmation claim must be complete")
        if self.confirmation_claim_id is None:
            return self
        if self.state is not SchedulerCancelDispositionState.CANCEL_REQUESTED:
            raise ValueError(
                "scheduler cancellation confirmation claim requires cancel-requested state"
            )
        claimed_at = self.confirmation_claimed_at
        expires_at = self.confirmation_claim_expires_at
        if claimed_at is None or expires_at is None or expires_at <= claimed_at:
            raise ValueError(
                "scheduler cancellation confirmation claim must expire after acquisition"
            )
        return self


def _empty_scheduler_cancel_dispositions() -> list[SchedulerCancelDisposition]:
    """Return a typed empty scheduler-cancellation disposition collection."""
    return []


class SchedulerCancelPending(BaseModel):
    """Crash-recoverable scheduler cancellation work for one relay job."""

    model_config = ConfigDict(extra="forbid")

    schema_version: str = "clio-relay.scheduler-cancel-pending.v1"
    job_id: DurableRecordId
    cluster: str = Field(min_length=1, max_length=256)
    requested_at: datetime = Field(default_factory=utc_now)
    reason: str = Field(default="operator_request", min_length=1, max_length=256)
    identity_resolution: Literal["pending", "resolved", "none", "superseded"] = "pending"
    dispositions: list[SchedulerCancelDisposition] = Field(
        default_factory=_empty_scheduler_cancel_dispositions,
        max_length=1_000,
    )
    updated_at: datetime = Field(default_factory=utc_now)

    @property
    def complete(self) -> bool:
        """Return whether no further scheduler cancellation work is due."""
        if self.identity_resolution in {"none", "superseded"}:
            return True
        return (
            self.identity_resolution == "resolved"
            and bool(self.dispositions)
            and all(
                item.state
                in {
                    SchedulerCancelDispositionState.CANCELED,
                    SchedulerCancelDispositionState.TERMINAL,
                    SchedulerCancelDispositionState.NOT_FOUND,
                    SchedulerCancelDispositionState.REFUSED,
                    SchedulerCancelDispositionState.EXHAUSTED,
                }
                for item in self.dispositions
            )
        )


class OwnerSessionJobMembership(BaseModel):
    """Durable job membership for one owner-session generation."""

    model_config = ConfigDict(extra="forbid")

    schema_version: str = "clio-relay.owner-session-job-membership.v1"
    owner_session_id: str = Field(min_length=1, max_length=256)
    session_generation_id: DurableRecordId | None = None
    job_id: DurableRecordId
    cluster: str = Field(min_length=1, max_length=256)
    state: JobState
    created_at: datetime
    updated_at: datetime


class SchedulerStatus(BaseModel):
    """Observed scheduler status for a relay task."""

    model_config = ConfigDict(extra="forbid")

    scheduler: str
    scheduler_job_id: str
    phase: SchedulerPhase = SchedulerPhase.UNKNOWN
    record_found: bool | None = None
    active_record_found: bool | None = None
    raw_state: str | None = None
    reason: str | None = None
    partition: str | None = None
    qos: str | None = None
    user: str | None = None
    nodes: int | None = Field(default=None, ge=0)
    cpus: int | None = Field(default=None, ge=0)
    memory: str | None = None
    submit_time: str | None = None
    eligible_time: str | None = None
    start_time: str | None = None
    elapsed: str | None = None
    time_limit: str | None = None
    queue_position: int | None = Field(default=None, ge=1)
    jobs_ahead: int | None = Field(default=None, ge=0)
    queue_position_scope: str | None = None
    queue_position_note: str | None = None
    observed_at: datetime = Field(default_factory=utc_now)


class SchedulerConnectorPlacement(BaseModel):
    """Provider-verified host for a connector inside one exact allocation."""

    model_config = ConfigDict(extra="forbid", strict=True)

    schema_version: Literal["clio-relay.scheduler-connector-placement.v1"] = (
        "clio-relay.scheduler-connector-placement.v1"
    )
    scheduler: str = Field(min_length=1, max_length=256)
    scheduler_job_id: str = Field(min_length=1, max_length=256)
    placement_host: str = Field(min_length=1, max_length=1_024)
    allocation_node_count: Literal[1]
    source: Literal["slurm-scontrol-batch-host"]
    verified: Literal[True]
    observed_at: datetime = Field(default_factory=utc_now)


class SchedulerConnectorStepIdentity(BaseModel):
    """Provider-native identity for one connector task inside an allocation."""

    model_config = ConfigDict(extra="forbid", strict=True)

    schema_version: Literal["clio-relay.scheduler-connector-step.v1"] = (
        "clio-relay.scheduler-connector-step.v1"
    )
    scheduler: str = Field(min_length=1, max_length=256)
    scheduler_job_id: str = Field(min_length=1, max_length=256)
    scheduler_step_id: str = Field(min_length=3, max_length=512)
    step_marker: str = Field(min_length=1, max_length=64)
    placement_host: str = Field(min_length=1, max_length=1_024)
    source: Literal[
        "slurm-srun-detached-marker",
        "slurm-squeue-step-marker",
    ]
    verified: Literal[True]
    observed_at: datetime = Field(default_factory=utc_now)


class SchedulerConnectorStepStatus(BaseModel):
    """Exact provider observation of one allocation-scoped connector step."""

    model_config = ConfigDict(extra="forbid", strict=True)

    schema_version: Literal["clio-relay.scheduler-connector-step-status.v1"] = (
        "clio-relay.scheduler-connector-step-status.v1"
    )
    scheduler: str = Field(min_length=1, max_length=256)
    scheduler_job_id: str = Field(min_length=1, max_length=256)
    scheduler_step_id: str = Field(min_length=3, max_length=512)
    placement_host: str = Field(min_length=1, max_length=1_024)
    record_found: bool
    state: Literal["active", "absent"]
    observed_host: str | None = Field(default=None, min_length=1, max_length=1_024)
    source: Literal["slurm-squeue-steps"] = "slurm-squeue-steps"
    verified: Literal[True]
    observed_at: datetime = Field(default_factory=utc_now)

    @model_validator(mode="after")
    def status_fields_are_consistent(self) -> SchedulerConnectorStepStatus:
        """Reject contradictory active/absent and placement observations."""
        if self.record_found != (self.state == "active"):
            raise ValueError("connector step state does not match record_found")
        if self.record_found and self.observed_host != self.placement_host:
            raise ValueError("active connector step host does not match placement")
        if not self.record_found and self.observed_host is not None:
            raise ValueError("absent connector step cannot report an observed host")
        return self

"""The durable relay job record and its full lifecycle, admission through GC.

Covers :class:`RelayJob` itself, the bounded terminal-wait outcome models
(:class:`WaitObservation`, :class:`JobWaitResult`), the owned-JARVIS-run
execution-ID admission binder (:func:`prepare_owned_jarvis_run_submission`),
and the crash-resumable terminal-job garbage-collection/closure records
(:class:`TerminalJobGcPlan`, :class:`JobTombstone`, :class:`TerminalJobGcResult`,
:class:`OwnerSessionClosure`) that retire a terminal job.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from clio_relay.identifiers import DurableRecordId
from clio_relay.models_artifact_provenance import ArtifactUse, validate_artifact_use_collection
from clio_relay.models_enums import TERMINAL_STATES, JobGcPhase, JobKind, JobState
from clio_relay.models_job_specs import (
    JobSpec,
    McpCallSpec,
    deterministic_jarvis_execution_id,
    is_owned_jarvis_run_spec,
    validate_jarvis_execution_id,
)
from clio_relay.models_shared import new_id, utc_now


def _empty_artifact_uses() -> list[ArtifactUse]:
    """Return a typed empty artifact dependency collection."""
    return []


class StorageReservationEstimate(BaseModel):
    """Validated per-job storage growth reserved before queue admission."""

    model_config = ConfigDict(extra="forbid", strict=True)

    core_bytes: int = Field(ge=0)
    spool_bytes: int = Field(ge=0)

    @model_validator(mode="after")
    def require_nonzero_total(self) -> StorageReservationEstimate:
        """Reject a reservation which provides no bounded growth capacity."""
        if self.core_bytes + self.spool_bytes <= 0:
            raise ValueError("storage reservation must contain at least one byte")
        return self


class RelayJob(BaseModel):
    """A durable relay job record."""

    model_config = ConfigDict(extra="forbid")

    job_id: DurableRecordId = Field(default_factory=lambda: new_id("job"))
    cluster: str
    kind: JobKind
    state: JobState = JobState.QUEUED
    spec: JobSpec
    idempotency_key: str
    owner_session_id: str | None = Field(default=None, min_length=1, max_length=256)
    owner_session_generation_id: DurableRecordId | None = None
    used_artifact_refs: list[ArtifactUse] = Field(
        default_factory=_empty_artifact_uses,
        max_length=1_000,
    )
    submission_digest: str | None = Field(default=None, min_length=64, max_length=64)
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)
    leased_by: str | None = None
    attempts: int = 0
    last_error: str | None = None
    storage_reservation: StorageReservationEstimate | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def owner_session_identity_must_be_complete(self) -> Self:
        """Require the explicit scheduler-attribution identity as one complete pair."""
        if (self.owner_session_id is None) != (self.owner_session_generation_id is None):
            raise ValueError(
                "owner_session_id and owner_session_generation_id must be supplied together"
            )
        return self

    @field_validator("used_artifact_refs")
    @classmethod
    def used_artifact_refs_must_be_unique_and_sorted(
        cls,
        value: list[ArtifactUse],
    ) -> list[ArtifactUse]:
        """Canonicalize dependency order and reject ambiguous duplicate edges."""
        artifact_ids = [item.artifact_id for item in value]
        if len(artifact_ids) != len(set(artifact_ids)):
            raise ValueError("used_artifact_refs must contain unique artifact_id values")
        canonical = sorted(value, key=lambda item: item.artifact_id)
        validate_artifact_use_collection(canonical)
        return canonical


class WaitObservation(BaseModel):
    """Machine-readable outcome of one bounded terminal-state observation."""

    model_config = ConfigDict(extra="forbid")

    outcome: Literal["terminal", "observation_unknown"]
    timeout_seconds: float = Field(gt=0, allow_inf_nan=False)
    scheduler_action: Literal["none"] = "none"
    relay_action: Literal["none"] = "none"


class JobWaitResult(RelayJob):
    """A durable job snapshot plus the outcome of one bounded observation."""

    observation: WaitObservation

    @model_validator(mode="after")
    def observation_must_match_durable_state(self) -> Self:
        """Reject contradictory terminal claims from local or remote wait surfaces."""
        expected = "terminal" if self.state in TERMINAL_STATES else "observation_unknown"
        if self.observation.outcome != expected:
            raise ValueError("wait observation outcome disagrees with durable job state")
        return self


def prepare_owned_jarvis_run_submission(job: RelayJob) -> RelayJob:
    """Bind a newly admitted trusted JARVIS run to one durable execution ID.

    This is intentionally an admission operation rather than a model validator.
    Durable jobs written by older relay releases must remain readable during an
    upgrade even when their historical public contract exposed ``wait`` or did
    not yet accept a caller-owned ``execution_id``.
    """
    if not is_owned_jarvis_run_spec(job.kind, job.spec):
        return job
    assert isinstance(job.spec, McpCallSpec)
    if "wait" in job.spec.arguments:
        raise ValueError(
            "trusted jarvis_run does not accept internal wait; query the returned "
            "execution with jarvis_get_execution"
        )
    expected_execution_id = deterministic_jarvis_execution_id(
        cluster=job.cluster,
        idempotency_key=job.idempotency_key,
        job_id=job.job_id,
    )
    supplied_execution_id = job.spec.arguments.get("execution_id")
    if supplied_execution_id is not None:
        validated_execution_id = validate_jarvis_execution_id(supplied_execution_id)
        if validated_execution_id != expected_execution_id:
            raise ValueError(
                "trusted jarvis_run execution_id must match the relay-owned "
                "cluster and idempotency identity"
            )
    execution_id = expected_execution_id
    return job.model_copy(
        update={
            "spec": job.spec.model_copy(
                update={
                    "arguments": {
                        **job.spec.arguments,
                        "execution_id": execution_id,
                    }
                }
            )
        }
    )


class TerminalJobGcPlan(BaseModel):
    """A fail-closed dry-run decision for one terminal job."""

    model_config = ConfigDict(extra="forbid")

    schema_version: str = "clio-relay.terminal-job-gc-plan.v1"
    job_id: DurableRecordId
    expected_updated_at: datetime
    eligible: bool
    protections: list[str] = Field(default_factory=list)
    planned_at: datetime = Field(default_factory=utc_now)


class JobTombstone(BaseModel):
    """Compact durable identity retained after terminal job collection."""

    model_config = ConfigDict(extra="forbid")

    schema_version: str = "clio-relay.job-tombstone.v1"
    job_id: DurableRecordId
    cluster: str
    kind: JobKind
    final_state: JobState
    idempotency_key: str
    job_digest: str
    created_at: datetime
    updated_at: datetime
    attempts: int = Field(default=0, ge=0)
    last_error: str | None = None
    external_quarantine_id: str = Field(min_length=1, max_length=512)
    phase: JobGcPhase = JobGcPhase.PREPARED
    gc_started_at: datetime = Field(default_factory=utc_now)
    gc_updated_at: datetime = Field(default_factory=utc_now)
    removed_records: int = Field(default=0, ge=0)
    records_trash_started: bool = False
    monitor_cursor: str | None = None
    monitor_scan_complete: bool = False


class TerminalJobGcResult(BaseModel):
    """Bounded progress from a dry-run or executable terminal-job GC call."""

    model_config = ConfigDict(extra="forbid")

    schema_version: str = "clio-relay.terminal-job-gc-result.v1"
    plan: TerminalJobGcPlan
    dry_run: bool = True
    phase: JobGcPhase | None = None
    complete: bool = False
    actions: int = Field(default=0, ge=0, le=100)
    tombstone: JobTombstone | None = None


class OwnerSessionClosure(BaseModel):
    """Verified terminal ownership state written only after session teardown."""

    model_config = ConfigDict(extra="forbid")

    schema_version: str = "clio-relay.owner-session-closure.v1"
    owner_session_id: str = Field(min_length=1, max_length=256)
    session_generation_id: DurableRecordId | None = None
    covered_by_session_generation_id: DurableRecordId | None = None
    covered_legacy_job_ids: list[DurableRecordId] = Field(default_factory=list, max_length=1_000)
    residual_resource_ids: list[str] = Field(default_factory=list, max_length=1_000)
    closed_at: datetime = Field(default_factory=utc_now)

    @model_validator(mode="after")
    def validate_generation_coverage(self) -> OwnerSessionClosure:
        """Keep generation closures and bounded legacy coverage unambiguous."""
        if self.session_generation_id is not None:
            if self.covered_by_session_generation_id is not None or self.covered_legacy_job_ids:
                raise ValueError("generation closures cannot contain legacy coverage")
            return self
        if not self.covered_by_session_generation_id:
            raise ValueError("legacy closure requires a covering generation")
        if not self.covered_legacy_job_ids:
            raise ValueError("legacy closure requires at least one exact job id")
        if self.covered_legacy_job_ids != sorted(set(self.covered_legacy_job_ids)):
            raise ValueError("legacy closure job ids must be unique and sorted")
        if self.residual_resource_ids:
            raise ValueError("legacy closure cannot retain residual resources")
        return self

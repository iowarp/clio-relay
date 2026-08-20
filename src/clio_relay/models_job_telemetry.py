"""Small durable per-job telemetry and cursor/lease records.

Lightweight records that reference a job or task by ID but carry no
provenance or spec logic of their own: sub-job task rows
(:class:`RelayTask`), the monotonic event/timeline streams
(:class:`RelayEvent`, :class:`TaskTimelineEvent`), progress observations
(:class:`ProgressRecord`), observer rules (:class:`MonitorRule`), and the
event-stream cursor/short-lived job lease (:class:`Cursor`, :class:`Lease`).
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

from clio_relay.identifiers import DurableRecordId
from clio_relay.models_enums import EventLevel, JobState, MonitorRuleAction, TaskEventStatus
from clio_relay.models_shared import new_id, utc_now


class RelayTask(BaseModel):
    """A durable task record belonging to a job."""

    model_config = ConfigDict(extra="forbid")

    task_id: DurableRecordId = Field(default_factory=lambda: new_id("task"))
    job_id: DurableRecordId
    sequence: int | None = Field(default=None, ge=1)
    name: str
    state: JobState = JobState.QUEUED
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)
    metadata: dict[str, Any] = Field(default_factory=dict)


class RelayEvent(BaseModel):
    """A per-job monotonic event."""

    model_config = ConfigDict(extra="forbid")

    job_id: DurableRecordId
    seq: int
    event_type: str
    message: str
    level: EventLevel = EventLevel.INFO
    created_at: datetime = Field(default_factory=utc_now)
    payload: dict[str, Any] = Field(default_factory=dict)


class TaskTimelineEvent(BaseModel):
    """A resumable structured timeline event for one relay task."""

    model_config = ConfigDict(extra="forbid")

    task_id: DurableRecordId
    seq: int = Field(default=0, ge=0)
    event_type: str
    label: str
    status: TaskEventStatus = TaskEventStatus.RUNNING
    summary: str
    detail: str | None = None
    artifact_refs: list[DurableRecordId] = Field(default_factory=list)
    path_refs: list[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=utc_now)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("event_type", "label", "summary")
    @classmethod
    def timeline_text_must_not_be_empty(cls, value: str) -> str:
        """Reject empty timeline fields used by UI labels."""
        if value == "":
            raise ValueError("timeline text fields must not be empty")
        return value


class ProgressRecord(BaseModel):
    """A durable job progress observation."""

    model_config = ConfigDict(extra="forbid")

    progress_id: DurableRecordId = Field(default_factory=lambda: new_id("progress"))
    job_id: DurableRecordId
    sequence: int | None = Field(default=None, ge=1)
    label: str = "progress"
    current: float | None = None
    total: float | None = Field(default=None, gt=0)
    unit: str | None = None
    message: str | None = None
    source_event_seq: int | None = Field(default=None, ge=1)
    created_at: datetime = Field(default_factory=utc_now)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @property
    def source(self) -> str:
        """Return the provenance source for this progress observation."""
        value = self.metadata.get("source")
        return value if isinstance(value, str) else "unknown"

    @field_validator("label")
    @classmethod
    def label_must_not_be_empty(cls, value: str) -> str:
        """Reject empty progress labels."""
        if value == "":
            raise ValueError("label must not be empty")
        return value


class MonitorRule(BaseModel):
    """A durable observer rule over a job event stream."""

    model_config = ConfigDict(extra="forbid")

    rule_id: DurableRecordId = Field(default_factory=lambda: new_id("rule"))
    job_id: DurableRecordId
    pattern: str
    action: MonitorRuleAction = MonitorRuleAction.EMIT_EVENT
    event_types: list[str] = Field(default_factory=list)
    next_seq: int = Field(default=1, ge=1)
    enabled: bool = True
    triggered_at: datetime | None = None
    created_at: datetime = Field(default_factory=utc_now)
    metadata: dict[str, Any] = Field(default_factory=dict)
    action_payload: dict[str, Any] = Field(default_factory=dict)


class Cursor(BaseModel):
    """A cursor into a job event stream."""

    model_config = ConfigDict(extra="forbid")

    job_id: DurableRecordId
    next_seq: int = Field(default=1, ge=1)


class Lease(BaseModel):
    """A short-lived job lease."""

    model_config = ConfigDict(extra="forbid")

    lease_id: DurableRecordId = Field(default_factory=lambda: new_id("lease"))
    job_id: DurableRecordId
    endpoint_id: DurableRecordId
    acquired_at: datetime = Field(default_factory=utc_now)
    expires_at: datetime

    @classmethod
    def new(cls, job_id: str, endpoint_id: str, ttl_seconds: int) -> Lease:
        """Create a lease with a relative TTL."""
        now = utc_now()
        return cls(
            job_id=job_id,
            endpoint_id=endpoint_id,
            acquired_at=now,
            expires_at=now + timedelta(seconds=ttl_seconds),
        )

    def is_expired(self, now: datetime | None = None) -> bool:
        """Return whether this lease is expired."""
        return (now or utc_now()) >= self.expires_at

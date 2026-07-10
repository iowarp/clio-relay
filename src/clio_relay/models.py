"""Typed relay records shared by CLI, HTTP, endpoints, and tests."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import Annotated, Any
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


def utc_now() -> datetime:
    """Return an aware UTC timestamp."""
    return datetime.now(UTC)


def new_id(prefix: str) -> str:
    """Create a readable relay identifier."""
    return f"{prefix}_{uuid4().hex}"


class EndpointRole(StrEnum):
    """Long-running endpoint roles."""

    DESKTOP = "desktop"
    WORKER = "worker"


class JobKind(StrEnum):
    """Supported top-level job intent kinds."""

    JARVIS = "jarvis"
    REMOTE_AGENT = "remote_agent"
    MCP_CALL = "mcp_call"


class JobState(StrEnum):
    """Durable job states."""

    QUEUED = "queued"
    LEASED = "leased"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELED = "canceled"


TERMINAL_STATES = {JobState.SUCCEEDED, JobState.FAILED, JobState.CANCELED}


class SchedulerPhase(StrEnum):
    """Cluster scheduler phase for a task."""

    SUBMITTED = "submitted"
    PENDING = "pending"
    ALLOCATED = "allocated"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELED = "canceled"
    UNKNOWN = "unknown"


class MonitorRuleAction(StrEnum):
    """Actions a monitor rule can take when it matches an event."""

    EMIT_EVENT = "emit_event"
    SUBMIT_AGENT = "submit_agent"
    RECORD_PROGRESS = "record_progress"


class EventLevel(StrEnum):
    """Event severity levels."""

    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


class TaskEventStatus(StrEnum):
    """Structured status for task timeline events."""

    PLANNED = "planned"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    WARNING = "warning"
    ERROR = "error"
    CANCELED = "canceled"


class GatewaySessionState(StrEnum):
    """Durable lifecycle state for a scheduler-backed service session."""

    CREATED = "created"
    SUBMITTED = "submitted"
    PENDING = "pending"
    ALLOCATED = "allocated"
    STARTING = "starting"
    READY = "ready"
    DEGRADED = "degraded"
    FAILED = "failed"
    CLOSED = "closed"
    UNKNOWN = "unknown"


class EndpointRegistration(BaseModel):
    """A registered relay endpoint."""

    model_config = ConfigDict(extra="forbid")

    endpoint_id: str = Field(default_factory=lambda: new_id("endpoint"))
    role: EndpointRole
    cluster: str | None = None
    hostname: str
    pid: int
    registered_at: datetime = Field(default_factory=utc_now)
    last_seen_at: datetime = Field(default_factory=utc_now)
    metadata: dict[str, Any] = Field(default_factory=dict)


class JarvisRunSpec(BaseModel):
    """A JARVIS-CD run intent submitted through the relay."""

    model_config = ConfigDict(extra="forbid")

    pipeline_yaml: str | None = None
    pipeline_path: Path | None = None
    pipeline_name: str | None = None
    package: str | None = None
    command: list[str] | None = None
    workdir: Path | None = None
    env: dict[str, str] = Field(default_factory=dict)
    timeout_seconds: int | None = Field(default=None, gt=0)
    progress: dict[str, Any] = Field(default_factory=dict)

    @field_validator("command")
    @classmethod
    def command_must_not_be_empty(cls, value: list[str] | None) -> list[str] | None:
        """Reject empty command arrays."""
        if value == []:
            raise ValueError("command must not be empty")
        return value

    @model_validator(mode="after")
    def exactly_one_pipeline_source(self) -> JarvisRunSpec:
        """Require a single source for a JARVIS run."""
        sources = [
            self.pipeline_yaml is not None,
            self.pipeline_path is not None,
            self.pipeline_name is not None,
            self.command is not None,
        ]
        if sum(1 for item in sources if item) != 1:
            raise ValueError(
                "exactly one of pipeline_yaml, pipeline_path, pipeline_name, or command is required"
            )
        return self


class RemoteAgentTaskSpec(BaseModel):
    """A remote agent task to execute on a cluster through JARVIS-CD."""

    model_config = ConfigDict(extra="forbid")

    prompt_path: str
    mcp_config_path: str | None = None
    model: str | None = None
    workdir: str | None = None
    timeout_seconds: int | None = Field(default=None, gt=0)
    context: dict[str, Any] = Field(default_factory=dict)


class McpCallSpec(BaseModel):
    """A remote MCP tool call request."""

    model_config = ConfigDict(extra="forbid")

    server: str
    server_args: list[str] = Field(default_factory=list)
    tool: str
    arguments: dict[str, Any] = Field(default_factory=dict)
    timeout_seconds: int | None = Field(default=None, gt=0)


JobSpec = Annotated[
    JarvisRunSpec | RemoteAgentTaskSpec | McpCallSpec,
    Field(union_mode="left_to_right"),
]


class RelayJob(BaseModel):
    """A durable relay job record."""

    model_config = ConfigDict(extra="forbid")

    job_id: str = Field(default_factory=lambda: new_id("job"))
    cluster: str
    kind: JobKind
    state: JobState = JobState.QUEUED
    spec: JobSpec
    idempotency_key: str
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)
    leased_by: str | None = None
    attempts: int = 0
    last_error: str | None = None


class RelayTask(BaseModel):
    """A durable task record belonging to a job."""

    model_config = ConfigDict(extra="forbid")

    task_id: str = Field(default_factory=lambda: new_id("task"))
    job_id: str
    name: str
    state: JobState = JobState.QUEUED
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)
    metadata: dict[str, Any] = Field(default_factory=dict)


class SchedulerStatus(BaseModel):
    """Observed scheduler status for a relay task."""

    model_config = ConfigDict(extra="forbid")

    scheduler: str = "slurm"
    scheduler_job_id: str
    phase: SchedulerPhase = SchedulerPhase.UNKNOWN
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


class RelayEvent(BaseModel):
    """A per-job monotonic event."""

    model_config = ConfigDict(extra="forbid")

    job_id: str
    seq: int
    event_type: str
    message: str
    level: EventLevel = EventLevel.INFO
    created_at: datetime = Field(default_factory=utc_now)
    payload: dict[str, Any] = Field(default_factory=dict)


class TaskTimelineEvent(BaseModel):
    """A resumable structured timeline event for one relay task."""

    model_config = ConfigDict(extra="forbid")

    task_id: str
    seq: int = Field(default=0, ge=0)
    event_type: str
    label: str
    status: TaskEventStatus = TaskEventStatus.RUNNING
    summary: str
    detail: str | None = None
    artifact_refs: list[str] = Field(default_factory=list)
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


class ArtifactRef(BaseModel):
    """A durable artifact index entry."""

    model_config = ConfigDict(extra="forbid")

    artifact_id: str = Field(default_factory=lambda: new_id("artifact"))
    job_id: str
    uri: str
    kind: str
    size_bytes: int | None = Field(default=None, ge=0)
    sha256: str | None = None
    created_at: datetime = Field(default_factory=utc_now)
    metadata: dict[str, Any] = Field(default_factory=dict)


class ProgressRecord(BaseModel):
    """A durable job progress observation."""

    model_config = ConfigDict(extra="forbid")

    progress_id: str = Field(default_factory=lambda: new_id("progress"))
    job_id: str
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

    rule_id: str = Field(default_factory=lambda: new_id("rule"))
    job_id: str
    pattern: str
    action: MonitorRuleAction = MonitorRuleAction.EMIT_EVENT
    event_types: list[str] = Field(default_factory=list)
    next_seq: int = Field(default=1, ge=1)
    enabled: bool = True
    triggered_at: datetime | None = None
    created_at: datetime = Field(default_factory=utc_now)
    metadata: dict[str, Any] = Field(default_factory=dict)
    action_payload: dict[str, Any] = Field(default_factory=dict)


class GatewaySession(BaseModel):
    """Durable state for a scheduler-backed gateway or visualization service."""

    model_config = ConfigDict(extra="forbid")

    session_id: str = Field(default_factory=lambda: new_id("gateway"))
    cluster: str
    name: str
    state: GatewaySessionState = GatewaySessionState.CREATED
    scheduler: str = "slurm"
    scheduler_job_id: str | None = None
    queue_state: str | None = None
    node: str | None = None
    requested_resources: dict[str, Any] = Field(default_factory=dict)
    submit_time: datetime | None = None
    start_time: datetime | None = None
    expected_expiry: datetime | None = None
    stdout_uri: str | None = None
    stderr_uri: str | None = None
    log_uris: list[str] = Field(default_factory=list)
    gateway: dict[str, Any] = Field(default_factory=dict)
    artifacts: list[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("cluster", "name")
    @classmethod
    def gateway_text_must_not_be_empty(cls, value: str) -> str:
        """Reject empty session labels."""
        if value == "":
            raise ValueError("cluster and name must not be empty")
        return value


class ServiceRuntimeSpec(BaseModel):
    """Generic runtime supervisor intent for a scheduler-backed remote service."""

    model_config = ConfigDict(extra="forbid")

    kind: str
    submit_command: list[str]
    status_command: list[str] | None = None
    cancel_command: list[str] | None = None
    deployment_driver: str = "jarvis"
    service_port: int = Field(gt=0, le=65535)
    health_path: str = "/healthz"
    stream_mode: str = "push"
    stream_path: str | None = "/stream"
    event_stream_path: str | None = "/events"
    state_path: str | None = "/state"
    compatibility_paths: dict[str, str] = Field(default_factory=dict)
    desktop_bind_addr: str = "127.0.0.1"
    desktop_bind_port: int = Field(gt=0, le=65535)
    proxy_name: str | None = None
    transport_mode: str = "frp-stcp-wss"
    readiness_timeout_seconds: float = Field(default=300.0, gt=0)
    poll_seconds: float = Field(default=2.0, gt=0)
    scheduler: str = "external"
    connect_url_template: str = "http://{bind_addr}:{bind_port}"
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("kind")
    @classmethod
    def service_runtime_kind_must_not_be_empty(cls, value: str) -> str:
        """Reject empty service runtime kinds."""
        if value == "":
            raise ValueError("kind must not be empty")
        return value

    @field_validator("submit_command")
    @classmethod
    def service_runtime_command_must_not_be_empty(cls, value: list[str]) -> list[str]:
        """Reject empty scheduler submission commands."""
        if not value:
            raise ValueError("submit_command must not be empty")
        return value

    @field_validator("status_command", "cancel_command")
    @classmethod
    def service_runtime_optional_commands_must_not_be_empty(
        cls,
        value: list[str] | None,
    ) -> list[str] | None:
        """Reject empty optional command arrays."""
        if value == []:
            raise ValueError("optional command arrays must not be empty")
        return value

    @field_validator("deployment_driver")
    @classmethod
    def service_runtime_deployment_driver_must_be_known(cls, value: str) -> str:
        """Restrict deployment driver labels to supported supervisor contracts."""
        if value not in {"jarvis", "scheduler", "custom"}:
            raise ValueError("deployment_driver must be jarvis, scheduler, or custom")
        return value

    @field_validator("stream_mode")
    @classmethod
    def service_runtime_stream_mode_must_be_known(cls, value: str) -> str:
        """Restrict stream mode labels to supported runtime semantics."""
        if value not in {"push", "pull", "hybrid"}:
            raise ValueError("stream_mode must be push, pull, or hybrid")
        return value

    @field_validator(
        "health_path",
        "stream_path",
        "event_stream_path",
        "state_path",
    )
    @classmethod
    def service_runtime_paths_must_be_absolute(cls, value: str | None) -> str | None:
        """Require HTTP paths to be absolute when present."""
        if value is not None and not value.startswith("/"):
            raise ValueError("service runtime HTTP paths must start with /")
        return value

    @field_validator("compatibility_paths")
    @classmethod
    def service_runtime_compatibility_paths_must_be_absolute(
        cls,
        value: dict[str, str],
    ) -> dict[str, str]:
        """Require named compatibility endpoints to be absolute HTTP paths."""
        for name, path in value.items():
            if not name:
                raise ValueError("compatibility path names must not be empty")
            if not path.startswith("/"):
                raise ValueError("compatibility paths must start with /")
        return value


class Cursor(BaseModel):
    """A cursor into a job event stream."""

    model_config = ConfigDict(extra="forbid")

    job_id: str
    next_seq: int = Field(default=1, ge=1)


class Lease(BaseModel):
    """A short-lived job lease."""

    model_config = ConfigDict(extra="forbid")

    lease_id: str = Field(default_factory=lambda: new_id("lease"))
    job_id: str
    endpoint_id: str
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

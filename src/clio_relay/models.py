"""Typed relay records shared by CLI, HTTP, endpoints, and tests."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import Annotated, Any, Literal
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from clio_relay.identifiers import DurableRecordId, validate_durable_record_id

RELAY_CREDENTIAL_ENV_NAMES = frozenset(
    {
        "CLIO_RELAY_API_TOKEN",
        "CLIO_RELAY_FRP_TOKEN",
        "CLIO_RELAY_PROGRESS_TOKEN",
        "CLIO_RELAY_RUNTIME_METADATA_TOKEN",
        "CLIO_RELAY_STCP_SECRET",
    }
)


def validate_mcp_env_from(value: dict[str, str]) -> dict[str, str]:
    """Validate child-to-source environment references without resolving values."""
    for child_name, source_name in value.items():
        if not _valid_environment_name(child_name) or not _valid_environment_name(source_name):
            raise ValueError("MCP env_from keys and values must be environment names")
        forbidden = {
            name
            for name in (child_name, source_name)
            if name in RELAY_CREDENTIAL_ENV_NAMES
            or (
                name.startswith("CLIO_RELAY_")
                and (name.endswith("_TOKEN") or name.endswith("_SECRET"))
            )
        }
        if forbidden:
            credential = sorted(forbidden)[0]
            raise ValueError(f"MCP env_from cannot expose relay credential {credential}")
    return value


def _valid_environment_name(value: str) -> bool:
    return (
        bool(value)
        and (value[0].isalpha() or value[0] == "_")
        and all(character.isalnum() or character == "_" for character in value)
    )


def utc_now() -> datetime:
    """Return an aware UTC timestamp."""
    return datetime.now(UTC)


def new_id(prefix: str) -> str:
    """Create a readable portable relay identifier."""
    return validate_durable_record_id(f"{prefix}_{uuid4().hex}")


class EndpointRole(StrEnum):
    """Long-running endpoint roles."""

    DESKTOP = "desktop"
    WORKER = "worker"


class JobKind(StrEnum):
    """Supported top-level job intent kinds."""

    JARVIS = "jarvis"
    REMOTE_AGENT = "remote_agent"
    MCP_CALL = "mcp_call"


class McpOperation(StrEnum):
    """Supported durable operations against a remote MCP server."""

    TOOLS_CALL = "tools/call"
    TOOLS_LIST = "tools/list"


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


class SchedulerCancelDispositionState(StrEnum):
    """Durable disposition for one requested scheduler cancellation."""

    PENDING = "pending"
    RETRY_WAIT = "retry_wait"
    CANCEL_REQUESTED = "cancel_requested"
    CANCELED = "canceled"
    TERMINAL = "terminal"
    NOT_FOUND = "not_found"
    REFUSED = "refused"
    EXHAUSTED = "exhausted"


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
    """A remote MCP tool call or discovery request."""

    model_config = ConfigDict(extra="forbid")

    server: str
    server_args: list[str] = Field(default_factory=list)
    env_from: dict[str, str] = Field(default_factory=dict)
    expected_server_artifact_digest: str | None = None
    operation: McpOperation = McpOperation.TOOLS_CALL
    tool: str | None = None
    arguments: dict[str, Any] = Field(default_factory=dict)
    timeout_seconds: int | None = Field(default=None, gt=0)

    @field_validator("env_from")
    @classmethod
    def validate_environment_references(cls, value: dict[str, str]) -> dict[str, str]:
        """Reject invalid names and relay-owned credential references."""
        return validate_mcp_env_from(value)

    @field_validator("expected_server_artifact_digest")
    @classmethod
    def validate_expected_server_artifact_digest(cls, value: str | None) -> str | None:
        """Require a canonical SHA-256 when a call is bound to discovery identity."""
        if value is None:
            return None
        normalized = value.lower()
        if len(normalized) != 64 or any(
            character not in "0123456789abcdef" for character in normalized
        ):
            raise ValueError("expected_server_artifact_digest must be a SHA-256")
        return normalized

    @model_validator(mode="after")
    def validate_operation_contract(self) -> McpCallSpec:
        """Require call-only fields and keep discovery requests unambiguous."""
        if self.operation == McpOperation.TOOLS_CALL:
            if self.tool is None or not self.tool:
                raise ValueError("tool is required for tools/call")
            return self
        if self.tool is not None:
            raise ValueError("tool must be omitted for tools/list")
        if self.arguments:
            raise ValueError("arguments must be empty for tools/list")
        return self


JobSpec = Annotated[
    JarvisRunSpec | RemoteAgentTaskSpec | McpCallSpec,
    Field(union_mode="left_to_right"),
]


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
    submission_digest: str | None = Field(default=None, min_length=64, max_length=64)
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)
    leased_by: str | None = None
    attempts: int = 0
    last_error: str | None = None
    storage_reservation: StorageReservationEstimate | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


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


class SchedulerStatus(BaseModel):
    """Observed scheduler status for a relay task."""

    model_config = ConfigDict(extra="forbid")

    scheduler: str
    scheduler_job_id: str
    phase: SchedulerPhase = SchedulerPhase.UNKNOWN
    record_found: bool | None = None
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


class ArtifactRef(BaseModel):
    """A durable artifact index entry."""

    model_config = ConfigDict(extra="forbid")

    artifact_id: DurableRecordId = Field(default_factory=lambda: new_id("artifact"))
    job_id: DurableRecordId
    sequence: int | None = Field(default=None, ge=1)
    uri: str
    kind: str
    size_bytes: int | None = Field(default=None, ge=0)
    sha256: str | None = None
    created_at: datetime = Field(default_factory=utc_now)
    metadata: dict[str, Any] = Field(default_factory=dict)


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


class GatewaySession(BaseModel):
    """Durable state for a scheduler-backed gateway or visualization service."""

    model_config = ConfigDict(extra="forbid")

    session_id: DurableRecordId = Field(default_factory=lambda: new_id("gateway"))
    cluster: str
    name: str
    state: GatewaySessionState = GatewaySessionState.CREATED
    scheduler: str = "external"
    scheduler_job_id: str | None = None
    queue_state: str | None = None
    node: str | None = None
    requested_resources: dict[str, Any] = Field(default_factory=dict)
    submit_time: datetime | None = None
    start_time: datetime | None = None
    expected_expiry: datetime | None = None
    stdout_uri: str | None = None
    stderr_uri: str | None = None
    log_uris: list[str] = Field(default_factory=list, max_length=1_000)
    gateway: dict[str, Any] = Field(default_factory=dict)
    artifacts: list[str] = Field(default_factory=list, max_length=1_000)
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


class JobGcPhase(StrEnum):
    """Crash-resumable phases for retiring one terminal job."""

    PREPARED = "prepared"
    IDEMPOTENCY_RETIRED = "idempotency_retired"
    RECORDS_TRASHED = "records_trashed"
    REFERENCES_TRASHED = "references_trashed"
    PURGING = "purging"
    COMPLETE = "complete"


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
    health_expected_body: str | None = Field(default=None, max_length=4096)
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

    @field_validator("health_expected_body")
    @classmethod
    def service_runtime_health_body_must_not_be_empty(
        cls,
        value: str | None,
    ) -> str | None:
        """Reject an empty exact health-response body assertion."""
        if value == "":
            raise ValueError("health_expected_body must not be empty")
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

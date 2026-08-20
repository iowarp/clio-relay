"""Durable state-machine enums shared across the relay's typed records.

These ``StrEnum`` types describe lifecycle state (job/task/scheduler/gateway
phases, event levels, monitor-rule actions) rather than any one domain's
provenance or wire shape, so they are grouped separately from the domain
models that reference them.
"""

from __future__ import annotations

from enum import StrEnum


class EndpointRole(StrEnum):
    """Long-running endpoint roles."""

    DESKTOP = "desktop"
    WORKER = "worker"


class JobKind(StrEnum):
    """Supported top-level job intent kinds."""

    JARVIS = "jarvis"
    REMOTE_AGENT = "remote_agent"
    MCP_CALL = "mcp_call"
    INPUT_INGEST = "input_ingest"


class McpOperation(StrEnum):
    """Supported durable operations against a remote MCP server."""

    TOOLS_CALL = "tools/call"
    TOOLS_LIST = "tools/list"


class McpAdmissionClass(StrEnum):
    """Durable worker-lane admission assigned to one remote MCP operation.

    ``control_query`` is a privileged scheduling assertion.  Generic callers
    must remain on ``workload``; trusted ingress may promote an artifact-bound,
    non-destructive read operation after validating its registered contract.
    """

    WORKLOAD = "workload"
    CONTROL_QUERY = "control_query"


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


class JobGcPhase(StrEnum):
    """Crash-resumable phases for retiring one terminal job."""

    PREPARED = "prepared"
    IDEMPOTENCY_RETIRED = "idempotency_retired"
    RECORDS_TRASHED = "records_trashed"
    REFERENCES_TRASHED = "references_trashed"
    PURGING = "purging"
    COMPLETE = "complete"

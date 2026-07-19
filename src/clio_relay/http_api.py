"""HTTP API for desktop-facing relay operations."""

# pyright: reportUnusedFunction=false

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import secrets
from collections.abc import AsyncGenerator, AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import Annotated, TypeVar, cast

from fastapi import (
    Depends,
    FastAPI,
    Header,
    HTTPException,
    Query,
    WebSocket,
    WebSocketDisconnect,
    WebSocketException,
    status,
)
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

from clio_relay.cluster_config import (
    CLUSTER_REGISTRY_ENV,
    MAX_CLUSTER_REGISTRY_BYTES,
    ClusterDefinition,
    ClusterRegistry,
    cluster_route_revision,
    default_registry_path,
    read_bounded_configuration_bytes,
)
from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.errors import ConfigurationError, NotFoundError, QueueConflictError
from clio_relay.identifiers import DurableRecordId
from clio_relay.jarvis_mcp import (
    is_virtual_jarvis_control_query,
    jarvis_cd_lock_binding_expectation,
    jarvis_mcp_artifact_binding,
    jarvis_mcp_env_from,
    jarvis_mcp_server,
    jarvis_mcp_server_args,
)
from clio_relay.models import (
    MCP_ADMISSION_AUTHORITY_METADATA_KEY,
    ArtifactRef,
    ArtifactUse,
    Cursor,
    GatewaySession,
    GatewaySessionState,
    JarvisRunSpec,
    JobKind,
    JobState,
    McpAdmissionAuthority,
    McpAdmissionClass,
    McpCallSpec,
    McpControlQueryEvidence,
    McpOperation,
    MonitorRule,
    ProgressRecord,
    RelayEvent,
    RelayJob,
    RelayTask,
    RemoteAgentTaskSpec,
    TaskEventStatus,
    TaskTimelineEvent,
    new_id,
    validate_mcp_env_from,
)
from clio_relay.pagination import (
    DEFAULT_RESPONSE_PAGE_RECORDS,
    MAX_RESPONSE_PAGE_RECORDS,
    validate_response_page_limit,
)
from clio_relay.progress_provenance import external_progress_metadata
from clio_relay.queue_management import (
    DEFAULT_STALE_SCAN_LIMIT,
    cancel_queue_job,
    cleanup_stale_jobs,
    diagnose_job,
    diagnose_queue,
    discover_stale_jobs,
    list_queue_jobs,
    worker_status,
)
from clio_relay.relay_ops import (
    cancel_job as request_cancel_job,
)
from clio_relay.relay_ops import (
    evaluate_monitor_rules,
    monitor_job,
    read_artifact_bytes,
    read_job_log,
    wait_for_terminal,
)
from clio_relay.relay_ops import (
    job_status as get_job_status_operation,
)
from clio_relay.remote_mcp import (
    MAX_PINNED_CONTROL_QUERY_TIMEOUT_SECONDS,
    resolve_pinned_mcp_admission,
    resolve_registered_remote_mcp_admission,
)
from clio_relay.retention import TerminalRetentionCoordinator
from clio_relay.session_api import (
    OWNER_SESSION_ID_HEADER,
    SESSION_GENERATION_ID_HEADER,
    session_identity_document,
)
from clio_relay.storage_runtime import StorageAdmissionError, storage_managed_queue
from clio_relay.validation_report import redact_sensitive_values

ModelRecord = TypeVar("ModelRecord", bound=BaseModel)


def _public_record(record: ModelRecord) -> ModelRecord:  # noqa: UP047
    """Return a response copy with nested capability values redacted."""
    original = record.model_dump(mode="json")
    payload = _restore_environment_references(original, redact_sensitive_values(original))
    return type(record).model_validate(payload)


def _public_payload(payload: dict[str, object]) -> dict[str, object]:
    """Redact nested capability values from a free-form HTTP payload."""
    redacted = _restore_environment_references(payload, redact_sensitive_values(payload))
    return cast(dict[str, object], redacted)


def _public_model_page(  # noqa: UP047
    record_key: str,
    records: list[ModelRecord],
    *,
    cursor: int,
    limit: int,
    next_cursor: int | None,
    total: int,
) -> dict[str, object]:
    """Return a redacted, stable one-based model collection page."""
    return {
        record_key: [record.model_dump(mode="json") for record in records],
        "cursor": cursor,
        "limit": limit,
        "next_cursor": next_cursor,
        "total": total,
    }


def _restore_environment_references(original: object, redacted: object) -> object:
    """Keep non-secret env_from variable names valid after capability redaction."""
    if isinstance(original, dict) and isinstance(redacted, dict):
        original_mapping = cast(dict[object, object], original)
        redacted_mapping = cast(dict[object, object], redacted)
        restored: dict[object, object] = {}
        for key, value in redacted_mapping.items():
            original_value = original_mapping.get(key)
            restored[key] = (
                original_value
                if key == "env_from" and isinstance(original_value, dict)
                else _restore_environment_references(original_value, value)
            )
        return restored
    if isinstance(original, list) and isinstance(redacted, list):
        original_values = cast(list[object], original)
        redacted_values = cast(list[object], redacted)
        return [
            _restore_environment_references(original_value, redacted_value)
            for original_value, redacted_value in zip(
                original_values,
                redacted_values,
                strict=False,
            )
        ]
    return redacted


def _list_owned_session_queue(
    queue: ClioCoreQueue,
    *,
    owner_session_id: str,
    session_generation_id: str,
    cluster: str | None,
    state: JobState | None,
    kind: JobKind | None,
    include_terminal: bool,
    cursor: int,
    limit: int,
    scan_limit: int,
) -> dict[str, object]:
    """List only one exact generation's membership without a global source window."""
    membership_cursor: str | None = None
    source_position = 1
    source_total: int | None = None
    while source_position < cursor:
        skip_limit = min(MAX_RESPONSE_PAGE_RECORDS, cursor - source_position)
        _, next_membership_cursor, source_total, scanned = queue.list_owner_session_jobs_page(
            owner_session_id,
            session_generation_id=session_generation_id,
            cursor=membership_cursor,
            limit=skip_limit,
            include_terminal=True,
        )
        source_position += scanned
        if scanned < skip_limit or next_membership_cursor is None:
            membership_cursor = None
            break
        membership_cursor = next_membership_cursor
    if source_total is not None and (source_position < cursor or cursor > source_total + 1):
        return _owned_queue_page(
            [],
            cluster=cluster,
            state=state,
            kind=kind,
            include_terminal=include_terminal,
            cursor=cursor,
            limit=limit,
            next_cursor=None,
            source_total=source_total,
            scan_limit=scan_limit,
            scanned=0,
        )

    selected: list[RelayJob] = []
    scanned_total = 0
    reached_end = source_total is not None and source_position > source_total
    while not reached_end and scanned_total < scan_limit and len(selected) < limit:
        page_limit = min(MAX_RESPONSE_PAGE_RECORDS, scan_limit - scanned_total)
        jobs, next_membership_cursor, observed_total, scanned = queue.list_owner_session_jobs_page(
            owner_session_id,
            session_generation_id=session_generation_id,
            cursor=membership_cursor,
            limit=page_limit,
            include_terminal=True,
        )
        if source_total is None:
            source_total = observed_total
        elif observed_total != source_total:
            raise QueueConflictError("owner-session membership changed during queue paging")
        consumed = 0
        for job in jobs:
            consumed += 1
            if cluster is not None and job.cluster != cluster:
                continue
            if state is not None and job.state is not state:
                continue
            if kind is not None and job.kind is not kind:
                continue
            if (
                not include_terminal
                and state is None
                and job.state in {JobState.SUCCEEDED, JobState.FAILED, JobState.CANCELED}
            ):
                continue
            selected.append(job)
            if len(selected) == limit:
                break
        scanned_total += consumed
        source_position += consumed
        if consumed < scanned:
            break
        membership_cursor = next_membership_cursor
        reached_end = membership_cursor is None
        if scanned == 0:
            reached_end = True
    resolved_total = source_total or 0
    next_cursor = source_position if source_position <= resolved_total else None
    return _owned_queue_page(
        selected,
        cluster=cluster,
        state=state,
        kind=kind,
        include_terminal=include_terminal,
        cursor=cursor,
        limit=limit,
        next_cursor=next_cursor,
        source_total=resolved_total,
        scan_limit=scan_limit,
        scanned=scanned_total,
    )


def _owned_queue_page(
    jobs: list[RelayJob],
    *,
    cluster: str | None,
    state: JobState | None,
    kind: JobKind | None,
    include_terminal: bool,
    cursor: int,
    limit: int,
    next_cursor: int | None,
    source_total: int,
    scan_limit: int,
    scanned: int,
) -> dict[str, object]:
    """Render a generation-scoped queue page without cross-session position evidence."""
    return {
        "jobs": [
            {
                "job": job.model_dump(mode="json"),
                "relay_queue": {
                    "state": job.state.value,
                    "jobs_ahead": None,
                    "position": None,
                },
            }
            for job in jobs
        ],
        "count": len(jobs),
        "cluster": cluster,
        "state": None if state is None else state.value,
        "kind": None if kind is None else kind.value,
        "include_terminal": include_terminal,
        "source_cursor": cursor,
        "source_limit": limit,
        "source_next_cursor": next_cursor,
        "source_total": source_total,
        "source_total_semantics": "owner_session_generation_membership",
        "filters_apply_within_source_window": True,
        "visibility_filter": "exact_owner_session_generation",
        "result_truncated": next_cursor is not None,
        "scan_limit": scan_limit,
        "scan_count": scanned,
        "scan_truncated": next_cursor is not None and scanned >= scan_limit,
    }


def _empty_artifact_uses() -> list[ArtifactUse]:
    """Return a typed empty artifact dependency collection."""
    return []


class JarvisSubmitRequest(BaseModel):
    """HTTP request to submit a JARVIS pipeline YAML document."""

    model_config = ConfigDict(extra="forbid")

    cluster: str
    pipeline_yaml: str
    idempotency_key: str
    used_artifact_refs: list[ArtifactUse] = Field(
        default_factory=_empty_artifact_uses,
        max_length=1_000,
    )


class JarvisPipelineSubmitRequest(BaseModel):
    """HTTP request to submit an existing JARVIS pipeline by name."""

    model_config = ConfigDict(extra="forbid")

    cluster: str
    pipeline_name: str
    idempotency_key: str
    used_artifact_refs: list[ArtifactUse] = Field(
        default_factory=_empty_artifact_uses,
        max_length=1_000,
    )


class RemoteAgentSubmitRequest(BaseModel):
    """HTTP request to submit a remote-agent task."""

    model_config = ConfigDict(extra="forbid")

    cluster: str
    prompt_path: str
    mcp_config_path: str | None = None
    model: str | None = None
    workdir: str | None = None
    timeout_seconds: int | None = Field(default=None, gt=0)
    idempotency_key: str
    used_artifact_refs: list[ArtifactUse] = Field(
        default_factory=_empty_artifact_uses,
        max_length=1_000,
    )


class McpCallSubmitRequest(BaseModel):
    """HTTP request to submit a remote MCP tool call."""

    model_config = ConfigDict(extra="forbid")

    cluster: str
    server: str
    server_args: list[str] = Field(default_factory=list)
    env_from: dict[str, str] = Field(default_factory=dict)
    expected_server_artifact_digest: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{64}$",
    )
    operation: McpOperation = McpOperation.TOOLS_CALL
    tool: str | None = None
    arguments: dict[str, object] = Field(default_factory=dict)
    control_query_evidence: McpControlQueryEvidence | None = None
    timeout_seconds: int | None = Field(default=None, gt=0)
    idempotency_key: str
    used_artifact_refs: list[ArtifactUse] = Field(
        default_factory=_empty_artifact_uses,
        max_length=1_000,
    )

    @field_validator("env_from")
    @classmethod
    def validate_environment_references(cls, value: dict[str, str]) -> dict[str, str]:
        """Reject invalid names and relay-owned credential references."""
        return validate_mcp_env_from(value)

    @model_validator(mode="after")
    def validate_operation_contract(self) -> McpCallSubmitRequest:
        """Keep call and discovery payloads unambiguous before admission."""
        if self.operation is McpOperation.TOOLS_CALL:
            if not self.tool:
                raise ValueError("tool is required for tools/call")
            return self
        if self.tool is not None:
            raise ValueError("tool must be omitted for tools/list")
        if self.arguments:
            raise ValueError("arguments must be empty for tools/list")
        if self.expected_server_artifact_digest is not None:
            raise ValueError("tools/list must not carry an expected server artifact digest")
        if self.control_query_evidence is not None:
            raise ValueError("tools/list must not carry control-query route evidence")
        return self


class JarvisMcpCallSubmitRequest(BaseModel):
    """HTTP request to submit a remote JARVIS MCP tool call."""

    model_config = ConfigDict(extra="forbid")

    cluster: str
    operation: McpOperation = McpOperation.TOOLS_CALL
    tool: str | None = None
    arguments: dict[str, object] = Field(default_factory=dict)
    expected_server_artifact_digest: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{64}$",
    )
    timeout_seconds: int | None = Field(default=None, gt=0)
    idempotency_key: str
    used_artifact_refs: list[ArtifactUse] = Field(
        default_factory=_empty_artifact_uses,
        max_length=1_000,
    )

    @model_validator(mode="after")
    def reject_internal_jarvis_run_wait(self) -> JarvisMcpCallSubmitRequest:
        """Keep workload waiting out of the trusted handle-first HTTP ingress."""
        if self.operation is McpOperation.TOOLS_CALL and not self.tool:
            raise ValueError("tool is required for tools/call")
        if self.operation is McpOperation.TOOLS_LIST:
            if self.tool is not None:
                raise ValueError("tool must be omitted for tools/list")
            if self.arguments:
                raise ValueError("arguments must be empty for tools/list")
            if self.expected_server_artifact_digest is not None:
                raise ValueError("tools/list must not carry an expected server artifact digest")
            if (
                self.timeout_seconds is not None
                and self.timeout_seconds > MAX_PINNED_CONTROL_QUERY_TIMEOUT_SECONDS
            ):
                raise ValueError(
                    "pinned MCP control-query timeout exceeds "
                    f"{MAX_PINNED_CONTROL_QUERY_TIMEOUT_SECONDS} seconds"
                )
            return self
        if self.tool == "jarvis_run" and "wait" in self.arguments:
            raise ValueError("jarvis_run does not accept internal wait; use jarvis_get_execution")
        if (
            self.expected_server_artifact_digest is not None
            and self.tool is not None
            and is_virtual_jarvis_control_query(self.tool)
            and self.timeout_seconds is not None
            and self.timeout_seconds > MAX_PINNED_CONTROL_QUERY_TIMEOUT_SECONDS
        ):
            raise ValueError(
                "pinned MCP control-query timeout exceeds "
                f"{MAX_PINNED_CONTROL_QUERY_TIMEOUT_SECONDS} seconds"
            )
        return self


class QueueCancelRequest(BaseModel):
    """HTTP request to cancel a relay job with explicit scheduler policy."""

    model_config = ConfigDict(extra="forbid")

    cluster: str | None = None
    cancel_scheduler_job: bool = False


class RetentionCollectRequest(BaseModel):
    """HTTP request to preview or advance bounded terminal retention."""

    model_config = ConfigDict(extra="forbid")

    execute: bool = False
    batch_size: int = Field(default=100, ge=1, le=100)
    expected_updated_at: datetime | None = None


class ProgressUpdateRequest(BaseModel):
    """HTTP request to record a job progress observation."""

    model_config = ConfigDict(extra="forbid")

    label: str = "progress"
    current: float | None = None
    total: float | None = Field(default=None, gt=0)
    unit: str | None = None
    message: str | None = None
    source_event_seq: int | None = Field(default=None, ge=1)
    metadata: dict[str, object] = Field(default_factory=dict)


class TaskTimelineEventRequest(BaseModel):
    """HTTP request to append a structured task timeline event."""

    model_config = ConfigDict(extra="forbid")

    event_type: str
    label: str
    status: TaskEventStatus = TaskEventStatus.RUNNING
    summary: str
    detail: str | None = None
    artifact_refs: list[DurableRecordId] = Field(default_factory=list)
    path_refs: list[str] = Field(default_factory=list)
    metadata: dict[str, object] = Field(default_factory=dict)


_RELAY_RUNTIME_GATEWAY_KEYS = frozenset(
    {
        "runtime_spec",
        "jarvis_runtime_binding",
        "browser_attachment",
        "ownership_intents",
        "teardown_intent",
        "teardown",
        "detach",
        "scheduler_provider",
        "scheduler_job_id",
        "scheduler_native_id",
    }
)
_RELAY_RUNTIME_CONNECTOR_KEYS = frozenset(
    {"browser_proxy", "desktop_connector", "remote_connector"}
)
_RELAY_OWNERSHIP_METADATA_KEYS = frozenset(
    {
        "owner",
        "owner_session_id",
        "owner_session_generation_id",
        "owner_session_admission_id",
        "runtime_kind",
        "binding_source",
        "source_relay_job_id",
        "source_relay_artifact_id",
        "jarvis_execution_id",
        "scheduler_provider",
        "scheduler_job_id",
        "scheduler_native_id",
    }
)


def _validate_generic_gateway_payload(
    value: dict[str, object] | None,
) -> dict[str, object] | None:
    """Reject fields whose identity is written only by the runtime supervisor."""
    if value is None:
        return None
    protected = sorted(_RELAY_RUNTIME_GATEWAY_KEYS.intersection(value))
    transport = value.get("transport")
    if isinstance(transport, dict):
        typed_transport = cast(dict[str, object], transport)
        protected.extend(
            f"transport.{key}"
            for key in sorted(_RELAY_RUNTIME_CONNECTOR_KEYS.intersection(typed_transport))
        )
    if protected:
        raise ValueError(
            "generic gateway requests cannot write relay-managed runtime fields: "
            + ", ".join(protected)
        )
    return value


def _validate_generic_gateway_metadata(value: dict[str, object]) -> dict[str, object]:
    """Reject client-provided relay ownership identity; the server stamps it."""
    protected = sorted(_RELAY_OWNERSHIP_METADATA_KEYS.intersection(value))
    if protected:
        raise ValueError(
            "generic gateway requests cannot write relay ownership metadata: "
            + ", ".join(protected)
        )
    return value


def _has_relay_managed_gateway_state(gateway: dict[str, object]) -> bool:
    """Return whether replacing this gateway payload could erase runtime ownership."""
    if _RELAY_RUNTIME_GATEWAY_KEYS.intersection(gateway):
        return True
    transport = gateway.get("transport")
    if not isinstance(transport, dict):
        return False
    return bool(_RELAY_RUNTIME_CONNECTOR_KEYS.intersection(cast(dict[str, object], transport)))


class GatewaySessionCreateRequest(BaseModel):
    """HTTP request to create a scheduler-backed gateway session."""

    model_config = ConfigDict(extra="forbid")

    cluster: str
    name: str
    state: GatewaySessionState = GatewaySessionState.CREATED
    queue_state: str | None = None
    node: str | None = None
    requested_resources: dict[str, object] = Field(default_factory=dict)
    stdout_uri: str | None = None
    stderr_uri: str | None = None
    log_uris: list[str] = Field(default_factory=list)
    gateway: dict[str, object] = Field(default_factory=dict)
    metadata: dict[str, object] = Field(default_factory=dict)

    _gateway_is_not_runtime_owned = field_validator("gateway")(_validate_generic_gateway_payload)
    _metadata_is_not_runtime_owned = field_validator("metadata")(_validate_generic_gateway_metadata)


class GatewaySessionUpdateRequest(BaseModel):
    """HTTP request to update scheduler-backed gateway session state."""

    model_config = ConfigDict(extra="forbid")

    state: GatewaySessionState | None = None
    queue_state: str | None = None
    node: str | None = None
    requested_resources: dict[str, object] | None = None
    stdout_uri: str | None = None
    stderr_uri: str | None = None
    log_uris: list[str] | None = None
    gateway: dict[str, object] | None = None
    artifacts: list[str] | None = None
    metadata: dict[str, object] = Field(default_factory=dict)

    _gateway_is_not_runtime_owned = field_validator("gateway")(_validate_generic_gateway_payload)
    _metadata_is_not_runtime_owned = field_validator("metadata")(_validate_generic_gateway_metadata)


_SESSION_REGISTRY_SHA256_ENV = "CLIO_RELAY_SESSION_REGISTRY_SHA256"
_SESSION_ROUTE_REVISION_ENV = "CLIO_RELAY_SESSION_ROUTE_REVISION"


def _bound_owner_session_cluster_definition(
    *, owner_session_id: str | None, owner_session_cluster: str | None
) -> ClusterDefinition | None:
    """Load one immutable process-bound cluster authority for an owned API."""
    raw_bindings = {
        CLUSTER_REGISTRY_ENV: os.getenv(CLUSTER_REGISTRY_ENV),
        _SESSION_REGISTRY_SHA256_ENV: os.getenv(_SESSION_REGISTRY_SHA256_ENV),
        _SESSION_ROUTE_REVISION_ENV: os.getenv(_SESSION_ROUTE_REVISION_ENV),
    }
    session_bindings = {
        _SESSION_REGISTRY_SHA256_ENV,
        _SESSION_ROUTE_REVISION_ENV,
    }
    configured_session_bindings = {
        name for name in session_bindings if raw_bindings[name] is not None
    }
    if not configured_session_bindings:
        if owner_session_id is not None:
            raise ConfigurationError(
                "owned relay session API requires process-bound cluster authority"
            )
        return None
    configured = {name for name, value in raw_bindings.items() if value is not None}
    if configured_session_bindings != session_bindings or configured != set(raw_bindings):
        raise ConfigurationError(
            "owned session cluster authority path, digest, and route revision must be configured "
            "together"
        )
    if owner_session_id is None or owner_session_cluster is None:
        raise ConfigurationError("session cluster authority requires an owned relay session")
    registry_path_raw = raw_bindings[CLUSTER_REGISTRY_ENV]
    if not registry_path_raw:
        raise ConfigurationError("owned session cluster registry path must not be blank")
    registry_sha256 = raw_bindings[_SESSION_REGISTRY_SHA256_ENV]
    route_revision = raw_bindings[_SESSION_ROUTE_REVISION_ENV]
    if (
        not registry_sha256
        or len(registry_sha256) != 64
        or any(character not in "0123456789abcdef" for character in registry_sha256)
    ):
        raise ConfigurationError("owned session cluster registry SHA-256 is invalid")
    if (
        not route_revision
        or len(route_revision) != 64
        or any(character not in "0123456789abcdef" for character in route_revision)
    ):
        raise ConfigurationError("owned session cluster route revision is invalid")
    registry_path = Path(registry_path_raw).expanduser()
    if not registry_path.is_absolute():
        raise ConfigurationError("owned session cluster registry path must be absolute")
    try:
        payload = read_bounded_configuration_bytes(
            registry_path,
            max_bytes=MAX_CLUSTER_REGISTRY_BYTES,
        )
    except (ConfigurationError, OSError) as exc:
        raise ConfigurationError("owned session cluster registry is unavailable") from exc
    if not secrets.compare_digest(hashlib.sha256(payload).hexdigest(), registry_sha256):
        raise ConfigurationError("owned session cluster registry digest does not match")
    try:
        registry = ClusterRegistry.model_validate_json(payload)
    except ValidationError as exc:
        raise ConfigurationError("owned session cluster registry is invalid") from exc
    if set(registry.clusters) != {owner_session_cluster}:
        raise ConfigurationError(
            "owned session cluster registry must contain exactly the owner session cluster"
        )
    definition = registry.require(owner_session_cluster)
    if not secrets.compare_digest(cluster_route_revision(definition), route_revision):
        raise ConfigurationError("owned session cluster route revision does not match")
    return definition


def create_app(settings: RelaySettings | None = None) -> FastAPI:
    """Create the FastAPI relay surface."""
    resolved = settings or RelaySettings.from_env()
    owner_session_cluster = resolved.resolved_owner_session_cluster()
    if resolved.owner_session_id is not None:
        if not owner_session_cluster:
            raise ConfigurationError(
                "owned relay session API requires CLIO_RELAY_OWNER_SESSION_CLUSTER"
            )
        if not resolved.session_owner_token:
            raise ConfigurationError(
                "owned relay session API requires CLIO_RELAY_SESSION_OWNER_TOKEN"
            )
        if len(resolved.session_owner_token.encode("utf-8")) < 32:
            raise ConfigurationError(
                "owned relay session API requires a session owner token of at least 32 bytes"
            )
        if not resolved.api_token:
            raise ConfigurationError("owned relay session API requires CLIO_RELAY_API_TOKEN")
    owner_session_cluster_definition = _bound_owner_session_cluster_definition(
        owner_session_id=resolved.owner_session_id,
        owner_session_cluster=owner_session_cluster,
    )
    queue = storage_managed_queue(resolved)
    queue.initialize()

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncGenerator[None]:
        """Retain shared core ownership for the API process lifetime."""
        if queue.closed:
            raise RuntimeError("clio-relay API application cannot restart after shutdown")
        try:
            yield
        finally:
            queue.close()

    app = FastAPI(title="clio-relay", lifespan=lifespan)
    auth_dependency = Depends(_require_api_token(resolved))
    session_submission_dependency = Depends(_require_session_submission_binding(resolved))

    def ensure_intake_open() -> None:
        if resolved.owner_session_id is None:
            return
        generation_id = resolved.owner_session_generation_id
        if generation_id is None:
            raise HTTPException(
                status_code=409,
                detail="relay session has no exact generation identity",
            )
        admission = queue.owner_session_generation_status(
            resolved.owner_session_id,
            session_generation_id=generation_id,
        )
        if admission.get("open") is not True:
            raise HTTPException(
                status_code=409,
                detail="relay session generation is not open for new work",
            )

    def owns_job(job: RelayJob) -> bool:
        return resolved.owner_session_id is None or (
            job.metadata.get("owner") == "clio-relay"
            and job.metadata.get("owner_session_id") == resolved.owner_session_id
            and job.metadata.get("owner_session_generation_id")
            == resolved.owner_session_generation_id
        )

    def require_owned_job(job_id: DurableRecordId) -> RelayJob:
        job = queue.get_job(job_id)
        if not owns_job(job):
            raise HTTPException(status_code=403, detail="job is not owned by this relay session")
        return job

    def require_owned_task(task_id: DurableRecordId) -> RelayTask:
        task = queue.get_task(task_id)
        require_owned_job(task.job_id)
        return task

    def require_owned_artifact(artifact_id: DurableRecordId) -> ArtifactRef:
        artifact = queue.get_artifact(artifact_id)
        require_owned_job(artifact.job_id)
        return artifact

    def submit_owned(
        job: RelayJob,
        *,
        mcp_admission_authority: McpAdmissionAuthority | None = None,
    ) -> RelayJob:
        ensure_intake_open()
        if owner_session_cluster is not None and job.cluster != owner_session_cluster:
            raise HTTPException(
                status_code=409,
                detail="job cluster does not match this owned relay session",
            )
        metadata = dict(job.metadata)
        protected = sorted(
            {
                "owner",
                "owner_session_id",
                "owner_session_generation_id",
                "owner_session_admission_id",
                MCP_ADMISSION_AUTHORITY_METADATA_KEY,
            }.intersection(metadata)
        )
        if protected:
            raise HTTPException(
                status_code=422,
                detail=(
                    "job ownership metadata is server-managed and cannot be supplied: "
                    + ", ".join(protected)
                ),
            )
        if job.kind is JobKind.MCP_CALL:
            if not isinstance(job.spec, McpCallSpec):
                raise HTTPException(status_code=422, detail="MCP job has an invalid specification")
            if job.spec.admission_class is McpAdmissionClass.CONTROL_QUERY:
                if mcp_admission_authority is None:
                    raise HTTPException(
                        status_code=422,
                        detail="control-query MCP admission requires server authority",
                    )
                metadata[MCP_ADMISSION_AUTHORITY_METADATA_KEY] = mcp_admission_authority.model_dump(
                    mode="json"
                )
            elif mcp_admission_authority is not None:
                raise HTTPException(
                    status_code=422,
                    detail="workload MCP admission must not carry control-query authority",
                )
        elif mcp_admission_authority is not None:
            raise HTTPException(
                status_code=422,
                detail="MCP admission authority cannot be attached to another job kind",
            )
        if resolved.owner_session_id is not None:
            for use in job.used_artifact_refs:
                require_owned_artifact(use.artifact_id)
            metadata.update(
                {
                    "owner": "clio-relay",
                    "owner_session_id": resolved.owner_session_id,
                }
            )
            if resolved.owner_session_generation_id is not None:
                metadata["owner_session_generation_id"] = resolved.owner_session_generation_id
        # Job ids crossing HTTP are caller-controlled, including on the raw
        # /jobs route. Generate new-admission entropy inside the server; an
        # idempotent retry is still canonicalized by the durable key record.
        job = job.model_copy(update={"job_id": new_id("job")})
        try:
            return _public_record(queue.submit_job(job.model_copy(update={"metadata": metadata})))
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        except QueueConflictError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except StorageAdmissionError as exc:
            raise HTTPException(status_code=507, detail=exc.decision.to_dict()) from exc

    def require_owned_gateway(session_id: DurableRecordId) -> GatewaySession:
        session = queue.get_gateway_session(session_id)
        if resolved.owner_session_id is None:
            return session
        if (
            session.metadata.get("owner") != "clio-relay"
            or session.metadata.get("owner_session_id") != resolved.owner_session_id
            or session.metadata.get("owner_session_generation_id")
            != resolved.owner_session_generation_id
        ):
            raise HTTPException(
                status_code=403,
                detail="gateway session is not owned by this relay session",
            )
        return session

    @app.get("/healthz")
    def healthz() -> dict[str, object]:
        return {"ok": True, "auth": resolved.api_token is not None}

    @app.get("/session-identity")
    def session_identity(nonce: Annotated[str, Query(pattern=r"^[0-9a-f]{64}$")]) -> dict[str, str]:
        """Prove the exact owned session identity without accepting credentials."""
        if (
            resolved.owner_session_id is None
            or resolved.owner_session_generation_id is None
            or owner_session_cluster is None
            or resolved.session_owner_token is None
        ):
            raise HTTPException(status_code=404, detail="owned session identity is unavailable")
        return session_identity_document(
            owner_token=resolved.session_owner_token,
            cluster=owner_session_cluster,
            session_id=resolved.owner_session_id,
            generation_id=resolved.owner_session_generation_id,
            nonce=nonce,
        )

    @app.get("/storage/status", dependencies=[auth_dependency])
    def storage_status() -> dict[str, object]:
        """Return the machine-readable queue admission and storage decision."""
        return _public_payload(queue.storage_runtime.status())

    @app.post(
        "/jobs",
        response_model=RelayJob,
        dependencies=[auth_dependency, session_submission_dependency],
    )
    def submit_job(job: RelayJob) -> RelayJob:
        if job.kind is JobKind.MCP_CALL:
            raise HTTPException(
                status_code=422,
                detail="MCP jobs must use /jobs/mcp-call or /jobs/jarvis-mcp-call",
            )
        return submit_owned(job)

    @app.post(
        "/jobs/jarvis",
        response_model=RelayJob,
        dependencies=[auth_dependency, session_submission_dependency],
    )
    def submit_jarvis(request: JarvisSubmitRequest) -> RelayJob:
        return submit_owned(
            RelayJob(
                cluster=request.cluster,
                kind=JobKind.JARVIS,
                spec=JarvisRunSpec(pipeline_yaml=request.pipeline_yaml),
                idempotency_key=request.idempotency_key,
                used_artifact_refs=request.used_artifact_refs,
            )
        )

    @app.post(
        "/jobs/jarvis-pipeline",
        response_model=RelayJob,
        dependencies=[auth_dependency, session_submission_dependency],
    )
    def submit_jarvis_pipeline(request: JarvisPipelineSubmitRequest) -> RelayJob:
        return submit_owned(
            RelayJob(
                cluster=request.cluster,
                kind=JobKind.JARVIS,
                spec=JarvisRunSpec(pipeline_name=request.pipeline_name),
                idempotency_key=request.idempotency_key,
                used_artifact_refs=request.used_artifact_refs,
            )
        )

    @app.post(
        "/jobs/remote-agent",
        response_model=RelayJob,
        dependencies=[auth_dependency, session_submission_dependency],
    )
    def submit_remote_agent(request: RemoteAgentSubmitRequest) -> RelayJob:
        return submit_owned(
            RelayJob(
                cluster=request.cluster,
                kind=JobKind.REMOTE_AGENT,
                spec=RemoteAgentTaskSpec(
                    prompt_path=request.prompt_path,
                    mcp_config_path=request.mcp_config_path,
                    model=request.model,
                    workdir=request.workdir,
                    timeout_seconds=request.timeout_seconds,
                ),
                idempotency_key=request.idempotency_key,
                used_artifact_refs=request.used_artifact_refs,
            )
        )

    @app.post(
        "/jobs/mcp-call",
        response_model=RelayJob,
        dependencies=[auth_dependency, session_submission_dependency],
    )
    def submit_mcp_call(request: McpCallSubmitRequest) -> RelayJob:
        registry_path = default_registry_path()
        try:
            definition = (
                owner_session_cluster_definition
                if owner_session_cluster_definition is not None
                else (
                    ClusterRegistry.load(registry_path).clusters.get(request.cluster)
                    if registry_path.exists()
                    else None
                )
            )
            admission_class, admission_authority = resolve_registered_remote_mcp_admission(
                queue=queue,
                definition=definition,
                cluster=request.cluster,
                server=request.server,
                server_args=request.server_args,
                env_from=request.env_from,
                operation=request.operation,
                tool=request.tool,
                expected_server_artifact_digest=request.expected_server_artifact_digest,
                evidence=request.control_query_evidence,
                timeout_seconds=request.timeout_seconds,
            )
        except (ConfigurationError, ValueError) as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return submit_owned(
            RelayJob(
                cluster=request.cluster,
                kind=JobKind.MCP_CALL,
                spec=McpCallSpec(
                    server=request.server,
                    server_args=request.server_args,
                    env_from=request.env_from,
                    expected_server_artifact_digest=(request.expected_server_artifact_digest),
                    admission_class=admission_class,
                    operation=request.operation,
                    tool=request.tool,
                    arguments=request.arguments,
                    timeout_seconds=request.timeout_seconds,
                ),
                idempotency_key=request.idempotency_key,
                used_artifact_refs=request.used_artifact_refs,
            ),
            mcp_admission_authority=admission_authority,
        )

    @app.post(
        "/jobs/jarvis-mcp-call",
        response_model=RelayJob,
        dependencies=[auth_dependency, session_submission_dependency],
    )
    def submit_jarvis_mcp_call(request: JarvisMcpCallSubmitRequest) -> RelayJob:
        expected_digest = request.expected_server_artifact_digest
        try:
            admission_class, admission_authority = resolve_pinned_mcp_admission(
                operation=request.operation,
                tool=request.tool,
                expected_server_artifact_digest=expected_digest,
                pinned_control_query=(
                    request.tool is not None and is_virtual_jarvis_control_query(request.tool)
                ),
                timeout_seconds=request.timeout_seconds,
            )
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        timeout_seconds = request.timeout_seconds
        if admission_class is McpAdmissionClass.CONTROL_QUERY and timeout_seconds is None:
            timeout_seconds = MAX_PINNED_CONTROL_QUERY_TIMEOUT_SECONDS
        if (
            resolved.owner_session_id is not None
            and request.operation is McpOperation.TOOLS_CALL
            and expected_digest is None
        ):
            raise HTTPException(
                status_code=422,
                detail=("owned JARVIS MCP submission requires expected_server_artifact_digest"),
            )
        # An owned cluster-side API receives the discovery binding from its
        # authenticated desktop owner. Its operator cache is intentionally
        # process-local and may not contain the desktop's discovery entry. Do
        # not substitute a second, unrelated cache as authority here: preserve
        # the supplied digest in the durable spec and let the MCP runner compare
        # it with the server artifact observed immediately before launch.
        if (
            request.operation is McpOperation.TOOLS_CALL
            and expected_digest is not None
            and resolved.owner_session_id is None
        ):
            try:
                observed_digest = jarvis_mcp_artifact_binding(request.cluster)
            except ValueError as exc:
                raise HTTPException(status_code=409, detail=str(exc)) from exc
            if not secrets.compare_digest(expected_digest, observed_digest):
                raise HTTPException(
                    status_code=409,
                    detail=(
                        "JARVIS MCP artifact identity changed; refresh discovery before submission"
                    ),
                )
        return submit_owned(
            RelayJob(
                cluster=request.cluster,
                kind=JobKind.MCP_CALL,
                spec=McpCallSpec(
                    server=jarvis_mcp_server(),
                    server_args=jarvis_mcp_server_args(),
                    env_from=jarvis_mcp_env_from(),
                    expected_server_artifact_digest=expected_digest,
                    expected_jarvis_cd_lock_binding=jarvis_cd_lock_binding_expectation(),
                    admission_class=admission_class,
                    operation=request.operation,
                    tool=request.tool,
                    arguments=request.arguments,
                    timeout_seconds=timeout_seconds,
                ),
                idempotency_key=request.idempotency_key,
                used_artifact_refs=request.used_artifact_refs,
            ),
            mcp_admission_authority=admission_authority,
        )

    @app.get("/jobs/{job_id}", response_model=RelayJob, dependencies=[auth_dependency])
    def get_job(job_id: DurableRecordId) -> RelayJob:
        try:
            return _public_record(require_owned_job(job_id))
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.get("/jobs/{job_id}/status", dependencies=[auth_dependency])
    def get_job_status(job_id: DurableRecordId) -> dict[str, object]:
        try:
            require_owned_job(job_id)
            return _public_payload(get_job_status_operation(queue, job_id))
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.get(
        "/jobs/{job_id}/events",
        response_model=list[RelayEvent],
        dependencies=[auth_dependency],
    )
    def get_events(
        job_id: DurableRecordId,
        cursor: Annotated[int, Query(ge=1)] = 1,
        limit: Annotated[int, Query(ge=1, le=MAX_RESPONSE_PAGE_RECORDS)] = (
            DEFAULT_RESPONSE_PAGE_RECORDS
        ),
    ) -> list[RelayEvent]:
        require_owned_job(job_id)
        events, _ = queue.drain_events(Cursor(job_id=job_id, next_seq=cursor), limit=limit)
        return [_public_record(event) for event in events]

    @app.get(
        "/jobs/{job_id}/tasks",
        dependencies=[auth_dependency],
    )
    def get_tasks(
        job_id: DurableRecordId,
        cursor: Annotated[int, Query(ge=1)] = 1,
        limit: Annotated[int, Query(ge=1, le=MAX_RESPONSE_PAGE_RECORDS)] = (
            DEFAULT_RESPONSE_PAGE_RECORDS
        ),
    ) -> dict[str, object]:
        require_owned_job(job_id)
        tasks, next_cursor, total = queue.list_tasks_page(
            job_id,
            cursor=cursor,
            limit=limit,
        )
        return _public_payload(
            _public_model_page(
                "tasks",
                tasks,
                cursor=cursor,
                limit=limit,
                next_cursor=next_cursor,
                total=total,
            )
        )

    @app.get(
        "/tasks/{task_id}/events",
        response_model=list[TaskTimelineEvent],
        dependencies=[auth_dependency],
    )
    def get_task_events(
        task_id: DurableRecordId,
        cursor: Annotated[int, Query(ge=1)] = 1,
        limit: Annotated[int, Query(ge=1, le=MAX_RESPONSE_PAGE_RECORDS)] = (
            DEFAULT_RESPONSE_PAGE_RECORDS
        ),
    ) -> list[TaskTimelineEvent]:
        try:
            require_owned_task(task_id)
            events, _ = queue.drain_task_events(task_id, cursor=cursor, limit=limit)
            return [_public_record(event) for event in events]
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.post(
        "/tasks/{task_id}/events",
        response_model=TaskTimelineEvent,
        dependencies=[auth_dependency],
    )
    def append_task_event(
        task_id: DurableRecordId,
        request: TaskTimelineEventRequest,
    ) -> TaskTimelineEvent:
        try:
            require_owned_task(task_id)
            return _public_record(
                queue.append_task_event(
                    TaskTimelineEvent(
                        task_id=task_id,
                        event_type=request.event_type,
                        label=request.label,
                        status=request.status,
                        summary=request.summary,
                        detail=request.detail,
                        artifact_refs=request.artifact_refs,
                        path_refs=request.path_refs,
                        metadata=request.metadata,
                    )
                )
            )
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.get("/tasks/{task_id}/events/sse", dependencies=[auth_dependency])
    def task_events_sse(
        task_id: DurableRecordId,
        cursor: Annotated[int, Query(ge=1)] = 1,
        limit: Annotated[int, Query(ge=1, le=MAX_RESPONSE_PAGE_RECORDS)] = (
            DEFAULT_RESPONSE_PAGE_RECORDS
        ),
        poll_seconds: float = 1.0,
        stop_after_replay: bool = False,
    ) -> StreamingResponse:
        """Stream task timeline events as Server-Sent Events."""
        if poll_seconds <= 0:
            raise HTTPException(status_code=400, detail="poll_seconds must be positive")
        try:
            require_owned_task(task_id)
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return StreamingResponse(
            _task_sse_events(
                queue,
                task_id,
                cursor=cursor,
                limit=limit,
                poll_seconds=poll_seconds,
                stop_after_replay=stop_after_replay,
            ),
            media_type="text/event-stream",
        )

    @app.websocket("/tasks/{task_id}/events/ws")
    async def task_events_ws(
        websocket: WebSocket,
        task_id: DurableRecordId,
        cursor: int = 1,
        limit: int = DEFAULT_RESPONSE_PAGE_RECORDS,
        poll_seconds: float = 1.0,
    ) -> None:
        """Stream task timeline events over a WebSocket."""
        _require_websocket_token(resolved, websocket)
        if poll_seconds <= 0 or cursor < 1:
            raise WebSocketException(code=status.WS_1008_POLICY_VIOLATION)
        _require_websocket_page_limit(limit)
        try:
            require_owned_task(task_id)
        except (NotFoundError, HTTPException) as exc:
            raise WebSocketException(code=status.WS_1008_POLICY_VIOLATION) from exc
        await websocket.accept()
        try:
            async for payload in _task_stream_payloads(
                queue,
                task_id,
                cursor=cursor,
                limit=limit,
                poll_seconds=poll_seconds,
            ):
                await websocket.send_json(payload)
        except WebSocketDisconnect:
            return

    @app.get("/jobs/{job_id}/monitor", dependencies=[auth_dependency])
    def monitor(
        job_id: DurableRecordId,
        cursor: Annotated[int, Query(ge=1)] = 1,
        limit: Annotated[int, Query(ge=1, le=MAX_RESPONSE_PAGE_RECORDS)] = (
            DEFAULT_RESPONSE_PAGE_RECORDS
        ),
    ) -> dict[str, object]:
        try:
            require_owned_job(job_id)
            return _public_payload(monitor_job(queue, job_id, cursor=cursor, limit=limit))
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.get("/jobs/{job_id}/monitor/sse", dependencies=[auth_dependency])
    def monitor_sse(
        job_id: DurableRecordId,
        cursor: Annotated[int, Query(ge=1)] = 1,
        limit: Annotated[int, Query(ge=1, le=MAX_RESPONSE_PAGE_RECORDS)] = (
            DEFAULT_RESPONSE_PAGE_RECORDS
        ),
        poll_seconds: float = 1.0,
        stop_on_terminal: bool = True,
    ) -> StreamingResponse:
        """Stream job monitor updates as Server-Sent Events."""
        if poll_seconds <= 0:
            raise HTTPException(status_code=400, detail="poll_seconds must be positive")
        try:
            require_owned_job(job_id)
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return StreamingResponse(
            _monitor_sse_events(
                queue,
                job_id,
                cursor=cursor,
                limit=limit,
                poll_seconds=poll_seconds,
                stop_on_terminal=stop_on_terminal,
            ),
            media_type="text/event-stream",
        )

    @app.websocket("/jobs/{job_id}/monitor/ws")
    async def monitor_ws(
        websocket: WebSocket,
        job_id: DurableRecordId,
        cursor: int = 1,
        limit: int = DEFAULT_RESPONSE_PAGE_RECORDS,
        poll_seconds: float = 1.0,
        stop_on_terminal: bool = True,
    ) -> None:
        """Stream job monitor updates over a WebSocket."""
        _require_websocket_token(resolved, websocket)
        if poll_seconds <= 0 or cursor < 1:
            raise WebSocketException(code=status.WS_1008_POLICY_VIOLATION)
        _require_websocket_page_limit(limit)
        try:
            require_owned_job(job_id)
        except (NotFoundError, HTTPException) as exc:
            raise WebSocketException(code=status.WS_1008_POLICY_VIOLATION) from exc
        await websocket.accept()
        try:
            async for payload in _monitor_stream_payloads(
                queue,
                job_id,
                cursor=cursor,
                limit=limit,
                poll_seconds=poll_seconds,
                stop_on_terminal=stop_on_terminal,
            ):
                await websocket.send_json(payload)
                if payload["event"] == "terminal":
                    await websocket.close()
                    return
        except WebSocketDisconnect:
            return

    @app.post("/jobs/{job_id}/wait", response_model=RelayJob, dependencies=[auth_dependency])
    def wait(
        job_id: DurableRecordId,
        timeout_seconds: float = 600,
        poll_seconds: float = 2,
    ) -> RelayJob:
        try:
            require_owned_job(job_id)
            return _public_record(
                wait_for_terminal(
                    queue,
                    job_id,
                    timeout_seconds=timeout_seconds,
                    poll_seconds=poll_seconds,
                )
            )
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except TimeoutError as exc:
            raise HTTPException(status_code=408, detail=str(exc)) from exc

    @app.get("/jobs/{job_id}/logs/{stream_name}", dependencies=[auth_dependency])
    def get_log(
        job_id: DurableRecordId,
        stream_name: str,
        offset: Annotated[int, Query(ge=0)] = 0,
        limit: Annotated[int, Query(ge=1, le=1_048_576)] = 65_536,
    ) -> dict[str, object]:
        try:
            if stream_name not in {"stdout", "stderr"}:
                raise HTTPException(status_code=400, detail="stream must be stdout or stderr")
            return _public_payload(
                read_job_log(
                    resolved,
                    require_owned_job(job_id),
                    stream_name="stdout" if stream_name == "stdout" else "stderr",
                    offset=offset,
                    limit=limit,
                )
            )
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.get(
        "/jobs/{job_id}/artifacts",
        dependencies=[auth_dependency],
    )
    def get_artifacts(
        job_id: DurableRecordId,
        cursor: Annotated[int, Query(ge=1)] = 1,
        limit: Annotated[int, Query(ge=1, le=MAX_RESPONSE_PAGE_RECORDS)] = (
            DEFAULT_RESPONSE_PAGE_RECORDS
        ),
    ) -> dict[str, object]:
        require_owned_job(job_id)
        artifacts, next_cursor, total = queue.list_artifacts_page(
            job_id,
            cursor=cursor,
            limit=limit,
        )
        return _public_payload(
            _public_model_page(
                "artifacts",
                artifacts,
                cursor=cursor,
                limit=limit,
                next_cursor=next_cursor,
                total=total,
            )
        )

    @app.get(
        "/jobs/{job_id}/used-artifacts",
        dependencies=[auth_dependency],
    )
    def get_used_artifacts(
        job_id: DurableRecordId,
        cursor: DurableRecordId | None = None,
        limit: Annotated[int, Query(ge=1, le=MAX_RESPONSE_PAGE_RECORDS)] = (
            DEFAULT_RESPONSE_PAGE_RECORDS
        ),
    ) -> dict[str, object]:
        """Return one page of immutable artifact dependencies for a job."""
        require_owned_job(job_id)
        records, next_cursor, total = queue.list_used_artifacts_page(
            job_id,
            cursor=cursor,
            limit=limit,
        )
        for record in records:
            require_owned_artifact(record.artifact_id)
        return _public_payload(
            {
                "used_artifacts": [record.model_dump(mode="json") for record in records],
                "cursor": cursor,
                "limit": limit,
                "next_cursor": next_cursor,
                "total": total,
            }
        )

    @app.get(
        "/artifacts/{artifact_id}/used-by",
        dependencies=[auth_dependency],
    )
    def get_artifact_users(
        artifact_id: DurableRecordId,
        cursor: DurableRecordId | None = None,
        limit: Annotated[int, Query(ge=1, le=MAX_RESPONSE_PAGE_RECORDS)] = (
            DEFAULT_RESPONSE_PAGE_RECORDS
        ),
    ) -> dict[str, object]:
        """Return one page of jobs that consumed a content-pinned artifact."""
        require_owned_artifact(artifact_id)
        records, next_cursor, total = queue.list_artifact_users_page(
            artifact_id,
            cursor=cursor,
            limit=limit,
        )
        for record in records:
            require_owned_job(record.consumer_job_id)
        return _public_payload(
            {
                "used_by": [record.model_dump(mode="json") for record in records],
                "cursor": cursor,
                "limit": limit,
                "next_cursor": next_cursor,
                "total": total,
            }
        )

    @app.get(
        "/jobs/{job_id}/progress",
        dependencies=[auth_dependency],
    )
    def get_progress(
        job_id: DurableRecordId,
        cursor: Annotated[int, Query(ge=1)] = 1,
        limit: Annotated[int, Query(ge=1, le=MAX_RESPONSE_PAGE_RECORDS)] = (
            DEFAULT_RESPONSE_PAGE_RECORDS
        ),
    ) -> dict[str, object]:
        require_owned_job(job_id)
        progress, next_cursor, total = queue.list_progress_page(
            job_id,
            cursor=cursor,
            limit=limit,
        )
        return _public_payload(
            _public_model_page(
                "progress",
                progress,
                cursor=cursor,
                limit=limit,
                next_cursor=next_cursor,
                total=total,
            )
        )

    @app.post(
        "/gateway-sessions",
        response_model=GatewaySession,
        dependencies=[auth_dependency, session_submission_dependency],
    )
    def create_gateway_session(request: GatewaySessionCreateRequest) -> GatewaySession:
        ensure_intake_open()
        if owner_session_cluster is not None and request.cluster != owner_session_cluster:
            raise HTTPException(
                status_code=409,
                detail="gateway cluster does not match this owned relay session",
            )
        metadata = dict(request.metadata)
        if resolved.owner_session_id is not None:
            metadata.update(
                {
                    "owner": "clio-relay",
                    "owner_session_id": resolved.owner_session_id,
                }
            )
            if resolved.owner_session_generation_id is not None:
                metadata["owner_session_generation_id"] = resolved.owner_session_generation_id
        try:
            return _public_record(
                queue.create_gateway_session(
                    GatewaySession(
                        cluster=request.cluster,
                        name=request.name,
                        state=request.state,
                        queue_state=request.queue_state,
                        node=request.node,
                        requested_resources=request.requested_resources,
                        stdout_uri=request.stdout_uri,
                        stderr_uri=request.stderr_uri,
                        log_uris=request.log_uris,
                        gateway=request.gateway,
                        metadata=metadata,
                    )
                )
            )
        except QueueConflictError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    @app.get(
        "/gateway-sessions",
        dependencies=[auth_dependency],
    )
    def list_gateway_sessions(
        cluster: str | None = None,
        cursor: Annotated[int, Query(ge=1)] = 1,
        limit: Annotated[int, Query(ge=1, le=MAX_RESPONSE_PAGE_RECORDS)] = (
            DEFAULT_RESPONSE_PAGE_RECORDS
        ),
    ) -> dict[str, object]:
        sessions, next_cursor, total = queue.list_gateway_sessions_page(
            cursor=cursor,
            limit=limit,
            cluster=cluster,
        )
        if resolved.owner_session_id is not None:
            sessions = [
                session
                for session in sessions
                if session.metadata.get("owner") == "clio-relay"
                and session.metadata.get("owner_session_id") == resolved.owner_session_id
                and session.metadata.get("owner_session_generation_id")
                == resolved.owner_session_generation_id
            ]
        return _public_payload(
            {
                "gateway_sessions": [session.model_dump(mode="json") for session in sessions],
                "source_cursor": cursor,
                "source_limit": limit,
                "source_next_cursor": next_cursor,
                "source_total": total,
                "source_total_semantics": "global_gateway_sequence_high_water",
                "filters_apply_within_source_window": True,
                "visibility_filter": (
                    "owner_session_within_source_window"
                    if resolved.owner_session_id is not None
                    else None
                ),
            }
        )

    @app.get(
        "/gateway-sessions/{session_id}",
        response_model=GatewaySession,
        dependencies=[auth_dependency],
    )
    def get_gateway_session(session_id: DurableRecordId) -> GatewaySession:
        try:
            return _public_record(require_owned_gateway(session_id))
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.patch(
        "/gateway-sessions/{session_id}",
        response_model=GatewaySession,
        dependencies=[auth_dependency],
    )
    def update_gateway_session(
        session_id: DurableRecordId,
        request: GatewaySessionUpdateRequest,
    ) -> GatewaySession:
        try:
            existing = require_owned_gateway(session_id)
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        if request.gateway is not None and _has_relay_managed_gateway_state(existing.gateway):
            raise HTTPException(
                status_code=409,
                detail=(
                    "relay-managed runtime gateway state can only be changed by the "
                    "runtime supervisor"
                ),
            )
        updates = request.model_dump(exclude={"state", "metadata"}, exclude_none=True)
        metadata = dict(request.metadata)
        if resolved.owner_session_id is not None:
            metadata.update(
                {
                    "owner": "clio-relay",
                    "owner_session_id": resolved.owner_session_id,
                }
            )
            if resolved.owner_session_generation_id is not None:
                metadata["owner_session_generation_id"] = resolved.owner_session_generation_id
        try:
            return _public_record(
                queue.update_gateway_session(
                    session_id,
                    state=request.state,
                    metadata=metadata,
                    reject_relay_managed_fields=True,
                    **updates,
                )
            )
        except QueueConflictError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.post(
        "/gateway-sessions/{session_id}/close",
        response_model=GatewaySession,
        dependencies=[auth_dependency],
    )
    def close_gateway_session(session_id: DurableRecordId) -> GatewaySession:
        try:
            require_owned_gateway(session_id)
            return _public_record(queue.close_gateway_session(session_id))
        except QueueConflictError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.post(
        "/jobs/{job_id}/progress",
        response_model=ProgressRecord,
        dependencies=[auth_dependency],
    )
    def record_progress(
        job_id: DurableRecordId,
        request: ProgressUpdateRequest,
    ) -> ProgressRecord:
        try:
            require_owned_job(job_id)
            metadata = external_progress_metadata("external_http", dict(request.metadata))
            return _public_record(
                queue.append_progress(
                    ProgressRecord(
                        job_id=job_id,
                        label=request.label,
                        current=request.current,
                        total=request.total,
                        unit=request.unit,
                        message=request.message,
                        source_event_seq=request.source_event_seq,
                        metadata=metadata,
                    )
                )
            )
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.get("/artifacts/{artifact_id}/content", dependencies=[auth_dependency])
    def get_artifact_content(artifact_id: DurableRecordId) -> dict[str, object]:
        try:
            require_owned_artifact(artifact_id)
            return _public_payload(read_artifact_bytes(queue, artifact_id))
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.post("/jobs/{job_id}/cancel", response_model=RelayJob, dependencies=[auth_dependency])
    def cancel_job(
        job_id: DurableRecordId,
        request: QueueCancelRequest | None = None,
    ) -> RelayJob:
        job = require_owned_job(job_id)
        if request is not None and request.cluster is not None and request.cluster != job.cluster:
            raise HTTPException(
                status_code=409,
                detail=(
                    f"job {job_id} belongs to cluster {job.cluster}, "
                    f"not requested cluster {request.cluster}"
                ),
            )
        cancel_scheduler = False if request is None else request.cancel_scheduler_job
        return _public_record(request_cancel_job(queue, job_id, cancel_scheduler=cancel_scheduler))

    @app.post("/queue/jobs/{job_id}/cancel", dependencies=[auth_dependency])
    def cancel_queue_job_route(
        job_id: DurableRecordId,
        request: QueueCancelRequest | None = None,
    ) -> dict[str, object]:
        cancel_scheduler = False if request is None else request.cancel_scheduler_job
        try:
            require_owned_job(job_id)
            return _public_payload(
                cancel_queue_job(
                    queue,
                    job_id,
                    cluster=None if request is None else request.cluster,
                    scheduler_policy="request-scheduler" if cancel_scheduler else "relay-only",
                )
            )
        except ConfigurationError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.get("/queue", dependencies=[auth_dependency])
    def list_queue(
        cluster: str | None = None,
        state: str | None = None,
        kind: JobKind | None = None,
        include_terminal: bool = False,
        cursor: Annotated[int, Query(ge=1)] = 1,
        limit: Annotated[int, Query(ge=1, le=500)] = 100,
        scan_limit: Annotated[int, Query(ge=1, le=10_000)] = 1_000,
    ) -> dict[str, object]:
        job_state = None
        if state is not None:
            try:
                job_state = JobState(state)
            except ValueError as exc:
                raise HTTPException(status_code=422, detail=f"unknown job state: {state}") from exc
        if scan_limit < limit:
            raise HTTPException(
                status_code=422,
                detail="scan_limit must be greater than or equal to limit",
            )
        if resolved.owner_session_id is not None:
            generation_id = resolved.owner_session_generation_id
            if generation_id is None:
                raise HTTPException(status_code=409, detail="owned session generation is missing")
            try:
                return _public_payload(
                    _list_owned_session_queue(
                        queue,
                        owner_session_id=resolved.owner_session_id,
                        session_generation_id=generation_id,
                        cluster=cluster,
                        state=job_state,
                        kind=kind,
                        include_terminal=include_terminal,
                        cursor=cursor,
                        limit=limit,
                        scan_limit=scan_limit,
                    )
                )
            except QueueConflictError as exc:
                raise HTTPException(status_code=409, detail=str(exc)) from exc
            except ValueError as exc:
                raise HTTPException(status_code=422, detail=str(exc)) from exc
        try:
            payload = list_queue_jobs(
                queue,
                cluster=cluster,
                state=job_state,
                kind=kind,
                include_terminal=include_terminal,
                cursor=cursor,
                limit=limit,
                scan_limit=scan_limit,
            )
        except ConfigurationError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        return _public_payload(payload)

    @app.get("/queue/jobs/{job_id}/diagnose", dependencies=[auth_dependency])
    def diagnose_queue_job_route(
        job_id: DurableRecordId,
        cluster: str | None = None,
        older_than_seconds: Annotated[int, Query(ge=1)] = 7_200,
        scan_limit: Annotated[int, Query(ge=1, le=10_000)] = 1_000,
    ) -> dict[str, object]:
        try:
            require_owned_job(job_id)
            return _public_payload(
                diagnose_job(
                    queue,
                    job_id,
                    cluster=cluster,
                    stale_after_seconds=older_than_seconds,
                    scan_limit=scan_limit,
                )
            )
        except ConfigurationError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.get("/retention/jobs/{job_id}/plan", dependencies=[auth_dependency])
    def retention_plan(
        job_id: DurableRecordId,
        expected_updated_at: datetime | None = None,
    ) -> dict[str, object]:
        """Build a read-only terminal-retention plan."""
        if resolved.owner_session_id is not None:
            raise HTTPException(
                status_code=403,
                detail="session-scoped APIs cannot inspect global retention state",
            )
        try:
            plan = TerminalRetentionCoordinator(queue, resolved.spool_dir).plan(
                job_id,
                expected_updated_at=expected_updated_at,
            )
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except QueueConflictError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return _public_payload(
            {
                "plan": plan.model_dump(mode="json"),
                "scheduler_cancel_requested": False,
            }
        )

    @app.get("/retention/jobs/{job_id}/status", dependencies=[auth_dependency])
    def retention_status(job_id: DurableRecordId) -> dict[str, object]:
        """Read the current crash-resumable retention phase without mutation."""
        if resolved.owner_session_id is not None:
            raise HTTPException(
                status_code=403,
                detail="session-scoped APIs cannot inspect global retention state",
            )
        try:
            plan = TerminalRetentionCoordinator(queue, resolved.spool_dir).plan(job_id)
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except QueueConflictError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return {
            "job_id": job_id,
            "receipt_id": plan.receipt_id,
            "phase": None if plan.receipt_phase is None else plan.receipt_phase.value,
            "complete": plan.receipt_phase is not None and plan.receipt_phase.value == "complete",
            "eligible": plan.eligible,
            "protections": plan.protections,
            "scheduler_cancel_requested": False,
        }

    @app.post("/retention/jobs/{job_id}/collect", dependencies=[auth_dependency])
    def retention_collect(
        job_id: DurableRecordId,
        request: RetentionCollectRequest | None = None,
    ) -> dict[str, object]:
        """Dry-run by default or advance bounded retention without scheduler cancellation."""
        if resolved.owner_session_id is not None:
            raise HTTPException(
                status_code=403,
                detail="session-scoped APIs cannot mutate global retention state",
            )
        options = request or RetentionCollectRequest()
        try:
            result = TerminalRetentionCoordinator(queue, resolved.spool_dir).collect(
                job_id,
                execute=options.execute,
                batch_size=options.batch_size,
                expected_updated_at=options.expected_updated_at,
            )
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except QueueConflictError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return _public_payload(result.model_dump(mode="json"))

    @app.get("/queue/stale", dependencies=[auth_dependency])
    def discover_stale_queue_route(
        cluster: str,
        older_than_seconds: Annotated[int, Query(ge=1)] = 7_200,
        job_id: DurableRecordId | None = None,
        kind: JobKind | None = None,
        limit: Annotated[int, Query(ge=1, le=500)] = 100,
        scan_limit: Annotated[int, Query(ge=1, le=10_000)] = DEFAULT_STALE_SCAN_LIMIT,
    ) -> dict[str, object]:
        if resolved.owner_session_id is not None:
            raise HTTPException(
                status_code=403,
                detail="session-scoped APIs cannot inspect global stale-job state",
            )
        try:
            return _public_payload(
                discover_stale_jobs(
                    queue,
                    cluster=cluster,
                    older_than_seconds=older_than_seconds,
                    job_id=job_id,
                    kind=kind,
                    limit=limit,
                    scan_limit=scan_limit,
                )
            )
        except ConfigurationError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    @app.get("/queue/diagnostics", dependencies=[auth_dependency])
    def diagnose_queue_route(cluster: str | None = None) -> dict[str, object]:
        if resolved.owner_session_id is not None:
            raise HTTPException(
                status_code=403,
                detail="session-scoped APIs cannot inspect global queue diagnostics",
            )
        return _public_payload(diagnose_queue(queue, cluster=cluster))

    @app.post("/queue/cleanup-stale", dependencies=[auth_dependency])
    def cleanup_stale_queue_route(
        cluster: str,
        older_than_seconds: Annotated[int, Query(ge=1)] = 7_200,
        job_id: DurableRecordId | None = None,
        kind: JobKind | None = None,
        max_attempts: Annotated[int, Query(ge=1)] = 3,
        dry_run: bool = True,
        cancel_queued: bool = False,
        limit: Annotated[int, Query(ge=1, le=500)] = 100,
        scan_limit: Annotated[int, Query(ge=1, le=10_000)] = DEFAULT_STALE_SCAN_LIMIT,
    ) -> dict[str, object]:
        if resolved.owner_session_id is not None:
            raise HTTPException(
                status_code=403,
                detail="session-scoped APIs cannot mutate global stale-job state",
            )
        try:
            return _public_payload(
                cleanup_stale_jobs(
                    queue,
                    cluster=cluster,
                    older_than_seconds=older_than_seconds,
                    job_id=job_id,
                    kind=kind,
                    max_attempts=max_attempts,
                    dry_run=dry_run,
                    cancel_queued=cancel_queued,
                    limit=limit,
                    scan_limit=scan_limit,
                )
            )
        except ConfigurationError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    @app.get("/workers", dependencies=[auth_dependency])
    def worker_status_route(cluster: str | None = None) -> dict[str, object]:
        if resolved.owner_session_id is not None:
            raise HTTPException(
                status_code=403,
                detail="session-scoped APIs cannot inspect global worker state",
            )
        return _public_payload(worker_status(queue, cluster=cluster))

    @app.post("/monitor/rules", response_model=MonitorRule, dependencies=[auth_dependency])
    def create_monitor_rule(rule: MonitorRule) -> MonitorRule:
        try:
            ensure_intake_open()
            require_owned_job(rule.job_id)
            return _public_record(queue.append_monitor_rule(rule))
        except NotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.get("/monitor/rules", dependencies=[auth_dependency])
    def list_monitor_rules(
        job_id: DurableRecordId | None = None,
        cursor: Annotated[int, Query(ge=1)] = 1,
        limit: Annotated[int, Query(ge=1, le=MAX_RESPONSE_PAGE_RECORDS)] = (
            DEFAULT_RESPONSE_PAGE_RECORDS
        ),
    ) -> dict[str, object]:
        if job_id is not None:
            require_owned_job(job_id)
        rules, next_cursor, total = queue.list_monitor_rules_page(
            cursor=cursor,
            limit=limit,
            job_id=job_id,
        )
        if resolved.owner_session_id is not None:
            rules = [rule for rule in rules if owns_job(queue.get_job(rule.job_id))]
        return _public_payload(
            {
                "rules": [rule.model_dump(mode="json") for rule in rules],
                "source_cursor": cursor,
                "source_limit": limit,
                "source_next_cursor": next_cursor,
                "source_total": total,
                "source_total_semantics": "global_monitor_rule_sequence_high_water",
                "filters_apply_within_source_window": True,
                "visibility_filter": (
                    "owner_session_within_source_window"
                    if resolved.owner_session_id is not None
                    else None
                ),
            }
        )

    @app.post("/monitor/run-once", dependencies=[auth_dependency])
    def run_monitor_once(
        limit: Annotated[int, Query(ge=1, le=MAX_RESPONSE_PAGE_RECORDS)] = (
            DEFAULT_RESPONSE_PAGE_RECORDS
        ),
    ) -> list[dict[str, object]]:
        if resolved.owner_session_id is not None:
            raise HTTPException(
                status_code=403,
                detail="session-scoped APIs cannot evaluate global monitor rules",
            )
        return [_public_payload(item) for item in evaluate_monitor_rules(queue, limit=limit)]

    return app


async def _monitor_sse_events(
    queue: ClioCoreQueue,
    job_id: str,
    *,
    cursor: int,
    limit: int,
    poll_seconds: float,
    stop_on_terminal: bool,
) -> AsyncIterator[str]:
    async for payload in _monitor_stream_payloads(
        queue,
        job_id,
        cursor=cursor,
        limit=limit,
        poll_seconds=poll_seconds,
        stop_on_terminal=stop_on_terminal,
    ):
        yield f"event: {payload['event']}\ndata: {json.dumps(payload['data'], default=str)}\n\n"


async def _task_sse_events(
    queue: ClioCoreQueue,
    task_id: str,
    *,
    cursor: int,
    limit: int,
    poll_seconds: float,
    stop_after_replay: bool,
) -> AsyncIterator[str]:
    async for payload in _task_stream_payloads(
        queue,
        task_id,
        cursor=cursor,
        limit=limit,
        poll_seconds=poll_seconds,
        stop_after_replay=stop_after_replay,
    ):
        yield f"event: {payload['event']}\ndata: {json.dumps(payload['data'], default=str)}\n\n"


async def _task_stream_payloads(
    queue: ClioCoreQueue,
    task_id: str,
    *,
    cursor: int,
    limit: int,
    poll_seconds: float,
    stop_after_replay: bool = False,
) -> AsyncIterator[dict[str, object]]:
    limit = validate_response_page_limit(limit)
    next_cursor = cursor
    while True:
        events, next_cursor = queue.drain_task_events(
            task_id,
            cursor=next_cursor,
            limit=limit,
        )
        if events:
            yield _public_payload(
                {
                    "event": "task_events",
                    "data": {
                        "task_id": task_id,
                        "events": [event.model_dump(mode="json") for event in events],
                        "next_cursor": next_cursor,
                    },
                }
            )
            if stop_after_replay:
                return
        elif stop_after_replay:
            return
        await asyncio.sleep(poll_seconds)


async def _monitor_stream_payloads(
    queue: ClioCoreQueue,
    job_id: str,
    *,
    cursor: int,
    limit: int,
    poll_seconds: float,
    stop_on_terminal: bool,
) -> AsyncIterator[dict[str, object]]:
    limit = validate_response_page_limit(limit)
    next_cursor = cursor
    while True:
        payload = monitor_job(queue, job_id, cursor=next_cursor, limit=limit)
        raw_next_cursor = payload["next_cursor"]
        if not isinstance(raw_next_cursor, int):
            raise TypeError("monitor payload next_cursor was not an integer")
        next_cursor = raw_next_cursor
        yield _public_payload({"event": "monitor", "data": payload})
        job = queue.get_job(job_id)
        if stop_on_terminal and job.state.value in {"succeeded", "failed", "canceled"}:
            yield {"event": "terminal", "data": {"job_id": job_id, "state": job.state.value}}
            return
        await asyncio.sleep(poll_seconds)


def _require_api_token(settings: RelaySettings) -> Callable[..., Awaitable[None]]:
    async def dependency(
        authorization: Annotated[str | None, Header()] = None,
        x_clio_relay_token: Annotated[str | None, Header()] = None,
        x_clio_relay_owner_session_id: Annotated[
            str | None,
            Header(alias=OWNER_SESSION_ID_HEADER),
        ] = None,
        x_clio_relay_session_generation_id: Annotated[
            str | None,
            Header(alias=SESSION_GENERATION_ID_HEADER),
        ] = None,
    ) -> None:
        if settings.api_token is not None:
            supplied = _extract_token(authorization, x_clio_relay_token)
            if supplied is None or not secrets.compare_digest(supplied, settings.api_token):
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="missing or invalid relay API token",
                )
        expected_session_id = settings.owner_session_id
        expected_generation_id = settings.owner_session_generation_id
        if expected_session_id is None:
            if (
                x_clio_relay_owner_session_id is not None
                or x_clio_relay_session_generation_id is not None
            ):
                raise HTTPException(
                    status_code=409,
                    detail="relay API is not bound to an owner session",
                )
            return
        if x_clio_relay_owner_session_id is None or x_clio_relay_session_generation_id is None:
            raise HTTPException(
                status_code=409,
                detail="exact owner session and generation headers are required",
            )
        if expected_generation_id is None or not (
            secrets.compare_digest(x_clio_relay_owner_session_id, expected_session_id)
            and secrets.compare_digest(
                x_clio_relay_session_generation_id,
                expected_generation_id,
            )
        ):
            raise HTTPException(
                status_code=409,
                detail="owner session or generation does not match this API process",
            )

    return dependency


def _require_session_submission_binding(
    settings: RelaySettings,
) -> Callable[..., Awaitable[None]]:
    """Require exact client intent before a session-scoped API stamps job ownership."""

    async def dependency(
        x_clio_relay_owner_session_id: Annotated[
            str | None,
            Header(alias=OWNER_SESSION_ID_HEADER),
        ] = None,
        x_clio_relay_session_generation_id: Annotated[
            str | None,
            Header(alias=SESSION_GENERATION_ID_HEADER),
        ] = None,
    ) -> None:
        expected_session_id = settings.owner_session_id
        expected_generation_id = settings.owner_session_generation_id
        if expected_session_id is None:
            if (
                x_clio_relay_owner_session_id is not None
                or x_clio_relay_session_generation_id is not None
            ):
                raise HTTPException(
                    status_code=409,
                    detail="relay API is not bound to an owner session",
                )
            return
        if settings.api_token is None:
            raise HTTPException(
                status_code=503,
                detail="owned relay session submissions require API token authentication",
            )
        if x_clio_relay_owner_session_id is None or x_clio_relay_session_generation_id is None:
            raise HTTPException(
                status_code=409,
                detail="exact owner session and generation headers are required",
            )
        if expected_generation_id is None or not (
            secrets.compare_digest(x_clio_relay_owner_session_id, expected_session_id)
            and secrets.compare_digest(
                x_clio_relay_session_generation_id,
                expected_generation_id,
            )
        ):
            raise HTTPException(
                status_code=409,
                detail="owner session or generation does not match this API process",
            )

    return dependency


def _require_websocket_page_limit(limit: object) -> None:
    try:
        validate_response_page_limit(limit)
    except ValueError as exc:
        raise WebSocketException(code=status.WS_1008_POLICY_VIOLATION) from exc


def _require_websocket_token(settings: RelaySettings, websocket: WebSocket) -> None:
    if settings.api_token is None:
        return
    supplied = websocket.query_params.get("token")
    if supplied is None:
        supplied = _extract_token(websocket.headers.get("authorization"), None)
    if supplied is None or not secrets.compare_digest(supplied, settings.api_token):
        raise WebSocketException(code=status.WS_1008_POLICY_VIOLATION)
    if settings.owner_session_id is None:
        return
    session_id = websocket.headers.get(OWNER_SESSION_ID_HEADER)
    generation_id = websocket.headers.get(SESSION_GENERATION_ID_HEADER)
    if (
        session_id is None
        or generation_id is None
        or settings.owner_session_generation_id is None
        or not secrets.compare_digest(session_id, settings.owner_session_id)
        or not secrets.compare_digest(generation_id, settings.owner_session_generation_id)
    ):
        raise WebSocketException(code=status.WS_1008_POLICY_VIOLATION)


def _extract_token(authorization: str | None, header_token: str | None) -> str | None:
    if header_token:
        return header_token
    if authorization is None:
        return None
    scheme, _, token = authorization.partition(" ")
    if scheme.lower() != "bearer" or token == "":
        return None
    return token


app = create_app()

"""Typed relay records shared by CLI, HTTP, endpoints, and tests."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import Annotated, Any, Literal, Self
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
MCP_ADMISSION_AUTHORITY_METADATA_KEY = "mcp_admission_authority"
INPUT_INGEST_POLICY_METADATA_KEY = "input_ingest_policy"
MAX_ARTIFACT_USE_PROVENANCE_BYTES = 8 * 1024
MAX_ARTIFACT_USE_AGGREGATE_BYTES = 256 * 1024
MAX_JARVIS_PACKAGE_INPUT_CONTRACT_BYTES = 256 * 1024
MAX_TRANSFORM_ENVIRONMENT_BYTES = 16 * 1024
MAX_TRANSFORM_REF_BYTES = 192 * 1024
MAX_TRANSFORM_USED_EVIDENCE = 1_000
REGISTERED_JARVIS_USER_CONTRACT = "clio-kit-jarvis-user-v3.6"


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


class McpControlQueryEvidence(BaseModel):
    """Cluster-owned discovery evidence offered for reserved query admission."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["clio-relay.mcp-control-query-evidence.v1"] = (
        "clio-relay.mcp-control-query-evidence.v1"
    )
    cluster: str = Field(min_length=1, max_length=256)
    registered_server_name: str = Field(min_length=1, max_length=256)
    cluster_route_revision: str = Field(pattern=r"^[0-9a-f]{64}$")
    registration_revision: str = Field(pattern=r"^[0-9a-f]{64}$")
    discovery_job_id: DurableRecordId
    discovery_artifact_id: DurableRecordId
    discovery_artifact_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    discovery_schema_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    expected_server_artifact_digest: str = Field(pattern=r"^[0-9a-f]{64}$")


class McpAdmissionAuthority(BaseModel):
    """Server-stamped provenance explaining one reserved MCP admission."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["clio-relay.mcp-admission-authority.v1"] = (
        "clio-relay.mcp-admission-authority.v1"
    )
    admission_class: Literal["control_query"] = "control_query"
    source: Literal[
        "intrinsic_tools_list",
        "pinned_jarvis_contract",
        "registered_discovery_artifact",
    ]
    operation: McpOperation
    tool: str | None = Field(default=None, max_length=512)
    expected_server_artifact_digest: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{64}$",
    )
    evidence: McpControlQueryEvidence | None = None

    @model_validator(mode="after")
    def validate_authority_source(self) -> McpAdmissionAuthority:
        """Require source-specific evidence and operation bindings."""
        if self.source == "intrinsic_tools_list":
            if self.operation is not McpOperation.TOOLS_LIST:
                raise ValueError("intrinsic MCP admission authority requires tools/list")
            if self.tool is not None or self.evidence is not None:
                raise ValueError("intrinsic tools/list authority must not name a tool or evidence")
            return self
        if self.operation is not McpOperation.TOOLS_CALL or not self.tool:
            raise ValueError("MCP control-query authority requires one tools/call tool")
        if self.expected_server_artifact_digest is None:
            raise ValueError("MCP control-query authority requires an artifact digest")
        if self.source == "pinned_jarvis_contract":
            if self.evidence is not None:
                raise ValueError("pinned JARVIS authority must not carry generic route evidence")
            return self
        if self.evidence is None:
            raise ValueError("registered MCP authority requires discovery evidence")
        if self.evidence.expected_server_artifact_digest != self.expected_server_artifact_digest:
            raise ValueError("registered MCP authority artifact binding changed")
        return self


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


class ArtifactUseEvidence(StrEnum):
    """How one transform established a used edge's identity."""

    SCHEMA_ARG = "schema-arg"
    HASH_PAIR = "hash-pair"
    LEASE_WINDOW = "lease-window"
    AUTHORITY = "authority"
    ASSERTION = "assertion"


class ArtifactMechanism(StrEnum):
    """What produced an artifact or transform record."""

    HARNESS = "harness"
    TOOL_SCHEMA = "tool-schema"
    CHANGE_FEED = "change-feed"
    MODEL = "model"
    NONE = "none"


class TransformEnvironmentTier(StrEnum):
    """Strength of one non-secret execution-environment identity."""

    DECLARED = "declared"
    LOCKFILE_HASH = "lockfile-hash"
    IMAGE_DIGEST = "image-digest"


class TransformReplayContract(StrEnum):
    """Permanent replay guarantee recorded for one transform."""

    REPRODUCIBLE = "reproducible"
    RE_RUNNABLE = "re-runnable"


class ArtifactUseProvenance(BaseModel):
    """Bounded non-secret evidence attached to one content-pinned used edge."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["clio-relay.artifact-use-provenance.v1"] = (
        "clio-relay.artifact-use-provenance.v1"
    )
    evidence: ArtifactUseEvidence
    authority: str = Field(default="", max_length=4_096)
    external_ref: str = Field(default="", max_length=4_096)
    arg: str = Field(default="", max_length=512)
    note: str = Field(default="", max_length=512)

    @model_validator(mode="after")
    def require_bounded_consistent_evidence(self) -> ArtifactUseProvenance:
        """Reject contradictory authority evidence and oversized JSON documents."""
        if self.evidence is ArtifactUseEvidence.AUTHORITY and not self.authority:
            raise ValueError("authority evidence requires a non-empty authority reference")
        _require_canonical_json_size(
            self.model_dump(mode="json"),
            label="artifact-use provenance",
            maximum=MAX_ARTIFACT_USE_PROVENANCE_BYTES,
        )
        return self


class ArtifactUse(BaseModel):
    """A content-pinned artifact dependency supplied with a job submission."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    artifact_id: DurableRecordId
    sha256: str = Field(min_length=64, max_length=64)
    provenance: ArtifactUseProvenance | None = None

    @field_validator("sha256")
    @classmethod
    def sha256_must_be_canonical(cls, value: str) -> str:
        """Normalize and validate the immutable content identity."""
        normalized = value.lower()
        if any(character not in "0123456789abcdef" for character in normalized):
            raise ValueError("sha256 must be a SHA-256 digest")
        return normalized


def artifact_use_payload(value: ArtifactUse) -> dict[str, Any]:
    """Return the canonical additive wire form without changing legacy identities."""
    return value.model_dump(mode="json", exclude_none=True)


class JarvisPackageInputRoute(BaseModel):
    """Exact immutable registered route used to describe one JARVIS package."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["clio-relay.jarvis-package-input-route.v1"] = (
        "clio-relay.jarvis-package-input-route.v1"
    )
    cluster: str = Field(min_length=1, max_length=256)
    server_name: str = Field(min_length=1, max_length=256)
    contract: Literal["clio-kit-jarvis-user-v3.6"] = "clio-kit-jarvis-user-v3.6"
    cluster_route_revision: str = Field(pattern=r"^[0-9a-f]{64}$")
    registration_revision: str = Field(pattern=r"^[0-9a-f]{64}$")
    expected_server_artifact_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    package_name: str = Field(min_length=1, max_length=512)

    def identity_sha256(self) -> str:
        """Return the canonical route-and-package storage identity."""
        return _canonical_json_sha256(self.model_dump(mode="json"))


class JarvisPackageLocalFileInput(BaseModel):
    """Closed local-file setting names learned from a package description."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    canonical_name: str = Field(min_length=1, max_length=512)
    accepted_names: tuple[Annotated[str, Field(min_length=1, max_length=512)], ...] = Field(
        min_length=1,
        max_length=64,
    )

    @model_validator(mode="after")
    def accepted_names_are_unique_and_canonical(self) -> Self:
        """Require the canonical spelling first and reject ambiguous aliases."""
        if self.accepted_names[0] != self.canonical_name:
            raise ValueError("package local-file accepted names must start with the canonical name")
        if len(self.accepted_names) != len(set(self.accepted_names)):
            raise ValueError("package local-file accepted names must be unique")
        return self


class JarvisPackageInputContractRecord(BaseModel):
    """Checksum-bound package input semantics for one exact immutable route."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["clio-relay.jarvis-package-input-contract.v1"] = (
        "clio-relay.jarvis-package-input-contract.v1"
    )
    route: JarvisPackageInputRoute
    route_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    package_names: tuple[Annotated[str, Field(min_length=1, max_length=512)], ...] = Field(
        min_length=1, max_length=64
    )
    local_file_settings: tuple[JarvisPackageLocalFileInput, ...] = Field(max_length=1_000)
    settings_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    created_at: datetime
    document_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def identity_and_document_are_exact(self) -> Self:
        """Reject route substitution, ambiguous settings, and record mutation."""
        if self.route_sha256 != self.route.identity_sha256():
            raise ValueError("package input route checksum does not match its identity")
        if self.route.package_name not in self.package_names:
            raise ValueError("package input route name is absent from the described package names")
        if len(self.package_names) != len(set(self.package_names)):
            raise ValueError("described package names must be unique")
        accepted_names: set[str] = set()
        for setting in self.local_file_settings:
            overlap = accepted_names.intersection(setting.accepted_names)
            if overlap:
                raise ValueError("package local-file setting names and aliases must be unique")
            accepted_names.update(setting.accepted_names)
        if self.document_sha256 != _jarvis_package_input_contract_sha256(self):
            raise ValueError("package input contract checksum does not match its document")
        _require_canonical_json_size(
            self.model_dump(mode="json"),
            label="JARVIS package input contract",
            maximum=MAX_JARVIS_PACKAGE_INPUT_CONTRACT_BYTES,
        )
        return self

    @classmethod
    def create(
        cls,
        *,
        route: JarvisPackageInputRoute,
        package_names: tuple[str, ...],
        local_file_settings: tuple[JarvisPackageLocalFileInput, ...],
        settings_sha256: str,
        created_at: datetime | None = None,
    ) -> JarvisPackageInputContractRecord:
        """Create one validated immutable package-input contract record."""
        provisional = cls.model_construct(
            route=route,
            route_sha256=route.identity_sha256(),
            package_names=package_names,
            local_file_settings=local_file_settings,
            settings_sha256=settings_sha256,
            created_at=created_at or utc_now(),
            document_sha256="0" * 64,
        )
        return cls.model_validate(
            {
                **provisional.model_dump(mode="python"),
                "document_sha256": _jarvis_package_input_contract_sha256(provisional),
            }
        )


class JarvisPipelineInputRoute(BaseModel):
    """Exact registered route and owner generation for staged pipeline inputs."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["clio-relay.jarvis-pipeline-input-route.v1"] = (
        "clio-relay.jarvis-pipeline-input-route.v1"
    )
    cluster: str = Field(min_length=1, max_length=256)
    server_name: str = Field(min_length=1, max_length=256)
    contract: Literal["clio-kit-jarvis-user-v3.6"] = "clio-kit-jarvis-user-v3.6"
    cluster_route_revision: str = Field(pattern=r"^[0-9a-f]{64}$")
    registration_revision: str = Field(pattern=r"^[0-9a-f]{64}$")
    expected_server_artifact_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    pipeline_id: str = Field(min_length=1, max_length=512)
    owner_session_id: str = Field(min_length=1, max_length=256)
    owner_session_generation_id: DurableRecordId

    def identity_sha256(self) -> str:
        """Return the canonical content identity used as the durable record key."""
        return _canonical_json_sha256(self.model_dump(mode="json"))


class JarvisPipelineInputLineage(BaseModel):
    """Tamper-evident staged-input lineage for one exact JARVIS pipeline route."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["clio-relay.jarvis-pipeline-input-lineage.v1"] = (
        "clio-relay.jarvis-pipeline-input-lineage.v1"
    )
    route: JarvisPipelineInputRoute
    route_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    artifact_uses: tuple[ArtifactUse, ...] = Field(max_length=1_000)
    manifest_sha256s: tuple[str, ...] = Field(max_length=1_000)
    created_at: datetime
    updated_at: datetime
    document_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")

    @field_validator("artifact_uses")
    @classmethod
    def artifact_uses_must_be_unique_and_canonical(
        cls,
        value: tuple[ArtifactUse, ...],
    ) -> tuple[ArtifactUse, ...]:
        """Forbid ambiguous duplicate artifacts and non-canonical record order."""
        canonical = tuple(sorted(value, key=lambda item: (item.artifact_id, item.sha256)))
        if value != canonical:
            raise ValueError("pipeline input artifact uses must be canonically ordered")
        identities = [item.artifact_id for item in value]
        if len(identities) != len(set(identities)):
            raise ValueError("pipeline input artifact uses must have unique artifact IDs")
        validate_artifact_use_collection(value)
        return value

    @field_validator("manifest_sha256s")
    @classmethod
    def manifests_must_be_unique_canonical_sha256s(
        cls,
        value: tuple[str, ...],
    ) -> tuple[str, ...]:
        """Require a sorted set of canonical staging-manifest identities."""
        if value != tuple(sorted(set(value))):
            raise ValueError("pipeline input manifests must be unique and canonically ordered")
        if any(
            len(item) != 64 or any(character not in "0123456789abcdef" for character in item)
            for item in value
        ):
            raise ValueError("pipeline input manifests must be canonical SHA-256 digests")
        return value

    @model_validator(mode="after")
    def checksums_must_match_exact_document(self) -> JarvisPipelineInputLineage:
        """Detect route substitution or record mutation before lineage is reused."""
        if self.route_sha256 != self.route.identity_sha256():
            raise ValueError("pipeline input route checksum does not match its identity")
        if self.updated_at < self.created_at:
            raise ValueError("pipeline input lineage updated_at predates created_at")
        if self.document_sha256 != _jarvis_pipeline_input_lineage_sha256(self):
            raise ValueError("pipeline input lineage checksum does not match its document")
        return self

    @classmethod
    def create(
        cls,
        *,
        route: JarvisPipelineInputRoute,
        artifact_uses: tuple[ArtifactUse, ...],
        manifest_sha256s: tuple[str, ...],
        created_at: datetime | None = None,
        updated_at: datetime | None = None,
    ) -> JarvisPipelineInputLineage:
        """Create one validated checksum-bound durable lineage record."""
        created = created_at or utc_now()
        updated = updated_at or created
        provisional = cls.model_construct(
            route=route,
            route_sha256=route.identity_sha256(),
            artifact_uses=tuple(
                sorted(artifact_uses, key=lambda item: (item.artifact_id, item.sha256))
            ),
            manifest_sha256s=tuple(sorted(set(manifest_sha256s))),
            created_at=created,
            updated_at=updated,
            document_sha256="0" * 64,
        )
        return cls.model_validate(
            {
                **provisional.model_dump(mode="python"),
                "document_sha256": _jarvis_pipeline_input_lineage_sha256(provisional),
            }
        )


def _canonical_json_sha256(value: object) -> str:
    """Hash one finite canonical JSON value."""
    return hashlib.sha256(
        json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def _canonical_json_bytes(value: object) -> bytes:
    """Serialize one finite JSON value deterministically for size enforcement."""
    try:
        return json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ValueError("provenance must contain finite JSON values") from exc


def _require_canonical_json_size(value: object, *, label: str, maximum: int) -> None:
    """Reject a provenance document whose canonical UTF-8 encoding is oversized."""
    if len(_canonical_json_bytes(value)) > maximum:
        raise ValueError(f"{label} exceeds {maximum} UTF-8 bytes")


def validate_artifact_use_collection(
    value: list[ArtifactUse] | tuple[ArtifactUse, ...],
) -> None:
    """Bound the complete dependency document independently from item count."""
    _require_canonical_json_size(
        [artifact_use_payload(item) for item in value],
        label="artifact-use collection",
        maximum=MAX_ARTIFACT_USE_AGGREGATE_BYTES,
    )


def _jarvis_pipeline_input_lineage_sha256(record: JarvisPipelineInputLineage) -> str:
    """Hash every durable lineage field except the checksum itself."""
    payload = record.model_dump(mode="json", exclude={"document_sha256"})
    payload["artifact_uses"] = [artifact_use_payload(item) for item in record.artifact_uses]
    return _canonical_json_sha256(payload)


def _jarvis_package_input_contract_sha256(record: JarvisPackageInputContractRecord) -> str:
    """Hash every durable package-input field except the checksum itself."""
    return _canonical_json_sha256(record.model_dump(mode="json", exclude={"document_sha256"}))


def _empty_artifact_uses() -> list[ArtifactUse]:
    """Return a typed empty artifact dependency collection."""
    return []


class UsedArtifactRef(BaseModel):
    """A durable W3C-PROV-style ``used`` edge between a job and an artifact."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["clio-relay.used-artifact-ref.v1"] = "clio-relay.used-artifact-ref.v1"
    artifact_id: DurableRecordId
    consumer_job_id: DurableRecordId
    producer_job_id: DurableRecordId
    sequence: int = Field(ge=1, lt=2**63)
    sha256: str = Field(min_length=64, max_length=64)
    provenance: ArtifactUseProvenance | None = None
    created_at: datetime

    @field_validator("sha256")
    @classmethod
    def sha256_must_be_canonical(cls, value: str) -> str:
        """Require the stored edge to contain a canonical SHA-256 digest."""
        if any(character not in "0123456789abcdef" for character in value):
            raise ValueError("sha256 must be a canonical SHA-256 digest")
        return value


class TransformUseEvidence(ArtifactUseProvenance):
    """One bounded used edge, including authority-only external inputs."""

    artifact_id: DurableRecordId | None = None
    sha256: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("sha256")
    @classmethod
    def optional_sha256_must_be_canonical(cls, value: str | None) -> str | None:
        """Normalize an optional content pin without inventing one for authority edges."""
        if value is None:
            return None
        normalized = value.lower()
        if any(character not in "0123456789abcdef" for character in normalized):
            raise ValueError("sha256 must be a SHA-256 digest")
        return normalized

    @model_validator(mode="after")
    def require_edge_identity(self) -> TransformUseEvidence:
        """Require an internal artifact or an explicit external/authority identity."""
        if (self.artifact_id is None) != (self.sha256 is None):
            raise ValueError("internal transform evidence requires artifact_id and sha256 together")
        if self.artifact_id is None and not self.external_ref and not self.authority:
            raise ValueError(
                "transform used evidence requires artifact_id, external_ref, or authority"
            )
        return self


class TransformEnvironment(BaseModel):
    """Fixed non-secret environment identity for a durable transform."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    tier: TransformEnvironmentTier = TransformEnvironmentTier.DECLARED
    clio_version: str = Field(default="", max_length=256)
    lockfile_sha256: str = Field(default="", max_length=64)
    launcher_fingerprint: str = Field(default="", max_length=512)
    provider_id: str = Field(default="", max_length=512)
    model_id: str = Field(default="", max_length=1_024)
    model_variant: str = Field(default="", max_length=512)
    model_source: str = Field(default="", max_length=256)
    os: str = Field(default="", max_length=256)
    arch: str = Field(default="", max_length=256)
    python_version: str = Field(default="", max_length=256)
    image_digest: str = Field(default="", max_length=256)

    @field_validator("lockfile_sha256")
    @classmethod
    def lockfile_sha256_must_be_canonical_or_empty(cls, value: str) -> str:
        """Require the lockfile identity to be an exact SHA-256 when present."""
        if value and (len(value) != 64 or any(char not in "0123456789abcdef" for char in value)):
            raise ValueError("lockfile_sha256 must be empty or a canonical SHA-256 digest")
        return value

    @field_validator("image_digest")
    @classmethod
    def image_digest_must_be_canonical_or_empty(cls, value: str) -> str:
        """Require an image digest rather than a mutable image tag."""
        if not value:
            return value
        digest = value.removeprefix("sha256:")
        if len(digest) != 64 or any(char not in "0123456789abcdef" for char in digest):
            raise ValueError("image_digest must be empty or a canonical SHA-256 digest")
        return value

    @model_validator(mode="after")
    def tier_must_have_its_identity(self) -> TransformEnvironment:
        """Keep tier claims consistent and the fixed environment document bounded."""
        if self.tier is TransformEnvironmentTier.LOCKFILE_HASH and not self.lockfile_sha256:
            raise ValueError("lockfile-hash environment requires lockfile_sha256")
        if self.tier is TransformEnvironmentTier.IMAGE_DIGEST and not self.image_digest:
            raise ValueError("image-digest environment requires image_digest")
        _require_canonical_json_size(
            self.model_dump(mode="json"),
            label="transform environment",
            maximum=MAX_TRANSFORM_ENVIRONMENT_BYTES,
        )
        return self


class TransformRef(BaseModel):
    """One immutable activity record for a relay job, independent of used edges."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["clio-relay.transform-ref.v1"] = "clio-relay.transform-ref.v1"
    job_id: DurableRecordId
    activity_id: str = Field(min_length=1, max_length=512)
    mechanism: ArtifactMechanism = ArtifactMechanism.NONE
    environment: TransformEnvironment = Field(default_factory=TransformEnvironment)
    replay: TransformReplayContract = TransformReplayContract.RE_RUNNABLE
    replay_reason: str = Field(default="", max_length=512)
    used_evidence: tuple[TransformUseEvidence, ...] = Field(
        default_factory=tuple,
        max_length=MAX_TRANSFORM_USED_EVIDENCE,
    )
    created_at: datetime = Field(default_factory=utc_now)

    @model_validator(mode="after")
    def record_must_be_bounded(self) -> TransformRef:
        """Bound a complete activity record independently from queue file limits."""
        _require_canonical_json_size(
            self.model_dump(mode="json"),
            label="transform ref",
            maximum=MAX_TRANSFORM_REF_BYTES,
        )
        return self


class ArtifactUserOrderHead(BaseModel):
    """Durable high-water mark for one artifact's ordered consumer edges."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["clio-relay.artifact-user-order-head.v1"] = (
        "clio-relay.artifact-user-order-head.v1"
    )
    artifact_id: DurableRecordId
    latest_sequence: int = Field(ge=0, lt=2**63)


class InputArtifactSpec(BaseModel):
    """Durable identity for one relay-ingested regular-file input."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["clio-relay.input-artifact.v1"] = "clio-relay.input-artifact.v1"
    logical_name: str = Field(min_length=1, max_length=255)
    size_bytes: int = Field(ge=0)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")

    @field_validator("logical_name")
    @classmethod
    def logical_name_must_be_a_safe_filename(cls, value: str) -> str:
        """Require one portable filename rather than a caller-controlled path."""
        if value in {".", ".."} or value.endswith((" ", ".")):
            raise ValueError("logical_name must be a portable regular-file name")
        if any(character in value for character in ("/", "\\", "\x00", ":")):
            raise ValueError("logical_name must not contain path separators or drive syntax")
        if any(ord(character) < 32 or ord(character) == 127 for character in value):
            raise ValueError("logical_name must not contain control characters")
        reserved_stem = value.split(".", 1)[0].upper()
        reserved = {
            "CON",
            "PRN",
            "AUX",
            "NUL",
            *(f"COM{number}" for number in range(1, 10)),
            *(f"LPT{number}" for number in range(1, 10)),
        }
        if reserved_stem in reserved:
            raise ValueError("logical_name must not be a reserved filename")
        return value


class InputArtifactIngestPolicy(BaseModel):
    """Server-stamped owner-generation limits for input ingestion."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    schema_version: Literal["clio-relay.input-artifact-ingest-policy.v1"] = (
        "clio-relay.input-artifact-ingest-policy.v1"
    )
    max_file_count: int = Field(ge=1, le=1_000)
    max_total_bytes: int = Field(ge=1)


def deterministic_input_artifact_id(job_id: str) -> str:
    """Return the single stable artifact identity owned by an ingest job."""
    validated_job_id = validate_durable_record_id(job_id)
    digest = hashlib.sha256(
        f"clio-relay.input-artifact.v1\0{validated_job_id}".encode()
    ).hexdigest()
    return validate_durable_record_id(f"artifact_{digest[:32]}")


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
    expected_registered_contract: str | None = Field(default=None, min_length=1, max_length=256)
    expected_jarvis_cd_lock_binding: dict[str, str] | None = None
    admission_class: McpAdmissionClass = McpAdmissionClass.WORKLOAD
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

    @field_validator("expected_jarvis_cd_lock_binding")
    @classmethod
    def validate_expected_jarvis_cd_lock_binding(
        cls,
        value: dict[str, str] | None,
    ) -> dict[str, str] | None:
        """Require a complete expected dependency artifact identity when present."""
        if value is None:
            return None
        expected_keys = {"schema_version", "version", "url", "sha256"}
        if set(value) != expected_keys or any(not item for item in value.values()):
            raise ValueError("expected_jarvis_cd_lock_binding must be a complete identity")
        if value["schema_version"] != "clio-relay.jarvis-cd-lock-expectation.v1":
            raise ValueError("expected_jarvis_cd_lock_binding schema is unsupported")
        sha256 = value["sha256"].lower()
        if len(sha256) != 64 or any(character not in "0123456789abcdef" for character in sha256):
            raise ValueError("expected_jarvis_cd_lock_binding SHA-256 is invalid")
        return {**value, "sha256": sha256}

    @model_validator(mode="after")
    def validate_operation_contract(self) -> McpCallSpec:
        """Require call-only fields and keep discovery requests unambiguous."""
        if self.operation == McpOperation.TOOLS_CALL:
            if self.tool is None or not self.tool:
                raise ValueError("tool is required for tools/call")
            if (
                self.admission_class is McpAdmissionClass.CONTROL_QUERY
                and self.expected_server_artifact_digest is None
            ):
                raise ValueError(
                    "control_query MCP calls require an expected server artifact digest"
                )
            if (
                self.expected_registered_contract is not None
                and self.expected_server_artifact_digest is None
            ):
                raise ValueError(
                    "registered MCP contract binding requires an expected server artifact digest"
                )
            if (
                self.expected_registered_contract is not None
                and self.expected_jarvis_cd_lock_binding is not None
            ):
                raise ValueError(
                    "registered MCP contract binding and built-in JARVIS lock pin are exclusive"
                )
            return self
        if self.tool is not None:
            raise ValueError("tool must be omitted for tools/list")
        if self.arguments:
            raise ValueError("arguments must be empty for tools/list")
        if self.expected_registered_contract is not None:
            raise ValueError("expected_registered_contract must be omitted for tools/list")
        return self


JobSpec = Annotated[
    JarvisRunSpec | RemoteAgentTaskSpec | McpCallSpec | InputArtifactSpec,
    Field(union_mode="left_to_right"),
]


def deterministic_jarvis_execution_id(
    *,
    cluster: str,
    idempotency_key: str,
    job_id: str,
) -> str:
    """Return the JARVIS execution identity owned by one relay admission."""
    canonical = json.dumps(
        {
            "schema_version": "clio-relay.jarvis-run-execution-identity.v2",
            "cluster": cluster,
            "idempotency_key": idempotency_key,
            "job_id": job_id,
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return f"jarvis_{hashlib.sha256(canonical).hexdigest()[:32]}"


def is_owned_jarvis_run_spec(kind: JobKind, spec: JobSpec) -> bool:
    """Recognize an artifact-bound built-in or registered JARVIS run."""
    if kind is not JobKind.MCP_CALL or not isinstance(spec, McpCallSpec):
        return False
    normalized_tool = (spec.tool or "").replace("-", "_").lower()
    artifact_bound_jarvis = (
        spec.operation is McpOperation.TOOLS_CALL
        and normalized_tool == "jarvis_run"
        and spec.expected_server_artifact_digest is not None
    )
    if not artifact_bound_jarvis:
        return False
    return (
        spec.expected_jarvis_cd_lock_binding is not None
        and spec.expected_registered_contract is None
    ) or (
        spec.expected_jarvis_cd_lock_binding is None
        and spec.expected_registered_contract == REGISTERED_JARVIS_USER_CONTRACT
    )


def _validate_jarvis_execution_id(value: object) -> str:
    """Validate the portable execution-id contract exposed by JARVIS-CD."""
    if (
        not isinstance(value, str)
        or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", value) is None
    ):
        raise ValueError("trusted jarvis_run execution_id must be 1-128 portable ASCII characters")
    reserved_stem = value.split(".", 1)[0].upper()
    reserved = {
        "CON",
        "PRN",
        "AUX",
        "NUL",
        *(f"COM{number}" for number in range(1, 10)),
        *(f"LPT{number}" for number in range(1, 10)),
    }
    if value.endswith(".") or reserved_stem in reserved:
        raise ValueError("trusted jarvis_run execution_id is not a portable path component")
    return value


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
        validated_execution_id = _validate_jarvis_execution_id(supplied_execution_id)
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
    submit_command: list[str] | None
    status_command: list[str] | None = None
    cancel_command: list[str] | None = None
    deployment_driver: str = "jarvis"
    service_port: int = Field(gt=0, le=65535)
    protocol: Literal["http", "https"] = "http"
    health_path: str = "/healthz"
    health_expected_body: str | None = Field(default=None, max_length=4096)
    stream_mode: str = "push"
    stream_path: str | None = "/stream"
    event_stream_path: str | None = "/events"
    state_path: str | None = "/state"
    command_path: str | None = None
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
    def service_runtime_command_must_not_be_empty(
        cls,
        value: list[str] | None,
    ) -> list[str] | None:
        """Reject empty scheduler submission commands."""
        if value == []:
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
        if value not in {"jarvis", "jarvis-bound", "scheduler", "custom"}:
            raise ValueError("deployment_driver must be jarvis, jarvis-bound, scheduler, or custom")
        return value

    @model_validator(mode="after")
    def service_runtime_commands_match_driver(self) -> ServiceRuntimeSpec:
        """Keep verified bindings command-free and submitted runtimes explicit."""
        commands = (self.submit_command, self.status_command, self.cancel_command)
        if self.deployment_driver == "jarvis-bound":
            if any(command is not None for command in commands):
                raise ValueError("jarvis-bound runtimes cannot contain lifecycle commands")
            return self
        if self.submit_command is None:
            raise ValueError("submitted service runtimes require submit_command")
        return self

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
        "command_path",
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

"""MCP control-query admission evidence and durable SEP-2663 task records.

Covers the server-stamped authority explaining a reserved ``control_query``
MCP admission (:class:`McpAdmissionAuthority`, :class:`McpControlQueryEvidence`)
and the durable re-entrant MCP task handle a relay job projects
(:class:`RelayMcpTaskRecord`, :class:`RelayMcpTaskProjection`,
:class:`RelayMcpInputRound`).
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from clio_relay.identifiers import DurableRecordId
from clio_relay.models_enums import JobState, McpOperation
from clio_relay.models_shared import (
    MAX_MCP_TASK_ARGUMENT_BYTES,
    MAX_MCP_TASK_INPUT_ROUND_BYTES,
    MAX_MCP_TASK_PROJECTION_BYTES,
    _require_bounded_mcp_task_json,
    utc_now,
)


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


class RelayMcpInputRound(BaseModel):
    """Durable outstanding input for one re-entrant MCP task leg."""

    model_config = ConfigDict(extra="forbid")

    leg: int = Field(ge=1)
    outstanding: dict[str, dict[str, Any]] = Field(default_factory=dict)
    answered: dict[str, Any] = Field(default_factory=dict)
    request_state: str | None = Field(default=None, max_length=65_536)

    @model_validator(mode="after")
    def input_round_must_be_bounded(self) -> RelayMcpInputRound:
        """Bound the complete round, including untrusted request payloads and answers."""
        _require_bounded_mcp_task_json(
            self.model_dump(mode="json"),
            label="MCP task input round",
            maximum_bytes=MAX_MCP_TASK_INPUT_ROUND_BYTES,
        )
        return self


class RelayMcpTaskProjection(BaseModel):
    """Typed SEP-2663 projection attached to an existing durable relay job."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["clio-relay.mcp-task-projection.v1"] = (
        "clio-relay.mcp-task-projection.v1"
    )
    tool_name: str = Field(min_length=1, max_length=512)
    profile: str = Field(min_length=1, max_length=64)
    arguments: dict[str, Any] = Field(default_factory=dict)
    catalog_revision: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    initial_result: dict[str, Any]
    issued_input_keys: list[str] = Field(default_factory=list, max_length=1_000)
    input_round: RelayMcpInputRound | None = None
    completed_result: dict[str, Any] | None = None
    protocol_error: dict[str, Any] | None = None

    @field_validator("issued_input_keys")
    @classmethod
    def input_keys_must_be_unique(cls, value: list[str]) -> list[str]:
        """Reject ambiguous replay identities for task input requests."""
        if len(value) != len(set(value)):
            raise ValueError("issued_input_keys must be unique")
        if any(not key or len(key) > 512 for key in value):
            raise ValueError("issued_input_keys must contain bounded non-empty strings")
        return value

    @model_validator(mode="after")
    def projection_must_be_bounded(self) -> RelayMcpTaskProjection:
        """Require one unambiguous, finite projection below its record limit."""
        if self.input_round is not None:
            issued = set(self.issued_input_keys)
            outstanding = set(self.input_round.outstanding)
            answered = set(self.input_round.answered)
            if not (outstanding | answered) <= issued:
                raise ValueError("MCP task input round contains an unissued request key")
            if outstanding & answered:
                raise ValueError("MCP task input request cannot be outstanding and answered")
        outcome_count = sum(
            (
                self.protocol_error is not None,
                self.completed_result is not None,
                self.input_round is not None and bool(self.input_round.outstanding),
            )
        )
        if outcome_count > 1:
            raise ValueError("MCP task projection contains conflicting result states")
        _require_bounded_mcp_task_json(
            self.arguments,
            label="MCP task arguments",
            maximum_bytes=MAX_MCP_TASK_ARGUMENT_BYTES,
        )
        _require_bounded_mcp_task_json(
            self.model_dump(mode="json"),
            label="MCP task projection",
            maximum_bytes=MAX_MCP_TASK_PROJECTION_BYTES,
        )
        return self


class RelayMcpTaskRecord(BaseModel):
    """Durable MCP task handle projecting one local or federated relay job."""

    model_config = ConfigDict(extra="forbid")

    task_id: DurableRecordId
    job_id: DurableRecordId
    state: JobState
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)
    projection: RelayMcpTaskProjection

    @model_validator(mode="after")
    def task_id_must_project_job_id(self) -> RelayMcpTaskRecord:
        """Keep the standardized task handle identical to the relay job handle."""
        if self.task_id != self.job_id:
            raise ValueError("MCP task_id must equal its projected relay job_id")
        return self

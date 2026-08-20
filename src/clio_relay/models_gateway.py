"""Artifact index entries and scheduler-backed gateway/service runtime records.

Covers the durable artifact index entry (:class:`ArtifactRef`, including its
clio-provenance-envelope lifting), the scheduler-backed gateway/visualization
session (:class:`GatewaySession`), and the generic runtime-supervisor intent
for a scheduler-backed remote service (:class:`ServiceRuntimeSpec`).
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal, cast

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from clio_relay.identifiers import DurableRecordId
from clio_relay.models_enums import GatewaySessionState
from clio_relay.models_shared import CLIO_PROVENANCE_METADATA_KEY, new_id, utc_now


class ArtifactRef(BaseModel):
    """A durable artifact index entry.

    Clio projections may carry relay fields in the versioned
    ``metadata["clio.provenance.v1"]`` envelope. Validation lifts those fields,
    plus clio's existing metadata ``kind``, only when their relay top-level
    counterparts are absent. The metadata remains unchanged so the clio
    provenance round-trips without permitting unknown top-level fields.
    """

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

    @model_validator(mode="before")
    @classmethod
    def lift_clio_provenance_fields(cls, value: Any) -> Any:
        """Lift relay fields from a versioned clio metadata envelope."""
        if not isinstance(value, dict):
            return value
        values = cast(dict[str, Any], value)
        raw_metadata = values.get("metadata")
        if not isinstance(raw_metadata, dict):
            return values
        metadata = cast(dict[str, Any], raw_metadata)
        lifted = dict(values)
        if "kind" not in lifted and "kind" in metadata:
            lifted["kind"] = metadata["kind"]

        raw_provenance = metadata.get(CLIO_PROVENANCE_METADATA_KEY)
        if not isinstance(raw_provenance, dict):
            return lifted
        provenance = cast(dict[str, Any], raw_provenance)

        for field in ("job_id", "sequence", "uri", "size_bytes", "created_at"):
            if field not in lifted and field in provenance:
                lifted[field] = provenance[field]
        return lifted


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

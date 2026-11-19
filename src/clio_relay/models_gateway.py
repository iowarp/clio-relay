"""Artifact index entries and gateway session records.

Covers the durable artifact index entry (:class:`ArtifactRef`, including its
provenance lifting) and the scheduler-backed gateway session
(:class:`GatewaySession`) that the worker's queue stores.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, cast

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from clio_relay.identifiers import DurableRecordId
from clio_relay.models_enums import GatewaySessionState
from clio_relay.models_shared import CLIO_PROVENANCE_METADATA_KEY, new_id, utc_now

PROVENANCE_KEY = "provenance"


class ArtifactRef(BaseModel):
    """A durable artifact index entry.

    Provenance lives in the neutral ``metadata["provenance"]`` block
    (``clio_relay.serve.provenance``), which any client reads. The legacy
    ``metadata["clio.provenance.v1"]`` envelope is still read until the old
    surface goes. Validation lifts relay fields from the neutral block first,
    then the legacy one, plus the metadata ``kind``, only when their top-level
    counterparts are absent. The metadata remains unchanged, so the block
    round-trips without permitting unknown top-level fields.
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
        """Lift relay fields from the neutral provenance block, then the legacy envelope."""
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

        for key in (PROVENANCE_KEY, CLIO_PROVENANCE_METADATA_KEY):
            raw_provenance = metadata.get(key)
            if not isinstance(raw_provenance, dict):
                continue
            provenance = dict(cast(dict[str, Any], raw_provenance))
            if key == PROVENANCE_KEY:
                if "job_id" not in provenance and "operation_id" in provenance:
                    provenance["job_id"] = provenance["operation_id"]
                fields = ("job_id", "sequence", "uri", "size_bytes", "sha256", "created_at")
                present = [name for name in fields if provenance.get(name) is not None]
            else:
                fields = ("job_id", "sequence", "uri", "size_bytes", "created_at")
                present = [name for name in fields if name in provenance]
            for field in present:
                if field not in lifted:
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

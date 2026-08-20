"""W3C-PROV-style artifact use/transform provenance records.

Covers the bounded, content-pinned dependency evidence a job submission or a
durable ``used`` edge carries (:class:`ArtifactUse`, :class:`UsedArtifactRef`,
:class:`ArtifactUseProvenance`), and the fixed non-secret transform-activity
identity recorded independently of those edges (:class:`TransformRef`,
:class:`TransformEnvironment`, :class:`TransformUseEvidence`).
"""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from clio_relay.identifiers import DurableRecordId
from clio_relay.models_shared import (
    MAX_ARTIFACT_USE_AGGREGATE_BYTES,
    MAX_ARTIFACT_USE_PROVENANCE_BYTES,
    MAX_TRANSFORM_ENVIRONMENT_BYTES,
    MAX_TRANSFORM_REF_BYTES,
    MAX_TRANSFORM_USED_EVIDENCE,
    _require_canonical_json_size,
    utc_now,
)


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


def validate_artifact_use_collection(
    value: list[ArtifactUse] | tuple[ArtifactUse, ...],
) -> None:
    """Bound the complete dependency document independently from item count."""
    _require_canonical_json_size(
        [artifact_use_payload(item) for item in value],
        label="artifact-use collection",
        maximum=MAX_ARTIFACT_USE_AGGREGATE_BYTES,
    )


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

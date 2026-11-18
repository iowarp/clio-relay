"""Typed job intent specs: the discriminated ``JobSpec`` union and its members.

Covers the three submittable job intents (:class:`InputArtifactSpec`,
:class:`JarvisRunSpec`, :class:`RemoteAgentTaskSpec`),
and the :data:`JobSpec` union DSPy/Pydantic dispatches a submission on.
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from clio_relay.identifiers import validate_durable_record_id


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


JobSpec = Annotated[
    JarvisRunSpec | RemoteAgentTaskSpec | InputArtifactSpec,
    Field(union_mode="left_to_right"),
]

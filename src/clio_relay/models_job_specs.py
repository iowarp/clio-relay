"""Typed job intent specs: the discriminated ``JobSpec`` union and its members.

Covers the four submittable job intents (:class:`InputArtifactSpec`,
:class:`JarvisRunSpec`, :class:`RemoteAgentTaskSpec`, :class:`McpCallSpec`),
the :data:`JobSpec` union DSPy/Pydantic dispatches a submission on, and the
deterministic identity helpers that bind a trusted ``jarvis_run`` MCP call to
one relay-owned execution ID.
"""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from clio_relay.identifiers import validate_durable_record_id
from clio_relay.models_enums import JobKind, McpAdmissionClass, McpOperation
from clio_relay.models_jarvis_pipeline import JarvisRunInputManifest
from clio_relay.models_shared import (
    REGISTERED_JARVIS_EXECUTION_CONTRACTS,
    REGISTERED_JARVIS_USER_CONTRACT,
    validate_mcp_env_from,
)


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
    jarvis_input_manifest: JarvisRunInputManifest | None = None
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
            if self.jarvis_input_manifest is not None and (
                self.tool != "jarvis_run"
                or self.expected_registered_contract != REGISTERED_JARVIS_USER_CONTRACT
                or self.expected_jarvis_cd_lock_binding is not None
                or self.arguments.get("pipeline_id") != self.jarvis_input_manifest.route.pipeline_id
            ):
                raise ValueError(
                    "JARVIS input manifests require the exact registered jarvis_run pipeline"
                )
            return self
        if self.tool is not None:
            raise ValueError("tool must be omitted for tools/list")
        if self.arguments:
            raise ValueError("arguments must be empty for tools/list")
        if self.expected_registered_contract is not None:
            raise ValueError("expected_registered_contract must be omitted for tools/list")
        if self.jarvis_input_manifest is not None:
            raise ValueError("jarvis_input_manifest must be omitted for tools/list")
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
        and spec.expected_registered_contract in REGISTERED_JARVIS_EXECUTION_CONTRACTS
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

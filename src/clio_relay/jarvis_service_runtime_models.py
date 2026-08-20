"""Wire/data models for the JARVIS execution service-runtime binding contract.

Extracted from ``jarvis_service_runtime.py`` (clio-relay file-size ratchet,
scripts/check_file_size.py): every Pydantic model JARVIS's service-runtime
snapshot and clio-relay's own binding provenance are built from, plus the
canonical digest/path/UTF-8 validation primitives their field validators are
built on. Pure data shape -- no queue, MCP, or transport dependency -- so it
has no import back onto the facade. The facade module re-exports the names
still referenced from outside this file under their original path so no
other module's imports change.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import math
from pathlib import PurePosixPath
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, SecretStr, field_validator, model_validator

from clio_relay.identifiers import DurableRecordId
from clio_relay.runtime_metadata import JarvisNativeExecutionDocuments

JSON = dict[str, Any]
JARVIS_SERVICE_RUNTIME_SCHEMA_V1 = "jarvis.service-runtime.v1"
JARVIS_SERVICE_RUNTIME_SCHEMA_V2 = "jarvis.service-runtime.v2"
JARVIS_SERVICE_RUNTIME_SCHEMA = JARVIS_SERVICE_RUNTIME_SCHEMA_V2
JARVIS_SERVICE_RUNTIME_SNAPSHOT_SCHEMA = "jarvis.execution.service-runtimes.v1"
JARVIS_DATASET_DESCRIPTOR_SCHEMA = "jarvis.dataset-descriptor.v1"
RELAY_JARVIS_RUNTIME_BINDING_SCHEMA_V1 = "clio-relay.jarvis-service-runtime-binding.v1"
RELAY_JARVIS_RUNTIME_BINDING_SCHEMA_V2 = "clio-relay.jarvis-service-runtime-binding.v2"
RELAY_JARVIS_RUNTIME_BINDING_SCHEMA = RELAY_JARVIS_RUNTIME_BINDING_SCHEMA_V2
JARVIS_SERVICE_RUNTIME_AUTHORITY_SCHEMA = "jarvis.execution.service-runtime-authority.v1"
_HEX_DIGITS = frozenset("0123456789abcdef")


class JarvisArtifactIdentity(BaseModel):
    """Optional exact JARVIS artifact identity attached to a dataset."""

    model_config = ConfigDict(extra="forbid", strict=True)

    artifact_id: str = Field(min_length=1, max_length=512)
    sha256: str

    @field_validator("sha256")
    @classmethod
    def validate_sha256(cls, value: str) -> str:
        """Require a canonical SHA-256 digest."""
        return _canonical_sha256(value, "source_artifact.sha256")


class JarvisDatasetFingerprint(BaseModel):
    """Content identity for a JARVIS dataset descriptor."""

    model_config = ConfigDict(extra="forbid", strict=True)

    algorithm: Literal["sha256"]
    digest: str

    @field_validator("digest")
    @classmethod
    def validate_digest(cls, value: str) -> str:
        """Require the dataset fingerprint to be a canonical SHA-256."""
        return _canonical_sha256(value, "fingerprint.digest")


class JarvisDatasetMember(BaseModel):
    """One ordered member of a dataset collection."""

    model_config = ConfigDict(extra="forbid", strict=True)

    index: int = Field(ge=0)
    location: str
    timestep: float | int | None = Field(default=None, exclude_if=lambda value: value is None)

    @model_validator(mode="after")
    def validate_member(self) -> JarvisDatasetMember:
        """Require one normalized absolute location and a finite optional timestep."""
        _validate_cluster_path(self.location)
        if self.timestep is not None and not math.isfinite(float(self.timestep)):
            raise ValueError("dataset member timestep must be finite")
        return self


class JarvisDatasetArray(BaseModel):
    """Array metadata advertised by a dataset descriptor."""

    model_config = ConfigDict(extra="forbid", strict=True)

    name: str
    association: Literal["point", "cell", "field"]
    components: int = Field(ge=1, le=64)
    units: str | None = Field(default=None, exclude_if=lambda value: value is None)

    @model_validator(mode="after")
    def validate_array(self) -> JarvisDatasetArray:
        """Require printable bounded array labels."""
        _validate_printable_utf8(self.name, "dataset array name", maximum=512)
        if self.units is not None:
            _validate_printable_utf8(self.units, "dataset array units", maximum=256)
        return self


class JarvisDatasetDescriptor(BaseModel):
    """Strict, transport-neutral dataset identity reported by JARVIS."""

    model_config = ConfigDict(extra="forbid", strict=True)

    schema_version: Literal["jarvis.dataset-descriptor.v1"]
    dataset_id: str = Field(min_length=1, max_length=512)
    kind: str = Field(min_length=1, max_length=256)
    format: str = Field(min_length=1, max_length=256)
    members: list[JarvisDatasetMember] = Field(min_length=1, max_length=512)
    arrays: list[JarvisDatasetArray] = Field(max_length=256)
    bounds: list[float | int] | None
    fingerprint: JarvisDatasetFingerprint
    source_artifact: JarvisArtifactIdentity | None

    @model_validator(mode="after")
    def validate_descriptor(self) -> JarvisDatasetDescriptor:
        """Require canonical members, arrays, bounds, and content fingerprint."""
        if [member.index for member in self.members] != list(range(len(self.members))):
            raise ValueError("dataset member indexes must be contiguous and ordered")
        locations = [member.location for member in self.members]
        if len(locations) != len(set(locations)):
            raise ValueError("dataset member locations must be unique")
        array_keys = [(array.association, array.name) for array in self.arrays]
        if len(array_keys) != len(set(array_keys)):
            raise ValueError("dataset arrays repeated an association/name identity")
        if self.bounds is not None:
            if len(self.bounds) != 6 or any(
                not math.isfinite(float(value)) for value in self.bounds
            ):
                raise ValueError("dataset bounds must contain exactly six finite numbers")
            if any(
                float(self.bounds[index]) > float(self.bounds[index + 1]) for index in (0, 2, 4)
            ):
                raise ValueError("dataset bounds minimum exceeded its paired maximum")
        payload = self.model_dump(mode="json")
        payload.pop("fingerprint")
        observed = _canonical_json_sha256(payload)
        if not hmac.compare_digest(observed, self.fingerprint.digest):
            raise ValueError("dataset descriptor fingerprint did not match canonical content")
        return self


class JarvisServiceAuthorization(BaseModel):
    """Public digest identity for one execution-owned service capability."""

    model_config = ConfigDict(extra="forbid", strict=True, hide_input_in_errors=True)

    scheme: Literal["bearer"]
    token_sha256: str

    @field_validator("token_sha256")
    @classmethod
    def validate_token_sha256(cls, value: str) -> str:
        """Require a canonical digest without exposing the bearer capability."""
        if len(value) != 64 or any(character not in _HEX_DIGITS for character in value):
            raise ValueError("service runtime token_sha256 must be 64 lowercase hex characters")
        return value


class JarvisPrivateServiceAuthorization(BaseModel):
    """Process-local bearer returned only by JARVIS's trusted resolver."""

    model_config = ConfigDict(extra="forbid", strict=True, hide_input_in_errors=True)

    scheme: Literal["bearer"]
    token: SecretStr

    @field_validator("token")
    @classmethod
    def validate_token(cls, value: SecretStr) -> SecretStr:
        """Require the exact 256-bit lowercase hexadecimal capability shape."""
        token = value.get_secret_value()
        if len(token) != 64 or any(character not in _HEX_DIGITS for character in token):
            raise ValueError("service runtime bearer token must be 64 lowercase hex characters")
        return value


class JarvisServiceRuntimeAuthority(BaseModel):
    """Identity-checked private authority for one exact current service revision."""

    model_config = ConfigDict(extra="forbid", strict=True, hide_input_in_errors=True)

    schema_version: Literal["jarvis.execution.service-runtime-authority.v1"]
    execution_id: str = Field(min_length=1, max_length=512)
    pipeline_id: str = Field(min_length=1, max_length=512)
    package_id: str = Field(min_length=1, max_length=256)
    service_instance_id: str = Field(min_length=1, max_length=512)
    revision: int = Field(ge=1)
    token_sha256: str
    authorization: JarvisPrivateServiceAuthorization

    @field_validator("token_sha256")
    @classmethod
    def validate_token_sha256(cls, value: str) -> str:
        """Require the canonical public identity of the resolved private token."""
        return _canonical_sha256(value, "service runtime authority token_sha256")

    @model_validator(mode="after")
    def validate_authority_digest(self) -> JarvisServiceRuntimeAuthority:
        """Bind the private bearer to the public digest in the same response."""
        observed = hashlib.sha256(
            self.authorization.token.get_secret_value().encode("ascii")
        ).hexdigest()
        if not hmac.compare_digest(observed, self.token_sha256):
            raise ValueError("service runtime authority token did not match token_sha256")
        return self


class JarvisServiceRuntime(BaseModel):
    """Latest exact service report for one JARVIS package instance."""

    model_config = ConfigDict(extra="forbid", strict=True, hide_input_in_errors=True)

    schema_version: Literal["jarvis.service-runtime.v1", "jarvis.service-runtime.v2"]
    execution_id: str = Field(min_length=1, max_length=512)
    package_name: str = Field(min_length=1, max_length=256)
    package_id: str = Field(min_length=1, max_length=256)
    service_instance_id: str = Field(min_length=1, max_length=512)
    revision: int = Field(ge=1)
    lifecycle: Literal["starting", "ready", "degraded", "stopping", "stopped", "failed"]
    host: str = Field(min_length=1, max_length=1_024)
    port: int = Field(gt=0, le=65_535)
    protocol: Literal["http", "https"]
    health_path: str
    live_data_path: str
    events_path: str
    state_path: str
    command_path: str
    delivery_mode: Literal["push"]
    authorization: JarvisServiceAuthorization | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )
    dataset_descriptor: JarvisDatasetDescriptor
    message: str | None = Field(default=None, max_length=16_384)
    observed_at_epoch: float = Field(ge=0)

    @field_validator("host")
    @classmethod
    def validate_host(cls, value: str) -> str:
        """Reject host strings that cannot safely identify one connector target."""
        if (
            value != value.strip()
            or value.startswith("-")
            or any(
                character.isspace() or ord(character) < 32 or ord(character) == 127
                for character in value
            )
            or any(character in value for character in "/\\?#@")
        ):
            raise ValueError("service runtime host is invalid")
        return value

    @field_validator(
        "health_path",
        "live_data_path",
        "events_path",
        "state_path",
        "command_path",
    )
    @classmethod
    def validate_http_path(cls, value: str) -> str:
        """Require one normalized absolute HTTP path without query or fragment data."""
        if (
            not value.startswith("/")
            or len(value) > 2_048
            or "\\" in value
            or "?" in value
            or "#" in value
            or "//" in value
            or str(PurePosixPath(value)) != value
        ):
            raise ValueError("service runtime paths must be normalized absolute HTTP paths")
        return value

    @field_validator("observed_at_epoch")
    @classmethod
    def validate_observed_at_epoch(cls, value: float) -> float:
        """Require JARVIS's exact finite, nonnegative epoch observation."""
        if not math.isfinite(value):
            raise ValueError("service runtime observed_at_epoch must be finite")
        return value

    @model_validator(mode="after")
    def validate_versioned_authorization(self) -> JarvisServiceRuntime:
        """Keep released v1 unauthenticated and require a capability in v2."""
        if self.schema_version == JARVIS_SERVICE_RUNTIME_SCHEMA_V1:
            if self.authorization is not None:
                raise ValueError("service runtime v1 cannot contain authorization")
        elif self.authorization is None:
            raise ValueError("service runtime v2 requires authorization")
        return self


class JarvisExecutionServiceRuntimes(BaseModel):
    """Strict execution-scoped snapshot returned by JARVIS."""

    model_config = ConfigDict(extra="forbid", strict=True)

    schema_version: Literal["jarvis.execution.service-runtimes.v1"]
    execution_id: str = Field(min_length=1, max_length=512)
    pipeline_id: str = Field(min_length=1, max_length=512)
    execution_state: str = Field(min_length=1, max_length=64)
    terminal: bool
    service_runtimes: list[JarvisServiceRuntime] = Field(max_length=4_096)

    @model_validator(mode="after")
    def validate_runtime_identities(self) -> JarvisExecutionServiceRuntimes:
        """Require one latest report per service instance and stable execution identity."""
        instances: set[str] = set()
        expected_order: list[tuple[str, str]] = []
        for runtime in self.service_runtimes:
            if runtime.execution_id != self.execution_id:
                raise ValueError("service runtime execution identity did not match snapshot")
            if runtime.service_instance_id in instances:
                raise ValueError("service runtime snapshot repeated a service_instance_id")
            instances.add(runtime.service_instance_id)
            expected_order.append((runtime.package_id, runtime.service_instance_id))
        if expected_order != sorted(expected_order):
            raise ValueError("service runtime snapshot order is not canonical")
        return self


class ClioKitJarvisExecutionQuery(BaseModel):
    """Exact clio-kit execution-v2 view required for service binding."""

    model_config = ConfigDict(extra="forbid", strict=True)

    schema_version: Literal["clio-kit.jarvis-execution.v2"]
    pipeline_id: str = Field(min_length=1, max_length=256)
    execution_id: str = Field(min_length=1, max_length=256)
    execution_handle: JSON
    execution_record: JSON
    runtime_metadata: JSON
    progress: JSON
    artifact_page: JSON | None
    service_runtimes: JarvisExecutionServiceRuntimes

    @model_validator(mode="after")
    def validate_execution_identity(self) -> ClioKitJarvisExecutionQuery:
        """Bind the requested execution identity to its service snapshot."""
        if (
            self.execution_id != self.service_runtimes.execution_id
            or self.pipeline_id != self.service_runtimes.pipeline_id
        ):
            raise ValueError("clio-kit execution query identity did not match service snapshot")
        return self


class JarvisServiceRuntimeBinding(BaseModel):
    """Immutable provenance persisted by clio-relay for a bound service."""

    model_config = ConfigDict(extra="forbid", strict=True)

    schema_version: Literal[
        "clio-relay.jarvis-service-runtime-binding.v1",
        "clio-relay.jarvis-service-runtime-binding.v2",
    ] = RELAY_JARVIS_RUNTIME_BINDING_SCHEMA_V2
    source_relay_job_id: str = Field(min_length=1, max_length=512)
    source_relay_artifact_id: str = Field(min_length=1, max_length=512)
    source_relay_artifact_sha256: str
    source_tool: Literal["jarvis_get_execution"]
    jarvis_execution_id: str = Field(min_length=1, max_length=512)
    scheduler_provider: str | None = Field(default=None, max_length=256)
    scheduler_native_id: str | None = Field(default=None, max_length=256)
    package_id: str = Field(min_length=1, max_length=256)
    package_name: str = Field(min_length=1, max_length=256)
    service_instance_id: str = Field(min_length=1, max_length=512)
    service_revision: int = Field(ge=1)
    service_report_sha256: str
    service_runtime_schema_version: Literal["jarvis.service-runtime.v2"] | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )
    authorization_sha256: str | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )
    dataset_descriptor_sha256: str
    dataset_descriptor: JarvisDatasetDescriptor

    @field_validator(
        "source_relay_artifact_sha256",
        "service_report_sha256",
        "authorization_sha256",
        "dataset_descriptor_sha256",
    )
    @classmethod
    def validate_digests(cls, value: str | None) -> str | None:
        """Require canonical SHA-256 values for every persisted evidence digest."""
        if value is None:
            return value
        return _canonical_sha256(value, "binding digest")

    @model_validator(mode="after")
    def validate_versioned_runtime_binding(self) -> JarvisServiceRuntimeBinding:
        """Require authenticated runtime provenance only in binding v2."""
        if self.schema_version == RELAY_JARVIS_RUNTIME_BINDING_SCHEMA_V1:
            if (
                self.service_runtime_schema_version is not None
                or self.authorization_sha256 is not None
            ):
                raise ValueError("JARVIS runtime binding v1 cannot contain v2 authorization fields")
        elif (
            self.service_runtime_schema_version != JARVIS_SERVICE_RUNTIME_SCHEMA_V2
            or self.authorization_sha256 is None
        ):
            raise ValueError("JARVIS runtime binding v2 requires authenticated runtime provenance")
        return self


class JarvisServiceRuntimeHandoff(BaseModel):
    """Agent-facing selectors copied unchanged into a relay runtime bind call."""

    model_config = ConfigDict(extra="forbid", strict=True)

    cluster: str = Field(min_length=1, max_length=256)
    source_job_id: DurableRecordId
    source_artifact_id: DurableRecordId
    package_id: str = Field(min_length=1, max_length=256)
    package_name: str = Field(min_length=1, max_length=256)
    service_instance_id: str = Field(min_length=1, max_length=512)


class VerifiedJarvisServiceRuntime(BaseModel):
    """Validated runtime and its immutable relay provenance."""

    model_config = ConfigDict(extra="forbid", strict=True, arbitrary_types_allowed=True)

    binding: JarvisServiceRuntimeBinding
    runtime: JarvisServiceRuntime
    native_execution: JarvisNativeExecutionDocuments


def _validate_cluster_path(value: str) -> None:
    path = PurePosixPath(value)
    if (
        not value.startswith("/")
        or str(path) != value
        or ".." in path.parts
        or any(ord(character) < 32 or ord(character) == 127 for character in value)
    ):
        raise ValueError("dataset members must use normalized absolute cluster paths")


def _validate_printable_utf8(value: str, label: str, *, maximum: int) -> None:
    if (
        not value
        or len(value.encode("utf-8")) > maximum
        or any(ord(character) < 32 or ord(character) == 127 for character in value)
    ):
        raise ValueError(f"{label} must be non-empty printable UTF-8 within {maximum} bytes")


def _canonical_sha256(value: str, label: str) -> str:
    if len(value) != 64 or any(character not in _HEX_DIGITS for character in value):
        raise ValueError(f"{label} must be a canonical SHA-256")
    return value


def _canonical_json_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def _canonical_json_sha256(value: object) -> str:
    return hashlib.sha256(_canonical_json_bytes(value)).hexdigest()

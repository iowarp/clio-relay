"""Wire models for the durable installation/component-identity receipt.

Extracted from ``installation.py`` (iowarp/clio-relay#231): the receipt-shaped
Pydantic models (and the schema-version constants their fields default to)
are pure data definitions with no dependency on any other installation
concern, so they are the natural first layer every probing/verification
owner module built on top of them (``persistent_uv_tool_probe.py``,
``native_jarvis_contract.py``, ``component_runtime_identity.py``,
``component_verification_remote.py``, ``worker_runtime_verification.py``)
imports from -- never the reverse.
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from clio_relay.contract_gate import SurfaceContractDegradation, SurfaceContractStatus
from clio_relay.validation_report import SoftwareIdentity

INSTALL_RECEIPT_SCHEMA = "clio-relay.install-receipt.v1"
NATIVE_JARVIS_CAPABILITY_SCHEMA = "clio-relay.jarvis-native-execution-capability.v1"
PERSISTENT_UV_TOOL_IDENTITY_SCHEMA = "clio-relay.persistent-uv-tool-identity.v2"
JARVIS_EXECUTION_HANDLE_SCHEMA = "jarvis.execution.handle.v1"
JARVIS_EXECUTION_RECORD_SCHEMA = "jarvis.execution.record.v1"
JARVIS_EXECUTION_PROGRESS_SCHEMA = "jarvis.execution.progress.v1"


def _is_sha256_text(value: object) -> bool:
    """Return whether a value is one canonical hexadecimal SHA-256 digest."""
    return (
        isinstance(value, str)
        and len(value) == 64
        and value == value.lower()
        and all(character in "0123456789abcdef" for character in value)
    )


class NativeJarvisExecutionCapability(BaseModel):
    """Receipt-bound native execution and progress contract for one component."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["clio-relay.jarvis-native-execution-capability.v1"] = (
        NATIVE_JARVIS_CAPABILITY_SCHEMA
    )
    handle_schema: Literal["jarvis.execution.handle.v1"] = JARVIS_EXECUTION_HANDLE_SCHEMA
    record_schema: Literal["jarvis.execution.record.v1"] = JARVIS_EXECUTION_RECORD_SCHEMA
    progress_schema: Literal["jarvis.execution.progress.v1"] = JARVIS_EXECUTION_PROGRESS_SCHEMA
    operations: list[str] = Field(min_length=1)
    contract_id: str | None = None
    contract_schema_version: str | None = None
    contract_sha256: str | None = None

    @model_validator(mode="after")
    def validate_identity(self) -> NativeJarvisExecutionCapability:
        """Reject ambiguous operation or optional contract identities."""
        if any(not operation or operation.strip() != operation for operation in self.operations):
            raise ValueError("native JARVIS capability operations must be non-empty strings")
        if len(set(self.operations)) != len(self.operations) or self.operations != sorted(
            self.operations
        ):
            raise ValueError("native JARVIS capability operations must be unique and sorted")
        contract_values = (
            self.contract_id,
            self.contract_schema_version,
            self.contract_sha256,
        )
        if any(value is not None for value in contract_values):
            if not all(isinstance(value, str) and value for value in contract_values):
                raise ValueError("native JARVIS contract identity must be complete")
            assert self.contract_sha256 is not None
            if not _is_sha256_text(self.contract_sha256):
                raise ValueError("native JARVIS contract SHA-256 is invalid")
        return self


class PersistentUvToolIdentity(BaseModel):
    """Receipt-bound identity of one install-once uv tool environment."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["clio-relay.persistent-uv-tool-identity.v2"] = (
        PERSISTENT_UV_TOOL_IDENTITY_SCHEMA
    )
    manager: Literal["uv"] = "uv"
    uv_executable: str
    uv_version: str
    uv_executable_sha256: str
    tool_directory: str
    tool_bin_directory: str
    environment_prefix: str
    provider_interpreter: str
    provider_interpreter_sha256: str
    tool_executable: str
    tool_executable_resolved: str
    tool_executable_sha256: str
    distribution_console_script_path: str
    distribution_console_script_sha256: str
    uv_receipt_path: str
    uv_receipt_sha256: str
    distribution: str
    distribution_version: str
    distribution_metadata_path: str
    entry_point: str
    source_artifact_path: str
    source_artifact_sha256: str
    record_path: str
    record_sha256: str
    runtime_closure_sha256: str
    runtime_file_count: int = Field(gt=0)
    runtime_bytes: int = Field(gt=0)
    pyvenv_uv_version: str

    @model_validator(mode="after")
    def validate_identity(self) -> PersistentUvToolIdentity:
        """Reject incomplete path, version, and digest identities."""
        paths = (
            self.uv_executable,
            self.tool_directory,
            self.tool_bin_directory,
            self.environment_prefix,
            self.provider_interpreter,
            self.tool_executable,
            self.tool_executable_resolved,
            self.distribution_console_script_path,
            self.uv_receipt_path,
            self.distribution_metadata_path,
            self.source_artifact_path,
            self.record_path,
        )
        if any(not path or path.strip() != path for path in paths):
            raise ValueError("persistent uv tool paths must be non-empty strings")
        digests = (
            self.uv_executable_sha256,
            self.provider_interpreter_sha256,
            self.tool_executable_sha256,
            self.distribution_console_script_sha256,
            self.uv_receipt_sha256,
            self.source_artifact_sha256,
            self.record_sha256,
            self.runtime_closure_sha256,
        )
        if any(not _is_sha256_text(digest) for digest in digests):
            raise ValueError("persistent uv tool SHA-256 identity is invalid")
        if self.pyvenv_uv_version != self.uv_version:
            raise ValueError("persistent uv tool pyvenv marker must match uv")
        return self


class ComponentArtifactIdentity(BaseModel):
    """Immutable install identity for a runtime component used by the relay."""

    model_config = ConfigDict(extra="forbid")

    distribution: str
    distribution_version: str | None = None
    install_spec: str
    requested_source: str
    artifact_filename: str | None = None
    artifact_sha256: str | None = None
    runtime_artifact_path: str | None = None
    runtime_command: list[str] = Field(default_factory=list)
    runtime_interpreters: dict[str, str] = Field(default_factory=dict)
    runtime_executables: dict[str, str] = Field(default_factory=dict)
    source_commit: str | None = None
    entry_points: list[str] = Field(default_factory=list)
    native_execution: NativeJarvisExecutionCapability | None = None
    persistent_tool: PersistentUvToolIdentity | None = None
    locked_server_runtime: dict[str, object] | None = None


class InstallReceipt(BaseModel):
    """Artifact and source identity recorded at cluster installation time."""

    model_config = ConfigDict(extra="forbid")

    schema_version: str = INSTALL_RECEIPT_SCHEMA
    installed_at: datetime
    install_spec: str
    requested_source: str
    artifact_filename: str | None = None
    artifact_sha256: str | None = None
    distribution_version: str
    software: SoftwareIdentity
    components: dict[str, str] = Field(default_factory=dict)
    component_artifacts: dict[str, ComponentArtifactIdentity] = Field(default_factory=dict)
    # Per-surface MCP contract identity (iowarp/clio-relay#242): what each
    # probed surface actually shipped, recorded regardless of whether it
    # meets this relay's pin, plus the typed degradation for any surface
    # that does not. An absent/empty mapping (every receipt written before
    # this change) means "not probed this way" -- callers that gate on a
    # specific surface fall back to their pre-existing behavior.
    contract_surfaces: dict[str, SurfaceContractStatus] = Field(default_factory=dict)
    contract_degradations: list[SurfaceContractDegradation] = Field(
        default_factory=lambda: list[SurfaceContractDegradation]()
    )
    deployment_fingerprint: str | None = None
    deployment_manifest: dict[str, object] | None = None
    generation: str | None = None
    # Provenance note for a mixed install (e.g. relay identity minted from a
    # local build, components inherited from a bootstrap generation): the
    # source receipt path components/component_artifacts were copied
    # verbatim from. None when this receipt's own process derived them.
    components_source_receipt: str | None = None

    @model_validator(mode="after")
    def validate_deployment_identity(self) -> InstallReceipt:
        """Require deployment identity fields to be present as one complete set."""
        manifest_present = self.deployment_manifest is not None
        fingerprint_present = self.deployment_fingerprint is not None
        if manifest_present is not fingerprint_present:
            raise ValueError(
                "deployment fingerprint and manifest must either both be present or both be absent"
            )
        if self.deployment_fingerprint is not None and not _is_sha256_text(
            self.deployment_fingerprint
        ):
            raise ValueError("deployment fingerprint must be one lowercase SHA-256 digest")
        if self.generation is not None and not _is_sha256_text(self.generation):
            raise ValueError("install generation must be one lowercase SHA-256 digest")
        return self

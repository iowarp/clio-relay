"""Pydantic identity models for bootstrap desired-state and reconcile plans.

``BootstrapDesiredState`` is the canonical identity requested by one
bootstrap invocation; the rest of this module captures the inspection/plan/
replacement-provider evidence built from it. Pure data + validators only --
no I/O (iowarp/clio-relay#255).
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from clio_relay.bootstrap_reconcile_constants import (
    BOOTSTRAP_DESIRED_STATE_SCHEMA,
    MANAGED_JARVIS_REPO_PATH,
)
from clio_relay.bootstrap_reconcile_primitives import _require_sha256, canonical_json_sha256


class BootstrapDesiredState(BaseModel):
    """Complete, canonical identity requested by one bootstrap invocation."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["clio-relay.bootstrap-desired-state.v1"] = (
        BOOTSTRAP_DESIRED_STATE_SCHEMA
    )
    bootstrap_profile: Literal["linux-user"] = "linux-user"
    cluster: str | None = None
    core_dir: str
    spool_dir: str
    worker_service: str | None = None
    relay_install_spec: str
    relay_artifact_sha256: str | None = None
    relay_source_identity: str = Field(min_length=1, max_length=512)
    frp_version: str
    frpc_sha256: str
    frps_sha256: str
    uv_version: str
    uv_sha256: str
    jarvis_util_commit: str
    jarvis_cd_version: str
    jarvis_cd_wheel_url: str
    jarvis_cd_wheel_sha256: str
    jarvis_resource_graph_profile: str | None = None
    allow_jarvis_resource_graph_build: bool = False
    clio_kit_install_spec: str
    clio_kit_version: str
    clio_kit_artifact_sha256: str
    agent_adapter: str
    agent_npm_package: str | None = None
    agent_npm_bin: str | None = None
    agent_args: list[str] = Field(default_factory=list)
    jarvis_root: str = "~/.ppi-jarvis"
    jarvis_config_dir: str = "~/.local/share/clio-relay/jarvis-config"
    jarvis_private_dir: str = "~/.local/share/clio-relay/jarvis-private"
    jarvis_shared_dir: str = "~/.local/share/clio-relay/jarvis-shared"
    managed_jarvis_repo: str = MANAGED_JARVIS_REPO_PATH

    @model_validator(mode="after")
    def validate_identity(self) -> BootstrapDesiredState:
        """Reject incomplete or ambiguous desired identities."""
        for field_name in (
            "frpc_sha256",
            "frps_sha256",
            "uv_sha256",
            "jarvis_cd_wheel_sha256",
            "clio_kit_artifact_sha256",
        ):
            _require_sha256(getattr(self, field_name), field=field_name)
        if self.relay_artifact_sha256 is not None:
            _require_sha256(self.relay_artifact_sha256, field="relay_artifact_sha256")
        if self.relay_artifact_sha256 is not None and not self.relay_source_identity.endswith(
            f":sha256:{self.relay_artifact_sha256}"
        ):
            raise ValueError("relay source identity must match its artifact SHA-256")
        if any(character in self.relay_source_identity for character in "\x00\r\n"):
            raise ValueError("relay source identity contains a control boundary")
        if self.cluster is None and self.worker_service is not None:
            raise ValueError("an unmanaged bootstrap cannot name a worker service")
        if self.cluster is not None and not self.worker_service:
            raise ValueError("a managed cluster bootstrap must name its worker service")
        profile = self.jarvis_resource_graph_profile
        if profile is not None and (
            not profile
            or profile != profile.strip()
            or len(profile) > 256
            or profile in {".", ".."}
            or "/" in profile
            or "\\" in profile
            or any(ord(character) < 32 or ord(character) == 127 for character in profile)
        ):
            raise ValueError("JARVIS resource graph profile must be one safe exact name")
        if self.allow_jarvis_resource_graph_build and profile is None:
            raise ValueError("JARVIS graph build fallback requires an exact requested profile")
        return self

    @property
    def fingerprint(self) -> str:
        """Return the content identity of this desired deployment."""
        return canonical_json_sha256(self.model_dump(mode="json"))


class JarvisStateEvidence(BaseModel):
    """Read-only identity of operator-owned JARVIS state."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    initialized: bool
    root: str
    roots: dict[str, str] = Field(default_factory=dict)
    config_sha256: str | None = None
    repos_sha256: str | None = None
    resource_graph_sha256: str | None = None
    managed_repo_registered: bool = False
    managed_builtin_repo_registered: bool = False


class BootstrapReadinessEvidence(BaseModel):
    """Bounded no-scheduler readiness proof for an installed generation."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    service_name: str | None = None
    service_was_active: bool | None = None
    service_was_enabled: bool | None = None
    queue_ready: bool
    queue: dict[str, object] | None = None
    worker_ready: bool | None = None
    worker: dict[str, object] | None = None
    scheduler_jobs_submitted: int = 0


class BootstrapInspection(BaseModel):
    """Result of a read-only exact-no-op inspection."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    exact_match: bool
    desired_fingerprint: str
    reasons: list[str] = Field(default_factory=list)
    install_receipt_sha256: str | None = None
    active_generation: str | None = None
    current_generation_target: str | None = None
    jarvis_state: JarvisStateEvidence
    readiness: BootstrapReadinessEvidence


class BootstrapActivationPathIdentity(BaseModel):
    """Immutable identity of one pre-activation path."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    device: int = Field(ge=0)
    inode: int = Field(ge=0)
    mode: int = Field(ge=0)
    size: int = Field(ge=0)
    modified_ns: int = Field(ge=0)
    changed_ns: int = Field(ge=0)
    file_type: Literal["file", "symlink"]
    sha256: str | None = None
    symlink_target: str | None = None

    @model_validator(mode="after")
    def validate_content_identity(self) -> BootstrapActivationPathIdentity:
        """Require content evidence appropriate to the captured file type."""
        if self.file_type == "file":
            if self.sha256 is None or self.symlink_target is not None:
                raise ValueError("bootstrap activation file identity is incomplete")
            _require_sha256(self.sha256, field="activation_path.sha256")
        elif self.sha256 is not None or not self.symlink_target:
            raise ValueError("bootstrap activation symlink identity is incomplete")
        return self


class BootstrapActivationPath(BaseModel):
    """One stable activation path and its exact state before fencing."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    path: str
    kind: Literal["file", "file_or_symlink", "symlink"]
    before: BootstrapActivationPathIdentity | None = None

    @model_validator(mode="after")
    def validate_path(self) -> BootstrapActivationPath:
        """Require an absolute normalized path and a compatible identity."""
        candidate = Path(self.path)
        if (
            not candidate.is_absolute()
            or ".." in candidate.parts
            or os.path.normpath(self.path) != self.path
            or any(character in self.path for character in "\x00\r\n")
        ):
            raise ValueError("bootstrap activation path must be absolute and normalized")
        if self.before is not None and not (
            (self.kind == "file" and self.before.file_type == "file")
            or (self.kind == "symlink" and self.before.file_type == "symlink")
            or self.kind == "file_or_symlink"
        ):
            raise ValueError("bootstrap activation path identity has an invalid type")
        return self


class BootstrapReconcilePlan(BaseModel):
    """Read-only component plan produced before journaling, fencing, or activation."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    mode: Literal["repair", "relay-only", "component-upgrade", "full"]
    desired_fingerprint: str
    reasons: list[str] = Field(default_factory=list)
    component_actions: dict[str, Literal["reuse", "replace"]]
    reusable_paths: dict[str, str] = Field(default_factory=dict)
    activation_paths: dict[str, BootstrapActivationPath] = Field(default_factory=dict)


class BootstrapPersistentUvToolIdentity(BaseModel):
    """Typed candidate uv-tool identity independent of the installed relay version."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["clio-relay.persistent-uv-tool-identity.v2"]
    manager: Literal["uv"]
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


class BootstrapReplacementProviderEvidence(BaseModel):
    """Attested candidate provider allowed to replace one legacy relay runtime."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["clio-relay.bootstrap-replacement-provider.v1"] = (
        "clio-relay.bootstrap-replacement-provider.v1"
    )
    desired_fingerprint: str
    relay_install_spec: str
    preparing_root: str
    extracted_source_root: str
    source_archive_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    coordinator_provider_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    persistent_tool: BootstrapPersistentUvToolIdentity

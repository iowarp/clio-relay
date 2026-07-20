"""Machine-readable evidence for live validation and release decisions."""

from __future__ import annotations

import base64
import binascii
import csv
import ctypes
import hashlib
import io
import ipaddress
import json
import math
import os
import re
import shutil
import socket
import stat
import subprocess
import sys
import tomllib
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from collections.abc import Generator, Iterable
from contextlib import contextmanager, suppress
from datetime import UTC, datetime
from enum import StrEnum
from importlib import metadata, resources
from pathlib import Path, PurePosixPath
from typing import Any, Literal, cast
from uuid import uuid4

import yaml
from pydantic import BaseModel, ConfigDict, Field, PrivateAttr, ValidationError, model_validator

from clio_relay import __version__
from clio_relay.ci_validation import ProvenanceError, load_release_acceptance_matrix
from clio_relay.errors import ConfigurationError
from clio_relay.filesystem_paths import (
    internal_filesystem_path,
    logical_filesystem_path,
    logical_filesystem_text,
)
from clio_relay.identifiers import DurableRecordId

REPORT_SCHEMA_VERSION = "1.0"
TRANSPORT_PROBE_EVIDENCE_KEY = "transport.probe_evidence"
MAX_TRANSPORT_PROBE_EVIDENCE_BYTES = 256 * 1024
MAX_TRANSPORT_PROBE_RESOURCES = 128
MAX_TRANSPORT_PROBE_JSON_DEPTH = 16
MAX_TRANSPORT_PROBE_JSON_NODES = 4096
MAX_LAUNCHER_PROCESS_ANCESTORS = 64
MAX_PYVENV_CONFIG_BYTES = 64 * 1024
MAX_UV_TOOL_RECEIPT_BYTES = 256 * 1024
MAX_DISTRIBUTION_WHEEL_BYTES = 128 * 1024 * 1024
_OFFICIAL_RELEASE_WHEEL_PATH = re.compile(
    r"/iowarp/clio-relay/releases/download/v(?P<version>[0-9A-Za-z][0-9A-Za-z.+-]*)/"
    r"clio_relay-(?P=version)-py3-none-any\.whl"
)
SPACK_FRESH_INSTALL_TRANSITION_CHECK_IDS = (
    "remote-mcp.spack-preinstall-absent",
    "remote-mcp.spack-fresh-install",
    "remote-mcp.spack-postinstall-locate",
    "remote-mcp.spack-disposable-store",
    "remote-mcp.spack-transition-identity",
    "remote-mcp.spack-transition-durable-evidence",
    "remote-mcp.spack-fresh-configuration",
)
TransportCleanupAction = Literal["retain", "stop", "close", "cancel"]
TransportCleanupOutcome = Literal[
    "retained",
    "stopped",
    "closed",
    "canceled",
    "terminal",
    "missing",
    "refused",
    "failed",
    "replaced",
    "residual",
    "unknown",
    "metadata_missing",
    "invalid_metadata",
    "ownership_refused",
]


def _utc_now() -> datetime:
    return datetime.now(UTC)


class ValidationStatus(StrEnum):
    """Terminal status for a report or one validation check."""

    PASSED = "passed"
    FAILED = "failed"


class InstallSourceKind(StrEnum):
    """How the clio-relay distribution under test was installed."""

    PYPI = "pypi"
    WHEEL = "wheel"
    EDITABLE = "editable"
    VCS = "vcs"
    CHECKOUT = "checkout"
    UNKNOWN = "unknown"


class EvidenceOrigin(StrEnum):
    """Who assembled a validation report before any release sealing step."""

    LOCAL_PROCESS = "local_process"
    OPERATOR_GENERATED = "operator_generated"


class EvidenceTrust(BaseModel):
    """Explicit trust boundary for machine-readable validation evidence."""

    model_config = ConfigDict(extra="forbid")

    origin: EvidenceOrigin = EvidenceOrigin.OPERATOR_GENERATED
    producer_execution_verified: Literal[False] = False
    producer_github_login: str | None = Field(
        default=None,
        min_length=1,
        max_length=39,
    )
    producer_github_id: int | None = Field(default=None, strict=True, gt=0)
    invocation_id: str | None = Field(
        default=None,
        min_length=8,
        max_length=128,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]*$",
    )
    limitation: str = (
        "Report fields are self-recorded by the validation process; non-local reports require "
        "reviewer sealing and do not independently prove target execution."
    )

    @model_validator(mode="after")
    def validate_producer_identity(self) -> EvidenceTrust:
        """Validate any producer fields present without blocking diagnostic reports."""
        login = self.producer_github_login
        if login is not None and (
            re.fullmatch(r"[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?", login) is None
            or "--" in login
        ):
            raise ValueError("producer_github_login is not a valid GitHub login")
        return self


class EvidenceReference(BaseModel):
    """A compact excerpt or stable reference supporting a check."""

    model_config = ConfigDict(extra="forbid")

    kind: str
    reference: str | None = None
    excerpt: str | None = None
    sha256: str | None = None
    metadata: dict[str, Any] = Field(default_factory=lambda: dict[str, Any]())

    @model_validator(mode="after")
    def require_reference_or_excerpt(self) -> EvidenceReference:
        """Reject evidence records that contain no usable evidence."""
        if self.reference is None and self.excerpt is None:
            raise ValueError("evidence requires reference or excerpt")
        return self


class ValidationCheck(BaseModel):
    """One independently reviewable acceptance check."""

    model_config = ConfigDict(extra="forbid")

    check_id: str
    summary: str
    status: ValidationStatus
    started_at: datetime
    completed_at: datetime
    evidence: list[EvidenceReference] = Field(default_factory=lambda: list[EvidenceReference]())
    error: str | None = None


class SoftwareIdentity(BaseModel):
    """Version-control identity embedded in or observed for the package."""

    model_config = ConfigDict(extra="forbid")

    version: str
    commit: str | None = None
    tag: str | None = None
    dirty: bool | None = None


class InstallSource(BaseModel):
    """Install provenance for the exact process running validation."""

    model_config = ConfigDict(extra="forbid")

    kind: InstallSourceKind
    detected_kind: InstallSourceKind = InstallSourceKind.UNKNOWN
    reference: str | None = None
    launcher: str = "unknown"
    package_path: str
    distribution_version: str
    artifact_sha256: str | None = None
    direct_url: dict[str, Any] | None = None
    artifact_identity_verified: bool = False
    released_artifact: bool = False
    launcher_verified: bool = False
    launcher_receipt: dict[str, Any] = Field(default_factory=dict[str, Any])

    @model_validator(mode="after")
    def released_source_requires_verified_artifact_identity(self) -> InstallSource:
        """Reject internally inconsistent released-artifact claims."""
        released_source = self.kind is InstallSourceKind.PYPI or (
            self.kind is InstallSourceKind.WHEEL
            and _is_official_github_release_wheel(self.direct_url, self.distribution_version)
        )
        if self.released_artifact and not (
            self.kind is self.detected_kind
            and released_source
            and self.launcher == "uv-tool"
            and self.artifact_sha256 is not None
            and self.artifact_identity_verified
            and self.launcher_verified
        ):
            raise ValueError("released artifact requires verified uv-tool artifact identity")
        return self


class ValidationResource(BaseModel):
    """A job, session, connector, scheduler allocation, or artifact in a run."""

    model_config = ConfigDict(extra="forbid")

    kind: str
    resource_id: str
    role: str | None = None
    cluster: str | None = None
    state: str | None = None
    provider: str | None = None
    references: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=lambda: dict[str, Any]())


class TransportCleanupResourceEvidence(BaseModel):
    """Strict cleanup result for one resource owned by a transport probe."""

    model_config = ConfigDict(extra="forbid", strict=True)

    kind: str = Field(min_length=1, max_length=128)
    resource_id: str = Field(min_length=1, max_length=1024)
    role: str = Field(min_length=1, max_length=128)
    location: str | None = Field(default=None, max_length=4096)
    action: TransportCleanupAction
    ownership_verified: bool
    outcome: TransportCleanupOutcome
    provider: str | None = Field(default=None, max_length=128)
    verified_after_operation: bool
    observed_state: str | None = Field(default=None, max_length=1024)
    residual: bool
    detail: str | None = Field(default=None, max_length=8192)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_cleanup_state(self) -> TransportCleanupResourceEvidence:
        successful = {"retained", "stopped", "closed", "canceled", "terminal"}
        unresolved = {
            "refused",
            "failed",
            "residual",
            "unknown",
            "metadata_missing",
            "invalid_metadata",
            "ownership_refused",
        }
        if self.outcome in successful and not (
            self.ownership_verified and self.verified_after_operation and not self.residual
        ):
            raise ValueError(
                "successful transport cleanup requires verified owned absence or state"
            )
        if self.outcome in {"missing", "replaced"} and not self.verified_after_operation:
            raise ValueError("absent or replaced transport resources require post-operation proof")
        if self.outcome in unresolved and not self.residual:
            raise ValueError("unresolved transport cleanup must identify a residual resource")
        if self.residual and self.outcome not in unresolved:
            raise ValueError("transport cleanup residual has a non-residual outcome")
        return self


class TransportProbeEvidence(BaseModel):
    """Bounded structured evidence emitted by one transport probe cleanup."""

    model_config = ConfigDict(extra="forbid", strict=True)

    schema_version: Literal["clio-relay.transport-probe-evidence.v1"] = (
        "clio-relay.transport-probe-evidence.v1"
    )
    probe_id: str = Field(min_length=1, max_length=1024)
    cluster: str = Field(min_length=1, max_length=256)
    cleanup_mode: str = Field(min_length=1, max_length=128)
    resources: list[TransportCleanupResourceEvidence] = Field(
        min_length=1,
        max_length=MAX_TRANSPORT_PROBE_RESOURCES,
    )

    @model_validator(mode="after")
    def require_unique_resource_actions(self) -> TransportProbeEvidence:
        """Reject ambiguous duplicate outcomes for the same cleanup action."""
        identities = [
            (resource.kind, resource.resource_id, resource.action) for resource in self.resources
        ]
        if len(identities) != len(set(identities)):
            raise ValueError("transport probe cleanup resource actions must be unique")
        return self


def transport_probe_evidence_line(evidence: TransportProbeEvidence) -> str:
    """Serialize bounded transport evidence for the acceptance line stream."""
    validated = TransportProbeEvidence.model_validate(evidence.model_dump(mode="python"))
    try:
        payload = json.dumps(
            validated.model_dump(mode="json"),
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
    except (TypeError, ValueError) as exc:
        raise ConfigurationError("transport probe evidence must contain finite JSON") from exc
    if len(payload.encode("utf-8")) > MAX_TRANSPORT_PROBE_EVIDENCE_BYTES:
        raise ConfigurationError("transport probe evidence exceeds the bounded payload size")
    return f"{TRANSPORT_PROBE_EVIDENCE_KEY}={payload}"


def parse_transport_probe_evidence(payload: str) -> TransportProbeEvidence:
    """Parse one bounded, finite, strict transport evidence payload."""
    if len(payload.encode("utf-8")) > MAX_TRANSPORT_PROBE_EVIDENCE_BYTES:
        raise ConfigurationError("transport probe evidence exceeds the bounded payload size")
    try:
        loaded = cast(
            object,
            json.loads(payload, parse_constant=_reject_transport_json_constant),
        )
        _assert_bounded_transport_json(loaded)
        return TransportProbeEvidence.model_validate(loaded)
    except (json.JSONDecodeError, RecursionError, ValidationError, ValueError) as exc:
        raise ConfigurationError(f"transport probe evidence is invalid: {exc}") from exc


def _reject_transport_json_constant(value: str) -> None:
    raise ValueError(f"non-finite JSON constant is forbidden: {value}")


def _assert_bounded_transport_json(value: object) -> None:
    nodes = 0

    def visit(item: object, *, depth: int) -> None:
        nonlocal nodes
        nodes += 1
        if nodes > MAX_TRANSPORT_PROBE_JSON_NODES:
            raise ValueError("transport probe evidence contains too many JSON values")
        if depth > MAX_TRANSPORT_PROBE_JSON_DEPTH:
            raise ValueError("transport probe evidence nesting is too deep")
        if isinstance(item, float) and not math.isfinite(item):
            raise ValueError("transport probe evidence contains a non-finite number")
        if isinstance(item, dict):
            for key, child in cast(dict[object, object], item).items():
                if not isinstance(key, str):
                    raise ValueError("transport probe evidence object keys must be strings")
                visit(child, depth=depth + 1)
        elif isinstance(item, list):
            for child in cast(list[object], item):
                visit(child, depth=depth + 1)

    visit(value, depth=0)


class CleanupEvidence(BaseModel):
    """Requested teardown policy and the resources remaining afterward."""

    model_config = ConfigDict(extra="forbid")

    requested: bool = False
    mode: str = "not_requested"
    operation_id: DurableRecordId | None = None
    cancel_relay_jobs: bool = False
    cancel_scheduler_jobs: bool = False
    stop_worker: bool = False
    actions: list[dict[str, Any]] = Field(default_factory=lambda: list[dict[str, Any]]())
    remaining_resources: list[ValidationResource] = Field(
        default_factory=lambda: list[ValidationResource]()
    )


class LiveValidationReport(BaseModel):
    """Stable JSON record for one local or live acceptance run."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["1.0"] = REPORT_SCHEMA_VERSION
    report_id: DurableRecordId = Field(default_factory=lambda: f"validation_{uuid4().hex}")
    scenario: str
    cluster: str
    transport_modes: list[str] = Field(default_factory=list)
    started_at: datetime = Field(default_factory=_utc_now)
    completed_at: datetime | None = None
    status: ValidationStatus = ValidationStatus.FAILED
    evidence_trust: EvidenceTrust = Field(default_factory=EvidenceTrust)
    software: SoftwareIdentity
    install_source: InstallSource
    invocation: list[str] = Field(default_factory=list)
    checks: list[ValidationCheck] = Field(default_factory=lambda: list[ValidationCheck]())
    resources: list[ValidationResource] = Field(default_factory=lambda: list[ValidationResource]())
    artifacts: list[EvidenceReference] = Field(default_factory=lambda: list[EvidenceReference]())
    cleanup: CleanupEvidence = Field(default_factory=CleanupEvidence)
    error: str | None = None
    _source_path: Path | None = PrivateAttr(default=None)

    @property
    def source_path(self) -> Path | None:
        """Return the validated source path when the report was loaded from disk."""
        return self._source_path

    @model_validator(mode="after")
    def validate_passed_report(self) -> LiveValidationReport:
        """Require internally consistent, evidenced terminal success reports."""
        if self.status is not ValidationStatus.PASSED:
            return self
        if self.completed_at is None:
            raise ValueError("passed validation reports require completed_at")
        if self.error is not None:
            raise ValueError("passed validation reports cannot contain an error")
        if not self.checks:
            raise ValueError("passed validation reports require at least one check")
        if any(check.status is not ValidationStatus.PASSED for check in self.checks):
            raise ValueError("passed validation reports cannot contain failed checks")
        if any(not check.evidence for check in self.checks):
            raise ValueError("passed validation checks require evidence")
        if self.cleanup.remaining_resources:
            raise ValueError("passed validation reports cannot contain remaining resources")
        return self


class ReleaseResourceRequirement(BaseModel):
    """Stateful resource evidence required by one release-gate condition."""

    model_config = ConfigDict(extra="forbid")

    kind: str
    minimum_count: int = Field(default=1, ge=1)
    roles: list[str] | None = None
    states: list[str] | None = None
    providers: list[str] | None = None
    metadata_equals: dict[str, Any] = Field(default_factory=lambda: dict[str, Any]())


class ReleaseSpackFreshInstallRequirement(BaseModel):
    """Fixed semantics independently rebound from one fresh-install report."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["clio-relay.release-spack-fresh-install.v1"] = (
        "clio-relay.release-spack-fresh-install.v1"
    )
    server_name: str = Field(min_length=1, max_length=255, pattern=r"^[A-Za-z0-9._-]+$")
    profile: str = Field(min_length=1, max_length=64, pattern=r"^[A-Za-z0-9._-]+$")
    package_name: str = Field(min_length=1, max_length=255, pattern=r"^[A-Za-z0-9._+-]+$")
    requested_spec: str = Field(min_length=1, max_length=4_096)
    reuse: Literal[False] = False

    @model_validator(mode="after")
    def validate_requested_spec(self) -> ReleaseSpackFreshInstallRequirement:
        """Reject ambiguous control characters or whitespace in the exact Spack spec."""
        if self.requested_spec != self.requested_spec.strip() or any(
            ord(character) < 32 or ord(character) == 127 for character in self.requested_spec
        ):
            raise ValueError("fresh-install requested_spec must be one exact printable value")
        return self


class ReleaseGateRequirement(BaseModel):
    """One evidence-backed condition in a release policy."""

    model_config = ConfigDict(extra="forbid")

    requirement_id: str
    description: str
    cluster: str
    scenarios: list[str] = Field(min_length=1)
    required_checks: list[str] = Field(min_length=1)
    required_resource_kinds: list[str] = Field(default_factory=list)
    required_resources: list[ReleaseResourceRequirement] = Field(
        default_factory=lambda: list[ReleaseResourceRequirement]()
    )
    evidence_group_resource_kind: str | None = None
    spack_fresh_install_transition: ReleaseSpackFreshInstallRequirement | None = None
    require_released_artifact: bool | None = None
    require_artifact_sha256: bool | None = None
    allowed_install_sources: list[InstallSourceKind] | None = None
    allowed_launchers: list[str] | None = None

    @model_validator(mode="after")
    def validate_specialized_evidence(self) -> ReleaseGateRequirement:
        """Require a coherent report and complete checks for typed Spack transitions."""
        if self.spack_fresh_install_transition is None:
            return self
        missing_checks = sorted(
            set(SPACK_FRESH_INSTALL_TRANSITION_CHECK_IDS) - set(self.required_checks)
        )
        if missing_checks:
            raise ValueError(f"fresh-install transition omits required checks: {missing_checks}")
        required_kinds = {"relay_job", "artifact", "configuration_manifest", "mcp_server"}
        missing_kinds = sorted(required_kinds - set(self.required_resource_kinds))
        if missing_kinds:
            raise ValueError(
                f"fresh-install transition omits required resource kinds: {missing_kinds}"
            )
        if self.evidence_group_resource_kind is not None:
            raise ValueError("fresh-install transition must be satisfied by one coherent report")
        return self


class ReleaseTargetIdentity(BaseModel):
    """Operator policy pin for one physical validation target."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["clio-relay.release-target-identity.v1"] = (
        "clio-relay.release-target-identity.v1"
    )
    hostnames: list[str] = Field(min_length=1)
    ssh_host_key_sha256: list[str] = Field(min_length=1)
    scheduler_provider: str = Field(min_length=1)
    scheduler_cluster_name: str | None = None
    site_marker_sha256: str = Field(min_length=1)
    identity_sha256: str = Field(min_length=1)

    @model_validator(mode="after")
    def validate_pin_fields(self) -> ReleaseTargetIdentity:
        """Reject ambiguous policy pins while allowing explicit pending sentinels."""
        normalized_hostnames = [_normalized_hostname(item) for item in self.hostnames]
        if any(not item for item in normalized_hostnames) or len(set(normalized_hostnames)) != len(
            normalized_hostnames
        ):
            raise ValueError("target hostnames must be non-empty and unique")
        fingerprints = [item.strip() for item in self.ssh_host_key_sha256]
        if any(not item for item in fingerprints) or len(set(fingerprints)) != len(fingerprints):
            raise ValueError("target SSH host-key fingerprints must be non-empty and unique")
        if not self.scheduler_provider.strip():
            raise ValueError("target scheduler_provider must be non-empty")
        if self.scheduler_cluster_name is not None and not self.scheduler_cluster_name.strip():
            raise ValueError("target scheduler_cluster_name must be non-empty or null")
        return self


class ReleaseGatePolicy(BaseModel):
    """Machine-readable release evidence policy."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["1.0"] = REPORT_SCHEMA_VERSION
    release_version: str
    acceptance_matrix_path: str | None = None
    acceptance_matrix_sha256: str | None = None
    artifact_stage: Literal["published", "immutable_candidate"] = "published"
    evidence_trust_model: Literal["maintainer_sealed_operator_evidence"] = (
        "maintainer_sealed_operator_evidence"
    )
    require_released_artifact: bool = True
    require_artifact_sha256: bool = True
    require_clean_build: bool = True
    require_commit: bool = True
    require_exact_tag: bool = True
    require_target_identity: bool = False
    allowed_install_sources: list[InstallSourceKind] = Field(
        default_factory=lambda: [InstallSourceKind.PYPI]
    )
    allowed_launchers: list[str] = Field(default_factory=lambda: ["uv-tool"])
    required_uv_version: str | None = None
    targets: dict[str, ReleaseTargetIdentity] = Field(
        default_factory=lambda: dict[str, ReleaseTargetIdentity]()
    )
    release_blockers: list[str] = Field(default_factory=list)
    requirements: list[ReleaseGateRequirement] = Field(min_length=1)
    _acceptance_matrix: dict[str, object] | None = PrivateAttr(default=None)

    @property
    def acceptance_matrix(self) -> dict[str, object] | None:
        """Return the digest-verified acceptance matrix bound while loading the policy."""
        return self._acceptance_matrix

    @model_validator(mode="after")
    def validate_artifact_stage(self) -> ReleaseGatePolicy:
        """Keep published and pre-publication policy semantics explicit."""
        if self.artifact_stage == "immutable_candidate":
            if self.require_released_artifact:
                raise ValueError(
                    "immutable candidate policies cannot require prior artifact publication"
                )
            if InstallSourceKind.WHEEL not in self.allowed_install_sources:
                raise ValueError("immutable candidate policies must allow wheel install evidence")
        if any(not blocker.strip() for blocker in self.release_blockers):
            raise ValueError("release blockers must be non-empty descriptions")
        if (self.acceptance_matrix_path is None) != (self.acceptance_matrix_sha256 is None):
            raise ValueError("acceptance matrix path and SHA-256 must be configured together")
        if self.acceptance_matrix_path is not None:
            matrix_path = PurePosixPath(self.acceptance_matrix_path)
            if (
                matrix_path.is_absolute()
                or ".." in matrix_path.parts
                or str(matrix_path) != self.acceptance_matrix_path
            ):
                raise ValueError("acceptance_matrix_path must be a canonical repository path")
            if re.fullmatch(r"[A-Za-z0-9._/-]+", self.acceptance_matrix_path) is None:
                raise ValueError("acceptance_matrix_path contains unsafe characters")
            if re.fullmatch(r"[0-9a-f]{64}", cast(str, self.acceptance_matrix_sha256)) is None:
                raise ValueError("acceptance_matrix_sha256 must be a lowercase SHA-256")
        if (
            self.required_uv_version is not None
            and re.fullmatch(
                r"[0-9]+\.[0-9]+\.[0-9]+(?:[A-Za-z0-9.+-]*)?",
                self.required_uv_version,
            )
            is None
        ):
            raise ValueError("required_uv_version must be an exact uv version")
        if any(not label.strip() or label != label.strip() for label in self.targets):
            raise ValueError("release target labels must be non-empty and whitespace-free")
        if self.require_target_identity and not self.targets:
            raise ValueError("target identity enforcement requires at least one policy target")
        return self


class ReleaseGateResult(BaseModel):
    """Result of evaluating validation reports against a release policy."""

    model_config = ConfigDict(extra="forbid")

    release_version: str
    artifact_sha256: str | None = None
    acceptance_matrix_schema_version: str | None = None
    acceptance_matrix_release_version: str | None = None
    acceptance_matrix_sha256: str | None = None
    acceptance_matrix_stage: str | None = None
    acceptance_report_ids: list[str] = Field(default_factory=list)
    acceptance_report_document_ids: list[str] = Field(default_factory=list)
    policy_target_identity_sha256: dict[str, str] = Field(default_factory=lambda: dict[str, str]())
    target_identity_sha256: dict[str, str] = Field(default_factory=lambda: dict[str, str]())
    passed: bool
    satisfied_requirements: list[str] = Field(default_factory=list)
    unsatisfied_requirements: dict[str, list[str]] = Field(default_factory=dict)
    report_ids: list[str] = Field(default_factory=list)


class ValidationRecorder:
    """Accumulate checks and resources, then atomically persist a report."""

    def __init__(self, report: LiveValidationReport) -> None:
        self.report = report
        self._active_check: str | None = None
        self._job_ids_by_scope: dict[str, str] = {}
        self._scheduler_providers_by_scope: dict[str, str] = {}
        self._transport_probe_ids: set[str] = set()

    @property
    def transport_probe_count(self) -> int:
        """Return the number of distinct structured transport probes observed."""
        return len(self._transport_probe_ids)

    @contextmanager
    def check(self, check_id: str, summary: str) -> Generator[list[EvidenceReference]]:
        """Record a passed or failed check around a block of live work."""
        if self._active_check is not None:
            raise RuntimeError(f"validation check already active: {self._active_check}")
        self._active_check = check_id
        started_at = _utc_now()
        evidence: list[EvidenceReference] = []
        try:
            yield evidence
        except Exception as exc:
            self.report.checks.append(
                ValidationCheck(
                    check_id=check_id,
                    summary=summary,
                    status=ValidationStatus.FAILED,
                    started_at=started_at,
                    completed_at=_utc_now(),
                    evidence=evidence,
                    error=logical_filesystem_text(f"{type(exc).__name__}: {exc}"),
                )
            )
            raise
        else:
            self.report.checks.append(
                ValidationCheck(
                    check_id=check_id,
                    summary=summary,
                    status=ValidationStatus.PASSED,
                    started_at=started_at,
                    completed_at=_utc_now(),
                    evidence=evidence,
                )
            )
        finally:
            self._active_check = None

    def add_resource(self, resource: ValidationResource) -> None:
        """Add or merge a resource without duplicating its stable identity."""
        for existing in self.report.resources:
            if existing.kind == resource.kind and existing.resource_id == resource.resource_id:
                merged = existing.model_copy(
                    update={
                        "role": resource.role or existing.role,
                        "cluster": resource.cluster or existing.cluster,
                        "state": resource.state or existing.state,
                        "provider": resource.provider or existing.provider,
                        "references": list(
                            dict.fromkeys([*existing.references, *resource.references])
                        ),
                        "metadata": {**existing.metadata, **resource.metadata},
                    }
                )
                self.report.resources[self.report.resources.index(existing)] = merged
                return
        self.report.resources.append(resource)

    def observe_line(self, line: str) -> None:
        """Convert a verified human-facing acceptance fact into structured evidence."""
        key, separator, value = line.partition("=")
        if not separator:
            if line == "live acceptance passed":
                self._record_passed_fact("live-test.completed", line, line)
            return
        if key == TRANSPORT_PROBE_EVIDENCE_KEY:
            self._observe_transport_probe_evidence(parse_transport_probe_evidence(value))
            return
        if _line_proves_success(key, value):
            self._record_passed_fact(key, key.replace(".", " "), line)
        scope = _acceptance_scope(key)
        if key.endswith("job_id") and value.startswith("job_"):
            role = key.removeprefix("acceptance.").removesuffix("_job_id").strip(".")
            role = role or "primary"
            self._job_ids_by_scope[scope] = value
            self.add_resource(
                ValidationResource(
                    kind="relay_job",
                    resource_id=value,
                    role=role,
                    cluster=self.report.cluster,
                )
            )
        if key.endswith("job_state") or key.endswith("state"):
            job_id = self._job_ids_by_scope.get(scope)
            if job_id is not None:
                self.add_resource(
                    ValidationResource(
                        kind="relay_job",
                        resource_id=job_id,
                        cluster=self.report.cluster,
                        state=value,
                    )
                )
        if key == "transport.protocol" and value not in self.report.transport_modes:
            self.report.transport_modes.append(value)
        if key == "transport.protocol":
            if value == "ssh_forward":
                self._record_passed_fact("transport.ssh", "SSH-forward transport", line)
            else:
                self._record_passed_fact("transport.relay", "relay transport", line)
        if key == "direct_transport.result" and value == "xtcp":
            self._record_passed_fact("transport.direct", "direct XTCP transport", line)
        if key.endswith(".runtime_scheduler_provider"):
            self._scheduler_providers_by_scope[scope] = value
        if key.endswith(".runtime_scheduler_job_id"):
            self.add_resource(
                ValidationResource(
                    kind="scheduler_job",
                    resource_id=value,
                    role=scope,
                    cluster=self.report.cluster,
                    provider=self._scheduler_providers_by_scope.get(scope),
                    metadata={"metadata_source": "structured_runtime"},
                )
            )
        if key.endswith(".runtime_metadata_artifact"):
            self.add_resource(
                ValidationResource(
                    kind="artifact",
                    resource_id=value,
                    role="runtime_metadata",
                    cluster=self.report.cluster,
                )
            )
        if key.endswith(".structured_runtime_scheduler_identity") and value == "ok":
            self._record_passed_fact(
                "scheduler.structured-metadata",
                "structured runtime scheduler identity",
                line,
            )
        if key == "worker.running" and value == "passed":
            self.add_resource(
                ValidationResource(
                    kind="relay_worker",
                    resource_id=f"worker:{self.report.cluster}",
                    role="cluster_worker",
                    cluster=self.report.cluster,
                    state="running",
                )
            )
        if key == "worker.artifact-version":
            self.add_resource(
                ValidationResource(
                    kind="relay_worker",
                    resource_id=f"worker:{self.report.cluster}",
                    role="cluster_worker",
                    cluster=self.report.cluster,
                    metadata={"clio_relay_version": value},
                )
            )
        if key == "worker.artifact-sha256":
            self.add_resource(
                ValidationResource(
                    kind="relay_worker",
                    resource_id=f"worker:{self.report.cluster}",
                    role="cluster_worker",
                    cluster=self.report.cluster,
                    metadata={"artifact_sha256": value},
                )
            )
        if key == "worker.source-identity":
            self.add_resource(
                ValidationResource(
                    kind="relay_worker",
                    resource_id=f"worker:{self.report.cluster}",
                    role="cluster_worker",
                    cluster=self.report.cluster,
                    metadata={"source_identity": value},
                )
            )
        if key == "worker.components":
            try:
                raw_components = cast(object, json.loads(value))
            except json.JSONDecodeError as exc:
                raise ConfigurationError("worker.components must be valid JSON") from exc
            if not isinstance(raw_components, dict) or not all(
                isinstance(name, str) and isinstance(component, str)
                for name, component in cast(dict[object, object], raw_components).items()
            ):
                raise ConfigurationError("worker.components must be a string object")
            typed_components = cast(dict[str, str], raw_components)
            self.add_resource(
                ValidationResource(
                    kind="relay_worker",
                    resource_id=f"worker:{self.report.cluster}",
                    role="cluster_worker",
                    cluster=self.report.cluster,
                    metadata={"components": dict(typed_components)},
                )
            )
        if key in {"worker.component-artifacts", "worker.component-runtime"}:
            try:
                component_value = cast(object, json.loads(value))
            except json.JSONDecodeError as exc:
                raise ConfigurationError(f"{key} must be valid JSON") from exc
            if not isinstance(component_value, dict):
                raise ConfigurationError(f"{key} must be an object")
            metadata_key = (
                "component_artifacts"
                if key == "worker.component-artifacts"
                else "component_runtime"
            )
            self.add_resource(
                ValidationResource(
                    kind="relay_worker",
                    resource_id=f"worker:{self.report.cluster}",
                    role="cluster_worker",
                    cluster=self.report.cluster,
                    metadata={
                        metadata_key: {
                            str(name): item
                            for name, item in cast(dict[object, object], component_value).items()
                        }
                    },
                )
            )
        if key.endswith(("stdout_bytes", "stderr_bytes")):
            job_id = self._job_ids_by_scope.get(scope)
            if job_id is not None:
                stream = "stdout" if key.endswith("stdout_bytes") else "stderr"
                self.report.artifacts.append(
                    EvidenceReference(
                        kind="log",
                        reference=(
                            f"relay-log://{self.report.cluster}/{job_id}/{stream}?bytes={value}"
                        ),
                    )
                )
        if key.endswith("artifacts"):
            job_id = self._job_ids_by_scope.get(scope)
            if job_id is not None:
                for kind in value.split(","):
                    self.report.artifacts.append(
                        EvidenceReference(
                            kind=kind,
                            reference=f"relay-artifact://{self.report.cluster}/{job_id}/{kind}",
                        )
                    )

    def _observe_transport_probe_evidence(self, evidence: TransportProbeEvidence) -> None:
        if evidence.cluster != self.report.cluster:
            raise ConfigurationError(
                "transport probe evidence cluster does not match the validation report"
            )
        self._transport_probe_ids.add(evidence.probe_id)
        self.report.cleanup.requested = True
        if self.report.cleanup.mode == "not_requested":
            self.report.cleanup.mode = evidence.cleanup_mode
        for cleanup_resource in evidence.resources:
            metadata = {
                **cleanup_resource.metadata,
                "transport_probe_id": evidence.probe_id,
                "cleanup_mode": evidence.cleanup_mode,
                "action": cleanup_resource.action,
                "ownership_verified": cleanup_resource.ownership_verified,
                "verified_after_operation": cleanup_resource.verified_after_operation,
                "observed_state": cleanup_resource.observed_state,
                "residual": cleanup_resource.residual,
                "detail": cleanup_resource.detail,
            }
            validation_resource = ValidationResource(
                kind=cleanup_resource.kind,
                resource_id=cleanup_resource.resource_id,
                role=cleanup_resource.role,
                cluster=evidence.cluster,
                state=cleanup_resource.outcome,
                provider=cleanup_resource.provider,
                references=(
                    [cleanup_resource.location] if cleanup_resource.location is not None else []
                ),
                metadata=metadata,
            )
            self.add_resource(validation_resource)
            action = cleanup_resource.model_dump(mode="json")
            action.update(
                {
                    "probe_id": evidence.probe_id,
                    "cluster": evidence.cluster,
                    "cleanup_mode": evidence.cleanup_mode,
                }
            )
            action_identity = (
                cleanup_resource.kind,
                cleanup_resource.resource_id,
                cleanup_resource.action,
                evidence.probe_id,
            )
            for index, existing in enumerate(self.report.cleanup.actions):
                existing_identity = (
                    existing.get("kind"),
                    existing.get("resource_id"),
                    existing.get("action"),
                    existing.get("probe_id"),
                )
                if existing_identity == action_identity:
                    self.report.cleanup.actions[index] = action
                    break
            else:
                self.report.cleanup.actions.append(action)

            remaining_identity = (
                cleanup_resource.kind,
                cleanup_resource.resource_id,
                evidence.probe_id,
            )
            matching_remaining = [
                item
                for item in self.report.cleanup.remaining_resources
                if (
                    item.kind,
                    item.resource_id,
                    item.metadata.get("transport_probe_id"),
                )
                == remaining_identity
            ]
            if cleanup_resource.residual:
                if matching_remaining:
                    index = self.report.cleanup.remaining_resources.index(matching_remaining[0])
                    self.report.cleanup.remaining_resources[index] = validation_resource
                else:
                    self.report.cleanup.remaining_resources.append(validation_resource)
            elif matching_remaining:
                self.report.cleanup.remaining_resources.remove(matching_remaining[0])

    def record_failure(self, check_id: str, summary: str, error: BaseException) -> None:
        """Record a terminal failure that occurred outside a check context."""
        now = _utc_now()
        self.report.checks.append(
            ValidationCheck(
                check_id=check_id,
                summary=summary,
                status=ValidationStatus.FAILED,
                started_at=now,
                completed_at=now,
                error=logical_filesystem_text(f"{type(error).__name__}: {error}"),
            )
        )

    def finish(self, error: BaseException | None = None) -> None:
        """Set terminal report state without hiding the original exception."""
        self.report.completed_at = _utc_now()
        self.report.error = (
            None if error is None else logical_filesystem_text(f"{type(error).__name__}: {error}")
        )
        self.report.status = (
            ValidationStatus.PASSED
            if error is None
            and self.report.checks
            and all(check.status is ValidationStatus.PASSED for check in self.report.checks)
            else ValidationStatus.FAILED
        )

    def write(self, json_path: Path, markdown_path: Path | None = None) -> None:
        """Atomically write stable JSON and optional Markdown evidence."""
        write_validation_report(self.report, json_path)
        if markdown_path is not None:
            _atomic_write_text(markdown_path, render_validation_markdown(self.report))

    def _record_passed_fact(self, check_id: str, summary: str, line: str) -> None:
        now = _utc_now()
        evidence = EvidenceReference(kind="acceptance_output", excerpt=line)
        for index, existing in enumerate(self.report.checks):
            if existing.check_id != check_id:
                continue
            merged = existing.model_copy(
                update={"evidence": [*existing.evidence, evidence], "completed_at": now}
            )
            self.report.checks[index] = merged
            return
        self.report.checks.append(
            ValidationCheck(
                check_id=check_id,
                summary=summary,
                status=ValidationStatus.PASSED,
                started_at=now,
                completed_at=now,
                evidence=[evidence],
            )
        )


def new_live_validation_report(
    *,
    scenario: str,
    cluster: str,
    transport_modes: Iterable[str] = (),
    launcher: str | None = None,
    install_source: str | None = None,
    artifact_sha256: str | None = None,
    report_id: DurableRecordId | None = None,
) -> LiveValidationReport:
    """Create a report seeded with package, source, and invocation provenance."""
    return LiveValidationReport(
        report_id=(report_id if report_id is not None else f"validation_{uuid4().hex}"),
        scenario=scenario,
        cluster=cluster,
        transport_modes=list(dict.fromkeys(transport_modes)),
        evidence_trust=_validation_evidence_trust(cluster),
        software=detect_software_identity(),
        install_source=detect_install_source(
            launcher=launcher,
            source_override=install_source,
            artifact_sha256=artifact_sha256,
        ),
        invocation=_redacted_invocation([str(item) for item in sys.orig_argv]),
    )


def _validation_evidence_trust(cluster: str) -> EvidenceTrust:
    """Build producer provenance only from explicit validation-run inputs."""
    login = os.environ.get("CLIO_RELAY_VALIDATION_PRODUCER_GITHUB_LOGIN")
    raw_github_id = os.environ.get("CLIO_RELAY_VALIDATION_PRODUCER_GITHUB_ID")
    invocation_id = os.environ.get("CLIO_RELAY_VALIDATION_INVOCATION_ID")
    github_id: int | None = None
    if raw_github_id is not None:
        if re.fullmatch(r"[1-9][0-9]*", raw_github_id) is None:
            raise ConfigurationError(
                "CLIO_RELAY_VALIDATION_PRODUCER_GITHUB_ID must be a positive integer"
            )
        github_id = int(raw_github_id)
    try:
        return EvidenceTrust(
            origin=(
                EvidenceOrigin.LOCAL_PROCESS
                if cluster == "local"
                else EvidenceOrigin.OPERATOR_GENERATED
            ),
            producer_github_login=login,
            producer_github_id=github_id,
            invocation_id=invocation_id,
        )
    except ValidationError as exc:
        raise ConfigurationError(f"invalid validation producer identity: {exc}") from exc


def detect_software_identity() -> SoftwareIdentity:
    """Read embedded build identity, falling back to a clean source checkout probe."""
    embedded = _embedded_build_info()
    if embedded is not None:
        return SoftwareIdentity(
            version=__version__,
            commit=_optional_string(embedded.get("commit")),
            tag=_optional_string(embedded.get("tag")),
            dirty=_optional_bool(embedded.get("dirty")),
        )
    checkout = _checkout_build_info()
    return SoftwareIdentity(
        version=__version__,
        commit=_optional_string(checkout.get("commit")),
        tag=_optional_string(checkout.get("tag")),
        dirty=_optional_bool(checkout.get("dirty")),
    )


def detect_install_source(
    *,
    launcher: str | None = None,
    source_override: str | None = None,
    artifact_sha256: str | None = None,
    infer_artifact_sha256: bool = False,
) -> InstallSource:
    """Inspect PEP 610 metadata and explicit acceptance provenance overrides.

    Validation callers should supply an independently computed artifact digest.
    ``infer_artifact_sha256`` exists for installation inspection, where the
    exact wheel URL is already bound by uv's persistent-tool receipt.
    """
    distribution = metadata.distribution("clio-relay")
    package_path = str(resources.files("clio_relay"))
    direct_url = _distribution_direct_url(distribution)
    detected_kind, reference = _classify_install_source(direct_url)
    kind = detected_kind
    if source_override is not None:
        kind, reference = _parse_source_override(source_override)
    resolved_launcher = launcher or os.environ.get("CLIO_RELAY_VALIDATION_LAUNCHER", "unknown")
    resolved_hash = artifact_sha256 or os.environ.get("CLIO_RELAY_VALIDATION_ARTIFACT_SHA256")
    if resolved_hash is None and infer_artifact_sha256:
        resolved_hash, artifact_identity_verified = _infer_running_artifact_identity(
            distribution,
            detected_kind=detected_kind,
            direct_url=direct_url,
            launcher=resolved_launcher,
        )
    else:
        artifact_identity_verified = _verify_running_artifact_identity(
            distribution,
            detected_kind=detected_kind,
            direct_url=direct_url,
            artifact_sha256=resolved_hash,
            launcher=resolved_launcher,
        )
    launcher_verified, launcher_receipt = _detect_launcher_receipt(
        resolved_launcher,
        detected_kind=detected_kind,
        package_path=package_path,
        distribution=distribution,
    )
    released = (
        kind is detected_kind
        and (
            kind is InstallSourceKind.PYPI
            or (
                kind is InstallSourceKind.WHEEL
                and _is_official_github_release_wheel(direct_url, distribution.version)
            )
        )
        and resolved_launcher == "uv-tool"
        and artifact_identity_verified
        and launcher_verified
    )
    return InstallSource(
        kind=kind,
        detected_kind=detected_kind,
        reference=reference,
        launcher=resolved_launcher,
        package_path=package_path,
        distribution_version=distribution.version,
        artifact_sha256=resolved_hash,
        direct_url=direct_url,
        artifact_identity_verified=artifact_identity_verified,
        released_artifact=released,
        launcher_verified=launcher_verified,
        launcher_receipt=launcher_receipt,
    )


def _detect_launcher_receipt(
    launcher: str,
    *,
    detected_kind: InstallSourceKind,
    package_path: str,
    distribution: metadata.Distribution,
) -> tuple[bool, dict[str, Any]]:
    """Capture process-observed uv tool-environment evidence, not a caller label alone."""
    if launcher == "uv-tool":
        return _detect_persistent_uv_tool_receipt(
            detected_kind=detected_kind,
            package_path=package_path,
            distribution=distribution,
        )
    uv_executable = os.environ.get("UV")
    prefix = Path(sys.prefix).resolve()
    base_prefix = Path(sys.base_prefix).resolve()
    process_executable = Path(os.path.abspath(sys.executable))
    process_executable_resolved = process_executable.resolve()
    package = Path(package_path).resolve()
    package_in_environment = False
    with suppress(ValueError):
        package.relative_to(prefix)
        package_in_environment = True
    executable_in_environment = False
    with suppress(ValueError):
        process_executable.relative_to(prefix)
        executable_in_environment = True
    executable_target_bound = _within_or_equal(process_executable_resolved, prefix) or (
        _within_or_equal(process_executable_resolved, base_prefix)
    )
    uv_path = Path(uv_executable) if uv_executable is not None else None
    uv_identity_before = _regular_file_identity(uv_path) if uv_path is not None else None
    uv_path_verified, uv_version, uv_executable_sha256 = _uv_executable_identity(uv_executable)
    uv_cache_directory = (
        _uv_cache_dir(uv_path) if uv_path_verified and uv_path is not None else None
    )
    uv_identity_after = _regular_file_identity(uv_path) if uv_path is not None else None
    uv_stable = uv_identity_before is not None and uv_identity_after == uv_identity_before
    cache_contains_environment = False
    if uv_cache_directory is not None:
        cache_contains_environment = _strictly_contains(uv_cache_directory, prefix)
    pyvenv_uv_version = _pyvenv_uv_version(prefix)
    pyvenv_matches_uv = uv_version is not None and pyvenv_uv_version == uv_version
    uv_ancestor_verified = False
    uv_ancestor: dict[str, Any] | None = None
    if uv_path_verified and uv_stable and uv_path is not None:
        uv_ancestor_verified, uv_ancestor = _uv_process_ancestor(uv_path)
    project_environment = (Path.cwd() / ".venv").resolve()
    isolated_environment = prefix != base_prefix and prefix != project_environment
    verified = (
        launcher == "uvx"
        and detected_kind in {InstallSourceKind.WHEEL, InstallSourceKind.PYPI}
        and uv_path_verified
        and uv_stable
        and uv_cache_directory is not None
        and cache_contains_environment
        and pyvenv_matches_uv
        and package_in_environment
        and executable_in_environment
        and executable_target_bound
        and isolated_environment
        and uv_ancestor_verified
    )
    return verified, {
        "schema_version": "clio-relay.launcher-receipt.v2",
        "claimed_launcher": launcher,
        "uv_executable": uv_executable,
        "uv_executable_verified": uv_path_verified,
        "uv_executable_stable": uv_stable,
        "uv_version": uv_version,
        "uv_executable_sha256": uv_executable_sha256,
        "uv_cache_directory": str(uv_cache_directory) if uv_cache_directory is not None else None,
        "uv_cache_contains_environment": cache_contains_environment,
        "uv_process_ancestor_verified": uv_ancestor_verified,
        "uv_process_ancestor": uv_ancestor,
        "invocation_id": os.environ.get("CLIO_RELAY_VALIDATION_INVOCATION_ID"),
        "process_prefix": str(prefix),
        "base_prefix": str(base_prefix),
        "process_executable": str(process_executable),
        "process_executable_resolved": str(process_executable_resolved),
        "package_in_process_environment": package_in_environment,
        "executable_in_process_environment": executable_in_environment,
        "executable_target_bound": executable_target_bound,
        "pyvenv_uv_version": pyvenv_uv_version,
        "pyvenv_matches_uv": pyvenv_matches_uv,
        "isolated_environment": isolated_environment,
        "detected_install_source": detected_kind.value,
        "verified": verified,
    }


def _detect_persistent_uv_tool_receipt(
    *,
    detected_kind: InstallSourceKind,
    package_path: str,
    distribution: metadata.Distribution,
) -> tuple[bool, dict[str, Any]]:
    """Capture structural evidence for an install-once uv tool invocation."""
    uv_executable = os.environ.get("UV") or shutil.which("uv")
    uv_path = Path(uv_executable) if uv_executable is not None else None
    uv_identity_before = _regular_file_identity(uv_path) if uv_path is not None else None
    uv_path_verified, uv_version, uv_executable_sha256 = _uv_executable_identity(uv_executable)
    uv_identity_after = _regular_file_identity(uv_path) if uv_path is not None else None
    uv_stable = uv_identity_before is not None and uv_identity_after == uv_identity_before
    tool_directory = _uv_tool_dir(uv_path, bin_directory=False) if uv_path_verified else None
    tool_bin_directory = _uv_tool_dir(uv_path, bin_directory=True) if uv_path_verified else None
    prefix = Path(sys.prefix).resolve()
    base_prefix = Path(sys.base_prefix).resolve()
    process_executable = Path(os.path.abspath(sys.executable))
    process_executable_resolved = process_executable.resolve()
    package = Path(package_path).resolve()
    package_in_environment = _within_or_equal(package, prefix)
    executable_in_environment = _within_or_equal(process_executable_resolved, prefix)
    environment_in_tool_directory = tool_directory is not None and _strictly_contains(
        tool_directory, prefix
    )
    pyvenv_uv_version = _pyvenv_uv_version(prefix)
    pyvenv_matches_uv = uv_version is not None and pyvenv_uv_version == uv_version
    configured_tool = os.environ.get("CLIO_RELAY_VALIDATION_TOOL_EXECUTABLE")
    tool_name = "clio-relay.exe" if os.name == "nt" else "clio-relay"
    # Ambient PATH and the Windows current directory can name a different tool environment.
    selected_tool = configured_tool or (
        shutil.which(str(tool_bin_directory / tool_name))
        if tool_bin_directory is not None
        else None
    )
    tool_path = Path(selected_tool).expanduser() if selected_tool is not None else None
    try:
        tool_path_absolute = tool_path.absolute() if tool_path is not None else None
        tool_target = tool_path.resolve(strict=True) if tool_path is not None else None
    except OSError:
        tool_path_absolute = None
        tool_target = None
    tool_bin_bound = (
        tool_path_absolute is not None
        and tool_bin_directory is not None
        and tool_path_absolute.parent.resolve() == tool_bin_directory
    )
    tool_target_identity = _regular_file_identity(tool_target) if tool_target is not None else None
    tool_executable_sha256 = (
        _hash_open_regular_file(tool_target, tool_target_identity)
        if tool_target is not None
        else None
    )
    record_identity = _installed_record_identity(distribution)
    owned_console_digests = record_identity.pop("console_script_sha256", [])
    tool_target_bound = tool_target is not None and (
        _within_or_equal(tool_target, prefix)
        or (
            isinstance(tool_executable_sha256, str)
            and tool_executable_sha256 in owned_console_digests
        )
    )
    project_environment = (Path.cwd() / ".venv").resolve()
    isolated_environment = prefix != base_prefix and prefix != project_environment
    uv_receipt_identity = _persistent_uv_tool_receipt_identity(
        environment_prefix=prefix,
        tool_executable=tool_path_absolute,
        distribution=distribution,
    )
    verified = (
        detected_kind in {InstallSourceKind.WHEEL, InstallSourceKind.PYPI}
        and uv_path_verified
        and uv_stable
        and tool_directory is not None
        and tool_bin_directory is not None
        and environment_in_tool_directory
        and pyvenv_matches_uv
        and package_in_environment
        and executable_in_environment
        and tool_bin_bound
        and tool_target_bound
        and record_identity.get("verified") is True
        and uv_receipt_identity.get("verified") is True
        and isolated_environment
    )
    return verified, {
        "schema_version": "clio-relay.launcher-receipt.v3",
        "claimed_launcher": "uv-tool",
        "uv_executable": uv_executable,
        "uv_executable_verified": uv_path_verified,
        "uv_executable_stable": uv_stable,
        "uv_version": uv_version,
        "uv_executable_sha256": uv_executable_sha256,
        "uv_tool_directory": str(tool_directory) if tool_directory is not None else None,
        "uv_tool_bin_directory": (
            str(tool_bin_directory) if tool_bin_directory is not None else None
        ),
        "tool_environment_verified": environment_in_tool_directory,
        "tool_executable": str(tool_path_absolute) if tool_path_absolute is not None else None,
        "tool_executable_resolved": str(tool_target) if tool_target is not None else None,
        "tool_executable_sha256": tool_executable_sha256,
        "tool_bin_bound": tool_bin_bound,
        "tool_target_bound": tool_target_bound,
        "invocation_id": os.environ.get("CLIO_RELAY_VALIDATION_INVOCATION_ID"),
        "process_prefix": str(prefix),
        "base_prefix": str(base_prefix),
        "process_executable": str(process_executable),
        "process_executable_resolved": str(process_executable_resolved),
        "package_in_process_environment": package_in_environment,
        "executable_in_process_environment": executable_in_environment,
        "pyvenv_uv_version": pyvenv_uv_version,
        "pyvenv_matches_uv": pyvenv_matches_uv,
        "isolated_environment": isolated_environment,
        "distribution_record": record_identity,
        "uv_tool_receipt": uv_receipt_identity,
        "detected_install_source": detected_kind.value,
        "verified": verified,
    }


def _persistent_uv_tool_receipt_identity(
    *,
    environment_prefix: Path,
    tool_executable: Path | None,
    distribution: metadata.Distribution,
) -> dict[str, Any]:
    """Bind uv's launcher and requirement records to the running distribution."""
    receipt_path = environment_prefix / "uv-receipt.toml"
    identity = _regular_file_identity(receipt_path)
    if identity is None or not 1 <= identity[2] <= MAX_UV_TOOL_RECEIPT_BYTES:
        return {"verified": False, "error": "uv tool receipt is missing or invalid"}
    payload = _read_open_regular_file(
        receipt_path,
        identity,
        maximum_bytes=MAX_UV_TOOL_RECEIPT_BYTES,
    )
    if payload is None:
        return {"verified": False, "error": "uv tool receipt changed while reading"}
    try:
        document = tomllib.loads(payload.decode("utf-8"))
    except (UnicodeError, tomllib.TOMLDecodeError):
        return {"verified": False, "error": "uv tool receipt is not valid TOML"}
    tool = document.get("tool")
    if not isinstance(tool, dict):
        return {"verified": False, "error": "uv tool receipt omitted its tool record"}
    tool_record = cast(dict[str, object], tool)
    entrypoints = tool_record.get("entrypoints")
    requirements = tool_record.get("requirements")
    if not isinstance(entrypoints, list) or not isinstance(requirements, list):
        return {"verified": False, "error": "uv tool receipt omitted its mappings"}

    launcher_matches: list[dict[str, object]] = []
    for raw_entrypoint in cast(list[object], entrypoints):
        if not isinstance(raw_entrypoint, dict):
            return {"verified": False, "error": "uv tool receipt entry point is invalid"}
        entrypoint = cast(dict[str, object], raw_entrypoint)
        source = entrypoint.get("from")
        if isinstance(source, str) and _normalized_distribution_name(source) == "clio-relay":
            launcher_matches.append(entrypoint)
    launcher_bound = False
    if len(launcher_matches) == 1 and tool_executable is not None:
        install_path = launcher_matches[0].get("install-path")
        install_location = (
            Path(install_path).expanduser() if isinstance(install_path, str) else None
        )
        launcher_bound = (
            install_location is not None
            and install_location.is_absolute()
            and _lexical_path_key(install_location) == _lexical_path_key(tool_executable)
        )

    requirement_matches: list[dict[str, object]] = []
    for raw_requirement in cast(list[object], requirements):
        if not isinstance(raw_requirement, dict):
            return {"verified": False, "error": "uv tool receipt requirement is invalid"}
        requirement = cast(dict[str, object], raw_requirement)
        name = requirement.get("name")
        if isinstance(name, str) and _normalized_distribution_name(name) == "clio-relay":
            requirement_matches.append(requirement)
    direct_url = _distribution_direct_url(distribution)
    source_bound = len(requirement_matches) == 1 and _uv_requirement_matches_distribution_source(
        requirement_matches[0] if requirement_matches else {},
        direct_url=direct_url,
        distribution_version=distribution.version,
    )
    requirement = requirement_matches[0] if len(requirement_matches) == 1 else {}
    source_url = requirement.get("url")
    source_path = requirement.get("path")
    source_specifier = requirement.get("specifier")
    verified = launcher_bound and source_bound
    return {
        "schema_version": "clio-relay.uv-tool-receipt.v1",
        "path": str(receipt_path.resolve()),
        "sha256": hashlib.sha256(payload).hexdigest(),
        "launcher_bound": launcher_bound,
        "requirement_name": requirement.get("name"),
        "requirement_url": _redact_url(source_url) if isinstance(source_url, str) else None,
        "requirement_path": source_path if isinstance(source_path, str) else None,
        "requirement_specifier": source_specifier if isinstance(source_specifier, str) else None,
        "distribution_url": direct_url.get("url") if direct_url is not None else None,
        "source_bound": source_bound,
        "verified": verified,
    }


def _uv_requirement_matches_distribution_source(
    requirement: dict[str, object],
    *,
    direct_url: dict[str, Any] | None,
    distribution_version: str,
) -> bool:
    """Match one uv requirement to the exact PEP 610 installation source."""
    source_url = requirement.get("url")
    source_path = requirement.get("path")
    source_specifier = requirement.get("specifier")
    if direct_url is None:
        return (
            source_url is None
            and source_path is None
            and source_specifier in {None, f"=={distribution_version}"}
        )
    distribution_url = direct_url.get("url")
    if not isinstance(distribution_url, str):
        return False
    parsed = urllib.parse.urlsplit(distribution_url)
    if parsed.scheme.casefold() == "file":
        if not isinstance(source_path, str):
            return False
        direct_path = _local_wheel_archive_path(direct_url)
        if direct_path is None:
            return False
        try:
            return Path(source_path).expanduser().resolve(strict=True) == direct_path.resolve(
                strict=True
            )
        except (OSError, RuntimeError, ValueError):
            return False
    return (
        parsed.scheme.casefold() == "https"
        and not parsed.username
        and not parsed.password
        and not parsed.query
        and not parsed.fragment
        and isinstance(source_url, str)
        and source_url == distribution_url
        and _redact_url(source_url) == source_url
    )


def _normalized_distribution_name(value: str) -> str:
    """Return the canonical comparison key for one Python distribution name."""
    return re.sub(r"[-_.]+", "-", value).casefold()


def _lexical_path_key(path: Path) -> str:
    """Return a platform-normalized lexical path key."""
    return os.path.normcase(os.path.normpath(str(path)))


def _uv_tool_dir(executable: Path | None, *, bin_directory: bool) -> Path | None:
    """Return one directory reported by the exact stable uv executable."""
    identity = _regular_file_identity(executable) if executable is not None else None
    if executable is None or identity is None:
        return None
    command = [str(executable), "tool", "dir"]
    if bin_directory:
        command.append("--bin")
    command.append("--no-config")
    try:
        completed = subprocess.run(
            command,
            capture_output=True,
            check=False,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    output = completed.stdout.strip()
    if (
        completed.returncode != 0
        or _regular_file_identity(executable) != identity
        or not output
        or "\x00" in output
        or "\n" in output
        or "\r" in output
    ):
        return None
    candidate = Path(output)
    if not candidate.is_absolute():
        return None
    try:
        resolved = candidate.resolve(strict=True)
    except OSError:
        return None
    return resolved if resolved.is_dir() else None


def _installed_record_identity(distribution: metadata.Distribution) -> dict[str, Any]:
    """Verify and summarize the complete installed distribution RECORD closure."""
    files = distribution.files
    if files is None or not files or len(files) > 100_000:
        return {"verified": False, "console_script_sha256": []}
    closure = hashlib.sha256()
    runtime_bytes = 0
    record_paths: list[Path] = []
    console_digests: list[str] = []
    try:
        for item in sorted(files, key=lambda value: str(value)):
            relative = str(item).replace("\\", "/")
            located = Path(str(distribution.locate_file(item))).resolve(strict=True)
            identity = _regular_file_identity(located)
            if identity is None:
                return {"verified": False, "console_script_sha256": []}
            digest = _hash_open_regular_file(located, identity)
            if digest is None:
                return {"verified": False, "console_script_sha256": []}
            size = identity[2]
            runtime_bytes += size
            if runtime_bytes > 4 * 1024 * 1024 * 1024:
                return {"verified": False, "console_script_sha256": []}
            expected_hash = item.hash
            if expected_hash is not None:
                if expected_hash.mode != "sha256":
                    return {"verified": False, "console_script_sha256": []}
                encoded = base64.urlsafe_b64encode(bytes.fromhex(digest)).rstrip(b"=").decode()
                if encoded != expected_hash.value:
                    return {"verified": False, "console_script_sha256": []}
            elif not relative.endswith(".dist-info/RECORD"):
                return {"verified": False, "console_script_sha256": []}
            closure.update(relative.encode("utf-8"))
            closure.update(b"\0")
            closure.update(digest.encode("ascii"))
            closure.update(b"\0")
            closure.update(str(size).encode("ascii"))
            closure.update(b"\n")
            if relative.endswith(".dist-info/RECORD"):
                record_paths.append(located)
            if located.name.casefold() in {"clio-relay", "clio-relay.exe"}:
                console_digests.append(digest)
    except (OSError, ValueError):
        return {"verified": False, "console_script_sha256": []}
    if len(record_paths) != 1:
        return {"verified": False, "console_script_sha256": []}
    record_identity = _regular_file_identity(record_paths[0])
    record_sha256 = _hash_open_regular_file(record_paths[0], record_identity)
    verified = record_sha256 is not None and bool(console_digests)
    return {
        "record_path": str(record_paths[0]),
        "record_sha256": record_sha256,
        "runtime_closure_sha256": closure.hexdigest(),
        "runtime_file_count": len(files),
        "runtime_bytes": runtime_bytes,
        "console_script_sha256": sorted(set(console_digests)),
        "verified": verified,
    }


def _uv_cache_dir(executable: Path) -> Path | None:
    """Return the cache directory reported by the exact uv executable."""
    identity = _regular_file_identity(executable)
    if identity is None:
        return None
    try:
        completed = subprocess.run(
            [str(executable), "cache", "dir"],
            capture_output=True,
            check=False,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if completed.returncode != 0 or _regular_file_identity(executable) != identity:
        return None
    output = completed.stdout.strip()
    if not output or "\x00" in output or "\n" in output or "\r" in output:
        return None
    candidate = Path(output)
    if not candidate.is_absolute():
        return None
    try:
        return candidate.resolve(strict=True)
    except OSError:
        return None


def _pyvenv_uv_version(prefix: Path) -> str | None:
    """Read uv's version marker from a bounded, path-anchored ``pyvenv.cfg``."""
    config = prefix / "pyvenv.cfg"
    identity = _regular_file_identity(config)
    if identity is None:
        return None
    content = _read_open_regular_file(config, identity, maximum_bytes=MAX_PYVENV_CONFIG_BYTES)
    if content is None:
        return None
    try:
        text = content.decode("utf-8")
    except UnicodeDecodeError:
        return None
    values: dict[str, str] = {}
    for line in text.splitlines():
        key, separator, value = line.partition("=")
        if not separator:
            continue
        normalized = key.strip().casefold()
        if normalized in values:
            return None
        values[normalized] = value.strip()
    version = values.get("uv")
    if (
        version is None
        or re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+(?:[A-Za-z0-9.+-]*)", version) is None
    ):
        return None
    return version


def _strictly_contains(parent: Path, child: Path) -> bool:
    """Return whether ``child`` is below, but is not equal to, ``parent``."""
    try:
        return child != parent and child.is_relative_to(parent)
    except (OSError, ValueError):
        return False


def _within_or_equal(path: Path, root: Path) -> bool:
    """Return whether a resolved path is equal to or below a resolved root."""
    try:
        return path == root or path.is_relative_to(root)
    except (OSError, ValueError):
        return False


def _uv_process_ancestor(executable: Path) -> tuple[bool, dict[str, Any] | None]:
    """Find the exact uv file identity in a bounded OS process ancestor chain."""
    expected_identity = _regular_file_identity(executable)
    if expected_identity is None:
        return False, None
    if os.name == "nt":
        ancestors = _windows_process_ancestors(os.getpid())
    elif sys.platform.startswith("linux"):
        ancestors = _linux_process_ancestors(os.getpid())
    else:
        return False, None
    for depth, (pid, image) in enumerate(ancestors, start=1):
        if _regular_file_identity(image) != expected_identity:
            continue
        return True, {"pid": pid, "depth": depth, "executable": str(image)}
    return False, None


def _linux_process_ancestors(pid: int) -> list[tuple[int, Path]]:
    """Read a bounded Linux parent chain from procfs."""
    ancestors: list[tuple[int, Path]] = []
    seen = {pid}
    current = pid
    for _ in range(MAX_LAUNCHER_PROCESS_ANCESTORS):
        try:
            stat_text = Path(f"/proc/{current}/stat").read_text(encoding="utf-8")
            closing = stat_text.rfind(")")
            fields = stat_text[closing + 2 :].split() if closing >= 0 else []
            parent = int(fields[1]) if len(fields) > 1 else 0
        except (OSError, UnicodeDecodeError, ValueError):
            break
        if parent <= 0 or parent in seen:
            break
        seen.add(parent)
        try:
            image = Path(f"/proc/{parent}/exe").resolve(strict=True)
        except OSError:
            break
        ancestors.append((parent, image))
        current = parent
    return ancestors


def _windows_process_ancestors(pid: int) -> list[tuple[int, Path]]:
    """Read a bounded Windows parent chain with Toolhelp and process-image handles."""
    if os.name != "nt":
        return []
    from ctypes import wintypes

    class ProcessEntry32W(ctypes.Structure):
        _fields_ = [
            ("dwSize", wintypes.DWORD),
            ("cntUsage", wintypes.DWORD),
            ("th32ProcessID", wintypes.DWORD),
            ("th32DefaultHeapID", ctypes.c_size_t),
            ("th32ModuleID", wintypes.DWORD),
            ("cntThreads", wintypes.DWORD),
            ("th32ParentProcessID", wintypes.DWORD),
            ("pcPriClassBase", wintypes.LONG),
            ("dwFlags", wintypes.DWORD),
            ("szExeFile", wintypes.WCHAR * 260),
        ]

    loader = cast(Any, ctypes.WinDLL)("kernel32", use_last_error=True)
    create_snapshot = loader.CreateToolhelp32Snapshot
    process_first = loader.Process32FirstW
    process_next = loader.Process32NextW
    open_process = loader.OpenProcess
    query_image = loader.QueryFullProcessImageNameW
    close_handle = loader.CloseHandle
    create_snapshot.argtypes = [wintypes.DWORD, wintypes.DWORD]
    create_snapshot.restype = wintypes.HANDLE
    process_first.argtypes = [wintypes.HANDLE, ctypes.POINTER(ProcessEntry32W)]
    process_first.restype = wintypes.BOOL
    process_next.argtypes = [wintypes.HANDLE, ctypes.POINTER(ProcessEntry32W)]
    process_next.restype = wintypes.BOOL
    open_process.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    open_process.restype = wintypes.HANDLE
    query_image.argtypes = [
        wintypes.HANDLE,
        wintypes.DWORD,
        wintypes.LPWSTR,
        ctypes.POINTER(wintypes.DWORD),
    ]
    query_image.restype = wintypes.BOOL
    close_handle.argtypes = [wintypes.HANDLE]
    close_handle.restype = wintypes.BOOL
    snapshot = create_snapshot(0x00000002, 0)
    invalid_handle = ctypes.c_void_p(-1).value
    if snapshot in {None, invalid_handle}:
        return []
    parents: dict[int, int] = {}
    try:
        entry = ProcessEntry32W()
        entry.dwSize = ctypes.sizeof(ProcessEntry32W)
        found = bool(process_first(snapshot, ctypes.byref(entry)))
        while found:
            parents[int(entry.th32ProcessID)] = int(entry.th32ParentProcessID)
            entry.dwSize = ctypes.sizeof(ProcessEntry32W)
            found = bool(process_next(snapshot, ctypes.byref(entry)))
    finally:
        close_handle(snapshot)

    ancestors: list[tuple[int, Path]] = []
    seen = {pid}
    current = pid
    for _ in range(MAX_LAUNCHER_PROCESS_ANCESTORS):
        parent = parents.get(current, 0)
        if parent <= 0 or parent in seen:
            break
        seen.add(parent)
        image = _windows_process_image(parent, open_process, query_image, close_handle)
        if image is None:
            break
        ancestors.append((parent, image))
        current = parent
    return ancestors


def _windows_process_image(
    pid: int,
    open_process: Any,
    query_image: Any,
    close_handle: Any,
) -> Path | None:
    """Resolve one Windows process image using a least-privilege query handle."""
    from ctypes import wintypes

    handle = open_process(0x1000, False, pid)
    if not handle:
        return None
    try:
        buffer = ctypes.create_unicode_buffer(32768)
        length = wintypes.DWORD(len(buffer))
        if not query_image(handle, 0, buffer, ctypes.byref(length)):
            return None
        return Path(buffer.value[: length.value]).resolve(strict=True)
    except OSError:
        return None
    finally:
        close_handle(handle)


def _uv_executable_identity(executable: str | None) -> tuple[bool, str | None, str | None]:
    """Version and hash an exact regular uv executable without accepting indirection."""
    if executable is None:
        return False, None, None
    path = Path(executable)
    if not path.is_absolute() or path.name.casefold() not in {"uv", "uv.exe"}:
        return False, None, None
    before = _regular_file_identity(path)
    if before is None:
        return False, None, None
    before_digest = _hash_open_regular_file(path, before)
    if before_digest is None:
        return False, None, None
    try:
        completed = subprocess.run(
            [str(path), "--version"],
            capture_output=True,
            check=False,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.TimeoutExpired):
        return False, None, None
    match = re.fullmatch(
        r"uv ([0-9]+\.[0-9]+\.[0-9]+(?:[A-Za-z0-9.+-]*))(?:\s+.*)?",
        completed.stdout.strip(),
    )
    if completed.returncode != 0 or match is None:
        return False, None, None
    after = _regular_file_identity(path)
    if after != before:
        return False, None, None
    after_digest = _hash_open_regular_file(path, after)
    if after_digest is None or after_digest != before_digest:
        return False, None, None
    return True, match.group(1), after_digest


def _regular_file_identity(path: Path) -> tuple[int, int, int, int] | None:
    """Return a stable identity only for a non-link, non-reparse regular file."""
    try:
        details = path.lstat()
    except OSError:
        return None
    file_attributes = getattr(details, "st_file_attributes", 0)
    reparse_attribute = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0)
    if stat.S_ISLNK(details.st_mode) or not stat.S_ISREG(details.st_mode):
        return None
    if reparse_attribute and file_attributes & reparse_attribute:
        return None
    return (details.st_dev, details.st_ino, details.st_size, details.st_mtime_ns)


def _hash_open_regular_file(
    path: Path,
    expected_identity: tuple[int, int, int, int] | None,
) -> str | None:
    """Hash a regular file while confirming the opened handle matches its path snapshot."""
    if expected_identity is None:
        return None
    digest = hashlib.sha256()
    try:
        with path.open("rb") as stream:
            opened = os.fstat(stream.fileno())
            opened_identity = (
                opened.st_dev,
                opened.st_ino,
                opened.st_size,
                opened.st_mtime_ns,
            )
            if opened_identity != expected_identity or not stat.S_ISREG(opened.st_mode):
                return None
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError:
        return None
    if _regular_file_identity(path) != expected_identity:
        return None
    return digest.hexdigest()


def _read_open_regular_file(
    path: Path,
    expected_identity: tuple[int, int, int, int],
    *,
    maximum_bytes: int,
) -> bytes | None:
    """Read one path-anchored regular file with a strict byte ceiling."""
    try:
        with path.open("rb") as stream:
            opened = os.fstat(stream.fileno())
            opened_identity = (
                opened.st_dev,
                opened.st_ino,
                opened.st_size,
                opened.st_mtime_ns,
            )
            if opened_identity != expected_identity or not stat.S_ISREG(opened.st_mode):
                return None
            content = stream.read(maximum_bytes + 1)
    except OSError:
        return None
    if len(content) > maximum_bytes or _regular_file_identity(path) != expected_identity:
        return None
    return content


def default_report_path(cluster: str, *, root: Path | None = None) -> Path:
    """Return a collision-resistant local path for a validation JSON report."""
    directory = root or Path(".clio-relay") / "validation-reports"
    timestamp = _utc_now().strftime("%Y%m%dT%H%M%S.%fZ")
    safe_cluster = "".join(char if char.isalnum() or char in "-_" else "-" for char in cluster)
    return directory / f"validation-{timestamp}-{safe_cluster}-{uuid4().hex[:8]}.json"


def write_validation_report(report: LiveValidationReport, path: Path) -> None:
    """Write a report atomically with deterministic JSON field ordering."""
    validated = LiveValidationReport.model_validate(report.model_dump(mode="python"))
    payload = redact_sensitive_values(validated.model_dump(mode="json"))
    _atomic_write_text(path, json.dumps(payload, indent=2, sort_keys=True) + "\n")


def redact_sensitive_values(value: object) -> object:
    """Return a JSON-compatible copy with capability and credential values removed.

    Runtime ownership tokens are intentionally durable because cleanup must be able
    to authenticate a process after the originating CLI exits. They are capabilities,
    however, and must never be copied into reports or routine CLI responses. Values
    found under a sensitive key are also removed from free-form strings elsewhere in
    the document so command/evidence text cannot accidentally disclose the same
    credential.
    """
    sensitive_values: set[str] = set()
    _collect_sensitive_values(value, sensitive_values)
    return _redact_sensitive_value(value, sensitive_values)


def write_release_gate_result(result: ReleaseGateResult, path: Path) -> None:
    """Atomically persist a machine-readable release gate decision."""
    payload = result.model_dump(mode="json")
    _atomic_write_text(path, json.dumps(payload, indent=2, sort_keys=True) + "\n")


def load_validation_report(path: Path) -> LiveValidationReport:
    """Load and strictly validate a report from disk."""
    logical_path = logical_filesystem_path(path)
    try:
        report = LiveValidationReport.model_validate_json(
            internal_filesystem_path(logical_path, force_extended=True).read_text(encoding="utf-8")
        )
    except (OSError, ValidationError) as exc:
        raise ConfigurationError(f"could not read validation report {logical_path}: {exc}") from exc
    report._source_path = logical_path  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
    return report


def load_release_gate_policy(path: Path) -> ReleaseGatePolicy:
    """Load a JSON or YAML release policy."""
    logical_path = logical_filesystem_path(path)
    internal_path = internal_filesystem_path(logical_path, force_extended=True)
    try:
        document = yaml.safe_load(internal_path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as exc:
        raise ConfigurationError(
            f"could not read release gate policy {logical_path}: {exc}"
        ) from exc
    try:
        policy = ReleaseGatePolicy.model_validate(document)
    except ValidationError as exc:
        raise ConfigurationError(f"invalid release gate policy {logical_path}: {exc}") from exc
    if policy.acceptance_matrix_path is None:
        return policy
    repository_root = next(
        (
            parent
            for parent in (internal_path.parent, *internal_path.parents)
            if (parent / "pyproject.toml").is_file()
        ),
        None,
    )
    if repository_root is None:
        raise ConfigurationError(
            f"could not resolve repository root for release gate policy {logical_path}"
        )
    matrix_path = (repository_root / PurePosixPath(policy.acceptance_matrix_path)).resolve()
    try:
        matrix_path.relative_to(repository_root.resolve())
    except ValueError as exc:
        raise ConfigurationError("release acceptance matrix escapes the policy repository") from exc
    try:
        policy._acceptance_matrix = load_release_acceptance_matrix(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            matrix_path,
            expected_sha256=policy.acceptance_matrix_sha256,
            expected_release_version=policy.release_version,
        )
    except (OSError, ProvenanceError) as exc:
        raise ConfigurationError(
            f"could not bind release acceptance matrix {matrix_path}: {exc}"
        ) from exc
    return policy


def evaluate_release_gate(
    policy: ReleaseGatePolicy,
    reports: Iterable[LiveValidationReport],
    *,
    expected_artifact_sha256: str | None = None,
) -> ReleaseGateResult:
    """Evaluate immutable-artifact reports without inferring untested claims."""
    all_reports = list(reports)
    expected_digest = _validated_sha256(expected_artifact_sha256)
    if _policy_requires_expected_artifact_digest(policy) and expected_digest is None:
        raise ConfigurationError(
            f"{policy.artifact_stage} gates requiring artifact SHA-256 evidence require an "
            "independently computed expected artifact SHA-256"
        )
    matrix = policy.acceptance_matrix
    if policy.acceptance_matrix_path is not None and matrix is None:
        raise ConfigurationError(
            "release gate policy acceptance matrix was not digest-verified by the policy loader"
        )
    matrix_stage: dict[str, object] | None = None
    matrix_pairs: list[tuple[dict[str, object], LiveValidationReport]] = []
    matrix_failures: list[str] = []
    if matrix is not None:
        stages = cast(list[dict[str, object]], matrix["stages"])
        matching_stages = [
            stage for stage in stages if stage.get("artifact_stage") == policy.artifact_stage
        ]
        if len(matching_stages) != 1:
            raise ConfigurationError(
                f"release acceptance matrix does not define artifact stage {policy.artifact_stage}"
            )
        matrix_stage = matching_stages[0]
        prefix = cast(str, matrix_stage["filename_prefix"])
        matrix_reports = cast(list[dict[str, object]], matrix["reports"])
        expected_names = [f"{prefix}-{entry['id']}.json" for entry in matrix_reports]
        nonlocal_reports = [report for report in all_reports if report.cluster != "local"]
        reports_by_name: dict[str, LiveValidationReport] = {}
        duplicate_names: set[str] = set()
        missing_source_ids: list[str] = []
        for report in nonlocal_reports:
            if report.source_path is None:
                missing_source_ids.append(report.report_id)
                continue
            name = report.source_path.name
            if name in reports_by_name:
                duplicate_names.add(name)
            reports_by_name[name] = report
        if missing_source_ids:
            matrix_failures.append(
                "matrix reports were not loaded from provenance-bearing paths: "
                f"{sorted(missing_source_ids)}"
            )
        if duplicate_names:
            matrix_failures.append(f"duplicate matrix report filenames: {sorted(duplicate_names)}")
        actual_names = set(reports_by_name)
        if len(nonlocal_reports) != len(expected_names) or actual_names != set(expected_names):
            matrix_failures.append(
                "non-local report filenames do not exactly match the acceptance matrix: "
                f"missing={sorted(set(expected_names) - actual_names)}, "
                f"unexpected={sorted(actual_names - set(expected_names))}"
            )
        document_ids = [report.report_id for report in nonlocal_reports]
        if len(document_ids) != len(set(document_ids)):
            matrix_failures.append(
                "acceptance matrix reports contain duplicate document report ids"
            )
        for entry, filename in zip(matrix_reports, expected_names, strict=True):
            report = reports_by_name.get(filename)
            if report is None:
                continue
            if report.cluster != entry["cluster"] or report.scenario != entry["scenario"]:
                matrix_failures.append(
                    f"{filename} cluster/scenario does not match acceptance matrix entry "
                    f"{entry['id']}"
                )
            if report.software.version != policy.release_version:
                matrix_failures.append(
                    f"{filename} does not identify clio-relay {policy.release_version}"
                )
            matrix_pairs.append((entry, report))

    candidates = [
        report for report in all_reports if report.software.version == policy.release_version
    ]
    policy_target_identity_sha256 = _policy_target_identity_digests(policy)
    target_identity_sha256: dict[str, str] = {}
    target_identity_failures: list[str] = []
    if policy.require_target_identity:
        target_identity_sha256, target_identity_failures = _report_set_target_identities(
            policy,
            candidates,
        )
    satisfied: list[str] = []
    unsatisfied: dict[str, list[str]] = {}
    used_report_ids: set[str] = set()
    for requirement in policy.requirements:
        reasons: set[str] = set()
        matching_report: LiveValidationReport | None = None
        for report in candidates:
            report_reasons = _report_requirement_failures(
                policy,
                requirement,
                report,
                expected_artifact_sha256=expected_digest,
            )
            if not report_reasons:
                matching_report = report
                break
            reasons.update(report_reasons)
        if matching_report is not None:
            satisfied.append(requirement.requirement_id)
            used_report_ids.add(matching_report.report_id)
            continue
        eligible = [
            report
            for report in candidates
            if not _report_requirement_failures(
                policy,
                requirement,
                report,
                include_requirement_evidence=False,
                expected_artifact_sha256=expected_digest,
            )
        ]
        evidence_groups = _requirement_evidence_groups(requirement, eligible)
        group_failures: list[tuple[int, list[str], list[str], list[str], list[str]]] = []
        matched_group: list[LiveValidationReport] | None = None
        for group in evidence_groups:
            combined_checks = {
                check.check_id
                for report in group
                for check in report.checks
                if check.status is ValidationStatus.PASSED and check.evidence
            }
            combined_resources = {
                resource.kind
                for report in group
                for resource in report.resources
                if resource.cluster == requirement.cluster
            }
            missing_checks = sorted(set(requirement.required_checks) - combined_checks)
            missing_resources = sorted(
                set(requirement.required_resource_kinds) - combined_resources
            )
            resource_predicate_failures = _required_resource_failures(
                requirement,
                [resource for report in group for resource in report.resources],
                expected_cluster=requirement.cluster,
            )
            resource_scope_failures = _requirement_resource_scope_failures(
                requirement,
                [resource for report in group for resource in report.resources],
            )
            identity_failures = _combined_evidence_identity_failures(
                policy,
                requirement,
                group,
                expected_artifact_sha256=expected_digest,
            )
            if (
                not missing_checks
                and not missing_resources
                and not resource_predicate_failures
                and not resource_scope_failures
                and not identity_failures
            ):
                matched_group = group
                break
            group_failures.append(
                (
                    len(missing_checks)
                    + len(missing_resources)
                    + len(resource_predicate_failures)
                    + len(resource_scope_failures)
                    + len(identity_failures),
                    missing_checks,
                    missing_resources,
                    [*resource_scope_failures, *resource_predicate_failures],
                    identity_failures,
                )
            )
        if matched_group is not None:
            satisfied.append(requirement.requirement_id)
            used_report_ids.update(report.report_id for report in matched_group)
            continue
        if eligible and not evidence_groups:
            if requirement.evidence_group_resource_kind is None:
                reasons.add("requirement evidence must be satisfied by one coherent report")
            else:
                reasons.add(
                    "no reports share required evidence group resource kind "
                    f"{requirement.evidence_group_resource_kind}"
                )
        if group_failures:
            (
                _,
                missing_checks,
                missing_resources,
                resource_predicate_failures,
                identity_failures,
            ) = min(group_failures, key=lambda item: item[0])
            if missing_checks:
                reasons.add(f"missing passed checks across reports: {missing_checks}")
            if missing_resources:
                reasons.add(f"missing resource evidence across reports: {missing_resources}")
            reasons.update(resource_predicate_failures)
            reasons.update(identity_failures)
        unsatisfied[requirement.requirement_id] = sorted(reasons) or [
            f"no report for clio-relay {policy.release_version}"
        ]
    used_reports = [report for report in candidates if report.report_id in used_report_ids]
    nonlocal_commits = {
        report.software.commit
        for report in used_reports
        if report.cluster != "local" and report.software.commit is not None
    }
    if policy.require_commit and len(nonlocal_commits) > 1:
        unsatisfied["release-artifact-identity"] = [
            "used non-local reports identify different source commits"
        ]
    if target_identity_failures:
        unsatisfied["target-identity"] = target_identity_failures
    if policy.release_blockers:
        unsatisfied["declared-release-blockers"] = list(policy.release_blockers)
    if matrix_pairs:
        unused_matrix_ids = [
            cast(str, entry["id"])
            for entry, report in matrix_pairs
            if report.report_id not in used_report_ids
        ]
        if unused_matrix_ids:
            matrix_failures.append(
                "acceptance matrix reports were not used by any policy requirement: "
                f"{unused_matrix_ids}"
            )
    if matrix_failures:
        unsatisfied["acceptance-matrix"] = matrix_failures
    return ReleaseGateResult(
        release_version=policy.release_version,
        artifact_sha256=expected_digest,
        acceptance_matrix_schema_version=(
            cast(str, matrix["schema_version"]) if matrix is not None else None
        ),
        acceptance_matrix_release_version=(
            cast(str, matrix["release_version"]) if matrix is not None else None
        ),
        acceptance_matrix_sha256=(
            cast(str, matrix["matrix_sha256"]) if matrix is not None else None
        ),
        acceptance_matrix_stage=(
            cast(str, matrix_stage["name"]) if matrix_stage is not None else None
        ),
        acceptance_report_ids=[cast(str, entry["id"]) for entry, _ in matrix_pairs],
        acceptance_report_document_ids=[report.report_id for _, report in matrix_pairs],
        policy_target_identity_sha256=policy_target_identity_sha256,
        target_identity_sha256=target_identity_sha256,
        passed=not unsatisfied,
        satisfied_requirements=satisfied,
        unsatisfied_requirements=unsatisfied,
        report_ids=sorted(used_report_ids),
    )


def _policy_requires_expected_artifact_digest(policy: ReleaseGatePolicy) -> bool:
    """Return whether any effective gate requirement needs an external artifact digest."""
    if policy.require_artifact_sha256:
        return True
    return any(requirement.require_artifact_sha256 is True for requirement in policy.requirements)


def _policy_target_identity_digests(policy: ReleaseGatePolicy) -> dict[str, str]:
    """Return only policy target digests proven to match their canonical fields."""
    digests: dict[str, str] = {}
    for label, target in sorted(policy.targets.items()):
        _, digest, failures = _validated_policy_target(target)
        if digest is not None and not failures:
            digests[label] = digest
    return digests


def _combined_evidence_identity_failures(
    policy: ReleaseGatePolicy,
    requirement: ReleaseGateRequirement,
    reports: list[LiveValidationReport],
    *,
    expected_artifact_sha256: str | None,
) -> list[str]:
    """Reject evidence aggregation across different builds or release artifacts."""
    failures: list[str] = []
    commits = {report.software.commit for report in reports if report.software.commit is not None}
    if policy.require_commit and len(commits) > 1:
        failures.append("combined reports identify different source commits")
    require_artifact_sha256 = (
        policy.require_artifact_sha256
        if requirement.require_artifact_sha256 is None
        else requirement.require_artifact_sha256
    )
    artifact_hashes = {
        report.install_source.artifact_sha256
        for report in reports
        if report.install_source.artifact_sha256 is not None
    }
    if require_artifact_sha256 and len(artifact_hashes) > 1:
        failures.append("combined reports identify different tested artifact SHA-256 values")
    if expected_artifact_sha256 is not None and any(
        report.cluster != "local"
        and report.install_source.artifact_sha256 != expected_artifact_sha256
        for report in reports
    ):
        failures.append("combined reports do not identify the expected candidate artifact")
    return failures


def _requirement_evidence_groups(
    requirement: ReleaseGateRequirement,
    reports: list[LiveValidationReport],
) -> list[list[LiveValidationReport]]:
    """Group multi-report evidence by a shared stable resource when required."""
    kind = requirement.evidence_group_resource_kind
    if kind is None:
        return []
    grouped: dict[str, list[LiveValidationReport]] = {}
    for report in reports:
        resource_ids = {
            resource.resource_id for resource in report.resources if resource.kind == kind
        }
        if len(resource_ids) != 1:
            continue
        resource_id = next(iter(resource_ids))
        grouped.setdefault(resource_id, []).append(report)
    return list(grouped.values())


def render_validation_markdown(report: LiveValidationReport) -> str:
    """Render a concise human-readable view of the canonical JSON report."""
    lines = [
        f"# live validation {report.report_id}",
        "",
        f"- status: `{report.status.value}`",
        f"- scenario: `{report.scenario}`",
        f"- cluster: `{report.cluster}`",
        f"- clio-relay: `{report.software.version}`",
        f"- commit: `{report.software.commit or 'unknown'}`",
        f"- install: `{report.install_source.kind.value}` via `{report.install_source.launcher}`",
        "",
        "## checks",
        "",
    ]
    lines.extend(
        f"- `{check.status.value}` `{check.check_id}`: {check.summary}" for check in report.checks
    )
    lines.extend(["", "## resources", ""])
    lines.extend(
        f"- `{resource.kind}` `{resource.resource_id}`"
        + (f" ({resource.state})" if resource.state is not None else "")
        for resource in report.resources
    )
    if report.error is not None:
        lines.extend(["", "## failure", "", f"`{report.error}`"])
    return "\n".join(lines) + "\n"


def sha256_file(path: Path) -> str:
    """Return the SHA-256 digest of an artifact without loading it all at once."""
    digest = hashlib.sha256()
    with internal_filesystem_path(path, force_extended=True).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _report_requirement_failures(
    policy: ReleaseGatePolicy,
    requirement: ReleaseGateRequirement,
    report: LiveValidationReport,
    *,
    include_requirement_evidence: bool = True,
    expected_artifact_sha256: str | None = None,
) -> list[str]:
    failures: list[str] = []
    allowed_sources = requirement.allowed_install_sources or policy.allowed_install_sources
    allowed_launchers = requirement.allowed_launchers or policy.allowed_launchers
    require_released = (
        policy.require_released_artifact
        if requirement.require_released_artifact is None
        else requirement.require_released_artifact
    )
    require_artifact_sha256 = (
        policy.require_artifact_sha256
        if requirement.require_artifact_sha256 is None
        else requirement.require_artifact_sha256
    )
    if report.cluster != requirement.cluster:
        failures.append(f"requires cluster {requirement.cluster}")
    if report.scenario not in requirement.scenarios:
        failures.append(f"requires scenario in {requirement.scenarios}")
    if report.status is not ValidationStatus.PASSED:
        failures.append("report did not pass")
    if report.cluster != "local" and not _has_complete_producer_identity(report.evidence_trust):
        failures.append(
            "non-local report omits authenticated producer GitHub identity or invocation id"
        )
    if report.cluster != "local":
        failures.extend(_launcher_identity_failures(policy, report))
    if report.install_source.kind not in allowed_sources:
        failures.append(
            f"install source {report.install_source.kind.value} is not release-approved"
        )
    if report.install_source.detected_kind not in allowed_sources:
        failures.append(
            "detected install source "
            f"{report.install_source.detected_kind.value} is not release-approved"
        )
    if report.install_source.launcher not in allowed_launchers:
        failures.append(f"launcher {report.install_source.launcher} is not release-approved")
    if (
        report.install_source.launcher in {"uv-tool", "uvx"}
        and not report.install_source.launcher_verified
    ):
        failures.append("report does not contain a process-observed uv launcher receipt")
    if require_released and not report.install_source.released_artifact:
        failures.append("report does not prove a released artifact")
    if require_released and not report.install_source.artifact_identity_verified:
        failures.append("report does not bind the running distribution to the released wheel")
    if require_artifact_sha256 and report.install_source.artifact_sha256 is None:
        failures.append("report does not identify the tested artifact SHA-256")
    if (
        expected_artifact_sha256 is not None
        and report.cluster != "local"
        and report.install_source.artifact_sha256 != expected_artifact_sha256
    ):
        failures.append(
            "tested artifact SHA-256 does not match the immutable candidate: "
            f"{report.install_source.artifact_sha256 or 'missing'}"
        )
    if (
        expected_artifact_sha256 is not None
        and report.cluster != "local"
        and not report.install_source.artifact_identity_verified
    ):
        failures.append("running distribution is not bound to the expected wheel bytes")
    if policy.require_clean_build and report.software.dirty is not False:
        failures.append("report does not prove a clean build")
    if policy.require_commit and report.software.commit is None:
        failures.append("report does not identify a source commit")
    if policy.require_exact_tag and report.software.tag != f"v{policy.release_version}":
        failures.append(
            f"report source tag must be v{policy.release_version}, got {report.software.tag}"
        )
    if report.install_source.distribution_version != policy.release_version:
        failures.append(
            "installed distribution version does not match the release policy: "
            f"{report.install_source.distribution_version}"
        )
    if policy.require_target_identity and report.cluster != "local":
        _, identity_failures = _report_target_identity(
            report,
            policy.targets.get(report.cluster),
        )
        failures.extend(identity_failures)
    failures.extend(_requirement_resource_scope_failures(requirement, report.resources))
    if include_requirement_evidence:
        passed_checks = {
            check.check_id
            for check in report.checks
            if check.status is ValidationStatus.PASSED and check.evidence
        }
        missing_checks = sorted(set(requirement.required_checks) - passed_checks)
        if missing_checks:
            failures.append(f"missing passed checks: {missing_checks}")
        resource_kinds = {
            resource.kind
            for resource in report.resources
            if resource.cluster == requirement.cluster
        }
        missing_resources = sorted(set(requirement.required_resource_kinds) - resource_kinds)
        if missing_resources:
            failures.append(f"missing resource evidence: {missing_resources}")
        if requirement.evidence_group_resource_kind is not None:
            grouping_ids = {
                resource.resource_id
                for resource in report.resources
                if resource.kind == requirement.evidence_group_resource_kind
            }
            if len(grouping_ids) != 1:
                failures.append(
                    "report must identify exactly one evidence-group resource "
                    f"of kind {requirement.evidence_group_resource_kind}; "
                    f"found {sorted(grouping_ids)}"
                )
        failures.extend(
            _required_resource_failures(
                requirement,
                report.resources,
                expected_cluster=requirement.cluster,
            )
        )
        failures.extend(_spack_fresh_install_transition_failures(requirement, report))
        failures.extend(_jarvis_execution_identity_failures(requirement, report))
    return failures


def _has_complete_producer_identity(trust: EvidenceTrust) -> bool:
    """Return whether report provenance contains the complete producer tuple."""
    return (
        trust.producer_github_login is not None
        and trust.producer_github_id is not None
        and trust.invocation_id is not None
    )


def _launcher_identity_failures(
    policy: ReleaseGatePolicy,
    report: LiveValidationReport,
) -> list[str]:
    """Require the launcher binary and invocation nonce to be process-bound evidence."""
    receipt = report.install_source.launcher_receipt
    failures: list[str] = []
    if receipt.get("verified") is not True or receipt.get("uv_executable_verified") is not True:
        failures.append("launcher receipt does not verify the exact uv executable")
    invocation_id = receipt.get("invocation_id")
    if invocation_id != report.evidence_trust.invocation_id:
        failures.append("launcher receipt invocation id does not match report producer provenance")
    uv_version = receipt.get("uv_version")
    if (
        not isinstance(uv_version, str)
        or re.fullmatch(
            r"[0-9]+\.[0-9]+\.[0-9]+(?:[A-Za-z0-9.+-]*)?",
            uv_version,
        )
        is None
    ):
        failures.append("launcher receipt omits an exact uv version")
    elif policy.required_uv_version is not None and uv_version != policy.required_uv_version:
        failures.append(
            f"launcher receipt uv version must be {policy.required_uv_version}, got {uv_version}"
        )
    executable_sha256 = receipt.get("uv_executable_sha256")
    if (
        not isinstance(executable_sha256, str)
        or re.fullmatch(r"[0-9a-f]{64}", executable_sha256) is None
    ):
        failures.append("launcher receipt omits a lowercase uv executable SHA-256")
    if report.install_source.launcher == "uv-tool":
        if receipt.get("claimed_launcher") != "uv-tool":
            failures.append("launcher receipt does not identify the persistent uv tool path")
        for field in ("uv_tool_directory", "uv_tool_bin_directory", "process_prefix"):
            value = receipt.get(field)
            if not isinstance(value, str) or not value or not Path(value).is_absolute():
                failures.append(f"launcher receipt omits absolute {field}")
        for field in (
            "tool_environment_verified",
            "tool_bin_bound",
            "tool_target_bound",
            "pyvenv_matches_uv",
            "package_in_process_environment",
            "executable_in_process_environment",
            "isolated_environment",
        ):
            if receipt.get(field) is not True:
                failures.append(f"launcher receipt does not verify {field}")
        record = receipt.get("distribution_record")
        record_mapping = cast(dict[str, Any], record) if isinstance(record, dict) else {}
        if record_mapping.get("verified") is not True:
            failures.append("launcher receipt does not verify the installed RECORD closure")
        for field in ("record_sha256", "runtime_closure_sha256"):
            value = record_mapping.get(field)
            if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None:
                failures.append(f"launcher receipt omits lowercase {field}")
    return failures


def _report_set_target_identities(
    policy: ReleaseGatePolicy,
    reports: Iterable[LiveValidationReport],
) -> tuple[dict[str, str], list[str]]:
    """Bind exact policy target coverage to policy-pinned physical identities."""
    digests_by_cluster: dict[str, set[str]] = {}
    failures: list[str] = []
    report_list = list(reports)
    observed_clusters = {report.cluster for report in report_list if report.cluster != "local"}
    policy_clusters = set(policy.targets)
    missing_clusters = sorted(policy_clusters - observed_clusters)
    extra_clusters = sorted(observed_clusters - policy_clusters)
    if missing_clusters:
        failures.append(f"policy targets lack report coverage: {missing_clusters}")
    if extra_clusters:
        failures.append(f"reports reference targets absent from policy: {extra_clusters}")
    for report in report_list:
        if report.cluster == "local":
            continue
        digest, report_failures = _report_target_identity(
            report,
            policy.targets.get(report.cluster),
        )
        failures.extend(
            f"report {report.report_id} for cluster {report.cluster}: {failure}"
            for failure in report_failures
        )
        if digest is not None:
            digests_by_cluster.setdefault(report.cluster, set()).add(digest)
    stable: dict[str, str] = {}
    for cluster, digests in sorted(digests_by_cluster.items()):
        if len(digests) == 1:
            stable[cluster] = next(iter(digests))
            continue
        failures.append(
            f"cluster {cluster} reports identify different physical target identities: "
            f"{sorted(digests)}"
        )
    return stable, sorted(set(failures))


def _report_target_identity(
    report: LiveValidationReport,
    policy_target: ReleaseTargetIdentity | None,
) -> tuple[str | None, list[str]]:
    """Validate an observed target and compare it with the independent policy pin."""
    failures: list[str] = []
    passed_checks = {
        check.check_id
        for check in report.checks
        if check.status is ValidationStatus.PASSED and check.evidence
    }
    if "worker.target-identity" not in passed_checks:
        failures.append("missing evidenced worker.target-identity check")
    targets = [resource for resource in report.resources if resource.kind == "cluster_target"]
    if len(targets) != 1:
        failures.append(f"must identify exactly one cluster_target resource; found {len(targets)}")
        return None, failures
    target = targets[0]
    if target.cluster != report.cluster:
        failures.append("cluster_target resource does not match the report cluster")
    if target.role != "physical_cluster_target":
        failures.append("cluster_target resource is not a physical_cluster_target")
    if target.state != "verified":
        failures.append("cluster_target resource state is not verified")
    metadata = target.metadata
    if metadata.get("verified") is not True:
        failures.append("cluster_target metadata is not verified")
    if metadata.get("schema_version") != "clio-relay.cluster-target-info.v1":
        failures.append("cluster_target schema version does not match")

    observed_hostnames = {
        normalized
        for key in ("hostname", "fqdn")
        if isinstance((value := metadata.get(key)), str)
        and (normalized := _normalized_hostname(value))
    }
    observed_fingerprints = _target_identity_string_set(
        metadata.get("ssh_host_key_sha256"),
        field="ssh_host_key_sha256",
        failures=failures,
    )
    if not observed_hostnames:
        failures.append("cluster_target must identify an observed hostname or FQDN")

    provider = target.provider
    observed_provider = metadata.get("scheduler_provider")
    if not isinstance(provider, str) or not provider.strip():
        failures.append("cluster_target resource omits its scheduler provider")
    elif observed_provider != provider:
        failures.append("cluster_target scheduler provider does not match its metadata")

    observed_scheduler = metadata.get("scheduler_cluster_name")
    if observed_scheduler is not None and (
        not isinstance(observed_scheduler, str) or not observed_scheduler.strip()
    ):
        failures.append("scheduler_cluster_name must be a non-empty string or null")

    observed_site_marker = metadata.get("site_marker_sha256")
    if (
        not isinstance(observed_site_marker, str)
        or re.fullmatch(r"[0-9a-fA-F]{64}", observed_site_marker) is None
    ):
        failures.append("site_marker_sha256 must identify the observed physical target")

    if (
        failures
        or not observed_hostnames
        or not observed_fingerprints
        or not isinstance(provider, str)
        or not isinstance(observed_site_marker, str)
    ):
        return None, failures
    canonical = {
        "schema_version": "clio-relay.cluster-target-identity.v1",
        "observed_hostnames": sorted(observed_hostnames),
        "observed_ssh_host_key_sha256": sorted(observed_fingerprints),
        "scheduler_cluster_name": (
            observed_scheduler.strip() if isinstance(observed_scheduler, str) else None
        ),
        "site_marker_sha256": observed_site_marker.lower(),
        "scheduler_provider": provider.strip().lower(),
    }
    digest = _canonical_target_identity_sha256(canonical)
    if policy_target is None:
        failures.append("cluster label has no independently pinned policy target")
        return digest, failures
    pinned_canonical, pinned_digest, pin_failures = _validated_policy_target(policy_target)
    failures.extend(pin_failures)
    if pinned_canonical is not None and canonical != pinned_canonical:
        differing_fields = sorted(
            key for key in canonical if canonical.get(key) != pinned_canonical.get(key)
        )
        failures.append(
            f"observed physical target does not match policy-pinned fields: {differing_fields}"
        )
    if pinned_digest is not None and digest != pinned_digest:
        failures.append("observed physical target digest does not match the policy pin")
    return digest, failures


def _validated_policy_target(
    target: ReleaseTargetIdentity,
) -> tuple[dict[str, Any] | None, str | None, list[str]]:
    """Validate a target pin and bind its declared digest to its canonical fields."""
    values = [
        *target.hostnames,
        *target.ssh_host_key_sha256,
        target.scheduler_provider,
        target.site_marker_sha256,
        target.identity_sha256,
    ]
    if target.scheduler_cluster_name is not None:
        values.append(target.scheduler_cluster_name)
    if any(value.strip().upper().startswith("PENDING") for value in values):
        return None, None, ["policy target identity contains a PENDING pin"]
    failures: list[str] = []
    if re.fullmatch(r"[0-9a-fA-F]{64}", target.site_marker_sha256) is None:
        failures.append("policy target site_marker_sha256 is not a SHA-256 digest")
    if re.fullmatch(r"[0-9a-fA-F]{64}", target.identity_sha256) is None:
        failures.append("policy target identity_sha256 is not a SHA-256 digest")
    if failures:
        return None, None, failures
    canonical: dict[str, Any] = {
        "schema_version": "clio-relay.cluster-target-identity.v1",
        "observed_hostnames": sorted(_normalized_hostname(item) for item in target.hostnames),
        "observed_ssh_host_key_sha256": sorted(item.strip() for item in target.ssh_host_key_sha256),
        "scheduler_cluster_name": (
            target.scheduler_cluster_name.strip()
            if target.scheduler_cluster_name is not None
            else None
        ),
        "site_marker_sha256": target.site_marker_sha256.lower(),
        "scheduler_provider": target.scheduler_provider.strip().lower(),
    }
    computed_digest = _canonical_target_identity_sha256(canonical)
    declared_digest = target.identity_sha256.lower()
    if computed_digest != declared_digest:
        failures.append("policy target identity_sha256 does not match its pinned fields")
    return canonical, declared_digest, failures


def _canonical_target_identity_sha256(canonical: dict[str, Any]) -> str:
    """Hash one normalized physical target identity deterministically."""
    encoded = json.dumps(
        canonical,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _target_identity_string_set(
    value: object,
    *,
    field: str,
    failures: list[str],
    normalize_hostname: bool = False,
) -> set[str]:
    """Validate a non-empty unique string list used in a target identity."""
    if not isinstance(value, list) or not value:
        failures.append(f"cluster_target {field} must be a non-empty list")
        return set()
    raw_items = cast(list[object], value)
    if any(not isinstance(item, str) or not item.strip() for item in raw_items):
        failures.append(f"cluster_target {field} contains a blank or non-string value")
        return set()
    normalized = {
        _normalized_hostname(item) if normalize_hostname else item.strip()
        for item in cast(list[str], raw_items)
    }
    if "" in normalized or len(normalized) != len(raw_items):
        failures.append(f"cluster_target {field} contains duplicate or invalid values")
        return set()
    return normalized


def _normalized_hostname(value: str) -> str:
    """Normalize hostnames for case-insensitive physical identity comparison."""
    return value.strip().rstrip(".").lower()


def _required_resource_failures(
    requirement: ReleaseGateRequirement,
    resources_to_check: Iterable[ValidationResource],
    *,
    expected_cluster: str,
) -> list[str]:
    """Return failures for stateful resource predicates in a release policy."""
    resources = list(resources_to_check)
    failures: list[str] = []
    for required in requirement.required_resources:
        matching = _matching_required_resources(
            required,
            resources,
            expected_cluster=expected_cluster,
        )
        if len(matching) >= required.minimum_count:
            continue
        constraints: list[str] = []
        if required.roles is not None:
            constraints.append(f"roles={required.roles}")
        if required.states is not None:
            constraints.append(f"states={required.states}")
        if required.providers is not None:
            constraints.append(f"providers={required.providers}")
        if required.metadata_equals:
            constraints.append(f"metadata_equals={required.metadata_equals}")
        suffix = f" ({', '.join(constraints)})" if constraints else ""
        failures.append(
            f"requires {required.minimum_count} matching {required.kind} resource(s){suffix}; "
            f"found {len(matching)}"
        )
    return failures


def _matching_required_resources(
    required: ReleaseResourceRequirement,
    resources: Iterable[ValidationResource],
    *,
    expected_cluster: str,
) -> list[ValidationResource]:
    """Return resources matching one predicate on the exact policy target."""
    return [
        resource
        for resource in resources
        if resource.cluster == expected_cluster
        and resource.kind == required.kind
        and (required.roles is None or resource.role in required.roles)
        and (required.states is None or resource.state in required.states)
        and (required.providers is None or resource.provider in required.providers)
        and all(
            _metadata_value_matches(resource.metadata.get(key), expected)
            for key, expected in required.metadata_equals.items()
        )
    ]


def _requirement_resource_scope_failures(
    requirement: ReleaseGateRequirement,
    resources_to_check: Iterable[ValidationResource],
) -> list[str]:
    """Reject required evidence kinds attributed to any other target or no target."""
    target_scoped_kinds = {
        *requirement.required_resource_kinds,
        *(required.kind for required in requirement.required_resources),
    }
    mismatched = sorted(
        {
            f"{resource.kind}:{resource.resource_id}:{resource.cluster or '<unscoped>'}"
            for resource in resources_to_check
            if resource.kind in target_scoped_kinds and resource.cluster != requirement.cluster
        }
    )
    if not mismatched:
        return []
    return [
        f"required evidence resources must belong to cluster {requirement.cluster}: {mismatched}"
    ]


_JARVIS_EXECUTION_CHECK_IDS = frozenset(
    {
        "jarvis.structured-runtime-metadata",
        "remote-mcp.jarvis-execution-query",
        "remote-mcp.jarvis-live-progress",
    }
)
_JARVIS_EXECUTION_RELAY_JOB_ROLES = frozenset(
    {"jarvis_mcp_execution_query", "virtual_jarvis_mcp_call"}
)


def _jarvis_execution_identity_failures(
    requirement: ReleaseGateRequirement,
    report: LiveValidationReport,
) -> list[str]:
    """Bind JARVIS checks and semantic resources to one durable execution."""
    if "jarvis_execution_progress" not in {
        *requirement.required_resource_kinds,
        *(required.kind for required in requirement.required_resources),
    }:
        return []

    failures: list[str] = []
    execution_ids: set[str] = set()
    identity_requirements = [
        required
        for required in requirement.required_resources
        if required.kind in {"jarvis_execution_progress", "jarvis_generated_artifact"}
        or (
            required.kind == "relay_job"
            and required.roles is not None
            and bool(_JARVIS_EXECUTION_RELAY_JOB_ROLES.intersection(required.roles))
        )
    ]
    for required in identity_requirements:
        for resource in _matching_required_resources(
            required,
            report.resources,
            expected_cluster=requirement.cluster,
        ):
            execution_id = resource.metadata.get("execution_id")
            if not isinstance(execution_id, str) or not execution_id:
                failures.append(
                    "JARVIS execution-scoped resource omits execution_id: "
                    f"{resource.kind}:{resource.resource_id}"
                )
                continue
            execution_ids.add(execution_id)

    if len(execution_ids) != 1:
        failures.append(
            "JARVIS execution-scoped resources do not identify exactly one execution: "
            f"{sorted(execution_ids)}"
        )
        return failures
    expected_execution_id = next(iter(execution_ids))

    for check_id in sorted(_JARVIS_EXECUTION_CHECK_IDS.intersection(requirement.required_checks)):
        checks = [
            check
            for check in report.checks
            if check.check_id == check_id
            and check.status is ValidationStatus.PASSED
            and check.evidence
        ]
        if len(checks) != 1:
            failures.append(
                f"JARVIS execution check {check_id} must appear exactly once in the report"
            )
            continue
        evidence_ids = [evidence.metadata.get("execution_id") for evidence in checks[0].evidence]
        if (
            not evidence_ids
            or any(not isinstance(value, str) or not value for value in evidence_ids)
            or set(cast(list[str], evidence_ids)) != {expected_execution_id}
        ):
            failures.append(
                f"JARVIS execution check {check_id} is not bound to "
                f"execution {expected_execution_id}"
            )
    return failures


def _spack_fresh_install_transition_failures(
    requirement: ReleaseGateRequirement,
    report: LiveValidationReport,
) -> list[str]:
    """Independently bind one typed Spack fresh-install transition report."""
    expected = requirement.spack_fresh_install_transition
    if expected is None:
        return []
    failures: list[str] = []
    checks: dict[str, dict[str, Any]] = {}
    for check_id in SPACK_FRESH_INSTALL_TRANSITION_CHECK_IDS:
        metadata = _unique_spack_transition_check_metadata(report, check_id, failures)
        if metadata is not None:
            checks[check_id] = metadata

    phase_definitions = (
        (
            "preinstall",
            "spack_preinstall_find",
            "spack_find",
            {"query": expected.requested_spec},
        ),
        (
            "install",
            "spack_fresh_install",
            "spack_install",
            {"spec": expected.requested_spec, "reuse": False},
        ),
        ("postinstall", "spack_postinstall_locate", "spack_locate", None),
    )
    phase_resources: dict[str, ValidationResource] = {}
    phase_indexes: list[int] = []
    for phase, role, tool, arguments in phase_definitions:
        matches = [
            (index, resource)
            for index, resource in enumerate(report.resources)
            if resource.cluster == requirement.cluster
            and resource.kind == "relay_job"
            and resource.role == role
        ]
        if len(matches) != 1:
            failures.append(
                f"Spack fresh-install transition requires exactly one {phase} phase job; "
                f"found {len(matches)}"
            )
            continue
        index, resource = matches[0]
        phase_indexes.append(index)
        phase_resources[phase] = resource
        metadata = resource.metadata
        if resource.state != "succeeded":
            failures.append(f"Spack {phase} phase job did not succeed")
        if metadata.get("remote_mcp_server_name") != expected.server_name:
            failures.append(f"Spack {phase} phase job identifies the wrong server")
        if metadata.get("profile") != expected.profile:
            failures.append(f"Spack {phase} phase job identifies the wrong profile")
        if metadata.get("remote_mcp_tool_name") != tool:
            failures.append(f"Spack {phase} phase job identifies the wrong tool")
        if arguments is not None and metadata.get("arguments") != arguments:
            failures.append(f"Spack {phase} phase job arguments do not match policy")

    if len(phase_indexes) == len(phase_definitions) and phase_indexes != sorted(phase_indexes):
        failures.append("Spack phase jobs are not recorded in preinstall/install/postinstall order")
    phase_job_ids = [
        phase_resources[phase].resource_id
        for phase in ("preinstall", "install", "postinstall")
        if phase in phase_resources
    ]
    if len(phase_job_ids) == 3 and len(set(phase_job_ids)) != 3:
        failures.append("Spack transition phase jobs do not have distinct durable identities")

    preinstall_result = _spack_phase_structured_result(
        phase_resources.get("preinstall"),
        phase="preinstall",
        failures=failures,
    )
    if preinstall_result is not None and preinstall_result != {
        "schema_version": "spack.mcp.result.v1",
        "operation": "find",
        "query": expected.requested_spec,
        "count": 0,
        "packages": [],
    }:
        failures.append("Spack preinstall phase does not prove the exact spec was absent")

    install_result = _spack_phase_structured_result(
        phase_resources.get("install"),
        phase="install",
        failures=failures,
    )
    dag_hash: str | None = None
    if install_result is not None:
        package = _spack_transition_mapping(install_result.get("package"))
        raw_hash = package.get("dag_hash") if package is not None else None
        if isinstance(raw_hash, str) and re.fullmatch(r"[a-z0-9]{32}", raw_hash) is not None:
            dag_hash = raw_hash
        install_matches = (
            install_result.get("schema_version") == "spack.mcp.result.v1"
            and install_result.get("operation") == "install"
            and install_result.get("requested_spec") == expected.requested_spec
            and install_result.get("reuse") is expected.reuse
            and install_result.get("status") == "installed"
            and install_result.get("package_count") == 1
            and package is not None
            and package.get("name") == expected.package_name
            and dag_hash is not None
        )
        if not install_matches:
            failures.append(
                "Spack install phase does not bind the exact package/spec with reuse=false"
            )

    postinstall_resource = phase_resources.get("postinstall")
    postinstall_result = _spack_phase_structured_result(
        postinstall_resource,
        phase="postinstall",
        failures=failures,
    )
    prefix: str | None = None
    exact_hash_spec = f"/{dag_hash}" if dag_hash is not None else None
    if postinstall_result is not None:
        package = _spack_transition_mapping(postinstall_result.get("package"))
        raw_prefix = postinstall_result.get("prefix")
        prefix = raw_prefix if isinstance(raw_prefix, str) and raw_prefix else None
        postinstall_matches = (
            exact_hash_spec is not None
            and postinstall_result.get("schema_version") == "spack.mcp.result.v1"
            and postinstall_result.get("operation") == "locate"
            and postinstall_result.get("requested_spec") == exact_hash_spec
            and postinstall_result.get("load_spec") == exact_hash_spec
            and package is not None
            and package.get("name") == expected.package_name
            and package.get("dag_hash") == dag_hash
            and prefix is not None
        )
        if not postinstall_matches:
            failures.append("Spack postinstall phase does not locate the exact installed DAG")
    if postinstall_resource is not None and postinstall_resource.metadata.get("arguments") != {
        "spec": exact_hash_spec
    }:
        failures.append("Spack postinstall phase does not query the exact /dag_hash")

    _bind_spack_transition_phase_checks(
        checks,
        expected=expected,
        preinstall_result=preinstall_result,
        install_result=install_result,
        postinstall_result=postinstall_result,
        dag_hash=dag_hash,
        failures=failures,
    )
    _bind_spack_transition_identity(
        checks.get("remote-mcp.spack-transition-identity"),
        requirement=requirement,
        expected=expected,
        failures=failures,
    )
    _bind_spack_transition_durable_evidence(
        checks.get("remote-mcp.spack-transition-durable-evidence"),
        phase_job_ids=phase_job_ids,
        failures=failures,
    )
    store_root = _bind_spack_disposable_store(
        checks.get("remote-mcp.spack-disposable-store"),
        prefix=prefix,
        failures=failures,
    )
    if store_root is None or prefix is None:
        failures.append("Spack transition omits its disposable store or installed prefix")
    _bind_spack_configuration_identity(
        checks.get("remote-mcp.spack-fresh-configuration"),
        report=report,
        requirement=requirement,
        failures=failures,
    )
    _bind_spack_transition_artifacts(
        report,
        requirement=requirement,
        phase_resources=phase_resources,
        failures=failures,
    )
    server_resources = [
        resource
        for resource in report.resources
        if resource.cluster == requirement.cluster
        and resource.kind == "mcp_server"
        and resource.role == "remote_mcp_server"
        and resource.metadata.get("server_name") == expected.server_name
    ]
    if len(server_resources) != 1 or server_resources[0].state != "verified":
        failures.append("Spack transition does not identify one verified fresh MCP server")
    return failures


def _unique_spack_transition_check_metadata(
    report: LiveValidationReport,
    check_id: str,
    failures: list[str],
) -> dict[str, Any] | None:
    """Return one passed transition check's single structured evidence object."""
    matches = [check for check in report.checks if check.check_id == check_id]
    if len(matches) != 1:
        failures.append(f"Spack transition check {check_id} must appear exactly once")
        return None
    check = matches[0]
    if check.status is not ValidationStatus.PASSED or len(check.evidence) != 1:
        failures.append(f"Spack transition check {check_id} is not one passed evidence record")
        return None
    metadata = check.evidence[0].metadata
    if not metadata:
        failures.append(f"Spack transition check {check_id} has no structured evidence")
        return None
    return metadata


def _spack_transition_mapping(value: object) -> dict[str, Any] | None:
    """Narrow one untrusted report value to a string-keyed mapping."""
    if not isinstance(value, dict):
        return None
    raw = cast(dict[object, object], value)
    if any(not isinstance(key, str) for key in raw):
        return None
    return cast(dict[str, Any], value)


def _spack_phase_structured_result(
    resource: ValidationResource | None,
    *,
    phase: str,
    failures: list[str],
) -> dict[str, Any] | None:
    """Read one phase result from its exact durable relay-job resource."""
    result = (
        _spack_transition_mapping(resource.metadata.get("structured_result"))
        if resource is not None
        else None
    )
    if result is None:
        failures.append(f"Spack {phase} phase job omits structured result evidence")
    return result


def _bind_spack_transition_phase_checks(
    checks: dict[str, dict[str, Any]],
    *,
    expected: ReleaseSpackFreshInstallRequirement,
    preinstall_result: dict[str, Any] | None,
    install_result: dict[str, Any] | None,
    postinstall_result: dict[str, Any] | None,
    dag_hash: str | None,
    failures: list[str],
) -> None:
    """Cross-bind phase check evidence to the three canonical job projections."""
    phase_checks = (
        (
            "remote-mcp.spack-preinstall-absent",
            {"query": expected.requested_spec},
            preinstall_result,
        ),
        (
            "remote-mcp.spack-fresh-install",
            {"spec": expected.requested_spec, "reuse": False},
            install_result,
        ),
        (
            "remote-mcp.spack-postinstall-locate",
            {"spec": f"/{dag_hash}" if dag_hash is not None else None},
            postinstall_result,
        ),
    )
    for check_id, arguments, observed in phase_checks:
        evidence = checks.get(check_id)
        if evidence is None:
            continue
        if (
            evidence.get("submitted_arguments") != arguments
            or evidence.get("observed") != observed
            or evidence.get("failures") != []
        ):
            failures.append(f"Spack transition check {check_id} is not bound to its phase job")
    preinstall = checks.get("remote-mcp.spack-preinstall-absent")
    if preinstall is not None and preinstall.get("expected_requested_spec") != (
        expected.requested_spec
    ):
        failures.append("Spack absence check identifies the wrong requested spec")
    install = checks.get("remote-mcp.spack-fresh-install")
    install_expected = (
        _spack_transition_mapping(install.get("expected")) if install is not None else None
    )
    if install_expected != {
        "requested_spec": expected.requested_spec,
        "package_name": expected.package_name,
        "dag_hash": dag_hash,
        "reuse": False,
        "status": "installed",
    }:
        failures.append("Spack fresh-install check does not match the policy package identity")
    locate = checks.get("remote-mcp.spack-postinstall-locate")
    locate_expected = (
        _spack_transition_mapping(locate.get("expected")) if locate is not None else None
    )
    if locate_expected != {
        "requested_spec": f"/{dag_hash}" if dag_hash is not None else None,
        "package_name": expected.package_name,
        "dag_hash": dag_hash,
    }:
        failures.append("Spack postinstall check does not match the installed package identity")


def _bind_spack_transition_identity(
    evidence: dict[str, Any] | None,
    *,
    requirement: ReleaseGateRequirement,
    expected: ReleaseSpackFreshInstallRequirement,
    failures: list[str],
) -> None:
    """Require all phases to retain the policy server, profile, and route identity."""
    if evidence is None:
        return
    revision_matches = _spack_transition_mapping(evidence.get("revision_matches"))
    if (
        evidence.get("underlying_reports_passed") is not True
        or evidence.get("scopes") != [[requirement.cluster, expected.server_name, expected.profile]]
        or evidence.get("tool_names") != ["spack_find", "spack_install", "spack_locate"]
        or evidence.get("expected_tool_names") != ["spack_find", "spack_install", "spack_locate"]
        or revision_matches != {"registration": True, "cluster_route": True, "catalog": True}
        or evidence.get("same_server_artifact") is not True
        or not _spack_sha256(evidence.get("server_artifact_sha256"))
    ):
        failures.append("Spack transition phases do not share one verified route identity")


def _bind_spack_transition_durable_evidence(
    evidence: dict[str, Any] | None,
    *,
    phase_job_ids: list[str],
    failures: list[str],
) -> None:
    """Cross-bind ordered phase jobs to the durable-evidence assertion."""
    if evidence is None:
        return
    phases = _spack_transition_mapping(evidence.get("phases"))
    valid = (
        len(phase_job_ids) == 3
        and evidence.get("job_ids") == phase_job_ids
        and evidence.get("distinct_job_ids") is True
        and evidence.get("distinct_artifact_ids") is True
        and evidence.get("required_artifact_kinds")
        == ["mcp_result", "provenance", "stderr", "stdout"]
        and phases is not None
    )
    if valid and phases is not None:
        for phase, job_id in zip(
            ("preinstall", "install", "postinstall"), phase_job_ids, strict=True
        ):
            phase_evidence = _spack_transition_mapping(phases.get(phase))
            valid = (
                valid
                and phase_evidence is not None
                and (
                    phase_evidence.get("job_id") == job_id
                    and phase_evidence.get("state") == "succeeded"
                    and phase_evidence.get("artifacts_valid") is True
                    and phase_evidence.get("stdio_valid") is True
                    and phase_evidence.get("passed") is True
                )
            )
    if not valid:
        failures.append("Spack transition durable evidence is not bound to its ordered jobs")


def _bind_spack_disposable_store(
    evidence: dict[str, Any] | None,
    *,
    prefix: str | None,
    failures: list[str],
) -> str | None:
    """Require nonempty dynamic store/prefix fields and their producer-validated relation."""
    if evidence is None:
        return None
    raw_root = evidence.get("fresh_install_store_root")
    store_root = raw_root if isinstance(raw_root, str) and raw_root else None
    if (
        store_root is None
        or prefix is None
        or not _release_spack_canonical_absolute_path(store_root)
        or not _release_spack_canonical_absolute_path(prefix)
        or not _release_spack_strict_descendant(prefix, store_root)
        or evidence.get("observed_prefix") != prefix
        or evidence.get("root_is_canonical_absolute") is not True
        or evidence.get("prefix_is_strict_descendant") is not True
    ):
        failures.append("Spack disposable-store evidence is missing or not prefix-bound")
    return store_root


def _bind_spack_configuration_identity(
    evidence: dict[str, Any] | None,
    *,
    report: LiveValidationReport,
    requirement: ReleaseGateRequirement,
    failures: list[str],
) -> None:
    """Bind one dynamic configuration SHA/path across checks, resource, and artifact."""
    if evidence is None:
        return
    expected = _spack_transition_mapping(evidence.get("expected"))
    preinstall = _spack_transition_mapping(evidence.get("preinstall"))
    postinstall = _spack_transition_mapping(evidence.get("postinstall"))
    path = expected.get("manifest_path") if expected is not None else None
    sha256 = expected.get("configuration_sha256") if expected is not None else None
    observations_match = (
        isinstance(path, str)
        and bool(path)
        and _release_spack_canonical_absolute_path(path)
        and _spack_sha256(sha256)
        and _spack_configuration_observation_matches(preinstall, "preinstall", path, sha256)
        and _spack_configuration_observation_matches(postinstall, "postinstall", path, sha256)
        and preinstall is not None
        and postinstall is not None
        and preinstall.get("components") == postinstall.get("components")
        and evidence.get("digest_matches") is True
        and evidence.get("path_matches") is True
        and evidence.get("components_match") is True
        and evidence.get("manifest_metadata_matches") is True
        and evidence.get("phases_match") is True
    )
    if not observations_match:
        failures.append("Spack configuration observations do not share one SHA/path identity")
        return
    resources = [
        resource
        for resource in report.resources
        if resource.cluster == requirement.cluster
        and resource.kind == "configuration_manifest"
        and resource.role == "spack_fresh_install_configuration"
    ]
    if len(resources) != 1:
        failures.append("Spack transition requires exactly one configuration manifest resource")
    else:
        resource = resources[0]
        if (
            resource.state != "verified"
            or resource.resource_id != sha256
            or resource.references != [path]
            or resource.metadata.get("expected_sha256") != sha256
            or resource.metadata.get("preinstall") != preinstall
            or resource.metadata.get("postinstall") != postinstall
        ):
            failures.append("Spack configuration resource differs from transition evidence")
    artifacts = [
        artifact
        for artifact in report.artifacts
        if artifact.kind == "spack_fresh_install_configuration"
    ]
    if len(artifacts) != 1 or artifacts[0].reference != path or artifacts[0].sha256 != sha256:
        failures.append("Spack configuration artifact differs from transition evidence")


def _spack_configuration_observation_matches(
    observation: dict[str, Any] | None,
    phase: str,
    path: object,
    sha256: object,
) -> bool:
    """Validate bounded dynamic configuration fields retained in canonical evidence."""
    if observation is None:
        return False
    components = observation.get("components")
    if not isinstance(components, list) or not components:
        return False
    for raw in cast(list[object], components):
        component = _spack_transition_mapping(raw)
        if (
            component is None
            or not isinstance(component.get("relative_path"), str)
            or not component.get("relative_path")
            or not _release_spack_canonical_relative_path(component.get("relative_path"))
            or not _spack_sha256(component.get("sha256"))
            or not isinstance(component.get("size_bytes"), int)
            or isinstance(component.get("size_bytes"), bool)
            or cast(int, component["size_bytes"]) < 0
            or component.get("regular_file") is not True
        ):
            return False
    size = observation.get("manifest_size_bytes")
    return (
        observation.get("schema_version") == "clio-relay.spack-configuration-observation.v1"
        and observation.get("phase") == phase
        and observation.get("manifest_path") == path
        and observation.get("manifest_sha256") == sha256
        and isinstance(size, int)
        and not isinstance(size, bool)
        and size > 0
        and observation.get("manifest_regular_file") is True
    )


def _bind_spack_transition_artifacts(
    report: LiveValidationReport,
    *,
    requirement: ReleaseGateRequirement,
    phase_resources: dict[str, ValidationResource],
    failures: list[str],
) -> None:
    """Require four distinct hashed durable artifacts for every phase job."""
    roles = {
        "preinstall": "spack_preinstall_find",
        "install": "spack_fresh_install",
        "postinstall": "spack_postinstall_locate",
    }
    artifact_ids: list[str] = []
    for phase, base_role in roles.items():
        phase_resource = phase_resources.get(phase)
        if phase_resource is None:
            continue
        for kind in ("stdout", "stderr", "mcp_result", "provenance"):
            role = f"{base_role}_{kind}"
            matches = [
                resource
                for resource in report.resources
                if resource.cluster == requirement.cluster
                and resource.kind == "artifact"
                and resource.role == role
            ]
            if len(matches) != 1:
                failures.append(f"Spack {phase} phase requires exactly one {kind} artifact")
                continue
            artifact = matches[0]
            artifact_ids.append(artifact.resource_id)
            if (
                artifact.metadata.get("transition_phase") != phase
                or artifact.metadata.get("kind") != kind
                or artifact.metadata.get("job_id") != phase_resource.resource_id
                or not _spack_sha256(artifact.metadata.get("sha256"))
            ):
                failures.append(f"Spack {phase} {kind} artifact is not phase/job/hash bound")
    if len(artifact_ids) == 12 and len(set(artifact_ids)) != 12:
        failures.append("Spack transition artifacts do not have distinct durable identities")


def _spack_sha256(value: object) -> bool:
    """Return whether dynamic transition evidence carries one lowercase SHA-256."""
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None


def _release_spack_canonical_absolute_path(value: object) -> bool:
    """Validate dynamic POSIX paths at the release boundary after JSON projection."""
    if (
        not isinstance(value, str)
        or not value.startswith("/")
        or value.startswith("//")
        or value == "/"
        or any(ord(character) < 32 or ord(character) == 127 for character in value)
    ):
        return False
    path = PurePosixPath(value)
    return ".." not in path.parts and str(path) == value


def _release_spack_canonical_relative_path(value: object) -> bool:
    """Validate component paths retained inside dynamic configuration evidence."""
    if (
        not isinstance(value, str)
        or value.startswith("/")
        or value in {"", "."}
        or any(ord(character) < 32 or ord(character) == 127 for character in value)
    ):
        return False
    path = PurePosixPath(value)
    return ".." not in path.parts and str(path) == value


def _release_spack_strict_descendant(path: str, root: str) -> bool:
    """Independently prove the located prefix is contained by the disposable store."""
    candidate = PurePosixPath(path)
    parent = PurePosixPath(root)
    return candidate != parent and parent in candidate.parents


def _metadata_value_matches(observed: object, expected: object) -> bool:
    """Match nested metadata dictionaries as required subsets and other values exactly."""
    if isinstance(expected, dict):
        if not isinstance(observed, dict):
            return False
        typed_expected = cast(dict[object, object], expected)
        typed_observed = cast(dict[object, object], observed)
        return all(
            key in typed_observed and _metadata_value_matches(typed_observed[key], expected_value)
            for key, expected_value in typed_expected.items()
        )
    return observed == expected


def _validated_sha256(value: str | None) -> str | None:
    """Normalize and validate an independently computed SHA-256 digest."""
    if value is None:
        return None
    normalized = value.strip().lower()
    if re.fullmatch(r"[0-9a-f]{64}", normalized) is None:
        raise ConfigurationError("expected artifact SHA-256 must be 64 hexadecimal characters")
    return normalized


def _embedded_build_info() -> dict[str, Any] | None:
    try:
        content = (
            resources.files("clio_relay").joinpath("_build_info.json").read_text(encoding="utf-8")
        )
    except (FileNotFoundError, ModuleNotFoundError):
        return None
    loaded = cast(object, json.loads(content))
    return cast(dict[str, Any], loaded) if isinstance(loaded, dict) else None


def _checkout_build_info() -> dict[str, Any]:
    package_path = Path(__file__).resolve()
    for parent in package_path.parents:
        if not (parent / ".git").exists():
            continue
        commit = _git_output(parent, ["rev-parse", "HEAD"])
        tag = _git_output(parent, ["describe", "--tags", "--exact-match", "HEAD"])
        dirty_output = _git_output(parent, ["status", "--porcelain"])
        return {"commit": commit, "tag": tag, "dirty": bool(dirty_output)}
    return {}


def _git_output(root: Path, args: list[str]) -> str | None:
    completed = subprocess.run(
        ["git", "-C", str(root), *args],
        capture_output=True,
        check=False,
        text=True,
    )
    value = completed.stdout.strip()
    return value if completed.returncode == 0 and value else None


def _distribution_direct_url(distribution: metadata.Distribution) -> dict[str, Any] | None:
    content = distribution.read_text("direct_url.json")
    if content is None:
        return None
    loaded = cast(object, json.loads(content))
    if not isinstance(loaded, dict):
        return None
    value = cast(dict[str, Any], loaded)
    url = value.get("url")
    if isinstance(url, str):
        value = {**value, "url": _redact_url(url)}
    return value


def _verify_running_artifact_identity(
    distribution: metadata.Distribution,
    *,
    detected_kind: InstallSourceKind,
    direct_url: dict[str, Any] | None,
    artifact_sha256: str | None,
    launcher: str,
) -> bool:
    """Bind a claimed archive digest to the files loaded by this process."""
    if artifact_sha256 is None or not re.fullmatch(r"[0-9a-fA-F]{64}", artifact_sha256):
        return False
    expected = artifact_sha256.lower()
    if launcher not in {"uv-tool", "uvx"}:
        return False
    if detected_kind is InstallSourceKind.WHEEL:
        if _local_wheel_archive_path(direct_url) is not None:
            return _local_wheel_matches_install(distribution, direct_url, expected)
        return _wheel_url_matches_install(distribution, direct_url, expected)
    if detected_kind is InstallSourceKind.PYPI:
        return _pypi_wheel_matches_install(distribution, expected)
    return False


def _infer_running_artifact_identity(
    distribution: metadata.Distribution,
    *,
    detected_kind: InstallSourceKind,
    direct_url: dict[str, Any] | None,
    launcher: str,
) -> tuple[str | None, bool]:
    """Inspect one exact direct wheel for human-facing installation information."""
    if launcher != "uv-tool" or detected_kind is not InstallSourceKind.WHEEL:
        return None, False
    wheel_bytes = _direct_wheel_bytes(direct_url)
    if wheel_bytes is None:
        return None, False
    digest = hashlib.sha256(wheel_bytes).hexdigest()
    direct_hashes = _direct_url_sha256_hashes(direct_url)
    if direct_hashes and digest not in direct_hashes:
        return digest, False
    try:
        return digest, _installed_files_match_wheel(distribution, wheel_bytes)
    except (OSError, ValueError, zipfile.BadZipFile):
        return digest, False


def _wheel_url_matches_install(
    distribution: metadata.Distribution,
    direct_url: dict[str, Any] | None,
    expected_sha256: str,
) -> bool:
    """Verify exact local or HTTPS wheel bytes and their installed RECORD closure."""
    wheel_bytes = _direct_wheel_bytes(direct_url)
    if wheel_bytes is None:
        return False
    direct_hashes = _direct_url_sha256_hashes(direct_url)
    if direct_hashes and expected_sha256 not in direct_hashes:
        return False
    if hashlib.sha256(wheel_bytes).hexdigest() != expected_sha256:
        return False
    try:
        return _installed_files_match_wheel(distribution, wheel_bytes)
    except (OSError, ValueError, zipfile.BadZipFile):
        return False


def _direct_wheel_bytes(direct_url: dict[str, Any] | None) -> bytes | None:
    """Read one bounded wheel from its exact local-file or clean HTTPS URL."""
    if direct_url is None:
        return None
    raw_url = direct_url.get("url")
    if not isinstance(raw_url, str):
        return None
    parsed = urllib.parse.urlsplit(raw_url)
    if not parsed.path.casefold().endswith(".whl"):
        return None
    if parsed.scheme.casefold() == "file":
        path = _local_wheel_archive_path(direct_url)
        if path is None:
            return None
        identity = _regular_file_identity(path)
        if identity is None or identity[2] > MAX_DISTRIBUTION_WHEEL_BYTES:
            return None
        return _read_open_regular_file(
            path,
            identity,
            maximum_bytes=MAX_DISTRIBUTION_WHEEL_BYTES,
        )
    if not _is_official_release_wheel_url(raw_url) or not _url_host_resolves_publicly(raw_url):
        return None
    try:
        opener = urllib.request.build_opener(_ReleaseWheelRedirectHandler())
        with opener.open(raw_url, timeout=60) as response:  # noqa: S310
            final_url = urllib.parse.urlsplit(str(response.geturl()))
            final_url_text = urllib.parse.urlunsplit(final_url)
            if not (
                _is_official_release_wheel_url(final_url_text)
                or _is_github_release_asset_url(final_url_text)
            ):
                return None
            content = response.read(MAX_DISTRIBUTION_WHEEL_BYTES + 1)
    except (OSError, ValueError, urllib.error.HTTPError):
        return None
    return content if len(content) <= MAX_DISTRIBUTION_WHEEL_BYTES else None


class _ReleaseWheelRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Reject a release download redirect before it can reach an unsafe host."""

    def redirect_request(
        self,
        req: urllib.request.Request,
        fp: Any,
        code: int,
        msg: str,
        headers: Any,
        newurl: str,
    ) -> urllib.request.Request | None:
        if not _is_github_release_asset_url(newurl) or not _url_host_resolves_publicly(newurl):
            raise urllib.error.HTTPError(
                newurl,
                403,
                "unsafe wheel download redirect",
                headers,
                fp,
            )
        return super().redirect_request(
            req,
            fp,
            code,
            msg,
            headers,
            newurl,
        )


def _is_official_release_wheel_url(value: str) -> bool:
    """Allow only one credential-free canonical clio-relay GitHub release URL."""
    try:
        parsed = urllib.parse.urlsplit(value)
        port = parsed.port
    except ValueError:
        return False
    return (
        parsed.scheme.casefold() == "https"
        and parsed.hostname == "github.com"
        and port in {None, 443}
        and parsed.username is None
        and parsed.password is None
        and not parsed.query
        and not parsed.fragment
        and _OFFICIAL_RELEASE_WHEEL_PATH.fullmatch(parsed.path) is not None
    )


def _is_github_release_asset_url(value: str) -> bool:
    """Allow only GitHub's credential-free HTTPS release-asset redirect target."""
    try:
        parsed = urllib.parse.urlsplit(value)
        port = parsed.port
    except ValueError:
        return False
    return (
        parsed.scheme.casefold() == "https"
        and parsed.hostname == "release-assets.githubusercontent.com"
        and port in {None, 443}
        and parsed.username is None
        and parsed.password is None
        and not parsed.fragment
        and parsed.path.startswith("/github-production-release-asset/")
    )


def _url_host_resolves_publicly(value: str) -> bool:
    """Fail closed unless every resolved address for one HTTPS URL is globally routable."""
    try:
        parsed = urllib.parse.urlsplit(value)
        hostname = parsed.hostname
        if parsed.scheme.casefold() != "https" or hostname is None:
            return False
        answers = socket.getaddrinfo(hostname, parsed.port or 443, type=socket.SOCK_STREAM)
        addresses = {
            str(answer[4][0]).split("%", maxsplit=1)[0]
            for answer in answers
            if answer[0] in {socket.AF_INET, socket.AF_INET6}
        }
        return bool(addresses) and all(
            ipaddress.ip_address(address).is_global for address in addresses
        )
    except (OSError, ValueError):
        return False


def _local_wheel_matches_install(
    distribution: metadata.Distribution,
    direct_url: dict[str, Any] | None,
    expected_sha256: str,
) -> bool:
    """Verify a local wheel archive and the installed files derived from its RECORD."""
    if _local_wheel_archive_path(direct_url) is None:
        return False
    return _wheel_url_matches_install(distribution, direct_url, expected_sha256)


def _local_wheel_archive_path(direct_url: dict[str, Any] | None) -> Path | None:
    """Resolve only an explicit local-file wheel reference from PEP 610 metadata."""
    if direct_url is None:
        return None
    raw_url = direct_url.get("url")
    if not isinstance(raw_url, str):
        return None
    parsed = urllib.parse.urlsplit(raw_url)
    if parsed.scheme.casefold() != "file" or parsed.query or parsed.fragment:
        return None
    if parsed.netloc not in {"", "localhost"}:
        return None
    decoded = urllib.parse.unquote(parsed.path)
    if os.name == "nt" and re.fullmatch(r"/[A-Za-z]:/.*", decoded):
        decoded = decoded[1:]
    return Path(decoded)


def _direct_url_sha256_hashes(direct_url: dict[str, Any] | None) -> set[str]:
    if direct_url is None:
        return set()
    archive_info = direct_url.get("archive_info")
    if not isinstance(archive_info, dict):
        return set()
    typed = cast(dict[str, Any], archive_info)
    values: list[object] = []
    if "hash" in typed:
        values.append(typed["hash"])
    hashes = typed.get("hashes")
    if isinstance(hashes, dict):
        sha256_value = cast(dict[object, object], hashes).get("sha256")
        values.append(f"sha256={sha256_value}" if isinstance(sha256_value, str) else None)
    verified: set[str] = set()
    for value in values:
        if not isinstance(value, str):
            continue
        algorithm, separator, digest = value.partition("=")
        if algorithm.lower() == "sha256" and separator and re.fullmatch(r"[0-9a-fA-F]{64}", digest):
            verified.add(digest.lower())
    return verified


def _pypi_wheel_matches_install(
    distribution: metadata.Distribution,
    expected_sha256: str,
) -> bool:
    """Verify installed files against the exact official PyPI wheel digest."""
    try:
        with urllib.request.urlopen(  # noqa: S310 - fixed HTTPS PyPI endpoint
            f"https://pypi.org/pypi/clio-relay/{urllib.parse.quote(distribution.version)}/json",
            timeout=30,
        ) as response:
            content = response.read(4 * 1024 * 1024 + 1)
        if len(content) > 4 * 1024 * 1024:
            return False
        payload = cast(object, json.loads(content))
        payload_mapping = cast(dict[object, object], payload) if isinstance(payload, dict) else {}
        urls = payload_mapping.get("urls")
        if not isinstance(urls, list):
            return False
        wheel_url: str | None = None
        for item in cast(list[object], urls):
            if not isinstance(item, dict):
                continue
            record = cast(dict[str, Any], item)
            digests = record.get("digests")
            digest = (
                cast(dict[str, Any], digests).get("sha256") if isinstance(digests, dict) else None
            )
            url = record.get("url")
            if (
                record.get("packagetype") == "bdist_wheel"
                and isinstance(digest, str)
                and digest.lower() == expected_sha256
                and isinstance(url, str)
                and url.startswith("https://files.pythonhosted.org/")
            ):
                wheel_url = url
                break
        if wheel_url is None:
            return False
        with urllib.request.urlopen(  # noqa: S310 - URL constrained above
            wheel_url,
            timeout=60,
        ) as response:
            wheel_bytes = response.read(128 * 1024 * 1024 + 1)
        if len(wheel_bytes) > 128 * 1024 * 1024:
            return False
        if hashlib.sha256(wheel_bytes).hexdigest() != expected_sha256:
            return False
        return _installed_files_match_wheel(distribution, wheel_bytes)
    except (OSError, ValueError, json.JSONDecodeError, zipfile.BadZipFile):
        return False


def _installed_files_match_wheel(
    distribution: metadata.Distribution,
    wheel_bytes: bytes,
) -> bool:
    with zipfile.ZipFile(io.BytesIO(wheel_bytes)) as archive:
        archive_names = [item.filename for item in archive.infolist() if not item.is_dir()]
        if len(archive_names) != len(set(archive_names)):
            return False
        record_names = [name for name in archive_names if name.endswith(".dist-info/RECORD")]
        if len(record_names) != 1:
            return False
        rows = csv.reader(io.StringIO(archive.read(record_names[0]).decode("utf-8")))
        records: dict[str, tuple[str, str]] = {}
        for row in rows:
            if len(row) != 3 or not _safe_wheel_member(row[0]) or row[0] in records:
                return False
            records[row[0]] = (row[1], row[2])
        if set(records) != set(archive_names):
            return False
        checked = 0
        for wheel_path, (hash_field, size_field) in records.items():
            if wheel_path == record_names[0]:
                if hash_field or size_field:
                    return False
                continue
            algorithm, separator, encoded_digest = hash_field.partition("=")
            if algorithm != "sha256" or not separator or not size_field.isdecimal():
                return False
            try:
                wheel_content = archive.read(wheel_path)
                installed_content = Path(str(distribution.locate_file(wheel_path))).read_bytes()
                expected = base64.b64decode(
                    encoded_digest + "=" * (-len(encoded_digest) % 4),
                    altchars=b"-_",
                    validate=True,
                )
            except (KeyError, OSError, ValueError, binascii.Error):
                return False
            if len(expected) != hashlib.sha256().digest_size:
                return False
            expected_size = int(size_field)
            if len(wheel_content) != expected_size or len(installed_content) != expected_size:
                return False
            if hashlib.sha256(wheel_content).digest() != expected:
                return False
            if hashlib.sha256(installed_content).digest() != expected:
                return False
            checked += 1
        return checked > 0


def _safe_wheel_member(value: str) -> bool:
    """Reject absolute, platform-ambiguous, or traversing wheel member names."""
    if not value or "\\" in value:
        return False
    segments = value.split("/")
    if any(part in {"", ".", ".."} for part in segments):
        return False
    path = PurePosixPath(value)
    return not path.is_absolute()


def _classify_install_source(
    direct_url: dict[str, Any] | None,
) -> tuple[InstallSourceKind, str | None]:
    if direct_url is None:
        return InstallSourceKind.PYPI, f"clio-relay=={__version__}"
    url = _optional_string(direct_url.get("url"))
    directory_info = direct_url.get("dir_info")
    if (
        isinstance(directory_info, dict)
        and cast(dict[str, Any], directory_info).get("editable") is True
    ):
        return InstallSourceKind.EDITABLE, url
    if isinstance(direct_url.get("vcs_info"), dict):
        return InstallSourceKind.VCS, url
    if url is not None and url.lower().endswith(".whl"):
        return InstallSourceKind.WHEEL, url
    if url is not None and url.startswith("file:"):
        return InstallSourceKind.CHECKOUT, url
    return InstallSourceKind.UNKNOWN, url


def _is_official_github_release_wheel(
    direct_url: dict[str, Any] | None,
    distribution_version: str,
) -> bool:
    """Recognize the canonical clio-relay wheel URL for one GitHub release."""
    if direct_url is None:
        return False
    value = direct_url.get("url")
    if not isinstance(value, str):
        return False
    expected = (
        "https://github.com/iowarp/clio-relay/releases/download/"
        f"v{distribution_version}/clio_relay-{distribution_version}-py3-none-any.whl"
    )
    return value == expected


def _parse_source_override(value: str) -> tuple[InstallSourceKind, str | None]:
    kind_value, separator, reference = value.partition(":")
    try:
        kind = InstallSourceKind(kind_value)
    except ValueError as exc:
        allowed = ", ".join(item.value for item in InstallSourceKind)
        raise ConfigurationError(f"install source must begin with one of: {allowed}") from exc
    return kind, reference if separator and reference else None


def _optional_string(value: object) -> str | None:
    return value if isinstance(value, str) and value else None


def _optional_bool(value: object) -> bool | None:
    return value if isinstance(value, bool) else None


def _sensitive_key(key: object) -> bool:
    if not isinstance(key, str):
        return False
    normalized = key.strip().casefold().replace("-", "_").replace(".", "_")
    if normalized in {
        "authorization",
        "credential",
        "credentials",
        "password",
        "private_key",
        "secret",
        "secret_key",
        "token",
    }:
        return True
    return normalized.endswith(
        (
            "_authorization",
            "_credential",
            "_credentials",
            "_password",
            "_private_key",
            "_secret",
            "_secret_key",
            "_token",
        )
    )


def _collect_sensitive_values(value: object, output: set[str]) -> None:
    if isinstance(value, dict):
        mapping = cast(dict[object, object], value)
        for key, nested in mapping.items():
            if _sensitive_key(key) and isinstance(nested, str) and nested:
                output.add(nested)
            else:
                _collect_sensitive_values(nested, output)
        return
    if isinstance(value, list):
        for nested in cast(list[object], value):
            _collect_sensitive_values(nested, output)
        return
    if isinstance(value, tuple):
        for nested in cast(tuple[object, ...], value):
            _collect_sensitive_values(nested, output)


def _redact_sensitive_value(value: object, sensitive_values: set[str]) -> object:
    if isinstance(value, dict):
        mapping = cast(dict[object, object], value)
        return {
            str(key): (
                "<redacted>"
                if _sensitive_key(key)
                else _redact_sensitive_value(nested, sensitive_values)
            )
            for key, nested in mapping.items()
        }
    if isinstance(value, list):
        return [
            _redact_sensitive_value(nested, sensitive_values)
            for nested in cast(list[object], value)
        ]
    if isinstance(value, tuple):
        return [
            _redact_sensitive_value(nested, sensitive_values)
            for nested in cast(tuple[object, ...], value)
        ]
    if isinstance(value, str):
        redacted = value
        for sensitive in sorted(sensitive_values, key=len, reverse=True):
            redacted = redacted.replace(sensitive, "<redacted>")
        return redacted
    return value


def _redacted_invocation(arguments: list[str]) -> list[str]:
    sensitive = {
        "--api-token",
        "--password",
        "--secret",
        "--token",
        "--transport-secret-key",
        "--transport-token",
    }
    redacted: list[str] = []
    hide_next = False
    for argument in arguments:
        if hide_next:
            redacted.append("<redacted>")
            hide_next = False
            continue
        flag, separator, _value = argument.partition("=")
        if flag in sensitive:
            redacted.append(f"{flag}=<redacted>" if separator else flag)
            hide_next = not separator
            continue
        redacted.append(argument)
    return redacted


def _redact_url(value: str) -> str:
    parsed = urllib.parse.urlsplit(value)
    if parsed.scheme not in {"http", "https", "ssh", "git"}:
        return value
    hostname = parsed.hostname or ""
    netloc = hostname
    if parsed.port is not None:
        netloc = f"{hostname}:{parsed.port}"
    return urllib.parse.urlunsplit((parsed.scheme, netloc, parsed.path, "", ""))


_SUCCESS_FACT_VALUES = frozenset(
    {
        "completed",
        "ok",
        "observed",
        "passed",
        "stopped",
        "succeeded",
        "verified",
    }
)
_FAILED_OR_UNKNOWN_FACT_VALUES = frozenset(
    {
        "",
        "detached",
        "error",
        "failed",
        "false",
        "frp_stcp",
        "none",
        "not_started",
        "refused",
        "residual",
        "unknown",
        "unverified",
    }
)
_TRANSPORT_SUCCESS_FACTS = {
    "transport.cleanup": {"passed"},
    "transport.healthz": {"ok"},
    "transport.http_artifacts": {"ok"},
    "transport.http_events": {"ok"},
    "transport.http_provenance": {"ok"},
    "transport.http_wait": {"succeeded"},
    "transport.remote_cleanup": {"passed"},
    "transport.remote_session_ownership": {"verified"},
}
_WORKER_IDENTITY_FACTS = frozenset(
    {
        "worker.artifact-sha256",
        "worker.artifact-version",
        "worker.component-artifacts",
        "worker.component-runtime",
        "worker.components",
        "worker.scheduler-provider",
        "worker.source-identity",
    }
)
_ACCEPTANCE_EXACT_FACTS = frozenset(
    {
        "acceptance.agent_child_job_id",
        "acceptance.agent_job_id",
        "acceptance.agent_prompt",
        "acceptance.agent_state",
        "acceptance.application_boundary",
        "acceptance.cluster_doctor",
        "acceptance.job_id",
        "acceptance.job_state",
        "acceptance.live_progress_adapter",
        "acceptance.monitor",
        "acceptance.package_adapter",
        "acceptance.package_owner",
        "acceptance.pipeline",
        "acceptance.progress",
    }
)
_ACCEPTANCE_VERIFIED_SUFFIXES = (
    ".artifact_read",
    ".artifacts",
    ".events",
    ".progress_adapter",
    ".provenance",
    ".runtime_metadata_artifact",
    ".runtime_metadata_source",
    ".runtime_scheduler_job_id",
    ".runtime_scheduler_job_id_source",
    ".runtime_scheduler_provider",
    ".stderr_bytes",
    ".stdout_bytes",
    ".structured_runtime_metadata",
    ".structured_runtime_scheduler_identity",
    ".tasks",
)


def _line_proves_success(key: str, value: str) -> bool:
    """Return whether a legacy text fact explicitly proves a successful check."""
    normalized = value.strip().lower()
    if normalized in _FAILED_OR_UNKNOWN_FACT_VALUES:
        return False
    if key.startswith("transport."):
        return normalized in _TRANSPORT_SUCCESS_FACTS.get(key, set())
    if key.startswith("direct_transport."):
        return False
    if key.startswith("scheduler."):
        return normalized in _SUCCESS_FACT_VALUES
    if key.startswith("cluster."):
        return normalized in _SUCCESS_FACT_VALUES
    if key.startswith("package-progress."):
        return normalized in _SUCCESS_FACT_VALUES or (
            key == "package-progress.identity" and bool(value.strip())
        )
    if key.startswith("worker."):
        if key in _WORKER_IDENTITY_FACTS:
            if key == "worker.source-identity" and "none" in normalized.split(":"):
                return False
            return bool(value.strip())
        return normalized in _SUCCESS_FACT_VALUES
    if key.startswith("acceptance."):
        if key not in _ACCEPTANCE_EXACT_FACTS and not key.endswith(_ACCEPTANCE_VERIFIED_SUFFIXES):
            return False
        if key.endswith(("job_state", ".job_state", "_state", ".state")):
            return normalized == "succeeded"
        if key in {
            "acceptance.agent_child_job_id",
            "acceptance.agent_job_id",
            "acceptance.job_id",
        }:
            return value.startswith("job_")
        # Acceptance code emits these facts only after validating the referenced
        # record, count, artifact, or package-owned identity. Negative sentinels
        # were rejected above; arbitrary non-acceptance prefixes never enter here.
        return bool(value.strip())
    return False


def _acceptance_scope(key: str) -> str:
    if ".runtime_" in key:
        return key.split(".runtime_", 1)[0]
    for suffix in (
        "_job_id",
        ".job_id",
        "_job_state",
        ".job_state",
        "_state",
        ".stdout_bytes",
        ".stderr_bytes",
        ".artifacts",
    ):
        if key.endswith(suffix):
            return key.removesuffix(suffix).rstrip(".")
    return key.rsplit(".", 1)[0]


def _atomic_write_text(path: Path, text: str) -> None:
    logical_path = logical_filesystem_path(path)
    storage_path = internal_filesystem_path(logical_path, force_extended=True)
    storage_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = storage_path.with_name(f".{storage_path.name}.{uuid4().hex}.tmp")
    try:
        with temporary.open("w", encoding="utf-8", newline="\n") as stream:
            stream.write(text)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, storage_path)
    finally:
        temporary.unlink(missing_ok=True)

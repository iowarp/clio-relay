"""Wire schema for validation reports and release-gate policy (#231).

Extracted from :mod:`clio_relay.validation_report`
(``docs/design/relay-architecture-2026-08.md``, #231). This module owns the
data-model concern: the ``pydantic.BaseModel``/``StrEnum`` types that define
the exact JSON shape of a live validation report
(:class:`LiveValidationReport` and everything it nests -- evidence trust,
software/install-source identity, resources, checks, cleanup), a release
policy (:class:`ReleaseGatePolicy` and its requirements), and the result of
evaluating one (:class:`ReleaseGateResult`), plus the paired
serialize/parse functions for the bounded structured transport-probe cleanup
evidence line (:func:`transport_probe_evidence_line` /
:func:`parse_transport_probe_evidence`) that sits directly on
:class:`TransportProbeEvidence`'s shape.

The logic that *constructs*, *records into*, *evaluates*, or *durably
writes* these types stays in their own owner modules (recorder, gate
evaluation, install-source detection, the durable validation directory) and
imports the types from here. Two validators below reach back for a small
classification helper: :func:`~clio_relay.artifact_identity_verification.
is_official_github_release_wheel` (its real owner) and
``_normalized_hostname`` (not yet extracted to its own owner module, still
:mod:`clio_relay.validation_report`); both imports are function-scoped to
avoid a load-order circular import (the proven idiom -- see the module
docstring precedent in :mod:`clio_relay.session_wire_models`), and each gets
re-pointed at its real owner module as that concern is extracted.
"""

from __future__ import annotations

import json
import math
import re
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path, PurePosixPath
from typing import Any, Literal, cast
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, PrivateAttr, ValidationError, model_validator

from clio_relay.errors import ConfigurationError
from clio_relay.identifiers import DurableRecordId
from clio_relay.validation_limits import (
    MAX_TRANSPORT_PROBE_EVIDENCE_BYTES,
    MAX_TRANSPORT_PROBE_JSON_DEPTH,
    MAX_TRANSPORT_PROBE_JSON_NODES,
    MAX_TRANSPORT_PROBE_RESOURCES,
    TRANSPORT_PROBE_EVIDENCE_KEY,
)

REPORT_SCHEMA_VERSION = "1.0"
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


class ValidationStatus(StrEnum):
    """Outcome status for a report or one validation check."""

    PASSED = "passed"
    FAILED = "failed"
    PENDING = "pending"


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
        from clio_relay.artifact_identity_verification import is_official_github_release_wheel

        released_source = self.kind is InstallSourceKind.PYPI or (
            self.kind is InstallSourceKind.WHEEL
            and is_official_github_release_wheel(self.direct_url, self.distribution_version)
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
    started_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
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
        from clio_relay.validation_report import _normalized_hostname

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

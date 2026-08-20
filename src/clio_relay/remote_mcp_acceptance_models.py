"""Wire models for remote MCP release-acceptance evidence.

Extracted from :mod:`clio_relay.remote_mcp` (iowarp/clio-relay#231; see
``docs/design/relay-architecture-2026-08.md`` §4.5/§5, the
``RemoteMcpAcceptanceReport`` row). This module owns the bounded evidence
contracts one virtual remote MCP acceptance call produces and validates
against: catalog-withholding reasons (:class:`RemoteMcpCatalogIssue`),
individual release-validation assertions (:class:`RemoteMcpAcceptanceCheck`),
operator-declared structured-result expectations for Spack tools
(:class:`RemoteMcpStructuredResultExpectation`), the ordered
disposable-store Spack fresh-install evidence cluster
(:class:`RemoteMcpSpackTransitionArtifactEvidence` through
:class:`RemoteMcpSpackInstallTransitionEvidence`), and the top-level
:class:`RemoteMcpAcceptanceReport` -- including its
:meth:`RemoteMcpAcceptanceReport.to_live_validation_report` conversion to
the canonical release-evidence schema.

The functions that *build* these reports (``build_remote_mcp_acceptance_report``,
the 22 Spack validator functions, the 4 scientific-catalog validator
functions) stay in ``remote_mcp.py`` for now -- doc §4.5 names that
validator cluster as needing its own reordering, a separate future slice,
not a contiguous cut alongside these models. ``remote_mcp.py`` imports the
two canonical path-canonicalization primitives back from here
(:func:`_is_canonical_absolute_posix_path`,
:func:`_is_canonical_relative_posix_path`) since its own validator
functions call them too.

``CLIO_KIT_SPACK_USER_CONTRACT_ID_V2_3`` is imported at function scope
inside :meth:`RemoteMcpStructuredResultExpectation.validate_operation_fields`
rather than at module scope -- a module-scope import back into
``remote_mcp.py`` (which imports this module for the re-export below) would
be a load-order circular import; deferring it to call time is the proven
idiom for that shape (design doc §4.6's "moved symbols" guidance).

``remote_mcp.py`` re-exports every model class it still references
directly under its original name via a plain ``from ... import``, and one
of the four bound Spack-configuration constants the same way
(``MAX_REMOTE_MCP_TRANSITION_ARTIFACTS_PER_CALL``, still read by the Spack
transition report builders there). The other three constants have no
reader left in remote_mcp.py's own body -- only ``cli.py`` imports them
directly -- so remote_mcp.py re-exports them via qualified assignment
(``X = remote_mcp_acceptance_models.X``) instead: ruff's unused-import
check has no equivalent for a plain module-level assignment, unlike the
``from ... import`` it kept stripping as dead once the last body reference
moved here. :class:`RemoteMcpSpackConfigurationComponentObservation` has no
importer anywhere (confirmed by ruff F401 and grep), so it alone is not
re-exported at all -- the same judgment call R8(iii)'s ``RemoteSession``
precedent made. ``_acceptance_artifact_resource`` and
``_append_spack_transition_resources`` are private with no callers outside
``RemoteMcpAcceptanceReport.to_live_validation_report`` itself (confirmed by
grep before the move), so they are not re-exported either.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import PurePosixPath
from typing import TYPE_CHECKING, Any, Literal, cast

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

if TYPE_CHECKING:
    from clio_relay.validation_report import LiveValidationReport, ValidationResource

JSON = dict[str, Any]

MAX_REMOTE_MCP_TRANSITION_ARTIFACTS_PER_CALL = 64
MAX_REMOTE_MCP_SPACK_CONFIGURATION_COMPONENTS = 64
MAX_REMOTE_MCP_SPACK_CONFIGURATION_COMPONENT_BYTES = 16 * 1024 * 1024
MAX_REMOTE_MCP_SPACK_CONFIGURATION_MANIFEST_BYTES = 64 * 1024


class RemoteMcpCatalogIssue(BaseModel):
    """Reason a registered or discovered remote capability is not exposed.

    ``enforcement`` (clio-relay#242 dev-mode course correction) marks a
    version/sha/contract-grounded issue that dev mode chose to LOG AND
    PROCEED past rather than withhold the capability for --
    ``"deferred_dev_mode"`` means this registration/tool IS in
    ``VirtualRemoteMcpCatalog.tools`` despite the reason recorded here; the
    default ``"enforced"`` means the capability really is withheld, exactly
    as every issue meant before this field existed.
    """

    model_config = ConfigDict(extra="forbid")

    cluster: str
    server_name: str
    reason: str
    tool_name: str | None = None
    enforcement: Literal["enforced", "deferred_dev_mode"] = "enforced"


class RemoteMcpAcceptanceCheck(BaseModel):
    """One canonical remote MCP release-validation assertion."""

    model_config = ConfigDict(extra="forbid")

    name: str
    passed: bool
    message: str
    evidence: JSON = Field(default_factory=dict)


class RemoteMcpStructuredResultExpectation(BaseModel):
    """Operator-supplied semantic expectations for one structured MCP result."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["clio-relay.remote-mcp-result-expectation.v1"] = (
        "clio-relay.remote-mcp-result-expectation.v1"
    )
    contract: Literal[
        "clio-kit-spack-user-v2.1", "clio-kit-spack-user-v2", "clio-kit-spack-user-v2.3"
    ]
    tool: Literal["spack_find", "spack_locate", "spack_install"]
    package_name: str = Field(min_length=1, max_length=255, pattern=r"^[A-Za-z0-9_.+-]+$")
    dag_hash: str = Field(pattern=r"^[a-z0-9]{32}$")
    requested_spec: str | None = Field(default=None, min_length=1, max_length=4_096)
    prefix: str | None = Field(default=None, min_length=2, max_length=4_096)
    reuse: bool | None = None
    fresh_install_store_root: str | None = Field(default=None, min_length=2, max_length=4_096)
    fresh_install_configuration_sha256: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{64}$",
    )
    fresh_install_configuration_manifest_path: str | None = Field(
        default=None,
        min_length=2,
        max_length=4_096,
    )

    @model_validator(mode="after")
    def validate_operation_fields(self) -> RemoteMcpStructuredResultExpectation:
        """Require only the operation-specific expectations used by the contract."""
        from clio_relay.remote_mcp import CLIO_KIT_SPACK_USER_CONTRACT_ID_V2_3

        if self.contract == CLIO_KIT_SPACK_USER_CONTRACT_ID_V2_3 and self.tool == "spack_install":
            # v2.3 revised spack_install's outputSchema (a single "package" object
            # plus "prefix"/"load_spec"/"log_path"/"log_tail" replacing the v2.1
            # "packages" array); _validate_spack_install_result still assumes the
            # v2.1 shape. Fail closed with a typed reason rather than silently
            # evaluating the wrong schema -- porting install-result verification
            # to v2.3 is tracked separately, out of this contract-recognition fix.
            raise ValueError(
                "structured-result expectations for spack_install under "
                "clio-kit-spack-user-v2.3 are not yet supported (v2.3 revised the "
                "install outputSchema); use spack_find or spack_locate under v2.3, "
                "or declare the v2.1/v2 contract for install verification"
            )
        if self.tool == "spack_find":
            if (
                self.requested_spec is not None
                or self.prefix is not None
                or self.reuse is not None
                or self.fresh_install_store_root is not None
                or self.fresh_install_configuration_sha256 is not None
                or self.fresh_install_configuration_manifest_path is not None
            ):
                raise ValueError(
                    "spack_find must not declare requested_spec, prefix, reuse, "
                    "or fresh-install configuration expectations"
                )
            return self
        if self.requested_spec is None:
            raise ValueError(f"{self.tool} requires requested_spec")
        if self.tool == "spack_locate":
            if (
                self.reuse is not None
                or self.fresh_install_store_root is not None
                or self.fresh_install_configuration_sha256 is not None
                or self.fresh_install_configuration_manifest_path is not None
            ):
                raise ValueError("spack_locate must not declare reuse or fresh_install_store_root")
            if not _is_canonical_absolute_posix_path(self.prefix):
                raise ValueError("spack_locate requires a canonical absolute POSIX prefix")
        if self.tool == "spack_install":
            if self.prefix is not None:
                raise ValueError("spack_install must not declare prefix")
            if self.reuse is None:
                raise ValueError("spack_install requires reuse")
            configuration_fields = (
                self.fresh_install_store_root,
                self.fresh_install_configuration_sha256,
                self.fresh_install_configuration_manifest_path,
            )
            if any(value is not None for value in configuration_fields):
                if not all(value is not None for value in configuration_fields):
                    raise ValueError(
                        "fresh install requires store root, configuration SHA-256, and "
                        "configuration manifest path together"
                    )
                if self.reuse is not False:
                    raise ValueError("fresh_install_store_root requires spack_install reuse=false")
                if not _is_canonical_absolute_posix_path(self.fresh_install_store_root):
                    raise ValueError(
                        "fresh_install_store_root must be a canonical absolute POSIX path"
                    )
                if not _is_canonical_absolute_posix_path(
                    self.fresh_install_configuration_manifest_path
                ):
                    raise ValueError(
                        "fresh_install_configuration_manifest_path must be a canonical "
                        "absolute POSIX path"
                    )
        return self


class RemoteMcpSpackTransitionArtifactEvidence(BaseModel):
    """Bounded identity for one durable artifact in a Spack transition call."""

    model_config = ConfigDict(extra="forbid")

    artifact_id: str | None = Field(default=None, max_length=1_024)
    job_id: str | None = Field(default=None, max_length=1_024)
    kind: str | None = Field(default=None, max_length=128)
    sha256: str | None = Field(default=None, max_length=64)
    uri: str | None = Field(default=None, max_length=4_096)


class RemoteMcpSpackTransitionStdioEvidence(BaseModel):
    """Bounded packaged-stdio proof associated with one transition call."""

    model_config = ConfigDict(extra="forbid")

    boundary: str | None = Field(default=None, max_length=128)
    returncode: int | None = None
    initialize_passed: bool
    tools_list_passed: bool
    call_job_id: str | None = Field(default=None, max_length=1_024)


class RemoteMcpSpackConfigurationComponentObservation(BaseModel):
    """One regular file bound into an observed fresh-install configuration."""

    model_config = ConfigDict(extra="forbid")

    relative_path: str = Field(min_length=1, max_length=1_024)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    size_bytes: int = Field(
        ge=0,
        le=MAX_REMOTE_MCP_SPACK_CONFIGURATION_COMPONENT_BYTES,
    )
    regular_file: Literal[True] = True

    @field_validator("relative_path")
    @classmethod
    def validate_relative_path(cls, value: str) -> str:
        """Reject absolute, traversing, or non-canonical component paths."""
        if not _is_canonical_relative_posix_path(value):
            raise ValueError("configuration component path must be canonical and relative")
        return value


class RemoteMcpSpackConfigurationObservation(BaseModel):
    """Independent digest observation of one bounded configuration manifest."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["clio-relay.spack-configuration-observation.v1"] = (
        "clio-relay.spack-configuration-observation.v1"
    )
    phase: Literal["preinstall", "postinstall"]
    manifest_path: str = Field(min_length=2, max_length=4_096)
    manifest_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    manifest_size_bytes: int = Field(
        ge=1,
        le=MAX_REMOTE_MCP_SPACK_CONFIGURATION_MANIFEST_BYTES,
    )
    manifest_regular_file: Literal[True] = True
    components: list[RemoteMcpSpackConfigurationComponentObservation] = Field(
        min_length=1,
        max_length=MAX_REMOTE_MCP_SPACK_CONFIGURATION_COMPONENTS,
    )

    @model_validator(mode="after")
    def validate_manifest(self) -> RemoteMcpSpackConfigurationObservation:
        """Require an absolute manifest and one canonical, sorted component set."""
        if not _is_canonical_absolute_posix_path(self.manifest_path):
            raise ValueError("configuration manifest path must be canonical and absolute")
        paths = [component.relative_path for component in self.components]
        if paths != sorted(paths) or len(paths) != len(set(paths)):
            raise ValueError("configuration component paths must be unique and sorted")
        return self


class RemoteMcpSpackTransitionCallEvidence(BaseModel):
    """Bounded durable call evidence for one phase of a fresh Spack install."""

    model_config = ConfigDict(extra="forbid")

    phase: Literal["preinstall", "install", "postinstall"]
    report_passed: bool
    cluster: str = Field(min_length=1, max_length=255)
    server_name: str = Field(min_length=1, max_length=255)
    profile: str = Field(min_length=1, max_length=64)
    remote_tool_name: str = Field(min_length=1, max_length=64)
    virtual_alias: str | None = Field(default=None, max_length=64)
    job_id: str | None = Field(default=None, max_length=1_024)
    state: str | None = Field(default=None, max_length=128)
    arguments: JSON = Field(default_factory=dict)
    artifacts: list[RemoteMcpSpackTransitionArtifactEvidence] = Field(
        default_factory=lambda: list[RemoteMcpSpackTransitionArtifactEvidence](),
        max_length=MAX_REMOTE_MCP_TRANSITION_ARTIFACTS_PER_CALL,
    )
    artifacts_truncated: bool = False
    stdio: RemoteMcpSpackTransitionStdioEvidence
    structured_result: JSON = Field(default_factory=dict)


class RemoteMcpSpackInstallTransitionEvidence(BaseModel):
    """Ordered, machine-readable evidence for a disposable-store Spack install."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["clio-relay.spack-fresh-install-transition.v1"] = (
        "clio-relay.spack-fresh-install-transition.v1"
    )
    cluster: str = Field(min_length=1, max_length=255)
    server_name: str = Field(min_length=1, max_length=255)
    profile: str = Field(min_length=1, max_length=64)
    requested_spec: str = Field(min_length=1, max_length=4_096)
    package_name: str = Field(min_length=1, max_length=255)
    dag_hash: str = Field(pattern=r"^[a-z0-9]{32}$")
    fresh_install_store_root: str = Field(min_length=2, max_length=4_096)
    fresh_install_configuration_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    fresh_install_configuration_manifest_path: str = Field(min_length=2, max_length=4_096)
    preinstall_configuration: RemoteMcpSpackConfigurationObservation
    postinstall_configuration: RemoteMcpSpackConfigurationObservation
    executed_spack_command_path: str | None = Field(default=None, max_length=4_096)
    executed_spack_command_relative_path: str | None = Field(default=None, max_length=1_024)
    executed_spack_command_sha256: str | None = Field(
        default=None,
        max_length=64,
    )
    executed_spack_command_size_bytes: int | None = Field(
        default=None,
        ge=0,
        le=MAX_REMOTE_MCP_SPACK_CONFIGURATION_COMPONENT_BYTES,
    )
    registration_revision: str | None = Field(default=None, max_length=128)
    cluster_route_revision: str | None = Field(default=None, max_length=128)
    catalog_revision: str | None = Field(default=None, max_length=128)
    server_artifact_sha256: str | None = Field(default=None, max_length=64)
    preinstall: RemoteMcpSpackTransitionCallEvidence
    install: RemoteMcpSpackTransitionCallEvidence
    postinstall: RemoteMcpSpackTransitionCallEvidence

    @model_validator(mode="after")
    def validate_transition_shape(self) -> RemoteMcpSpackInstallTransitionEvidence:
        """Reject forged phase labels or an unsafe disposable-store root."""
        if not _is_canonical_absolute_posix_path(self.fresh_install_store_root):
            raise ValueError("fresh_install_store_root must be a canonical absolute POSIX path")
        if not _is_canonical_absolute_posix_path(self.fresh_install_configuration_manifest_path):
            raise ValueError(
                "fresh_install_configuration_manifest_path must be a canonical absolute POSIX path"
            )
        command_identity = (
            self.executed_spack_command_path,
            self.executed_spack_command_relative_path,
            self.executed_spack_command_sha256,
            self.executed_spack_command_size_bytes,
        )
        if any(value is not None for value in command_identity):
            if not all(value is not None for value in command_identity):
                raise ValueError("executed Spack command identity must be complete")
            if not _is_canonical_absolute_posix_path(self.executed_spack_command_path):
                raise ValueError("executed Spack command path must be canonical and absolute")
            if not _is_canonical_relative_posix_path(self.executed_spack_command_relative_path):
                raise ValueError("executed Spack command relative path must be canonical")
            command_sha256 = cast(str, self.executed_spack_command_sha256)
            if len(command_sha256) != 64 or any(
                character not in "0123456789abcdef" for character in command_sha256
            ):
                raise ValueError("executed Spack command SHA-256 must be lowercase hexadecimal")
            if cast(int, self.executed_spack_command_size_bytes) < 1:
                raise ValueError("executed Spack command size must be positive")
            expected_path = str(
                PurePosixPath(self.fresh_install_configuration_manifest_path).parent
                / cast(str, self.executed_spack_command_relative_path)
            )
            if self.executed_spack_command_path != expected_path:
                raise ValueError(
                    "executed Spack command path must resolve from the configuration manifest"
                )
            relative_path = cast(str, self.executed_spack_command_relative_path)
            preinstall_components = [
                component
                for component in self.preinstall_configuration.components
                if component.relative_path == relative_path
            ]
            postinstall_components = [
                component
                for component in self.postinstall_configuration.components
                if component.relative_path == relative_path
            ]
            if len(preinstall_components) != 1 or len(postinstall_components) != 1:
                raise ValueError(
                    "executed Spack command must identify one preinstall and postinstall "
                    "configuration component"
                )
            if preinstall_components[0] != postinstall_components[0]:
                raise ValueError(
                    "executed Spack command configuration component must remain unchanged"
                )
            if (
                command_sha256 != preinstall_components[0].sha256
                or self.executed_spack_command_size_bytes != preinstall_components[0].size_bytes
            ):
                raise ValueError(
                    "executed Spack command SHA-256 and size must match its configuration component"
                )
        if (
            self.preinstall_configuration.phase != "preinstall"
            or self.postinstall_configuration.phase != "postinstall"
        ):
            raise ValueError("configuration observations must retain their ordered phases")
        expected_phases = (
            (self.preinstall, "preinstall", "spack_find"),
            (self.install, "install", "spack_install"),
            (self.postinstall, "postinstall", "spack_locate"),
        )
        for call, phase, tool in expected_phases:
            if call.phase != phase or call.remote_tool_name != tool:
                raise ValueError(f"{phase} evidence must represent {tool}")
        return self


class RemoteMcpAcceptanceReport(BaseModel):
    """Machine-readable evidence for one virtual remote MCP acceptance call."""

    model_config = ConfigDict(extra="forbid")

    schema_version: str = "1.0"
    report_type: str = "clio-relay.remote-mcp-acceptance"
    generated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    cluster: str
    server_name: str
    remote_tool_name: str
    virtual_alias: str | None = None
    profile: str
    passed: bool
    checks: list[RemoteMcpAcceptanceCheck]
    discovery: JSON = Field(default_factory=dict)
    call_job: JSON = Field(default_factory=dict)
    artifacts: list[JSON] = Field(default_factory=lambda: list[JSON]())
    mcp_stdio: JSON = Field(default_factory=dict)
    spack_install_transition: RemoteMcpSpackInstallTransitionEvidence | None = None

    def to_live_validation_report(
        self,
        *,
        launcher: str | None = None,
        install_source: str | None = None,
        artifact_sha256: str | None = None,
    ) -> LiveValidationReport:
        """Convert domain assertions into the canonical release evidence schema."""
        from clio_relay.validation_report import (
            EvidenceReference,
            ValidationCheck,
            ValidationResource,
            ValidationStatus,
            new_live_validation_report,
        )

        report = new_live_validation_report(
            scenario="remote-mcp",
            cluster=self.cluster,
            launcher=launcher,
            install_source=install_source,
            artifact_sha256=artifact_sha256,
        )
        report.started_at = self.generated_at
        report.completed_at = datetime.now(UTC)
        report.checks = [
            ValidationCheck(
                check_id=check.name,
                summary=check.message,
                status=(ValidationStatus.PASSED if check.passed else ValidationStatus.FAILED),
                started_at=self.generated_at,
                completed_at=report.completed_at,
                evidence=[
                    EvidenceReference(
                        kind="remote_mcp_acceptance",
                        excerpt=check.message,
                        metadata=check.evidence,
                    )
                ],
                error=None if check.passed else check.message,
            )
            for check in self.checks
        ]
        report.status = ValidationStatus.PASSED if self.passed else ValidationStatus.FAILED
        report.error = None if self.passed else "one or more remote MCP checks failed"
        call_job_id = self.call_job.get("job_id")
        if self.spack_install_transition is None and isinstance(call_job_id, str):
            call_metadata = {
                **self.call_job,
                "remote_mcp_server_name": self.server_name,
                "remote_mcp_tool_name": self.remote_tool_name,
                "virtual_alias": self.virtual_alias,
                "profile": self.profile,
            }
            result_check = next(
                (check for check in self.checks if check.name == "remote-mcp.structured-result"),
                None,
            )
            if result_check is not None:
                call_metadata["structured_result_assertion"] = result_check.evidence
            catalog_result_check = next(
                (
                    check
                    for check in self.checks
                    if check.name == "remote-mcp.scientific-catalog-result"
                ),
                None,
            )
            if catalog_result_check is not None:
                call_metadata["scientific_catalog_result_assertion"] = catalog_result_check.evidence
            report.resources.append(
                ValidationResource(
                    kind="relay_job",
                    resource_id=call_job_id,
                    role="virtual_remote_mcp_call",
                    cluster=self.cluster,
                    state=(
                        str(self.call_job["state"])
                        if self.call_job.get("state") is not None
                        else None
                    ),
                    metadata=call_metadata,
                )
            )
        raw_provenance = self.discovery.get("provenance")
        discovery_provenance = (
            cast(JSON, raw_provenance) if isinstance(raw_provenance, dict) else {}
        )
        discovery_job_id = discovery_provenance.get("discovery_job_id")
        if isinstance(discovery_job_id, str):
            report.resources.append(
                ValidationResource(
                    kind="relay_job",
                    resource_id=discovery_job_id,
                    role="remote_mcp_discovery",
                    cluster=self.cluster,
                    state="succeeded",
                    metadata=discovery_provenance,
                )
            )
        discovery_artifact_id = discovery_provenance.get("artifact_id")
        if isinstance(discovery_artifact_id, str):
            report.resources.append(
                ValidationResource(
                    kind="artifact",
                    resource_id=discovery_artifact_id,
                    role="remote_mcp_schema",
                    cluster=self.cluster,
                    metadata=discovery_provenance,
                )
            )
        if self.spack_install_transition is None:
            for artifact in self.artifacts:
                resource = _acceptance_artifact_resource(self.cluster, artifact)
                if resource is None:
                    continue
                report.resources.append(resource)
                report.artifacts.append(
                    EvidenceReference(
                        kind=resource.role or "artifact",
                        reference=(
                            resource.references[0]
                            if resource.references
                            else f"relay-artifact://{self.cluster}/{resource.resource_id}"
                        ),
                        sha256=(
                            str(artifact["sha256"])
                            if isinstance(artifact.get("sha256"), str)
                            else None
                        ),
                    )
                )
        else:
            _append_spack_transition_resources(report, self.spack_install_transition)
        server_check = next(
            (check for check in self.checks if check.name == "remote-mcp.server-artifact"),
            None,
        )
        contract_check = next(
            (
                check
                for check in self.checks
                if check.name
                in {
                    "remote-mcp.spack-user-contract",
                    "remote-mcp.scientific-catalog-user-contract",
                }
            ),
            None,
        )
        contract_metadata: JSON = {}
        if contract_check is not None:
            contract_id = contract_check.evidence.get("declared_contract")
            contract_sha256 = contract_check.evidence.get("observed_contract_sha256")
            if isinstance(contract_id, str):
                contract_metadata["contract_id"] = contract_id
            if isinstance(contract_sha256, str):
                contract_metadata["contract_sha256"] = contract_sha256
        raw_server_artifact = (
            server_check.evidence.get("call_server_artifact") if server_check is not None else None
        )
        if isinstance(raw_server_artifact, dict):
            server_artifact = cast(JSON, raw_server_artifact)
            identity = (
                str(server_artifact.get("install_spec"))
                if server_artifact.get("install_spec") is not None
                else str(server_artifact.get("resolved_executable", self.server_name))
            )
            report.resources.append(
                ValidationResource(
                    kind="mcp_server",
                    resource_id=f"{self.server_name}:{identity}",
                    role="remote_mcp_server",
                    cluster=self.cluster,
                    state=(
                        "verified"
                        if server_check is not None and server_check.passed
                        else "unverified"
                    ),
                    metadata={
                        "server_name": self.server_name,
                        "server_info": discovery_provenance.get("server_info", {}),
                        "remote_tool_names": self.discovery.get("remote_tool_names", []),
                        "allowlisted_tool_names": self.discovery.get("allowlisted_tool_names", []),
                        **server_artifact,
                        **contract_metadata,
                    },
                )
            )
        return report


def _acceptance_artifact_resource(
    cluster: str,
    artifact: JSON,
) -> ValidationResource | None:
    from clio_relay.validation_report import ValidationResource

    artifact_id = artifact.get("artifact_id")
    if not isinstance(artifact_id, str):
        return None
    uri = artifact.get("uri")
    return ValidationResource(
        kind="artifact",
        resource_id=artifact_id,
        role=str(artifact.get("kind", "artifact")),
        cluster=cluster,
        references=[str(uri)] if isinstance(uri, str) else [],
        metadata=artifact,
    )


def _append_spack_transition_resources(
    report: LiveValidationReport,
    transition: RemoteMcpSpackInstallTransitionEvidence,
) -> None:
    """Append phase-scoped jobs and artifacts without duplicating the install call."""
    from clio_relay.validation_report import EvidenceReference, ValidationResource

    roles = {
        "preinstall": "spack_preinstall_find",
        "install": "spack_fresh_install",
        "postinstall": "spack_postinstall_locate",
    }
    report.resources.append(
        ValidationResource(
            kind="configuration_manifest",
            resource_id=transition.fresh_install_configuration_sha256,
            role="spack_fresh_install_configuration",
            cluster=transition.cluster,
            state="verified",
            references=[transition.fresh_install_configuration_manifest_path],
            metadata={
                "expected_sha256": transition.fresh_install_configuration_sha256,
                "preinstall": transition.preinstall_configuration.model_dump(mode="json"),
                "postinstall": transition.postinstall_configuration.model_dump(mode="json"),
            },
        )
    )
    report.artifacts.append(
        EvidenceReference(
            kind="spack_fresh_install_configuration",
            reference=transition.fresh_install_configuration_manifest_path,
            sha256=transition.fresh_install_configuration_sha256,
        )
    )
    for call in (transition.preinstall, transition.install, transition.postinstall):
        role = roles[call.phase]
        if call.job_id is not None:
            report.resources.append(
                ValidationResource(
                    kind="relay_job",
                    resource_id=call.job_id,
                    role=role,
                    cluster=transition.cluster,
                    state=call.state,
                    metadata={
                        "remote_mcp_server_name": transition.server_name,
                        "remote_mcp_tool_name": call.remote_tool_name,
                        "virtual_alias": call.virtual_alias,
                        "profile": transition.profile,
                        "arguments": call.arguments,
                        "stdio": call.stdio.model_dump(mode="json"),
                        "structured_result": call.structured_result,
                    },
                )
            )
        for artifact in call.artifacts:
            if artifact.artifact_id is None:
                continue
            artifact_role = f"{role}_{artifact.kind or 'artifact'}"
            references = [artifact.uri] if artifact.uri is not None else []
            report.resources.append(
                ValidationResource(
                    kind="artifact",
                    resource_id=artifact.artifact_id,
                    role=artifact_role,
                    cluster=transition.cluster,
                    references=references,
                    metadata={
                        **artifact.model_dump(mode="json"),
                        "transition_phase": call.phase,
                    },
                )
            )
            report.artifacts.append(
                EvidenceReference(
                    kind=artifact_role,
                    reference=(
                        artifact.uri
                        if artifact.uri is not None
                        else f"relay-artifact://{transition.cluster}/{artifact.artifact_id}"
                    ),
                    sha256=artifact.sha256,
                )
            )


def _is_canonical_absolute_posix_path(value: object) -> bool:
    """Return whether a value is a normalized absolute POSIX path without traversal."""
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


def _is_canonical_relative_posix_path(value: object) -> bool:
    """Return whether a value is a normalized, non-traversing relative POSIX path."""
    if (
        not isinstance(value, str)
        or value.startswith("/")
        or value in {"", "."}
        or any(ord(character) < 32 or ord(character) == 127 for character in value)
    ):
        return False
    path = PurePosixPath(value)
    return ".." not in path.parts and str(path) == value

"""Machine-readable evidence for live validation and release decisions."""

from __future__ import annotations

import hashlib
import json
import os
import sys
from contextlib import suppress
from datetime import UTC, datetime
from importlib import metadata, resources
from pathlib import Path, PurePosixPath
from typing import Any
from uuid import uuid4

import yaml
from pydantic import ValidationError

from clio_relay import __version__
from clio_relay.artifact_identity_verification import (
    _direct_url_sha256_hashes as _direct_url_sha256_hashes,
)
from clio_relay.artifact_identity_verification import _direct_wheel_bytes as _direct_wheel_bytes
from clio_relay.artifact_identity_verification import (
    _installed_files_match_wheel as _installed_files_match_wheel,
)
from clio_relay.artifact_identity_verification import (
    _local_wheel_matches_install as _local_wheel_matches_install,
)
from clio_relay.artifact_identity_verification import (
    _pypi_wheel_matches_install as _pypi_wheel_matches_install,
)
from clio_relay.artifact_identity_verification import (
    _ReleaseWheelRedirectHandler as _ReleaseWheelRedirectHandler,
)
from clio_relay.artifact_identity_verification import _safe_wheel_member as _safe_wheel_member
from clio_relay.artifact_identity_verification import (
    _vcs_commit_identity_verified as _vcs_commit_identity_verified,
)
from clio_relay.artifact_identity_verification import (
    _wheel_url_matches_install as _wheel_url_matches_install,
)
from clio_relay.artifact_identity_verification import checkout_build_info as _checkout_build_info
from clio_relay.artifact_identity_verification import (
    classify_install_source as _classify_install_source,
)
from clio_relay.artifact_identity_verification import (
    distribution_direct_url as _distribution_direct_url,
)
from clio_relay.artifact_identity_verification import embedded_build_info as _embedded_build_info
from clio_relay.artifact_identity_verification import (
    infer_running_artifact_identity as _infer_running_artifact_identity,
)
from clio_relay.artifact_identity_verification import (
    is_github_release_asset_url as _is_github_release_asset_url,  # noqa: F401
)
from clio_relay.artifact_identity_verification import (
    is_official_github_release_wheel as _is_official_github_release_wheel,
)
from clio_relay.artifact_identity_verification import (
    is_official_release_wheel_url as _is_official_release_wheel_url,  # noqa: F401
)
from clio_relay.artifact_identity_verification import (
    url_host_resolves_publicly as _url_host_resolves_publicly,  # noqa: F401
)
from clio_relay.artifact_identity_verification import (
    verify_running_artifact_identity as _verify_running_artifact_identity,
)
from clio_relay.ci_validation import ProvenanceError, load_release_acceptance_matrix
from clio_relay.durable_validation_write import (
    atomic_write_text as _atomic_write_text,
)
from clio_relay.durable_validation_write import (
    atomic_write_text_locked as _atomic_write_text_locked,  # noqa: F401
)
from clio_relay.durable_validation_write import (
    create_posix_validation_directory_child as _create_posix_validation_directory_child,  # noqa: F401
)
from clio_relay.durable_validation_write import (
    durably_ensure_validation_directory as durably_ensure_validation_directory,
)
from clio_relay.errors import ConfigurationError
from clio_relay.filesystem_paths import (
    internal_filesystem_path,
    logical_filesystem_path,
)
from clio_relay.process_ancestry import strictly_contains as _strictly_contains
from clio_relay.process_ancestry import uv_executable_identity as _uv_executable_identity
from clio_relay.process_ancestry import uv_process_ancestor as _uv_process_ancestor
from clio_relay.process_ancestry import within_or_equal as _within_or_equal
from clio_relay.redaction import redact_sensitive_values as redact_sensitive_values
from clio_relay.regular_file_identity import regular_file_identity as _regular_file_identity
from clio_relay.release_gate_evaluation import evaluate_release_gate as evaluate_release_gate
from clio_relay.uv_tool_receipt import (
    detect_persistent_uv_tool_receipt as _detect_persistent_uv_tool_receipt,
)
from clio_relay.uv_tool_receipt import (
    persistent_uv_tool_receipt_identity as _persistent_uv_tool_receipt_identity,  # noqa: F401
)
from clio_relay.uv_tool_receipt import pyvenv_uv_version_marker as _pyvenv_uv_version
from clio_relay.uv_tool_receipt import uv_cache_dir as _uv_cache_dir

# Schema + budget catalog (#231; docs/design/relay-architecture-2026-08.md):
# the pydantic/StrEnum wire-model catalog moved to validation_schema.py, and
# the byte/count budget constants moved to validation_limits.py. Every name
# is re-exported here under its original binding (the `X as X` self-import
# idiom -- see door_errors.py's precedent -- so ruff's F401 does not prune a
# name this module no longer references internally but a caller, test, or
# `validation_report.<Symbol>` monkeypatch seam still resolves through this
# path) -- a pure move, not a behavior change.
from clio_relay.validation_limits import (
    MAX_DISTRIBUTION_WHEEL_BYTES as MAX_DISTRIBUTION_WHEEL_BYTES,
)
from clio_relay.validation_limits import (
    MAX_LAUNCHER_PROCESS_ANCESTORS as MAX_LAUNCHER_PROCESS_ANCESTORS,
)
from clio_relay.validation_limits import MAX_PYVENV_CONFIG_BYTES as MAX_PYVENV_CONFIG_BYTES
from clio_relay.validation_limits import (
    MAX_TRANSPORT_PROBE_EVIDENCE_BYTES as MAX_TRANSPORT_PROBE_EVIDENCE_BYTES,
)
from clio_relay.validation_limits import (
    MAX_TRANSPORT_PROBE_JSON_DEPTH as MAX_TRANSPORT_PROBE_JSON_DEPTH,
)
from clio_relay.validation_limits import (
    MAX_TRANSPORT_PROBE_JSON_NODES as MAX_TRANSPORT_PROBE_JSON_NODES,
)
from clio_relay.validation_limits import (
    MAX_TRANSPORT_PROBE_RESOURCES as MAX_TRANSPORT_PROBE_RESOURCES,
)
from clio_relay.validation_limits import MAX_UV_TOOL_RECEIPT_BYTES as MAX_UV_TOOL_RECEIPT_BYTES
from clio_relay.validation_limits import (
    MAX_VALIDATION_PENDING_FILES as MAX_VALIDATION_PENDING_FILES,
)
from clio_relay.validation_limits import (
    MAX_VALIDATION_REPORT_WRITE_BYTES as MAX_VALIDATION_REPORT_WRITE_BYTES,
)
from clio_relay.validation_limits import (
    TRANSPORT_PROBE_EVIDENCE_KEY as TRANSPORT_PROBE_EVIDENCE_KEY,
)

# Report construction (#231): ValidationRecorder and the seeded-report
# factory moved to validation_recorder.py, re-exported here the same way
# (see the header comment above).
from clio_relay.validation_recorder import ValidationRecorder as ValidationRecorder
from clio_relay.validation_recorder import (
    new_live_validation_report as new_live_validation_report,
)
from clio_relay.validation_schema import REPORT_SCHEMA_VERSION as REPORT_SCHEMA_VERSION
from clio_relay.validation_schema import (
    SPACK_FRESH_INSTALL_TRANSITION_CHECK_IDS as SPACK_FRESH_INSTALL_TRANSITION_CHECK_IDS,
)
from clio_relay.validation_schema import CleanupEvidence as CleanupEvidence
from clio_relay.validation_schema import EvidenceOrigin as EvidenceOrigin
from clio_relay.validation_schema import EvidenceReference as EvidenceReference
from clio_relay.validation_schema import EvidenceTrust as EvidenceTrust
from clio_relay.validation_schema import InstallSource as InstallSource
from clio_relay.validation_schema import InstallSourceKind as InstallSourceKind
from clio_relay.validation_schema import LiveValidationReport as LiveValidationReport
from clio_relay.validation_schema import ReleaseGatePolicy as ReleaseGatePolicy
from clio_relay.validation_schema import ReleaseGateRequirement as ReleaseGateRequirement
from clio_relay.validation_schema import ReleaseGateResult as ReleaseGateResult
from clio_relay.validation_schema import ReleaseResourceRequirement as ReleaseResourceRequirement
from clio_relay.validation_schema import (
    ReleaseSpackFreshInstallRequirement as ReleaseSpackFreshInstallRequirement,
)
from clio_relay.validation_schema import ReleaseTargetIdentity as ReleaseTargetIdentity
from clio_relay.validation_schema import SoftwareIdentity as SoftwareIdentity
from clio_relay.validation_schema import TransportCleanupAction as TransportCleanupAction
from clio_relay.validation_schema import TransportCleanupOutcome as TransportCleanupOutcome
from clio_relay.validation_schema import (
    TransportCleanupResourceEvidence as TransportCleanupResourceEvidence,
)
from clio_relay.validation_schema import TransportProbeEvidence as TransportProbeEvidence
from clio_relay.validation_schema import ValidationCheck as ValidationCheck
from clio_relay.validation_schema import ValidationResource as ValidationResource
from clio_relay.validation_schema import ValidationStatus as ValidationStatus
from clio_relay.validation_schema import (
    parse_transport_probe_evidence as parse_transport_probe_evidence,
)
from clio_relay.validation_schema import (
    transport_probe_evidence_line as transport_probe_evidence_line,
)


def _utc_now() -> datetime:
    return datetime.now(UTC)


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

"""Machine-readable evidence for live validation and release decisions."""

from __future__ import annotations

import ctypes
import hashlib
import json
import os
import stat
import sys
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from importlib import import_module, metadata, resources
from pathlib import Path, PurePosixPath
from typing import Any, cast
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
from clio_relay.cluster_config import (
    acquire_private_configuration_windows_parent_guard,
    create_private_configuration_directory,
    ensure_private_configuration_windows_handle,
    open_private_atomic_file,
    open_private_configuration_windows_descriptor,
    release_private_configuration_windows_parent_guard,
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
from clio_relay.validation_limits import (
    VALIDATION_PENDING_PATTERN as _VALIDATION_PENDING_PATTERN,
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


class _WindowsValidationFileTime(ctypes.Structure):
    _fields_ = [("low", ctypes.c_uint32), ("high", ctypes.c_uint32)]


class _WindowsValidationFileInformation(ctypes.Structure):
    _fields_ = [
        ("attributes", ctypes.c_uint32),
        ("creation_time", _WindowsValidationFileTime),
        ("last_access_time", _WindowsValidationFileTime),
        ("last_write_time", _WindowsValidationFileTime),
        ("volume_serial_number", ctypes.c_uint32),
        ("file_size_high", ctypes.c_uint32),
        ("file_size_low", ctypes.c_uint32),
        ("number_of_links", ctypes.c_uint32),
        ("file_index_high", ctypes.c_uint32),
        ("file_index_low", ctypes.c_uint32),
    ]


@dataclass(frozen=True, slots=True)
class _WindowsValidationDirectoryAnchor:
    """One non-reparse Windows directory pinned without delete sharing."""

    path: Path
    status: os.stat_result
    handle: ctypes.c_void_p
    identity: tuple[int, int, int]


@dataclass(frozen=True, slots=True)
class _ValidationWriterLock:
    """Parent-wide lock bounding deterministic validation staging files."""

    path: Path
    descriptor: int
    parent_fd: int | None = None
    windows_parent: _WindowsValidationDirectoryAnchor | None = None


def _windows_validation_directory_identity(
    handle: ctypes.c_void_p,
    *,
    path: Path,
) -> tuple[int, int, int]:
    if os.name != "nt":  # pragma: no cover - platform contract
        raise OSError("Windows validation handles cannot be inspected on this platform")
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    get_information = kernel32.GetFileInformationByHandle
    get_information.argtypes = [
        ctypes.c_void_p,
        ctypes.POINTER(_WindowsValidationFileInformation),
    ]
    get_information.restype = ctypes.c_int
    information = _WindowsValidationFileInformation()
    if not get_information(handle, ctypes.byref(information)):
        error_number = ctypes.get_last_error()
        raise OSError(error_number, ctypes.FormatError(error_number), str(path))
    if not information.attributes & 0x00000010 or information.attributes & 0x00000400:
        raise OSError(f"validation report directory is a Windows reparse point: {path}")
    return (
        int(information.volume_serial_number),
        int(information.file_index_high),
        int(information.file_index_low),
    )


def _close_windows_validation_directory(
    anchor: _WindowsValidationDirectoryAnchor | None,
) -> None:
    if anchor is None:
        return
    _close_windows_validation_handle(anchor.handle, path=anchor.path)


def _close_windows_validation_handle(handle: ctypes.c_void_p, *, path: Path) -> None:
    if os.name != "nt":  # pragma: no cover - platform contract
        raise OSError("Windows validation handles cannot be closed on this platform")
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    close_handle = kernel32.CloseHandle
    close_handle.argtypes = [ctypes.c_void_p]
    close_handle.restype = ctypes.c_int
    if not close_handle(handle):
        error_number = ctypes.get_last_error()
        raise OSError(error_number, ctypes.FormatError(error_number), str(path))


def _open_windows_validation_handle(
    path: Path,
    *,
    allow_delete_share: bool,
    acl_write: bool,
) -> ctypes.c_void_p:
    if os.name != "nt":  # pragma: no cover - platform contract
        raise OSError("Windows validation handles cannot be opened on this platform")
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    create_file = kernel32.CreateFileW
    create_file.argtypes = [
        ctypes.c_wchar_p,
        ctypes.c_uint32,
        ctypes.c_uint32,
        ctypes.c_void_p,
        ctypes.c_uint32,
        ctypes.c_uint32,
        ctypes.c_void_p,
    ]
    create_file.restype = ctypes.c_void_p
    storage_path = internal_filesystem_path(path, force_extended=True)
    share = 0x00000001 | 0x00000002 | (0x00000004 if allow_delete_share else 0)
    raw_handle = create_file(
        str(storage_path),
        0x00000080 | (0x00020000 | 0x00040000 | 0x00080000 if acl_write else 0),
        share,
        None,
        3,
        0x02000000 | 0x00200000,
        None,
    )
    if raw_handle in (None, ctypes.c_void_p(-1).value):
        error_number = ctypes.get_last_error()
        raise OSError(error_number, ctypes.FormatError(error_number), str(path))
    return ctypes.c_void_p(raw_handle)


def _open_windows_validation_directory(
    path: Path,
    *,
    expected_status: os.stat_result,
    expected_identity: tuple[int, int, int] | None = None,
    allow_delete_share: bool = False,
    acl_write: bool = False,
) -> _WindowsValidationDirectoryAnchor:
    storage_path = internal_filesystem_path(path, force_extended=True)
    handle = _open_windows_validation_handle(
        path,
        allow_delete_share=allow_delete_share,
        acl_write=acl_write,
    )
    try:
        identity = _windows_validation_directory_identity(handle, path=path)
        observed = os.lstat(storage_path)
        if not (
            os.path.samestat(expected_status, observed)
            and stat.S_ISDIR(observed.st_mode)
            and not stat.S_ISLNK(observed.st_mode)
            and not getattr(observed, "st_file_attributes", 0) & 0x00000400
            and (expected_identity is None or identity == expected_identity)
        ):
            raise OSError(f"validation report directory changed while pinning: {path}")
        anchor = _WindowsValidationDirectoryAnchor(
            path=path,
            status=observed,
            handle=handle,
            identity=identity,
        )
        verification_handle = _open_windows_validation_handle(
            path,
            allow_delete_share=False,
            acl_write=False,
        )
        try:
            if _windows_validation_directory_identity(verification_handle, path=path) != identity:
                raise OSError(f"validation report directory path changed: {path}")
        finally:
            _close_windows_validation_handle(verification_handle, path=path)
        return anchor
    except BaseException:
        _close_windows_validation_handle(handle, path=path)
        raise


def _verify_windows_validation_directory(
    anchor: _WindowsValidationDirectoryAnchor,
) -> None:
    if _windows_validation_directory_identity(anchor.handle, path=anchor.path) != anchor.identity:
        raise OSError(f"validation report directory handle changed: {anchor.path}")
    storage_path = internal_filesystem_path(anchor.path, force_extended=True)
    observed = os.lstat(storage_path)
    if not (
        os.path.samestat(anchor.status, observed)
        and stat.S_ISDIR(observed.st_mode)
        and not stat.S_ISLNK(observed.st_mode)
        and not getattr(observed, "st_file_attributes", 0) & 0x00000400
    ):
        raise OSError(f"validation report directory path changed: {anchor.path}")
    verification_handle = _open_windows_validation_handle(
        anchor.path,
        allow_delete_share=False,
        acl_write=False,
    )
    try:
        if (
            _windows_validation_directory_identity(
                verification_handle,
                path=anchor.path,
            )
            != anchor.identity
        ):
            raise OSError(f"validation report directory path changed: {anchor.path}")
    finally:
        _close_windows_validation_handle(verification_handle, path=anchor.path)


def _acquire_validation_writer_lock(parent: Path) -> _ValidationWriterLock:
    """Serialize validation replacement and stale-pending recovery in one parent."""
    parent_status = os.lstat(parent)
    lock_path = parent / ".clio-validation-writer-v1.lock"
    if os.name == "posix":
        parent_fd = os.open(
            parent,
            os.O_RDONLY
            | getattr(os, "O_DIRECTORY", 0)
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_CLOEXEC", 0),
        )
        descriptor: int | None = None
        try:
            if not os.path.samestat(parent_status, os.fstat(parent_fd)):
                raise OSError("validation writer lock parent changed while opening")
            try:
                descriptor = os.open(
                    lock_path.name,
                    os.O_RDWR
                    | os.O_CREAT
                    | os.O_EXCL
                    | getattr(os, "O_NOFOLLOW", 0)
                    | getattr(os, "O_CLOEXEC", 0),
                    0o600,
                    dir_fd=parent_fd,
                )
                os.fchmod(descriptor, 0o600)
                os.fsync(descriptor)
                os.fsync(parent_fd)
            except FileExistsError:
                descriptor = os.open(
                    lock_path.name,
                    os.O_RDWR | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0),
                    dir_fd=parent_fd,
                )
            opened = os.fstat(descriptor)
            linked = os.stat(lock_path.name, dir_fd=parent_fd, follow_symlinks=False)
            if not (
                stat.S_ISREG(opened.st_mode)
                and stat.S_ISREG(linked.st_mode)
                and opened.st_nlink == 1
                and linked.st_nlink == 1
                and opened.st_uid == os.geteuid()
                and linked.st_uid == os.geteuid()
                and stat.S_IMODE(opened.st_mode) == 0o600
                and stat.S_IMODE(linked.st_mode) == 0o600
                and os.path.samestat(opened, linked)
            ):
                raise OSError("validation writer lock is not one owner-private regular file")
            try:
                import_module("fcntl").flock(descriptor, 2 | 4)
            except BlockingIOError:
                raise OSError("another validation writer owns this directory") from None
            confirmed = os.stat(lock_path.name, dir_fd=parent_fd, follow_symlinks=False)
            if not os.path.samestat(opened, confirmed):
                raise OSError("validation writer lock changed during acquisition")
            return _ValidationWriterLock(
                path=lock_path,
                descriptor=descriptor,
                parent_fd=parent_fd,
            )
        except BaseException:
            if descriptor is not None:
                os.close(descriptor)
            os.close(parent_fd)
            raise

    windows_parent: _WindowsValidationDirectoryAnchor | None = None
    windows_parent_guard: tuple[Path, ctypes.c_void_p] | None = None
    descriptor: int | None = None
    try:
        windows_parent_guard = acquire_private_configuration_windows_parent_guard(parent)
        windows_parent = _open_windows_validation_directory(
            parent,
            expected_status=parent_status,
        )
        storage_lock_path = internal_filesystem_path(lock_path, force_extended=True)
        try:
            os.lstat(storage_lock_path)
        except FileNotFoundError:
            try:
                with open_private_atomic_file(storage_lock_path) as stream:
                    stream.flush()
                    os.fsync(stream.fileno())
            except FileExistsError:
                pass
        try:
            descriptor = open_private_configuration_windows_descriptor(
                storage_lock_path,
                exclusive=True,
            )
        except ConfigurationError as exc:
            raise OSError("validation writer lock could not be acquired") from exc
        _verify_windows_validation_directory(windows_parent)
        result = _ValidationWriterLock(
            path=lock_path,
            descriptor=descriptor,
            windows_parent=windows_parent,
        )
        acquired_parent_guard = windows_parent_guard
        windows_parent_guard = None
        release_private_configuration_windows_parent_guard(acquired_parent_guard)
        return result
    except BaseException:
        if descriptor is not None:
            os.close(descriptor)
        _close_windows_validation_directory(windows_parent)
        release_private_configuration_windows_parent_guard(windows_parent_guard)
        raise


def _release_validation_writer_lock(lock: _ValidationWriterLock) -> None:
    """Release one validation writer lock, preserving its single stable inode."""
    release_error: BaseException | None = None
    if lock.parent_fd is not None:
        try:
            import_module("fcntl").flock(lock.descriptor, 8)
        except BaseException as exc:  # pragma: no cover - OS release failure
            release_error = exc
    try:
        os.close(lock.descriptor)
    except BaseException as exc:  # pragma: no cover - OS release failure
        release_error = release_error or exc
    if lock.parent_fd is not None:
        try:
            os.close(lock.parent_fd)
        except BaseException as exc:  # pragma: no cover - OS release failure
            release_error = release_error or exc
    try:
        _close_windows_validation_directory(lock.windows_parent)
    except BaseException as exc:  # pragma: no cover - OS release failure
        release_error = release_error or exc
    if release_error is not None:
        raise OSError(f"validation writer lock could not be released: {release_error}")


def _verify_validation_writer_lock_parent(
    lock: _ValidationWriterLock,
    parent: Path,
) -> os.stat_result:
    """Verify the named parent against the retained writer lock without mutation."""
    requested_parent = parent.absolute()
    if os.path.normcase(str(requested_parent)) != os.path.normcase(str(lock.path.parent)):
        raise OSError("validation report parent differs from its writer lock")
    if lock.parent_fd is not None:
        try:
            linked_parent = os.stat(requested_parent, follow_symlinks=False)
            linked_lock = os.stat(
                lock.path.name,
                dir_fd=lock.parent_fd,
                follow_symlinks=False,
            )
        except FileNotFoundError:
            raise OSError("validation report parent disappeared after writer lock") from None
        opened_parent = os.fstat(lock.parent_fd)
        opened_lock = os.fstat(lock.descriptor)
        if not (
            stat.S_ISDIR(linked_parent.st_mode)
            and not stat.S_ISLNK(linked_parent.st_mode)
            and os.path.samestat(opened_parent, linked_parent)
            and stat.S_ISREG(opened_lock.st_mode)
            and stat.S_ISREG(linked_lock.st_mode)
            and opened_lock.st_nlink == 1
            and linked_lock.st_nlink == 1
            and os.path.samestat(opened_lock, linked_lock)
        ):
            raise OSError("validation report parent differs from its writer lock")
        return opened_parent
    if lock.windows_parent is None:
        raise OSError("validation writer lock omitted its parent ownership handle")
    _verify_windows_validation_directory(lock.windows_parent)
    linked_parent = os.lstat(internal_filesystem_path(requested_parent, force_extended=True))
    if not os.path.samestat(lock.windows_parent.status, linked_parent):
        raise OSError("validation report parent differs from its writer lock")
    return linked_parent


def _prune_stale_validation_pending_files(
    parent: Path,
    *,
    current_name: str,
    writer_lock: _ValidationWriterLock,
) -> None:
    """Bound crash-recovery staging to the current target under the parent-wide lock."""
    candidates: list[tuple[str, os.stat_result]] = []
    scan_target: int | Path = (
        writer_lock.parent_fd
        if writer_lock.parent_fd is not None
        else internal_filesystem_path(parent, force_extended=True)
    )
    with os.scandir(scan_target) as entries:
        for entry in entries:
            if _VALIDATION_PENDING_PATTERN.fullmatch(entry.name) is None:
                continue
            if len(candidates) >= MAX_VALIDATION_PENDING_FILES:
                raise OSError("validation report pending-file retention limit was exceeded")
            observed = (
                os.stat(
                    entry.name,
                    dir_fd=writer_lock.parent_fd,
                    follow_symlinks=False,
                )
                if writer_lock.parent_fd is not None
                else os.lstat(internal_filesystem_path(parent / entry.name, force_extended=True))
            )
            candidates.append((entry.name, observed))
    for name, observed in candidates:
        if name == current_name:
            continue
        if not (
            stat.S_ISREG(observed.st_mode)
            and not stat.S_ISLNK(observed.st_mode)
            and observed.st_nlink == 1
            and 0 <= observed.st_size <= MAX_VALIDATION_REPORT_WRITE_BYTES
            and not getattr(observed, "st_file_attributes", 0) & 0x00000400
        ):
            raise OSError("stale validation report pending file is unsafe")
        if os.name == "posix" and not (
            observed.st_uid == os.geteuid() and stat.S_IMODE(observed.st_mode) == 0o600
        ):
            raise OSError("stale validation report pending file is not owner-private")
        if os.name == "nt":
            descriptor = open_private_configuration_windows_descriptor(
                internal_filesystem_path(parent / name, force_extended=True)
            )
            os.close(descriptor)
            confirmed = os.lstat(internal_filesystem_path(parent / name, force_extended=True))
        else:
            confirmed = os.stat(
                name,
                dir_fd=writer_lock.parent_fd,
                follow_symlinks=False,
            )
        if not (
            confirmed.st_nlink == 1
            and (confirmed.st_dev, confirmed.st_ino, confirmed.st_size)
            == (observed.st_dev, observed.st_ino, observed.st_size)
        ):
            raise OSError("stale validation report pending file changed before deletion")
        if writer_lock.parent_fd is not None:
            os.unlink(name, dir_fd=writer_lock.parent_fd)
            os.fsync(writer_lock.parent_fd)
        else:
            os.unlink(internal_filesystem_path(parent / name, force_extended=True))

    remaining: list[str] = []
    with os.scandir(scan_target) as entries:
        for entry in entries:
            if (
                _VALIDATION_PENDING_PATTERN.fullmatch(entry.name) is not None
                and entry.name != current_name
            ):
                remaining.append(entry.name)
    if remaining:
        raise OSError("stale validation report pending-file pruning was incomplete")


def _create_windows_validation_directory_child(
    parent: _WindowsValidationDirectoryAnchor,
    child_name: str,
) -> _WindowsValidationDirectoryAnchor:
    """Create, durably name, and pin one private child below a pinned parent."""
    if os.name != "nt":  # pragma: no cover - platform contract
        raise OSError("Windows validation directories cannot be created on this platform")
    if Path(child_name).name != child_name or child_name in {".", ".."}:
        raise OSError(f"unsafe validation report directory component: {child_name}")
    _verify_windows_validation_directory(parent)
    child_path = parent.path / child_name
    pending_identity = hashlib.sha256(child_name.encode("utf-8")).hexdigest()[:32]
    temporary_path = parent.path / f".clio-validation-dir-{pending_identity}.pending"
    temporary_anchor: _WindowsValidationDirectoryAnchor | None = None
    child_anchor: _WindowsValidationDirectoryAnchor | None = None
    try:
        temporary_storage_path = internal_filesystem_path(
            temporary_path,
            force_extended=True,
        )
        try:
            temporary_status = os.lstat(temporary_storage_path)
        except FileNotFoundError:
            create_private_configuration_directory(temporary_storage_path)
            temporary_status = os.lstat(temporary_storage_path)
        if not (
            stat.S_ISDIR(temporary_status.st_mode)
            and not stat.S_ISLNK(temporary_status.st_mode)
            and not getattr(temporary_status, "st_file_attributes", 0) & 0x00000400
        ):
            raise OSError(f"validation report pending directory is unsafe: {temporary_path}")
        temporary_anchor = _open_windows_validation_directory(
            temporary_path,
            expected_status=temporary_status,
            allow_delete_share=True,
            acl_write=True,
        )
        ensure_private_configuration_windows_handle(
            temporary_storage_path,
            handle=temporary_anchor.handle,
            directory=True,
        )
        with os.scandir(temporary_storage_path) as entries:
            if next(entries, None) is not None:
                raise OSError(f"validation report pending directory is not empty: {temporary_path}")
        _verify_windows_validation_directory(temporary_anchor)
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        move_file_ex = kernel32.MoveFileExW
        move_file_ex.argtypes = [ctypes.c_wchar_p, ctypes.c_wchar_p, ctypes.c_uint32]
        move_file_ex.restype = ctypes.c_int
        if not move_file_ex(
            str(temporary_storage_path),
            str(internal_filesystem_path(child_path, force_extended=True)),
            0x00000008,
        ):
            error_number = ctypes.get_last_error()
            raise OSError(error_number, ctypes.FormatError(error_number), str(child_path))
        child_status = os.lstat(internal_filesystem_path(child_path, force_extended=True))
        child_anchor = _open_windows_validation_directory(
            child_path,
            expected_status=child_status,
            expected_identity=temporary_anchor.identity,
            acl_write=True,
        )
        ensure_private_configuration_windows_handle(
            internal_filesystem_path(child_path, force_extended=True),
            handle=child_anchor.handle,
            directory=True,
        )
        _verify_windows_validation_directory(parent)
        _verify_windows_validation_directory(child_anchor)
        result = child_anchor
        child_anchor = None
        return result
    finally:
        _close_windows_validation_directory(child_anchor)
        _close_windows_validation_directory(temporary_anchor)
        with suppress(OSError):
            os.rmdir(internal_filesystem_path(temporary_path, force_extended=True))


def _verify_posix_validation_directory(directory_fd: int, path: Path) -> None:
    opened = os.fstat(directory_fd)
    linked = os.stat(path, follow_symlinks=False)
    resolved = path.resolve(strict=True)
    if not (
        stat.S_ISDIR(opened.st_mode)
        and stat.S_ISDIR(linked.st_mode)
        and not stat.S_ISLNK(linked.st_mode)
        and os.path.samestat(opened, linked)
        and os.path.normcase(str(resolved)) == os.path.normcase(str(path))
    ):
        raise OSError(f"validation report directory path changed: {path}")


def _create_posix_validation_directory_child(
    parent_fd: int,
    child_name: str,
) -> int:
    """Create and pin one owner-private child through a retained POSIX dirfd."""
    if Path(child_name).name != child_name or child_name in {".", ".."}:
        raise OSError(f"unsafe validation report directory component: {child_name}")
    child_fd: int | None = None
    created = False
    platform_os = cast(Any, os)
    fchmod = cast(Callable[[int, int], None], platform_os.fchmod)
    geteuid = cast(Callable[[], int], platform_os.geteuid)
    try:
        os.mkdir(child_name, 0o700, dir_fd=parent_fd)
        created = True
        child_fd = os.open(
            child_name,
            os.O_RDONLY
            | getattr(os, "O_DIRECTORY", 0)
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_CLOEXEC", 0),
            dir_fd=parent_fd,
        )
        fchmod(child_fd, 0o700)
        opened = os.fstat(child_fd)
        linked = os.stat(child_name, dir_fd=parent_fd, follow_symlinks=False)
        if not (
            stat.S_ISDIR(opened.st_mode)
            and stat.S_ISDIR(linked.st_mode)
            and opened.st_uid == geteuid()
            and linked.st_uid == geteuid()
            and stat.S_IMODE(opened.st_mode) == 0o700
            and stat.S_IMODE(linked.st_mode) == 0o700
            and os.path.samestat(opened, linked)
        ):
            raise OSError(f"validation report directory child is not owner-private: {child_name}")
        os.fsync(child_fd)
        os.fsync(parent_fd)
        result = child_fd
        child_fd = None
        return result
    except BaseException:
        if child_fd is not None:
            os.close(child_fd)
        if created:
            with suppress(OSError):
                os.rmdir(child_name, dir_fd=parent_fd)
                os.fsync(parent_fd)
        raise


def durably_ensure_validation_directory(path: Path) -> None:
    """Create missing ancestry relative to pinned parents and persist every entry."""
    requested = Path(os.path.abspath(path))
    missing: list[Path] = []
    cursor = requested
    while True:
        try:
            existing = os.stat(cursor, follow_symlinks=False)
        except FileNotFoundError:
            missing.append(cursor)
            parent = cursor.parent
            if parent == cursor:  # pragma: no cover - filesystem root must exist
                raise OSError(
                    f"validation report directory root is unavailable: {cursor}"
                ) from None
            cursor = parent
            continue
        if (
            not stat.S_ISDIR(existing.st_mode)
            or stat.S_ISLNK(existing.st_mode)
            or getattr(existing, "st_file_attributes", 0) & 0x00000400
        ):
            raise OSError(f"validation report ancestor is not a real directory: {cursor}")
        resolved = cursor.resolve(strict=True)
        if os.path.normcase(str(resolved)) != os.path.normcase(str(cursor)):
            raise OSError(
                f"validation report ancestor traverses a symlink or reparse point: {cursor}"
            )
        break

    if os.name == "nt":
        anchor = _open_windows_validation_directory(cursor, expected_status=existing)
        try:
            for directory in reversed(missing):
                child_anchor = _create_windows_validation_directory_child(
                    anchor,
                    directory.name,
                )
                try:
                    _verify_windows_validation_directory(anchor)
                    _verify_windows_validation_directory(child_anchor)
                    resolved = directory.resolve(strict=True)
                    if os.path.normcase(str(resolved)) != os.path.normcase(str(directory)):
                        raise OSError(
                            "validation report directory traverses a Windows reparse point: "
                            f"{directory}"
                        )
                except BaseException:
                    _close_windows_validation_directory(child_anchor)
                    raise
                _close_windows_validation_directory(anchor)
                anchor = child_anchor
            _verify_windows_validation_directory(anchor)
        finally:
            _close_windows_validation_directory(anchor)
        return

    directory_fd = os.open(
        cursor,
        os.O_RDONLY
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_CLOEXEC", 0),
    )
    try:
        if not os.path.samestat(existing, os.fstat(directory_fd)):
            raise OSError(f"validation report ancestor changed while pinning: {cursor}")
        _verify_posix_validation_directory(directory_fd, cursor)
        for directory in reversed(missing):
            child_fd = _create_posix_validation_directory_child(
                directory_fd,
                directory.name,
            )
            try:
                _verify_posix_validation_directory(directory_fd, cursor)
                _verify_posix_validation_directory(child_fd, directory)
            except BaseException:
                os.close(child_fd)
                raise
            os.close(directory_fd)
            directory_fd = child_fd
            cursor = directory
        _verify_posix_validation_directory(directory_fd, requested)
    finally:
        os.close(directory_fd)


def _atomic_write_text(path: Path, text: str) -> None:
    """Serialize and durably replace one validation text file."""
    logical_path = logical_filesystem_path(path)
    requested_parent = logical_path.parent.absolute()
    durably_ensure_validation_directory(requested_parent)
    resolved_parent = requested_parent.resolve(strict=True)
    if os.path.normcase(str(resolved_parent)) != os.path.normcase(str(requested_parent)):
        raise OSError(
            "validation report parent cannot traverse a symlink or reparse point: "
            f"{requested_parent}"
        )
    writer_lock = _acquire_validation_writer_lock(resolved_parent)
    try:
        _atomic_write_text_locked(path, text, writer_lock=writer_lock)
    finally:
        _release_validation_writer_lock(writer_lock)


def _atomic_write_text_locked(
    path: Path,
    text: str,
    *,
    writer_lock: _ValidationWriterLock,
) -> None:
    """Durably replace one text file through a pinned, revalidated parent."""
    logical_path = logical_filesystem_path(path)
    requested_parent = logical_path.parent.absolute()
    parent_status = _verify_validation_writer_lock_parent(writer_lock, requested_parent)
    resolved_parent = writer_lock.path.parent
    logical_path = resolved_parent / logical_path.name
    storage_path = internal_filesystem_path(logical_path, force_extended=True)
    payload = text.encode("utf-8")
    if len(payload) > MAX_VALIDATION_REPORT_WRITE_BYTES:
        raise OSError(f"validation report exceeds {MAX_VALIDATION_REPORT_WRITE_BYTES} bytes")
    if not stat.S_ISDIR(parent_status.st_mode) or stat.S_ISLNK(parent_status.st_mode):
        raise OSError(f"validation report parent is not a real directory: {storage_path.parent}")
    pending_identity = hashlib.sha256(storage_path.name.encode("utf-8")).hexdigest()[:32]
    temporary_name = f".clio-validation-{pending_identity}.pending"
    _prune_stale_validation_pending_files(
        resolved_parent,
        current_name=temporary_name,
        writer_lock=writer_lock,
    )

    if os.name == "posix":
        if writer_lock.parent_fd is None:  # pragma: no cover - platform invariant
            raise OSError("validation writer lock omitted its POSIX parent descriptor")
        directory_fd = os.dup(writer_lock.parent_fd)
        output_fd: int | None = None
        try:
            if not os.path.samestat(
                os.fstat(writer_lock.parent_fd),
                os.fstat(directory_fd),
            ):
                raise OSError("validation report parent differs from its writer lock")
            pending_exact = False
            pending_fd: int | None = None
            with suppress(FileNotFoundError):
                pending_fd = os.open(
                    temporary_name,
                    os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0),
                    dir_fd=directory_fd,
                )
            if pending_fd is not None:
                try:
                    pending_opened = os.fstat(pending_fd)
                    pending_linked = os.stat(
                        temporary_name,
                        dir_fd=directory_fd,
                        follow_symlinks=False,
                    )
                    if not (
                        stat.S_ISREG(pending_opened.st_mode)
                        and stat.S_ISREG(pending_linked.st_mode)
                        and pending_opened.st_nlink == 1
                        and pending_linked.st_nlink == 1
                        and pending_opened.st_uid == os.geteuid()
                        and pending_linked.st_uid == os.geteuid()
                        and stat.S_IMODE(pending_opened.st_mode) == 0o600
                        and stat.S_IMODE(pending_linked.st_mode) == 0o600
                        and os.path.samestat(pending_opened, pending_linked)
                    ):
                        raise OSError("validation report pending file is unsafe")
                    pending_value = os.read(pending_fd, len(payload) + 1)
                    pending_final = os.fstat(pending_fd)
                    pending_exact = bool(
                        pending_value == payload
                        and (
                            pending_opened.st_dev,
                            pending_opened.st_ino,
                            pending_opened.st_size,
                            pending_opened.st_mtime_ns,
                            pending_opened.st_ctime_ns,
                        )
                        == (
                            pending_final.st_dev,
                            pending_final.st_ino,
                            pending_final.st_size,
                            pending_final.st_mtime_ns,
                            pending_final.st_ctime_ns,
                        )
                    )
                finally:
                    os.close(pending_fd)
                if not pending_exact:
                    os.unlink(temporary_name, dir_fd=directory_fd)
                    os.fsync(directory_fd)
            if not pending_exact:
                output_fd = os.open(
                    temporary_name,
                    os.O_WRONLY
                    | os.O_CREAT
                    | os.O_EXCL
                    | getattr(os, "O_NOFOLLOW", 0)
                    | getattr(os, "O_CLOEXEC", 0),
                    0o600,
                    dir_fd=directory_fd,
                )
                os.fchmod(output_fd, 0o600)
                view = memoryview(payload)
                while view:
                    written = os.write(output_fd, view)
                    if written <= 0:
                        raise OSError("validation report write made no progress")
                    view = view[written:]
                os.fsync(output_fd)
                os.close(output_fd)
                output_fd = None
            os.replace(
                temporary_name,
                storage_path.name,
                src_dir_fd=directory_fd,
                dst_dir_fd=directory_fd,
            )
            os.fsync(directory_fd)
            final_fd = os.open(
                storage_path.name,
                os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0),
                dir_fd=directory_fd,
            )
            try:
                opened = os.fstat(final_fd)
                linked = os.stat(
                    storage_path.name,
                    dir_fd=directory_fd,
                    follow_symlinks=False,
                )
                reread = bytearray()
                while len(reread) <= len(payload):
                    chunk = os.read(final_fd, min(64 * 1024, len(payload) + 1 - len(reread)))
                    if not chunk:
                        break
                    reread.extend(chunk)
                final_opened = os.fstat(final_fd)
                final_linked = os.stat(
                    storage_path.name,
                    dir_fd=directory_fd,
                    follow_symlinks=False,
                )
                if not (
                    bytes(reread) == payload
                    and stat.S_ISREG(opened.st_mode)
                    and opened.st_nlink == 1
                    and linked.st_nlink == 1
                    and opened.st_uid == os.geteuid()
                    and linked.st_uid == os.geteuid()
                    and stat.S_IMODE(opened.st_mode) == 0o600
                    and stat.S_IMODE(linked.st_mode) == 0o600
                    and (opened.st_dev, opened.st_ino) == (linked.st_dev, linked.st_ino)
                    and (
                        opened.st_dev,
                        opened.st_ino,
                        opened.st_size,
                        opened.st_mtime_ns,
                        opened.st_ctime_ns,
                    )
                    == (
                        final_opened.st_dev,
                        final_opened.st_ino,
                        final_opened.st_size,
                        final_opened.st_mtime_ns,
                        final_opened.st_ctime_ns,
                    )
                    and (final_linked.st_dev, final_linked.st_ino)
                    == (final_opened.st_dev, final_opened.st_ino)
                    and os.path.samestat(
                        parent_status,
                        os.stat(storage_path.parent, follow_symlinks=False),
                    )
                ):
                    raise OSError("validation report changed during durable replacement")
            finally:
                os.close(final_fd)
        finally:
            if output_fd is not None:
                os.close(output_fd)
            os.close(directory_fd)
        return

    temporary = storage_path.with_name(temporary_name)
    parent_anchor: _WindowsValidationDirectoryAnchor | None = None
    try:
        if os.name == "nt":
            kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
            parent_anchor = _open_windows_validation_directory(
                resolved_parent,
                expected_status=parent_status,
            )
            if (
                writer_lock.windows_parent is None
                or parent_anchor.identity != writer_lock.windows_parent.identity
            ):
                raise OSError("validation report parent differs from its writer lock")
        if os.name == "nt":
            pending_exact = False
            try:
                pending_status = os.lstat(temporary)
            except FileNotFoundError:
                pending_status = None
            if pending_status is not None:
                if not (
                    stat.S_ISREG(pending_status.st_mode)
                    and not stat.S_ISLNK(pending_status.st_mode)
                    and pending_status.st_nlink == 1
                ):
                    raise OSError("validation report pending file is unsafe")
                pending_descriptor = open_private_configuration_windows_descriptor(temporary)
                with os.fdopen(pending_descriptor, "rb") as stream:
                    pending_opened = os.fstat(stream.fileno())
                    pending_linked = os.lstat(temporary)
                    if not (
                        stat.S_ISREG(pending_opened.st_mode)
                        and stat.S_ISREG(pending_linked.st_mode)
                        and pending_opened.st_nlink == 1
                        and pending_linked.st_nlink == 1
                        and os.path.samestat(pending_status, pending_opened)
                        and os.path.samestat(pending_opened, pending_linked)
                    ):
                        raise OSError("validation report pending file changed before recovery")
                    secured_pending = os.fstat(stream.fileno())
                    secured_linked = os.lstat(temporary)
                    if not (
                        secured_pending.st_nlink == 1
                        and secured_linked.st_nlink == 1
                        and os.path.samestat(pending_opened, secured_pending)
                        and os.path.samestat(secured_pending, secured_linked)
                    ):
                        raise OSError(
                            "validation report pending file changed while securing its ACL"
                        )
                    pending_value = stream.read(len(payload) + 1)
                    pending_final = os.fstat(stream.fileno())
                    if (
                        secured_pending.st_dev,
                        secured_pending.st_ino,
                        secured_pending.st_size,
                        secured_pending.st_mtime_ns,
                        secured_pending.st_ctime_ns,
                    ) != (
                        pending_final.st_dev,
                        pending_final.st_ino,
                        pending_final.st_size,
                        pending_final.st_mtime_ns,
                        pending_final.st_ctime_ns,
                    ):
                        raise OSError("validation report pending file changed during recovery")
                pending_exact = pending_value == payload
                if not pending_exact:
                    temporary.unlink()
            if not pending_exact:
                with open_private_atomic_file(temporary) as stream:
                    view = memoryview(payload)
                    while view:
                        written = stream.write(view)
                        if written <= 0:
                            raise OSError("validation report write made no progress")
                        view = view[written:]
                    stream.flush()
                    os.fsync(stream.fileno())
        else:
            with temporary.open("x", encoding="utf-8", newline="\n") as stream:
                stream.write(text)
                stream.flush()
                os.fsync(stream.fileno())
        if os.name == "nt":
            move_file_ex = kernel32.MoveFileExW
            move_file_ex.argtypes = [ctypes.c_wchar_p, ctypes.c_wchar_p, ctypes.c_uint32]
            move_file_ex.restype = ctypes.c_int
            if not move_file_ex(str(temporary), str(storage_path), 0x00000001 | 0x00000008):
                error_number = ctypes.get_last_error()
                raise OSError(error_number, ctypes.FormatError(error_number), str(storage_path))
        else:
            os.replace(temporary, storage_path)
        final_descriptor = (
            open_private_configuration_windows_descriptor(storage_path)
            if os.name == "nt"
            else os.open(storage_path, os.O_RDONLY | getattr(os, "O_CLOEXEC", 0))
        )
        with os.fdopen(final_descriptor, "rb") as stream:
            opened = os.fstat(stream.fileno())
            linked = os.stat(storage_path, follow_symlinks=False)
            if not (
                stat.S_ISREG(opened.st_mode)
                and stat.S_ISREG(linked.st_mode)
                and opened.st_nlink == 1
                and linked.st_nlink == 1
                and os.path.samestat(opened, linked)
            ):
                raise OSError("validation report replacement is not one exact regular file")
            reread = stream.read(len(payload) + 1)
            final_opened = os.fstat(stream.fileno())
        linked = os.stat(storage_path, follow_symlinks=False)
        if not (
            reread == payload
            and stat.S_ISREG(opened.st_mode)
            and stat.S_ISREG(final_opened.st_mode)
            and stat.S_ISREG(linked.st_mode)
            and opened.st_nlink == 1
            and final_opened.st_nlink == 1
            and linked.st_nlink == 1
            and (opened.st_dev, opened.st_ino, opened.st_size)
            == (final_opened.st_dev, final_opened.st_ino, final_opened.st_size)
            == (linked.st_dev, linked.st_ino, linked.st_size)
            and os.path.samestat(
                parent_status,
                os.stat(storage_path.parent, follow_symlinks=False),
            )
        ):
            raise OSError("validation report changed during durable replacement")
    finally:
        _close_windows_validation_directory(parent_anchor)

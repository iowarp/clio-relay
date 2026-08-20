"""Machine-readable evidence for live validation and release decisions."""

from __future__ import annotations

import base64
import ctypes
import hashlib
import json
import os
import re
import shutil
import stat
import subprocess
import sys
import tomllib
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Iterable
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
    _local_wheel_archive_path,
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
from clio_relay.redaction import redact_sensitive_values as redact_sensitive_values
from clio_relay.redaction import redact_url as _redact_url

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
    # POSIX virtual environments normally expose ``bin/python`` as a symlink to
    # the base interpreter. Keep the launcher location and its resolved target
    # as separate trust claims: the lexical executable must belong to the uv
    # tool environment, while the target may belong to that environment or to
    # the interpreter's exact base prefix.
    executable_in_environment = _within_or_equal(process_executable, prefix)
    executable_target_bound = _within_or_equal(process_executable_resolved, prefix) or (
        _within_or_equal(process_executable_resolved, base_prefix)
    )
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
        detected_kind in {InstallSourceKind.WHEEL, InstallSourceKind.PYPI, InstallSourceKind.VCS}
        and uv_path_verified
        and uv_stable
        and tool_directory is not None
        and tool_bin_directory is not None
        and environment_in_tool_directory
        and pyvenv_matches_uv
        and package_in_environment
        and executable_in_environment
        and executable_target_bound
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
        "executable_target_bound": executable_target_bound,
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
            "executable_target_bound",
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

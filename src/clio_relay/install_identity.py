"""Identify the running clio-relay: its version-control identity and install source.

:func:`detect_install_source` reads PEP 610 metadata, binds an artifact
digest to the installed files where it can, and records process-observed uv
launcher evidence. Installation receipts and run reports both use it.
"""

from __future__ import annotations

import os
import sys
from contextlib import suppress
from importlib import metadata, resources
from pathlib import Path
from typing import Any

from clio_relay import __version__
from clio_relay.artifact_identity_verification import (
    checkout_build_info as _checkout_build_info,
)
from clio_relay.artifact_identity_verification import (
    classify_install_source as _classify_install_source,
)
from clio_relay.artifact_identity_verification import (
    distribution_direct_url as _distribution_direct_url,
)
from clio_relay.artifact_identity_verification import (
    embedded_build_info as _embedded_build_info,
)
from clio_relay.artifact_identity_verification import (
    infer_running_artifact_identity as _infer_running_artifact_identity,
)
from clio_relay.artifact_identity_verification import (
    is_official_github_release_wheel as _is_official_github_release_wheel,
)
from clio_relay.artifact_identity_verification import (
    verify_running_artifact_identity as _verify_running_artifact_identity,
)
from clio_relay.errors import ConfigurationError
from clio_relay.process_ancestry import (
    strictly_contains as _strictly_contains,
)
from clio_relay.process_ancestry import (
    uv_executable_identity as _uv_executable_identity,
)
from clio_relay.process_ancestry import (
    uv_process_ancestor as _uv_process_ancestor,
)
from clio_relay.process_ancestry import (
    within_or_equal as _within_or_equal,
)
from clio_relay.regular_file_identity import (
    regular_file_identity as _regular_file_identity,
)
from clio_relay.run_report_models import InstallSource, InstallSourceKind, SoftwareIdentity
from clio_relay.uv_tool_receipt import (
    detect_persistent_uv_tool_receipt as _detect_persistent_uv_tool_receipt,
)
from clio_relay.uv_tool_receipt import (
    pyvenv_uv_version_marker as _pyvenv_uv_version,
)
from clio_relay.uv_tool_receipt import (
    uv_cache_dir as _uv_cache_dir,
)


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

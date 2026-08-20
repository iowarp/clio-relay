"""Verify a remote worker's receipt-bound components from their runtime evidence.

Extracted from ``installation.py`` (iowarp/clio-relay#231): this owns the
release-gate proof that a worker's component provenance (clio-kit,
jarvis-cd) names an immutable, hash-bound official release artifact, plus
the layer that reads the remote worker's self-reported runtime identity
(``_remote_component_runtime_identity``) back out of the installation-info
payload ``worker_runtime_verification.py`` assembles.
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import cast

from pydantic import ValidationError

from clio_relay.contract_gate import require_surface_contract
from clio_relay.errors import ConfigurationError
from clio_relay.installation_receipt_models import (
    ComponentArtifactIdentity,
    InstallReceipt,
    NativeJarvisExecutionCapability,
    _is_sha256_text,
)
from clio_relay.native_jarvis_contract import _native_capability_matches_component
from clio_relay.python_distribution_probe import _normalized_distribution_name

OFFICIAL_COMPONENT_RELEASE_REPOSITORIES = {
    "clio-kit": ("iowarp", "clio-kit"),
    "jarvis-cd": ("grc-iit", "jarvis-cd"),
}


def _remote_component_runtime_identity(
    info: dict[str, object],
    component_name: str,
) -> dict[str, object]:
    installation = info.get("installation")
    if not isinstance(installation, dict):
        return {}
    runtime = cast(dict[object, object], installation).get("component_runtime")
    if not isinstance(runtime, dict):
        return {}
    identity = cast(dict[object, object], runtime).get(component_name)
    if not isinstance(identity, dict):
        return {}
    return {str(key): value for key, value in cast(dict[object, object], identity).items()}


def verify_remote_package_progress_component(
    info: dict[str, object],
    receipt: InstallReceipt,
    *,
    component_name: str = "jarvis-cd",
) -> dict[str, object]:
    """Verify a legacy package-progress plugin for compatibility diagnostics only.

    This compatibility proof is intentionally not used by the 1.0 release gate.
    """
    component = receipt.component_artifacts.get(component_name)
    if component is None:
        raise ConfigurationError(
            f"worker installation omitted package progress component {component_name}"
        )
    if (
        not _is_official_github_release_component(component, component_name="jarvis-cd")
        or component.runtime_artifact_path is None
        or set(component.runtime_interpreters) != {"provider", "execution"}
        or set(component.runtime_executables) != {"jarvis"}
        or not component.entry_points
    ):
        raise ConfigurationError(
            f"worker package progress component {component_name} has incomplete provenance"
        )
    runtime = _remote_component_runtime_identity(info, component_name)
    for field in (
        "verified",
        "distribution_identity_verified",
        "entry_points_visible",
        "runtime_artifact_path_verified",
        "artifact_sha256_verified",
        "provider_interpreter_verified",
        "execution_interpreter_verified",
        "execution_distribution_identity_verified",
        "execution_entry_points_visible",
        "execution_source_verified",
        "jarvis_executable_verified",
    ):
        if runtime.get(field) is not True:
            raise ConfigurationError(
                f"worker package progress component {component_name} did not prove {field}"
            )
    return runtime


def verify_remote_clio_kit_native_execution_component(
    info: dict[str, object],
    receipt: InstallReceipt,
) -> dict[str, object]:
    """Require the exact receipt-bound clio-kit native JARVIS MCP contract.

    A receipt with no ``native_execution`` for a RECORDED, below-pin jarvis
    surface (``receipt.contract_surfaces["jarvis"].meets_requirement`` is
    False) is not a generic configuration problem: bootstrap already proved
    that surface's shipped identity and recorded the gap loudly
    (iowarp/clio-relay#242). Refuse with the typed
    :class:`clio_relay.errors.ContractSurfaceUnavailableError` naming
    surface/have/need instead of the generic message below, which stays for
    a receipt that never probed the surface at all.

    In dev mode (clio-relay#242 course correction), the below-pin refusal
    :func:`clio_relay.contract_gate.require_surface_contract` raises is
    deferred (logged at WARNING, never silent), and this returns the
    worker's self-reported runtime identity UNVERIFIED instead of falling
    into the generic "omitted" error next -- dev mode means the surface
    still serves.
    """
    component = receipt.component_artifacts.get("clio-kit")
    if component is None or component.native_execution is None:
        jarvis_surface = receipt.contract_surfaces.get("jarvis")
        if jarvis_surface is not None and not jarvis_surface.meets_requirement:
            require_surface_contract(jarvis_surface)
            return _remote_component_runtime_identity(info, "clio-kit")
        raise ConfigurationError("worker installation omitted the clio-kit native JARVIS contract")
    if not _native_capability_matches_component(
        component.native_execution,
        component_name="clio-kit",
    ):
        raise ConfigurationError("worker clio-kit native JARVIS contract is invalid")
    runtime = _remote_component_runtime_identity(info, "clio-kit")
    for field in (
        "artifact_identity_verified",
        "command_matches_receipt",
        "locked_server_runtime_verified",
        "native_execution_capability_verified",
    ):
        if runtime.get(field) is not True:
            raise ConfigurationError(
                f"worker clio-kit native JARVIS contract did not prove {field}"
            )
    try:
        observed = NativeJarvisExecutionCapability.model_validate(
            runtime.get("native_execution_capability")
        )
    except ValidationError as exc:
        raise ConfigurationError(
            f"worker clio-kit native JARVIS runtime contract was invalid: {exc}"
        ) from exc
    if observed != component.native_execution:
        raise ConfigurationError(
            "worker clio-kit native JARVIS runtime contract changed from its receipt"
        )
    return runtime


def verify_remote_native_jarvis_component(
    info: dict[str, object],
    receipt: InstallReceipt,
    *,
    component_name: str = "jarvis-cd",
) -> dict[str, object]:
    """Require immutable JARVIS-CD provenance and native execution API proof."""
    component = receipt.component_artifacts.get(component_name)
    if component is None:
        raise ConfigurationError(
            f"worker installation omitted native JARVIS component {component_name}"
        )
    if (
        not _is_official_github_release_component(
            component,
            component_name=component_name,
        )
        or component.runtime_artifact_path is None
        or "execution" not in component.runtime_interpreters
        or set(component.runtime_executables) != {"jarvis"}
        or component.native_execution is None
        or not _native_capability_matches_component(
            component.native_execution,
            component_name="jarvis-cd",
        )
    ):
        raise ConfigurationError(
            f"worker native JARVIS component {component_name} has incomplete provenance"
        )
    runtime = _remote_component_runtime_identity(info, component_name)
    for field in (
        "verified",
        "distribution_identity_verified",
        "runtime_artifact_path_verified",
        "artifact_sha256_verified",
        "execution_interpreter_verified",
        "execution_distribution_identity_verified",
        "execution_source_verified",
        "jarvis_executable_verified",
        "execution_native_execution_capability_verified",
        "native_execution_capability_verified",
    ):
        if runtime.get(field) is not True:
            raise ConfigurationError(
                f"worker native JARVIS component {component_name} did not prove {field}"
            )
    return runtime


def _is_released_component(component: ComponentArtifactIdentity) -> bool:
    version = component.distribution_version
    if (
        version is None
        or not _is_sha256_text(component.artifact_sha256)
        or component.runtime_artifact_path is None
        or not component.runtime_command
    ):
        return False
    if component.requested_source == "pypi":
        return component.install_spec == f"{component.distribution}=={version}"
    return _is_official_github_release_component(
        component,
        component_name="clio-kit",
    )


def _is_official_github_release_component(
    component: ComponentArtifactIdentity,
    *,
    component_name: str,
) -> bool:
    """Return whether provenance names one canonical, hash-bound release wheel."""
    normalized_name = _normalized_distribution_name(component_name)
    repository = OFFICIAL_COMPONENT_RELEASE_REPOSITORIES.get(normalized_name)
    version = component.distribution_version
    if (
        repository is None
        or version is None
        or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", version) is None
        or _normalized_distribution_name(component.distribution) != normalized_name
        or component.requested_source != "github_release"
        or not _is_sha256_text(component.artifact_sha256)
    ):
        return False
    owner, repository_name = repository
    wheel_distribution = normalized_name.replace("-", "_")
    filename = f"{wheel_distribution}-{version}-py3-none-any.whl"
    expected_url = (
        f"https://github.com/{owner}/{repository_name}/releases/download/v{version}/{filename}"
    )
    return component.artifact_filename == filename and component.install_spec == expected_url


def _worker_process_matches(pid: int) -> bool:
    """Return whether pid is a live clio-relay endpoint worker process."""
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    if os.name == "nt":
        return True
    try:
        command = (Path("/proc") / str(pid) / "cmdline").read_bytes().replace(b"\0", b" ")
    except OSError:
        return False
    normalized = command.decode("utf-8", errors="replace")
    return "clio-relay" in normalized and "endpoint" in normalized and "start" in normalized

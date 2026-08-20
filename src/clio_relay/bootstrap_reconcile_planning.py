"""Component-reuse reconcile planning.

``plan_bootstrap_reconcile`` is the single entry point that decides, from
live installation/runtime evidence, whether a bootstrap invocation is a
``repair``, ``relay-only``, ``component-upgrade``, or ``full`` reconcile --
deliberately kept as its own module (~370 lines, one function) since forcing
a further cut would mean rewriting its body, not moving it
(iowarp/clio-relay#255).
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import cast

from clio_relay.bootstrap_reconcile_activation_paths import _capture_reconcile_activation_paths
from clio_relay.bootstrap_reconcile_execution_identity import inspect_jarvis_state
from clio_relay.bootstrap_reconcile_inspection import _inspect_installation_identity
from clio_relay.bootstrap_reconcile_models import BootstrapDesiredState, BootstrapReconcilePlan
from clio_relay.bootstrap_reconcile_planning_support import (
    _full_plan,
    _managed_generation_jarvis_environment,
    _verify_jarvis_util_reuse,
)
from clio_relay.bootstrap_reconcile_primitives import (
    _path_is_directory_alias,
    _read_regular_bounded_with_identity,
    _stat_identity,
)
from clio_relay.bootstrap_reconcile_readiness import _verify_binary, _verify_uv
from clio_relay.bootstrap_reconcile_replacement_provider import (
    BootstrapReplacementProviderEvidence,
    _verify_bootstrap_replacement_provider,
)
from clio_relay.errors import ConfigurationError
from clio_relay.installation import installation_info
from clio_relay.validation_report import sha256_file


def plan_bootstrap_reconcile(
    desired: BootstrapDesiredState,
    *,
    home: Path | None = None,
    replacement_provider: BootstrapReplacementProviderEvidence | None = None,
) -> BootstrapReconcilePlan:
    """Plan a relay-only generation when every non-relay component verifies.

    This deliberately supports the first upgrade from a pre-generation install:
    an older receipt need not contain a deployment manifest, but every reusable
    component must have exact artifact and live-runtime evidence.
    """
    lexical_home = Path(os.path.abspath((home or Path.home()).expanduser()))
    resolved_home = lexical_home.resolve()
    reasons: list[str] = []
    upgrade_reasons: list[str] = []
    upgrade_components: set[str] = set()
    reusable_paths: dict[str, str] = {}
    receipt_path = resolved_home / ".local/share/clio-relay/install-receipt.json"
    replacement_verified = False
    if replacement_provider is not None:
        try:
            _verify_bootstrap_replacement_provider(
                desired,
                replacement_provider,
                home=lexical_home,
            )
        except (ConfigurationError, OSError, RuntimeError, ValueError) as exc:
            return _full_plan(desired, f"candidate replacement provider did not verify: {exc}")
        replacement_verified = True
    try:
        info = installation_info(receipt_path)
    except (ConfigurationError, OSError, ValueError) as exc:
        return _full_plan(desired, f"installation identity did not verify: {exc}")
    if info.get("receipt_matches_install") is not True and not replacement_verified:
        return _full_plan(desired, "install receipt does not match the running relay")
    raw_receipt = info.get("receipt")
    raw_runtime = info.get("component_runtime")
    if not isinstance(raw_receipt, dict) or not isinstance(raw_runtime, dict):
        return _full_plan(desired, "installation identity omitted component evidence")
    receipt = cast(dict[str, object], raw_receipt)
    runtime = cast(dict[str, object], raw_runtime)
    relay_runtime = runtime.get("clio-relay")
    if not replacement_verified and (
        not isinstance(relay_runtime, dict)
        or cast(dict[str, object], relay_runtime).get("persistent_tool_verified") is not True
        or cast(dict[str, object], relay_runtime).get("execution_runtime_verified") is not True
    ):
        relay_runtime_error = (
            cast(dict[str, object], relay_runtime).get("error")
            if isinstance(relay_runtime, dict)
            else None
        )
        reason = "clio-relay live provider is not reusable"
        if isinstance(relay_runtime_error, str) and relay_runtime_error:
            reason += ": " + relay_runtime_error[:512]
        return _full_plan(desired, reason)
    raw_components = receipt.get("components")
    raw_artifacts = receipt.get("component_artifacts")
    if not isinstance(raw_components, dict) or not isinstance(raw_artifacts, dict):
        return _full_plan(desired, "install receipt omitted reusable component artifacts")
    components = cast(dict[str, object], raw_components)
    artifacts = cast(dict[str, object], raw_artifacts)
    raw_relay_artifact = artifacts.get("clio-relay")
    relay_executable = None
    if isinstance(raw_relay_artifact, dict):
        raw_relay_executables = cast(dict[str, object], raw_relay_artifact).get(
            "runtime_executables"
        )
        if isinstance(raw_relay_executables, dict):
            relay_executable = cast(dict[str, object], raw_relay_executables).get("clio-relay")
    expected_relay_executable = lexical_home / ".local/bin/clio-relay"
    if not replacement_verified and (
        not isinstance(relay_executable, str) or relay_executable != str(expected_relay_executable)
    ):
        return _full_plan(desired, "clio-relay launcher is not bound to its install receipt")
    relay_execution_reusable = False
    resolved_relay_artifact: Path | None = None
    if isinstance(raw_relay_artifact, dict):
        relay_artifact = cast(dict[str, object], raw_relay_artifact)
        relay_artifact_path = relay_artifact.get("runtime_artifact_path")
        relay_artifact_sha256 = relay_artifact.get("artifact_sha256")
        relay_execution_runtime = runtime.get("clio-relay")
        if (
            isinstance(relay_artifact_path, str)
            and isinstance(relay_artifact_sha256, str)
            and desired.relay_artifact_sha256 is not None
            and relay_artifact.get("install_spec") == desired.relay_install_spec
            and relay_artifact_sha256 == desired.relay_artifact_sha256
            and isinstance(relay_execution_runtime, dict)
            and cast(dict[str, object], relay_execution_runtime).get("execution_runtime_verified")
            is True
        ):
            try:
                lexical_relay_artifact = Path(relay_artifact_path).expanduser()
                relay_artifact_before = lexical_relay_artifact.lstat()
                resolved_relay_artifact = lexical_relay_artifact.resolve(strict=True)
                relay_execution_reusable = (
                    not lexical_relay_artifact.is_symlink()
                    and resolved_relay_artifact.is_file()
                    and sha256_file(resolved_relay_artifact) == relay_artifact_sha256
                    and _stat_identity(lexical_relay_artifact.lstat())
                    == _stat_identity(relay_artifact_before)
                )
            except (OSError, RuntimeError, ValueError):
                relay_execution_reusable = False
            if relay_execution_reusable and resolved_relay_artifact is not None:
                reusable_paths["clio-relay_artifact"] = str(resolved_relay_artifact)
    expected_components = {
        "clio-kit": (desired.clio_kit_version, desired.clio_kit_artifact_sha256),
        "jarvis-cd": (desired.jarvis_cd_version, desired.jarvis_cd_wheel_sha256),
    }
    for component, (expected_version, expected_digest) in expected_components.items():
        raw_artifact = artifacts.get(component)
        if isinstance(raw_artifact, dict):
            artifact = cast(dict[str, object], raw_artifact)
            raw_interpreters = artifact.get("runtime_interpreters")
            raw_executables = artifact.get("runtime_executables")
            if isinstance(raw_interpreters, dict):
                for name, value in cast(dict[str, object], raw_interpreters).items():
                    if isinstance(value, str) and value:
                        reusable_paths[f"{component}_{name}_interpreter"] = value
            if isinstance(raw_executables, dict):
                for name, value in cast(dict[str, object], raw_executables).items():
                    if isinstance(value, str) and value:
                        reusable_paths[f"{component}_{name}_executable"] = value
        if components.get(component) != expected_version:
            reason = f"{component} version requires a staged upgrade"
            reasons.append(reason)
            upgrade_reasons.append(reason)
            upgrade_components.add(component)
            continue
        if not isinstance(raw_artifact, dict):
            reasons.append(f"{component} artifact identity is missing")
            continue
        artifact = cast(dict[str, object], raw_artifact)
        artifact_path = artifact.get("runtime_artifact_path")
        if artifact.get("artifact_sha256") != expected_digest or not isinstance(artifact_path, str):
            reasons.append(f"{component} artifact identity is not reusable")
            continue
        try:
            lexical_path = Path(artifact_path).expanduser()
            details = lexical_path.lstat()
            if lexical_path.is_symlink() or not lexical_path.is_file():
                raise ConfigurationError("artifact is not one regular file")
            path = lexical_path.resolve(strict=True)
            if _stat_identity(lexical_path.lstat()) != _stat_identity(details):
                raise ConfigurationError("artifact changed while resolving")
            if sha256_file(path) != expected_digest:
                raise ConfigurationError("artifact changed")
        except (ConfigurationError, OSError, RuntimeError, ValueError):
            reasons.append(f"{component} artifact did not reverify")
            continue
        reusable_paths[f"{component}_artifact"] = str(path)
    if components.get("jarvis-util") != desired.jarvis_util_commit:
        reasons.append("jarvis-util commit is not reusable")
    else:
        _verify_jarvis_util_reuse(
            resolved_home,
            desired=desired,
            reusable_paths=reusable_paths,
            reasons=reasons,
        )

    if "clio-kit" not in upgrade_components:
        clio_kit_runtime = runtime.get("clio-kit")
        if not isinstance(clio_kit_runtime, dict) or any(
            cast(dict[str, object], clio_kit_runtime).get(flag) is not True
            for flag in (
                "artifact_identity_verified",
                "command_matches_receipt",
                "locked_server_runtime_verified",
                "native_execution_capability_verified",
                "persistent_tool_verified",
            )
        ):
            reasons.append("clio-kit live runtime is not reusable")
    if "jarvis-cd" not in upgrade_components:
        jarvis_runtime = runtime.get("jarvis-cd")
        if (
            not isinstance(jarvis_runtime, dict)
            or cast(dict[str, object], jarvis_runtime).get("verified") is not True
        ):
            reasons.append("JARVIS-CD live execution runtime is not reusable")

    _verify_binary(
        resolved_home / ".local/bin/frpc",
        desired.frpc_sha256,
        label="frpc",
        reasons=reasons,
    )
    _verify_binary(
        resolved_home / ".local/bin/frps",
        desired.frps_sha256,
        label="frps",
        reasons=reasons,
    )
    _verify_uv(resolved_home / ".local/bin/uv", desired=desired, reasons=reasons)
    try:
        jarvis_state = inspect_jarvis_state(desired, home=resolved_home)
    except ConfigurationError as exc:
        raise ConfigurationError(
            f"existing JARVIS state is incompatible with bootstrap: {exc}"
        ) from exc
    if not jarvis_state.initialized:
        reasons.append("JARVIS is not initialized")
    legacy_python_text = reusable_paths.get("jarvis-cd_execution_interpreter")
    legacy_executable_text = reusable_paths.get("jarvis-cd_jarvis_executable")
    legacy_python = (
        Path(legacy_python_text).expanduser()
        if legacy_python_text is not None
        else resolved_home
        / ".local/share/clio-relay/jarvis-venv"
        / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
    )
    legacy_executable = (
        Path(legacy_executable_text).expanduser()
        if legacy_executable_text is not None
        else legacy_python.parent / ("jarvis.exe" if os.name == "nt" else "jarvis")
    )
    lexical_legacy_venv = legacy_python.parent.parent
    supported_execution_roots: set[Path] = set()
    supported_legacy_venv = resolved_home / ".local/share/clio-relay/jarvis-venv"
    try:
        if supported_legacy_venv.is_dir() and not _path_is_directory_alias(supported_legacy_venv):
            supported_execution_roots.add(supported_legacy_venv.resolve(strict=True))
    except (OSError, RuntimeError, ValueError):
        pass
    managed_execution_root = _managed_generation_jarvis_environment(
        receipt,
        execution_environment=lexical_legacy_venv,
        home=resolved_home,
    )
    if managed_execution_root is not None:
        supported_execution_roots.add(managed_execution_root)
    expected_legacy_executable = legacy_python.parent / (
        "jarvis.exe" if os.name == "nt" else "jarvis"
    )
    resolved_legacy_executable: Path | None = None
    try:
        legacy_python_before = legacy_python.lstat()
        legacy_executable_before = legacy_executable.lstat()
        expected_executable_before = expected_legacy_executable.lstat()
        resolved_legacy_venv = lexical_legacy_venv.resolve(strict=True)
        resolved_legacy_python_target = legacy_python.resolve(strict=True)
        legacy_python_target_before = resolved_legacy_python_target.lstat()
        resolved_legacy_executable = legacy_executable.resolve(strict=True)
        resolved_expected_executable = expected_legacy_executable.resolve(strict=True)
        executable_target_before = resolved_expected_executable.lstat()
        executable_payload, _executable_target_identity = _read_regular_bounded_with_identity(
            resolved_expected_executable,
            maximum=1024 * 1024,
        )
        legacy_boundary_reusable = (
            lexical_legacy_venv.is_absolute()
            and ".." not in lexical_legacy_venv.parts
            and not lexical_legacy_venv.is_symlink()
            and resolved_legacy_venv in supported_execution_roots
            and legacy_python.is_file()
            and expected_legacy_executable.is_file()
            and bool(executable_payload)
            and os.access(legacy_python, os.X_OK)
            and os.access(expected_legacy_executable, os.X_OK)
            and resolved_legacy_executable == resolved_expected_executable
            and _stat_identity(legacy_python.lstat()) == _stat_identity(legacy_python_before)
            and _stat_identity(legacy_executable.lstat())
            == _stat_identity(legacy_executable_before)
            and _stat_identity(expected_legacy_executable.lstat())
            == _stat_identity(expected_executable_before)
            and _stat_identity(resolved_legacy_python_target.lstat())
            == _stat_identity(legacy_python_target_before)
            and _stat_identity(resolved_expected_executable.lstat())
            == _stat_identity(executable_target_before)
        )
    except (ConfigurationError, OSError, RuntimeError, ValueError):
        legacy_boundary_reusable = False
    if not legacy_boundary_reusable or resolved_legacy_executable is None:
        reasons.append("legacy JARVIS execution environment is not reusable")
    else:
        reusable_paths["jarvis_execution_environment"] = str(lexical_legacy_venv)
        reusable_paths["jarvis_execution_python"] = str(legacy_python)
        reusable_paths["jarvis_execution_executable"] = str(expected_legacy_executable)

    if reasons:
        if upgrade_reasons and reasons == upgrade_reasons:
            try:
                activation_paths = _capture_reconcile_activation_paths(home=lexical_home)
            except (ConfigurationError, OSError, RuntimeError, ValueError) as exc:
                return _full_plan(desired, f"legacy activation boundary is not reusable: {exc}")
            return BootstrapReconcilePlan(
                mode="component-upgrade",
                desired_fingerprint=desired.fingerprint,
                reasons=upgrade_reasons,
                component_actions={
                    "clio-relay": "replace",
                    "jarvis-cd": "replace",
                    "jarvis-util": "reuse",
                    "clio-kit": "replace",
                    "frp": "reuse",
                    "uv": "reuse",
                },
                reusable_paths=reusable_paths,
                activation_paths=activation_paths,
            )
        return BootstrapReconcilePlan(
            mode="full",
            desired_fingerprint=desired.fingerprint,
            reasons=reasons,
            component_actions={
                "clio-relay": "replace",
                "jarvis-cd": "replace",
                "jarvis-util": "replace",
                "clio-kit": "replace",
                "frp": "replace",
                "uv": "replace",
            },
        )
    exact_install_reasons: list[str] = []
    _inspect_installation_identity(desired, info, exact_install_reasons)
    if not exact_install_reasons:
        return BootstrapReconcilePlan(
            mode="repair",
            desired_fingerprint=desired.fingerprint,
            reasons=["deployment components match; queue or worker readiness requires repair"],
            component_actions={
                "clio-relay": "reuse",
                "jarvis-cd": "reuse",
                "jarvis-util": "reuse",
                "clio-kit": "reuse",
                "frp": "reuse",
                "uv": "reuse",
            },
            reusable_paths=reusable_paths,
        )
    try:
        activation_paths = _capture_reconcile_activation_paths(home=lexical_home)
    except (ConfigurationError, OSError, RuntimeError, ValueError) as exc:
        return _full_plan(desired, f"legacy activation boundary is not reusable: {exc}")
    if not relay_execution_reusable:
        return BootstrapReconcilePlan(
            mode="component-upgrade",
            desired_fingerprint=desired.fingerprint,
            reasons=["relay JARVIS execution runtime requires a staged replacement"],
            component_actions={
                "clio-relay": "replace",
                "jarvis-cd": "replace",
                "jarvis-util": "reuse",
                "clio-kit": "replace",
                "frp": "reuse",
                "uv": "reuse",
            },
            reusable_paths=reusable_paths,
            activation_paths=activation_paths,
        )
    return BootstrapReconcilePlan(
        mode="relay-only",
        desired_fingerprint=desired.fingerprint,
        reasons=["relay desired identity changed; all non-relay components reverified"],
        component_actions={
            "clio-relay": "replace",
            "jarvis-cd": "reuse",
            "jarvis-util": "reuse",
            "clio-kit": "reuse",
            "frp": "reuse",
            "uv": "reuse",
        },
        reusable_paths=reusable_paths,
        activation_paths=activation_paths,
    )

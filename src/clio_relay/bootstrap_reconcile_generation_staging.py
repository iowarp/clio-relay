"""Staged-generation reverification and idempotent activation finish.

``inspect_prepared_generation`` reverifies a content-addressed generation
before any activation fence; ``finish_staged_activation`` reverifies and
idempotently finishes activation plus the exact JARVIS repository migration
(iowarp/clio-relay#255).
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import cast

from clio_relay.bootstrap_reconcile_activation_paths import (
    _verify_stable_symlink,
    reconcile_staged_activation_links,
)
from clio_relay.bootstrap_reconcile_builtin_repos import _relay_owned_jarvis_builtin_repositories
from clio_relay.bootstrap_reconcile_constants import LEGACY_MANAGED_JARVIS_REPO_PATH
from clio_relay.bootstrap_reconcile_execution_identity import (
    execution_environment_identity,
    jarvis_wrapper_payload,
)
from clio_relay.bootstrap_reconcile_models import BootstrapDesiredState, BootstrapReconcilePlan
from clio_relay.bootstrap_reconcile_primitives import (
    _expand_home,
    _read_regular_bounded,
    _require_sha256,
    _stat_identity,
)
from clio_relay.bootstrap_reconcile_repository import reconcile_managed_jarvis_repository
from clio_relay.errors import ConfigurationError
from clio_relay.installation import installation_info
from clio_relay.validation_report import sha256_file


def inspect_prepared_generation(
    desired: BootstrapDesiredState,
    *,
    generation: Path,
    legacy_execution_identity: dict[str, object],
) -> dict[str, object]:
    """Reverify a content-addressed generation before any activation fence."""
    resolved_generation = generation.resolve(strict=True)
    if generation.is_symlink() or not generation.is_dir():
        raise ConfigurationError("prepared generation is not one owned directory")
    prepared_path = generation / ".prepared"
    manifest_path = generation / "manifest.json"
    receipt_path = generation / "install-receipt.json"
    prepared = _read_regular_bounded(prepared_path, maximum=1024)
    if prepared != (desired.fingerprint + "\n").encode("ascii"):
        raise ConfigurationError("prepared generation marker fingerprint changed")
    raw_manifest = _read_regular_bounded(manifest_path, maximum=4 * 1024 * 1024)
    try:
        raw_value = cast(object, json.loads(raw_manifest))
    except json.JSONDecodeError as exc:
        raise ConfigurationError("prepared generation manifest is invalid") from exc
    if not isinstance(raw_value, dict):
        raise ConfigurationError("prepared generation manifest is not an object")
    manifest = cast(dict[str, object], raw_value)
    if set(manifest) != {
        "schema_version",
        "fingerprint",
        "plan",
        "legacy_execution_identity",
        "active_execution_identity",
        "jarvis_wrapper_sha256",
        "install_receipt",
        "install_receipt_sha256",
    }:
        raise ConfigurationError("prepared generation manifest has an unknown shape")
    if not (
        manifest.get("schema_version") == "clio-relay.bootstrap-generation.v1"
        and manifest.get("fingerprint") == desired.fingerprint
        and manifest.get("legacy_execution_identity") == legacy_execution_identity
        and manifest.get("install_receipt") == str(receipt_path)
        and manifest.get("install_receipt_sha256") == sha256_file(receipt_path)
    ):
        raise ConfigurationError("prepared generation manifest identity changed")
    plan = manifest.get("plan")
    if (
        not isinstance(plan, dict)
        or cast(dict[str, object], plan).get("desired_fingerprint") != desired.fingerprint
    ):
        raise ConfigurationError("prepared generation plan identity changed")
    raw_active_identity = manifest.get("active_execution_identity")
    if not isinstance(raw_active_identity, dict):
        raise ConfigurationError("prepared generation omitted active execution identity")
    active_identity = cast(dict[str, object], raw_active_identity)
    raw_active_root = active_identity.get("root")
    raw_executables = active_identity.get("executables")
    if not isinstance(raw_active_root, str) or not isinstance(raw_executables, dict):
        raise ConfigurationError("prepared generation omitted active execution boundary")
    typed_executables = cast(dict[str, object], raw_executables)
    if set(typed_executables) != {"python", "jarvis"}:
        raise ConfigurationError("prepared generation active executable set changed")
    raw_python = typed_executables.get("python")
    raw_jarvis = typed_executables.get("jarvis")
    raw_python_path = (
        cast(dict[str, object], raw_python).get("lexical_path")
        if isinstance(raw_python, dict)
        else None
    )
    raw_jarvis_path = (
        cast(dict[str, object], raw_jarvis).get("lexical_path")
        if isinstance(raw_jarvis, dict)
        else None
    )
    if not isinstance(raw_python_path, str) or not isinstance(raw_jarvis_path, str):
        raise ConfigurationError("prepared generation omitted active interpreter identity")
    recomputed_active_identity = execution_environment_identity(
        Path(raw_active_root),
        executables={
            "python": Path(raw_python_path),
            "jarvis": Path(raw_jarvis_path),
        },
    )
    if recomputed_active_identity != active_identity:
        raise ConfigurationError("prepared generation active execution identity changed")
    jarvis_payload = jarvis_wrapper_payload(Path(raw_python_path))
    jarvis_wrapper = generation / "bin/jarvis"
    wrapper_bytes = _read_regular_bounded(jarvis_wrapper, maximum=64 * 1024)
    wrapper_sha256 = hashlib.sha256(wrapper_bytes).hexdigest()
    if (
        wrapper_bytes != jarvis_payload
        or manifest.get("jarvis_wrapper_sha256") != wrapper_sha256
        or not os.access(jarvis_wrapper, os.X_OK)
    ):
        raise ConfigurationError("prepared generation JARVIS wrapper identity changed")

    info = installation_info(receipt_path)
    receipt = info.get("receipt")
    runtime = info.get("component_runtime")
    if not isinstance(receipt, dict) or not isinstance(runtime, dict):
        raise ConfigurationError("prepared generation omitted runtime identity")
    typed_receipt = cast(dict[str, object], receipt)
    typed_runtime = cast(dict[str, object], runtime)
    receipt_checks = {
        "receipt_matches_install": info.get("receipt_matches_install") is True,
        "deployment_fingerprint": (
            typed_receipt.get("deployment_fingerprint") == desired.fingerprint
        ),
        "deployment_manifest": (
            typed_receipt.get("deployment_manifest") == desired.model_dump(mode="json")
        ),
        "generation": typed_receipt.get("generation") == desired.fingerprint,
    }
    failed_receipt_checks = sorted(
        name for name, verified in receipt_checks.items() if not verified
    )
    if failed_receipt_checks:
        raise ConfigurationError(
            "prepared generation install receipt identity changed: "
            + ", ".join(failed_receipt_checks)
        )
    raw_artifacts = typed_receipt.get("component_artifacts")
    raw_jarvis_artifact = (
        cast(dict[str, object], raw_artifacts).get("jarvis-cd")
        if isinstance(raw_artifacts, dict)
        else None
    )
    raw_interpreters = (
        cast(dict[str, object], raw_jarvis_artifact).get("runtime_interpreters")
        if isinstance(raw_jarvis_artifact, dict)
        else None
    )
    receipt_execution_python = (
        cast(dict[str, object], raw_interpreters).get("execution")
        if isinstance(raw_interpreters, dict)
        else None
    )
    if (
        not isinstance(receipt_execution_python, str)
        or receipt_execution_python != raw_python_path
        or not Path(receipt_execution_python).is_absolute()
        or os.path.normpath(receipt_execution_python) != receipt_execution_python
        or any(character in receipt_execution_python for character in "\x00\r\n")
    ):
        raise ConfigurationError(
            "prepared active JARVIS interpreter is not bound to its install receipt"
        )
    relay_runtime = typed_runtime.get("clio-relay")
    clio_kit_runtime = typed_runtime.get("clio-kit")
    jarvis_runtime = typed_runtime.get("jarvis-cd")
    if not (
        isinstance(relay_runtime, dict)
        and cast(dict[str, object], relay_runtime).get("persistent_tool_verified") is True
        and cast(dict[str, object], relay_runtime).get("execution_runtime_verified") is True
        and isinstance(clio_kit_runtime, dict)
        and cast(dict[str, object], clio_kit_runtime).get("persistent_tool_verified") is True
        and cast(dict[str, object], clio_kit_runtime).get("native_execution_capability_verified")
        is True
        and isinstance(jarvis_runtime, dict)
        and cast(dict[str, object], jarvis_runtime).get("verified") is True
    ):
        raise ConfigurationError("prepared generation runtime identity changed")
    launcher_targets: dict[str, str] = {}
    for name in ("clio-relay", "clio-kit"):
        launcher = generation / "bin" / name
        try:
            before = launcher.lstat()
            if not launcher.is_symlink():
                raise ConfigurationError(f"prepared generation launcher is invalid: {name}")
            target = launcher.resolve(strict=True)
            if not target.is_file() or not os.access(target, os.X_OK):
                raise ConfigurationError(f"prepared generation launcher is invalid: {name}")
            if _stat_identity(launcher.lstat()) != _stat_identity(before):
                raise ConfigurationError(f"prepared generation launcher changed: {name}")
        except OSError as exc:
            raise ConfigurationError(f"prepared generation launcher is invalid: {name}") from exc
        launcher_targets[name] = str(target)
    launcher_targets["jarvis"] = str(jarvis_wrapper)
    if resolved_generation != generation.resolve(strict=True):
        raise ConfigurationError("prepared generation changed during inspection")
    return {
        "fingerprint": desired.fingerprint,
        "manifest_sha256": hashlib.sha256(raw_manifest).hexdigest(),
        "install_receipt_sha256": sha256_file(receipt_path),
        "launcher_targets": launcher_targets,
    }


def finish_staged_activation(
    desired: BootstrapDesiredState,
    *,
    generation: Path,
    expected_manifest_sha256: str,
    home: Path | None = None,
) -> dict[str, object]:
    """Reverify and idempotently finish activation plus exact repo migration."""
    try:
        _require_sha256(expected_manifest_sha256, field="expected_manifest_sha256")
    except ValueError as exc:
        raise ConfigurationError(str(exc)) from exc
    raw_manifest = _read_regular_bounded(generation / "manifest.json", maximum=4 * 1024 * 1024)
    if hashlib.sha256(raw_manifest).hexdigest() != expected_manifest_sha256:
        raise ConfigurationError("prepared generation manifest changed before activation")
    try:
        raw_value = cast(object, json.loads(raw_manifest))
    except json.JSONDecodeError as exc:
        raise ConfigurationError("prepared generation manifest is invalid") from exc
    if not isinstance(raw_value, dict):
        raise ConfigurationError("prepared generation manifest is not an object")
    manifest = cast(dict[str, object], raw_value)
    try:
        plan = BootstrapReconcilePlan.model_validate(manifest.get("plan"))
    except ValueError as exc:
        raise ConfigurationError("prepared generation reconcile plan is invalid") from exc
    if plan.desired_fingerprint != desired.fingerprint or plan.mode not in {
        "relay-only",
        "component-upgrade",
    }:
        raise ConfigurationError("prepared generation reconcile plan changed")
    legacy_venv = plan.reusable_paths.get("jarvis_execution_environment")
    legacy_python = plan.reusable_paths.get("jarvis_execution_python")
    legacy_jarvis = plan.reusable_paths.get("jarvis_execution_executable")
    if not all(
        isinstance(value, str) and value for value in (legacy_venv, legacy_python, legacy_jarvis)
    ):
        raise ConfigurationError("prepared generation omitted its legacy execution boundary")
    assert legacy_venv is not None
    assert legacy_python is not None
    assert legacy_jarvis is not None
    legacy_identity = execution_environment_identity(
        Path(legacy_venv),
        executables={"python": Path(legacy_python), "jarvis": Path(legacy_jarvis)},
    )
    if manifest.get("legacy_execution_identity") != legacy_identity:
        raise ConfigurationError("legacy execution environment changed before activation")
    inspection = inspect_prepared_generation(
        desired,
        generation=generation,
        legacy_execution_identity=legacy_identity,
    )
    if inspection.get("manifest_sha256") != expected_manifest_sha256:
        raise ConfigurationError("prepared generation inspection did not bind its manifest")
    activation = reconcile_staged_activation_links(plan, generation=generation, home=home)
    lexical_home = Path(os.path.abspath((home or Path.home()).expanduser()))
    managed_repo = _expand_home(desired.managed_jarvis_repo, lexical_home)
    legacy_managed_repo = _expand_home(LEGACY_MANAGED_JARVIS_REPO_PATH, lexical_home)
    previous_repo = lexical_home / ".local/src/clio-relay/jarvis-packages/clio_relay"
    relay_owned_builtin_repos = _relay_owned_jarvis_builtin_repositories(
        home=lexical_home,
        execution_environments=(Path(legacy_venv), generation / "jarvis-venv"),
    )
    repositories = reconcile_managed_jarvis_repository(
        _expand_home(desired.jarvis_root, lexical_home) / "repos.yaml",
        managed_repo,
        managed_builtin_repo=_expand_home(desired.jarvis_root, lexical_home) / "builtin",
        previous_managed_repos=(
            legacy_managed_repo,
            previous_repo,
            *relay_owned_builtin_repos,
        ),
        exchange_identity=desired.fingerprint,
    )
    expected_managed_target = (
        lexical_home / ".local/share/clio-relay/current/source/jarvis-packages/clio_relay"
    )
    _verify_stable_symlink(
        managed_repo,
        expected=expected_managed_target,
        label="relay-managed repository",
    )
    reported_managed_repo = _expand_home(desired.managed_jarvis_repo, lexical_home)
    reported_managed_target = (
        lexical_home / ".local/share/clio-relay/current/source/jarvis-packages/clio_relay"
    )
    actions = activation.get("actions")
    if not isinstance(actions, dict):  # pragma: no cover - produced above
        raise ConfigurationError("staged activation omitted link actions")
    return {
        "schema_version": "clio-relay.bootstrap-staged-activation.v1",
        "prepared_inspection": inspection,
        "activation": activation,
        "jarvis_repository": {
            "link_action": cast(dict[str, object], actions).get("managed_repo"),
            "link": str(reported_managed_repo),
            "target": str(reported_managed_target),
            "repositories": repositories,
        },
    }

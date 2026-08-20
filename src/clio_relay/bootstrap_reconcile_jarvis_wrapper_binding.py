"""Active-generation JARVIS wrapper binding and receipt-bound interpreter resolution.

``_verify_active_generation_jarvis_wrapper`` binds the active launcher and
manifest to immutable installed evidence; ``resolve_receipt_bound_jarvis_python``
is the public entry point a worker uses to resolve its exact, receipt-bound
JARVIS interpreter (or ``None`` for a deliberately unmanaged launcher)
(iowarp/clio-relay#255).
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import shutil
import stat
from pathlib import Path
from typing import cast

from clio_relay.bootstrap_reconcile_activation_paths import _verify_stable_symlink
from clio_relay.bootstrap_reconcile_execution_identity import (
    execution_environment_identity,
    jarvis_wrapper_payload,
)
from clio_relay.bootstrap_reconcile_models import BootstrapDesiredState, BootstrapReconcilePlan
from clio_relay.bootstrap_reconcile_primitives import _read_regular_bounded, _stat_identity
from clio_relay.errors import ConfigurationError
from clio_relay.installation import installation_info
from clio_relay.validation_report import sha256_file

logger = logging.getLogger(__name__)


def _verify_active_generation_jarvis_wrapper(
    generation: Path,
    *,
    desired: BootstrapDesiredState,
    installation: dict[str, object] | None,
) -> None:
    """Bind the active launcher and manifest to immutable installed evidence."""
    raw_manifest = _read_regular_bounded(generation / "manifest.json", maximum=4 * 1024 * 1024)
    try:
        raw_value = cast(object, json.loads(raw_manifest))
    except json.JSONDecodeError as exc:
        raise ConfigurationError("active generation manifest is invalid") from exc
    if not isinstance(raw_value, dict):
        raise ConfigurationError("active generation manifest is not an object")
    manifest = cast(dict[str, object], raw_value)
    expected_manifest_keys = {
        "schema_version",
        "fingerprint",
        "plan",
        "legacy_execution_identity",
        "active_execution_identity",
        "jarvis_wrapper_sha256",
        "install_receipt",
        "install_receipt_sha256",
    }
    if set(manifest) != expected_manifest_keys:
        raise ConfigurationError("active generation manifest has an unknown shape")
    expected_receipt_path = generation / "install-receipt.json"
    manifest_receipt_value = manifest.get("install_receipt")
    manifest_receipt_matches = False
    if isinstance(manifest_receipt_value, str):
        manifest_receipt_path = Path(manifest_receipt_value)
        if (
            manifest_receipt_path.is_absolute()
            and os.path.normpath(manifest_receipt_value) == manifest_receipt_value
            and not any(character in manifest_receipt_value for character in "\x00\r\n")
            and manifest_receipt_path.name == "install-receipt.json"
        ):
            try:
                manifest_receipt_matches = manifest_receipt_path.parent.resolve(
                    strict=True
                ) == generation.resolve(strict=True) and manifest_receipt_path.resolve(
                    strict=True
                ) == expected_receipt_path.resolve(strict=True)
            except (OSError, RuntimeError, ValueError):
                manifest_receipt_matches = False
    if not (
        manifest.get("schema_version") == "clio-relay.bootstrap-generation.v1"
        and manifest.get("fingerprint") == desired.fingerprint
        and manifest_receipt_matches
        and manifest.get("install_receipt_sha256") == sha256_file(expected_receipt_path)
    ):
        raise ConfigurationError("active generation manifest identity changed")
    raw_plan = manifest.get("plan")
    try:
        plan = BootstrapReconcilePlan.model_validate(raw_plan)
    except ValueError as exc:
        raise ConfigurationError("active generation reconcile plan is invalid") from exc
    if plan.desired_fingerprint != desired.fingerprint:
        raise ConfigurationError("active generation reconcile plan identity changed")
    raw_identity = manifest.get("active_execution_identity")
    if not isinstance(raw_identity, dict):
        raise ConfigurationError("active generation omitted active execution identity")
    identity = cast(dict[str, object], raw_identity)
    raw_root = identity.get("root")
    raw_executables = identity.get("executables")
    if not isinstance(raw_root, str) or not isinstance(raw_executables, dict):
        raise ConfigurationError("active generation omitted active execution boundary")
    typed_executables = cast(dict[str, object], raw_executables)
    if set(typed_executables) != {"python", "jarvis"}:
        raise ConfigurationError("active generation executable set changed")
    raw_python = typed_executables.get("python")
    raw_jarvis = typed_executables.get("jarvis")
    python_path = (
        cast(dict[str, object], raw_python).get("lexical_path")
        if isinstance(raw_python, dict)
        else None
    )
    jarvis_path = (
        cast(dict[str, object], raw_jarvis).get("lexical_path")
        if isinstance(raw_jarvis, dict)
        else None
    )
    if not isinstance(python_path, str) or not isinstance(jarvis_path, str):
        raise ConfigurationError("active generation omitted JARVIS interpreter identity")
    recomputed_identity = execution_environment_identity(
        Path(raw_root),
        executables={
            "python": Path(python_path),
            "jarvis": Path(jarvis_path),
        },
    )
    if recomputed_identity != identity:
        raise ConfigurationError("active generation JARVIS execution identity changed")
    receipt = installation.get("receipt") if installation is not None else None
    raw_artifacts = (
        cast(dict[str, object], receipt).get("component_artifacts")
        if isinstance(receipt, dict)
        else None
    )
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
        or receipt_execution_python != python_path
        or not Path(receipt_execution_python).is_absolute()
        or os.path.normpath(receipt_execution_python) != receipt_execution_python
        or any(character in receipt_execution_python for character in "\x00\r\n")
    ):
        raise ConfigurationError("active JARVIS interpreter is not bound to its install receipt")
    expected_payload = jarvis_wrapper_payload(Path(python_path))
    wrapper = generation / "bin/jarvis"
    observed_payload = _read_regular_bounded(wrapper, maximum=64 * 1024)
    expected_sha256 = manifest.get("jarvis_wrapper_sha256")
    if (
        observed_payload != expected_payload
        or not isinstance(expected_sha256, str)
        or hashlib.sha256(observed_payload).hexdigest() != expected_sha256
        or not os.access(wrapper, os.X_OK)
    ):
        raise ConfigurationError("active generation JARVIS wrapper identity changed")


def _relay_managed_jarvis_launcher_selected(
    stable_launcher: Path,
    *,
    lexical_home: Path,
) -> bool:
    """Return whether one stable launcher names the relay activation namespace.

    ``~/.local/bin`` is a conventional location shared by uv, pipx, and manual
    installations.  The path alone therefore proves no relay ownership.  A
    relay-managed launcher is distinguished by its stable symlink target: the
    current relay generation, or the equivalent direct generation target used
    by an older activation.  Receipt and generation validation remains the
    caller's fail-closed responsibility after this ownership boundary is met.
    """
    try:
        before = stable_launcher.lstat()
    except (OSError, RuntimeError, ValueError):
        return False
    if not stat.S_ISLNK(before.st_mode):
        return False
    try:
        raw_target = os.readlink(stable_launcher)
    except (OSError, RuntimeError, ValueError):
        return False
    target = Path(raw_target)
    if not target.is_absolute():
        target = stable_launcher.parent / target
    target = Path(os.path.abspath(target))
    try:
        canonical_home = lexical_home.resolve(strict=True)
    except (OSError, RuntimeError, ValueError):
        return False
    home_aliases = {lexical_home, canonical_home}
    current_targets = {root / ".local/share/clio-relay/current/bin/jarvis" for root in home_aliases}
    relay_target = target in current_targets
    if not relay_target and target.name == "jarvis" and target.parent.name == "bin":
        generation = target.parent.parent
        generation_name = generation.name
        generation_roots = {root / ".local/share/clio-relay/generations" for root in home_aliases}
        relay_target = bool(
            generation.parent in generation_roots
            and len(generation_name) == 64
            and all(character in "0123456789abcdef" for character in generation_name)
        )
    if not relay_target:
        return False
    try:
        after = stable_launcher.lstat()
    except (OSError, RuntimeError, ValueError) as exc:
        raise ConfigurationError(
            "relay-managed JARVIS launcher changed during ownership inspection"
        ) from exc
    if _stat_identity(after) != _stat_identity(before):
        raise ConfigurationError(
            "relay-managed JARVIS launcher changed during ownership inspection"
        )
    return True


def resolve_receipt_bound_jarvis_python(
    jarvis_bin: str,
    *,
    home: Path | None = None,
) -> str | None:
    """Return the verified interpreter for a relay-managed JARVIS launcher.

    Non-managed launchers return ``None`` so an explicitly unmanaged provider can
    retain its compatibility discovery.  A conventional ``~/.local/bin/jarvis``
    file or external symlink is not relay ownership evidence.  Once the exact
    relay activation symlink is selected, every receipt, generation, runtime,
    and wrapper mismatch fails closed instead of falling back to an ambient
    Python interpreter.
    """
    lexical_home = Path(os.path.abspath((home or Path.home()).expanduser()))
    launcher = Path(jarvis_bin).expanduser()
    if not launcher.is_absolute():
        discovered = shutil.which(jarvis_bin)
        if discovered is None:
            return None
        launcher = Path(discovered)
    lexical_launcher = Path(os.path.abspath(launcher))
    stable_launcher = lexical_home / ".local/bin/jarvis"
    if lexical_launcher != stable_launcher:
        try:
            canonical_launcher = (
                lexical_launcher.parent.resolve(strict=True) / lexical_launcher.name
            )
            canonical_stable = stable_launcher.parent.resolve(strict=True) / stable_launcher.name
        except (OSError, RuntimeError, ValueError):
            return None
        if canonical_launcher != canonical_stable:
            return None
    if not _relay_managed_jarvis_launcher_selected(
        stable_launcher,
        lexical_home=lexical_home,
    ):
        return None

    stable_receipt = lexical_home / ".local/share/clio-relay/install-receipt.json"
    try:
        installation = installation_info(stable_receipt)
    except (ConfigurationError, OSError, ValueError) as exc:
        raise ConfigurationError("relay-managed JARVIS installation receipt is invalid") from exc
    if (
        installation.get("schema_version") != "clio-relay.installation-info.v1"
        or installation.get("receipt_matches_install") is not True
    ):
        from clio_relay.dev_mode import dev_mode_enabled

        if not dev_mode_enabled():
            raise ConfigurationError(
                "relay-managed JARVIS installation receipt does not match this worker"
            )
    raw_receipt = installation.get("receipt")
    raw_runtime = installation.get("component_runtime")
    if not isinstance(raw_receipt, dict) or not isinstance(raw_runtime, dict):
        raise ConfigurationError("relay-managed JARVIS installation identity is incomplete")
    receipt = cast(dict[str, object], raw_receipt)
    runtime = cast(dict[str, object], raw_runtime)
    jarvis_runtime = runtime.get("jarvis-cd")
    if (
        not isinstance(jarvis_runtime, dict)
        or cast(dict[str, object], jarvis_runtime).get("verified") is not True
    ):
        # Dev mode defers this identity enforcement LOUDLY, exactly like the
        # receipt_matches_install sibling above -- a hand-deployed runtime is
        # the sanctioned dev-mode state (2026-08-19: the un-deferred form
        # crash-looped the ares worker every ~10s after a hand-install, #250
        # family). Enforcement returns with the release recipe.
        from clio_relay.dev_mode import dev_mode_enabled

        if not dev_mode_enabled():
            raise ConfigurationError("relay-managed JARVIS runtime did not verify its receipt")
        logger.warning(
            "jarvis runtime receipt verification deferred reason=deferred_dev_mode "
            "check=jarvis_runtime_verified"
        )
    relay_runtime = runtime.get("clio-relay")
    if (
        not isinstance(relay_runtime, dict)
        or cast(dict[str, object], relay_runtime).get("execution_runtime_verified") is not True
    ):
        from clio_relay.dev_mode import dev_mode_enabled

        if not dev_mode_enabled():
            raise ConfigurationError(
                "relay-managed JARVIS execution runtime did not verify its relay receipt"
            )
        logger.warning(
            "jarvis execution runtime verification deferred reason=deferred_dev_mode "
            "check=execution_runtime_verified"
        )

    raw_manifest = receipt.get("deployment_manifest")
    fingerprint = receipt.get("deployment_fingerprint")
    generation_name = receipt.get("generation")
    try:
        desired = BootstrapDesiredState.model_validate(raw_manifest)
    except ValueError as exc:
        raise ConfigurationError(
            "relay-managed JARVIS receipt omitted a valid deployment manifest"
        ) from exc
    if (
        not isinstance(fingerprint, str)
        or fingerprint != desired.fingerprint
        or generation_name != fingerprint
    ):
        raise ConfigurationError("relay-managed JARVIS generation identity changed")

    generation = lexical_home / ".local/share/clio-relay/generations" / fingerprint
    _verify_stable_symlink(
        lexical_home / ".local/share/clio-relay/current",
        expected=generation,
        label="current generation pointer",
    )
    _verify_stable_symlink(
        stable_receipt,
        expected=generation / "install-receipt.json",
        label="stable install receipt",
    )
    _verify_stable_symlink(
        stable_launcher,
        expected=generation / "bin/jarvis",
        label="stable JARVIS launcher",
    )
    _verify_active_generation_jarvis_wrapper(
        generation,
        desired=desired,
        installation=installation,
    )

    raw_artifacts = receipt.get("component_artifacts")
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
    execution_python = (
        cast(dict[str, object], raw_interpreters).get("execution")
        if isinstance(raw_interpreters, dict)
        else None
    )
    if not isinstance(execution_python, str):
        raise ConfigurationError("relay-managed JARVIS receipt omitted its interpreter")
    return execution_python

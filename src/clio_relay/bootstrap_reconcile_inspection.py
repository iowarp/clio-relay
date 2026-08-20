"""Read-only exact-noop bootstrap inspection and active-generation identity.

``inspect_exact_bootstrap_noop`` is the top-level verification entry point;
``proven_active_generation_mismatch``/``_inspect_installation_identity``/
``_inspect_active_generation`` are the identity checks it (and reconcile
planning) compose (iowarp/clio-relay#255).
"""

from __future__ import annotations

import os
import stat
from pathlib import Path
from typing import cast

from clio_relay.bootstrap_reconcile_activation_paths import _verify_stable_symlink
from clio_relay.bootstrap_reconcile_execution_identity import inspect_jarvis_state
from clio_relay.bootstrap_reconcile_jarvis_wrapper_binding import (
    _verify_active_generation_jarvis_wrapper,
)
from clio_relay.bootstrap_reconcile_models import (
    BootstrapDesiredState,
    BootstrapInspection,
    BootstrapReadinessEvidence,
)
from clio_relay.bootstrap_reconcile_primitives import (
    _expand_home,
    _path_is_directory_alias,
    _stat_identity,
)
from clio_relay.bootstrap_reconcile_readiness import (
    _queue_readiness_verified,
    _verify_binary,
    _verify_uv,
    _worker_readiness_verified,
)
from clio_relay.errors import ConfigurationError
from clio_relay.installation import installation_info
from clio_relay.validation_report import sha256_file


def inspect_exact_bootstrap_noop(
    desired: BootstrapDesiredState,
    *,
    home: Path | None = None,
    service_was_active: bool | None,
    service_was_enabled: bool | None = None,
    queue_evidence: dict[str, object] | None,
    worker_evidence: dict[str, object] | None,
    installation_snapshot: dict[str, object] | None = None,
) -> BootstrapInspection:
    """Verify that the exact desired deployment is live without mutating it.

    The caller obtains systemd state and invokes the bounded queue/worker read
    commands.  No scheduler command is part of this contract.
    """
    lexical_home = Path(os.path.abspath((home or Path.home()).expanduser()))
    reasons: list[str] = []
    receipt_path = lexical_home / ".local/share/clio-relay/install-receipt.json"
    install_receipt_sha256: str | None = None
    info: dict[str, object] | None = installation_snapshot
    try:
        install_receipt_sha256 = sha256_file(receipt_path)
        if info is None:
            info = installation_info(receipt_path)
    except (ConfigurationError, OSError, ValueError) as exc:
        reasons.append(f"installation identity did not verify: {exc}")
    if info is not None:
        _inspect_installation_identity(desired, info, reasons)
    active_generation, current_generation_target = _inspect_active_generation(
        desired,
        home=lexical_home,
        installation=info,
        reasons=reasons,
    )

    _verify_binary(
        lexical_home / ".local/bin/frpc",
        desired.frpc_sha256,
        label="frpc",
        reasons=reasons,
    )
    _verify_binary(
        lexical_home / ".local/bin/frps",
        desired.frps_sha256,
        label="frps",
        reasons=reasons,
    )
    _verify_uv(lexical_home / ".local/bin/uv", desired=desired, reasons=reasons)
    jarvis_state = inspect_jarvis_state(desired, home=lexical_home)
    if not jarvis_state.initialized:
        reasons.append("JARVIS is not initialized")
    if not jarvis_state.managed_repo_registered:
        reasons.append("the exact relay-managed JARVIS repository is not registered")
    if not jarvis_state.managed_builtin_repo_registered:
        reasons.append("the exact JARVIS-managed builtin repository slot is not registered")

    queue_ready = _queue_readiness_verified(queue_evidence)
    if not queue_ready:
        reasons.append("queue migration readiness did not verify")
    worker_ready: bool | None
    if desired.worker_service is None:
        worker_ready = None
    elif service_was_active is False:
        worker_ready = False
        reasons.append("managed endpoint service is inactive")
    elif service_was_active is True:
        worker_ready = _worker_readiness_verified(worker_evidence, desired.cluster)
        if not worker_ready:
            reasons.append("active endpoint worker readiness did not verify")
    else:
        worker_ready = None
        reasons.append("managed endpoint service state was not observed")
    if desired.worker_service is not None:
        if service_was_enabled is False:
            reasons.append("managed endpoint service is disabled")
        elif service_was_enabled is None:
            reasons.append("managed endpoint service enabled state was not observed")

    return BootstrapInspection(
        exact_match=not reasons,
        desired_fingerprint=desired.fingerprint,
        reasons=reasons,
        install_receipt_sha256=install_receipt_sha256,
        active_generation=active_generation,
        current_generation_target=current_generation_target,
        jarvis_state=jarvis_state,
        readiness=BootstrapReadinessEvidence(
            service_name=desired.worker_service,
            service_was_active=service_was_active,
            service_was_enabled=service_was_enabled,
            queue_ready=queue_ready,
            queue=queue_evidence,
            worker_ready=worker_ready,
            worker=worker_evidence,
        ),
    )


def proven_active_generation_mismatch(
    desired: BootstrapDesiredState,
    *,
    home: Path | None = None,
) -> str | None:
    """Return a safely identified active generation only when it differs.

    This deliberately proves only that the stable ``current`` pointer names a
    different relay-managed generation.  It never proves an exact deployment
    match, so callers may use it solely to request normal payload
    reconciliation before performing the comparatively expensive runtime
    identity inspection.
    """
    lexical_home = Path(os.path.abspath((home or Path.home()).expanduser()))
    share = lexical_home / ".local/share/clio-relay"
    generations_path = share / "generations"
    current = share / "current"
    try:
        before = current.lstat()
        if not stat.S_ISLNK(before.st_mode):
            return None
        raw_target = os.readlink(current)
        if not raw_target or any(character in raw_target for character in "\x00\r\n"):
            return None
        target = Path(raw_target)
        if not target.is_absolute():
            target = current.parent / target
        generations_before = generations_path.lstat()
        if not stat.S_ISDIR(generations_before.st_mode) or _path_is_directory_alias(
            generations_path
        ):
            return None
        generations = generations_path.resolve(strict=True)
        resolved_target = target.resolve(strict=True)
        relative = resolved_target.relative_to(generations)
        generation = relative.name
        generation_path = generations_path / generation
        generation_before = generation_path.lstat()
        if (
            not stat.S_ISDIR(generation_before.st_mode)
            or _path_is_directory_alias(generation_path)
            or generation_path.resolve(strict=True) != resolved_target
        ):
            return None
        after = current.lstat()
        generations_after = generations_path.lstat()
        generation_after = generation_path.lstat()
    except (OSError, RuntimeError, ValueError):
        return None
    if (
        _stat_identity(after) != _stat_identity(before)
        or _stat_identity(generations_after) != _stat_identity(generations_before)
        or _stat_identity(generation_after) != _stat_identity(generation_before)
        or len(relative.parts) != 1
        or len(generation) != 64
        or any(character not in "0123456789abcdef" for character in generation)
        or generation == desired.fingerprint
    ):
        return None
    getuid = getattr(os, "getuid", None)
    if callable(getuid) and any(
        details.st_uid != getuid()
        for details in (
            generations_after,
            generation_after,
        )
    ):
        return None
    return generation


def _inspect_installation_identity(
    desired: BootstrapDesiredState,
    info: dict[str, object],
    reasons: list[str],
) -> None:
    if info.get("schema_version") != "clio-relay.installation-info.v1":
        reasons.append("installation identity schema does not match")
    if info.get("receipt_matches_install") is not True:
        reasons.append("install receipt does not match the running relay")
    raw_receipt = info.get("receipt")
    if not isinstance(raw_receipt, dict):
        reasons.append("installation identity omitted its receipt")
        return
    receipt = cast(dict[str, object], raw_receipt)
    if receipt.get("deployment_fingerprint") != desired.fingerprint:
        reasons.append("desired deployment fingerprint changed")
    if receipt.get("deployment_manifest") != desired.model_dump(mode="json"):
        reasons.append("desired deployment manifest changed")
    if receipt.get("install_spec") != desired.relay_install_spec:
        reasons.append("relay install specification changed")
    if desired.relay_artifact_sha256 is not None and (
        receipt.get("artifact_sha256") != desired.relay_artifact_sha256
    ):
        reasons.append("relay artifact digest changed")
    raw_components = receipt.get("components")
    expected_components = {
        "clio-kit": desired.clio_kit_version,
        "jarvis-cd": desired.jarvis_cd_version,
        "jarvis-util": desired.jarvis_util_commit,
    }
    if not isinstance(raw_components, dict):
        reasons.append("install receipt omitted component identities")
    else:
        components = cast(dict[str, object], raw_components)
        for component, expected in expected_components.items():
            if components.get(component) != expected:
                reasons.append(f"{component} identity changed")
    raw_runtime = info.get("component_runtime")
    if not isinstance(raw_runtime, dict):
        reasons.append("installation identity omitted component runtime evidence")
        return
    runtime = cast(dict[str, object], raw_runtime)
    relay_runtime = runtime.get("clio-relay")
    if (
        not isinstance(relay_runtime, dict)
        or cast(dict[str, object], relay_runtime).get("persistent_tool_verified") is not True
    ):
        reasons.append("clio-relay persistent tool identity did not verify")
    elif cast(dict[str, object], relay_runtime).get("execution_runtime_verified") is not True:
        reasons.append("clio-relay JARVIS execution runtime did not verify")
    clio_kit_runtime = runtime.get("clio-kit")
    required_clio_kit = (
        "artifact_identity_verified",
        "command_matches_receipt",
        "locked_server_runtime_verified",
        "native_execution_capability_verified",
        "persistent_tool_verified",
    )
    if not isinstance(clio_kit_runtime, dict) or any(
        cast(dict[str, object], clio_kit_runtime).get(flag) is not True
        for flag in required_clio_kit
    ):
        reasons.append("clio-kit runtime identity did not verify")
    jarvis_runtime = runtime.get("jarvis-cd")
    if (
        not isinstance(jarvis_runtime, dict)
        or cast(dict[str, object], jarvis_runtime).get("verified") is not True
    ):
        reasons.append("JARVIS-CD execution identity did not verify")


def _inspect_active_generation(
    desired: BootstrapDesiredState,
    *,
    home: Path,
    installation: dict[str, object] | None,
    reasons: list[str],
) -> tuple[str | None, str | None]:
    """Verify the stable pointer and receipt name the desired generation."""
    active_generation: str | None = None
    raw_receipt = installation.get("receipt") if installation is not None else None
    if isinstance(raw_receipt, dict):
        raw_generation = cast(dict[str, object], raw_receipt).get("generation")
        if isinstance(raw_generation, str) and raw_generation:
            active_generation = raw_generation
    if active_generation != desired.fingerprint:
        reasons.append("install receipt does not name the desired active generation")

    current = home / ".local/share/clio-relay/current"
    try:
        expected_target = (
            home / ".local/share/clio-relay/generations" / desired.fingerprint
        ).resolve(strict=True)
        resolved_target = _verify_stable_symlink(
            current,
            expected=expected_target,
            label="current generation pointer",
        )
        _verify_stable_symlink(
            home / ".local/share/clio-relay/install-receipt.json",
            expected=expected_target / "install-receipt.json",
            label="stable install receipt",
        )
        for executable in ("clio-relay", "jarvis"):
            _verify_stable_symlink(
                home / ".local/bin" / executable,
                expected=expected_target / "bin" / executable,
                label=f"stable {executable} launcher",
            )
        _verify_stable_symlink(
            _expand_home(desired.managed_jarvis_repo, home),
            expected=expected_target / "source/jarvis-packages/clio_relay",
            label="relay-managed JARVIS repository",
        )
        _verify_active_generation_jarvis_wrapper(
            expected_target,
            desired=desired,
            installation=installation,
        )
    except (ConfigurationError, OSError, RuntimeError, ValueError) as exc:
        reasons.append(str(exc))
        return active_generation, None
    return active_generation, str(resolved_target)

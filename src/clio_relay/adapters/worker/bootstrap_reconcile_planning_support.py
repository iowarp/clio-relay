"""Reconcile-planning helpers: legacy JARVIS environment reuse and identity commands.

``_managed_generation_jarvis_environment`` resolves a receipt-bound relay
generation's real (possibly retained) JARVIS execution root;
``_bounded_subprocess``/``_full_plan`` are the shared identity-command and
full-replan primitives ``plan_bootstrap_reconcile`` composes
(iowarp/clio-relay#255).
"""

from __future__ import annotations

import os
import stat
from pathlib import Path

from clio_relay.adapters.worker.bootstrap_reconcile_models import (
    BootstrapDesiredState,
    BootstrapReconcilePlan,
)
from clio_relay.adapters.worker.bootstrap_reconcile_primitives import (
    _is_sha256,
    _path_is_directory_alias,
    _stat_identity,
)
from clio_relay.bounded_process import (
    BoundedProcessError,
    BoundedProcessOutputLimit,
    run_bounded_process,
)
from clio_relay.errors import ConfigurationError


def _managed_generation_jarvis_environment(
    receipt: dict[str, object],
    *,
    execution_environment: Path,
    home: Path,
) -> Path | None:
    """Return a receipt-bound relay generation's real JARVIS execution root.

    Relay-only generations intentionally retain the JARVIS environment from
    the preceding component generation.  The active receipt therefore binds
    both the active generation and an execution root that may belong to a
    different retained generation.
    """
    active_generation = receipt.get("generation")
    if not isinstance(active_generation, str) or not _is_sha256(active_generation):
        return None
    relay_root = home / ".local/share/clio-relay"
    generations_root = relay_root / "generations"
    active_generation_root = generations_root / active_generation
    current = relay_root / "current"
    environment = Path(os.path.abspath(execution_environment.expanduser()))
    try:
        current_before = current.lstat()
        generations_before = generations_root.lstat()
        active_generation_before = active_generation_root.lstat()
        environment_before = environment.lstat()
        if (
            not _path_is_directory_alias(current)
            or not stat.S_ISDIR(generations_before.st_mode)
            or _path_is_directory_alias(generations_root)
            or not stat.S_ISDIR(active_generation_before.st_mode)
            or _path_is_directory_alias(active_generation_root)
            or not stat.S_ISDIR(environment_before.st_mode)
            or _path_is_directory_alias(environment)
            or not environment.is_absolute()
            or ".." in environment.parts
        ):
            return None
        resolved_generations = generations_root.resolve(strict=True)
        resolved_active_generation = active_generation_root.resolve(strict=True)
        resolved_environment = environment.resolve(strict=True)
        if current.resolve(strict=True) != resolved_active_generation:
            return None
        relative_environment = resolved_environment.relative_to(resolved_generations)
        if (
            len(relative_environment.parts) != 2
            or relative_environment.parts[1] != "jarvis-venv"
            or not _is_sha256(relative_environment.parts[0])
        ):
            return None
        execution_generation_root = generations_root / relative_environment.parts[0]
        execution_generation_before = execution_generation_root.lstat()
        if (
            not stat.S_ISDIR(execution_generation_before.st_mode)
            or _path_is_directory_alias(execution_generation_root)
            or execution_generation_root.resolve(strict=True) != resolved_environment.parent
        ):
            return None
        current_after = current.lstat()
        generations_after = generations_root.lstat()
        active_generation_after = active_generation_root.lstat()
        execution_generation_after = execution_generation_root.lstat()
        environment_after = environment.lstat()
        if (
            _stat_identity(current_after) != _stat_identity(current_before)
            or _stat_identity(generations_after) != _stat_identity(generations_before)
            or _stat_identity(active_generation_after) != _stat_identity(active_generation_before)
            or _stat_identity(execution_generation_after)
            != _stat_identity(execution_generation_before)
            or _stat_identity(environment_after) != _stat_identity(environment_before)
        ):
            return None
        getuid = getattr(os, "getuid", None)
        if callable(getuid) and any(
            details.st_uid != getuid()
            for details in (
                generations_after,
                active_generation_after,
                execution_generation_after,
                environment_after,
            )
        ):
            return None
    except (OSError, RuntimeError, ValueError):
        return None
    return resolved_environment


def _full_plan(desired: BootstrapDesiredState, reason: str) -> BootstrapReconcilePlan:
    return BootstrapReconcilePlan(
        mode="full",
        desired_fingerprint=desired.fingerprint,
        reasons=[reason],
        component_actions={
            "clio-relay": "replace",
            "jarvis-cd": "replace",
            "clio-kit": "replace",
            "frp": "replace",
            "uv": "replace",
        },
    )


def _bounded_subprocess(command: list[str], *, maximum: int) -> str:
    """Run one identity command while retaining at most bounded output bytes."""
    if maximum < 1:
        raise ValueError("identity command output bound must be positive")
    try:
        completed = run_bounded_process(
            command,
            timeout_seconds=20,
            stdout_maximum_bytes=maximum,
            stderr_maximum_bytes=4096,
        )
    except BoundedProcessOutputLimit as exc:
        raise ConfigurationError(
            f"identity command output exceeded its bound: {command[0]}"
        ) from exc
    except (OSError, BoundedProcessError) as exc:
        raise ConfigurationError(f"identity command failed: {command[0]}") from exc
    if completed.returncode != 0:
        detail = completed.stderr.strip()
        raise ConfigurationError(
            f"identity command failed: {command[0]}" + (f": {detail}" if detail else "")
        )
    return completed.stdout.strip()

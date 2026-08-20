"""Reconcile-planning helpers: legacy JARVIS environment reuse and identity commands.

``_managed_generation_jarvis_environment`` resolves a receipt-bound relay
generation's real (possibly retained) JARVIS execution root;
``_verify_jarvis_util_reuse`` reverifies the jarvis-util checkout live;
``_bounded_subprocess``/``_full_plan`` are the shared identity-command and
full-replan primitives ``plan_bootstrap_reconcile`` composes
(iowarp/clio-relay#255).
"""

from __future__ import annotations

import json
import os
import stat
import subprocess
from pathlib import Path
from typing import cast
from urllib.parse import unquote, urlsplit

from clio_relay.bootstrap_reconcile_models import BootstrapDesiredState, BootstrapReconcilePlan
from clio_relay.bootstrap_reconcile_primitives import (
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
            "jarvis-util": "replace",
            "clio-kit": "replace",
            "frp": "replace",
            "uv": "replace",
        },
    )


def _verify_jarvis_util_reuse(
    home: Path,
    *,
    desired: BootstrapDesiredState,
    reusable_paths: dict[str, str],
    reasons: list[str],
) -> None:
    checkout = home / ".local/src/jarvis-util"
    try:
        if checkout.is_symlink() or not (checkout / ".git").is_dir():
            raise ConfigurationError("jarvis-util checkout is unavailable")
        commit = _bounded_subprocess(
            ["git", "-C", str(checkout), "rev-parse", "HEAD"],
            maximum=4096,
        )
        status = _bounded_subprocess(
            [
                "git",
                "-C",
                str(checkout),
                "status",
                "--porcelain=v1",
                "--untracked-files=all",
            ],
            maximum=1024 * 1024,
        )
        if commit != desired.jarvis_util_commit or status:
            raise ConfigurationError("jarvis-util checkout commit or cleanliness changed")
        receipt_python = reusable_paths.get("jarvis-cd_execution_interpreter")
        legacy_python = (
            Path(receipt_python).expanduser()
            if receipt_python is not None
            else home
            / ".local/share/clio-relay/jarvis-venv"
            / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
        )
        probe = _bounded_subprocess(
            [
                str(legacy_python),
                "-c",
                (
                    "import json; from importlib.metadata import distribution; "
                    "d=distribution('jarvis-util'); "
                    "print(json.dumps({'name':d.metadata['Name'],"
                    "'direct_url':d.read_text('direct_url.json'),"
                    "'record':d.read_text('RECORD') is not None}))"
                ),
            ],
            maximum=1024 * 1024,
        )
        raw_probe = cast(object, json.loads(probe))
        if not isinstance(raw_probe, dict):
            raise ConfigurationError("jarvis-util distribution probe is invalid")
        evidence = cast(dict[str, object], raw_probe)
        direct_url_text = evidence.get("direct_url")
        if not isinstance(direct_url_text, str) or evidence.get("record") is not True:
            raise ConfigurationError("jarvis-util distribution omitted source evidence")
        raw_direct_url = cast(object, json.loads(direct_url_text))
        if not isinstance(raw_direct_url, dict):
            raise ConfigurationError("jarvis-util direct-url evidence is invalid")
        source_url = cast(dict[str, object], raw_direct_url).get("url")
        if not isinstance(source_url, str):
            raise ConfigurationError("jarvis-util distribution source changed")
        parsed = urlsplit(source_url)
        source_path_text = unquote(parsed.path)
        if os.name == "nt" and len(source_path_text) > 2 and source_path_text[0] == "/":
            source_path_text = source_path_text[1:]
        if (
            parsed.scheme != "file"
            or parsed.netloc
            or parsed.query
            or parsed.fragment
            or Path(source_path_text).resolve() != checkout.resolve()
        ):
            raise ConfigurationError("jarvis-util distribution source changed")
    except (ConfigurationError, OSError, subprocess.SubprocessError, json.JSONDecodeError) as exc:
        reasons.append(f"jarvis-util live installation is not reusable: {exc}")
        return
    reusable_paths["jarvis_util_checkout"] = str(checkout.resolve())


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

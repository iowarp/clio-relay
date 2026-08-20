"""No-scheduler binary/version/queue/worker readiness verification.

Bounded, read-only verification helpers shared by the exact-noop inspection
and reconcile-planning paths: frpc/frps/uv binary identity, uv's reported
version, and the queue/worker readiness evidence shape
(iowarp/clio-relay#255).
"""

from __future__ import annotations

import os
from pathlib import Path

from clio_relay.bootstrap_reconcile_models import BootstrapDesiredState
from clio_relay.bounded_process import BoundedProcessError, run_bounded_process
from clio_relay.errors import ConfigurationError
from clio_relay.validation_report import sha256_file


def _queue_readiness_verified(evidence: dict[str, object] | None) -> bool:
    if evidence is None:
        return False
    return bool(
        evidence.get("schema_version") == "clio-relay.queue-readiness.v1"
        and evidence.get("complete") is True
        and evidence.get("sealed") is True
        and evidence.get("repair_required") is False
    )


def _worker_readiness_verified(
    evidence: dict[str, object] | None,
    cluster: str | None,
) -> bool:
    return bool(
        evidence is not None
        and evidence.get("schema_version")
        in {
            "clio-relay.worker-runtime-info.v1",
            "clio-relay.worker-readiness.v1",
        }
        and evidence.get("cluster") == cluster
        and evidence.get("fresh") is True
        and evidence.get("process_running") is True
        and evidence.get("identity_matches_current") is True
        and evidence.get("running") is True
    )


def _verify_binary(path: Path, expected: str, *, label: str, reasons: list[str]) -> None:
    try:
        if path.is_symlink() or not path.is_file() or not os.access(path, os.X_OK):
            raise ConfigurationError(f"{label} is not one regular executable")
        if sha256_file(path) != expected:
            raise ConfigurationError(f"{label} digest changed")
    except (ConfigurationError, OSError, ValueError) as exc:
        reasons.append(str(exc))


def _verify_uv(path: Path, *, desired: BootstrapDesiredState, reasons: list[str]) -> None:
    _verify_binary(path, desired.uv_sha256, label="uv", reasons=reasons)
    if any(reason.startswith("uv ") for reason in reasons):
        return
    try:
        completed = run_bounded_process(
            [str(path), "--version"],
            timeout_seconds=10,
            stdout_maximum_bytes=4096,
            stderr_maximum_bytes=4096,
        )
    except (OSError, BoundedProcessError) as exc:
        reasons.append(f"uv version probe failed: {exc}")
        return
    if completed.returncode != 0 or not _uv_version_output_matches(
        completed.stdout,
        expected_version=desired.uv_version,
    ):
        reasons.append("uv version changed")


def _uv_version_output_matches(value: str, *, expected_version: str) -> bool:
    """Match uv's pinned version with its optional bounded build target."""
    observed = value
    if observed.endswith("\r\n"):
        observed = observed[:-2]
    elif observed.endswith("\n"):
        observed = observed[:-1]
    if (
        not observed
        or observed != observed.strip()
        or any(ord(character) < 32 or ord(character) == 127 for character in observed)
    ):
        return False
    exact = f"uv {expected_version}"
    if observed == exact:
        return True
    prefix = exact + " ("
    if (
        len(observed) > len(prefix) + 128
        or not observed.startswith(prefix)
        or not observed.endswith(")")
    ):
        return False
    target = observed[len(prefix) : -1]
    return bool(
        target
        and all(
            character.isascii() and (character.isalnum() or character in {"-", "_", "."})
            for character in target
        )
    )

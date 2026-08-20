"""Query and terminate a persisted Linux systemd scope by exact identity.

Owner module for `clio_relay.process_containment` (iowarp/clio-relay#231).

`_systemctl_user`, `_terminate_linux_systemd_scope`, and
`_release_linux_systemd_scope` live in `process_containment_systemd_scope`
and are individually replaced by the test suite via `monkeypatch.setattr` on
the facade module -- so every call to one of them here goes through the live
facade module (`clio_relay.process_containment`) instead of a plain import,
keeping this module's dependency on its sibling one-directional (through the
facade only, never a direct import of `process_containment_systemd_scope`).
`_validated_systemd_cgroup_path` and `_linux_cgroup_process_ids` are
similarly replaced by the test suite, even though they live in this module's
own one-directional dependency `process_containment_systemd_core`.
"""

from __future__ import annotations

import re
from pathlib import Path

from clio_relay import process_containment as _pc
from clio_relay.process_containment_systemd_core import (
    _parse_systemd_properties,
    _validated_recorded_systemd_scope_path,
)
from clio_relay.process_containment_types import DISCOVERY_TIMEOUT_SECONDS


def recorded_linux_systemd_scope_process_ids(
    *,
    unit: str,
    cgroup_path: str,
    invocation_id: str,
    description: str,
) -> list[int]:
    """Return PIDs only after exact persistent systemd scope identity verification."""
    if (
        re.fullmatch(r"clio-relay-session-[A-Za-z0-9_-]+\.scope", unit) is None
        or re.fullmatch(r"[0-9a-f]{32}", invocation_id) is None
        or not description
        or len(description.encode("utf-8")) > 512
    ):
        raise RuntimeError("recorded persistent systemd scope identity is invalid")
    recorded_path = _validated_recorded_systemd_scope_path(cgroup_path, unit=unit)
    result = _pc._systemctl_user(
        [
            "show",
            unit,
            "--property=ControlGroup",
            "--property=InvocationID",
            "--property=Description",
            "--property=LoadState",
        ],
        timeout_seconds=DISCOVERY_TIMEOUT_SECONDS,
    )
    try:
        properties = _parse_systemd_properties(
            result.stdout,
            expected={"ControlGroup", "InvocationID", "Description", "LoadState"},
        )
    except RuntimeError:
        properties = {}
    if result.returncode != 0 or properties.get("LoadState") == "not-found":
        if recorded_path.exists():
            raise RuntimeError("recorded systemd unit vanished while its cgroup remained")
        return []
    if not (
        properties.get("LoadState") == "loaded"
        and properties.get("InvocationID") == invocation_id
        and properties.get("Description") == description
    ):
        raise RuntimeError("recorded systemd unit identity drifted or was reused")
    observed = _pc._validated_systemd_cgroup_path(properties.get("ControlGroup", ""), unit=unit)
    try:
        expected = recorded_path.resolve(strict=True)
    except OSError as exc:
        raise RuntimeError("recorded systemd cgroup path is unavailable") from exc
    if observed != expected:
        raise RuntimeError("recorded systemd ControlGroup drifted")
    return _pc._linux_cgroup_process_ids(observed)


def terminate_recorded_linux_systemd_scope(
    *,
    unit: str,
    cgroup_path: str,
    invocation_id: str,
    description: str,
) -> list[int]:
    """Terminate an exact persisted scope and prove its cgroup became absent or empty."""
    targeted = recorded_linux_systemd_scope_process_ids(
        unit=unit,
        cgroup_path=cgroup_path,
        invocation_id=invocation_id,
        description=description,
    )
    scope = Path(cgroup_path)
    if not targeted and not scope.exists():
        return []
    try:
        resolved_scope = scope.resolve(strict=True)
    except OSError as exc:
        raise RuntimeError("recorded systemd cgroup path is unavailable") from exc
    _pc._terminate_linux_systemd_scope(unit, resolved_scope)
    residual = recorded_linux_systemd_scope_process_ids(
        unit=unit,
        cgroup_path=cgroup_path,
        invocation_id=invocation_id,
        description=description,
    )
    if residual:
        raise RuntimeError(f"recorded systemd scope survived cleanup: {residual}")
    _pc._release_linux_systemd_scope(unit)
    return targeted


def adopt_linux_systemd_scope_identity(
    *,
    unit: str,
    description: str,
) -> dict[str, str] | None:
    """Recover an on-disk-predeclared scope before its launcher callback persisted identity."""
    if (
        re.fullmatch(r"clio-relay-session-[A-Za-z0-9_-]+\.scope", unit) is None
        or not description
        or len(description.encode("utf-8")) > 512
    ):
        raise RuntimeError("predeclared persistent systemd scope identity is invalid")
    result = _pc._systemctl_user(
        [
            "show",
            unit,
            "--property=ControlGroup",
            "--property=InvocationID",
            "--property=Description",
            "--property=LoadState",
        ],
        timeout_seconds=DISCOVERY_TIMEOUT_SECONDS,
    )
    try:
        properties = _parse_systemd_properties(
            result.stdout,
            expected={"ControlGroup", "InvocationID", "Description", "LoadState"},
        )
    except RuntimeError:
        properties = {}
    if result.returncode != 0 or properties.get("LoadState") == "not-found":
        return None
    if not (
        properties.get("LoadState") == "loaded"
        and properties.get("Description") == description
        and re.fullmatch(r"[0-9a-f]{32}", properties.get("InvocationID", ""))
    ):
        raise RuntimeError("predeclared systemd scope identity drifted or was reused")
    scope = _pc._validated_systemd_cgroup_path(properties.get("ControlGroup", ""), unit=unit)
    return {
        "systemd_unit": unit,
        "systemd_description": description,
        "systemd_invocation_id": properties["InvocationID"],
        "cgroup_path": str(scope),
    }

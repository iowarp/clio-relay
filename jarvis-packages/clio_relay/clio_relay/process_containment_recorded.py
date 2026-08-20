"""Reconnect to and terminate a process recorded before a relay restart.

Owner module for `clio_relay.process_containment` (iowarp/clio-relay#231).

`process_start_identity` is individually replaced by the test suite via
`monkeypatch.setattr` on the facade module -- including by tests that then
exercise `terminate_recorded_process_tree` and
`_terminate_recorded_windows_process_tree` defined right here, so both read
it through the live facade module (`clio_relay.process_containment`) rather
than calling the local definition directly. The same applies to the
systemd-scope termination/release functions this module reaches (which live
in `process_containment_systemd_scope`) and to `_current_posix_group`,
`_signal_posix_tree`, and `_wait_for_exit` (which live in
`process_containment_posix`). `_posix_process_group_ids` and
`_process_exists` are never individually replaced, so they stay plain
imports.
"""

from __future__ import annotations

import os
import signal
import subprocess
from pathlib import Path
from typing import cast

from clio_relay import process_containment as _pc
from clio_relay.process_containment_posix import _posix_process_group_ids, _process_exists
from clio_relay.process_containment_systemd_core import _validated_recorded_systemd_scope_path
from clio_relay.process_containment_systemd_query import recorded_linux_systemd_scope_process_ids
from clio_relay.process_containment_types import (
    DISCOVERY_TIMEOUT_SECONDS,
    TERMINATION_TIMEOUT_SECONDS,
)
from clio_relay.process_containment_windows import _windows_process_start_identity


def process_start_identity(process_id: int) -> str | None:
    """Return a stable per-process start identity, or ``None`` after exit."""
    if process_id <= 0:
        raise ValueError("process_id must be positive")
    if os.name == "nt":
        return _windows_process_start_identity(process_id)
    proc_stat = Path("/proc") / str(process_id) / "stat"
    try:
        raw = proc_stat.read_text(encoding="utf-8")
    except (FileNotFoundError, OSError, UnicodeDecodeError):
        raw = ""
    closing = raw.rfind(")")
    if closing >= 0:
        fields = raw[closing + 1 :].split()
        if len(fields) > 19:
            return f"linux-proc-start:{fields[19]}"
    try:
        result = subprocess.run(
            ["ps", "-p", str(process_id), "-o", "lstart="],
            check=False,
            capture_output=True,
            text=True,
            timeout=DISCOVERY_TIMEOUT_SECONDS,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise RuntimeError(f"could not inspect process start identity {process_id}: {exc}") from exc
    value = " ".join(result.stdout.split())
    if result.returncode == 0 and value:
        return f"posix-ps-start:{value}"
    if _process_exists(process_id):
        raise RuntimeError(f"process exists but its start identity is unavailable: {process_id}")
    return None


def _terminate_recorded_windows_process_tree(
    process_id: int,
    expected_start_identity: str,
) -> None:
    try:
        result = subprocess.run(
            ["taskkill", "/PID", str(process_id), "/T", "/F"],
            check=False,
            capture_output=True,
            text=True,
            timeout=TERMINATION_TIMEOUT_SECONDS,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise RuntimeError(f"could not terminate recorded process {process_id}: {exc}") from exc
    observed_identity = _pc.process_start_identity(process_id)
    if observed_identity is None:
        return
    if observed_identity != expected_start_identity:
        raise RuntimeError(
            f"refused cleanup for reused process id {process_id}: "
            f"expected {expected_start_identity}, observed {observed_identity}"
        )
    detail = (result.stderr or "").strip()
    suffix = f": {detail}" if detail else ""
    raise RuntimeError(f"recorded process survived cleanup: {process_id}{suffix}")


def terminate_recorded_process_tree(
    *,
    process_id: int,
    expected_start_identity: str,
    process_group_id: int | None,
    containment_mode: str | None = None,
    systemd_unit: str | None = None,
    cgroup_path: str | None = None,
    systemd_invocation_id: str | None = None,
    systemd_description: str | None = None,
) -> None:
    """Terminate a prior worker execution while refusing a reused process id."""
    observed_identity = _pc.process_start_identity(process_id)
    if observed_identity is not None and observed_identity != expected_start_identity:
        raise RuntimeError(
            f"refused cleanup for reused process id {process_id}: "
            f"expected {expected_start_identity}, observed {observed_identity}"
        )
    if containment_mode == "linux_systemd_scope":
        if (
            systemd_unit is None
            or not systemd_unit.startswith("clio-relay-")
            or not systemd_unit.endswith(".scope")
            or cgroup_path is None
        ):
            raise RuntimeError("recorded systemd execution has invalid scope identity")
        exact_persistent_identity = (
            systemd_invocation_id is not None and systemd_description is not None
        )
        if (systemd_invocation_id is None) is not (systemd_description is None):
            raise RuntimeError("recorded systemd execution has partial persistent identity")
        recorded_scope = _validated_recorded_systemd_scope_path(
            cgroup_path,
            unit=systemd_unit,
        )
        if exact_persistent_identity:
            existing_pids = recorded_linux_systemd_scope_process_ids(
                unit=systemd_unit,
                cgroup_path=cgroup_path,
                invocation_id=cast(str, systemd_invocation_id),
                description=cast(str, systemd_description),
            )
            if not existing_pids and not recorded_scope.exists():
                return
        if observed_identity is None and not recorded_scope.exists():
            return
        scope = recorded_scope.resolve(strict=True)
        _pc._terminate_linux_systemd_scope(systemd_unit, scope)
        residual = (
            recorded_linux_systemd_scope_process_ids(
                unit=systemd_unit,
                cgroup_path=cgroup_path,
                invocation_id=cast(str, systemd_invocation_id),
                description=cast(str, systemd_description),
            )
            if exact_persistent_identity
            else _pc._linux_cgroup_process_ids(scope)
        )
        if residual:
            raise RuntimeError(f"recorded systemd scope survived cleanup: {residual}")
        _pc._release_linux_systemd_scope(systemd_unit)
        return
    if os.name == "nt":
        if observed_identity is None:
            return
        _terminate_recorded_windows_process_tree(process_id, expected_start_identity)
        return
    if process_group_id is None or process_group_id <= 0:
        raise RuntimeError("recorded POSIX execution has no process-group identity")
    if process_group_id == _pc._current_posix_group():
        raise RuntimeError("refused to terminate the replacement worker process group")
    residual = _posix_process_group_ids(process_group_id)
    if not residual and observed_identity is None:
        return
    targets = sorted(set([process_id, *residual]))
    _pc._signal_posix_tree(targets, [process_group_id], signal.SIGTERM)
    residual = _pc._wait_for_exit(
        process_ids=targets,
        process_group=process_group_id,
        timeout_seconds=TERMINATION_TIMEOUT_SECONDS,
    )
    if residual:
        _pc._signal_posix_tree(residual, [process_group_id], signal.SIGKILL)
        residual = _pc._wait_for_exit(
            process_ids=residual,
            process_group=process_group_id,
            timeout_seconds=TERMINATION_TIMEOUT_SECONDS,
        )
    if residual:
        raise RuntimeError(f"recorded process tree survived cleanup: {residual}")

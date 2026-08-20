"""Termination of owned, nested, and completed relay-owned process trees.

Owner module for `clio_relay.process_containment` (iowarp/clio-relay#231).

`ensure_owned_process_tree_empty`, `_close_windows_handle`,
`_release_linux_systemd_scope`, `_terminate_linux_systemd_scope`,
`_linux_cgroup_process_ids`, `_current_posix_group`,
`_posix_descendant_process_ids`, `_signal_posix_tree`, and `_wait_for_exit`
are individually replaced by the test suite via `monkeypatch.setattr` on the
facade module -- including by tests that exercise `release_owned_process`
and `terminate_owned_process` defined right here, so every call to one of
those names goes through the live facade module
(`clio_relay.process_containment`) instead of a bare name, matching how the
pre-split monolith resolved the same bare name at call time. `POLL_SECONDS`
is read the same way for the same reason. `_posix_process_group_ids` is
never individually replaced, so it stays a plain import.
"""

from __future__ import annotations

import os
import signal
import subprocess
import time

from clio_relay import process_containment as _pc
from clio_relay.process_containment_popen import inherited_relay_containment
from clio_relay.process_containment_posix import _posix_process_group_ids
from clio_relay.process_containment_registry import (
    _OWNED_PROCESSES,
    _OWNED_PROCESSES_LOCK,
    _OWNED_PROCESSES_RELEASING,
)
from clio_relay.process_containment_types import DISCOVERY_ROUNDS, TERMINATION_TIMEOUT_SECONDS
from clio_relay.process_containment_windows import (
    _terminate_windows_job,
    _terminate_windows_tree,
    _windows_job_active_processes,
)


def terminate_owned_process(process: subprocess.Popen[str]) -> None:
    """Terminate a registered root process through its strongest ownership provider."""
    with _OWNED_PROCESSES_LOCK:
        state = _OWNED_PROCESSES.get(process.pid)
        releasing = process.pid in _OWNED_PROCESSES_RELEASING
    if releasing:
        raise RuntimeError(f"process containment release is already in progress: {process.pid}")
    if state is None or not state.enforceable:
        terminate_process_tree(process, owns_group=True)
        return
    if state.job_handle is not None:
        _terminate_windows_job(state.job_handle)
        if process.poll() is None:
            process.wait(timeout=TERMINATION_TIMEOUT_SECONDS)
        if _windows_job_active_processes(state.job_handle) != 0:
            raise RuntimeError("Windows Job Object remained populated after termination")
        return
    if state.cgroup_path is not None and state.systemd_unit is not None:
        _pc._terminate_linux_systemd_scope(state.systemd_unit, state.cgroup_path)
        if process.poll() is None:
            process.wait(timeout=TERMINATION_TIMEOUT_SECONDS)
        residual = _pc._linux_cgroup_process_ids(state.cgroup_path)
        if residual:
            raise RuntimeError(f"Linux cgroup remained populated after termination: {residual}")
        return
    raise RuntimeError(f"invalid enforceable containment state for process {process.pid}")


def release_owned_process(process: subprocess.Popen[str]) -> None:
    """Release an empty containment provider after execution observation completes."""
    with _OWNED_PROCESSES_LOCK:
        state = _OWNED_PROCESSES.get(process.pid)
        if state is None:
            return
        if process.pid in _OWNED_PROCESSES_RELEASING:
            raise RuntimeError(f"process containment release is already in progress: {process.pid}")
        _OWNED_PROCESSES_RELEASING.add(process.pid)
    try:
        _pc.ensure_owned_process_tree_empty(process)
        if state.job_handle is not None:
            _pc._close_windows_handle(state.job_handle)
        if state.systemd_unit is not None:
            _pc._release_linux_systemd_scope(state.systemd_unit)
    except BaseException:
        with _OWNED_PROCESSES_LOCK:
            _OWNED_PROCESSES_RELEASING.discard(process.pid)
        raise
    with _OWNED_PROCESSES_LOCK:
        _OWNED_PROCESSES_RELEASING.discard(process.pid)
        if _OWNED_PROCESSES.get(process.pid) is not state:
            raise RuntimeError("owned process registration changed during provider release")
        _OWNED_PROCESSES.pop(process.pid)


def terminate_process_tree(
    process: subprocess.Popen[str],
    *,
    owns_group: bool,
    timeout_seconds: float = TERMINATION_TIMEOUT_SECONDS,
) -> None:
    """Terminate and verify a process tree without signaling the caller's group."""
    if os.name == "nt":
        _terminate_windows_tree(process, timeout_seconds=timeout_seconds)
        return
    discovery_error: RuntimeError | None = None
    descendants: list[int] = []
    for round_index in range(DISCOVERY_ROUNDS):
        try:
            observed = _pc._posix_descendant_process_ids(process.pid)
        except RuntimeError as exc:
            discovery_error = exc
            break
        descendants.extend(item for item in observed if item not in descendants)
        if round_index + 1 < DISCOVERY_ROUNDS:
            time.sleep(_pc.POLL_SECONDS)
    process_ids = [process.pid, *descendants]
    groups = [process.pid] if owns_group and process.pid != _pc._current_posix_group() else []
    _pc._signal_posix_tree(process_ids, groups, signal.SIGTERM)
    if process.poll() is None:
        try:
            process.wait(timeout=timeout_seconds)
        except subprocess.TimeoutExpired:
            _pc._signal_posix_tree(process_ids, groups, signal.SIGKILL)
            process.wait(timeout=timeout_seconds)
    residual = _pc._wait_for_exit(
        process_ids=descendants,
        process_group=process.pid if owns_group else None,
        timeout_seconds=timeout_seconds,
    )
    if residual:
        _pc._signal_posix_tree(
            residual,
            [process.pid] if owns_group and process.pid != _pc._current_posix_group() else [],
            signal.SIGKILL,
        )
        residual = _pc._wait_for_exit(
            process_ids=residual,
            process_group=process.pid if owns_group else None,
            timeout_seconds=timeout_seconds,
        )
    if residual:
        raise RuntimeError(f"relay-owned descendant processes survived cleanup: {residual}")
    if discovery_error is not None:
        raise RuntimeError(
            f"process tree was terminated without complete descendant discovery: {discovery_error}"
        )


def terminate_nested_process(
    process: subprocess.Popen[str],
    *,
    timeout_seconds: float = TERMINATION_TIMEOUT_SECONDS,
) -> None:
    """Terminate a child from an embedded runner without killing its relay parent."""
    terminate_process_tree(
        process,
        owns_group=not inherited_relay_containment(),
        timeout_seconds=timeout_seconds,
    )


def ensure_owned_process_tree_empty(process: subprocess.Popen[str]) -> None:
    """Reject a completed outer process that left owned descendants."""
    with _OWNED_PROCESSES_LOCK:
        state = _OWNED_PROCESSES.get(process.pid)
    if state is not None and state.enforceable:
        if state.job_handle is not None:
            residual_count = _windows_job_active_processes(state.job_handle)
            if residual_count == 0:
                return
            _terminate_windows_job(state.job_handle)
            raise RuntimeError(
                f"completed process left {residual_count} Windows Job Object descendants"
            )
        if state.cgroup_path is not None and state.systemd_unit is not None:
            residual = _pc._linux_cgroup_process_ids(state.cgroup_path)
            if not residual:
                return
            _pc._terminate_linux_systemd_scope(state.systemd_unit, state.cgroup_path)
            raise RuntimeError(f"completed process left systemd-scope descendants: {residual}")
        raise RuntimeError(f"invalid enforceable containment state for process {process.pid}")
    if os.name == "nt":
        return
    residual = _posix_process_group_ids(process.pid)
    residual = [process_id for process_id in residual if process_id != process.pid]
    if not residual:
        return
    _pc._signal_posix_tree(residual, [process.pid], signal.SIGKILL)
    remaining = _pc._wait_for_exit(
        process_ids=residual,
        process_group=process.pid,
        timeout_seconds=TERMINATION_TIMEOUT_SECONDS,
    )
    if remaining:
        raise RuntimeError(f"completed process left relay-owned descendants: {remaining}")
    raise RuntimeError(f"completed process left relay-owned descendants: {residual}")

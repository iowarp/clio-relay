"""Containment capability probing and owned-process spawn orchestration.

Owner module for `clio_relay.process_containment` (iowarp/clio-relay#231).

`containment_capability` caches its result in `_containment_capability_cache`
via a `global` statement, which is inherently module-local in Python -- that
cache and its lock stay in this file specifically so the cache keeps working
after the split (a `global` in a moved function would silently rebind a
*new*, disconnected variable in whichever module it landed in).

`containment_capability`, `_spawn_broker`, `_release_broker`,
`_spawn_linux_systemd_scope`, `terminate_owned_process`,
`release_owned_process`, `_terminate_linux_systemd_scope`,
`_release_linux_systemd_scope`, `_close_windows_handle`, and
`_remove_broker_readiness` are all individually replaced by the test suite
via `monkeypatch.setattr` on the facade module -- including by tests that
exercise `spawn_owned_process` and `_cleanup_failed_owned_spawn` defined
right here. Every call to one of those names, even ones defined in this same
file, goes through the live facade module
(`clio_relay.process_containment`) rather than a bare name or plain import,
so a monkeypatch applied to the facade after import is observed exactly as
it was when all of this code lived in one file.
"""

from __future__ import annotations

import math
import os
import re
import subprocess
import sys
import threading
import time
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

from clio_relay import process_containment as _pc
from clio_relay.process_containment_broker import _validate_broker_target_environment
from clio_relay.process_containment_registry import (
    _OWNED_PROCESSES,
    _OWNED_PROCESSES_LOCK,
    _register_owned_process,
)
from clio_relay.process_containment_systemd_scope import _probe_linux_systemd_scope_capability
from clio_relay.process_containment_types import (
    BROKER_READY_TIMEOUT_SECONDS,
    TERMINATION_TIMEOUT_SECONDS,
    OwnedProcessSpawnError,
    _BrokerReadiness,
    _OwnedProcessState,
)
from clio_relay.process_containment_windows import _assign_windows_job, _create_windows_job

_containment_capability_cache: dict[str, object] | None = None
_CONTAINMENT_CAPABILITY_LOCK = threading.Lock()


def containment_capability(*, startup_deadline: float | None = None) -> dict[str, object]:
    """Return whether this host offers kernel-enforced descendant containment."""
    global _containment_capability_cache
    with _CONTAINMENT_CAPABILITY_LOCK:
        cached = _containment_capability_cache
    if cached is not None:
        return dict(cached)
    result: dict[str, object]
    if os.name == "nt":
        try:
            handle = _create_windows_job()
        except RuntimeError as exc:
            result = {
                "mode": "windows_job_object",
                "enforceable": False,
                "reason": str(exc),
            }
            with _CONTAINMENT_CAPABILITY_LOCK:
                _containment_capability_cache = result
            return dict(result)
        _pc._close_windows_handle(handle)
        result = {
            "mode": "windows_job_object",
            "enforceable": True,
            "reason": "kill-on-close Job Object available",
        }
        with _CONTAINMENT_CAPABILITY_LOCK:
            _containment_capability_cache = result
        return dict(result)
    if sys.platform.startswith("linux"):
        result = _probe_linux_systemd_scope_capability(startup_deadline=startup_deadline)
        if result.pop("transient", False) is not True:
            with _CONTAINMENT_CAPABILITY_LOCK:
                _containment_capability_cache = result
        return dict(result)
    result = {
        "mode": "cooperative_process_group",
        "enforceable": False,
        "reason": "no supported kernel containment provider",
    }
    with _CONTAINMENT_CAPABILITY_LOCK:
        _containment_capability_cache = result
    return dict(result)


def spawn_owned_process(
    command: list[str],
    *,
    on_ready: Callable[[int, dict[str, object]], None] | None = None,
    credential_payload: str | None = None,
    credential_payload_factory: Callable[[int, dict[str, object]], str] | None = None,
    stdin_payload: bytes | None = None,
    interactive_stdin: bool = False,
    target_environment: Mapping[str, str] | None = None,
    startup_timeout_seconds: float = BROKER_READY_TIMEOUT_SECONDS,
    require_enforceable: bool = False,
    linux_systemd_unit_base: str | None = None,
    linux_systemd_description: str | None = None,
    **popen_kwargs: Any,
) -> subprocess.Popen[str]:
    """Spawn a root process after establishing enforceable containment when available."""
    if not math.isfinite(startup_timeout_seconds) or startup_timeout_seconds <= 0:
        raise ValueError("owned process startup timeout must be finite and positive")
    startup_deadline = time.monotonic() + startup_timeout_seconds
    if credential_payload is not None and credential_payload_factory is not None:
        raise ValueError("owned process credential payload sources are mutually exclusive")
    _pc._validate_broker_credential_payload(credential_payload)
    validated_target_environment = _validate_broker_target_environment(target_environment)
    if interactive_stdin and stdin_payload is not None:
        raise ValueError("interactive owned process stdin cannot include a fixed payload")
    capability = _pc.containment_capability(startup_deadline=startup_deadline)
    mode = str(capability["mode"])
    enforceable = capability.get("enforceable") is True
    if require_enforceable and not enforceable:
        raise RuntimeError("enforceable owned process containment is unavailable")
    if credential_payload_factory is not None and not (
        enforceable and mode == "linux_systemd_scope"
    ):
        raise RuntimeError("deferred credential payload requires Linux systemd containment")
    if linux_systemd_unit_base is not None:
        if not (
            sys.platform.startswith("linux")
            and re.fullmatch(r"clio-relay-session-[A-Za-z0-9_-]+", linux_systemd_unit_base)
        ):
            raise ValueError("persistent Linux systemd unit identity is invalid")
        if mode != "linux_systemd_scope" or not enforceable:
            raise RuntimeError("persistent Linux session containment requires a systemd user scope")
    if linux_systemd_description is not None and (
        not linux_systemd_description
        or len(linux_systemd_description.encode("utf-8")) > 512
        or "\x00" in linux_systemd_description
        or "\n" in linux_systemd_description
    ):
        raise ValueError("persistent Linux systemd description is invalid")
    if enforceable and mode == "windows_job_object":
        handle = _create_windows_job()
        process, readiness = _pc._spawn_broker(command, popen_kwargs)
        registered = False
        try:
            _assign_windows_job(handle, process)
            _register_owned_process(
                process.pid,
                _OwnedProcessState(mode=mode, enforceable=True, job_handle=handle),
            )
            registered = True
            _notify_containment_ready(process, on_ready)
            _pc._release_broker(
                process,
                readiness=readiness,
                credential_payload=credential_payload,
                stdin_payload=stdin_payload,
                interactive_stdin=interactive_stdin,
                target_environment=validated_target_environment,
                startup_deadline=startup_deadline,
            )
        except BaseException as exc:
            cleanup_errors = _cleanup_failed_owned_spawn(
                process,
                readiness=readiness,
                registered=registered,
                unregistered_windows_handle=None if registered else handle,
            )
            raise OwnedProcessSpawnError(
                process_id=process.pid,
                mode=mode,
                cleanup_errors=cleanup_errors,
                cause=exc,
            ) from exc
        return process
    if enforceable and mode == "linux_systemd_scope":
        (
            process,
            unit,
            scope,
            invocation_id,
            description,
            readiness,
        ) = _pc._spawn_linux_systemd_scope(
            command,
            popen_kwargs,
            startup_deadline=startup_deadline,
            unit_base=linux_systemd_unit_base,
            description=linux_systemd_description,
        )
        registered = False
        try:
            _register_owned_process(
                process.pid,
                _OwnedProcessState(
                    mode=mode,
                    enforceable=True,
                    cgroup_path=scope,
                    systemd_unit=unit,
                    systemd_invocation_id=invocation_id,
                    systemd_description=description,
                ),
            )
            registered = True
            metadata = owned_process_metadata(process.pid)
            if on_ready is not None:
                on_ready(process.pid, metadata)
            selected_credential_payload = (
                credential_payload_factory(process.pid, metadata)
                if credential_payload_factory is not None
                else credential_payload
            )
            _pc._validate_broker_credential_payload(selected_credential_payload)
            _pc._release_broker(
                process,
                readiness=readiness,
                credential_payload=selected_credential_payload,
                stdin_payload=stdin_payload,
                interactive_stdin=interactive_stdin,
                target_environment=validated_target_environment,
                startup_deadline=startup_deadline,
            )
        except BaseException as exc:
            cleanup_errors = _cleanup_failed_owned_spawn(
                process,
                readiness=readiness,
                registered=registered,
                unregistered_systemd_unit=None if registered else unit,
                unregistered_systemd_scope=None if registered else scope,
            )
            raise OwnedProcessSpawnError(
                process_id=process.pid,
                mode=mode,
                cleanup_errors=cleanup_errors,
                cause=exc,
            ) from exc
        return process
    process, readiness = _pc._spawn_broker(command, popen_kwargs)
    registered = False
    try:
        _register_owned_process(
            process.pid,
            _OwnedProcessState(mode="cooperative_process_group", enforceable=False),
        )
        registered = True
        _notify_containment_ready(process, on_ready)
        _pc._release_broker(
            process,
            readiness=readiness,
            credential_payload=credential_payload,
            stdin_payload=stdin_payload,
            interactive_stdin=interactive_stdin,
            target_environment=validated_target_environment,
            startup_deadline=startup_deadline,
        )
    except BaseException as exc:
        cleanup_errors = _cleanup_failed_owned_spawn(
            process,
            readiness=readiness,
            registered=registered,
        )
        raise OwnedProcessSpawnError(
            process_id=process.pid,
            mode="cooperative_process_group",
            cleanup_errors=cleanup_errors,
            cause=exc,
        ) from exc
    return process


def _cleanup_failed_owned_spawn(
    process: subprocess.Popen[str],
    *,
    readiness: _BrokerReadiness,
    registered: bool,
    unregistered_windows_handle: int | None = None,
    unregistered_systemd_unit: str | None = None,
    unregistered_systemd_scope: Path | None = None,
) -> list[str]:
    """Attempt every cleanup step after a failed broker launch, preserving ownership."""
    errors: list[str] = []
    if registered:
        try:
            _pc.terminate_owned_process(process)
        except BaseException as exc:
            errors.append(f"owned spawn termination failed: {type(exc).__name__}")
        try:
            _pc.release_owned_process(process)
        except BaseException as exc:
            errors.append(f"owned spawn provider release failed: {type(exc).__name__}")
    elif unregistered_systemd_unit is not None and unregistered_systemd_scope is not None:
        try:
            _pc._terminate_linux_systemd_scope(
                unregistered_systemd_unit,
                unregistered_systemd_scope,
            )
        except BaseException as exc:
            errors.append(f"unregistered systemd scope termination failed: {type(exc).__name__}")
        try:
            if process.poll() is None:
                process.kill()
            process.wait(timeout=TERMINATION_TIMEOUT_SECONDS)
        except BaseException as exc:
            errors.append(f"unregistered systemd broker termination failed: {type(exc).__name__}")
        try:
            _pc._release_linux_systemd_scope(unregistered_systemd_unit)
        except BaseException as exc:
            errors.append(f"unregistered systemd scope release failed: {type(exc).__name__}")
    else:
        try:
            if process.poll() is None:
                process.kill()
            process.wait(timeout=TERMINATION_TIMEOUT_SECONDS)
        except BaseException as exc:
            errors.append(f"unregistered broker termination failed: {type(exc).__name__}")
        if unregistered_windows_handle is not None:
            try:
                _pc._close_windows_handle(unregistered_windows_handle)
            except BaseException as exc:
                errors.append(f"unregistered Job Object release failed: {type(exc).__name__}")
    try:
        _pc._remove_broker_readiness(readiness)
    except BaseException as exc:
        errors.append(f"broker readiness cleanup failed: {type(exc).__name__}")
    errors.extend(_close_failed_broker_streams(process))
    return errors


def _close_failed_broker_streams(process: subprocess.Popen[str]) -> list[str]:
    """Close broker-owned pipes after startup failure without reading their contents."""
    errors: list[str] = []
    closed: set[int] = set()
    for stream_name in ("stdin", "stdout", "stderr"):
        stream = getattr(process, stream_name, None)
        if stream is None or id(stream) in closed:
            continue
        closed.add(id(stream))
        try:
            stream.close()
        except BaseException as exc:
            errors.append(f"broker {stream_name} cleanup failed: {type(exc).__name__}")
        setattr(process, stream_name, None)
    return errors


def owned_process_metadata(process_id: int) -> dict[str, object]:
    """Return persisted ownership evidence for one process started by this relay."""
    with _OWNED_PROCESSES_LOCK:
        state = _OWNED_PROCESSES.get(process_id)
    if state is None:
        return {
            "mode": "unregistered",
            "enforceable": False,
            "cgroup_path": None,
        }
    return {
        "mode": state.mode,
        "enforceable": state.enforceable,
        "cgroup_path": None if state.cgroup_path is None else str(state.cgroup_path),
        "systemd_unit": state.systemd_unit,
        "systemd_invocation_id": state.systemd_invocation_id,
        "systemd_description": state.systemd_description,
    }


def _notify_containment_ready(
    process: subprocess.Popen[str],
    callback: Callable[[int, dict[str, object]], None] | None,
) -> None:
    if callback is not None:
        callback(process.pid, owned_process_metadata(process.pid))

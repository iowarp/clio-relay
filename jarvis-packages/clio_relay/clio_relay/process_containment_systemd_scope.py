"""Linux systemd user-scope capability probing, spawn, and teardown.

Owner module for `clio_relay.process_containment` (iowarp/clio-relay#231).

`_systemctl_user`, `_terminate_linux_systemd_scope`, `_release_linux_systemd_scope`,
and `_validated_systemd_cgroup_path` are individually replaced by the test
suite via `monkeypatch.setattr` on the facade module, as is `_BROKER_SCRIPT`
and `POLL_SECONDS`/`_remove_broker_readiness` from the broker owner module.
Every call to (or read of) one of those names -- including the ones made by
a sibling function defined right here -- goes through the live facade module
(`clio_relay.process_containment`) rather than a plain import, so a
monkeypatch applied to the facade after import is observed exactly as it was
when all of this code lived in one file.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import signal
import subprocess
import sys
import threading
import time
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any, cast
from uuid import uuid4

from clio_relay import process_containment as _pc
from clio_relay.process_containment_broker import _precreate_broker_readiness
from clio_relay.process_containment_systemd_core import (
    _parse_systemd_properties,
    _remaining_deadline_seconds,
    _wait_for_linux_cgroup_empty,
)
from clio_relay.process_containment_types import (
    DISCOVERY_TIMEOUT_SECONDS,
    SYSTEMCTL_OUTPUT_MAX_BYTES,
    TERMINATION_TIMEOUT_SECONDS,
    OwnedProcessSpawnError,
    _BrokerReadiness,
)


def _probe_linux_systemd_scope_capability(
    *,
    startup_deadline: float | None,
) -> dict[str, object]:
    if not (Path("/sys/fs/cgroup") / "cgroup.controllers").is_file():
        return {
            "mode": "linux_systemd_scope",
            "enforceable": False,
            "reason": "the host does not expose cgroup v2",
        }
    systemd_run = shutil.which("systemd-run")
    systemctl = shutil.which("systemctl")
    if systemd_run is None or systemctl is None:
        return {
            "mode": "linux_systemd_scope",
            "enforceable": False,
            "reason": "systemd-run and systemctl are required",
        }
    unit_base = f"clio-relay-probe-{uuid4().hex}"
    command = [
        systemd_run,
        "--user",
        "--scope",
        "--quiet",
        f"--unit={unit_base}",
        "--property=Delegate=yes",
        "--property=KillMode=control-group",
        "--",
        sys.executable,
        "-c",
        "pass",
    ]
    try:
        result = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            timeout=_remaining_deadline_seconds(
                startup_deadline,
                maximum=TERMINATION_TIMEOUT_SECONDS,
            ),
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return {
            "mode": "linux_systemd_scope",
            "enforceable": False,
            "reason": f"systemd user-scope probe failed: {type(exc).__name__}",
            "transient": isinstance(exc, subprocess.TimeoutExpired),
        }
    _pc._release_linux_systemd_scope(
        f"{unit_base}.scope",
        startup_deadline=startup_deadline,
    )
    if result.returncode != 0:
        return {
            "mode": "linux_systemd_scope",
            "enforceable": False,
            "reason": result.stderr.strip() or "systemd user-scope probe returned nonzero",
        }
    return {
        "mode": "linux_systemd_scope",
        "enforceable": True,
        "reason": "named systemd user scopes with cgroup-v2 delegation are available",
    }


def _spawn_linux_systemd_scope(
    command: list[str],
    popen_kwargs: dict[str, Any],
    *,
    startup_deadline: float,
    unit_base: str | None = None,
    description: str | None = None,
) -> tuple[subprocess.Popen[str], str, Path, str, str | None, _BrokerReadiness]:
    if "stdin" in popen_kwargs:
        raise RuntimeError("owned process launch reserves stdin for containment setup")
    systemd_run = shutil.which("systemd-run")
    if systemd_run is None:
        raise RuntimeError("systemd-run disappeared after containment capability probing")
    selected_unit_base = unit_base or f"clio-relay-{uuid4().hex}"
    unit = f"{selected_unit_base}.scope"
    readiness = _precreate_broker_readiness()
    effective_kwargs = dict(popen_kwargs)
    requested_environment = effective_kwargs.get("env")
    environment_source = (
        cast(Mapping[str, str], requested_environment)
        if isinstance(requested_environment, Mapping)
        else os.environ
    )
    systemd_environment = dict(environment_source)
    for name in ("XDG_RUNTIME_DIR", "DBUS_SESSION_BUS_ADDRESS"):
        if name in os.environ:
            systemd_environment.setdefault(name, os.environ[name])
    effective_kwargs["env"] = systemd_environment
    wrapped = [
        systemd_run,
        "--user",
        "--scope",
        "--quiet",
        f"--unit={selected_unit_base}",
        "--property=Delegate=yes",
        "--property=KillMode=control-group",
        "--",
        sys.executable,
        "-I",
        "-S",
        "-u",
        "-c",
        _pc._BROKER_SCRIPT,
        json.dumps(command),
        str(readiness.path),
        json.dumps(readiness.anchor(), separators=(",", ":")),
        str(Path(__file__).resolve().parent.parent),
    ]
    if description is not None:
        wrapped[5:5] = [f"--description={description}"]
    try:
        process = subprocess.Popen(
            wrapped,
            **effective_kwargs,
            stdin=subprocess.PIPE,
            start_new_session=False,
            creationflags=0,
        )
    except BaseException:
        _pc._remove_broker_readiness(readiness)
        raise
    try:
        properties = _wait_for_systemd_scope_identity(
            unit,
            process=process,
            startup_deadline=startup_deadline,
        )
        control_group = properties["ControlGroup"]
        invocation_id = properties["InvocationID"]
        observed_description = properties.get("Description")
        scope = _pc._validated_systemd_cgroup_path(control_group, unit=unit)
        if description is not None and observed_description != description:
            raise RuntimeError("systemd scope description did not match its launch identity")
    except BaseException as exc:
        cleanup_errors = _cleanup_failed_linux_systemd_spawn(
            process,
            unit=unit,
            readiness=readiness,
            startup_deadline=startup_deadline,
        )
        raise OwnedProcessSpawnError(
            process_id=process.pid,
            mode="linux_systemd_scope",
            cleanup_errors=cleanup_errors,
            cause=exc,
        ) from exc
    return process, unit, scope, invocation_id, observed_description, readiness


def _cleanup_failed_linux_systemd_spawn(
    process: subprocess.Popen[str],
    *,
    unit: str,
    readiness: _BrokerReadiness,
    startup_deadline: float,
) -> list[str]:
    """Attempt every cleanup action for a scope that failed before registration."""
    errors: list[str] = []
    try:
        if process.poll() is None:
            process.kill()
    except BaseException as exc:
        errors.append(f"unregistered systemd broker termination failed: {type(exc).__name__}")
    try:
        process.wait(
            timeout=_remaining_deadline_seconds(
                startup_deadline,
                maximum=TERMINATION_TIMEOUT_SECONDS,
            )
        )
    except BaseException as exc:
        errors.append(f"unregistered systemd broker wait failed: {type(exc).__name__}")
    try:
        _pc._release_linux_systemd_scope(unit, startup_deadline=startup_deadline)
    except BaseException as exc:
        errors.append(f"unregistered systemd scope release failed: {type(exc).__name__}")
    try:
        _pc._remove_broker_readiness(readiness)
    except BaseException as exc:
        errors.append(f"broker readiness cleanup failed: {type(exc).__name__}")
    return errors


def _wait_for_systemd_scope_identity(
    unit: str,
    *,
    process: subprocess.Popen[str],
    startup_deadline: float,
) -> dict[str, str]:
    deadline = startup_deadline
    last_error = "unit was not observable"
    while time.monotonic() < deadline:
        if process.poll() is not None:
            diagnostic = ""
            if process.stderr is not None:
                try:
                    diagnostic = process.stderr.read(4096).strip()
                except (OSError, ValueError):
                    diagnostic = ""
            raise RuntimeError(
                f"systemd-run exited before scope setup with return code {process.returncode}"
                + (f": {diagnostic}" if diagnostic else "")
            )
        result = _pc._systemctl_user(
            [
                "show",
                unit,
                "--property=ControlGroup",
                "--property=InvocationID",
                "--property=Description",
                "--property=LoadState",
            ],
            timeout_seconds=_remaining_deadline_seconds(
                deadline,
                maximum=DISCOVERY_TIMEOUT_SECONDS,
            ),
        )
        try:
            properties = _parse_systemd_properties(
                result.stdout,
                expected={"ControlGroup", "InvocationID", "Description", "LoadState"},
            )
        except RuntimeError as exc:
            properties = {}
            last_error = str(exc)
        if (
            result.returncode == 0
            and properties.get("LoadState") == "loaded"
            and properties.get("ControlGroup")
            and re.fullmatch(r"[0-9a-f]{32}", properties.get("InvocationID", ""))
        ):
            return properties
        last_error = result.stderr.strip() or last_error
        time.sleep(min(_pc.POLL_SECONDS, max(0.0, deadline - time.monotonic())))
    raise RuntimeError(f"systemd scope setup timed out: {unit}: {last_error}")


def _terminate_linux_systemd_scope(unit: str, cgroup_path: Path) -> None:
    _pc._systemctl_user(
        ["kill", "--kill-who=all", "--signal=SIGTERM", unit],
        timeout_seconds=TERMINATION_TIMEOUT_SECONDS,
    )
    residual = _wait_for_linux_cgroup_empty(
        cgroup_path,
        timeout_seconds=TERMINATION_TIMEOUT_SECONDS,
    )
    if residual:
        kill_result = _pc._systemctl_user(
            ["kill", "--kill-who=all", "--signal=SIGKILL", unit],
            timeout_seconds=TERMINATION_TIMEOUT_SECONDS,
        )
        if kill_result.returncode != 0 and (cgroup_path / "cgroup.kill").is_file():
            (cgroup_path / "cgroup.kill").write_text("1", encoding="ascii")
        residual = _wait_for_linux_cgroup_empty(
            cgroup_path,
            timeout_seconds=TERMINATION_TIMEOUT_SECONDS,
        )
    stop_result = _pc._systemctl_user(
        ["stop", unit],
        timeout_seconds=TERMINATION_TIMEOUT_SECONDS,
    )
    if stop_result.returncode != 0 and residual:
        raise RuntimeError(stop_result.stderr.strip() or f"could not stop systemd scope {unit}")
    if residual:
        raise RuntimeError(f"systemd scope remained populated after cleanup: {unit}: {residual}")


def _release_linux_systemd_scope(
    unit: str,
    *,
    startup_deadline: float | None = None,
) -> None:
    if shutil.which("systemctl") is None:
        return
    _pc._systemctl_user(
        ["stop", unit],
        timeout_seconds=_remaining_deadline_seconds(
            startup_deadline,
            maximum=DISCOVERY_TIMEOUT_SECONDS,
        ),
    )
    _pc._systemctl_user(
        ["reset-failed", unit],
        timeout_seconds=_remaining_deadline_seconds(
            startup_deadline,
            maximum=DISCOVERY_TIMEOUT_SECONDS,
        ),
    )


def _systemctl_user(
    arguments: list[str],
    *,
    timeout_seconds: float,
) -> subprocess.CompletedProcess[str]:
    systemctl = shutil.which("systemctl") or "systemctl"
    command = [systemctl, "--user", *arguments]
    try:
        process = subprocess.Popen(
            command,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True,
        )
    except OSError as exc:
        return subprocess.CompletedProcess(
            command,
            1,
            "",
            str(exc),
        )

    stdout = bytearray()
    stderr = bytearray()
    overflow = threading.Event()

    def read_bounded(stream: Any, destination: bytearray) -> None:
        try:
            while True:
                remaining = SYSTEMCTL_OUTPUT_MAX_BYTES + 1 - len(destination)
                if remaining <= 0:
                    overflow.set()
                    return
                chunk = stream.read(min(8192, remaining))
                if not chunk:
                    return
                destination.extend(chunk)
                if len(destination) > SYSTEMCTL_OUTPUT_MAX_BYTES:
                    overflow.set()
                    return
        except OSError:
            overflow.set()

    if process.stdout is None or process.stderr is None:  # pragma: no cover - Popen contract
        process.kill()
        process.wait()
        return subprocess.CompletedProcess(command, 1, "", "systemctl pipes were unavailable")
    readers = [
        threading.Thread(target=read_bounded, args=(process.stdout, stdout), daemon=True),
        threading.Thread(target=read_bounded, args=(process.stderr, stderr), daemon=True),
    ]
    for reader in readers:
        reader.start()
    deadline = time.monotonic() + timeout_seconds
    timed_out = False
    while process.poll() is None and not overflow.is_set():
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            timed_out = True
            break
        time.sleep(min(_pc.POLL_SECONDS, remaining))
    if process.poll() is None:
        try:
            killpg = cast(Callable[[int, int], None], vars(os)["killpg"])
            sigkill = cast(int, vars(signal)["SIGKILL"])
            killpg(process.pid, sigkill)
        except ProcessLookupError:
            pass
        except OSError:
            process.kill()
    try:
        process.wait(timeout=DISCOVERY_TIMEOUT_SECONDS)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=DISCOVERY_TIMEOUT_SECONDS)
    for reader in readers:
        reader.join(timeout=DISCOVERY_TIMEOUT_SECONDS)
    stdout_text = bytes(stdout[:SYSTEMCTL_OUTPUT_MAX_BYTES]).decode("utf-8", errors="replace")
    stderr_text = bytes(stderr[:SYSTEMCTL_OUTPUT_MAX_BYTES]).decode("utf-8", errors="replace")
    if overflow.is_set():
        return subprocess.CompletedProcess(
            command,
            1,
            stdout_text,
            "systemctl output exceeded its byte limit",
        )
    if timed_out:
        return subprocess.CompletedProcess(command, 1, stdout_text, "systemctl timed out")
    return subprocess.CompletedProcess(command, process.returncode, stdout_text, stderr_text)

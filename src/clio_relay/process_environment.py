"""Child process launch, termination, and minimal-environment construction.

Split from ``runner.py`` (iowarp/clio-relay#231/#775 decomposition wave 3).
``_open_process``, ``_resolve_executable``, ``_install_parent_termination_handlers``,
and ``_restore_parent_termination_handlers`` are individually monkeypatched by
``tests/test_mcp_call_runner.py`` and observed through calls the facade or
:mod:`clio_relay.session_runtime` makes -- their own bodies here do not
call any other overridable name, so this module itself needs no facade
reach-back.
"""

from __future__ import annotations

import os
import shutil
import signal
import subprocess
import threading
from collections.abc import Callable
from pathlib import Path
from typing import Any, cast

from clio_relay.constants import (
    _BASE_CHILD_ENV_NAMES,
    _RELAY_CREDENTIAL_ENV_NAMES,
    MCP_SERVER_TERMINATION_TIMEOUT_SECONDS,
)
from clio_relay.process_containment import (
    CONTAINMENT_ENV,
    nested_popen_kwargs,
    terminate_nested_process,
)

_SignalHandler = Callable[[int, Any], None] | int | None


def _open_process(
    command: list[str],
    *,
    env_from: dict[str, str] | None = None,
    environment_overrides: dict[str, str] | None = None,
) -> subprocess.Popen[str]:
    child_env = _child_env(env_from) if env_from else _scrubbed_env()
    if environment_overrides is not None:
        child_env.update(environment_overrides)
    return subprocess.Popen(
        command,
        env=child_env,
        text=True,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        **nested_popen_kwargs(child_env),
    )


def _resolve_executable(executable: str) -> str:
    """Resolve executables commonly installed into user-local cluster paths."""
    resolved = shutil.which(executable)
    if resolved is not None:
        return resolved
    if executable in {"uv", "uvx"}:
        user_local_executable = Path.home() / ".local" / "bin" / executable
        if user_local_executable.exists():
            return str(user_local_executable)
    return executable


def _terminate_process_tree(process: subprocess.Popen[str]) -> None:
    terminate_nested_process(
        process,
        timeout_seconds=MCP_SERVER_TERMINATION_TIMEOUT_SECONDS,
    )


def _install_parent_termination_handlers(
    process: subprocess.Popen[str],
) -> dict[int, _SignalHandler]:
    """Ensure outer JARVIS termination cleans the separately-owned MCP group."""
    # Python signal handlers are process-global and may only be installed by the
    # main interpreter thread. Durable JARVIS executions invoke package start
    # hooks from a worker thread, where the session's ``finally`` block and the
    # relay containment boundary own child cleanup instead.
    if threading.current_thread() is not threading.main_thread():
        return {}
    previous: dict[int, _SignalHandler] = {}
    terminating = False

    def terminate(signum: int, _frame: Any) -> None:
        nonlocal terminating
        if terminating:
            return
        terminating = True
        _terminate_process_tree(process)
        raise SystemExit(128 + signum)

    signals: list[int] = [int(signal.SIGTERM), int(signal.SIGINT)]
    if os.name == "nt" and hasattr(signal, "SIGBREAK"):
        signals.append(int(vars(signal)["SIGBREAK"]))
    try:
        for signum in signals:
            previous[int(signum)] = signal.getsignal(signum)
            signal.signal(signum, terminate)
    except ValueError:
        for signum, handler in previous.items():
            signal.signal(signum, handler)
        return {}
    return previous


def _restore_parent_termination_handlers(previous: dict[int, _SignalHandler]) -> None:
    for signum, handler in previous.items():
        signal.signal(signum, handler)


def _child_env(env_from: dict[str, str]) -> dict[str, str]:
    """Build a minimal child environment plus explicit named references."""
    env = {name: os.environ[name] for name in _BASE_CHILD_ENV_NAMES if name in os.environ}
    if CONTAINMENT_ENV in os.environ:
        env[CONTAINMENT_ENV] = os.environ[CONTAINMENT_ENV]
    for child_name, source_name in env_from.items():
        _validate_environment_reference(child_name, source_name)
        try:
            env[child_name] = os.environ[source_name]
        except KeyError as exc:
            raise ValueError(f"MCP env_from source is not set: {source_name}") from exc
    return env


def _scrubbed_env() -> dict[str, str]:
    """Compatibility alias for the minimal environment without explicit references."""
    return _child_env({})


def _environment_references(value: Any) -> dict[str, str]:
    if not isinstance(value, dict):
        raise ValueError("env_from must be a string object")
    references: dict[str, str] = {}
    for child_name, source_name in cast(dict[object, object], value).items():
        if not isinstance(child_name, str) or not isinstance(source_name, str):
            raise ValueError("env_from must be a string object")
        _validate_environment_reference(child_name, source_name)
        references[child_name] = source_name
    return references


def _validate_environment_reference(child_name: str, source_name: str) -> None:
    if not _valid_environment_name(child_name) or not _valid_environment_name(source_name):
        raise ValueError("MCP env_from keys and values must be environment names")
    forbidden = {
        name
        for name in (child_name, source_name)
        if name in _RELAY_CREDENTIAL_ENV_NAMES
        or (
            name.startswith("CLIO_RELAY_") and (name.endswith("_TOKEN") or name.endswith("_SECRET"))
        )
    }
    if forbidden:
        credential = sorted(forbidden)[0]
        raise ValueError(f"MCP env_from cannot expose relay credential {credential}")


def _valid_environment_name(value: str) -> bool:
    return (
        bool(value)
        and (value[0].isalpha() or value[0] == "_")
        and all(character.isalnum() or character == "_" for character in value)
    )

"""Describe the executable and immutable package inputs used for one MCP server.

Split from ``runner.py`` (iowarp/clio-relay#231/#775 decomposition wave 3).
``_server_artifact_identity`` calls ``_resolve_executable``, ``_file_identity``,
and ``_python_console_distribution_identity`` -- all three individually
monkeypatched on the ``runner`` facade by ``tests/test_mcp_call_runner.py`` and
expected to take effect here. Since this module's own top-level import would
otherwise bind those names at import time (immune to a later
``monkeypatch.setattr(runner, ...)``), all three calls go through ``_facade()``
-- see :mod:`clio_relay.clio_kit_runtime_identity` for the full
reach-back contract this decomposition wave relies on.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, cast

from clio_relay._mcp_call_runner_facade import facade as _facade
from clio_relay.clio_kit_runtime_identity import (
    _installed_clio_kit_runtime_identity,
    _locked_clio_kit_runtime_identity,
    _nested_clio_kit_server_name,
)
from clio_relay.clio_kit_wheel_archive import _install_spec_source
from clio_relay.python_console_distribution import _direct_distribution_source_identity


def mcp_server_artifact_identity(
    server: str,
    server_args: list[str],
    *,
    verify_relay_jarvis_cd_lock: bool = False,
) -> dict[str, Any]:
    """Return machine-readable launch identity for one stdio MCP server."""
    return _server_artifact_identity(
        server,
        server_args,
        verify_relay_jarvis_cd_lock=verify_relay_jarvis_cd_lock,
    )


def _server_artifact_identity(
    server: str,
    server_args: list[str],
    *,
    verify_relay_jarvis_cd_lock: bool = False,
) -> dict[str, Any]:
    """Describe the executable and immutable package inputs used for one MCP server."""
    resolved_executable = Path(_facade()._resolve_executable(server)).expanduser()
    executable = _facade()._file_identity(resolved_executable)
    install_spec: str | None = None
    for index, argument in enumerate(server_args[:-1]):
        if argument == "--from":
            install_spec = server_args[index + 1]
            break
    input_files: list[dict[str, Any]] = []
    for argument in server_args:
        identity = _facade()._file_identity(Path(argument).expanduser())
        if identity is not None and identity not in input_files:
            input_files.append(identity)
    install_source = _install_spec_source(install_spec)
    resolved_install_spec = (
        str(Path(install_spec).expanduser().resolve()) if install_spec is not None else None
    )
    install_artifact = next(
        (item for item in input_files if item["path"] == resolved_install_spec),
        None,
    )
    python_distribution_runtime = (
        _facade()._python_console_distribution_identity(resolved_executable)
        if install_spec is None and executable is not None
        else None
    )
    runtime_launcher_identity = (
        python_distribution_runtime.get("external_launcher_identity")
        if python_distribution_runtime is not None
        else None
    )
    runtime_launcher_verified = (
        executable is not None
        and isinstance(runtime_launcher_identity, dict)
        and cast(dict[str, Any], runtime_launcher_identity) == executable
    )
    if (
        python_distribution_runtime is not None
        and python_distribution_runtime.get("runtime_closure_verified") is True
        and not runtime_launcher_verified
    ):
        python_distribution_runtime["runtime_closure_verified"] = False
        python_distribution_runtime["error"] = (
            "direct server executable changed during Python runtime inspection"
        )
    direct_runtime_verified = (
        python_distribution_runtime is not None
        and python_distribution_runtime.get("runtime_closure_verified") is True
        and runtime_launcher_verified
    )
    direct_install_artifact = _direct_distribution_source_identity(python_distribution_runtime)
    if direct_install_artifact is not None and direct_install_artifact not in input_files:
        input_files.append(direct_install_artifact)
    recorded_install_spec = (
        install_spec
        if install_spec is not None
        else (str(direct_install_artifact["path"]) if direct_install_artifact is not None else None)
    )
    recorded_install_source = (
        install_source
        if install_spec is not None
        else ("uv-tool" if direct_install_artifact is not None else None)
    )
    recorded_install_artifact = install_artifact or direct_install_artifact
    launcher_artifact_verified = executable is not None and (
        (install_spec is None and direct_runtime_verified)
        or (install_spec is not None and install_artifact is not None)
    )
    nested_server_name = _nested_clio_kit_server_name(
        server_args,
        python_distribution_runtime=python_distribution_runtime,
    )
    nested_launcher = nested_server_name is not None
    nested_runtime = (
        (
            _locked_clio_kit_runtime_identity(
                install_artifact,
                server_name=nested_server_name,
                resolved_executable=resolved_executable,
                verify_relay_jarvis_cd_lock=verify_relay_jarvis_cd_lock,
            )
            if install_artifact is not None
            else _installed_clio_kit_runtime_identity(
                python_distribution_runtime,
                server_name=nested_server_name,
                resolved_executable=resolved_executable,
                verify_relay_jarvis_cd_lock=verify_relay_jarvis_cd_lock,
            )
        )
        if nested_server_name is not None
        else None
    )
    nested_runtime_verified = (
        nested_runtime is not None and nested_runtime.get("locked_runtime_verified") is True
    )
    server_process_artifact_verified = launcher_artifact_verified and (
        not nested_launcher or nested_runtime_verified
    )
    return {
        "requested_command": server,
        "resolved_executable": str(resolved_executable),
        "executable": executable,
        "install_spec": recorded_install_spec,
        "install_source": recorded_install_source,
        "install_artifact_sha256": (
            recorded_install_artifact.get("sha256")
            if recorded_install_artifact is not None
            else None
        ),
        "input_files": input_files,
        "launcher_artifact_verified": launcher_artifact_verified,
        "python_distribution_runtime": python_distribution_runtime,
        "nested_launcher": nested_launcher,
        "nested_runtime": nested_runtime,
        "server_process_artifact_verified": server_process_artifact_verified,
        "identity_error": (
            "clio-kit mcp-server child source, lock, or uv runtime is not bound to its "
            "persistent tool distribution"
            if nested_launcher and not nested_runtime_verified
            else (
                "direct server executable is not bound to a verified Python entry-point "
                "distribution RECORD closure"
                if install_spec is None and not direct_runtime_verified
                else None
            )
        ),
        "verified": server_process_artifact_verified,
    }


def _server_artifact_digest(server_artifact: dict[str, Any]) -> str:
    """Return the canonical discovery/execution artifact binding digest."""
    return hashlib.sha256(
        json.dumps(
            {"server_artifact": server_artifact},
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def _reject_verified_runtime_environment_remap(
    *,
    server_artifact: dict[str, Any],
    env_from: dict[str, str],
) -> None:
    """Keep a locked clio-kit child on the uv resolution identity just verified."""
    python_runtime = server_artifact.get("python_distribution_runtime")
    python_runtime_verified = (
        isinstance(python_runtime, dict)
        and cast(dict[str, Any], python_runtime).get("runtime_closure_verified") is True
    )
    nested_runtime = server_artifact.get("nested_runtime")
    nested_runtime_verified = (
        isinstance(nested_runtime, dict)
        and cast(dict[str, Any], nested_runtime).get("locked_runtime_verified") is True
    )
    if not python_runtime_verified and not nested_runtime_verified:
        return
    fixed_names = {
        "home",
        "homedrive",
        "homepath",
        "nodefaultcurrentdirectoryinexepath",
        "path",
        "pathext",
        "userprofile",
        "virtual_env",
        "xdg_cache_home",
        "xdg_config_home",
        "xdg_data_home",
        "xdg_state_home",
    }
    forbidden = sorted(
        child_name
        for child_name in env_from
        if (
            (
                python_runtime_verified
                and (
                    child_name.casefold() == "__pyvenv_launcher__"
                    or child_name.casefold().startswith("python")
                )
            )
            or (
                (python_runtime_verified or nested_runtime_verified)
                and (
                    child_name.casefold() in {"libpath", "shlib_path"}
                    or child_name.casefold().startswith(("dyld_", "ld_"))
                )
            )
            or (
                nested_runtime_verified
                and (
                    child_name.casefold() in fixed_names
                    or child_name.casefold().startswith(("clio_kit_", "python", "uv_"))
                )
            )
        )
    )
    if forbidden:
        raise ValueError(
            "verified MCP runtime cannot remap interpreter, native loader, or uv "
            "resolution environment through env_from"
        )


def _server_artifact_launch_executable(server_artifact: dict[str, Any]) -> str:
    """Return the exact executable path captured by server artifact inspection."""
    executable = server_artifact.get("executable")
    if isinstance(executable, dict):
        path = cast(dict[str, Any], executable).get("path")
        if isinstance(path, str) and path:
            return path
    if server_artifact.get("verified") is True:
        raise ValueError("verified MCP server artifact omitted its executable path")
    resolved = server_artifact.get("resolved_executable")
    if not isinstance(resolved, str) or not resolved:
        raise ValueError("MCP server artifact omitted its resolved executable")
    return resolved

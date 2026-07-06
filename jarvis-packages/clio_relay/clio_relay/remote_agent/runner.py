"""Provider adapters for remote-agent execution under JARVIS-CD."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path
from typing import Any
from uuid import uuid4


def run_remote_agent_from_params(params: dict[str, Any]) -> int:
    """Run a configured remote-agent task and return the process exit code."""
    agent_bin = _required_str(params, "agent_bin")
    adapter = str(params.get("agent_adapter", "exec"))
    prompt_path = Path(_required_str(params, "prompt_path"))
    prompt_text = prompt_path.read_text(encoding="utf-8")
    mcp_config_path = _optional_path(params.get("mcp_config_path"))
    workdir = _optional_path(params.get("workdir"))
    timeout = _optional_int(params.get("timeout_seconds"))
    model = params.get("model")
    agent_args = _string_list(params.get("agent_args"))

    if adapter == "codex":
        command, cleanup_path = _codex_command(
            agent_bin=agent_bin,
            agent_args=agent_args,
            prompt_text=prompt_text,
            mcp_config_path=mcp_config_path,
            model=model if isinstance(model, str) else None,
            workdir=workdir,
        )
    elif adapter == "exec":
        command = _exec_command(
            agent_bin=agent_bin,
            agent_args=agent_args,
            prompt_text=prompt_text,
            prompt_path=prompt_path,
            mcp_config_path=mcp_config_path,
            model=model if isinstance(model, str) else None,
        )
        cleanup_path = None
    else:
        raise ValueError(f"unsupported agent_adapter: {adapter}")

    try:
        result = subprocess.run(command, cwd=workdir, timeout=timeout, check=False)
        return result.returncode
    finally:
        if cleanup_path is not None:
            cleanup_path.unlink(missing_ok=True)


def _codex_command(
    *,
    agent_bin: str,
    agent_args: list[str],
    prompt_text: str,
    mcp_config_path: Path | None,
    model: str | None,
    workdir: Path | None,
) -> tuple[list[str], Path | None]:
    output_path = Path.cwd() / "agent-last-message.txt"
    command = [
        agent_bin,
        "exec",
        "--json",
        "--output-last-message",
        str(output_path),
        "--skip-git-repo-check",
    ]
    cleanup_path = _write_codex_profile(mcp_config_path)
    if cleanup_path is not None:
        command.extend(["--profile", cleanup_path.stem.removesuffix(".config")])
    if model:
        command.extend(["--model", model])
    if workdir is not None:
        command.extend(["--cd", str(workdir)])
    command.extend(agent_args)
    command.append(prompt_text)
    return command, cleanup_path


def _write_codex_profile(mcp_config_path: Path | None) -> Path | None:
    if mcp_config_path is None:
        return None
    codex_home = Path(os.environ.get("CODEX_HOME", Path.home() / ".codex"))
    codex_home.mkdir(parents=True, exist_ok=True)
    profile_name = f"clio-relay-agent-{os.getpid()}-{uuid4().hex}"
    profile_path = codex_home / f"{profile_name}.config.toml"
    profile_path.write_text(mcp_config_path.read_text(encoding="utf-8"), encoding="utf-8")
    return profile_path


def _exec_command(
    *,
    agent_bin: str,
    agent_args: list[str],
    prompt_text: str,
    prompt_path: Path,
    mcp_config_path: Path | None,
    model: str | None,
) -> list[str]:
    if not agent_args:
        return [agent_bin, prompt_text]
    replacements = {
        "{prompt}": prompt_text,
        "{prompt_path}": str(prompt_path),
        "{mcp_config_path}": "" if mcp_config_path is None else str(mcp_config_path),
        "{model}": "" if model is None else model,
        "{output_path}": str(Path.cwd() / "agent-last-message.txt"),
    }
    rendered = [replacements.get(arg, arg) for arg in agent_args]
    return [agent_bin, *rendered]


def _required_str(params: dict[str, Any], key: str) -> str:
    value = params.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"{key} is required")
    return value


def _optional_path(value: Any) -> Path | None:
    if value is None:
        return None
    if not isinstance(value, str) or value == "":
        raise ValueError("path values must be non-empty strings")
    return Path(value)


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    return int(value)


def _string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ValueError("agent_args must be a list of strings")
    return value

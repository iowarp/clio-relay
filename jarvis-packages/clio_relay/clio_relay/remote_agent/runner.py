"""Provider adapters for remote-agent execution under JARVIS-CD."""

from __future__ import annotations

import json
import os
import subprocess
import time
from pathlib import Path
from typing import Any
from uuid import uuid4


def run_remote_agent_from_params(params: dict[str, Any]) -> int:
    """Run a configured remote-agent task and return the process exit code."""
    started_at = time.time()
    output_path = Path.cwd() / "agent-last-message.txt"
    result_path = Path.cwd() / "agent-result.json"
    cleanup_path: Path | None = None
    agent_bin = str(params.get("agent_bin", ""))
    adapter = str(params.get("agent_adapter", "exec"))
    prompt_path = _safe_optional_path(params.get("prompt_path"))
    mcp_config_path = _safe_optional_path(params.get("mcp_config_path"))
    workdir = _safe_optional_path(params.get("workdir"))
    model = params.get("model") if isinstance(params.get("model"), str) else None
    try:
        agent_bin = _required_str(params, "agent_bin")
        prompt_path = Path(_required_str(params, "prompt_path"))
        prompt_text = prompt_path.read_text(encoding="utf-8")
        mcp_config_path = _optional_path(params.get("mcp_config_path"))
        workdir = _optional_path(params.get("workdir"))
        timeout = _optional_int(params.get("timeout_seconds"))
        model = params.get("model")
        rendered_model = model if isinstance(model, str) else None
        agent_args = _string_list(params.get("agent_args"))

        if adapter == "codex":
            command, cleanup_path = _codex_command(
                agent_bin=agent_bin,
                agent_args=agent_args,
                prompt_text=prompt_text,
                mcp_config_path=mcp_config_path,
                model=rendered_model,
                workdir=workdir,
                output_path=output_path,
            )
        elif adapter == "exec":
            command = _exec_command(
                agent_bin=agent_bin,
                agent_args=agent_args,
                prompt_text=prompt_text,
                prompt_path=prompt_path,
                mcp_config_path=mcp_config_path,
                model=rendered_model,
            )
        else:
            raise ValueError(f"unsupported agent_adapter: {adapter}")

        try:
            result = subprocess.run(command, cwd=workdir, timeout=timeout, check=False)
            returncode = result.returncode
            timed_out = False
        except subprocess.TimeoutExpired:
            returncode = 124
            timed_out = True
            error_type = "TimeoutExpired"
            error_message = f"agent exceeded timeout_seconds={timeout}"
        except OSError as exc:
            returncode = 127
            timed_out = False
            error_type = type(exc).__name__
            error_message = str(exc)
        else:
            error_type = None
            error_message = None
        _write_agent_result(
            result_path=result_path,
            agent_bin=agent_bin,
            adapter=adapter,
            prompt_path=prompt_path,
            mcp_config_path=mcp_config_path,
            model=rendered_model,
            workdir=workdir,
            returncode=returncode,
            timed_out=timed_out,
            started_at=started_at,
            output_path=output_path,
            error_type=error_type,
            error_message=error_message,
        )
        return returncode
    except (OSError, ValueError) as exc:
        _write_agent_result(
            result_path=result_path,
            agent_bin=agent_bin,
            adapter=adapter,
            prompt_path=prompt_path,
            mcp_config_path=mcp_config_path,
            model=model,
            workdir=workdir,
            returncode=2,
            timed_out=False,
            started_at=started_at,
            output_path=output_path,
            error_type=type(exc).__name__,
            error_message=str(exc),
        )
        return 2
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
    output_path: Path,
) -> tuple[list[str], Path | None]:
    command = [
        agent_bin,
        "--dangerously-bypass-approvals-and-sandbox",
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


def _write_agent_result(
    *,
    result_path: Path,
    agent_bin: str,
    adapter: str,
    prompt_path: Path,
    mcp_config_path: Path | None,
    model: str | None,
    workdir: Path | None,
    returncode: int,
    timed_out: bool,
    started_at: float,
    output_path: Path,
    error_type: str | None,
    error_message: str | None,
) -> None:
    finished_at = time.time()
    result = {
        "adapter": adapter,
        "agent_bin": agent_bin,
        "prompt_path": str(prompt_path),
        "mcp_config_path": None if mcp_config_path is None else str(mcp_config_path),
        "model": model,
        "workdir": None if workdir is None else str(workdir),
        "returncode": returncode,
        "timed_out": timed_out,
        "started_at": started_at,
        "finished_at": finished_at,
        "duration_seconds": finished_at - started_at,
        "error_message": error_message,
        "error_type": error_type,
        "last_message_path": str(output_path) if output_path.exists() else None,
    }
    result_path.write_text(json.dumps(result, indent=2, sort_keys=True), encoding="utf-8")


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


def _safe_optional_path(value: Any) -> Path | None:
    return Path(value) if isinstance(value, str) and value else None


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

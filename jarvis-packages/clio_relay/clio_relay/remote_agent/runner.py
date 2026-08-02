"""Provider adapters for remote-agent execution under JARVIS-CD."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast
from uuid import uuid4

from clio_schemas import ArtifactKind, ArtifactVersion, Custody, IdentityEvidence, Mechanism

from clio_relay.process_containment import nested_popen_kwargs, terminate_nested_process

CODEX_PROFILE_MAX_BYTES = 1_048_576
AGENT_PROMPT_MAX_BYTES = 4 * 1_048_576
AGENT_FINAL_ANSWER_MAX_BYTES = 16 * 1_048_576
RELAY_SIDE_CHANNEL_ENV_NAMES = frozenset(
    {
        "CLIO_RELAY_PROGRESS_FILE",
        "CLIO_RELAY_RUNTIME_METADATA_FILE",
    }
)


def run_remote_agent_from_params(params: dict[str, Any]) -> int:
    """Run a configured remote-agent task and return the process exit code."""
    started_at = time.time()
    output_path = Path.cwd() / "agent-last-message.txt"
    result_path = Path.cwd() / "agent-result.json"
    cleanup_path: Path | None = None
    gact_config_root: Path | None = None
    gact_env: dict[str, str] = {}
    relay_job_id = _safe_optional_str(params.get("relay_job_id"))
    agent_bin = str(params.get("agent_bin", ""))
    adapter = str(params.get("agent_adapter", "exec"))
    prompt_path = _safe_optional_path(params.get("prompt_path"))
    mcp_config_path = _safe_optional_path(params.get("mcp_config_path"))
    workdir = _safe_optional_path(params.get("workdir"))
    model = params.get("model") if isinstance(params.get("model"), str) else None
    try:
        agent_bin = _required_str(params, "agent_bin")
        prompt_path = Path(_required_str(params, "prompt_path"))
        prompt_text = _read_utf8_bounded(
            prompt_path,
            limit=AGENT_PROMPT_MAX_BYTES,
            label="agent prompt",
        )
        prompt_text = _append_context(prompt_text, params.get("context"))
        _require_text_within_limit(
            prompt_text,
            limit=AGENT_PROMPT_MAX_BYTES,
            label="agent prompt with context",
        )
        mcp_config_path = _optional_path(params.get("mcp_config_path"))
        workdir = _optional_path(params.get("workdir"))
        timeout = _optional_int(params.get("timeout_seconds"))
        rendered_model = model
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
        elif adapter == "gact":
            command = _gact_command(
                agent_bin=agent_bin,
                agent_args=agent_args,
                prompt_text=prompt_text,
            )
            gact_env, gact_config_root = _gact_environment(
                mcp_config_path=mcp_config_path,
                model=rendered_model,
            )
        else:
            raise ValueError(f"unsupported agent_adapter: {adapter}")

        try:
            if adapter == "gact":
                result = _run_process(
                    command,
                    cwd=workdir,
                    timeout=timeout,
                    capture_stdout=True,
                    env_overrides=gact_env,
                )
                if result.returncode == 0:
                    _write_gact_last_message(result.stdout, output_path=output_path)
            else:
                result = _run_process(command, cwd=workdir, timeout=timeout)
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
        final_answer_artifact_ref = (
            _mint_final_answer_artifact_ref(
                output_path,
                relay_job_id=relay_job_id,
                adapter=adapter,
                agent_bin=agent_bin,
                model=rendered_model,
            )
            if returncode == 0 and output_path.exists()
            else None
        )
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
            final_answer_artifact_ref=final_answer_artifact_ref,
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
            final_answer_artifact_ref=None,
        )
        return 2
    finally:
        if cleanup_path is not None:
            cleanup_path.unlink(missing_ok=True)
        if gact_config_root is not None:
            _remove_gact_config_root(gact_config_root)


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
    profile_bytes = _read_bytes_bounded(
        mcp_config_path,
        limit=CODEX_PROFILE_MAX_BYTES,
        label="Codex MCP profile",
    )
    try:
        profile_bytes.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError("Codex MCP profile must be UTF-8") from exc
    flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
    flags |= getattr(os, "O_BINARY", 0)
    flags |= getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOINHERIT", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(profile_path, flags, 0o600)
        try:
            _write_all(descriptor, profile_bytes)
        finally:
            os.close(descriptor)
        profile_path.chmod(0o600)
    except OSError as exc:
        try:
            profile_path.unlink(missing_ok=True)
        except OSError as cleanup_exc:
            raise OSError(
                f"private Codex profile setup and cleanup both failed: {cleanup_exc}"
            ) from exc
        raise
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


def _gact_command(
    *,
    agent_bin: str,
    agent_args: list[str],
    prompt_text: str,
) -> list[str]:
    """Build the non-interactive CLIO/GACT command executed through uv."""
    prefix = agent_args or ["run", "--no-sync", "clio-agent"]
    return [agent_bin, *prefix, "--query", prompt_text, "--json"]


def _gact_environment(
    *,
    mcp_config_path: Path | None,
    model: str | None,
) -> tuple[dict[str, str], Path | None]:
    """Build CLIO overrides and stage its private user MCP configuration."""
    overrides = {"CLIO_LM_MODEL": model} if model is not None else {}
    if mcp_config_path is None:
        return overrides, None
    root = Path(tempfile.mkdtemp(prefix="clio-relay-gact-"))
    try:
        config_dir = root / "clio-agent"
        config_dir.mkdir(mode=0o700)
        target = config_dir / "mcp.yaml"
        payload = _read_bytes_bounded(
            mcp_config_path,
            limit=CODEX_PROFILE_MAX_BYTES,
            label="CLIO MCP configuration",
        )
        try:
            payload.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ValueError("CLIO MCP configuration must be UTF-8") from exc
        flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
        flags |= getattr(os, "O_BINARY", 0)
        flags |= getattr(os, "O_CLOEXEC", 0)
        flags |= getattr(os, "O_NOINHERIT", 0)
        flags |= getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(target, flags, 0o600)
        try:
            _write_all(descriptor, payload)
        finally:
            os.close(descriptor)
        target.chmod(0o600)
    except (OSError, ValueError):
        _remove_gact_config_root(root)
        raise
    overrides["XDG_CONFIG_HOME"] = str(root)
    return overrides, root


def _remove_gact_config_root(root: Path) -> None:
    """Remove only the exact private CLIO profile tree created by this run."""
    config_dir = root / "clio-agent"
    (config_dir / "mcp.yaml").unlink(missing_ok=True)
    if config_dir.exists():
        config_dir.rmdir()
    if root.exists():
        root.rmdir()


def _run_process(
    command: list[str],
    *,
    cwd: Path | None,
    timeout: int | None,
    capture_stdout: bool = False,
    env_overrides: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    child_env = _scrubbed_env()
    if env_overrides is not None:
        child_env.update(env_overrides)
    process = subprocess.Popen(
        command,
        cwd=cwd,
        env=child_env,
        text=True,
        stdout=subprocess.PIPE if capture_stdout else None,
        **nested_popen_kwargs(child_env),
    )
    try:
        if capture_stdout:
            stdout, _stderr = process.communicate(timeout=timeout)
            returncode = process.returncode
        else:
            returncode = process.wait(timeout=timeout)
            stdout = None
    except subprocess.TimeoutExpired:
        _terminate_process_tree(process)
        raise
    return subprocess.CompletedProcess(command, returncode, stdout=stdout)


def _write_gact_last_message(stdout: str | None, *, output_path: Path) -> None:
    """Extract CLIO's JSON answer and preserve its raw terminal output."""
    if stdout is None:
        raise ValueError("gact adapter did not capture CLIO output")
    print(stdout, end="" if stdout.endswith("\n") else "\n", flush=True)
    try:
        value = json.loads(stdout)
    except json.JSONDecodeError as exc:
        raise ValueError("gact adapter expected one CLIO JSON result") from exc
    if not isinstance(value, dict):
        raise ValueError("gact adapter CLIO result must be an object")
    answer = cast(dict[str, object], value).get("answer")
    if not isinstance(answer, str):
        raise ValueError("gact adapter CLIO result omitted its answer")
    _require_text_within_limit(
        answer,
        limit=AGENT_FINAL_ANSWER_MAX_BYTES,
        label="gact final answer",
    )
    output_path.write_text(answer, encoding="utf-8")


def _terminate_process_tree(process: subprocess.Popen[str]) -> None:
    terminate_nested_process(process)


def _scrubbed_env() -> dict[str, str]:
    env = os.environ.copy()
    for name in list(env):
        if _relay_owned_environment_name(name):
            env.pop(name, None)
    return env


def _write_agent_result(
    *,
    result_path: Path,
    agent_bin: str,
    adapter: str,
    prompt_path: Path | None,
    mcp_config_path: Path | None,
    model: str | None,
    workdir: Path | None,
    returncode: int,
    timed_out: bool,
    started_at: float,
    output_path: Path,
    error_type: str | None,
    error_message: str | None,
    final_answer_artifact_ref: dict[str, Any] | None,
) -> None:
    finished_at = time.time()
    result = {
        "adapter": adapter,
        "agent_bin": agent_bin,
        "prompt_path": None if prompt_path is None else str(prompt_path),
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
        "final_answer_artifact_ref": final_answer_artifact_ref,
    }
    result_path.write_text(json.dumps(result, indent=2, sort_keys=True), encoding="utf-8")


def _mint_final_answer_artifact_ref(
    output_path: Path,
    *,
    relay_job_id: str | None,
    adapter: str,
    agent_bin: str,
    model: str | None,
) -> dict[str, Any]:
    """Mint the child's final answer through CLIO's converged projection."""
    if relay_job_id is None:
        raise ValueError("relay_job_id is required to mint the final answer artifact")
    payload = _read_bytes_bounded(
        output_path,
        limit=AGENT_FINAL_ANSWER_MAX_BYTES,
        label="agent final answer",
    )
    created_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    version = ArtifactVersion(
        kind=ArtifactKind.REPORT,
        custody=Custody.WORKSPACE_REFERENCED,
        mechanism=Mechanism.MODEL,
        evidence=IdentityEvidence.hashed_at_use(
            sha256=hashlib.sha256(payload).hexdigest(),
            size_bytes=len(payload),
            mtime=output_path.stat().st_mtime,
        ),
        producer={
            "adapter": adapter,
            "agent_bin": agent_bin,
            "model": model,
        },
        path=str(output_path),
        created_at=created_at,
        annotation="Remote agent final answer",
    )
    projection = version.to_artifact_ref()
    raw_metadata = projection.get("metadata")
    if not isinstance(raw_metadata, dict):
        raise ValueError("clio final answer projection omitted metadata")
    metadata = cast(dict[str, Any], raw_metadata)
    metadata["clio.provenance.v1"] = {
        "job_id": relay_job_id,
        "uri": output_path.as_uri(),
        "size_bytes": len(payload),
        "created_at": created_at,
    }
    return projection


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


def _safe_optional_str(value: Any) -> str | None:
    return value if isinstance(value, str) and value else None


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    return int(value)


def _string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise ValueError("agent_args must be a list of strings")
    raw = cast(list[object], value)
    if not all(isinstance(item, str) for item in raw):
        raise ValueError("agent_args must be a list of strings")
    return [cast(str, item) for item in raw]


def _append_context(prompt_text: str, context: Any) -> str:
    if context is None:
        return prompt_text
    if not isinstance(context, dict):
        raise ValueError("context must be an object")
    typed_context = {str(key): value for key, value in cast(dict[object, object], context).items()}
    return (
        prompt_text.rstrip()
        + "\n\nRelay monitor context:\n"
        + json.dumps(typed_context, indent=2, sort_keys=True)
        + "\n"
    )


def _relay_owned_environment_name(name: str) -> bool:
    if name in RELAY_SIDE_CHANNEL_ENV_NAMES:
        return True
    return name.startswith("CLIO_RELAY_") and (name.endswith("_TOKEN") or name.endswith("_SECRET"))


def _write_all(descriptor: int, payload: bytes) -> None:
    """Write an entire private profile or raise on a short write."""
    view = memoryview(payload)
    while view:
        written = os.write(descriptor, view)
        if written <= 0:
            raise OSError("private Codex profile write was incomplete")
        view = view[written:]


def _read_utf8_bounded(path: Path, *, limit: int, label: str) -> str:
    """Read a UTF-8 file without ever materializing more than its limit."""
    payload = _read_bytes_bounded(path, limit=limit, label=label)
    try:
        return payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError(f"{label} must be UTF-8") from exc


def _read_bytes_bounded(path: Path, *, limit: int, label: str) -> bytes:
    """Read at most ``limit`` bytes and reject larger inputs."""
    with path.open("rb") as stream:
        payload = stream.read(limit + 1)
    if len(payload) > limit:
        raise ValueError(f"{label} exceeded its byte limit")
    return payload


def _require_text_within_limit(value: str, *, limit: int, label: str) -> None:
    if len(value.encode("utf-8")) > limit:
        raise ValueError(f"{label} exceeded its byte limit")

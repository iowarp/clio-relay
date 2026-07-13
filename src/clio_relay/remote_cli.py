"""SSH-backed execution helpers for cluster-targeted CLI commands."""

from __future__ import annotations

import hashlib
import os
import posixpath
import shlex
import subprocess
from collections.abc import Generator
from contextlib import contextmanager
from contextvars import ContextVar
from pathlib import Path
from typing import cast

import yaml

from clio_relay.cluster_config import ClusterDefinition
from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.jarvis_mcp import JARVIS_MCP_SPACK_COMMAND_ENV

_REMOTE_COMMAND_TIMEOUT_SECONDS: ContextVar[float | None] = ContextVar(
    "clio_relay_remote_command_timeout_seconds",
    default=None,
)

_VALIDATION_PROVENANCE_ENV = (
    "CLIO_RELAY_VALIDATION_PRODUCER_GITHUB_LOGIN",
    "CLIO_RELAY_VALIDATION_PRODUCER_GITHUB_ID",
    "CLIO_RELAY_VALIDATION_INVOCATION_ID",
    "CLIO_RELAY_VALIDATION_LAUNCHER",
    "CLIO_RELAY_VALIDATION_ARTIFACT_SHA256",
)


@contextmanager
def remote_command_timeout(timeout_seconds: float) -> Generator[None, None, None]:
    """Bound nested remote CLI calls in the current execution context."""
    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be positive")
    token = _REMOTE_COMMAND_TIMEOUT_SECONDS.set(timeout_seconds)
    try:
        yield
    finally:
        _REMOTE_COMMAND_TIMEOUT_SECONDS.reset(token)


def should_execute_on_cluster(definition: ClusterDefinition) -> bool:
    """Return whether a cluster-targeted CLI command should be run over SSH."""
    mode = os.getenv("CLIO_RELAY_CLI_MODE", "auto").strip().lower()
    if mode == "local":
        return False
    if mode == "ssh":
        return True
    if mode != "auto":
        raise ConfigurationError("CLIO_RELAY_CLI_MODE must be one of: auto, local, ssh")
    if os.getenv("CLIO_RELAY_REMOTE_CLUSTER") == definition.name:
        return False
    return definition.ssh_host not in {"", "localhost", "127.0.0.1", "::1"}


def run_remote_clio(definition: ClusterDefinition, args: list[str]) -> str:
    """Run a clio-relay command on a configured cluster and return stdout."""
    rendered_args = " ".join(shlex.quote(arg) for arg in args)
    return run_remote_shell(definition, f"{remote_env(definition)} clio-relay {rendered_args}")


def run_remote_shell(definition: ClusterDefinition, script: str) -> str:
    """Run a bash script on a configured cluster through SSH."""
    command = ["ssh", definition.ssh_host, f"bash -lc {shlex.quote(script)}"]
    timeout_seconds = _REMOTE_COMMAND_TIMEOUT_SECONDS.get()
    try:
        if timeout_seconds is None:
            result = subprocess.run(command, capture_output=True, check=False)
        else:
            result = subprocess.run(
                command,
                capture_output=True,
                check=False,
                timeout=timeout_seconds,
            )
    except subprocess.TimeoutExpired as exc:
        raise RelayError(
            f"remote command timed out after {timeout_seconds:g} seconds: {definition.ssh_host}"
        ) from exc
    except OSError as exc:
        raise RelayError(f"remote command could not start: {definition.ssh_host}: {exc}") from exc
    if result.returncode != 0:
        raise RelayError(_command_error("remote command failed", result))
    return result.stdout.decode("utf-8", errors="replace")


def write_remote_file(definition: ClusterDefinition, remote_path: str, data: bytes) -> None:
    """Write private bytes to a remote path, creating the parent directory first."""
    parent = posixpath.dirname(remote_path)
    if parent:
        rendered_parent = shlex.quote(parent)
        run_remote_shell(
            definition,
            f"umask 077; mkdir -p {rendered_parent}; chmod 700 {rendered_parent}",
        )
    rendered_path = shlex.quote(remote_path)
    result = subprocess.run(
        [
            "ssh",
            definition.ssh_host,
            f"umask 077; cat > {rendered_path} && chmod 600 {rendered_path}",
        ],
        input=data,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        raise RelayError(_command_error("remote file write failed", result))


def remove_remote_file(
    definition: ClusterDefinition,
    remote_path: str,
    *,
    remove_empty_parent: bool = False,
) -> None:
    """Remove one explicitly owned staged file and optionally its empty parent."""
    rendered_path = shlex.quote(remote_path)
    script = f"rm -f -- {rendered_path}"
    parent = posixpath.dirname(remote_path)
    if remove_empty_parent and parent:
        script += f" && {{ rmdir -- {shlex.quote(parent)} 2>/dev/null || true; }}"
    run_remote_shell(definition, script)


def stage_jarvis_yaml(
    definition: ClusterDefinition,
    *,
    jarvis_yaml: Path,
    pipeline_yaml_text: str,
    idempotency_key: str,
) -> str:
    """Stage a local JARVIS YAML and declared input files on the cluster."""
    run_id = _submission_run_id(jarvis_yaml, idempotency_key)
    rendered_yaml = _stage_declared_files(
        definition,
        jarvis_yaml=jarvis_yaml,
        pipeline_yaml_text=pipeline_yaml_text,
        run_id=run_id,
    )
    remote_yaml = f".local/share/clio-relay/desktop-submissions/{run_id}/pipeline.yaml"
    write_remote_file(definition, remote_yaml, rendered_yaml.encode("utf-8"))
    return remote_yaml


def remote_env(definition: ClusterDefinition) -> str:
    """Render environment exports for remote clio-relay invocations."""
    jarvis_bin = definition.jarvis_bin or "$HOME/.local/bin/jarvis"
    frpc_bin = definition.frpc_bin or "$HOME/.local/bin/frpc"
    agent_bin = _cluster_agent_bin(definition)
    exports = [
        'export PATH="$HOME/.local/bin:$PATH";',
        'export UV="$HOME/.local/bin/uv";',
        'export CLIO_RELAY_VALIDATION_TOOL_EXECUTABLE="$HOME/.local/bin/clio-relay";',
        "export CLIO_RELAY_CLI_MODE=local;",
        f"export CLIO_RELAY_REMOTE_CLUSTER={shlex.quote(definition.name)};",
        f"export CLIO_RELAY_CORE_DIR={_shell_double_quoted(definition.core_dir)};",
        f"export CLIO_RELAY_SPOOL_DIR={_shell_double_quoted(definition.spool_dir)};",
        f"export CLIO_RELAY_JARVIS_BIN={_shell_double_quoted(jarvis_bin)};",
        f"export CLIO_RELAY_FRPC_BIN={_shell_double_quoted(frpc_bin)};",
        f"export CLIO_RELAY_AGENT_BIN={_shell_double_quoted(agent_bin)};",
        f"export CLIO_RELAY_AGENT_ADAPTER={shlex.quote(definition.agent_adapter)};",
    ]
    if definition.agent_args:
        exports.append(
            f"export CLIO_RELAY_AGENT_ARGS={shlex.quote(shlex.join(definition.agent_args))};"
        )
    if definition.spack_executable is not None:
        exports.append(
            f"export {JARVIS_MCP_SPACK_COMMAND_ENV}={shlex.quote(definition.spack_executable)};"
        )
    for name in _VALIDATION_PROVENANCE_ENV:
        value = os.environ.get(name)
        if value:
            exports.append(f"export {name}={shlex.quote(value)};")
    return " ".join(exports)


def _stage_declared_files(
    definition: ClusterDefinition,
    *,
    jarvis_yaml: Path,
    pipeline_yaml_text: str,
    run_id: str,
) -> str:
    loaded = cast(object, yaml.safe_load(pipeline_yaml_text))
    if not isinstance(loaded, dict):
        return pipeline_yaml_text
    document = cast(dict[str, object], loaded)
    relay_extension = document.pop("x_clio_relay", None)
    if relay_extension is not None:
        if not isinstance(relay_extension, dict):
            raise ConfigurationError("x_clio_relay must be an object")
        stage_files = cast(dict[str, object], relay_extension).get("stage_files", [])
        if not isinstance(stage_files, list):
            raise ConfigurationError("x_clio_relay.stage_files must be a list")
        for item in cast(list[object], stage_files):
            _stage_file(definition, jarvis_yaml=jarvis_yaml, item=item, run_id=run_id)
    formatted_document = _format_run_id(document, run_id)
    return yaml.safe_dump(formatted_document, sort_keys=False)


def _stage_file(
    definition: ClusterDefinition,
    *,
    jarvis_yaml: Path,
    item: object,
    run_id: str,
) -> None:
    if not isinstance(item, dict):
        raise ConfigurationError("x_clio_relay.stage_files entries must be objects")
    typed_item = cast(dict[str, object], item)
    local_path_value = typed_item.get("local_path")
    remote_path_value = typed_item.get("remote_path")
    if not isinstance(local_path_value, str) or not isinstance(remote_path_value, str):
        raise ConfigurationError(
            "x_clio_relay.stage_files entries require local_path and remote_path strings"
        )
    local_path = Path(local_path_value)
    if not local_path.is_absolute():
        local_path = jarvis_yaml.parent / local_path
    if not local_path.exists():
        raise ConfigurationError(f"staged file does not exist: {local_path}")
    remote_path = remote_path_value.format(run_id=run_id)
    write_remote_file(definition, remote_path, local_path.read_bytes())


def _format_run_id(value: object, run_id: str) -> object:
    if isinstance(value, str):
        return value.format(run_id=run_id)
    if isinstance(value, list):
        return [_format_run_id(item, run_id) for item in cast(list[object], value)]
    if isinstance(value, dict):
        typed = cast(dict[object, object], value)
        return {str(key): _format_run_id(item, run_id) for key, item in typed.items()}
    return value


def _submission_run_id(jarvis_yaml: Path, idempotency_key: str) -> str:
    digest = hashlib.sha256(idempotency_key.encode("utf-8")).hexdigest()[:16]
    return f"{jarvis_yaml.stem}-{digest}"


def _cluster_agent_bin(definition: ClusterDefinition) -> str:
    if definition.agent_bin is not None:
        return definition.agent_bin
    if definition.agent_npm_bin is not None:
        return f"$HOME/.local/bin/{definition.agent_npm_bin}"
    return "agent"


def _shell_double_quoted(value: str) -> str:
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'


def _command_error(prefix: str, result: subprocess.CompletedProcess[bytes]) -> str:
    stdout = result.stdout.decode("utf-8", errors="replace").strip()
    stderr = result.stderr.decode("utf-8", errors="replace").strip()
    detail = stderr or stdout
    return f"{prefix}: {detail}"

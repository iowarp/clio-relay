"""JARVIS-CD provider boundary.

clio-relay translates durable relay intents into JARVIS-CD package/pipeline
inputs. JARVIS-CD remains responsible for scheduler submission, deployment,
environment capture, output collection, and provenance.
"""

from __future__ import annotations

import os
import shutil
import signal
import subprocess
import threading
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any, cast

import yaml

from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.models import JarvisRunSpec, McpCallSpec, RemoteAgentTaskSpec


class JarvisCdProvider:
    """Materialize and invoke relay jobs through JARVIS-CD."""

    def __init__(
        self,
        *,
        jarvis_bin: str = "jarvis",
        agent_bin: str = "agent",
        agent_adapter: str = "exec",
        agent_args: list[str] | None = None,
    ) -> None:
        self.jarvis_bin = jarvis_bin
        self.agent_bin = agent_bin
        self.agent_adapter = agent_adapter
        self.agent_args = agent_args or []

    def require_available(self) -> None:
        """Raise if the configured JARVIS executable is unavailable."""
        if shutil.which(self.jarvis_bin) is None:
            raise ConfigurationError(f"JARVIS-CD executable not found: {self.jarvis_bin}")

    def render_bounded_command_yaml(self, spec: JarvisRunSpec) -> str:
        """Render a bounded-command JARVIS pipeline YAML document."""
        if spec.pipeline_yaml is not None:
            return spec.pipeline_yaml
        if spec.pipeline_path is not None:
            return spec.pipeline_path.read_text(encoding="utf-8")
        if spec.command is None:
            raise ConfigurationError(
                "JarvisRunSpec requires pipeline_yaml, pipeline_path, or command"
            )
        document: dict[str, Any] = {
            "name": spec.package or "clio-relay-bounded-command",
            "pkgs": [
                {
                    "pkg_type": "clio_relay.bounded_command",
                    "pkg_name": "bounded_command",
                    "command": spec.command,
                    "workdir": str(spec.workdir) if spec.workdir is not None else None,
                    "env": spec.env,
                    "timeout_seconds": spec.timeout_seconds,
                    "progress": spec.progress,
                }
            ],
        }
        return yaml.safe_dump(_drop_none(document), sort_keys=False)

    def render_remote_agent_task_yaml(self, spec: RemoteAgentTaskSpec) -> str:
        """Render a JARVIS pipeline for a remote agent task."""
        document: dict[str, Any] = {
            "name": "clio-relay-remote-agent",
            "pkgs": [
                {
                    "pkg_type": "clio_relay.remote_agent",
                    "pkg_name": "remote_agent",
                    "agent_bin": self.agent_bin,
                    "agent_adapter": self.agent_adapter,
                    "agent_args": self.agent_args,
                    "prompt_path": str(spec.prompt_path),
                    "mcp_config_path": (
                        str(spec.mcp_config_path) if spec.mcp_config_path is not None else None
                    ),
                    "model": spec.model,
                    "workdir": str(spec.workdir) if spec.workdir is not None else None,
                    "timeout_seconds": spec.timeout_seconds,
                }
            ],
        }
        return yaml.safe_dump(_drop_none(document), sort_keys=False)

    def render_mcp_call_yaml(self, spec: McpCallSpec) -> str:
        """Render a JARVIS pipeline for a remote MCP tool call."""
        document: dict[str, Any] = {
            "name": "clio-relay-mcp-call",
            "pkgs": [
                {
                    "pkg_type": "clio_relay.mcp_call",
                    "pkg_name": "mcp_call",
                    "server": spec.server,
                    "tool": spec.tool,
                    "arguments": spec.arguments,
                    "timeout_seconds": spec.timeout_seconds,
                }
            ],
        }
        return yaml.safe_dump(_drop_none(document), sort_keys=False)

    def write_pipeline(self, yaml_text: str, path: Path) -> Path:
        """Write a rendered JARVIS pipeline YAML file."""
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(yaml_text, encoding="utf-8")
        return path

    def run_pipeline(
        self,
        pipeline_path: Path,
        *,
        cwd: Path | None = None,
    ) -> subprocess.CompletedProcess[str]:
        """Invoke JARVIS-CD for an already materialized pipeline."""
        self.require_available()
        command = [self.jarvis_bin, "ppl", "run", "yaml", str(pipeline_path)]
        try:
            return subprocess.run(
                command,
                cwd=cwd,
                text=True,
                capture_output=True,
                check=False,
            )
        except OSError as exc:
            raise RelayError(f"failed to execute JARVIS-CD: {exc}") from exc

    def run_pipeline_streaming(
        self,
        pipeline_path: Path,
        *,
        cwd: Path | None = None,
        on_stdout: Callable[[str], None] | None = None,
        on_stderr: Callable[[str], None] | None = None,
        on_start: Callable[[int], None] | None = None,
        should_cancel: Callable[[], bool] | None = None,
        on_poll: Callable[[], None] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        """Invoke JARVIS-CD and stream output chunks while retaining final output."""
        self.require_available()
        command = [self.jarvis_bin, "ppl", "run", "yaml", str(pipeline_path)]
        try:
            process = subprocess.Popen(
                command,
                cwd=cwd,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                bufsize=1,
                start_new_session=os.name != "nt",
                creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0,
            )
        except OSError as exc:
            raise RelayError(f"failed to execute JARVIS-CD: {exc}") from exc
        if on_start is not None:
            on_start(process.pid)

        stdout_chunks: list[str] = []
        stderr_chunks: list[str] = []
        stdout_thread = threading.Thread(
            target=_drain_stream,
            args=(process.stdout, stdout_chunks, on_stdout),
            daemon=True,
        )
        stderr_thread = threading.Thread(
            target=_drain_stream,
            args=(process.stderr, stderr_chunks, on_stderr),
            daemon=True,
        )
        stdout_thread.start()
        stderr_thread.start()
        canceled = False
        while True:
            return_code = process.poll()
            if return_code is not None:
                break
            if should_cancel is not None and should_cancel():
                canceled = True
                _terminate_process(process)
                return_code = process.wait()
                break
            if on_poll is not None:
                on_poll()
            time.sleep(0.25)
        stdout_thread.join()
        stderr_thread.join()
        return subprocess.CompletedProcess(
            command,
            return_code if not canceled else -15,
            stdout="".join(stdout_chunks),
            stderr="".join(stderr_chunks),
        )


def _drop_none(value: Any) -> Any:
    if isinstance(value, dict):
        typed = cast(dict[str, Any], value)
        return {key: _drop_none(item) for key, item in typed.items() if item is not None}
    if isinstance(value, list):
        typed_list = cast(list[Any], value)
        return [_drop_none(item) for item in typed_list]
    return value


def _drain_stream(
    stream: Any,
    chunks: list[str],
    callback: Callable[[str], None] | None,
) -> None:
    if stream is None:
        return
    for chunk in stream:
        chunks.append(chunk)
        if callback is not None:
            callback(chunk)


def _terminate_process(process: subprocess.Popen[str]) -> None:
    if process.poll() is not None:
        return
    if os.name == "nt":
        process.send_signal(signal.CTRL_BREAK_EVENT)
        try:
            process.wait(timeout=5)
            return
        except subprocess.TimeoutExpired:
            process.kill()
            return
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            return

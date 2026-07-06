"""JARVIS-CD provider boundary.

clio-relay translates durable relay intents into JARVIS-CD package/pipeline
inputs. JARVIS-CD remains responsible for scheduler submission, deployment,
environment capture, output collection, and provenance.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path
from typing import Any, cast

import yaml

from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.models import JarvisRunSpec, McpCallSpec, RemoteAgentTaskSpec


class JarvisCdProvider:
    """Materialize and invoke relay jobs through JARVIS-CD."""

    def __init__(self, *, jarvis_bin: str = "jarvis", codex_bin: str = "codex") -> None:
        self.jarvis_bin = jarvis_bin
        self.codex_bin = codex_bin

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
            "packages": [
                {
                    "name": "clio-relay.bounded-command",
                    "parameters": {
                        "command": spec.command,
                        "workdir": str(spec.workdir) if spec.workdir is not None else None,
                        "env": spec.env,
                        "timeout_seconds": spec.timeout_seconds,
                    },
                }
            ],
        }
        return yaml.safe_dump(_drop_none(document), sort_keys=False)

    def render_codex_task_yaml(self, spec: RemoteAgentTaskSpec) -> str:
        """Render a JARVIS pipeline for a remote Codex agent task."""
        document: dict[str, Any] = {
            "name": "clio-relay-codex-agent",
            "packages": [
                {
                    "name": "clio-relay.codex-agent",
                    "parameters": {
                        "codex_bin": self.codex_bin,
                        "prompt_path": str(spec.prompt_path),
                        "mcp_config_path": str(spec.mcp_config_path),
                        "model": spec.model,
                        "workdir": str(spec.workdir) if spec.workdir is not None else None,
                        "timeout_seconds": spec.timeout_seconds,
                    },
                }
            ],
        }
        return yaml.safe_dump(_drop_none(document), sort_keys=False)

    def render_mcp_call_yaml(self, spec: McpCallSpec) -> str:
        """Render a JARVIS pipeline for a remote MCP tool call."""
        document: dict[str, Any] = {
            "name": "clio-relay-mcp-call",
            "packages": [
                {
                    "name": "clio-relay.mcp-call",
                    "parameters": {
                        "server": spec.server,
                        "tool": spec.tool,
                        "arguments": spec.arguments,
                        "timeout_seconds": spec.timeout_seconds,
                    },
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
        command = [self.jarvis_bin, "pipeline", "run", str(pipeline_path)]
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


def _drop_none(value: Any) -> Any:
    if isinstance(value, dict):
        typed = cast(dict[str, Any], value)
        return {key: _drop_none(item) for key, item in typed.items() if item is not None}
    if isinstance(value, list):
        typed_list = cast(list[Any], value)
        return [_drop_none(item) for item in typed_list]
    return value

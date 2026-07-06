"""JARVIS-CD package for remote agent tasks."""

from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Any

from jarvis_cd.core.pkg import Application


class RemoteAgent(Application):
    """Run a configured agent binary against a prompt and MCP config."""

    def _init(self) -> None:
        """Initialize package state."""

    def _configure_menu(self) -> list[dict[str, Any]]:
        """Return JARVIS configurator options."""
        return []

    def _configure(self, **kwargs: Any) -> None:
        """Store configuration provided by the pipeline YAML."""
        self.config.update(kwargs)

    def start(self) -> None:
        """Run the configured agent binary."""
        if "agent_bin" not in self.config:
            raise ValueError("agent_bin is required")
        agent_bin = str(self.config["agent_bin"])
        prompt_path = Path(str(self.config["prompt_path"]))
        mcp_config_path = Path(str(self.config["mcp_config_path"]))
        command = [
            agent_bin,
            "--mcp-config",
            str(mcp_config_path),
            "exec",
            prompt_path.read_text(encoding="utf-8"),
        ]
        model = self.config.get("model")
        if isinstance(model, str) and model:
            command[1:1] = ["--model", model]
        workdir_value = self.config.get("workdir")
        workdir = Path(workdir_value) if isinstance(workdir_value, str) else None
        timeout_value = self.config.get("timeout_seconds")
        timeout = int(timeout_value) if timeout_value is not None else None
        result = subprocess.run(command, cwd=workdir, timeout=timeout, check=False)
        if result.returncode != 0:
            raise RuntimeError(f"agent failed with exit code {result.returncode}")

    def stop(self) -> None:
        """Stop hook for remote agent tasks."""

    def clean(self) -> None:
        """Clean hook for remote agent tasks."""

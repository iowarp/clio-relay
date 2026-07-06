"""JARVIS-CD package for bounded relay commands."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path
from typing import Any

from jarvis_cd.core.pkg import Application


class BoundedCommand(Application):
    """Execute a bounded command and let JARVIS-CD capture provenance."""

    def _init(self) -> None:
        """Initialize package state."""

    def _configure_menu(self) -> list[dict[str, Any]]:
        """Return JARVIS configurator options."""
        return []

    def _configure(self, **kwargs: Any) -> None:
        """Store configuration provided by the pipeline YAML."""
        self.config.update(kwargs)

    def start(self) -> None:
        """Run the configured command."""
        command = self.config.get("command")
        if not isinstance(command, list) or not all(isinstance(item, str) for item in command):
            raise ValueError("command must be a string array")
        env = os.environ.copy()
        supplied_env = self.config.get("env", {})
        if isinstance(supplied_env, dict):
            env.update({str(key): str(value) for key, value in supplied_env.items()})
        workdir_value = self.config.get("workdir")
        workdir = Path(workdir_value) if isinstance(workdir_value, str) else None
        timeout_value = self.config.get("timeout_seconds")
        timeout = int(timeout_value) if timeout_value is not None else None
        result = subprocess.run(command, cwd=workdir, env=env, timeout=timeout, check=False)
        if result.returncode != 0:
            raise RuntimeError(f"command failed with exit code {result.returncode}")

    def stop(self) -> None:
        """Stop hook for bounded commands."""

    def clean(self) -> None:
        """Clean hook for bounded commands."""

"""JARVIS-CD package for MCP tool calls."""

from __future__ import annotations

import json
import subprocess
from typing import Any

from jarvis_cd.core.pkg import Application


class McpCall(Application):
    """Call a stdio MCP server tool."""

    def _init(self) -> None:
        """Initialize package state."""

    def _configure_menu(self) -> list[dict[str, Any]]:
        """Return JARVIS configurator options."""
        return []

    def _configure(self, **kwargs: Any) -> None:
        """Store configuration provided by the pipeline YAML."""
        self.config.update(kwargs)

    def start(self) -> None:
        """Run a single MCP tools/call request."""
        server = str(self.config["server"])
        tool = str(self.config["tool"])
        arguments = self.config.get("arguments", {})
        timeout_value = self.config.get("timeout_seconds")
        timeout = int(timeout_value) if timeout_value is not None else None
        request = {
            "jsonrpc": "2.0",
            "id": "clio-relay-mcp-call",
            "method": "tools/call",
            "params": {"name": tool, "arguments": arguments},
        }
        result = subprocess.run(
            [server],
            input=json.dumps(request) + "\n",
            text=True,
            timeout=timeout,
            check=False,
        )
        if result.returncode != 0:
            raise RuntimeError(f"MCP call failed with exit code {result.returncode}")

    def stop(self) -> None:
        """Stop hook for MCP calls."""

    def clean(self) -> None:
        """Clean hook for MCP calls."""

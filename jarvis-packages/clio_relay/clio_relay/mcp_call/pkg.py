"""JARVIS-CD package for MCP tool calls."""

from __future__ import annotations

import json
import subprocess
import sys
import time
from pathlib import Path
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
        started_at = time.time()
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
            capture_output=True,
            timeout=timeout,
            check=False,
        )
        finished_at = time.time()
        Path("mcp-result.json").write_text(
            json.dumps(
                {
                    "server": server,
                    "tool": tool,
                    "arguments": arguments,
                    "returncode": result.returncode,
                    "stdout": result.stdout,
                    "stderr": result.stderr,
                    "started_at": started_at,
                    "finished_at": finished_at,
                    "duration_seconds": finished_at - started_at,
                },
                indent=2,
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        if result.stdout:
            print(result.stdout, end="")
        if result.stderr:
            print(result.stderr, end="", file=sys.stderr)
        if result.returncode != 0:
            raise RuntimeError(f"MCP call failed with exit code {result.returncode}")

    def stop(self) -> None:
        """Stop hook for MCP calls."""

    def clean(self) -> None:
        """Clean hook for MCP calls."""

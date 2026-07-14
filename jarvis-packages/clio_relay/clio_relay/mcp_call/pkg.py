"""JARVIS-CD package for MCP tool calls."""

from __future__ import annotations

import sys
from typing import Any

from clio_relay._jarvis_api import Application
from clio_relay.mcp_call.runner import run_mcp_call_from_params


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
        return_code = run_mcp_call_from_params(dict(self.config))
        result_text = ""
        result_path = "mcp-result.json"
        try:
            import json
            from pathlib import Path

            result = json.loads(Path(result_path).read_text(encoding="utf-8"))
            result_text = str(result.get("stdout") or "")
            error_text = str(result.get("stderr") or "")
        except (OSError, ValueError):
            error_text = ""
        if result_text:
            print(result_text, end="")
        if error_text:
            print(error_text, end="", file=sys.stderr)
        if return_code != 0:
            raise RuntimeError(f"MCP call failed with exit code {return_code}")

    def stop(self) -> None:
        """Stop hook for MCP calls."""

    def clean(self) -> None:
        """Clean hook for MCP calls."""

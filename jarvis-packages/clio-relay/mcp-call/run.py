"""JARVIS package entrypoint for MCP tool calls."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any


def main() -> int:
    """Call an MCP tool through a stdio server command.

    The `server` parameter is interpreted as an executable command available in
    the Ares environment. This keeps relay code provider-neutral while letting
    JARVIS-CD capture process provenance.
    """
    params = _load_params()
    server = str(params["server"])
    tool = str(params["tool"])
    arguments = params.get("arguments", {})
    timeout_value = params.get("timeout_seconds")
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
    return result.returncode


def _load_params() -> dict[str, Any]:
    path = os.getenv("JARVIS_PARAMS_JSON")
    if path is None:
        return json.loads(sys.stdin.read() or "{}")
    return json.loads(Path(path).read_text(encoding="utf-8"))


if __name__ == "__main__":
    raise SystemExit(main())

"""JARVIS package entrypoint for MCP tool calls."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any


def main() -> int:
    """Call an MCP tool through a stdio server command.

    The `server` parameter is interpreted as an executable command available in
    the cluster environment. This keeps relay code provider-neutral while letting
    JARVIS-CD capture process provenance.
    """
    params = _load_params()
    server = str(params["server"])
    tool = str(params["tool"])
    arguments = params.get("arguments", {})
    timeout_value = params.get("timeout_seconds")
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
    return result.returncode


def _load_params() -> dict[str, Any]:
    path = os.getenv("JARVIS_PARAMS_JSON")
    if path is None:
        return json.loads(sys.stdin.read() or "{}")
    return json.loads(Path(path).read_text(encoding="utf-8"))


if __name__ == "__main__":
    raise SystemExit(main())

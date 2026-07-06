"""JARVIS package entrypoint for Codex agent runs."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any


def main() -> int:
    """Run Codex with the same shell-visible binary configured for the endpoint."""
    params = _load_params()
    codex_bin = str(params.get("codex_bin", "codex"))
    prompt_path = Path(str(params["prompt_path"]))
    mcp_config_path = Path(str(params["mcp_config_path"]))
    command = [
        codex_bin,
        "--mcp-config",
        str(mcp_config_path),
        "exec",
        prompt_path.read_text(encoding="utf-8"),
    ]
    model = params.get("model")
    if isinstance(model, str) and model:
        command[1:1] = ["--model", model]
    workdir_value = params.get("workdir")
    workdir = Path(workdir_value) if isinstance(workdir_value, str) else None
    timeout_value = params.get("timeout_seconds")
    timeout = int(timeout_value) if timeout_value is not None else None
    result = subprocess.run(command, cwd=workdir, timeout=timeout, check=False)
    return result.returncode


def _load_params() -> dict[str, Any]:
    path = os.getenv("JARVIS_PARAMS_JSON")
    if path is None:
        return json.loads(sys.stdin.read() or "{}")
    return json.loads(Path(path).read_text(encoding="utf-8"))


if __name__ == "__main__":
    raise SystemExit(main())

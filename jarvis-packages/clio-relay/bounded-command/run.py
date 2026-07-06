"""JARVIS package entrypoint for bounded commands."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any


def main() -> int:
    """Run the bounded command described by a JSON parameter file."""
    params = _load_params()
    command = params.get("command")
    if not isinstance(command, list) or not all(isinstance(item, str) for item in command):
        print("parameter 'command' must be a string array", file=sys.stderr)
        return 2
    env = os.environ.copy()
    supplied_env = params.get("env", {})
    if isinstance(supplied_env, dict):
        env.update({str(key): str(value) for key, value in supplied_env.items()})
    workdir_value = params.get("workdir")
    workdir = Path(workdir_value) if isinstance(workdir_value, str) else None
    timeout_value = params.get("timeout_seconds")
    timeout = int(timeout_value) if timeout_value is not None else None
    result = subprocess.run(command, cwd=workdir, env=env, timeout=timeout, check=False)
    return result.returncode


def _load_params() -> dict[str, Any]:
    path = os.getenv("JARVIS_PARAMS_JSON")
    if path is None:
        return json.loads(sys.stdin.read() or "{}")
    return json.loads(Path(path).read_text(encoding="utf-8"))


if __name__ == "__main__":
    raise SystemExit(main())

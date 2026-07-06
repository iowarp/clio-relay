"""JARVIS package entrypoint for remote agent runs."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any

from clio_relay.remote_agent.runner import run_remote_agent_from_params


def main() -> int:
    """Run the configured agent binary."""
    return run_remote_agent_from_params(_load_params())


def _load_params() -> dict[str, Any]:
    path = os.getenv("JARVIS_PARAMS_JSON")
    if path is None:
        return json.loads(sys.stdin.read() or "{}")
    return json.loads(Path(path).read_text(encoding="utf-8"))


if __name__ == "__main__":
    raise SystemExit(main())

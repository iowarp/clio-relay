"""Process settings read from the environment."""

from __future__ import annotations

import os
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

TransportMode = Literal["brokered_tcp", "udp_rendezvous", "ssh_forward"]
"""How one remote connection reaches its remote relay.

The mode is a deployment-time choice, made per connection. Nothing probes one
mode and switches to another: a link failure re-establishes the same mode or
reports a typed failure. ``brokered_tcp`` is TCP joined by a reachable server,
``udp_rendezvous`` is the same rendezvous with NAT traversal, and
``ssh_forward`` is for infrastructure that permits nothing else.
"""


@dataclass(frozen=True)
class RelaySettings:
    """The local tools relay starts."""

    frpc_bin: str = "frpc"

    @classmethod
    def from_env(cls) -> RelaySettings:
        """Resolve settings from ``CLIO_RELAY_*`` variables and the local tool paths."""
        return cls(frpc_bin=_tool_bin("CLIO_RELAY_FRPC_BIN", "frpc"))


def _tool_bin(env_name: str, name: str) -> str:
    """Return the configured tool, else the first working one on PATH or in a known place."""
    configured = os.getenv(env_name)
    if configured:
        return configured
    on_path = shutil.which(name)
    if on_path is not None and _works(Path(on_path)):
        return name
    candidates = [Path.home() / ".local" / "bin" / name, Path.cwd() / ".tools" / "frp" / "bin" / name]
    if os.name == "nt":
        candidates.append(Path.cwd() / ".tools" / "frp" / "bin" / f"{name}.exe")
    for candidate in candidates:
        if candidate.exists() and _works(candidate):
            return str(candidate.resolve())
    return name


def _works(path: Path) -> bool:
    try:
        result = subprocess.run(
            [str(path), "--version"], capture_output=True, text=True, timeout=5, check=False
        )
    except (OSError, subprocess.TimeoutExpired):
        return False
    return result.returncode == 0

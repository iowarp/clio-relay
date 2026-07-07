"""Configuration loading for clio-relay."""

from __future__ import annotations

import os
import shlex
import shutil
import subprocess
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field


class RelaySettings(BaseModel):
    """Runtime settings loaded from environment variables."""

    model_config = ConfigDict(extra="forbid")

    core_dir: Path = Field(default_factory=lambda: Path(".clio-relay/core"))
    spool_dir: Path = Field(default_factory=lambda: Path(".clio-relay/spool"))
    frps_addr: str | None = None
    frp_token: str | None = None
    jarvis_bin: str = "jarvis"
    frpc_bin: str = "frpc"
    api_token: str | None = None
    agent_bin: str = "agent"
    agent_adapter: str = "exec"
    agent_args: list[str] = Field(default_factory=list)

    @classmethod
    def from_env(cls) -> RelaySettings:
        """Load settings from the current process environment."""
        return cls(
            core_dir=_env_or_bootstrap_data_dir("CLIO_RELAY_CORE_DIR", "core"),
            spool_dir=_env_or_bootstrap_data_dir("CLIO_RELAY_SPOOL_DIR", "spool"),
            frps_addr=os.getenv("CLIO_RELAY_FRPS_ADDR"),
            frp_token=os.getenv("CLIO_RELAY_FRP_TOKEN"),
            jarvis_bin=_env_or_bootstrap_bin("CLIO_RELAY_JARVIS_BIN", "jarvis"),
            frpc_bin=_env_or_bootstrap_bin("CLIO_RELAY_FRPC_BIN", "frpc"),
            api_token=os.getenv("CLIO_RELAY_API_TOKEN"),
            agent_bin=os.getenv(
                "CLIO_RELAY_AGENT_BIN",
                "agent",
            ),
            agent_adapter=os.getenv("CLIO_RELAY_AGENT_ADAPTER", "exec"),
            agent_args=_split_args(os.getenv("CLIO_RELAY_AGENT_ARGS")),
        )


def _env_or_bootstrap_data_dir(env_name: str, family: str) -> Path:
    configured = os.getenv(env_name)
    if configured:
        return Path(configured).expanduser().resolve()
    bootstrap_path = Path.home() / ".local" / "share" / "clio-relay" / family
    if bootstrap_path.exists():
        return bootstrap_path.resolve()
    return Path(".clio-relay") / family


def _env_or_bootstrap_bin(env_name: str, executable_name: str) -> str:
    configured = os.getenv(env_name)
    if configured:
        return configured
    path_executable = shutil.which(executable_name)
    if path_executable is not None and _candidate_is_usable(executable_name, Path(path_executable)):
        return executable_name
    bootstrap_path = Path.home() / ".local" / "bin" / executable_name
    if bootstrap_path.exists() and _candidate_is_usable(executable_name, bootstrap_path):
        return str(bootstrap_path)
    local_tool_path = _local_tool_bin(executable_name)
    if local_tool_path is not None and _candidate_is_usable(executable_name, local_tool_path):
        return str(local_tool_path)
    return executable_name


def _candidate_is_usable(executable_name: str, path: Path) -> bool:
    if executable_name not in {"frpc", "frps"}:
        return True
    try:
        result = subprocess.run(
            [str(path), "--version"],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except OSError:
        return False
    except subprocess.TimeoutExpired:
        return False
    return result.returncode == 0


def _local_tool_bin(executable_name: str) -> Path | None:
    candidates = [Path.cwd() / ".tools" / "frp" / "bin" / executable_name]
    if os.name == "nt" and not executable_name.lower().endswith(".exe"):
        candidates.append(Path.cwd() / ".tools" / "frp" / "bin" / f"{executable_name}.exe")
    for candidate in candidates:
        if candidate.exists():
            return candidate.resolve()
    return None


def _split_args(value: str | None) -> list[str]:
    if value is None or value.strip() == "":
        return []
    return shlex.split(value)

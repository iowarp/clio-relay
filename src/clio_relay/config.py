"""Configuration loading for clio-relay."""

from __future__ import annotations

import os
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
    agent_bin: str = "codex"

    @classmethod
    def from_env(cls) -> RelaySettings:
        """Load settings from the current process environment."""
        return cls(
            core_dir=Path(os.getenv("CLIO_RELAY_CORE_DIR", ".clio-relay/core")),
            spool_dir=Path(os.getenv("CLIO_RELAY_SPOOL_DIR", ".clio-relay/spool")),
            frps_addr=os.getenv("CLIO_RELAY_FRPS_ADDR"),
            frp_token=os.getenv("CLIO_RELAY_FRP_TOKEN"),
            jarvis_bin=os.getenv("CLIO_RELAY_JARVIS_BIN", "jarvis"),
            frpc_bin=os.getenv("CLIO_RELAY_FRPC_BIN", "frpc"),
            agent_bin=os.getenv(
                "CLIO_RELAY_AGENT_BIN",
                "codex",
            ),
        )

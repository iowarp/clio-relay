"""Environment checks for local and live relay operation."""

from __future__ import annotations

import shutil

from clio_relay.config import RelaySettings
from clio_relay.errors import ConfigurationError


def check_required_binary(name: str, value: str) -> str:
    """Return a status line for a required executable."""
    resolved = shutil.which(value)
    if resolved is None:
        raise ConfigurationError(f"{name} not found: {value}")
    return f"{name}: {resolved}"


def run_doctor(settings: RelaySettings, *, live: bool = False) -> list[str]:
    """Run configuration checks and return human-readable status lines."""
    lines = [
        f"core_dir: {settings.core_dir}",
        f"spool_dir: {settings.spool_dir}",
    ]
    if live:
        if settings.frps_addr is None:
            raise ConfigurationError("CLIO_RELAY_FRPS_ADDR is required for live checks")
        if settings.frp_token is None:
            raise ConfigurationError("CLIO_RELAY_FRP_TOKEN is required for live checks")
        lines.append(f"frps_addr: {settings.frps_addr}")
        lines.append(check_required_binary("frpc", settings.frpc_bin))
        lines.append(check_required_binary("jarvis", settings.jarvis_bin))
        lines.append(check_required_binary("agent", settings.agent_bin))
    return lines

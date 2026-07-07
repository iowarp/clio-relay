"""Environment checks for local and live relay operation."""

from __future__ import annotations

import shutil
import subprocess

from clio_relay.cluster_config import ClusterDefinition
from clio_relay.config import RelaySettings
from clio_relay.errors import ConfigurationError, RelayError


def check_required_binary(name: str, value: str) -> str:
    """Return a status line for a required executable."""
    resolved = shutil.which(value)
    if resolved is None:
        raise ConfigurationError(f"{name} not found: {value}")
    return f"{name}: {resolved}"


def run_doctor(
    settings: RelaySettings,
    *,
    live: bool = False,
    frps_addr: str | None = None,
) -> list[str]:
    """Run configuration checks and return human-readable status lines."""
    lines = [
        f"core_dir: {settings.core_dir}",
        f"spool_dir: {settings.spool_dir}",
    ]
    if live:
        resolved_frps_addr = frps_addr or settings.frps_addr
        if resolved_frps_addr is None:
            raise ConfigurationError("CLIO_RELAY_FRPS_ADDR is required for live checks")
        lines.append(f"frps_addr: {resolved_frps_addr}")
        lines.append(f"frp_token: {'configured' if settings.frp_token is not None else 'missing'}")
        lines.append(check_required_binary("frpc", settings.frpc_bin))
    return lines


def run_cluster_doctor(definition: ClusterDefinition) -> list[str]:
    """Run live cluster-side checks over SSH and return status lines."""
    jarvis_bin = _shell_double_quote(definition.jarvis_bin or "$HOME/.local/bin/jarvis")
    frpc_bin = _shell_double_quote(definition.frpc_bin or "$HOME/.local/bin/frpc")
    agent_bin = _shell_double_quote(
        definition.agent_bin or f"$HOME/.local/bin/{definition.agent_npm_bin}"
    )
    script = f"""set -euo pipefail
export PATH="$HOME/.local/bin:$PATH"
echo "cluster: {definition.name}"
echo "ssh_host: {definition.ssh_host}"
FRPC_BIN={frpc_bin}
JARVIS_BIN={jarvis_bin}
AGENT_BIN="${{CLIO_RELAY_AGENT_BIN:-}}"
if [ -z "$AGENT_BIN" ]; then
  AGENT_BIN={agent_bin}
fi
echo "frpc=$("$FRPC_BIN" --version)"
echo "frps=$(frps --version)"
echo "jarvis=$("$JARVIS_BIN" --help | head -n 1)"
if [ ! -x "$AGENT_BIN" ]; then
  AGENT_BIN="$(command -v {definition.agent_npm_bin})"
fi
echo "agent=$("$AGENT_BIN" --version)"
echo "clio_relay=$(clio-relay --help | head -n 1)"
"""
    result = subprocess.run(
        ["ssh", definition.ssh_host, "bash", "-s"],
        input=script.encode("utf-8"),
        capture_output=True,
        check=False,
    )
    stdout = result.stdout.decode("utf-8", errors="replace")
    stderr = result.stderr.decode("utf-8", errors="replace")
    if result.returncode != 0:
        detail = stderr.strip() or stdout.strip()
        raise RelayError(f"cluster doctor failed for {definition.name}: {detail}")
    return stdout.splitlines()


def _shell_double_quote(value: str) -> str:
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'

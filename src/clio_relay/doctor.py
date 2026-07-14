"""Environment checks for local and live relay operation."""

from __future__ import annotations

import shlex
import shutil
import subprocess

from clio_relay.cluster_config import ClusterDefinition
from clio_relay.config import RelaySettings
from clio_relay.errors import ConfigurationError, RelayError
from clio_relay.remote_values import render_remote_shell_value


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
    jarvis_bin = render_remote_shell_value(
        definition.jarvis_bin or "$HOME/.local/bin/jarvis",
        field="jarvis_bin",
    )
    frpc_bin = render_remote_shell_value(
        definition.frpc_bin or "$HOME/.local/bin/frpc",
        field="frpc_bin",
    )
    agent_bin = render_remote_shell_value(definition.agent_bin or "", field="agent_bin")
    agent_npm_bin = shlex.quote(definition.agent_npm_bin or "")
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
AGENT_NPM_BIN={agent_npm_bin}
if [ -z "$AGENT_BIN" ] && [ -n "$AGENT_NPM_BIN" ]; then
  AGENT_BIN="$HOME/.local/bin/$AGENT_NPM_BIN"
fi
echo "frpc=$("$FRPC_BIN" --version)"
echo "frps=$(frps --version)"
echo "jarvis=$("$JARVIS_BIN" --help | head -n 1)"
if [ -z "$AGENT_BIN" ]; then
  echo "agent=not_configured"
elif [ ! -x "$AGENT_BIN" ] && [ -n "$AGENT_NPM_BIN" ]; then
  AGENT_BIN="$(command -v "$AGENT_NPM_BIN" || true)"
fi
if [ -n "$AGENT_BIN" ]; then
  echo "agent=$("$AGENT_BIN" --version)"
fi
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

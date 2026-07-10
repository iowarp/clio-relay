"""Sudo-less endpoint deployment helpers."""

from __future__ import annotations

import os
import shlex
import subprocess
from pathlib import Path

from clio_relay.cluster_config import ClusterDefinition
from clio_relay.errors import RelayError
from clio_relay.jarvis_mcp import JARVIS_MCP_COMMAND_ENV


def render_endpoint_user_service(
    *,
    cluster: str,
    definition: ClusterDefinition,
    relay_bin: str = "%h/.local/bin/clio-relay",
    concurrency: int = 1,
) -> str:
    """Render a user-level systemd service for a configured worker endpoint."""
    if concurrency < 1:
        raise RelayError("worker concurrency must be at least 1")
    core_dir = _systemd_home_path(definition.core_dir)
    spool_dir = _systemd_home_path(definition.spool_dir)
    jarvis_bin = _systemd_home_path(definition.jarvis_bin or "$HOME/.local/bin/jarvis")
    frpc_bin = _systemd_home_path(definition.frpc_bin or "$HOME/.local/bin/frpc")
    agent_bin = _systemd_home_path(_configured_agent_bin(definition))
    agent_args = " ".join(definition.agent_args)
    jarvis_mcp_line = _optional_environment_line(
        JARVIS_MCP_COMMAND_ENV,
        os.environ.get(JARVIS_MCP_COMMAND_ENV),
    )
    return f"""[Unit]
Description=clio-relay worker endpoint for {cluster}
After=network-online.target

[Service]
Type=simple
Environment="PATH=%h/.local/bin:/usr/local/bin:/usr/bin:/bin"
Environment="CLIO_RELAY_CORE_DIR={core_dir}"
Environment="CLIO_RELAY_SPOOL_DIR={spool_dir}"
Environment="CLIO_RELAY_JARVIS_BIN={jarvis_bin}"
Environment="CLIO_RELAY_FRPC_BIN={frpc_bin}"
Environment="CLIO_RELAY_AGENT_BIN={agent_bin}"
Environment="CLIO_RELAY_AGENT_ADAPTER={definition.agent_adapter}"
Environment="CLIO_RELAY_AGENT_ARGS={agent_args}"
{jarvis_mcp_line}
ExecStart={relay_bin} endpoint start --role worker --cluster {cluster} --concurrency {concurrency}
Restart=on-failure
RestartSec=5

[Install]
WantedBy=default.target
"""


def install_endpoint_user_service_over_ssh(
    *,
    cluster: str,
    ssh_host: str,
    service_text: str,
    start: bool,
    enable: bool,
) -> list[str]:
    """Install a user-level systemd service on a remote cluster without sudo."""
    service_name = f"clio-relay-worker-{cluster}.service"
    remote_script = _remote_install_script(
        service_name=service_name,
        service_text=service_text,
        start=start,
        enable=enable,
    )
    result = subprocess.run(
        ["ssh", ssh_host, "bash", "-s"],
        input=remote_script.encode("utf-8"),
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        stderr = result.stderr.decode("utf-8", errors="replace")
        stdout = result.stdout.decode("utf-8", errors="replace")
        detail = stderr.strip() or stdout.strip()
        raise RelayError(f"failed to install endpoint user service: {detail}")
    return result.stdout.decode("utf-8", errors="replace").splitlines()


def _remote_install_script(
    *,
    service_name: str,
    service_text: str,
    start: bool,
    enable: bool,
) -> str:
    service_literal = shlex.quote(service_text)
    command = "systemctl --user daemon-reload\n"
    if enable:
        command += f"systemctl --user enable {shlex.quote(service_name)}\n"
    if start:
        command += f"systemctl --user restart {shlex.quote(service_name)}\n"
    command += (
        "echo user_systemd=$(systemctl --user is-system-running || true)\n"
        'echo linger=$(loginctl show-user "$USER" -p Linger --value 2>/dev/null || true)\n'
        "export SYSTEMD_COLORS=0 LANG=C LC_ALL=C\n"
        f"systemctl --user --no-pager --plain --full status "
        f"{shlex.quote(service_name)} || true\n"
    )
    script = f"""set -euo pipefail
mkdir -p "$HOME/.config/systemd/user"
printf '%s' {service_literal} > "$HOME/.config/systemd/user/{service_name}"
{command}"""
    return script.replace("\r\n", "\n")


def write_endpoint_user_service(path: Path, service_text: str) -> Path:
    """Write a user-level systemd service to a local path."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(service_text, encoding="utf-8")
    return path


def _systemd_home_path(value: str) -> str:
    return value.replace("$HOME", "%h")


def _optional_environment_line(name: str, value: str | None) -> str:
    if value is None or value == "":
        return ""
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'Environment="{name}={escaped}"'


def _configured_agent_bin(definition: ClusterDefinition) -> str:
    if definition.agent_bin is not None:
        return definition.agent_bin
    if definition.agent_npm_bin is not None:
        return f"$HOME/.local/bin/{definition.agent_npm_bin}"
    return "agent"

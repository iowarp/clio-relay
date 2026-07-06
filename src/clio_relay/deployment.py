"""Sudo-less endpoint deployment helpers."""

from __future__ import annotations

import shlex
import subprocess
from pathlib import Path

from clio_relay.cluster_config import ClusterDefinition
from clio_relay.errors import RelayError


def render_endpoint_user_service(
    *,
    cluster: str,
    definition: ClusterDefinition,
    relay_bin: str = "%h/.local/bin/clio-relay",
) -> str:
    """Render a user-level systemd service for a configured worker endpoint."""
    core_dir = _systemd_home_path(definition.core_dir)
    spool_dir = _systemd_home_path(definition.spool_dir)
    agent_args = " ".join(definition.agent_args)
    return f"""[Unit]
Description=clio-relay worker endpoint for {cluster}
After=network-online.target

[Service]
Type=simple
Environment="PATH=%h/.local/bin:/usr/local/bin:/usr/bin:/bin"
Environment="CLIO_RELAY_CORE_DIR={core_dir}"
Environment="CLIO_RELAY_SPOOL_DIR={spool_dir}"
Environment="CLIO_RELAY_JARVIS_BIN=%h/.local/bin/jarvis"
Environment="CLIO_RELAY_FRPC_BIN=%h/.local/bin/frpc"
Environment="CLIO_RELAY_AGENT_BIN=%h/.local/bin/{definition.agent_npm_bin}"
Environment="CLIO_RELAY_AGENT_ADAPTER={definition.agent_adapter}"
Environment="CLIO_RELAY_AGENT_ARGS={agent_args}"
ExecStart={relay_bin} endpoint start --role worker --cluster {cluster}
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
        input=remote_script,
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip()
        raise RelayError(f"failed to install endpoint user service: {detail}")
    return result.stdout.splitlines()


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
        f"systemctl --user --no-pager --full status {shlex.quote(service_name)} || true\n"
    )
    return f"""set -euo pipefail
mkdir -p "$HOME/.config/systemd/user"
printf '%s' {service_literal} > "$HOME/.config/systemd/user/{service_name}"
{command}"""


def write_endpoint_user_service(path: Path, service_text: str) -> Path:
    """Write a user-level systemd service to a local path."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(service_text, encoding="utf-8")
    return path


def _systemd_home_path(value: str) -> str:
    return value.replace("$HOME", "%h")

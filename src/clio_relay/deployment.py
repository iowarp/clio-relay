"""Sudo-less endpoint deployment helpers."""

from __future__ import annotations

import os
import re
import shlex
import subprocess
from math import isfinite
from pathlib import Path

from clio_relay.cluster_config import ClusterDefinition
from clio_relay.errors import RelayError
from clio_relay.identifiers import filesystem_key
from clio_relay.installation import INSTALL_RECEIPT_PATH_ENV
from clio_relay.jarvis_mcp import JARVIS_MCP_COMMAND_ENV, JARVIS_MCP_SPACK_COMMAND_ENV
from clio_relay.remote_values import (
    remote_value_expands_home,
    render_systemd_remote_path,
    render_systemd_remote_value,
)
from clio_relay.worker_concurrency import KindConcurrencyInput, kind_concurrency_metadata

_SYSTEMD_UNQUOTED_ARGUMENT = re.compile(r"[A-Za-z0-9_./:@%+=,{}-]+\Z")
_SYSTEMD_SERVICE_NAME = re.compile(r"clio-relay-worker-[a-z0-9_-]+\.service\Z")


def render_endpoint_user_service(
    *,
    cluster: str,
    definition: ClusterDefinition,
    relay_bin: str = "%h/.local/bin/clio-relay",
    concurrency: int = 1,
    kind_concurrency: KindConcurrencyInput | None = None,
) -> str:
    """Render a user-level systemd service for a configured worker endpoint."""
    if concurrency < 1:
        raise RelayError("worker concurrency must be at least 1")
    core_source = definition.core_dir
    spool_source = definition.spool_dir
    jarvis_source = definition.jarvis_bin or "$HOME/.local/bin/jarvis"
    frpc_source = definition.frpc_bin or "$HOME/.local/bin/frpc"
    agent_source = _configured_agent_bin(definition)
    spack_source = definition.spack_executable
    core_dir = render_systemd_remote_path(core_source, field="core_dir")
    spool_dir = render_systemd_remote_path(spool_source, field="spool_dir")
    jarvis_bin = render_systemd_remote_value(
        jarvis_source,
        field="jarvis_bin",
    )
    frpc_bin = render_systemd_remote_value(
        frpc_source,
        field="frpc_bin",
    )
    agent_bin = render_systemd_remote_value(agent_source, field="agent_bin")
    agent_args = " ".join(definition.agent_args)
    kind_limits = kind_concurrency_metadata(kind_concurrency)
    jarvis_mcp_line = _optional_environment_line(
        JARVIS_MCP_COMMAND_ENV,
        os.environ.get(JARVIS_MCP_COMMAND_ENV),
    )
    jarvis_mcp_unset_line = "" if jarvis_mcp_line else f"UnsetEnvironment={JARVIS_MCP_COMMAND_ENV}"
    jarvis_mcp_spack_line = _optional_environment_line(
        JARVIS_MCP_SPACK_COMMAND_ENV,
        (
            render_systemd_remote_value(
                spack_source,
                field="spack_executable",
            )
            if spack_source is not None
            else None
        ),
        allow_home_specifier=(spack_source is not None and remote_value_expands_home(spack_source)),
    )
    jarvis_mcp_spack_unset_line = (
        "" if jarvis_mcp_spack_line else f"UnsetEnvironment={JARVIS_MCP_SPACK_COMMAND_ENV}"
    )
    exec_start_arguments = [
        relay_bin,
        "endpoint",
        "start",
        "--role",
        "worker",
        "--cluster",
        cluster,
        "--concurrency",
        str(concurrency),
    ]
    for kind, limit in kind_limits.items():
        exec_start_arguments.extend(["--kind-concurrency", f"{kind}={limit}"])
    exec_start_arguments.extend(["--scheduler-provider", definition.scheduler_provider])
    exec_start = " ".join(
        _systemd_exec_argument(argument, allow_home_specifier=index == 0)
        for index, argument in enumerate(exec_start_arguments)
    )
    exec_start_pre = " ".join(
        _systemd_exec_argument(argument, allow_home_specifier=index == 0)
        for index, argument in enumerate(
            [relay_bin, "queue", "migrate-indexes", "--all", "--batch-size", "500"]
        )
    )
    description_cluster = _systemd_exec_argument(cluster, allow_home_specifier=False)
    core_line = _environment_line(
        "CLIO_RELAY_CORE_DIR",
        core_dir,
        allow_home_specifier=remote_value_expands_home(core_source),
    )
    spool_line = _environment_line(
        "CLIO_RELAY_SPOOL_DIR",
        spool_dir,
        allow_home_specifier=remote_value_expands_home(spool_source),
    )
    jarvis_line = _environment_line(
        "CLIO_RELAY_JARVIS_BIN",
        jarvis_bin,
        allow_home_specifier=remote_value_expands_home(jarvis_source),
    )
    frpc_line = _environment_line(
        "CLIO_RELAY_FRPC_BIN",
        frpc_bin,
        allow_home_specifier=remote_value_expands_home(frpc_source),
    )
    agent_line = _environment_line(
        "CLIO_RELAY_AGENT_BIN",
        agent_bin,
        allow_home_specifier=remote_value_expands_home(agent_source),
    )
    return f"""[Unit]
Description=clio-relay worker endpoint for {description_cluster}
After=network-online.target

[Service]
Type=simple
Environment="PATH=%h/.local/bin:/usr/local/bin:/usr/bin:/bin"
{core_line}
{spool_line}
{jarvis_line}
{frpc_line}
{agent_line}
{_environment_line("CLIO_RELAY_AGENT_ADAPTER", definition.agent_adapter)}
{_environment_line("CLIO_RELAY_AGENT_ARGS", agent_args)}
Environment="{INSTALL_RECEIPT_PATH_ENV}=%h/.local/share/clio-relay/install-receipt.json"
{jarvis_mcp_line}
{jarvis_mcp_spack_line}
{jarvis_mcp_unset_line}
{jarvis_mcp_spack_unset_line}
ExecStartPre={exec_start_pre}
ExecStart={exec_start}
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
    require_persistent: bool = True,
    timeout_seconds: float = 120.0,
) -> list[str]:
    """Install a user-level systemd service on a remote cluster without sudo.

    Persistent installs require systemd user lingering so the worker remains
    available after the operator's final login session exits. The caller must
    explicitly opt into a login-scoped service when site policy forbids linger.
    """
    _validate_ssh_destination(ssh_host)
    if not isfinite(timeout_seconds) or timeout_seconds <= 0:
        raise RelayError("endpoint service SSH timeout must be finite and positive")
    service_name = endpoint_user_service_name(cluster)
    remote_script = _remote_install_script(
        service_name=service_name,
        service_text=service_text,
        start=start,
        enable=enable,
        require_persistent=require_persistent,
    )
    try:
        result = subprocess.run(
            ["ssh", ssh_host, "bash", "-s"],
            input=remote_script.encode("utf-8"),
            capture_output=True,
            check=False,
            timeout=timeout_seconds,
        )
    except subprocess.TimeoutExpired as exc:
        raise RelayError(
            f"endpoint service installation exceeded {timeout_seconds:g} seconds"
        ) from exc
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
    require_persistent: bool,
) -> str:
    if _SYSTEMD_SERVICE_NAME.fullmatch(service_name) is None:
        raise RelayError(f"unsafe endpoint systemd service name: {service_name!r}")
    service_literal = shlex.quote(service_text)
    persistent_literal = "1" if require_persistent else "0"
    command = "systemctl --user daemon-reload\n"
    if enable:
        command += f"systemctl --user enable {shlex.quote(service_name)}\n"
    if start:
        command += f"systemctl --user restart {shlex.quote(service_name)}\n"
    command += (
        'service_enabled="$(systemctl --user is-enabled '
        f'{shlex.quote(service_name)} 2>/dev/null || true)"\n'
        'service_active="$(systemctl --user is-active '
        f'{shlex.quote(service_name)} 2>/dev/null || true)"\n'
    )
    if enable:
        command += (
            'if [ "$service_enabled" != "enabled" ]; then\n'
            f'  echo "endpoint service is not enabled: {service_name} '
            '$service_enabled" >&2\n'
            "  exit 1\n"
            "fi\n"
        )
    if start:
        command += (
            'if [ "$service_active" != "active" ]; then\n'
            f'  echo "endpoint service is not active: {service_name} '
            '$service_active" >&2\n'
            "  exit 1\n"
            "fi\n"
        )
    command += (
        "echo user_systemd=$(systemctl --user is-system-running || true)\n"
        'echo linger="$linger"\n'
        'echo endpoint_service.persistence="$persistence_mode"\n'
        'echo endpoint_service.enabled="${service_enabled:-unknown}"\n'
        'echo endpoint_service.active="${service_active:-unknown}"\n'
        "export SYSTEMD_COLORS=0 LANG=C LC_ALL=C\n"
        f"systemctl --user --no-pager --plain --full status "
        f"{shlex.quote(service_name)} || true\n"
    )
    script = f"""set -euo pipefail
require_persistent={persistent_literal}
relay_user="${{USER:-$(id -un)}}"
linger="$(loginctl show-user "$relay_user" -p Linger --value 2>/dev/null || true)"
if [ "$linger" = "yes" ]; then
  persistence_mode=systemd-user-linger
elif [ "$require_persistent" = "1" ]; then
  echo "persistent endpoint service requires systemd user lingering (Linger=yes)" >&2
  echo "run 'loginctl enable-linger $relay_user' once, or ask the site administrator" >&2
  echo "to enable lingering for this account" >&2
  echo "use --allow-login-scoped only when logout-time shutdown is explicitly acceptable" >&2
  exit 78
else
  persistence_mode=login-scoped
  echo "warning: endpoint service is login-scoped and may stop after the final login exits" >&2
fi
mkdir -p "$HOME/.config/systemd/user"
printf '%s' {service_literal} > "$HOME/.config/systemd/user/{service_name}"
{command}"""
    return script.replace("\r\n", "\n")


def write_endpoint_user_service(path: Path, service_text: str) -> Path:
    """Write a user-level systemd service to a local path."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(service_text, encoding="utf-8")
    return path


def _optional_environment_line(
    name: str,
    value: str | None,
    *,
    allow_home_specifier: bool = False,
) -> str:
    if value is None or value == "":
        return ""
    return _environment_line(name, value, allow_home_specifier=allow_home_specifier)


def _environment_line(
    name: str,
    value: str,
    *,
    allow_home_specifier: bool = False,
) -> str:
    """Render one systemd environment assignment without directive injection."""
    if not name or any(
        not (character.isupper() or character.isdigit() or character == "_") for character in name
    ):
        raise RelayError(f"unsafe systemd environment name: {name!r}")
    escaped_value = _systemd_escape(value, allow_home_specifier=allow_home_specifier)
    assignment = f"{name}={escaped_value}"
    return f'Environment="{assignment}"'


def _systemd_exec_argument(value: str, *, allow_home_specifier: bool) -> str:
    """Render one exact systemd command argument."""
    escaped = _systemd_escape(value, allow_home_specifier=allow_home_specifier)
    if _SYSTEMD_UNQUOTED_ARGUMENT.fullmatch(escaped) is not None:
        return escaped
    return f'"{escaped}"'


def _systemd_escape(value: str, *, allow_home_specifier: bool) -> str:
    """Escape one value using systemd.syntax quoted-string rules."""
    if "\x00" in value:
        raise RelayError("systemd values cannot contain NUL")
    if allow_home_specifier and value.startswith("%h"):
        escaped_specifiers = "%h" + value.removeprefix("%h").replace("%", "%%")
    else:
        escaped_specifiers = value.replace("%", "%%")
    rendered: list[str] = []
    for character in escaped_specifiers:
        if character == "\\":
            rendered.append("\\\\")
        elif character == '"':
            rendered.append('\\"')
        elif character == "\n":
            rendered.append("\\n")
        elif character == "\r":
            rendered.append("\\r")
        elif character == "\t":
            rendered.append("\\t")
        elif ord(character) < 32 or ord(character) == 127:
            rendered.append(f"\\x{ord(character):02x}")
        else:
            rendered.append(character)
    return "".join(rendered)


def endpoint_user_service_name(cluster: str) -> str:
    """Map one logical cluster label to its portable deterministic worker unit name."""
    key = filesystem_key(cluster, domain="systemd-cluster")
    return f"clio-relay-worker-{key}.service"


def _validate_ssh_destination(value: str) -> None:
    """Reject destinations that SSH could interpret as options or multiple tokens."""
    if (
        not value
        or value != value.strip()
        or value.startswith("-")
        or any(
            character.isspace() or ord(character) < 32 or ord(character) == 127
            for character in value
        )
    ):
        raise RelayError(
            "ssh host must be one non-option destination without whitespace or controls"
        )


def _configured_agent_bin(definition: ClusterDefinition) -> str:
    if definition.agent_bin is not None:
        return definition.agent_bin
    if definition.agent_npm_bin is not None:
        return f"$HOME/.local/bin/{definition.agent_npm_bin}"
    return "agent"

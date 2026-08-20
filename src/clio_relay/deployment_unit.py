"""Render and name the worker endpoint's user-level systemd unit.

Owns the systemd unit-file template (``render_endpoint_user_service``), the
systemd value/argument escaping helpers it and nothing else needs, the
deterministic cluster -> unit-name mapping (``endpoint_user_service_name``),
and writing a rendered unit to a local path. Split from ``deployment.py``
(clio-relay#231): these all serve one concern -- turning a
:class:`~clio_relay.cluster_config.ClusterDefinition` into unit text/a unit
name -- distinct from the activation-observer template and the SSH-borne
install/restart operations that consume this module's output.
"""

from __future__ import annotations

import os
import re
from pathlib import Path

from clio_relay.cluster_config import ClusterDefinition
from clio_relay.deployment_activation import ENDPOINT_SERVICE_SYSTEMD_START_TIMEOUT_SECONDS
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


def render_endpoint_user_service(
    *,
    cluster: str,
    definition: ClusterDefinition,
    relay_bin: str = "%h/.local/bin/clio-relay",
    concurrency: int | None = None,
    control_query_concurrency: int | None = None,
    kind_concurrency: KindConcurrencyInput | None = None,
) -> str:
    """Render a user-level systemd service for a configured worker endpoint."""
    capacity = definition.worker_capacity
    selected_concurrency = capacity.concurrency if concurrency is None else concurrency
    selected_control_concurrency = (
        capacity.control_query_concurrency
        if control_query_concurrency is None
        else control_query_concurrency
    )
    selected_kind_concurrency = (
        capacity.kind_concurrency if kind_concurrency is None else kind_concurrency
    )
    if selected_concurrency < 2:
        raise RelayError("managed worker concurrency must be at least 2")
    if selected_control_concurrency < 1:
        raise RelayError("managed worker control-query concurrency must be at least 1")
    if selected_control_concurrency >= selected_concurrency:
        raise RelayError(
            "managed worker control-query concurrency must be less than total concurrency"
        )
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
    kind_limits = kind_concurrency_metadata(selected_kind_concurrency)
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
        str(selected_concurrency),
        "--control-query-concurrency",
        str(selected_control_concurrency),
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
# Never strand an enabled endpoint after repeated unexpected exits. Each start
# remains bounded by TimeoutStartSec and retries are paced by RestartSec.
StartLimitIntervalSec=0

[Service]
Type=simple
Environment="PATH=%h/.local/share/clio-relay/current/bin:%h/.local/bin:/usr/local/bin:/usr/bin:/bin"
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
# Queue-index migration runs in ExecStartPre. Give systemd a finite bound that
# is longer than normal migration, while keeping the external observer bounded
# slightly beyond this deadline for a definitive terminal state.
TimeoutStartSec={ENDPOINT_SERVICE_SYSTEMD_START_TIMEOUT_SECONDS}s
# Keep an enabled persistent endpoint available after clean or failed process
# exits. Explicit systemd stop operations are not restarted by this policy.
Restart=always
RestartSec=5

[Install]
WantedBy=default.target
"""


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


def _configured_agent_bin(definition: ClusterDefinition) -> str:
    if definition.agent_bin is not None:
        return definition.agent_bin
    if definition.agent_npm_bin is not None:
        return f"$HOME/.local/bin/{definition.agent_npm_bin}"
    return "agent"

"""Rendering for the dumb frps relay host."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from clio_relay.errors import ConfigurationError


class FrpTransportProtocol(StrEnum):
    """Supported frpc-to-frps transport protocols."""

    TCP = "tcp"
    WEBSOCKET = "websocket"
    WSS = "wss"


@dataclass(frozen=True)
class FrpsConfig:
    """Settings required to render frps configuration."""

    bind_port: int = 7000
    token: str = ""
    transport_protocol: FrpTransportProtocol = FrpTransportProtocol.WSS
    dashboard_port: int | None = None


def render_frps_config(config: FrpsConfig) -> str:
    """Render an frps config that contains no application state."""
    lines = [
        'bindAddr = "0.0.0.0"',
        f"bindPort = {config.bind_port}",
        'auth.method = "token"',
        f"auth.token = {_toml_string(config.token)}",
        "transport.tcpMux = true",
    ]
    if config.dashboard_port is not None:
        lines.append(f"webServer.port = {config.dashboard_port}")
    return "\n".join(lines) + "\n"


@dataclass(frozen=True)
class FrpcConfig:
    """Settings required to render frpc configuration."""

    server_addr: str
    server_port: int
    token: str
    transport_protocol: FrpTransportProtocol = FrpTransportProtocol.WSS
    proxy_name: str = "relay-stcp"
    local_ip: str = "127.0.0.1"
    local_port: int = 0
    secret_key: str = ""


def render_frpc_config(config: FrpcConfig) -> str:
    """Render an frpc config for the selected frp transport protocol."""
    if config.local_port <= 0:
        raise ConfigurationError("frpc local_port must be configured")
    lines = [
        f"serverAddr = {_toml_string(config.server_addr)}",
        f"serverPort = {config.server_port}",
        'auth.method = "token"',
        f"auth.token = {_toml_string(config.token)}",
        f"transport.protocol = {_toml_string(config.transport_protocol.value)}",
        "transport.tcpMux = true",
        "",
        "[[proxies]]",
        f"name = {_toml_string(config.proxy_name)}",
        'type = "stcp"',
        f"secretKey = {_toml_string(config.secret_key)}",
        f"localIP = {_toml_string(config.local_ip)}",
        f"localPort = {config.local_port}",
    ]
    return "\n".join(lines) + "\n"


def _toml_string(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'

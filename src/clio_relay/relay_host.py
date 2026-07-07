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
    proxy_type: str = "stcp"
    local_ip: str = "127.0.0.1"
    local_port: int = 0
    secret_key: str = ""


@dataclass(frozen=True)
class FrpcVisitorConfig:
    """Settings required to render a desktop-side STCP visitor."""

    server_addr: str
    server_port: int
    token: str
    transport_protocol: FrpTransportProtocol = FrpTransportProtocol.WSS
    visitor_name: str = "relay-stcp-visitor"
    visitor_type: str = "stcp"
    server_name: str = "relay-stcp"
    bind_addr: str = "127.0.0.1"
    bind_port: int = 0
    secret_key: str = ""
    keep_tunnel_open: bool = False


def render_frpc_config(config: FrpcConfig) -> str:
    """Render an frpc config for the selected frp transport protocol."""
    if config.proxy_type not in {"stcp", "xtcp"}:
        raise ConfigurationError(f"unsupported frpc proxy type: {config.proxy_type}")
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
        f"type = {_toml_string(config.proxy_type)}",
        f"secretKey = {_toml_string(config.secret_key)}",
        f"localIP = {_toml_string(config.local_ip)}",
        f"localPort = {config.local_port}",
    ]
    return "\n".join(lines) + "\n"


def render_frpc_visitor_config(config: FrpcVisitorConfig) -> str:
    """Render an frpc STCP visitor config for desktop access to a relay endpoint."""
    if config.visitor_type not in {"stcp", "xtcp"}:
        raise ConfigurationError(f"unsupported frpc visitor type: {config.visitor_type}")
    if config.bind_port <= 0:
        raise ConfigurationError("frpc visitor bind_port must be configured")
    lines = [
        f"serverAddr = {_toml_string(config.server_addr)}",
        f"serverPort = {config.server_port}",
        'auth.method = "token"',
        f"auth.token = {_toml_string(config.token)}",
        f"transport.protocol = {_toml_string(config.transport_protocol.value)}",
        "transport.tcpMux = true",
        "",
        "[[visitors]]",
        f"name = {_toml_string(config.visitor_name)}",
        f"type = {_toml_string(config.visitor_type)}",
        f"serverName = {_toml_string(config.server_name)}",
        f"secretKey = {_toml_string(config.secret_key)}",
        f"bindAddr = {_toml_string(config.bind_addr)}",
        f"bindPort = {config.bind_port}",
    ]
    if config.visitor_type == "xtcp":
        lines.append(f"keepTunnelOpen = {_toml_bool(config.keep_tunnel_open)}")
    return "\n".join(lines) + "\n"


def _toml_string(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def _toml_bool(value: bool) -> str:
    return "true" if value else "false"

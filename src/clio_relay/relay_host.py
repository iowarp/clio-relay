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
        "[common]",
        f"bind_port = {config.bind_port}",
        f"token = {config.token}",
        "tcp_mux = true",
    ]
    if config.dashboard_port is not None:
        lines.append(f"dashboard_port = {config.dashboard_port}")
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
        "[common]",
        f"server_addr = {config.server_addr}",
        f"server_port = {config.server_port}",
        f"token = {config.token}",
        f"transport.protocol = {config.transport_protocol.value}",
        "tcp_mux = true",
        "",
        f"[{config.proxy_name}]",
        "type = stcp",
        f"secret_key = {config.secret_key}",
        f"local_ip = {config.local_ip}",
        f"local_port = {config.local_port}",
    ]
    return "\n".join(lines) + "\n"

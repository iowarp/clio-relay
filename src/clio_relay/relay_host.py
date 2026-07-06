"""Rendering for the dumb frps relay host."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class FrpsConfig:
    """Settings required to render frps configuration."""

    bind_port: int = 7000
    token: str = ""
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

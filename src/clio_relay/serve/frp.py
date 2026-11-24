"""frp configuration for both ends of a served port (RL20).

Secrets never enter these files. frpc, and relay's own visitor on the desktop,
fill ``{{ .Envs.NAME }}`` from their environment, so the files can sit in a job
directory or relay's state. The site end runs frpc; the desktop end runs
``clio_relay.frp_visitor``, which reads the same TOML.

Over ``wss`` frpc 0.69 does not verify the server unless it is given a CA
file, so relay always gives one: the rendezvous ``ca_file``, or the certifi
bundle when the server has a public certificate.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field

from clio_relay.frp_visitor import START_MARKER
from clio_relay.serve.config import Rendezvous

ENV_TOKEN = "CLIO_RELAY_FRP_TOKEN"
ENV_SECRET_KEY = "CLIO_RELAY_FRP_SK"
ENV_HOP_TOKEN = "CLIO_RELAY_HOP_TOKEN"


def _q(value: str) -> str:
    return json.dumps(value)


def _env(name: str) -> str:
    return "{{ .Envs." + name + " }}"


@dataclass
class Common:
    """What every frpc needs to log in."""

    address: str
    port: int
    protocol: str
    ca_file: str | None = None
    server_name: str | None = None
    proxy_url: str | None = None
    stun_server: str | None = None
    extra: list[str] = field(default_factory=list[str])

    def render(self) -> list[str]:
        lines = [
            f"serverAddr = {_q(self.address)}",
            f"serverPort = {self.port}",
            "loginFailExit = false",
            'auth.method = "token"',
            f"auth.token = {_q(_env(ENV_TOKEN))}",
            f"transport.protocol = {_q(self.protocol)}",
            "transport.heartbeatInterval = 10",
            "transport.heartbeatTimeout = 30",
            'log.to = "console"',
            'log.level = "info"',
        ]
        if self.proxy_url:
            lines.append(f"transport.proxyURL = {_q(self.proxy_url)}")
        if self.stun_server:
            lines.append(f"natHoleStunServer = {_q(self.stun_server)}")
        if self.protocol == "wss":
            if not self.ca_file:
                raise ValueError("a wss rendezvous needs a CA file to verify the server")
            lines += [
                f"transport.tls.trustedCaFile = {_q(self.ca_file)}",
                f"transport.tls.serverName = {_q(self.server_name or self.address)}",
            ]
        lines += self.extra
        return lines


def proxy_name(operation_id: str, port_name: str) -> str:
    """Return the frp proxy name of one served port."""
    return f"relay-{operation_id}-{port_name}"


def render_site(
    common: Common, ports: list[tuple[str, int]], operation_id: str, *, direct: bool
) -> str:
    """The frpc beside the service: one STCP proxy per port, plus XTCP when direct."""
    lines = common.render()
    for name, port in ports:
        base = proxy_name(operation_id, name)
        kinds = [("stcp", base)] + ([("xtcp", f"{base}-p2p")] if direct else [])
        for kind, proxy in kinds:
            lines += [
                "",
                "[[proxies]]",
                f"name = {_q(proxy)}",
                f"type = {_q(kind)}",
                f"secretKey = {_q(_env(ENV_SECRET_KEY))}",
                'localIP = "127.0.0.1"',
                f"localPort = {port}",
            ]
    return "\n".join(lines) + "\n"


def render_visitor(
    common: Common,
    ports: list[tuple[str, int]],
    operation_id: str,
    *,
    direct: bool,
    direct_wait_s: float = 10,
) -> str:
    """The desktop frpc: one visitor per port, bound to loopback only (RL22).

    When direct, the XTCP visitor starts its punch as soon as it starts, so the
    path is usually open before the first connection. A connection that arrives
    while there is no direct path waits ``direct_wait_s``, then goes through
    the rendezvous host.
    """
    lines = common.render()
    for name, local_port in ports:
        base = proxy_name(operation_id, name)
        stcp = f"{base}-visitor"
        if direct:
            lines += [
                "",
                "[[visitors]]",
                f"name = {_q(f'{base}-p2p-visitor')}",
                'type = "xtcp"',
                f"serverName = {_q(f'{base}-p2p')}",
                f"secretKey = {_q(_env(ENV_SECRET_KEY))}",
                'bindAddr = "127.0.0.1"',
                f"bindPort = {local_port}",
                "keepTunnelOpen = true",
                f"fallbackTo = {_q(stcp)}",
                f"fallbackTimeoutMs = {round(direct_wait_s * 1000)}",
            ]
        lines += [
            "",
            "[[visitors]]",
            f"name = {_q(stcp)}",
            'type = "stcp"',
            f"serverName = {_q(base)}",
            f"secretKey = {_q(_env(ENV_SECRET_KEY))}",
            'bindAddr = "127.0.0.1"',
            f"bindPort = {-1 if direct else local_port}",
        ]
    return "\n".join(lines) + "\n"


def desktop_common(rendezvous: Rendezvous, ca_file: str | None) -> Common:
    """The desktop's login settings."""
    return Common(
        address=rendezvous.address,
        port=rendezvous.port,
        protocol=rendezvous.protocol,
        ca_file=ca_file,
        server_name=rendezvous.tls_name(),
        stun_server=rendezvous.stun_server if rendezvous.direct else None,
    )


DIRECT_OPEN = "establishing nat hole connection successful"
DIRECT_FAILED = (
    "nathole precheck error",
    "nathole prepare error",
    "nathole exchange info error",
    "make hole error",
    "init tunnel session error",
    "keepTunnelOpenWorker get tunnel connection error",
)
FELL_BACK = "open tunnel error"
RESTARTED = ("start frpc service", START_MARKER, "relay: forward ended")


def direct_note(log_lines: list[str], wait_s: float) -> str:
    """Say plainly which path the desktop visitor's log shows, so a fallback is never silent."""
    state = "trying"
    reason = ""
    for line in log_lines:
        if any(marker in line for marker in RESTARTED):
            state, reason = "trying", ""
        elif DIRECT_OPEN in line:
            state, reason = "open", ""
        elif FELL_BACK in line:
            state = "fell_back"
        elif any(marker in line for marker in DIRECT_FAILED):
            state = "failed"
            reason = _frp_message(line)
    wait = f"{wait_s:g} s"
    if state == "open":
        return "direct path (XTCP) open"
    if state == "trying":
        return "trying the direct path (XTCP)"
    because = f" ({reason})" if reason else ""
    if state == "failed":
        return (
            f"the direct path (XTCP) failed{because}; a connection waits {wait} for it, "
            "then goes through the rendezvous host (STCP)"
        )
    return (
        f"a connection waited {wait} for the direct path (XTCP){because} and went "
        "through the rendezvous host (STCP)"
    )


def _frp_message(line: str) -> str:
    """Drop frp's time, level and source prefix: ``... [W] [xtcp.go:1] [id] message``."""
    message = line.rsplit("] ", 1)[-1].strip()
    return message[:200]


def default_ca_file() -> str | None:
    """Return the certifi bundle, for a wss server with a public certificate."""
    try:
        import certifi
    except ImportError:  # pragma: no cover - certifi ships with httpx
        return None
    return certifi.where()

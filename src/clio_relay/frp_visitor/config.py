"""Read the frpc TOML subset relay's visitor understands.

frpc fills ``{{ .Envs.NAME }}`` from its environment, so secrets stay out of the
file; this does the same, after parsing, so a secret's characters never change
how the file parses. A key outside the subset is refused by name: a setting
that is silently ignored would make the path behave differently from what the
file says.
"""

from __future__ import annotations

import re
import tomllib
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

from clio_relay.frp_visitor import proxy

PROTOCOLS = ("tcp", "websocket", "wss")
DEFAULT_STUN_SERVER = "stun.easyvoip.com:3478"
"""frp's default ``natHoleStunServer``."""

_COMMON_KEYS = {
    "serverAddr",
    "serverPort",
    "user",
    "loginFailExit",
    "auth.method",
    "auth.token",
    "natHoleStunServer",
    "log.to",
    "log.level",
    "transport.protocol",
    "transport.tcpMux",
    "transport.tcpMuxKeepaliveInterval",
    "transport.heartbeatInterval",
    "transport.heartbeatTimeout",
    "transport.dialServerTimeout",
    "transport.proxyURL",
    "transport.tls.enable",
    "transport.tls.trustedCaFile",
    "transport.tls.serverName",
    "transport.tls.disableCustomTLSFirstByte",
}
_VISITOR_KEYS = {
    "name",
    "type",
    "serverName",
    "serverUser",
    "secretKey",
    "bindAddr",
    "bindPort",
    "transport.useEncryption",
    "transport.useCompression",
}
_XTCP_KEYS = {
    "protocol",
    "keepTunnelOpen",
    "fallbackTo",
    "fallbackTimeoutMs",
    "maxRetriesAnHour",
    "minRetryInterval",
    "natTraversal.disableAssistedAddrs",
}
_NOT_YET = {
    "transport.tls.certFile": "a client TLS certificate",
    "transport.tls.keyFile": "a client TLS certificate",
    "transport.poolCount": "a pool of work connections",
}
_TEMPLATE = re.compile(r"\{\{\s*\.Envs\.([A-Za-z_][A-Za-z0-9_]*)\s*\}\}")


class ConfigError(ValueError):
    """The file asks for something relay's visitor does not do."""


@dataclass(frozen=True)
class Server:
    """How to reach and log in to ``frps``."""

    address: str
    port: int
    token: str
    protocol: str = "tcp"
    user: str = ""
    tcp_mux: bool = True
    mux_keepalive_s: int = 30
    heartbeat_s: int = 30
    heartbeat_timeout_s: int = 90
    dial_timeout_s: int = 10
    tls: bool = True
    ca_file: str | None = None
    tls_name: str | None = None
    tls_first_byte: bool = False
    login_fail_exit: bool = False
    stun_server: str = DEFAULT_STUN_SERVER
    proxy_url: str | None = None
    """``transport.proxyURL``; when unset, the host's proxy variables apply."""

    @property
    def uses_tls(self) -> bool:
        """Return whether the connection to the server is TLS (wss always is)."""
        return self.tls or self.protocol == "wss"

    @property
    def server_name(self) -> str:
        """Return the name TLS and the websocket ``Host`` header use."""
        return self.tls_name or self.address


@dataclass(frozen=True)
class Visitor:
    """One visitor: a loopback port that reaches one of the site's proxies."""

    name: str
    kind: str
    server_name: str
    secret_key: str
    bind_addr: str = "127.0.0.1"
    bind_port: int = -1
    server_user: str = ""
    use_encryption: bool = False
    fallback_to: str | None = None
    protocol: str = "quic"
    keep_tunnel_open: bool = False
    fallback_timeout_ms: int = 1000
    max_retries_an_hour: int = 8
    min_retry_interval_s: int = 90
    disable_assisted_addrs: bool = False

    def proxy_name(self, user: str) -> str:
        """Return the proxy name frps knows, with frp's ``user.`` prefix when there is one."""
        owner = self.server_user or user
        return f"{owner}.{self.server_name}" if owner else self.server_name


@dataclass(frozen=True)
class Config:
    """A whole visitor file."""

    server: Server
    visitors: tuple[Visitor, ...]
    debug: bool = False

    def fallback(self, visitor: Visitor) -> Visitor | None:
        """Return the stcp visitor an xtcp visitor's connections go to when there is no hole."""
        if not visitor.fallback_to:
            return None
        return next(v for v in self.visitors if v.name == visitor.fallback_to)

    def bound(self) -> list[Visitor]:
        """Return the visitors that listen on a port of their own."""
        return [v for v in self.visitors if v.bind_port > 0]


def load(path: Path, env: Mapping[str, str]) -> Config:
    """Read ``path``, filling ``{{ .Envs.NAME }}`` from ``env``."""
    try:
        raw = tomllib.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError) as exc:
        raise ConfigError(f"cannot read {path}: {exc}") from exc
    except tomllib.TOMLDecodeError as exc:
        raise ConfigError(f"{path} is not valid TOML: {exc}") from exc
    return parse(_fill(raw, env))


def parse(raw: dict[str, Any]) -> Config:
    """Build a ``Config`` from parsed TOML whose templates are already filled."""
    raw = dict(raw)
    if raw.pop("proxies", None):
        raise ConfigError("relay's frp visitor only runs visitors, not proxies")
    entries: object = raw.pop("visitors", [])
    if not isinstance(entries, list):
        raise ConfigError("visitors must be a list of [[visitors]] tables")
    common = _flatten(raw)
    _refuse_unknown(common, _COMMON_KEYS, "")
    server = _server(common)
    tables: list[dict[str, Any]] = []
    for entry in cast(list[object], entries):
        if not isinstance(entry, dict):
            raise ConfigError("each [[visitors]] entry must be a table")
        tables.append(cast(dict[str, Any], entry))
    visitors = tuple(_visitor(_flatten(table)) for table in tables)
    _check_visitors(visitors)
    level = str(common.get("log.level", "info")).lower()
    return Config(server=server, visitors=visitors, debug=level in ("debug", "trace"))


def _server(common: dict[str, Any]) -> Server:
    method = common.get("auth.method", "token")
    if method != "token":
        raise ConfigError(f"auth.method {method!r} is not supported; use token")
    protocol = str(common.get("transport.protocol", "tcp"))
    if protocol not in PROTOCOLS:
        raise ConfigError(
            f"transport.protocol {protocol!r} is not supported; relay's frp visitor speaks "
            + ", ".join(PROTOCOLS)
        )
    if common.get("log.to", "console") != "console":
        raise ConfigError("log.to must be console: the visitor logs to its standard output")
    address = common.get("serverAddr")
    if not isinstance(address, str) or not address:
        raise ConfigError("serverAddr is required")
    token = common.get("auth.token", "")
    if not isinstance(token, str):
        raise ConfigError("auth.token must be a string")
    proxy_url = common.get("transport.proxyURL") or None
    if proxy_url is not None:
        if not isinstance(proxy_url, str):
            raise ConfigError("transport.proxyURL must be a string")
        try:
            proxy.parse(proxy_url)
        except ValueError as exc:
            raise ConfigError(f"transport.proxyURL: {exc}") from exc
    return Server(
        address=address,
        port=_int(common, "serverPort", 7000),
        token=token,
        protocol=protocol,
        user=str(common.get("user", "")),
        tcp_mux=_bool(common, "transport.tcpMux", True),
        mux_keepalive_s=_int(common, "transport.tcpMuxKeepaliveInterval", 30),
        heartbeat_s=_int(common, "transport.heartbeatInterval", 30),
        heartbeat_timeout_s=_int(common, "transport.heartbeatTimeout", 90),
        dial_timeout_s=_int(common, "transport.dialServerTimeout", 10),
        tls=_bool(common, "transport.tls.enable", True),
        ca_file=common.get("transport.tls.trustedCaFile") or None,
        tls_name=common.get("transport.tls.serverName") or None,
        tls_first_byte=not _bool(common, "transport.tls.disableCustomTLSFirstByte", True),
        login_fail_exit=_bool(common, "loginFailExit", True),
        stun_server=str(common.get("natHoleStunServer") or DEFAULT_STUN_SERVER),
        proxy_url=proxy_url,
    )


def _visitor(entry: dict[str, Any]) -> Visitor:
    name = entry.get("name")
    if not isinstance(name, str) or not name:
        raise ConfigError("every visitor needs a name")
    kind = entry.get("type")
    if kind not in ("stcp", "xtcp"):
        raise ConfigError(f"visitor {name!r}: type {kind!r} is not supported; use stcp or xtcp")
    allowed = _VISITOR_KEYS | _XTCP_KEYS if kind == "xtcp" else _VISITOR_KEYS
    _refuse_unknown(entry, allowed, f"visitor {name!r}: ")
    protocol = str(entry.get("protocol", "quic"))
    if protocol != "quic":
        raise ConfigError(
            f"visitor {name!r}: protocol {protocol!r} is not supported; relay's frp visitor "
            "carries the direct path over quic"
        )
    if _bool(entry, "transport.useCompression", False):
        raise ConfigError(f"visitor {name!r}: transport.useCompression is not supported")
    server_name = entry.get("serverName")
    if not isinstance(server_name, str) or not server_name:
        raise ConfigError(f"visitor {name!r}: serverName is required")
    secret = entry.get("secretKey", "")
    if not isinstance(secret, str):
        raise ConfigError(f"visitor {name!r}: secretKey must be a string")
    bind_port = _int(entry, "bindPort", -1)
    if bind_port == 0 or bind_port < -1 or bind_port > 65535:
        raise ConfigError(f"visitor {name!r}: bindPort {bind_port} is not a port")
    return Visitor(
        name=name,
        kind=str(kind),
        server_name=server_name,
        secret_key=secret,
        bind_addr=str(entry.get("bindAddr", "127.0.0.1")),
        bind_port=bind_port,
        server_user=str(entry.get("serverUser", "")),
        use_encryption=_bool(entry, "transport.useEncryption", False),
        fallback_to=entry.get("fallbackTo") or None,
        protocol=protocol,
        keep_tunnel_open=_bool(entry, "keepTunnelOpen", False),
        fallback_timeout_ms=_positive(entry, "fallbackTimeoutMs", 1000),
        max_retries_an_hour=_positive(entry, "maxRetriesAnHour", 8),
        min_retry_interval_s=_positive(entry, "minRetryInterval", 90),
        disable_assisted_addrs=_bool(entry, "natTraversal.disableAssistedAddrs", False),
    )


def _check_visitors(visitors: tuple[Visitor, ...]) -> None:
    names = [v.name for v in visitors]
    duplicate = next((n for n in names if names.count(n) > 1), None)
    if duplicate:
        raise ConfigError(f"two visitors are named {duplicate!r}")
    by_name = {v.name: v for v in visitors}
    for visitor in visitors:
        if visitor.kind != "xtcp" or not visitor.fallback_to:
            continue
        target = by_name.get(visitor.fallback_to)
        if target is None or target.kind != "stcp":
            raise ConfigError(
                f"visitor {visitor.name!r}: fallbackTo {visitor.fallback_to!r} must name an "
                "stcp visitor in the same file"
            )
    if not any(v.bind_port > 0 for v in visitors):
        raise ConfigError("no visitor has a bindPort, so there is nothing to listen on")


def _refuse_unknown(entry: dict[str, Any], allowed: set[str], prefix: str) -> None:
    for key in sorted(entry):
        if key in allowed:
            continue
        if key in _NOT_YET:
            raise ConfigError(f"{prefix}{key}: {_NOT_YET[key]} is not supported yet")
        raise ConfigError(f"{prefix}{key} is not a setting relay's frp visitor knows")


def _flatten(table: dict[str, Any], prefix: str = "") -> dict[str, Any]:
    flat: dict[str, Any] = {}
    for key, value in table.items():
        dotted = f"{prefix}{key}"
        if isinstance(value, dict):
            flat.update(_flatten(cast(dict[str, Any], value), f"{dotted}."))
        else:
            flat[dotted] = value
    return flat


def _fill(value: Any, env: Mapping[str, str]) -> Any:
    if isinstance(value, dict):
        table = cast(dict[str, Any], value)
        return {key: _fill(item, env) for key, item in table.items()}
    if isinstance(value, list):
        return [_fill(item, env) for item in cast(list[Any], value)]
    if isinstance(value, str) and "{{" in value:

        def one(match: re.Match[str]) -> str:
            name = match.group(1)
            if name not in env:
                raise ConfigError(f"the file uses {{{{ .Envs.{name} }}}} but it is not set")
            return env[name]

        filled = _TEMPLATE.sub(one, value)
        if "{{" in _TEMPLATE.sub("", value):
            raise ConfigError(f"only {{{{ .Envs.NAME }}}} templates are supported: {value!r}")
        return filled
    return value


def _int(entry: dict[str, Any], key: str, default: int) -> int:
    value = entry.get(key, default)
    if isinstance(value, bool) or not isinstance(value, int):
        raise ConfigError(f"{key} must be a whole number")
    return value


def _bool(entry: dict[str, Any], key: str, default: bool) -> bool:
    value = entry.get(key, default)
    if not isinstance(value, bool):
        raise ConfigError(f"{key} must be true or false")
    return value


def _positive(entry: dict[str, Any], key: str, default: int) -> int:
    """Read a count where 0 means frp's default, as frp's ``EmptyOr`` does."""
    value = _int(entry, key, default)
    if value < 0:
        raise ConfigError(f"{key} must not be negative")
    return value or default

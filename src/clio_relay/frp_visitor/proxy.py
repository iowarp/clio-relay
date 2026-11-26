"""Reach ``frps`` through the proxy the host gives, the way frpc does.

frpc goes through ``transport.proxyURL``, or else ``HTTP_PROXY``. Relay's
visitor does the same, and for ``wss``, which is HTTPS, it reads
``HTTPS_PROXY`` first. ``ALL_PROXY`` is the last resort, and ``NO_PROXY``
names hosts that are dialed directly. A fenced host, such as an agent whose
network leaves only through its own proxy, sets these for the processes it
starts. Without them the visitor's connection never leaves the machine.

An ``http://`` proxy is asked to ``CONNECT``. A ``socks5h://`` proxy is given
the host name, and a ``socks5://`` proxy is given an address resolved here.
"""

from __future__ import annotations

import asyncio
import base64
import ipaddress
import socket
import struct
from collections.abc import Mapping
from dataclasses import dataclass
from urllib.parse import unquote, urlsplit

SCHEMES = ("http", "socks5", "socks5h")
_HEADER_LIMIT = 16 * 1024
_SOCKS_REPLIES = {
    1: "general failure",
    2: "the proxy's rules do not allow it",
    3: "the network is unreachable",
    4: "the host is unreachable",
    5: "the connection was refused",
    6: "the time to live expired",
    7: "the command is not supported",
    8: "the address type is not supported",
}


class ProxyError(ConnectionError):
    """The proxy did not open the tunnel."""


@dataclass(frozen=True)
class Proxy:
    """One proxy, parsed."""

    scheme: str
    host: str
    port: int
    username: str = ""
    password: str = ""

    @property
    def shown(self) -> str:
        """Return the proxy's address without its credentials."""
        host = f"[{self.host}]" if ":" in self.host else self.host
        return f"{self.scheme}://{host}:{self.port}"


def parse(url: str) -> Proxy:
    """Parse a proxy URL, refusing a scheme the visitor does not speak."""
    parts = urlsplit(url if "://" in url else f"http://{url}")
    scheme = parts.scheme.lower()
    if scheme not in SCHEMES:
        raise ValueError(
            f"the proxy scheme {scheme!r} is not supported; use one of " + ", ".join(SCHEMES)
        )
    if not parts.hostname:
        raise ValueError("the proxy URL names no host")
    try:
        port = parts.port
    except ValueError as exc:
        raise ValueError(f"the proxy URL's port is not a port: {exc}") from exc
    return Proxy(
        scheme=scheme,
        host=parts.hostname,
        port=port or (8080 if scheme == "http" else 1080),
        username=unquote(parts.username or ""),
        password=unquote(parts.password or ""),
    )


def from_environment(protocol: str, host: str, env: Mapping[str, str]) -> str | None:
    """Return the proxy URL the host's variables give for ``host``, or None to go direct.

    Lower-case names win over upper-case ones, as curl and Python read them.
    """
    found: dict[str, str] = {}
    for key, value in env.items():
        lowered = key.lower()
        if not lowered.endswith("_proxy") or not value:
            continue
        if key == lowered or lowered not in found:
            found[lowered] = value
    no_proxy = found.get("no_proxy", "")
    if no_proxy and _bypassed(host, no_proxy):
        return None
    order = ("https_proxy", "all_proxy") if protocol == "wss" else ("http_proxy", "all_proxy")
    return next((found[name] for name in order if found.get(name)), None)


def _bypassed(host: str, no_proxy: str) -> bool:
    """Return whether ``NO_PROXY`` names ``host``: ``*``, a domain suffix, an address or a CIDR."""
    name = host.lower().strip("[]").rstrip(".")
    try:
        address = ipaddress.ip_address(name)
    except ValueError:
        address = None
    for raw in no_proxy.split(","):
        entry = raw.strip().lower()
        if not entry:
            continue
        if entry == "*":
            return True
        if address is not None and "/" in entry:
            try:
                if address in ipaddress.ip_network(entry, strict=False):
                    return True
            except ValueError:
                pass
            continue
        if entry.startswith("[") and "]" in entry:
            entry = entry[1 : entry.index("]")]
        elif entry.count(":") == 1:
            entry = entry.split(":", 1)[0]
        entry = entry.lstrip("*").lstrip(".").rstrip(".")
        if entry and (name == entry or name.endswith("." + entry)):
            return True
    return False


async def open_tunnel(
    proxy: Proxy, host: str, port: int
) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    """Open a TCP connection to ``host:port`` through ``proxy``."""
    try:
        reader, writer = await asyncio.open_connection(proxy.host, proxy.port)
    except OSError as exc:
        raise ProxyError(f"cannot reach the proxy at {proxy.shown}: {exc}") from exc
    try:
        if proxy.scheme == "http":
            await _connect(reader, writer, proxy, host, port)
        else:
            await _socks5(reader, writer, proxy, host, port)
    except BaseException:
        writer.close()
        raise
    return reader, writer


async def _connect(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    proxy: Proxy,
    host: str,
    port: int,
) -> None:
    authority = f"[{host}]:{port}" if ":" in host else f"{host}:{port}"
    lines = [f"CONNECT {authority} HTTP/1.1", f"Host: {authority}"]
    if proxy.username or proxy.password:
        token = base64.b64encode(f"{proxy.username}:{proxy.password}".encode()).decode()
        lines.append(f"Proxy-Authorization: Basic {token}")
    writer.write(("\r\n".join(lines) + "\r\n\r\n").encode())
    await writer.drain()
    try:
        head = await reader.readuntil(b"\r\n\r\n")
    except asyncio.LimitOverrunError as exc:
        raise ProxyError(f"the proxy at {proxy.shown} sent an answer too long to read") from exc
    except asyncio.IncompleteReadError as exc:
        raise ProxyError(
            f"the proxy at {proxy.shown} closed the connection before it answered CONNECT"
        ) from exc
    if len(head) > _HEADER_LIMIT:
        raise ProxyError(f"the proxy at {proxy.shown} sent an answer too long to read")
    status = head.split(b"\r\n", 1)[0].decode("latin-1").strip()
    fields = status.split(" ", 2)
    if len(fields) < 2 or not fields[0].startswith("HTTP/") or not fields[1].isdigit():
        raise ProxyError(f"the proxy at {proxy.shown} did not answer CONNECT: {status!r}")
    if not 200 <= int(fields[1]) < 300:
        raise ProxyError(f"the proxy at {proxy.shown} refused CONNECT {authority}: {status}")


async def _socks5(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    proxy: Proxy,
    host: str,
    port: int,
) -> None:
    methods = b"\x00\x02" if proxy.username or proxy.password else b"\x00"
    writer.write(b"\x05" + bytes([len(methods)]) + methods)
    await writer.drain()
    version, method = await _read(reader, 2, proxy)
    if version != 5:
        raise ProxyError(f"the proxy at {proxy.shown} does not speak SOCKS5")
    if method == 2:
        user, password = proxy.username.encode(), proxy.password.encode()
        if len(user) > 255 or len(password) > 255:
            raise ProxyError("the SOCKS5 user name and password must each be under 256 bytes")
        writer.write(b"\x01" + bytes([len(user)]) + user + bytes([len(password)]) + password)
        await writer.drain()
        _, status = await _read(reader, 2, proxy)
        if status != 0:
            raise ProxyError(f"the proxy at {proxy.shown} refused the user name and password")
    elif method != 0:
        raise ProxyError(f"the proxy at {proxy.shown} accepts no way to log in that relay offers")
    writer.write(b"\x05\x01\x00" + await _address(proxy, host, port) + struct.pack(">H", port))
    await writer.drain()
    _, reply, _, kind = await _read(reader, 4, proxy)
    if reply != 0:
        reason = _SOCKS_REPLIES.get(reply, f"reply {reply}")
        raise ProxyError(f"the proxy at {proxy.shown} did not connect to {host}:{port}: {reason}")
    if kind == 1:
        await _read(reader, 4 + 2, proxy)
    elif kind == 4:
        await _read(reader, 16 + 2, proxy)
    elif kind == 3:
        (length,) = await _read(reader, 1, proxy)
        await _read(reader, length + 2, proxy)
    else:
        raise ProxyError(f"the proxy at {proxy.shown} sent an address relay cannot read")


async def _address(proxy: Proxy, host: str, port: int) -> bytes:
    try:
        literal = ipaddress.ip_address(host)
    except ValueError:
        literal = None
    if literal is None and proxy.scheme == "socks5h":
        name = host.encode("idna")
        if len(name) > 255:
            raise ProxyError(f"the host name {host!r} is too long for SOCKS5")
        return b"\x03" + bytes([len(name)]) + name
    if literal is None:
        loop = asyncio.get_running_loop()
        try:
            infos = await loop.getaddrinfo(host, port, type=socket.SOCK_STREAM)
        except OSError as exc:
            raise ProxyError(f"cannot resolve {host}: {exc}") from exc
        literal = ipaddress.ip_address(infos[0][4][0])
    if literal.version == 4:
        return b"\x01" + literal.packed
    return b"\x04" + literal.packed


async def _read(reader: asyncio.StreamReader, n: int, proxy: Proxy) -> bytes:
    try:
        return await reader.readexactly(n)
    except asyncio.IncompleteReadError as exc:
        raise ProxyError(f"the proxy at {proxy.shown} closed the connection") from exc

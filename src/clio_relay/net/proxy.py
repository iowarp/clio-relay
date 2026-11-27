"""Reach a host through the proxy the environment gives, as any network client does.

Relay's frp visitor and its held ssh connections both dial through here. A
fenced host, such as an agent whose network leaves only through its own proxy,
sets the proxy variables for the processes it starts; without them relay's own
connections never leave the machine.

Which variable counts depends on what is dialed (:data:`ORDERS`): frpc reads
``HTTP_PROXY`` for plain frp and ``HTTPS_PROXY`` first for ``wss``, and ssh
reads ``HTTPS_PROXY``, then ``ALL_PROXY``, then ``HTTP_PROXY``. ``NO_PROXY``
names hosts that are dialed directly.

An ``http://`` proxy is asked to ``CONNECT``. A ``socks5h://`` proxy is given
the host name, and a ``socks5://`` proxy is given an address resolved here.
The answer is read to its last byte and no further, so whatever the far end
sends next stays on the socket for its own protocol.
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
ORDERS: dict[str, tuple[str, ...]] = {
    "wss": ("https_proxy", "all_proxy"),
    "ssh": ("https_proxy", "all_proxy", "http_proxy"),
}
"""The proxy variables each protocol reads, first one set wins; others read ``http_proxy``."""
_DEFAULT_ORDER = ("http_proxy", "all_proxy")
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
    """Parse a proxy URL, refusing a scheme relay does not speak."""
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
        if not lowered.endswith("_proxy") or not value.strip():
            continue
        if key == lowered or lowered not in found:
            found[lowered] = value.strip()
    no_proxy = found.get("no_proxy", "")
    if no_proxy and _bypassed(host, no_proxy):
        return None
    order = ORDERS.get(protocol, _DEFAULT_ORDER)
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


class _Wire:
    """A proxy socket read exactly: never past the end of the proxy's answer."""

    def __init__(self, sock: socket.socket, proxy: Proxy) -> None:
        self.sock, self.proxy = sock, proxy
        self.loop = asyncio.get_running_loop()

    async def send(self, data: bytes) -> None:
        await self.loop.sock_sendall(self.sock, data)

    async def exactly(self, size: int, when: str = "") -> bytes:
        data = b""
        while len(data) < size:
            chunk = await self.loop.sock_recv(self.sock, size - len(data))
            if not chunk:
                raise ProxyError(f"the proxy at {self.proxy.shown} closed the connection{when}")
            data += chunk
        return data


async def _reach(proxy: Proxy) -> socket.socket:
    loop = asyncio.get_running_loop()
    try:
        infos = await loop.getaddrinfo(proxy.host, proxy.port, type=socket.SOCK_STREAM)
    except OSError as exc:
        raise ProxyError(f"cannot reach the proxy at {proxy.shown}: {exc}") from exc
    failure: OSError | None = None
    for family, kind, number, _, address in infos:
        sock = socket.socket(family, kind, number)
        sock.setblocking(False)
        try:
            await loop.sock_connect(sock, address)
        except OSError as exc:
            sock.close()
            failure = exc
            continue
        return sock
    raise ProxyError(f"cannot reach the proxy at {proxy.shown}: {failure}")


async def open_socket(proxy: Proxy, host: str, port: int) -> socket.socket:
    """Return a socket joined to ``host:port`` through ``proxy``, for a client that takes one."""
    sock = await _reach(proxy)
    try:
        wire = _Wire(sock, proxy)
        if proxy.scheme == "http":
            await _connect(wire, host, port)
        else:
            await _socks5(wire, host, port)
    except BaseException:
        sock.close()
        raise
    return sock


async def open_tunnel(
    proxy: Proxy, host: str, port: int
) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    """Open a TCP connection to ``host:port`` through ``proxy``, as streams."""
    sock = await open_socket(proxy, host, port)
    try:
        return await asyncio.open_connection(sock=sock)
    except BaseException:
        sock.close()
        raise


async def _connect(wire: _Wire, host: str, port: int) -> None:
    proxy = wire.proxy
    authority = f"[{host}]:{port}" if ":" in host else f"{host}:{port}"
    lines = [f"CONNECT {authority} HTTP/1.1", f"Host: {authority}"]
    if proxy.username or proxy.password:
        token = base64.b64encode(f"{proxy.username}:{proxy.password}".encode()).decode()
        lines.append(f"Proxy-Authorization: Basic {token}")
    await wire.send(("\r\n".join(lines) + "\r\n\r\n").encode())
    head = b""
    while not head.endswith(b"\r\n\r\n"):
        if len(head) >= _HEADER_LIMIT:
            raise ProxyError(f"the proxy at {proxy.shown} sent an answer too long to read")
        head += await wire.exactly(1, " before it answered CONNECT")
    status = head.split(b"\r\n", 1)[0].decode("latin-1").strip()
    fields = status.split(" ", 2)
    if len(fields) < 2 or not fields[0].startswith("HTTP/") or not fields[1].isdigit():
        raise ProxyError(f"the proxy at {proxy.shown} did not answer CONNECT: {status!r}")
    if not 200 <= int(fields[1]) < 300:
        raise ProxyError(f"the proxy at {proxy.shown} refused CONNECT {authority}: {status}")


async def _socks5(wire: _Wire, host: str, port: int) -> None:
    proxy = wire.proxy
    methods = b"\x00\x02" if proxy.username or proxy.password else b"\x00"
    await wire.send(b"\x05" + bytes([len(methods)]) + methods)
    version, method = await wire.exactly(2)
    if version != 5:
        raise ProxyError(f"the proxy at {proxy.shown} does not speak SOCKS5")
    if method == 2:
        user, password = proxy.username.encode(), proxy.password.encode()
        if len(user) > 255 or len(password) > 255:
            raise ProxyError("the SOCKS5 user name and password must each be under 256 bytes")
        await wire.send(b"\x01" + bytes([len(user)]) + user + bytes([len(password)]) + password)
        _, status = await wire.exactly(2)
        if status != 0:
            raise ProxyError(f"the proxy at {proxy.shown} refused the user name and password")
    elif method != 0:
        raise ProxyError(f"the proxy at {proxy.shown} accepts no way to log in that relay offers")
    await wire.send(b"\x05\x01\x00" + await _address(proxy, host, port) + struct.pack(">H", port))
    _, reply, _, kind = await wire.exactly(4)
    if reply != 0:
        reason = _SOCKS_REPLIES.get(reply, f"reply {reply}")
        raise ProxyError(f"the proxy at {proxy.shown} did not connect to {host}:{port}: {reason}")
    if kind == 1:
        await wire.exactly(4 + 2)
    elif kind == 4:
        await wire.exactly(16 + 2)
    elif kind == 3:
        (length,) = await wire.exactly(1)
        await wire.exactly(length + 2)
    else:
        raise ProxyError(f"the proxy at {proxy.shown} sent an address relay cannot read")


async def _address(proxy: Proxy, host: str, port: int) -> bytes:
    try:
        literal = ipaddress.ip_address(host.strip("[]"))
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

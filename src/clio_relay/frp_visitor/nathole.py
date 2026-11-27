"""Punch a UDP hole to the peer frpc, as frp 0.69.1's ``pkg/nathole`` does from the visitor side.

The steps, each with frp's own error text so relay reads it as it reads frpc:

1. ``precheck``: ask frps whether the xtcp proxy is there at all.
2. ``prepare``: learn this side's public addresses from a STUN server (two
   answers, from the server's two addresses) on the socket the hole will use,
   and classify the NAT from them.
3. ``exchange``: send those addresses to frps, which pairs them with the
   peer's and answers with the peer's candidates and a detect behavior.
4. ``make_hole``: send the session id, sealed with the secret key, to the
   peer's candidates until one answers on one of this side's sockets.

The socket that wins is handed to the QUIC tunnel. Every other socket is closed
on every path out of here. Nothing here logs; the visitor logs each step's
outcome in frpc's words.
"""

from __future__ import annotations

import asyncio
import contextlib
import ipaddress
import os
import random
import secrets
import socket
import struct
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any, cast

from clio_relay.frp_visitor.pipe import ProtocolError
from clio_relay.frp_visitor.wire import NAT_HOLE_SID, auth_key, pack, parse, seal, unseal

Address = tuple[str, int]
Transact = Callable[[dict[str, Any]], Awaitable[dict[str, Any]]]
"""Send one NatHoleVisitor message on the control connection and return frps' NatHoleResp."""

ERROR_PORT_UNREACHABLE = 1234
"""Windows' ``winerror`` for a datagram that reached a closed port."""

STUN_TIMEOUT_S = 3.0
DETECT_TIMEOUT_S = 5.0
MAX_LOCAL_IPS = 10
ROLE_SENDER = "sender"

_STUN_MAGIC = 0x2112A442
_STUN_BINDING = 0x0001
_STUN_HEADER = struct.Struct("!HHI12s")
_ATTR_MAPPED = 0x0001
_ATTR_CHANGED = 0x0005
_ATTR_XOR_MAPPED = 0x0020
_ATTR_OTHER = 0x802C


class HoleError(Exception):
    """One step of the punch failed; the text is frp's own."""


@dataclass
class Prepared:
    """What ``prepare`` learned, with the socket the hole is punched from."""

    sock: socket.socket
    addrs: list[str]
    assisted: list[str]
    nat_type: str
    behavior: str


@dataclass(frozen=True)
class Hole:
    """frps' answer to the exchange: who the peer is and how to reach it."""

    sid: str
    protocol: str
    candidates: list[str]
    assisted: list[str]
    behavior: dict[str, Any] = field(default_factory=dict[str, Any])


def transaction_id() -> str:
    """Return a new id in frp's shape: the Unix time, then 16 random hex digits."""
    return f"{int(time.time())}{secrets.token_hex(8)}"


def udp_socket(host: str = "0.0.0.0") -> socket.socket:
    """Return a non-blocking IPv4 UDP socket on a random port of ``host``."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.setblocking(False)
        sock.bind((host, 0))
    except BaseException:
        sock.close()
        raise
    return sock


def split(address: str) -> Address:
    """Split ``host:port``."""
    host, _, port = address.rpartition(":")
    if not host or not port.isdigit():
        raise ValueError(f"{address!r} is not host:port")
    return host.strip("[]"), int(port)


async def resolve(address: str) -> Address:
    """Resolve ``host:port`` to an IPv4 address, as Go's ``ResolveUDPAddr("udp4", ...)`` does."""
    host, port = split(address)
    loop = asyncio.get_running_loop()
    infos = await loop.getaddrinfo(host, port, family=socket.AF_INET, type=socket.SOCK_DGRAM)
    if not infos:
        raise OSError(f"no IPv4 address for {host}")
    sockaddr = cast(tuple[Any, ...], infos[0][4])
    return str(sockaddr[0]), int(sockaddr[1])


async def receive(sock: socket.socket, size: int = 65536) -> tuple[bytes, Address]:
    """Read one datagram, skipping the "unreachable" errors an earlier send left behind.

    Windows reports an ICMP "port unreachable" for an earlier datagram on the
    socket's next read. A punch sends to many closed ports, so those reads are
    expected and say nothing about the datagram that comes next.
    """
    loop = asyncio.get_running_loop()
    while True:
        try:
            data, address = await loop.sock_recvfrom(sock, size)
        except (ConnectionResetError, ConnectionRefusedError):
            continue
        except OSError as exc:
            if getattr(exc, "winerror", None) == ERROR_PORT_UNREACHABLE:
                continue
            raise
        peer = cast(tuple[Any, ...], address)
        return data, (str(peer[0]), int(peer[1]))


def stun_request() -> tuple[bytes, bytes]:
    """Return a STUN Binding request and its transaction id."""
    tid = os.urandom(12)
    return _STUN_HEADER.pack(_STUN_BINDING, 0, _STUN_MAGIC, tid), tid


def stun_parse(data: bytes, tid: bytes) -> tuple[str, str] | None:
    """Return the mapped and the other address a STUN answer names, or None if it is not ours.

    XOR-MAPPED-ADDRESS wins over MAPPED-ADDRESS, and OTHER-ADDRESS over
    CHANGED-ADDRESS, as frp reads them. Either may be "" when absent.
    """
    if len(data) < _STUN_HEADER.size:
        return None
    _kind, length, magic, got = _STUN_HEADER.unpack_from(data)
    if magic != _STUN_MAGIC or got != tid:
        return None
    found: dict[int, str] = {}
    at = _STUN_HEADER.size
    end = min(len(data), _STUN_HEADER.size + length)
    while at + 4 <= end:
        attr, size = struct.unpack_from("!HH", data, at)
        value = data[at + 4 : at + 4 + size]
        at += 4 + size + (-size % 4)
        if attr in (_ATTR_MAPPED, _ATTR_CHANGED, _ATTR_OTHER, _ATTR_XOR_MAPPED):
            address = _stun_address(value, xor=attr == _ATTR_XOR_MAPPED)
            if address:
                found.setdefault(attr, address)
    mapped = found.get(_ATTR_XOR_MAPPED) or found.get(_ATTR_MAPPED, "")
    other = found.get(_ATTR_OTHER) or found.get(_ATTR_CHANGED, "")
    return mapped, other


def _stun_address(value: bytes, *, xor: bool) -> str:
    if len(value) < 8 or value[1] != 0x01:
        return ""
    (port,) = struct.unpack_from("!H", value, 2)
    raw = value[4:8]
    if xor:
        port ^= _STUN_MAGIC >> 16
        raw = bytes(a ^ b for a, b in zip(raw, struct.pack("!I", _STUN_MAGIC), strict=True))
    return f"{ipaddress.IPv4Address(raw)}:{port}"


async def _stun(sock: socket.socket, server: str) -> tuple[str, str]:
    address = await resolve(server)
    request, tid = stun_request()
    sock.sendto(request, address)
    loop = asyncio.get_running_loop()
    deadline = loop.time() + STUN_TIMEOUT_S
    while (remaining := deadline - loop.time()) > 0:
        try:
            data, _ = await asyncio.wait_for(receive(sock), remaining)
        except TimeoutError:
            break
        answer = stun_parse(data, tid)
        if answer is not None:
            return answer
    raise HoleError("wait response from stun server timeout")


async def discover(sock: socket.socket, server: str) -> list[str]:
    """Return this socket's public addresses, as seen from both of the STUN server's addresses."""
    mapped, other = await _stun(sock, server)
    if not mapped:
        raise HoleError("no external address found")
    addrs = [mapped]
    if other:
        again, _ = await _stun(sock, other)
        if again:
            addrs.append(again)
    return addrs


def classify(addrs: list[str]) -> tuple[str, str]:
    """Return frp's NAT type and behavior for the public addresses one socket was seen at."""
    pairs = [split(a) for a in addrs]
    ip_changed = any(ip != pairs[0][0] for ip, _ in pairs)
    port_changed = any(port != pairs[0][1] for _, port in pairs)
    if ip_changed and port_changed:
        return "HardNAT", "BehaviorBothChanged"
    if ip_changed:
        return "HardNAT", "BehaviorIPChanged"
    if port_changed:
        return "HardNAT", "BehaviorPortChanged"
    return "EasyNAT", "BehaviorNoChange"


async def route_source(toward: str) -> str | None:
    """Return the source address the routing table picks toward ``toward``, if any."""
    with contextlib.suppress(OSError, ValueError):
        probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            probe.connect(await resolve(toward))
            return str(probe.getsockname()[0])
        finally:
            probe.close()
    return None


async def local_ips(toward: list[str]) -> list[str]:
    """Return this machine's usable IPv4 addresses, at most ``MAX_LOCAL_IPS``.

    frp lists every interface. The standard library cannot, so this takes the
    source address the routing table picks toward each of ``toward`` (the
    rendezvous and STUN hosts), plus what the host name resolves to.
    """
    loop = asyncio.get_running_loop()
    found = [ip for ip in [await route_source(host) for host in toward] if ip is not None]
    with contextlib.suppress(OSError):
        infos = await loop.getaddrinfo(socket.gethostname(), None, family=socket.AF_INET)
        found.extend(str(cast(tuple[Any, ...], info[4])[0]) for info in infos)
    usable = list(dict.fromkeys(ip for ip in found if _usable(ip)))
    return usable[:MAX_LOCAL_IPS]


def _usable(text: str) -> bool:
    ip = ipaddress.IPv4Address(text)
    return not (ip.is_loopback or ip.is_link_local or ip.is_multicast or ip.is_unspecified)


async def precheck(transact: Transact, proxy_name: str) -> None:
    """Ask frps whether the peer's xtcp proxy exists, before any STUN traffic."""
    answer = await transact(
        {"transaction_id": transaction_id(), "proxy_name": proxy_name, "pre_check": True}
    )
    if answer.get("error"):
        raise HoleError(str(answer["error"]))


async def prepare(stun_server: str, rendezvous: str, *, disable_assisted: bool) -> Prepared:
    """Open the punching socket and learn its public addresses.

    The socket is bound to the address toward the STUN server, so a split-tunnel
    VPN can't carry the punch from an address STUN never saw (ledger, a44).
    """
    bound = await route_source(stun_server)
    sock = udp_socket(bound or "0.0.0.0")
    try:
        try:
            addrs = await discover(sock, stun_server)
        except (HoleError, OSError, ValueError) as exc:
            raise HoleError(f"discover error: {exc}") from exc
        if len(addrs) < 2:
            raise HoleError("discover error: not enough addresses")
        nat_type, behavior = classify(addrs)
        # A bound socket hears only its own address.
        ips = [bound] if bound else await local_ips([rendezvous, stun_server])
        port = int(sock.getsockname()[1])
        assisted = [] if disable_assisted else [f"{ip}:{port}" for ip in ips if _usable(ip)]
    except BaseException:
        sock.close()
        raise
    return Prepared(sock, addrs, assisted, nat_type, behavior)


async def exchange(
    transact: Transact, prepared: Prepared, proxy_name: str, secret: str, protocol: str
) -> Hole:
    """Send this side's addresses to frps and return its pairing with the peer's."""
    now = int(time.time())
    answer = await transact(
        {
            "transaction_id": transaction_id(),
            "proxy_name": proxy_name,
            "protocol": protocol,
            "sign_key": auth_key(secret, now),
            "timestamp": now,
            "mapped_addrs": prepared.addrs,
            "assisted_addrs": prepared.assisted,
        }
    )
    if answer.get("error"):
        raise HoleError(f"natHoleRespMsg get error info: {answer['error']}")
    candidates = [str(a) for a in cast(list[Any], answer.get("candidate_addrs") or [])]
    if not candidates:
        raise HoleError("natHoleRespMsg get empty candidate addresses")
    behavior: object = answer.get("detect_behavior") or {}
    return Hole(
        sid=str(answer.get("sid", "")),
        protocol=str(answer.get("protocol", "")),
        candidates=candidates,
        assisted=[str(a) for a in cast(list[Any], answer.get("assisted_addrs") or [])],
        behavior=cast(dict[str, Any], behavior) if isinstance(behavior, dict) else {},
    )


def _compact(items: list[str]) -> list[str]:
    """Drop consecutive repeats, as Go's ``slices.Compact`` does."""
    return [item for i, item in enumerate(items) if i == 0 or items[i - 1] != item]


def _ips(addrs: list[str]) -> list[str]:
    ips: list[str] = []
    for address in addrs:
        with contextlib.suppress(ValueError):
            ips.append(split(address)[0])
    return _compact(ips)


def _sid_message(hole: Hole, tid: str, secret: str, *, response: bool = False) -> bytes:
    body: dict[str, Any] = {
        "transaction_id": tid,
        "sid": hole.sid,
        "response": response,
        "nonce": "0" * secrets.randbelow(20),
    }
    return seal(pack(NAT_HOLE_SID, body), secret)


def _send(sock: socket.socket, data: bytes, address: Address, ttl: int) -> None:
    original: int | None = None
    if ttl > 0:
        with contextlib.suppress(OSError):
            original = sock.getsockopt(socket.IPPROTO_IP, socket.IP_TTL)
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_TTL, ttl)
    try:
        with contextlib.suppress(OSError):
            sock.sendto(data, address)
    finally:
        if original is not None:
            with contextlib.suppress(OSError):
                sock.setsockopt(socket.IPPROTO_IP, socket.IP_TTL, original)


async def _send_to(
    sock: socket.socket, hole: Hole, tid: str, secret: str, address: str, ttl: int
) -> None:
    with contextlib.suppress(OSError, ValueError):
        target = await resolve(address)
        _send(sock, _sid_message(hole, tid, secret), target, ttl)


async def _random_ports(
    sock: socket.socket, hole: Hole, tid: str, secret: str, count: int, ttl: int
) -> None:
    used: set[int] = set()
    ips = _ips(hole.candidates)
    for _ in range(count):
        port = next(
            (p for p in (random.randrange(1024, 65535) for _ in range(10)) if p not in used), 0
        )
        if not port:
            continue
        used.add(port)
        for ip in ips:
            await _send_to(sock, hole, tid, secret, f"{ip}:{port}", ttl)
            await asyncio.sleep(0.015)


async def _wait_sid(
    sock: socket.socket, hole: Hole, secret: str, timeout: float, sender: bool
) -> Address:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while (remaining := deadline - loop.time()) > 0:
        try:
            data, address = await asyncio.wait_for(receive(sock), remaining)
        except TimeoutError:
            break
        try:
            kind, body = parse(unseal(data, secret))
        except ProtocolError:
            continue
        if kind != NAT_HOLE_SID or body.get("sid") != hole.sid:
            continue
        if not body.get("response"):
            if sender:
                continue
            tid = str(body.get("transaction_id", ""))
            _send(sock, _sid_message(hole, tid, secret, response=True), address, 0)
        return address
    raise TimeoutError("i/o timeout")


async def make_hole(sock: socket.socket, hole: Hole, secret: str) -> tuple[socket.socket, Address]:
    """Send the session id to the peer until it answers, and return the socket it answered on.

    ``sock`` is owned from here on: it is returned, or closed when the punch fails.
    """
    behavior = hole.behavior
    sender = behavior.get("role") == ROLE_SENDER
    ttl = int(behavior.get("ttl") or 0)
    ranges = cast(list[dict[str, Any]], behavior.get("candidate_ports") or [])
    tid = transaction_id()
    socks = [sock]
    background: list[asyncio.Task[None]] = []
    try:
        if sender:
            delay = int(behavior.get("send_delay_ms") or 0)
            if delay > 0:
                await asyncio.sleep(delay / 1000)
            detect = hole.assisted + hole.candidates
        else:
            detect = [] if ranges else list(hole.candidates)
            for _ in range(int(behavior.get("listen_random_ports") or 0)):
                with contextlib.suppress(OSError):
                    socks.append(udp_socket(str(sock.getsockname()[0])))
        for address in _compact(detect):
            for each in socks:
                await _send_to(each, hole, tid, secret, address, ttl)
        if ranges:
            for each in socks:
                for ip in _ips(hole.candidates):
                    for span in ranges:
                        for port in range(int(span.get("from", 0)), int(span.get("to", 0)) + 1):
                            await _send_to(each, hole, tid, secret, f"{ip}:{port}", ttl)
                            await asyncio.sleep(0.002)
        count = int(behavior.get("send_random_ports") or 0)
        if count > 0:
            background = [
                asyncio.create_task(_random_ports(each, hole, tid, secret, count, ttl))
                for each in socks
            ]
        read_ms = int(behavior.get("read_timeout") or 0)
        timeout = read_ms / 1000 if read_ms > 0 else DETECT_TIMEOUT_S
        winner = await _first_answer(socks, hole, secret, timeout, sender)
    except BaseException:
        for each in socks:
            each.close()
        raise
    finally:
        for task in background:
            task.cancel()
        await asyncio.gather(*background, return_exceptions=True)
    chosen, address = winner
    for each in socks:
        if each is not chosen:
            each.close()
    return chosen, address


async def _first_answer(
    socks: list[socket.socket], hole: Hole, secret: str, timeout: float, sender: bool
) -> tuple[socket.socket, Address]:
    if len(socks) == 1:
        try:
            return socks[0], await _wait_sid(socks[0], hole, secret, timeout, sender)
        except (TimeoutError, OSError) as exc:
            raise HoleError(f"wait detect message error: {exc}") from exc
    waits = {
        asyncio.create_task(_wait_sid(each, hole, secret, timeout, sender)): each for each in socks
    }
    try:
        pending = set(waits)
        while pending:
            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                if not task.cancelled() and task.exception() is None:
                    return waits[task], task.result()
    finally:
        for task in waits:
            task.cancel()
        await asyncio.gather(*waits, return_exceptions=True)
    raise HoleError("wait detect message timeout")

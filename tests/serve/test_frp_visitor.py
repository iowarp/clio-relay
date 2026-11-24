"""Relay's own frp visitor: the files it accepts and the wire it speaks (RL20)."""

from __future__ import annotations

import asyncio
import hashlib
import tomllib
from pathlib import Path

import pytest

from clio_relay.frp_visitor import NO_DIRECT_MARKER, START_MARKER
from clio_relay.frp_visitor.config import ConfigError, load, parse
from clio_relay.frp_visitor.pipe import BufferedPipe, Pipe
from clio_relay.frp_visitor.wire import (
    LOGIN,
    CryptoPipe,
    auth_key,
    pack,
    read_message,
)
from clio_relay.serve import frp
from clio_relay.serve.config import Rendezvous, SiteServe, frp_unusable

ENV = {frp.ENV_TOKEN: "frp-token", frp.ENV_SECRET_KEY: "shared-key"}


class MemoryPipe(Pipe):
    """One end of an in-memory byte pipe."""

    def __init__(self, incoming: asyncio.Queue[bytes], outgoing: asyncio.Queue[bytes]) -> None:
        self._incoming = incoming
        self._outgoing = outgoing

    async def read(self, n: int = 65536) -> bytes:
        return await self._incoming.get()

    async def write(self, data: bytes) -> None:
        await self._outgoing.put(data)

    async def close(self) -> None:
        await self._outgoing.put(b"")


def pair() -> tuple[MemoryPipe, MemoryPipe]:
    one: asyncio.Queue[bytes] = asyncio.Queue()
    other: asyncio.Queue[bytes] = asyncio.Queue()
    return MemoryPipe(one, other), MemoryPipe(other, one)


def common(protocol: str = "tcp") -> frp.Common:
    ca = "/ca.pem" if protocol == "wss" else None
    return frp.Common(address="relay.example.org", port=443, protocol=protocol, ca_file=ca)


def write(tmp_path: Path, text: str) -> Path:
    path = tmp_path / "visitor.toml"
    path.write_text(text, encoding="utf-8")
    return path


@pytest.mark.parametrize("protocol", ["tcp", "websocket", "wss"])
@pytest.mark.parametrize("direct", [False, True])
def test_relay_visitor_files_load(tmp_path: Path, protocol: str, direct: bool) -> None:
    text = frp.render_visitor(
        common(protocol), [("web", 41000), ("rpc", 41001)], "op", direct=direct
    )
    config = load(write(tmp_path, text), ENV)
    assert config.server.token == "frp-token"
    assert config.server.protocol == protocol
    bound = config.bound()
    assert [visitor.bind_port for visitor in bound] == [41000, 41001]
    for visitor in bound:
        route = config.route(visitor)
        assert route.kind == "stcp"
        assert route.secret_key == "shared-key"
        assert route.server_name in ("relay-op-web", "relay-op-rpc")


def test_a_missing_secret_is_refused_by_name(tmp_path: Path) -> None:
    text = frp.render_visitor(common(), [("web", 41000)], "op", direct=False)
    with pytest.raises(ConfigError, match=frp.ENV_SECRET_KEY):
        load(write(tmp_path, text), {frp.ENV_TOKEN: "frp-token"})


@pytest.mark.parametrize(
    ("extra", "match"),
    [
        ('transport.protocol = "kcp"', "kcp"),
        ('transport.proxyURL = "http://proxy:3128"', "proxyURL"),
        ("transport.poolCount = 2", "poolCount"),
        ('auth.method = "oidc"', "oidc"),
        ("surprise = 1", "surprise"),
    ],
)
def test_settings_it_does_not_run_are_refused(extra: str, match: str) -> None:
    raw = tomllib.loads(
        "\n".join(
            [
                'serverAddr = "h"',
                'auth.token = "t"',
                extra,
                "[[visitors]]",
                'name = "v"',
                'type = "stcp"',
                'serverName = "p"',
                'secretKey = "s"',
                "bindPort = 41000",
            ]
        )
    )
    with pytest.raises(ConfigError, match=match):
        parse(raw)


def test_proxies_and_unbound_files_are_refused() -> None:
    base = {"serverAddr": "h", "auth": {"token": "t"}}
    with pytest.raises(ConfigError, match="proxies"):
        parse({**base, "proxies": [{"name": "p", "type": "stcp"}]})
    unbound = {"name": "v", "type": "stcp", "serverName": "p", "secretKey": "s"}
    with pytest.raises(ConfigError):
        parse({**base, "visitors": [unbound]})


def test_auth_key_is_frps_md5_signature() -> None:
    assert auth_key("token", 1700000000) == hashlib.md5(b"token1700000000").hexdigest()


def test_messages_leave_out_empty_fields_and_read_back() -> None:
    async def scenario() -> None:
        client, server = pair()
        await client.write(pack(LOGIN, {"user": "", "run_id": "abc", "pool_count": 0}))
        kind, body = await read_message(BufferedPipe(server))
        assert kind == LOGIN
        assert body == {"run_id": "abc"}

    asyncio.run(scenario())


def test_crypto_pipes_carry_bytes_both_ways() -> None:
    async def scenario() -> None:
        client, server = pair()
        near = CryptoPipe(BufferedPipe(client), "secret")
        far = CryptoPipe(BufferedPipe(server), "secret")
        await near.write(b"hello ")
        await near.write(b"frps")
        received = b""
        while len(received) < 10:
            received += await far.read()
        assert received == b"hello frps"
        await far.write(b"back")
        assert await near.read() == b"back"

    asyncio.run(scenario())


def test_direct_note_says_when_the_visitor_has_no_direct_path() -> None:
    start = f"2026-01-01 00:00:00.000 [I] [relay/visitor] {START_MARKER} for config file [x]"
    none = f"2026-01-01 00:00:01.000 [W] [relay/visitor] [v] relay's frp visitor {NO_DIRECT_MARKER}"
    assert frp.direct_note([start], 10) == "trying the direct path (XTCP)"
    assert frp.direct_note([start, none], 10) == (
        "the direct path (XTCP) is not in relay's frp visitor yet; connections go through "
        "the rendezvous host (STCP)"
    )
    assert frp.direct_note([start, none, start], 10) == "trying the direct path (XTCP)"


def test_auto_falls_back_only_when_the_visitor_cannot_speak_the_transport(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("CLIO_RELAY_TEST_TOKEN", "frp-token")
    site = SiteServe(frpc="/opt/frp/frpc")
    for protocol in ("tcp", "websocket", "wss"):
        usable = Rendezvous(
            address="h", port=7000, protocol=protocol, token_env="CLIO_RELAY_TEST_TOKEN"
        )
        assert frp_unusable(site, usable) is None
    for protocol in ("kcp", "quic"):
        refused = Rendezvous(
            address="h", port=7000, protocol=protocol, token_env="CLIO_RELAY_TEST_TOKEN"
        )
        assert f"uses {protocol}" in (frp_unusable(site, refused) or "")

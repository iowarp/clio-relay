"""Relay's own frp visitor: the files it accepts and the wire it speaks (RL20)."""

from __future__ import annotations

import asyncio
import hashlib
import random
import socket
import struct
import tomllib
from pathlib import Path

import pytest

from clio_relay.frp_visitor import START_MARKER, nathole
from clio_relay.frp_visitor.config import DEFAULT_STUN_SERVER, ConfigError, load, parse
from clio_relay.frp_visitor.pipe import BufferedPipe, Pipe, ProtocolError
from clio_relay.frp_visitor.quic import RECEIVE_WINDOW, DirectPath, QuicTunnel
from clio_relay.frp_visitor.wire import (
    LOGIN,
    NAT_HOLE_SID,
    CryptoPipe,
    auth_key,
    pack,
    read_message,
    seal,
    unseal,
)
from clio_relay.frp_visitor.wire import parse as parse_message
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
        fallback = config.fallback(visitor)
        if direct:
            assert visitor.kind == "xtcp"
            assert visitor.keep_tunnel_open
            assert visitor.protocol == "quic"
            assert visitor.fallback_timeout_ms == 10000
            assert visitor.secret_key == "shared-key"
            assert fallback is not None
            assert fallback.kind == "stcp"
            assert fallback.bind_port == -1
            target = fallback
        else:
            assert visitor.kind == "stcp"
            assert fallback is None
            target = visitor
        assert target.secret_key == "shared-key"
        assert target.server_name in ("relay-op-web", "relay-op-rpc")


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


def _line(level: str, message: str) -> str:
    return f"2026-01-01 00:00:00.000 [{level}] [relay/visitor] {message}"


def test_direct_note_reads_the_visitors_own_xtcp_lines() -> None:
    start = _line("I", f"{START_MARKER} for config file [x]")
    opened = _line(
        "I", "[v] establishing nat hole connection successful, sid [s], remoteAddr [1.2.3.4:5]"
    )
    failed = _line("W", "[v] make hole error: wait detect message error: i/o timeout")
    fell = _line("E", "[v] open tunnel error: open tunnel timeout")
    assert frp.direct_note([start], 10) == "trying the direct path (XTCP)"
    assert frp.direct_note([start, opened], 10) == "direct path (XTCP) open"
    assert frp.direct_note([start, failed], 10) == (
        "the direct path (XTCP) failed (make hole error: wait detect message error: i/o "
        "timeout); a connection waits 10 s for it, then goes through the rendezvous host (STCP)"
    )
    assert "went through the rendezvous host (STCP)" in frp.direct_note([start, failed, fell], 10)
    assert frp.direct_note([start, failed, start], 10) == "trying the direct path (XTCP)"


def _xtcp(**extra: object) -> dict[str, object]:
    visitor: dict[str, object] = {
        "name": "p2p",
        "type": "xtcp",
        "serverName": "p",
        "secretKey": "s",
        "bindPort": 41000,
    }
    visitor.update(extra)
    return visitor


def test_an_xtcp_visitor_takes_frps_defaults_and_needs_no_fallback() -> None:
    config = parse({"serverAddr": "h", "auth": {"token": "t"}, "visitors": [_xtcp()]})
    (visitor,) = config.visitors
    assert config.fallback(visitor) is None
    assert config.server.stun_server == DEFAULT_STUN_SERVER
    assert (visitor.protocol, visitor.fallback_timeout_ms) == ("quic", 1000)
    assert (visitor.max_retries_an_hour, visitor.min_retry_interval_s) == (8, 90)
    assert not visitor.keep_tunnel_open
    assert not visitor.disable_assisted_addrs
    tuned = parse(
        {
            "serverAddr": "h",
            "natHoleStunServer": "stun.example.org:3478",
            "visitors": [_xtcp(natTraversal={"disableAssistedAddrs": True}, maxRetriesAnHour=0)],
        }
    )
    assert tuned.server.stun_server == "stun.example.org:3478"
    assert tuned.visitors[0].disable_assisted_addrs
    assert tuned.visitors[0].max_retries_an_hour == 8


@pytest.mark.parametrize(
    ("visitors", "match"),
    [
        ([_xtcp(protocol="kcp")], "kcp"),
        ([_xtcp(fallbackTo="nowhere")], "nowhere"),
        ([_xtcp(fallbackTo="other"), _xtcp(name="other", bindPort=-1)], "stcp"),
        (
            [
                {
                    "name": "v",
                    "type": "stcp",
                    "serverName": "p",
                    "bindPort": 1,
                    "keepTunnelOpen": True,
                }
            ],
            "keepTunnelOpen",
        ),
        ([_xtcp(minRetryInterval=-1)], "minRetryInterval"),
    ],
)
def test_xtcp_settings_frp_would_not_run_are_refused(
    visitors: list[dict[str, object]], match: str
) -> None:
    with pytest.raises(ConfigError, match=match):
        parse({"serverAddr": "h", "visitors": visitors})


def _stun_answer(tid: bytes, attrs: list[tuple[int, bytes]]) -> bytes:
    body = b"".join(
        struct.pack("!HH", kind, len(value)) + value + b"\0" * (-len(value) % 4)
        for kind, value in attrs
    )
    return struct.pack("!HHI12s", 0x0101, len(body), 0x2112A442, tid) + body


def _plain(ip: str, port: int) -> bytes:
    return struct.pack("!BBH", 0, 1, port) + socket.inet_aton(ip)


def _xored(ip: str, port: int) -> bytes:
    raw = bytes(
        a ^ b for a, b in zip(socket.inet_aton(ip), struct.pack("!I", 0x2112A442), strict=True)
    )
    return struct.pack("!BBH", 0, 1, port ^ 0x2112) + raw


def test_stun_answers_are_read_as_frp_reads_them() -> None:
    request, tid = nathole.stun_request()
    assert len(request) == 20
    assert request[:2] == b"\x00\x01"
    assert request[8:] == tid
    answer = _stun_answer(
        tid,
        [
            (0x0001, _plain("10.0.0.1", 1000)),
            (0x0020, _xored("203.0.113.7", 40000)),
            (0x0005, _plain("198.51.100.1", 3479)),
            (0x802C, _plain("198.51.100.2", 3479)),
        ],
    )
    assert nathole.stun_parse(answer, tid) == ("203.0.113.7:40000", "198.51.100.2:3479")
    only_mapped = _stun_answer(tid, [(0x0001, _plain("10.0.0.1", 1000))])
    assert nathole.stun_parse(only_mapped, tid) == ("10.0.0.1:1000", "")
    assert nathole.stun_parse(answer, b"x" * 12) is None
    assert nathole.stun_parse(b"short", tid) is None


def test_nat_classes_match_frps() -> None:
    assert nathole.classify(["1.1.1.1:5", "1.1.1.1:5"]) == ("EasyNAT", "BehaviorNoChange")
    assert nathole.classify(["1.1.1.1:5", "1.1.1.1:6"]) == ("HardNAT", "BehaviorPortChanged")
    assert nathole.classify(["1.1.1.1:5", "1.1.1.2:5"]) == ("HardNAT", "BehaviorIPChanged")
    assert nathole.classify(["1.1.1.1:5", "1.1.1.2:6"]) == ("HardNAT", "BehaviorBothChanged")


def test_sealed_datagrams_open_only_with_the_secret() -> None:
    body = {"transaction_id": "t", "sid": "s", "response": True, "nonce": "000"}
    sealed = seal(pack(NAT_HOLE_SID, body), "secret")
    assert sealed[16:] != seal(pack(NAT_HOLE_SID, body), "secret")[16:]
    assert parse_message(unseal(sealed, "secret")) == (NAT_HOLE_SID, body)
    with pytest.raises(ProtocolError):
        parse_message(unseal(sealed, "other"))


def test_two_sides_punch_to_each_other_over_loopback() -> None:
    async def scenario() -> None:
        sender, receiver = nathole.udp_socket(), nathole.udp_socket()
        to_receiver = f"127.0.0.1:{receiver.getsockname()[1]}"
        to_sender = f"127.0.0.1:{sender.getsockname()[1]}"
        near = nathole.Hole("sid-1", "quic", [to_receiver], [], {"role": "sender"})
        far = nathole.Hole("sid-1", "quic", [to_sender], [], {"role": "receiver"})
        (got_near, peer_near), (got_far, peer_far) = await asyncio.gather(
            nathole.make_hole(sender, near, "secret"), nathole.make_hole(receiver, far, "secret")
        )
        try:
            assert (got_near, got_far) == (sender, receiver)
            assert f"{peer_near[0]}:{peer_near[1]}" == to_receiver
            assert f"{peer_far[0]}:{peer_far[1]}" == to_sender
        finally:
            sender.close()
            receiver.close()

    asyncio.run(scenario())


def test_a_stalled_reader_holds_the_peer_back_and_loses_nothing(tmp_path: Path) -> None:
    import datetime

    from aioquic.asyncio.server import serve  # pyright: ignore[reportUnknownVariableType]
    from aioquic.quic.configuration import QuicConfiguration
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.x509.oid import NameOID

    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "frpc")])
    now = datetime.datetime.now(datetime.UTC)
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(1)
        .not_valid_before(now)
        .not_valid_after(now + datetime.timedelta(days=1))
        .sign(key, hashes.SHA256())
    )
    (tmp_path / "cert.pem").write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    (tmp_path / "key.pem").write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    blob = random.randbytes(4 * RECEIVE_WINDOW)

    async def scenario() -> None:
        configuration = QuicConfiguration(is_client=False, alpn_protocols=["frp"])
        configuration.load_cert_chain(  # pyright: ignore[reportUnknownMemberType]
            str(tmp_path / "cert.pem"), str(tmp_path / "key.pem")
        )
        tasks: list[asyncio.Task[None]] = []

        def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            async def send() -> None:
                await reader.readexactly(2)
                writer.write(blob)
                writer.write_eof()

            tasks.append(asyncio.create_task(send()))

        server = await serve("127.0.0.1", 0, configuration=configuration, stream_handler=handle)
        sock = nathole.udp_socket()
        transport = server._transport  # pyright: ignore[reportPrivateUsage]
        assert transport is not None
        port = transport.get_extra_info("sockname")[1]
        tunnel = await QuicTunnel.open(sock, ("127.0.0.1", port))
        try:
            pipe = await tunnel.open_stream()
            await pipe.write(b"go")
            # Nothing reads for a while, as when the local writer is stuck: the
            # peer may send only the first credit, however much it has queued.
            await asyncio.sleep(3)
            paced = tunnel._protocol.paced  # pyright: ignore[reportPrivateUsage]
            assert 0 < sum(paced.unread.values()) <= RECEIVE_WINDOW
            got = bytearray()
            while data := await pipe.read():
                got += data
            assert hashlib.sha256(got).digest() == hashlib.sha256(blob).digest()
            await pipe.close()
            assert paced.unread == {}
        finally:
            await tunnel.close()
            server.close()
            await asyncio.gather(*tasks, return_exceptions=True)

    asyncio.run(scenario())


def test_a_punch_with_no_answer_says_so_in_frps_words() -> None:
    async def scenario() -> None:
        sock = nathole.udp_socket()
        silent = nathole.udp_socket()
        hole = nathole.Hole(
            "sid-1",
            "quic",
            [f"127.0.0.1:{silent.getsockname()[1]}"],
            [],
            {"role": "sender", "read_timeout": 200},
        )
        try:
            with pytest.raises(nathole.HoleError, match="wait detect message error"):
                await nathole.make_hole(sock, hole, "secret")
            assert sock.fileno() == -1
        finally:
            silent.close()

    asyncio.run(scenario())


def test_a_connection_with_no_hole_is_told_why_and_a_punch_is_asked_for() -> None:
    async def scenario() -> None:
        lines: list[str] = []

        async def refuse(body: dict[str, object]) -> dict[str, object]:
            return {
                "transaction_id": body["transaction_id"],
                "error": "xtcp server for [p] doesn't exist",
            }

        config = parse({"serverAddr": "h", "visitors": [_xtcp()]})
        path = DirectPath(
            config.visitors[0],
            proxy_name="p",
            stun_server=DEFAULT_STUN_SERVER,
            rendezvous="h:7000",
            transact=refuse,
            log=lambda level, message: lines.append(f"[{level}] {message}"),
        )
        path.start()
        try:
            with pytest.raises(TimeoutError, match="open tunnel timeout"):
                await path.open(0.3)
        finally:
            await path.close()
        assert "[W] [p2p] nathole precheck error: xtcp server for [p] doesn't exist" in lines

    asyncio.run(scenario())


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

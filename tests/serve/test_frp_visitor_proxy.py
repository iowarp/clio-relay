"""The visitor reaches frps through the host's proxy, and says why when it cannot."""

from __future__ import annotations

import asyncio
import struct
from typing import Any

import pytest

from clio_relay import frp_transport
from clio_relay.errors import RelayError
from clio_relay.frp_visitor import dial, proxy
from clio_relay.frp_visitor.config import ConfigError, parse

FENCED = {
    "HTTPS_PROXY": "http://127.0.0.1:3128",
    "HTTP_PROXY": "http://127.0.0.1:3129",
    "ALL_PROXY": "socks5h://127.0.0.1:1080",
    "NO_PROXY": "localhost,127.0.0.1,::1,10.0.0.0/8,.internal.example",
}


def test_the_environment_picks_the_proxy_for_the_protocol() -> None:
    assert proxy.from_environment("wss", "frps.example.org", FENCED) == "http://127.0.0.1:3128"
    assert proxy.from_environment("tcp", "frps.example.org", FENCED) == "http://127.0.0.1:3129"
    assert proxy.from_environment("tcp", "h", {"ALL_PROXY": "socks5://p:1080"}) == (
        "socks5://p:1080"
    )
    assert proxy.from_environment(
        "wss", "h", {"HTTPS_PROXY": "http://up", "https_proxy": "http://low"}
    ) == ("http://low")
    assert proxy.from_environment("tcp", "frps.example.org", {}) is None


@pytest.mark.parametrize(
    "host", ["127.0.0.1", "10.2.3.4", "localhost", "a.internal.example", "::1"]
)
def test_no_proxy_hosts_are_dialed_directly(host: str) -> None:
    assert proxy.from_environment("wss", host, FENCED) is None


def test_proxy_urls_parse_and_bad_ones_are_refused() -> None:
    found = proxy.parse("http://user:p%40ss@proxy.example:3128")
    assert (found.host, found.port, found.username, found.password) == (
        "proxy.example",
        3128,
        "user",
        "p@ss",
    )
    assert found.shown == "http://proxy.example:3128"
    assert proxy.parse("socks5h://s").port == 1080
    with pytest.raises(ValueError, match="scheme"):
        proxy.parse("ftp://proxy:21")
    with pytest.raises(ConfigError, match="proxyURL"):
        parse(
            {
                "serverAddr": "h",
                "auth": {"token": "t"},
                "transport": {"proxyURL": "ftp://proxy:21"},
                "visitors": [
                    {
                        "name": "v",
                        "type": "stcp",
                        "serverName": "p",
                        "secretKey": "s",
                        "bindPort": 41000,
                    }
                ],
            }
        )


async def _echo_target() -> tuple[asyncio.Server, int]:
    async def echo(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        writer.write(await reader.readexactly(4))
        await writer.drain()
        writer.close()

    server = await asyncio.start_server(echo, "127.0.0.1", 0)
    return server, server.sockets[0].getsockname()[1]


async def _http_proxy(answer: bytes, seen: list[bytes]) -> tuple[asyncio.Server, int]:
    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        head = await reader.readuntil(b"\r\n\r\n")
        seen.append(head)
        writer.write(answer)
        await writer.drain()
        if not answer.startswith(b"HTTP/1.1 200"):
            writer.close()
            return
        target = head.split(b" ")[1].decode()
        host, port = target.rsplit(":", 1)
        up_reader, up_writer = await asyncio.open_connection(host, int(port))
        up_writer.write(await reader.readexactly(4))
        writer.write(await up_reader.readexactly(4))
        await writer.drain()
        writer.close()
        up_writer.close()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    return server, server.sockets[0].getsockname()[1]


def test_http_connect_carries_bytes_and_sends_credentials() -> None:
    async def scenario() -> None:
        target, target_port = await _echo_target()
        seen: list[bytes] = []
        server, port = await _http_proxy(b"HTTP/1.1 200 Connection established\r\n\r\n", seen)
        async with target, server:
            found = proxy.parse(f"http://relay:secret@127.0.0.1:{port}")
            reader, writer = await proxy.open_tunnel(found, "127.0.0.1", target_port)
            writer.write(b"ping")
            await writer.drain()
            assert await reader.readexactly(4) == b"ping"
            writer.close()
        assert seen[0].startswith(f"CONNECT 127.0.0.1:{target_port} HTTP/1.1".encode())
        assert b"Proxy-Authorization: Basic cmVsYXk6c2VjcmV0" in seen[0]

    asyncio.run(scenario())


def test_a_refused_connect_says_what_the_proxy_answered() -> None:
    async def scenario() -> None:
        server, port = await _http_proxy(b"HTTP/1.1 403 Forbidden\r\n\r\n", [])
        async with server:
            with pytest.raises(
                proxy.ProxyError, match="refused CONNECT frps.example.org:443: HTTP/1.1 403"
            ):
                await proxy.open_tunnel(
                    proxy.parse(f"http://127.0.0.1:{port}"), "frps.example.org", 443
                )

    asyncio.run(scenario())


def test_socks5h_gives_the_proxy_the_host_name() -> None:
    async def scenario() -> None:
        asked: list[Any] = []

        async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            greeting = await reader.readexactly(3)
            asked.append(greeting)
            writer.write(b"\x05\x00")
            head = await reader.readexactly(5)
            name = await reader.readexactly(head[4])
            (port,) = struct.unpack(">H", await reader.readexactly(2))
            asked.append((head[3], name, port))
            writer.write(b"\x05\x00\x00\x01" + bytes(4) + b"\x00\x00")
            writer.write(b"pong")
            await writer.drain()
            writer.close()

        server = await asyncio.start_server(handle, "127.0.0.1", 0)
        async with server:
            port = server.sockets[0].getsockname()[1]
            reader, writer = await proxy.open_tunnel(
                proxy.parse(f"socks5h://127.0.0.1:{port}"), "frps.example.org", 443
            )
            assert await reader.readexactly(4) == b"pong"
            writer.close()
        assert asked == [b"\x05\x01\x00", (3, b"frps.example.org", 443)]

    asyncio.run(scenario())


def test_an_unusable_proxy_variable_is_a_network_error(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in ("http_proxy", "all_proxy", "ALL_PROXY", "no_proxy", "NO_PROXY"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("HTTP_PROXY", "ftp://proxy:21")
    server = parse(
        {
            "serverAddr": "frps.example.org",
            "auth": {"token": "t"},
            "visitors": [
                {"name": "v", "type": "stcp", "serverName": "p", "secretKey": "s", "bindPort": 1}
            ],
        }
    ).server
    with pytest.raises(proxy.ProxyError, match="proxy setting cannot be used"):
        asyncio.run(dial._open(server))  # pyright: ignore[reportPrivateUsage]


def test_a_failed_link_says_what_the_visitor_said() -> None:
    detail = (
        "2026-11-26 10:00:00.000 [I] [relay/visitor] start\n"
        "2026-11-26 10:00:01.000 [E] [client/service.go:389] [relay/visitor] "
        "login to the server failed: cannot reach the proxy at http://127.0.0.1:3128\n"
        "exited 1"
    )
    reason = frp_transport._visitor_reason(detail)  # pyright: ignore[reportPrivateUsage]
    assert reason == "login to the server failed: cannot reach the proxy at http://127.0.0.1:3128"
    assert frp_transport._visitor_reason("no levels here") is None  # pyright: ignore[reportPrivateUsage]
    link = object.__new__(frp_transport.BrokeredTcpTransport)
    link._failure_detail = detail  # pyright: ignore[reportPrivateUsage, reportAttributeAccessIssue]
    told = link._translate_tunnel_failure(  # pyright: ignore[reportPrivateUsage]
        RelayError("frp stcp link exited during bring-up: timed out")
    )
    assert str(told).startswith(
        "frp stcp link exited during bring-up: timed out; the visitor said:"
    )
    assert "cannot reach the proxy" in str(told)
    other = ValueError("x")
    assert link._translate_tunnel_failure(other) is other  # pyright: ignore[reportPrivateUsage]

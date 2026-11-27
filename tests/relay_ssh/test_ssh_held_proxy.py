"""Relay's held ssh connection dials through the proxy its environment names.

A loopback HTTP ``CONNECT`` proxy is the only way to reach ``site.invalid``.
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from pathlib import Path

import pytest
import pytest_asyncio

from clio_relay.adapters.ssh import errors, held
from clio_relay.adapters.ssh.profile import Profile
from clio_relay.net import proxy as proxies
from tests.relay_ssh.test_ssh_held import (
    Site,
    _free_port,  # pyright: ignore[reportPrivateUsage]
    _held,  # pyright: ignore[reportPrivateUsage]
    _profile,  # pyright: ignore[reportPrivateUsage]
    _run,  # pyright: ignore[reportPrivateUsage]
    serve_site,
)

pytestmark = pytest.mark.asyncio


@pytest_asyncio.fixture
async def site(tmp_path: Path) -> AsyncIterator[Site]:
    async with serve_site(tmp_path) as served:
        yield served


@dataclass
class Proxy:
    port: int
    asked: list[str] = field(default_factory=list[str])
    refuse: bool = False


async def _proxy(site: Site) -> tuple[asyncio.Server, Proxy]:
    """An HTTP ``CONNECT`` proxy: the only way to reach ``site.invalid``."""
    proxy = Proxy(port=0)

    async def pump(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        with contextlib.suppress(OSError):
            while data := await reader.read(65536):
                writer.write(data)
                await writer.drain()
        writer.close()

    async def serve(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        head = await reader.readuntil(b"\r\n\r\n")
        target = head.split(b"\r\n", 1)[0].split()[1].decode()
        proxy.asked.append(target)
        if proxy.refuse or target != f"site.invalid:{site.port}":
            writer.write(b"HTTP/1.1 403 Forbidden\r\n\r\n")
            writer.close()
            return
        up_reader, up_writer = await asyncio.open_connection("127.0.0.1", site.port)
        writer.write(b"HTTP/1.1 200 Connection established\r\n\r\n")
        await asyncio.gather(pump(reader, up_writer), pump(up_reader, writer))

    server = await asyncio.start_server(serve, "127.0.0.1", 0)
    proxy.port = server.sockets[0].getsockname()[1]
    return server, proxy


def _no_proxy_set(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in ("HTTPS_PROXY", "ALL_PROXY", "HTTP_PROXY", "NO_PROXY"):
        monkeypatch.delenv(name, raising=False)
        monkeypatch.delenv(name.lower(), raising=False)


def _at(
    monkeypatch: pytest.MonkeyPatch, connection: held.Held, site: Site, host: str, port: int
) -> None:
    resolved = dataclasses.replace(_profile(site, user="keyed", keys=True), host=host, port=port)

    async def resolve() -> Profile:
        return resolved

    monkeypatch.setattr(connection, "_resolve", resolve)


async def test_relay_reaches_the_site_through_the_proxy_its_environment_names(
    site: Site, monkeypatch: pytest.MonkeyPatch
) -> None:
    server, proxy = await _proxy(site)
    _no_proxy_set(monkeypatch)
    connection = _held(monkeypatch, site, signs_in=False)
    _at(monkeypatch, connection, site, "site.invalid", site.port)
    try:
        monkeypatch.setenv("HTTPS_PROXY", f"http://127.0.0.1:{proxy.port}")
        done = await _run(connection, "echo one")
        assert done.stdout == b"ran:echo one\n", done.stderr
        assert proxy.asked == [f"site.invalid:{site.port}"]
        env = {"HTTP_PROXY": "h", "HTTPS_PROXY": "s", "ALL_PROXY": "a"}
        assert proxies.from_environment("ssh", "site.invalid", env) == "s"
        assert (
            proxies.from_environment("ssh", "site.invalid", {**env, "NO_PROXY": ".invalid"}) is None
        )
    finally:
        await connection.close()
        server.close()


async def test_a_failed_dial_says_how_relay_dialed(
    site: Site, monkeypatch: pytest.MonkeyPatch
) -> None:
    server, proxy = await _proxy(site)
    _no_proxy_set(monkeypatch)
    connection = _held(monkeypatch, site, signs_in=False)
    try:
        _at(monkeypatch, connection, site, "127.0.0.1", _free_port())
        closed = await _run(connection, "echo one")
        text = closed.stderr.decode()
        assert errors.cause(text, closed.exit_code) == "network", text
        assert "Connection refused\nrelay dialed 127.0.0.1:" in text
        assert "directly (no proxy in its environment) ([" in text

        monkeypatch.setenv("HTTPS_PROXY", f"http://127.0.0.1:{proxy.port}")
        proxy.refuse = True
        _at(monkeypatch, connection, site, "site.invalid", site.port)
        refused = (await _run(connection, "echo one")).stderr.decode()
        assert refused.startswith("ssh: Could not resolve hostname site.invalid")
        assert (
            f"relay's proxy http://127.0.0.1:{proxy.port} did not open site.invalid:{site.port}"
            f" (refused CONNECT site.invalid:{site.port}: HTTP/1.1 403 Forbidden),"
            " so relay dialed directly"
        ) in refused
    finally:
        await connection.close()
        server.close()

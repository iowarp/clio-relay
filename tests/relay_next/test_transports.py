"""Relay over real stdio and Streamable HTTP, started the way a host starts it (LQ41).

The fastmcp client runs a task; the plain ``mcp`` SDK client, with nothing from
fastmcp, runs a job and answers a form and a URL question, on both protocol eras.
"""

from __future__ import annotations

import asyncio
import os
import socket
import subprocess
import sys
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import httpx
import httpx2
import mcp_types
import pytest
from fastmcp import Client as FastMCPClient
from fastmcp.client.transports import StdioTransport, StreamableHttpTransport
from fastmcp_tasks.client import (
    call_tool_task,  # pyright: ignore[reportUnknownVariableType]
)
from mcp import Client
from mcp.client.context import ClientRequestContext
from mcp.client.stdio import StdioServerParameters
from mcp.client.streamable_http import streamable_http_client

from clio_relay.surface.settings import RelayPaths

from .conftest import add_site, answer_page

pytestmark = pytest.mark.release_platform("posix")

ECHO: dict[str, Any] = {"executable": "echo", "arguments": ["over-the-wire"]}
ENTRY = "from clio_relay.cli_next import main; main()"
MODES = ["legacy", "auto"]


def _env(paths: RelayPaths) -> dict[str, str]:
    env = dict(os.environ)
    env["CLIO_RELAY_CONFIG_DIR"] = str(paths.config_dir)
    env["CLIO_RELAY_STATE_DIR"] = str(paths.state_dir)
    env.pop("CLIO_RELAY_TOKEN", None)
    return env


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _stdio(paths: RelayPaths) -> StdioServerParameters:
    return StdioServerParameters(
        command=sys.executable, args=["-c", ENTRY, "serve"], env=_env(paths)
    )


async def _answer(
    context: ClientRequestContext, params: mcp_types.ElicitRequestParams
) -> mcp_types.ElicitResult:
    """A person who approves: opens the page for a URL question, says yes to a form."""
    del context
    if isinstance(params, mcp_types.ElicitRequestURLParams):
        assert await asyncio.to_thread(answer_page, params.url) == 200
        return mcp_types.ElicitResult(action="accept")
    return mcp_types.ElicitResult(action="accept", content={"approve": True})


def _state(result: mcp_types.CallToolResult) -> str:
    assert not result.is_error, result
    assert result.structured_content is not None
    return str(result.structured_content["state"])


@pytest.fixture
def relay_http(relay_paths: RelayPaths, tmp_path: Path) -> Iterator[tuple[str, str]]:
    """Serve relay over Streamable HTTP; yield its URL and bearer token."""
    port = _free_port()
    token_file = tmp_path / "token"
    process = subprocess.Popen(
        [
            sys.executable,
            "-c",
            ENTRY,
            "serve",
            "--http",
            "--port",
            str(port),
            "--token-file",
            str(token_file),
        ],
        env=_env(relay_paths),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    url = f"http://127.0.0.1:{port}/mcp"
    try:
        deadline = time.monotonic() + 30
        while True:
            try:
                httpx.get(url, timeout=1)
                break
            except httpx.TransportError:
                if time.monotonic() > deadline or process.poll() is not None:
                    raise
                time.sleep(0.2)
        assert oct(token_file.stat().st_mode & 0o777) == "0o600"
        yield url, token_file.read_text(encoding="utf-8").strip()
    finally:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()


def test_stdio_task(relay_paths: RelayPaths) -> None:
    add_site(relay_paths)
    params = _stdio(relay_paths)
    transport = StdioTransport(command=params.command, args=params.args, env=params.env)

    async def scenario() -> None:
        async with FastMCPClient(transport, mode="auto") as client:
            tools = {tool.name for tool in await client.list_tools()}
            assert tools == {"describe", "run", "exec", "files", "serve"}
            task = await call_tool_task(client, "run", {"site": "fx", "spec": ECHO})
            result = await task.result()
            assert result.structured_content is not None
            assert result.structured_content["state"] == "completed"

    asyncio.run(scenario())


def test_http_refuses_a_wrong_token(relay_http: tuple[str, str]) -> None:
    url, _token = relay_http
    for headers in ({}, {"Authorization": "Bearer wrong"}):
        refused = httpx.post(
            url,
            json={"jsonrpc": "2.0", "id": 1, "method": "tools/list"},
            headers=headers,
            timeout=5,
        )
        assert refused.status_code == 401


def test_http_task(relay_paths: RelayPaths, relay_http: tuple[str, str]) -> None:
    add_site(relay_paths)
    url, token = relay_http

    async def scenario() -> None:
        async with FastMCPClient(StreamableHttpTransport(url, auth=token), mode="auto") as client:
            task = await call_tool_task(client, "run", {"site": "fx", "spec": ECHO})
            result = await task.result()
            assert result.structured_content is not None
            assert result.structured_content["state"] == "completed"

    asyncio.run(scenario())


@pytest.mark.parametrize("mode", MODES)
def test_plain_mcp_client_over_stdio(relay_paths: RelayPaths, mode: str) -> None:
    add_site(relay_paths, site_id="open")
    add_site(relay_paths, site_id="spend", spend="always")
    add_site(relay_paths, site_id="sso", config={"queue_s": 0.2, "run_s": 0.3, "sign_in": True})

    async def scenario() -> None:
        async with Client(_stdio(relay_paths), mode=mode, elicitation_callback=_answer) as client:
            expected = "2025-11-25" if mode == "legacy" else "2026-07-28"
            assert client.protocol_version == expected
            listed = await client.list_tools()
            assert {tool.name for tool in listed.tools} == {
                "describe",
                "run",
                "exec",
                "files",
                "serve",
            }
            for site in ("open", "spend", "sso"):
                ran = await client.call_tool("run", {"site": site, "spec": ECHO, "wait_s": 60})
                assert _state(ran) == "completed", site

    asyncio.run(scenario())


@pytest.mark.parametrize("mode", MODES)
def test_plain_mcp_client_over_http(
    relay_paths: RelayPaths, relay_http: tuple[str, str], mode: str
) -> None:
    add_site(relay_paths, site_id="spend", spend="always")
    add_site(relay_paths, site_id="sso", config={"queue_s": 0.2, "run_s": 0.3, "sign_in": True})
    url, token = relay_http

    async def scenario() -> None:
        async with httpx2.AsyncClient(
            headers={"Authorization": f"Bearer {token}"}, timeout=120
        ) as http:
            transport = streamable_http_client(url, http_client=http)
            async with Client(transport, mode=mode, elicitation_callback=_answer) as client:
                for site in ("spend", "sso"):
                    ran = await client.call_tool("run", {"site": site, "spec": ECHO, "wait_s": 60})
                    assert _state(ran) == "completed", site

    asyncio.run(scenario())

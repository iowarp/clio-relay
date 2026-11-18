"""Relay over real stdio and Streamable HTTP, started the way a host starts it (LQ41)."""

from __future__ import annotations

import asyncio
import os
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import httpx
import pytest
from fastmcp import Client
from fastmcp.client.transports import StdioTransport, StreamableHttpTransport
from fastmcp_tasks.client import (
    call_tool_task,  # pyright: ignore[reportUnknownVariableType]
)

from clio_relay.surface.settings import RelayPaths

from .conftest import add_site

pytestmark = pytest.mark.release_platform("posix")

ECHO: dict[str, Any] = {"executable": "echo", "arguments": ["over-the-wire"]}
ENTRY = "from clio_relay.cli_next import main; main()"


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


def test_stdio(relay_paths: RelayPaths) -> None:
    add_site(relay_paths)
    transport = StdioTransport(
        command=sys.executable, args=["-c", ENTRY, "serve"], env=_env(relay_paths)
    )

    async def scenario() -> None:
        async with Client(transport, mode="auto") as client:
            tools = {tool.name for tool in await client.list_tools()}
            assert tools == {"describe", "run", "exec", "files", "serve"}
            task = await call_tool_task(client, "run", {"site": "fx", "spec": ECHO})
            result = await task.result()
            assert result.structured_content is not None
            assert result.structured_content["state"] == "completed"

    asyncio.run(scenario())


def test_streamable_http_needs_the_token(relay_paths: RelayPaths, tmp_path: Path) -> None:
    add_site(relay_paths)
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
        stderr=subprocess.PIPE,
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
        token = token_file.read_text(encoding="utf-8").strip()
        refused = httpx.post(
            url,
            json={"jsonrpc": "2.0", "id": 1, "method": "tools/list"},
            headers={"Authorization": "Bearer wrong"},
            timeout=5,
        )
        assert refused.status_code == 401

        async def scenario() -> None:
            transport = StreamableHttpTransport(url, auth=token)
            async with Client(transport, mode="auto") as client:
                task = await call_tool_task(client, "run", {"site": "fx", "spec": ECHO})
                result = await task.result()
                assert result.structured_content is not None
                assert result.structured_content["state"] == "completed"
            async with Client(StreamableHttpTransport(url, auth=token), mode="legacy") as legacy:
                ran = await legacy.call_tool("run", {"site": "fx", "spec": ECHO, "wait_s": 30})
                assert ran.structured_content is not None
                assert ran.structured_content["state"] == "completed"

        asyncio.run(scenario())
    finally:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()

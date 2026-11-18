"""The ``local`` adapter through the MCP server: one ordinary run and one negotiated task."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from fastmcp import Client
from fastmcp_tasks.client import (
    call_tool_task,  # pyright: ignore[reportUnknownVariableType]
)

from clio_relay.surface.server import create_server
from clio_relay.surface.settings import RelayPaths

from .conftest import add_site

pytestmark = pytest.mark.release_platform("posix")

ECHO: dict[str, Any] = {"executable": "echo", "arguments": ["hello-local"]}


def _structured(result: Any) -> dict[str, Any]:
    data = result.structured_content
    assert isinstance(data, dict), result
    return data  # pyright: ignore[reportUnknownVariableType]


def test_local_ordinary_run_and_task(relay_paths: RelayPaths) -> None:
    add_site(relay_paths, "here", adapter="local", config={"scheduler": "none"})

    async def scenario() -> None:
        async with Client(create_server(relay_paths), mode="legacy") as client:
            result = await client.call_tool("run", {"site": "here", "spec": ECHO, "wait_s": 30})
            view = _structured(result)
            assert view["state"] == "completed", view
            assert "hello-local" in view["result"]["stdout_tail"]
        async with Client(create_server(relay_paths), mode="auto") as client:
            task = await call_tool_task(client, "run", {"site": "here", "spec": ECHO})
            result = await task.result()
            assert result.is_error is False
            view = _structured(result)
            assert view["operation_id"] == task.task_id
            assert view["state"] == "completed", view
            assert "hello-local" in view["result"]["stdout_tail"]

    asyncio.run(scenario())

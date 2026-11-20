"""Relay's MCP surface through real MCP clients (LQ41, LQ43).

Every client here is a stock FastMCP client on top of the ``mcp`` SDK, with no
relay-specific code: legacy (initialize handshake), modern without the tasks
extension (multi-round-trip input), and modern with tasks.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import mcp_types
import pytest
from fastmcp import Client
from fastmcp.client.elicitation import ElicitResult
from fastmcp.client.transports import FastMCPTransport
from fastmcp_tasks.client import (
    call_tool_task,  # pyright: ignore[reportUnknownVariableType]
)
from fastmcp_tasks.client_models import (
    ClientGetTaskResult,
    GetTaskRequest,
    GetTaskRequestParams,
    UpdateTaskRequest,
    UpdateTaskRequestParams,
)

from clio_relay.jobs.records import OperationState
from clio_relay.surface.server import OPERATION_META, create_server
from clio_relay.surface.service import RelayService
from clio_relay.surface.settings import RelayPaths

from .conftest import add_site, answer_page

pytestmark = pytest.mark.release_platform("posix")

ECHO: dict[str, Any] = {"executable": "echo", "arguments": ["hello-relay"]}
LONG: dict[str, Any] = {"executable": "sleep", "arguments": ["300"]}


class NoTasksClient(Client[FastMCPTransport]):
    """A modern client that does not opt into the tasks extension."""

    _auto_internal_extensions = False


def _structured(result: Any) -> dict[str, Any]:
    structured = result.structured_content
    assert isinstance(structured, dict), result
    return structured  # pyright: ignore[reportUnknownVariableType]


def _dumped(request: Any) -> dict[str, Any]:
    if isinstance(request, dict):
        return dict(request)  # pyright: ignore[reportUnknownArgumentType]
    return request.model_dump(by_alias=True)


async def _approve(
    message: str, response_type: Any, params: Any, context: Any
) -> dict[str, Any] | ElicitResult[Any]:
    if isinstance(params, mcp_types.ElicitRequestURLParams):
        assert await asyncio.to_thread(answer_page, params.url) == 200
        return ElicitResult(action="accept", content=None)
    return {"approve": True}


async def _decline(message: str, response_type: Any, params: Any, context: Any) -> Any:
    return ElicitResult(action="decline", content=None)


def _ask_answers(paths: RelayPaths) -> list[str | None]:
    service = RelayService(paths)
    return [ask.answered_by for ask in service.asks.all()]


def test_tools_are_the_verbs_and_operations(relay_paths: RelayPaths) -> None:
    async def scenario() -> None:
        for mode in ("auto", "legacy"):
            async with Client(create_server(relay_paths), mode=mode) as client:
                tools = {tool.name: tool for tool in await client.list_tools()}
                assert set(tools) == {
                    "describe",
                    "run",
                    "exec",
                    "files",
                    "serve",
                    "packages",
                    "shutdown",
                }
                for tool in tools.values():
                    listed = tool.model_dump(by_alias=True, mode="json", exclude_none=True)
                    # 2026-07-28 tasks are negotiated per call, not listed; 2025-11-25
                    # core tasks are not served, so no tool may claim them.
                    support = listed.get("execution", {}).get("taskSupport", "forbidden")
                    assert support == "forbidden", (mode, tool.name)

    asyncio.run(scenario())


@pytest.mark.parametrize("mode", ["legacy", "modern"])
def test_ordinary_run_returns_the_finished_operation(relay_paths: RelayPaths, mode: str) -> None:
    add_site(relay_paths)

    async def scenario() -> None:
        server = create_server(relay_paths)
        client: Client[Any] = (
            Client(server, mode="legacy") if mode == "legacy" else NoTasksClient(server)
        )
        async with client:
            result = await client.call_tool("run", {"site": "fx", "spec": ECHO, "wait_s": 30})
            view = _structured(result)
            assert view["state"] == "completed", view
            assert "hello-relay" in view["result"]["stdout_tail"]
            assert result.meta is not None and result.meta[OPERATION_META] == view["operation_id"]
            observed = await client.call_tool("describe", {"job": view["operation_id"]})
            assert _structured(observed)["state"] == "completed"

    asyncio.run(scenario())


def test_ordinary_run_returns_a_handle_when_the_wait_ends(relay_paths: RelayPaths) -> None:
    add_site(relay_paths)

    async def scenario() -> None:
        async with Client(create_server(relay_paths), mode="legacy") as client:
            result = await client.call_tool("run", {"site": "fx", "spec": LONG, "wait_s": 0})
            view = _structured(result)
            assert view["state"] in ("queued", "running")
            cancelled = await client.call_tool("run", {"cancel": view["operation_id"]})
            assert _structured(cancelled)["state"] == "cancelled"

    asyncio.run(scenario())


def test_failed_job_is_an_error_result(relay_paths: RelayPaths) -> None:
    add_site(relay_paths)

    async def scenario() -> None:
        async with Client(create_server(relay_paths), mode="legacy") as client:
            result = await client.call_tool(
                "run", {"site": "fx", "spec": {"executable": "false"}}, raise_on_error=False
            )
            assert result.is_error
            assert _structured(result)["state"] == "failed"

    asyncio.run(scenario())


@pytest.mark.usefixtures("stub_guard")
def test_refusals_are_typed(relay_paths: RelayPaths) -> None:
    add_site(relay_paths, "guarded", policy="guarded")

    async def scenario() -> None:
        async with Client(create_server(relay_paths), mode="legacy") as client:
            missing = await client.call_tool(
                "run", {"site": "nope", "spec": ECHO}, raise_on_error=False
            )
            assert missing.is_error
            assert _structured(missing)["refusal"]["reason"] == "site_not_found"
            guarded = await client.call_tool(
                "run", {"site": "guarded", "spec": ECHO}, raise_on_error=False
            )
            assert _structured(guarded)["refusal"]["reason"] == "guard_unavailable"
            served = await client.call_tool(
                "serve",
                {"site": "guarded", "spec": {"job": ECHO, "ports": [{"name": "m", "port": 1}]}},
                raise_on_error=False,
            )
            assert _structured(served)["refusal"]["reason"] == "verb_not_implemented"

    asyncio.run(scenario())


def test_task_run_completes(relay_paths: RelayPaths) -> None:
    add_site(relay_paths)

    async def scenario() -> None:
        async with Client(create_server(relay_paths), mode="auto") as client:
            task = await call_tool_task(client, "run", {"site": "fx", "spec": ECHO})
            record = RelayService(relay_paths).operation(task.task_id)
            assert record.task
            result = await task.result()
            assert result.is_error is False
            view = _structured(result)
            assert view["operation_id"] == task.task_id
            assert view["state"] == "completed"
            assert (await task.status()).status == "completed"

    asyncio.run(scenario())


def test_task_asks_with_input_required_and_takes_tasks_update(relay_paths: RelayPaths) -> None:
    add_site(relay_paths, spend="always")

    async def scenario() -> None:
        async with Client(create_server(relay_paths), mode="auto") as client:
            task = await call_tool_task(client, "run", {"site": "fx", "spec": ECHO})
            parked = await task.status()
            assert parked.status == "input_required"
            assert parked.input_requests is not None
            (ask_id,) = parked.input_requests
            request = parked.input_requests[ask_id]
            dumped = _dumped(request)
            assert dumped["params"]["requestedSchema"]["properties"]["approve"]["type"] == (
                "boolean"
            )
            await client.session.send_request(
                UpdateTaskRequest(
                    params=UpdateTaskRequestParams(
                        task_id=task.task_id,
                        input_responses={
                            ask_id: {"action": "accept", "content": {"approve": True}}
                        },
                    )
                ),
                mcp_types.Result,
            )
            result = await task.result()
            assert _structured(result)["state"] == "completed"

    asyncio.run(scenario())
    assert _ask_answers(relay_paths) == ["tasks/update"]


def test_task_decline_cancels(relay_paths: RelayPaths) -> None:
    add_site(relay_paths, spend="always")

    async def scenario() -> None:
        async with Client(create_server(relay_paths), mode="auto") as client:
            task = await call_tool_task(client, "run", {"site": "fx", "spec": ECHO})
            parked = await task.status()
            assert parked.input_requests is not None
            (ask_id,) = parked.input_requests
            await client.session.send_request(
                UpdateTaskRequest(
                    params=UpdateTaskRequestParams(
                        task_id=task.task_id, input_responses={ask_id: {"action": "decline"}}
                    )
                ),
                mcp_types.Result,
            )
            assert (await task.status()).status == "cancelled"

    asyncio.run(scenario())


def test_task_cancel(relay_paths: RelayPaths) -> None:
    add_site(relay_paths)

    async def scenario() -> None:
        async with Client(create_server(relay_paths), mode="auto") as client:
            task = await call_tool_task(client, "run", {"site": "fx", "spec": LONG})
            assert (await task.status()).status == "working"
            await task.cancel()
            assert (await task.status()).status == "cancelled"
        record = RelayService(relay_paths).operation(task.task_id)
        assert record.state == OperationState.CANCELLED

    asyncio.run(scenario())


def test_task_outlives_the_server(relay_paths: RelayPaths) -> None:
    add_site(relay_paths, config={"queue_s": 0.5, "run_s": 1.0})

    async def scenario() -> None:
        async with Client(create_server(relay_paths), mode="auto") as client:
            task = await call_tool_task(client, "run", {"site": "fx", "spec": ECHO})
            task_id = task.task_id
        async with Client(create_server(relay_paths), mode="auto") as client:
            status = "working"
            for _ in range(100):
                got = await client.session.send_request(
                    GetTaskRequest(params=GetTaskRequestParams(task_id=task_id)),
                    ClientGetTaskResult,
                )
                status = got.status
                if status == "completed":
                    break
                await asyncio.sleep(0.1)
            assert status == "completed"

    asyncio.run(scenario())


@pytest.mark.parametrize("mode", ["legacy", "modern"])
def test_form_elicitation_approves(relay_paths: RelayPaths, mode: str) -> None:
    add_site(relay_paths, spend="always")

    async def scenario() -> None:
        server = create_server(relay_paths)
        client: Client[Any] = (
            Client(server, mode="legacy", elicitation_handler=_approve)
            if mode == "legacy"
            else NoTasksClient(server, elicitation_handler=_approve)
        )
        async with client:
            result = await client.call_tool("run", {"site": "fx", "spec": ECHO, "wait_s": 30})
            assert _structured(result)["state"] == "completed"

    asyncio.run(scenario())
    expected = "elicitation" if mode == "legacy" else "input_required"
    assert _ask_answers(relay_paths) == [expected]


@pytest.mark.parametrize("mode", ["legacy", "modern"])
def test_form_elicitation_decline_cancels(relay_paths: RelayPaths, mode: str) -> None:
    add_site(relay_paths, spend="always")

    async def scenario() -> None:
        server = create_server(relay_paths)
        client: Client[Any] = (
            Client(server, mode="legacy", elicitation_handler=_decline)
            if mode == "legacy"
            else NoTasksClient(server, elicitation_handler=_decline)
        )
        async with client:
            result = await client.call_tool("run", {"site": "fx", "spec": ECHO})
            view = _structured(result)
            assert view["state"] == "cancelled"
            assert view["error"]["reason"] == "declined"

    asyncio.run(scenario())


@pytest.mark.parametrize("mode", ["legacy", "modern"])
def test_url_elicitation_signs_in(relay_paths: RelayPaths, mode: str) -> None:
    add_site(relay_paths, config={"queue_s": 0.1, "run_s": 0.2, "sign_in": True})

    async def scenario() -> None:
        server = create_server(relay_paths)
        client: Client[Any] = (
            Client(server, mode="legacy", elicitation_handler=_approve)
            if mode == "legacy"
            else NoTasksClient(server, elicitation_handler=_approve)
        )
        async with client:
            result = await client.call_tool("run", {"site": "fx", "spec": ECHO, "wait_s": 30})
            assert _structured(result)["state"] == "completed"

    asyncio.run(scenario())
    signed = Path(relay_paths.adapter_dir("fx", "fixture")) / "signed_in"
    assert signed.is_file()


def test_task_url_sign_in(relay_paths: RelayPaths) -> None:
    add_site(relay_paths, config={"queue_s": 0.1, "run_s": 0.2, "sign_in": True})

    async def scenario() -> None:
        async with Client(create_server(relay_paths), mode="auto") as client:
            task = await call_tool_task(client, "run", {"site": "fx", "spec": ECHO})
            parked = await task.status()
            assert parked.status == "input_required"
            assert parked.input_requests is not None
            (ask_id,) = parked.input_requests
            request = parked.input_requests[ask_id]
            dumped = _dumped(request)
            assert dumped["params"]["mode"] == "url"
            assert await asyncio.to_thread(answer_page, dumped["params"]["url"]) == 200
            await client.session.send_request(
                UpdateTaskRequest(
                    params=UpdateTaskRequestParams(
                        task_id=task.task_id, input_responses={ask_id: {"action": "accept"}}
                    )
                ),
                mcp_types.Result,
            )
            assert _structured(await task.result())["state"] == "completed"

    asyncio.run(scenario())


def test_client_without_elicitation_gets_a_typed_refusal_and_a_page(
    relay_paths: RelayPaths,
) -> None:
    add_site(relay_paths, spend="always")

    async def scenario() -> None:
        async with Client(create_server(relay_paths), mode="legacy") as client:
            result = await client.call_tool(
                "run", {"site": "fx", "spec": ECHO, "wait_s": 0}, raise_on_error=False
            )
            assert result.is_error
            refusal = _structured(result)["refusal"]
            assert refusal["reason"] == "needs_approval"
            (ask,) = refusal["data"]["asks"]
            assert ask["page"].startswith("http://127.0.0.1:")
            assert await asyncio.to_thread(answer_page, ask["page"]) == 200
            operation_id = refusal["data"]["operation_id"]
            changed = {**ECHO, "arguments": ["something-else"]}
            mismatch = await client.call_tool(
                "run",
                {"resume": operation_id, "site": "fx", "spec": changed, "wait_s": 0},
                raise_on_error=False,
            )
            assert mismatch.is_error
            assert _structured(mismatch)["refusal"]["reason"] == "approval_mismatch"
            resumed = await client.call_tool(
                "run", {"resume": operation_id, "site": "fx", "spec": ECHO, "wait_s": 30}
            )
            assert _structured(resumed)["state"] == "completed"

    asyncio.run(scenario())


def test_exec_without_a_limit_reports_quiet_stretches(relay_paths: RelayPaths) -> None:
    add_site(relay_paths)
    notes: list[str] = []

    async def progress(progress: float, total: float | None, message: str | None) -> None:
        notes.append(message or "")

    async def scenario() -> None:
        server = create_server(relay_paths)
        async with Client(server, mode="legacy") as client:
            schema = next(t for t in await client.list_tools() if t.name == "exec").input_schema
            assert "timeout_s" not in schema.get("required", [])
            ran = await client.call_tool(
                "exec", {"site": "fx", "command": ["quiet"]}, progress_handler=progress
            )
            assert _structured(ran)["exit_code"] == 0
            assert not _structured(ran)["timed_out"]

    asyncio.run(scenario())
    assert any("still running" in note and "cancel" in note for note in notes), notes


def test_describe(relay_paths: RelayPaths) -> None:
    add_site(relay_paths)

    async def scenario() -> None:
        async with Client(create_server(relay_paths), mode="auto") as client:
            listed = _structured(await client.call_tool("describe", {}))
            assert [site["id"] for site in listed["sites"]] == ["fx"]
            assert "fixture" in listed["adapters"]
            site = _structured(await client.call_tool("describe", {"site": "fx"}))
            assert site["info"]["adapter"] == "fixture"

    asyncio.run(scenario())

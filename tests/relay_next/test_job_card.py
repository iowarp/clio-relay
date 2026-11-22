"""A tool that confirms itself asks once, with the job's card; the card rides every carrier."""

from __future__ import annotations

import asyncio
from typing import Any

import mcp_types
import pytest
from fastmcp import Client
from fastmcp.client.elicitation import ElicitResult
from fastmcp.client.transports import FastMCPTransport
from fastmcp_tasks.client import (
    call_tool_task,  # pyright: ignore[reportUnknownVariableType]
)

from clio_relay.jobs.records import OperationRecord, OperationState
from clio_relay.jobs.spec import JobSpec
from clio_relay.surface import confirm, held_services, job_card
from clio_relay.surface.server import RelayServer, create_server
from clio_relay.surface.service import RelayService
from clio_relay.surface.settings import RelayPaths

from .conftest import add_site

ECHO: dict[str, Any] = {"executable": "echo", "arguments": ["hello-relay"]}
LONG: dict[str, Any] = {"executable": "sleep", "arguments": ["300"]}
FLAG = {job_card.CONFIRM_META: job_card.CONFIRM_SERVER}


class NoTasksClient(Client[FastMCPTransport]):
    """A modern client that does not opt into the tasks extension."""

    _auto_internal_extensions = False


class Person:
    """An elicitation handler that keeps what it was shown and answers each the same way."""

    def __init__(self, answer: dict[str, Any] | None = None, action: str = "accept") -> None:
        self.shown: list[Any] = []
        self.answer = answer if answer is not None else {"approve": "approve"}
        self.action = action

    async def __call__(self, message: str, response_type: Any, params: Any, context: Any) -> Any:
        self.shown.append(params)
        if self.action != "accept":
            return ElicitResult(action=self.action, content=None)  # pyright: ignore[reportArgumentType]
        return self.answer


def _structured(result: Any) -> dict[str, Any]:
    structured = result.structured_content
    assert isinstance(structured, dict), result
    return structured  # pyright: ignore[reportUnknownVariableType]


def _client(server: Any, mode: str, person: Person | None) -> Client[Any]:
    if mode == "legacy":
        return Client(server, mode="legacy", elicitation_handler=person)
    return NoTasksClient(server, elicitation_handler=person)


def _the_card(person: Person) -> Any:
    (params,) = person.shown
    schema = params.requested_schema
    assert [item["const"] for item in schema["properties"]["approve"]["oneOf"]] == [
        "approve",
        "decline",
    ]
    return params


@pytest.fixture
def rounds(monkeypatch: pytest.MonkeyPatch) -> list[mcp_types.InputRequiredResult]:
    """Every modern input request the client received, as it parsed it off the wire."""
    import fastmcp.client.client as mcp_client

    seen: list[mcp_types.InputRequiredResult] = []
    driver: Any = vars(mcp_client)["run_input_required_driver"]

    async def recording(first: mcp_types.InputRequiredResult, *args: Any, **kwargs: Any) -> Any:
        seen.append(first)
        return await driver(first, *args, **kwargs)

    monkeypatch.setattr(mcp_client, "run_input_required_driver", recording)
    return seen


def _job_on(params: Any, mode: str, seen: list[mcp_types.InputRequiredResult]) -> Any:
    """The card's job, from the elicitation's ``_meta`` or, modern, the result's."""
    if mode == "legacy":
        shown: dict[str, Any] = dict(params.meta or {})
        return shown[job_card.JOB_META]
    (first,) = seen
    assert params.meta is None
    meta: dict[str, Any] = dict(first.meta or {})
    found: Any = meta[job_card.JOB_META]
    return found


def test_the_confirming_tools_say_so(relay_paths: RelayPaths) -> None:
    async def scenario() -> None:
        async with Client(create_server(relay_paths), mode="auto") as client:
            tools = {tool.name: tool for tool in await client.list_tools()}
        for name, tool in tools.items():
            declared = (tool.meta or {}).get(job_card.CONFIRM_META)
            expected = job_card.CONFIRM_TOOL if name in ("run", "serve", "exec") else None
            assert declared == expected, name

    asyncio.run(scenario())


@pytest.mark.parametrize("mode", ["legacy", "modern"])
@pytest.mark.parametrize("spend", ["never", "always"])
def test_a_flagged_run_asks_once_with_its_card(
    relay_paths: RelayPaths, mode: str, spend: str, rounds: list[mcp_types.InputRequiredResult]
) -> None:
    add_site(relay_paths, spend=spend)
    person = Person()

    async def scenario() -> None:
        async with _client(create_server(relay_paths), mode, person) as client:
            result = await client.call_tool(
                "run", {"site": "fx", "spec": ECHO, "wait_s": 30}, meta=FLAG
            )
            view = _structured(result)
            assert view["state"] == "completed", view
            assert view["job"]["phase"] == "ended"
            assert view["job"]["outcome"] == "succeeded"

    asyncio.run(scenario())
    params = _the_card(person)
    assert "left this call's confirm to relay" in params.message
    card = _job_on(params, mode, rounds)
    assert card["phase"] == "planned"
    assert card["site"] == {"id": "fx", "label": "fx"}
    assert [c["id"] for c in card["components"]] == ["job"]
    assert card["components"][0]["state"] == "planned"
    asks = RelayService(relay_paths).asks.all()
    assert [ask.binding["kind"] for ask in asks] == [confirm.CARD_KIND]


def test_an_unflagged_run_asks_as_before(relay_paths: RelayPaths) -> None:
    add_site(relay_paths)
    person = Person()

    async def scenario() -> None:
        async with _client(create_server(relay_paths), "legacy", person) as client:
            result = await client.call_tool("run", {"site": "fx", "spec": ECHO, "wait_s": 30})
            assert _structured(result)["state"] == "completed"
            assert job_card.CONFIRM_META not in (result.meta or {})

    asyncio.run(scenario())
    assert person.shown == []


def test_a_declined_card_cancels_the_job(relay_paths: RelayPaths) -> None:
    add_site(relay_paths)
    person = Person(action="decline")

    async def scenario() -> None:
        async with _client(create_server(relay_paths), "legacy", person) as client:
            result = await client.call_tool("run", {"site": "fx", "spec": ECHO}, meta=FLAG)
            view = _structured(result)
            assert view["state"] == "cancelled"
            assert view["error"]["reason"] == "declined"

    asyncio.run(scenario())


def test_a_flagged_run_without_forms_is_refused_and_nothing_runs(relay_paths: RelayPaths) -> None:
    add_site(relay_paths)

    async def scenario() -> None:
        async with Client(create_server(relay_paths), mode="legacy") as client:
            result = await client.call_tool(
                "run", {"site": "fx", "spec": ECHO}, meta=FLAG, raise_on_error=False
            )
            assert result.is_error
            refusal = _structured(result)["refusal"]
            assert refusal["reason"] == "needs_approval"
            operation_id = refusal["data"]["operation_id"]
            again = await client.call_tool(
                "run",
                {"resume": operation_id, "site": "fx", "spec": ECHO, "wait_s": 0},
                meta=FLAG,
                raise_on_error=False,
            )
            assert again.is_error
            assert _structured(again)["refusal"]["reason"] == "needs_approval"

    asyncio.run(scenario())
    service = RelayService(relay_paths)
    (record,) = service.operations.all()
    assert record.state == OperationState.AWAITING_INPUT
    assert record.handle is None


def test_a_flagged_resume_after_the_answer_says_no_confirm_was_needed(
    relay_paths: RelayPaths,
) -> None:
    add_site(relay_paths)

    async def scenario() -> None:
        async with Client(create_server(relay_paths), mode="legacy") as client:
            result = await client.call_tool(
                "run", {"site": "fx", "spec": ECHO}, meta=FLAG, raise_on_error=False
            )
            refusal = _structured(result)["refusal"]
            (shown,) = refusal["data"]["asks"]
            RelayService(relay_paths).answer(
                shown["ask_id"], "accept", {"approve": "approve"}, by="test"
            )
            resumed = await client.call_tool(
                "run",
                {
                    "resume": refusal["data"]["operation_id"],
                    "site": "fx",
                    "spec": ECHO,
                    "wait_s": 30,
                },
                meta=FLAG,
            )
            assert _structured(resumed)["state"] == "completed"
            assert (resumed.meta or {})[job_card.CONFIRM_META] == job_card.CONFIRM_NOT_NEEDED

    asyncio.run(scenario())


@pytest.mark.parametrize("answer", ["approve", "decline"])
def test_a_flagged_exec_asks_once_for_that_command(relay_paths: RelayPaths, answer: str) -> None:
    add_site(relay_paths)
    person = Person({"approve": answer})

    async def scenario() -> None:
        async with _client(create_server(relay_paths), "modern", person) as client:
            result = await client.call_tool(
                "exec", {"site": "fx", "command": ["echo", "hi"]}, meta=FLAG, raise_on_error=False
            )
            view = _structured(result)
            if answer == "approve":
                assert view["stdout"] == "hi\n"
                assert view["ask_id"]
            else:
                assert view["refusal"]["reason"] == "policy_refused"
            plain = await client.call_tool("exec", {"site": "fx", "command": ["echo", "hi"]})
            assert _structured(plain)["stdout"] == "hi\n"

    asyncio.run(scenario())
    params = _the_card(person)
    assert "echo hi" in params.message
    (ask,) = RelayService(relay_paths).asks.all()
    assert ask.binding["kind"] == confirm.EXEC_CONFIRM_KIND
    assert ask.completed_at is not None


@pytest.mark.parametrize("mode", ["legacy", "modern"])
def test_a_flagged_cancel_asks_once_with_the_card(
    relay_paths: RelayPaths, mode: str, rounds: list[mcp_types.InputRequiredResult]
) -> None:
    add_site(relay_paths)
    person = Person()

    async def scenario() -> None:
        async with _client(create_server(relay_paths), mode, person) as client:
            started = await client.call_tool("run", {"site": "fx", "spec": LONG, "wait_s": 0})
            operation_id = _structured(started)["operation_id"]
            assert person.shown == []
            cancelled = await client.call_tool("run", {"cancel": operation_id}, meta=FLAG)
            assert _structured(cancelled)["state"] == "cancelled"
            again = await client.call_tool("run", {"cancel": operation_id}, meta=FLAG)
            assert (again.meta or {})[job_card.CONFIRM_META] == job_card.CONFIRM_NOT_NEEDED

    asyncio.run(scenario())
    params = _the_card(person)
    assert params.message.startswith("Cancel op_")
    assert _job_on(params, mode, rounds)["phase"] in ("queued", "running")


def test_the_card_folds_the_running_services_choice(relay_paths: RelayPaths) -> None:
    add_site(relay_paths, config={"queue_s": 0.1, "run_s": 0.1})

    async def scenario() -> None:
        service = RelayService(relay_paths)
        pilot = OperationRecord(
            operation_id="op_pilot",
            kind="serve",
            site="fx",
            adapter="fixture",
            spec={"job": {"executable": "pvserver"}},
            spec_digest="d",
            state=OperationState.RUNNING,
        )
        service.operations.put(pilot.operation_id, pilot)
        cancelled: list[str] = []

        async def _cancel(operation_id: str) -> OperationRecord:
            cancelled.append(operation_id)
            record = service.operation(operation_id)
            record.move(OperationState.CANCELLED)
            return record

        service.cancel = _cancel  # type: ignore[method-assign]
        record = await service.start_run("fx", JobSpec.model_validate(ECHO), confirm=True)
        (ask,) = service.outstanding(record)
        assert "op_pilot" in ask.message
        schema = ask.requested_schema or {}
        assert schema["properties"][held_services.FIELD]["default"] == held_services.KEEP
        assert held_services.ask_ids(service.operation(record.operation_id)) == set()
        content = {"approve": "approve", held_services.FIELD: held_services.END}
        service.answer(ask.ask_id, "accept", content, by="test")
        record = await service.advance(record.operation_id)
        assert record.state not in (OperationState.AWAITING_INPUT, OperationState.FAILED)
        assert cancelled == ["op_pilot"]
        await service.close()

    asyncio.run(scenario())


def test_task_status_and_site_report_carry_the_card(relay_paths: RelayPaths) -> None:
    add_site(relay_paths)

    async def scenario() -> None:
        async with Client(create_server(relay_paths), mode="auto") as client:
            task = await call_tool_task(client, "run", {"site": "fx", "spec": ECHO})
            await task.result()
            described = await client.call_tool("describe", {"site": "fx"})
            (listed,) = _structured(described)["jobs"]
            assert listed["operation_id"] == task.task_id
        service = RelayService(relay_paths)
        status = await RelayServer(service).tasks.task_result(service.operation(task.task_id))
        dumped = status.model_dump(by_alias=True, mode="json", exclude_none=True)
        card = dumped["_meta"][job_card.JOB_META]
        assert card["operation_id"] == task.task_id
        assert card["phase"] == "ended"
        assert card["revision"] > 0

    asyncio.run(scenario())


def test_the_snapshot_follows_the_job(relay_paths: RelayPaths) -> None:
    add_site(relay_paths, config={"queue_s": 0.3, "run_s": 0.3})

    async def scenario() -> None:
        service = RelayService(relay_paths)
        record = await service.start_run("fx", JobSpec.model_validate(ECHO))
        phases = [job_card.snapshot(service, record)]
        for _ in range(40):
            record = await service.refresh(record.operation_id)
            phases.append(job_card.snapshot(service, record))
            if record.state.final:
                break
            await asyncio.sleep(0.05)
        seen = [card["phase"] for card in phases if card]
        assert seen[0] == "queued" and seen[-1] == "ended"
        assert "running" in seen
        revisions = [card["revision"] for card in phases if card]
        assert revisions == sorted(revisions)
        await service.close()

    asyncio.run(scenario())
    _ = mcp_types

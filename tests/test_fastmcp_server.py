from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any, BinaryIO, cast

import mcp_types
import pytest
from fastmcp import Client, FastMCP
from fastmcp.client.transports import FastMCPTransport
from fastmcp.server.dependencies import get_context
from fastmcp.tools import InputRequiredToolResult, ToolResult
from fastmcp.utilities.tests import asgi_server
from fastmcp_tasks.client import (
    ToolTask,
    call_tool_task,  # pyright: ignore[reportUnknownVariableType]
)
from fastmcp_tasks.client_models import (
    ClientGetTaskResult,
    GetTaskRequest,
    GetTaskRequestParams,
    UpdateTaskRequest,
    UpdateTaskRequestParams,
)
from fastmcp_tasks.models import MISSING_REQUIRED_CLIENT_CAPABILITY
from mcp.shared.exceptions import MCPError

import clio_relay.fastmcp_server as fastmcp_server_module
from clio_relay import door_errors
from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue, LegacyQueueStateError
from clio_relay.errors import McpTaskIdentityConflictError, NotFoundError, QueueConflictError
from clio_relay.fastmcp_server import (
    MAX_TASK_ARGUMENT_BYTES,
    RelayMcpRuntime,
    RelayTasksExtension,
    RelayTool,
    create_fastmcp_server,
)
from clio_relay.mcp_server import mcp_tool_definitions_and_remote_catalog
from clio_relay.models import (
    JobKind,
    JobState,
    McpCallSpec,
    RelayJob,
    RelayMcpInputRound,
    RelayMcpTaskProjection,
    RelayMcpTaskRecord,
    RemoteAgentTaskSpec,
)
from clio_relay.remote_mcp import VirtualRemoteMcpCatalog

JSON = dict[str, Any]


class _NoInternalExtensionsClient(Client[FastMCPTransport]):
    """FastMCP client variant that deliberately omits built-in extensions."""

    _auto_internal_extensions = False


def _task_server(
    settings: RelaySettings,
    queue: ClioCoreQueue,
) -> FastMCP[dict[str, Any]]:
    runtime = RelayMcpRuntime(settings=settings, profile="user", queue=queue)
    definitions, _catalog = mcp_tool_definitions_and_remote_catalog(profile="user")
    definition = next(item for item in definitions if item["name"] == "relay_submit_agent")
    tool = RelayTool(
        definition,
        runtime=runtime,
        catalog_revision=None,
        task_capable=True,
    )
    server: FastMCP[dict[str, Any]] = FastMCP(
        "relay-task-test",
        tools=[tool],
        lifespan=runtime.lifespan,
        tasks=False,
        strict_input_validation=True,
    )
    server.add_extension(RelayTasksExtension(runtime))
    return server


def _submit_arguments(tmp_path: Path, suffix: str) -> dict[str, object]:
    return {
        "cluster": "test-cluster",
        "prompt_path": str(tmp_path / f"prompt-{suffix}.md"),
        "timeout_seconds": 45,
        "idempotency_key": f"fastmcp-task-{suffix}",
    }


def _elicitation_request(message: str) -> mcp_types.ElicitRequest:
    return mcp_types.ElicitRequest(
        params=mcp_types.ElicitRequestFormParams(
            message=message,
            requested_schema={
                "type": "object",
                "properties": {"approved": {"type": "boolean"}},
                "required": ["approved"],
                "additionalProperties": False,
            },
        )
    )


def _elicitation_document(message: str) -> dict[str, Any]:
    return _elicitation_request(message).model_dump(
        by_alias=True,
        mode="json",
        exclude_none=True,
    )


def _put_input_round(
    queue: ClioCoreQueue,
    task_id: str,
    *,
    keys: tuple[str, ...],
) -> None:
    record = queue.get_mcp_task(task_id)
    outstanding = {key: _elicitation_document(f"Approve {key}?") for key in keys}
    projection = RelayMcpTaskProjection.model_validate(
        {
            **record.projection.model_dump(mode="python"),
            "issued_input_keys": list(keys),
            "input_round": RelayMcpInputRound(
                leg=1,
                outstanding=outstanding,
                request_state="relay-input-round-v1",
            ),
        },
    )
    queue.update_mcp_task_projection(
        task_id,
        projection,
        expected_updated_at=record.updated_at,
    )


class _GuardedRelayTool(RelayTool):
    """Test-only relay tool proving a guard round precedes task creation."""

    def __init__(
        self,
        definition: JSON,
        *,
        runtime: RelayMcpRuntime,
        catalog_revision: str | None,
        task_capable: bool,
    ) -> None:
        super().__init__(
            definition,
            runtime=runtime,
            catalog_revision=catalog_revision,
            task_capable=task_capable,
        )

    async def run(self, arguments: dict[str, Any]) -> ToolResult:
        context = get_context()
        responses = context.input_responses
        if responses is None:
            return InputRequiredToolResult(
                input_required=mcp_types.InputRequiredResult(
                    input_requests={"approval": _elicitation_request("Approve submission?")},
                    request_state="guarded-submit-v1",
                )
            )
        approval = responses.get("approval")
        if not isinstance(approval, mcp_types.ElicitResult):
            raise ValueError("guarded relay submission omitted its elicitation response")
        if approval.action != "accept" or approval.content != {"approved": True}:
            raise ValueError("guarded relay submission was not approved")
        if context.request_state != "guarded-submit-v1":
            raise ValueError("guarded relay submission lost its request state")
        return await super().run(arguments)


def _guarded_task_server(
    settings: RelaySettings,
    queue: ClioCoreQueue,
) -> FastMCP[dict[str, Any]]:
    runtime = RelayMcpRuntime(settings=settings, profile="user", queue=queue)
    definitions, _catalog = mcp_tool_definitions_and_remote_catalog(profile="user")
    definition = next(item for item in definitions if item["name"] == "relay_submit_agent")
    server: FastMCP[dict[str, Any]] = FastMCP(
        "relay-guarded-task-test",
        tools=[
            _GuardedRelayTool(
                definition,
                runtime=runtime,
                catalog_revision=None,
                task_capable=True,
            )
        ],
        lifespan=runtime.lifespan,
        tasks=False,
        strict_input_validation=True,
    )
    server.add_extension(RelayTasksExtension(runtime))
    return server


def test_fastmcp_factory_advertises_tasks_and_preserves_user_catalog(
    tmp_path: Path,
) -> None:
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
    )
    queue = ClioCoreQueue(settings.core_dir)

    async def scenario() -> None:
        server = create_fastmcp_server(settings=settings, queue=queue)
        async with Client(server, mode="auto") as client:
            tools = await client.list_tools()
            tool_names = {tool.name for tool in tools}
            assert "relay_submit_agent" in tool_names
            assert "relay_status" in tool_names
            capabilities = client.session.server_capabilities
            assert capabilities is not None
            extensions = capabilities.extensions or {}
            assert "io.modelcontextprotocol/tasks" in extensions

    asyncio.run(scenario())


def test_fastmcp_provider_exposes_dynamic_catalog_revision(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
    )
    queue = ClioCoreQueue(settings.core_dir)
    definitions, catalog = mcp_tool_definitions_and_remote_catalog(profile="user")
    revision = "a" * 64
    dynamic_definition: JSON = {
        "name": "remote_demo_echo",
        "description": "One test-only dynamically discovered remote tool.",
        "inputSchema": {
            "type": "object",
            "properties": {"cluster": {"type": "string"}},
            "required": ["cluster"],
            "additionalProperties": False,
        },
        "outputSchema": {
            "type": "object",
            "additionalProperties": True,
        },
    }

    def dynamic_catalog(*, profile: str) -> tuple[list[JSON], VirtualRemoteMcpCatalog]:
        assert profile == "user"
        return (
            [*definitions, dynamic_definition],
            VirtualRemoteMcpCatalog(
                revision=revision,
                tools={},
                issues=catalog.issues,
            ),
        )

    monkeypatch.setattr(
        fastmcp_server_module,
        "mcp_tool_definitions_and_remote_catalog",
        dynamic_catalog,
    )

    async def scenario() -> None:
        server = create_fastmcp_server(settings=settings, queue=queue)
        async with Client(server, mode="auto") as client:
            tools = await client.list_tools()
        dynamic = next(tool for tool in tools if tool.name == "remote_demo_echo")
        assert dynamic.meta is not None
        assert dynamic.meta["clio-relay/catalog-revision"] == revision

    asyncio.run(scenario())


def test_fastmcp_factory_tasks_virtual_jarvis_tool(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
    )
    queue = ClioCoreQueue(settings.core_dir)
    definitions, catalog = mcp_tool_definitions_and_remote_catalog(profile="user")
    submit_definition = next(item for item in definitions if item["name"] == "relay_submit_agent")
    virtual_definition = {**submit_definition, "name": "jarvis_describe"}
    original_call = fastmcp_server_module.call_mcp_tool

    def virtual_catalog(*, profile: str) -> tuple[list[JSON], VirtualRemoteMcpCatalog]:
        assert profile == "user"
        return (
            [virtual_definition],
            VirtualRemoteMcpCatalog(
                revision="b" * 64,
                tools={},
                issues=catalog.issues,
            ),
        )

    def virtual_call(params: JSON, **kwargs: Any) -> JSON:
        assert params["name"] == "jarvis_describe"
        return original_call(
            {
                "name": "relay_submit_agent",
                "arguments": params.get("arguments", {}),
            },
            **kwargs,
        )

    monkeypatch.setattr(
        fastmcp_server_module,
        "mcp_tool_definitions_and_remote_catalog",
        virtual_catalog,
    )
    monkeypatch.setattr(fastmcp_server_module, "call_mcp_tool", virtual_call)

    async def scenario() -> None:
        server = create_fastmcp_server(settings=settings, queue=queue)
        component = await server.get_tool("jarvis_describe")
        assert component is not None
        assert component.task_config.mode == "optional"
        async with Client(
            server,
            mode="auto",
        ) as client:
            task = await call_tool_task(
                client,
                "jarvis_describe",
                _submit_arguments(tmp_path, "factory-virtual"),
            )
            assert task.task_id == queue.list_jobs()[0].job_id

    asyncio.run(scenario())


def test_official_fastmcp_tool_task_projects_job_and_survives_reopen(
    tmp_path: Path,
) -> None:
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
    )
    queue = ClioCoreQueue(settings.core_dir)

    async def scenario() -> None:
        async with Client(_task_server(settings, queue), mode="auto") as client:
            task = await call_tool_task(
                client,
                "relay_submit_agent",
                _submit_arguments(tmp_path, "complete"),
            )
            job = queue.get_job(task.task_id)
            assert task.task_id == job.job_id
            assert queue.get_mcp_task(task.task_id).projection.tool_name == ("relay_submit_agent")
            assert (await task.status()).status == "working"

            reopened = ClioCoreQueue(settings.core_dir)
            restored = reopened.get_mcp_task(task.task_id)
            assert restored.task_id == job.job_id
            assert restored.projection.arguments["idempotency_key"] == ("fastmcp-task-complete")

            queue.update_job_state(job.job_id, JobState.SUCCEEDED)
            result = await task.result()
            assert result.is_error is False
            assert result.structured_content is not None
            assert result.structured_content["job"]["job_id"] == job.job_id
            assert (await task.status()).status == "completed"

    asyncio.run(scenario())


def test_agent_task_parks_post_admission_input_and_resumes_with_answer(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def create_test_directory(path: Path) -> None:
        path.mkdir(parents=True, exist_ok=True)

    def accept_test_path(path: Path, *, directory: bool) -> None:
        if directory:
            create_test_directory(path)

    for module_name in (
        "clio_relay.cluster_config",
        "clio_relay.core_queue",
        "clio_relay.worker_lifetime_lock",
    ):
        monkeypatch.setattr(
            f"{module_name}.ensure_private_configuration_directory",
            create_test_directory,
        )
        monkeypatch.setattr(
            f"{module_name}.ensure_private_configuration_path",
            accept_test_path,
        )

    def _open_atomic(path: Path) -> BinaryIO:
        return path.open("xb")

    monkeypatch.setattr(
        "clio_relay.queue_store_write.cluster_config.open_private_atomic_file",
        _open_atomic,
    )

    def _no_cluster(_cluster: str) -> None:
        return None

    monkeypatch.setattr(
        "clio_relay.mcp_server._optional_cluster_definition",
        _no_cluster,
    )
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
    )
    queue = ClioCoreQueue(settings.core_dir)

    async def update_task(
        client: Client[FastMCPTransport],
        task_id: str,
        responses: dict[str, Any],
    ) -> None:
        await client.session.send_request(
            UpdateTaskRequest(
                params=UpdateTaskRequestParams(
                    task_id=task_id,
                    input_responses=responses,
                )
            ),
            mcp_types.Result,
        )

    async def scenario() -> None:
        async with Client(
            create_fastmcp_server(settings=settings, queue=queue),
            mode="auto",
        ) as client:
            task = await call_tool_task(
                client,
                "relay_submit_agent",
                {
                    **_submit_arguments(tmp_path, "post-admission-input"),
                    "request_followup_message": True,
                },
            )
            assert queue.get_job(task.task_id).job_id == task.task_id

            parked = await task.status()
            assert parked.status == "input_required"
            assert parked.input_requests is not None
            assert set(parked.input_requests) == {"agent_message"}

            accepted = {
                "action": "accept",
                "content": {"message": "Use the new boundary condition."},
            }
            update_projection = queue.update_mcp_task_projection
            cas_attempts = 0

            def conflict_once(*args: Any, **kwargs: Any) -> RelayMcpTaskRecord:
                nonlocal cas_attempts
                cas_attempts += 1
                if cas_attempts == 1:
                    raise QueueConflictError("forced acceptance CAS conflict")
                return update_projection(*args, **kwargs)

            monkeypatch.setattr(queue, "update_mcp_task_projection", conflict_once)
            await update_task(client, task.task_id, {"agent_message": accepted})
            assert cas_attempts == 2

            resumed = await task.status()
            assert resumed.status == "working"
            persisted = queue.get_mcp_task(task.task_id)
            input_round = persisted.projection.input_round
            assert input_round is not None
            assert input_round.leg == 2
            assert input_round.outstanding == {}
            assert input_round.answered == {"agent_message": accepted}
            assert input_round.request_state is None

            consumed_at = persisted.updated_at
            await update_task(
                client,
                task.task_id,
                {
                    "agent_message": {
                        "action": "accept",
                        "content": {"message": "duplicate must not replace the answer"},
                    },
                    "unknown": {"action": "decline"},
                },
            )
            replayed = queue.get_mcp_task(task.task_id)
            assert replayed.updated_at == consumed_at
            assert replayed.projection.input_round == input_round

            queue.update_job_state(task.task_id, JobState.SUCCEEDED)
            result = await task.result()
            assert result.is_error is False

            # relay#234: an "ordinary" relay_submit_agent call (no
            # request_followup_message opt-in) from this SAME task-declaring
            # client is now ALSO durably projected as an mcp task -- task
            # admission depends only on this client having declared task
            # semantics (mode="optional"), never on whether the call happens
            # to be part of a post-admission-input round. Before the fix this
            # assertion inverted that on purpose (`pytest.raises(NotFoundError)`)
            # to pin the defect: the client's own transparent `call_tool`
            # polling now genuinely drives a real task to completion here,
            # exactly like `test_transparent_fastmcp_call_tool_polls_relay_
            # task_to_completion` -- so the job is admitted, observed via
            # `list_jobs`, and settled to SUCCEEDED from the test while the
            # client's poll loop is in flight.
            pending_ordinary = asyncio.create_task(
                client.call_tool(
                    "relay_submit_agent",
                    _submit_arguments(tmp_path, "ordinary-agent"),
                )
            )
            for _attempt in range(1_000):
                candidates = [
                    job
                    for job in await asyncio.to_thread(queue.list_jobs)
                    if job.job_id not in {task.task_id}
                ]
                if candidates:
                    break
                await asyncio.sleep(0.01)
            else:
                raise AssertionError("ordinary relay job was not admitted")
            ordinary_job_id = candidates[0].job_id
            queue.update_job_state(ordinary_job_id, JobState.SUCCEEDED)
            ordinary = await asyncio.wait_for(pending_ordinary, timeout=5)
            assert ordinary.is_error is False
            assert ordinary.structured_content is not None
            assert ordinary.structured_content["job"]["job_id"] == ordinary_job_id
            assert queue.get_job(ordinary_job_id).job_id == ordinary_job_id
            assert queue.get_mcp_task(ordinary_job_id).job_id == ordinary_job_id

    asyncio.run(scenario())


def _mock_local_job_admission(monkeypatch: pytest.MonkeyPatch) -> None:
    """Force relay_submit_agent's local (non-owned-session) admission path.

    Shared by relay#234's admission-gate tests below: monkeypatches the
    private-configuration helpers so a tmp_path-rooted RelaySettings works
    without touching the real user config directory, and forces
    ``_optional_cluster_definition`` to report no matching cluster so
    submission always takes the deterministic local-job path rather than
    depending on whatever cluster config happens to exist on the test
    runner.
    """

    def create_test_directory(path: Path) -> None:
        path.mkdir(parents=True, exist_ok=True)

    def accept_test_path(path: Path, *, directory: bool) -> None:
        if directory:
            create_test_directory(path)

    for module_name in (
        "clio_relay.cluster_config",
        "clio_relay.core_queue",
        "clio_relay.worker_lifetime_lock",
    ):
        monkeypatch.setattr(
            f"{module_name}.ensure_private_configuration_directory",
            create_test_directory,
        )
        monkeypatch.setattr(
            f"{module_name}.ensure_private_configuration_path",
            accept_test_path,
        )

    def _open_atomic(path: Path) -> BinaryIO:
        return path.open("xb")

    monkeypatch.setattr(
        "clio_relay.queue_store_write.cluster_config.open_private_atomic_file",
        _open_atomic,
    )

    def _no_cluster(_cluster: str) -> None:
        return None

    monkeypatch.setattr("clio_relay.mcp_server._optional_cluster_definition", _no_cluster)


def test_agent_task_admission_engages_without_the_followup_opt_in(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """relay#234: a task-requesting client must receive a task for
    ``relay_submit_agent`` even when the call does NOT opt into
    ``request_followup_message`` -- the two-client repro on the issue
    (fastmcp_tasks' own ``call_tool_task`` client helper AND hand-rolled
    JSON-RPC with the tasks extension declared) both observed a plain
    ``CallToolResult`` instead of a minted task.

    Root cause: ``create_task``'s early-return gate conflated "this tool
    REQUIRES post-admission input" (the agent lane's per-call opt-in for one
    extra elicitation round, ``request_followup_message``) with "this tool
    is ELIGIBLE for task creation at all". Every
    ``relay_submit_agent``/``relay_submit_remote_agent`` call omitting
    ``request_followup_message=True`` fell back to inline, unconditionally --
    a probe against production wiring proved this fires identically for a
    job still sitting in ``queued`` state, so this was never a fast-settle
    race: the admission gate simply never engaged for the agent lane's
    default (no-follow-up) case.

    Exercises the REAL production wiring (``create_fastmcp_server`` ->
    ``RelayToolProvider``), not this module's ``_task_server`` helper --
    ``_task_server`` constructs ``RelayTool`` directly and never reproduced
    ``task_requires_post_admission_input=True`` (the flag only
    ``RelayToolProvider`` ever set before this fix), which is exactly why
    the existing suite never caught this defect.
    """
    _mock_local_job_admission(monkeypatch)
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
    )
    queue = ClioCoreQueue(settings.core_dir)

    async def scenario() -> None:
        async with Client(
            create_fastmcp_server(settings=settings, queue=queue),
            mode="auto",
        ) as client:
            arguments = _submit_arguments(tmp_path, "no-followup-opt-in")
            assert "request_followup_message" not in arguments
            # Before the fix this raised ToolError("... did not run as a
            # task: the server returned a CallToolResult instead of a
            # task ...") -- the exact official-helper failure from the
            # issue's two-client repro.
            task = await call_tool_task(client, "relay_submit_agent", arguments)
            job = queue.get_job(task.task_id)
            assert task.task_id == job.job_id
            assert queue.get_mcp_task(task.task_id).projection.tool_name == "relay_submit_agent"
            status = await task.status()
            assert status.status == "working"

    asyncio.run(scenario())


def test_agent_task_admission_is_terminal_at_birth_for_instant_settling_calls(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """relay#234 acceptance: an instant-settling ``relay_submit_agent`` call
    must yield a terminal-at-birth task (the #215 family shape) for a client
    that requested task semantics without ``request_followup_message`` --
    never a bare ``CallToolResult``. ``wait_for_terminal`` makes the
    server-side admission call itself observe the job's terminal state
    before ``tools/call`` returns (the underlying job is flipped to
    ``succeeded`` from this test, concurrently, once it is visible in the
    queue), reproducing the "instant-settling call" shape without a wall-
    clock race.
    """
    _mock_local_job_admission(monkeypatch)
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
    )
    queue = ClioCoreQueue(settings.core_dir)

    async def scenario() -> None:
        async with Client(
            create_fastmcp_server(settings=settings, queue=queue),
            mode="auto",
        ) as client:
            arguments = {
                **_submit_arguments(tmp_path, "instant-settle"),
                "wait_for_terminal": True,
                "wait_timeout_seconds": 5,
                "poll_seconds": 0.01,
            }
            pending = asyncio.create_task(call_tool_task(client, "relay_submit_agent", arguments))
            for _attempt in range(1_000):
                jobs = await asyncio.to_thread(queue.list_jobs)
                if jobs:
                    break
                await asyncio.sleep(0.01)
            else:
                raise AssertionError("relay job was not admitted")
            queue.update_job_state(jobs[0].job_id, JobState.SUCCEEDED)
            task = await asyncio.wait_for(pending, timeout=5)
            assert task.task_id == jobs[0].job_id
            # Terminal-at-birth: the CreateTaskResult itself already reports
            # "completed" -- the #215 family shape -- never "working", and a
            # client never has to poll tasks/get to observe a state
            # transition that already happened before the task was minted.
            assert task.create_result.status == "completed"
            persisted = queue.get_mcp_task(task.task_id)
            assert persisted.projection.completed_result is not None
            status = await task.status()
            assert status.status == "completed"
            assert status.result is not None
            result = await task.result()
            assert result.is_error is False

    asyncio.run(scenario())


def test_official_fastmcp_tool_task_cancel_uses_relay_cancellation(
    tmp_path: Path,
) -> None:
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
    )
    queue = ClioCoreQueue(settings.core_dir)

    async def scenario() -> None:
        async with Client(_task_server(settings, queue), mode="auto") as client:
            task = await call_tool_task(
                client,
                "relay_submit_agent",
                _submit_arguments(tmp_path, "cancel"),
                raise_on_error=False,
            )
            await task.cancel()
            assert queue.get_job(task.task_id).state is JobState.CANCELED
            assert (await task.status()).status == "cancelled"
            result = await task.result()
            assert result.is_error is True

    asyncio.run(scenario())


def test_transparent_fastmcp_call_tool_polls_relay_task_to_completion(
    tmp_path: Path,
) -> None:
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
    )
    queue = ClioCoreQueue(settings.core_dir)

    async def scenario() -> None:
        async with Client(_task_server(settings, queue), mode="auto") as client:
            pending = asyncio.create_task(
                client.call_tool(
                    "relay_submit_agent",
                    _submit_arguments(tmp_path, "transparent"),
                )
            )
            for _attempt in range(1_000):
                jobs = await asyncio.to_thread(queue.list_jobs)
                if jobs:
                    break
                await asyncio.sleep(0.01)
            else:
                raise AssertionError("relay job was not admitted")
            queue.update_job_state(jobs[0].job_id, JobState.SUCCEEDED)
            result = await asyncio.wait_for(pending, timeout=5)
            assert result.is_error is False
            assert result.structured_content is not None
            assert result.structured_content["job"]["job_id"] == jobs[0].job_id

    asyncio.run(scenario())


def test_task_capability_is_visible_without_starting_docket(tmp_path: Path) -> None:
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
    )
    queue = ClioCoreQueue(settings.core_dir)
    server = _task_server(settings, queue)

    async def scenario() -> None:
        assert server.docket is None
        async with Client(server, mode="auto") as client:
            tools = await client.list_tools()
            submit = next(tool for tool in tools if tool.name == "relay_submit_agent")
            assert submit.execution is None
            component = await server.get_tool("relay_submit_agent")
            assert component is not None
            assert component.task_config.mode == "optional"
            capabilities = client.session.server_capabilities
            assert capabilities is not None
            assert "io.modelcontextprotocol/tasks" in (capabilities.extensions or {})
            assert server.docket is None
        assert server.docket is None

    asyncio.run(scenario())


def test_task_methods_require_the_client_extension_capability(tmp_path: Path) -> None:
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
    )
    queue = ClioCoreQueue(settings.core_dir)
    server = _task_server(settings, queue)

    async def scenario() -> None:
        async with Client(server, mode="auto") as client:
            task = await call_tool_task(
                client,
                "relay_submit_agent",
                _submit_arguments(tmp_path, "capability"),
            )
            task_id = task.task_id
            with pytest.raises(MCPError) as missing:
                await client.session.send_request(
                    GetTaskRequest(params=GetTaskRequestParams(task_id="job-missing")),
                    ClientGetTaskResult,
                )
            assert missing.value.code == mcp_types.INVALID_PARAMS

        async with _NoInternalExtensionsClient(server, mode="auto") as client:
            with pytest.raises(MCPError) as failure:
                await client.session.send_request(
                    GetTaskRequest(params=GetTaskRequestParams(task_id=task_id)),
                    ClientGetTaskResult,
                )
            assert failure.value.code == MISSING_REQUIRED_CLIENT_CAPABILITY
            assert failure.value.data == {
                "requiredCapabilities": {"extensions": {"io.modelcontextprotocol/tasks": {}}}
            }

    asyncio.run(scenario())


def test_task_methods_are_not_available_on_legacy_protocol(tmp_path: Path) -> None:
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
    )
    queue = ClioCoreQueue(settings.core_dir)

    async def scenario() -> None:
        async with _NoInternalExtensionsClient(
            _task_server(settings, queue),
            mode="legacy",
        ) as client:
            with pytest.raises(MCPError) as failure:
                await client.session.send_request(
                    GetTaskRequest(params=GetTaskRequestParams(task_id="job-missing")),
                    ClientGetTaskResult,
                )
            assert failure.value.code == mcp_types.METHOD_NOT_FOUND

    asyncio.run(scenario())


def test_streamable_http_requires_bearer_authentication(tmp_path: Path) -> None:
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
        api_token="relay-http-secret",
    )
    queue = ClioCoreQueue(settings.core_dir)
    server = create_fastmcp_server(
        settings=settings,
        queue=queue,
        http_base_url="http://127.0.0.1/mcp",
    )

    async def scenario() -> None:
        async with asgi_server(server) as running:
            async with running.http_client() as raw_client:
                missing = await raw_client.post(running.url, json={})
                assert missing.status_code == 401
            async with running.http_client(
                headers={"Authorization": "Bearer incorrect"}
            ) as raw_client:
                incorrect = await raw_client.post(running.url, json={})
                assert incorrect.status_code == 401
            async with Client(
                running.transport(auth="relay-http-secret"),
                mode="auto",
            ) as client:
                tools = await client.list_tools()
                assert "relay_status" in {tool.name for tool in tools}

    asyncio.run(scenario())


def test_http_task_management_rejects_mcp_name_header_mismatch(
    tmp_path: Path,
) -> None:
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
    )
    queue = ClioCoreQueue(settings.core_dir)

    async def scenario() -> None:
        async with (
            asgi_server(_task_server(settings, queue)) as running,
            Client(
                running.transport(headers={"Mcp-Name": "not-the-relay-job-id"}),
                mode="auto",
            ) as client,
        ):
            task = await call_tool_task(
                client,
                "relay_submit_agent",
                _submit_arguments(tmp_path, "header"),
            )
            with pytest.raises(MCPError) as failure:
                await task.status()
            assert failure.value.code == mcp_types.HEADER_MISMATCH

    asyncio.run(scenario())


def test_official_tool_task_reattaches_over_new_http_client(tmp_path: Path) -> None:
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
    )
    queue = ClioCoreQueue(settings.core_dir)

    async def scenario() -> None:
        async with asgi_server(_task_server(settings, queue)) as running:
            async with Client(
                running.transport(),
                mode="auto",
            ) as first_client:
                first = await call_tool_task(
                    first_client,
                    "relay_submit_agent",
                    _submit_arguments(tmp_path, "reattach"),
                )
                create_result = first.create_result
                task_id = first.task_id

            queue.update_job_state(task_id, JobState.SUCCEEDED)

            async with Client(
                running.transport(),
                mode="auto",
            ) as second_client:
                restored = ToolTask(
                    second_client,
                    "relay_submit_agent",
                    create_result,
                )
                result = await restored.result()
                assert result.is_error is False
                assert result.structured_content is not None
                assert result.structured_content["job"]["job_id"] == task_id

    asyncio.run(scenario())


def test_failed_relay_job_is_completed_task_with_tool_error(tmp_path: Path) -> None:
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
    )
    queue = ClioCoreQueue(settings.core_dir)

    async def scenario() -> None:
        async with Client(_task_server(settings, queue), mode="auto") as client:
            task = await call_tool_task(
                client,
                "relay_submit_agent",
                _submit_arguments(tmp_path, "failed"),
                raise_on_error=False,
            )
            queue.update_job_state(
                task.task_id,
                JobState.FAILED,
                error="remote agent failed",
            )
            status = await task.status()
            assert status.status == "completed"
            assert status.result is not None
            assert status.result["isError"] is True
            result = await task.result()
            assert result.is_error is True
            assert result.structured_content is not None
            assert result.structured_content["job"]["state"] == JobState.FAILED.value

    asyncio.run(scenario())


def test_official_client_answers_durable_input_required_task(tmp_path: Path) -> None:
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
    )
    queue = ClioCoreQueue(settings.core_dir)
    elicitation_messages: list[str] = []

    async def approve(
        message: str,
        _response_type: type[Any] | None,
        _params: mcp_types.ElicitRequestParams,
        _context: Any,
    ) -> dict[str, bool]:
        elicitation_messages.append(message)
        jobs = await asyncio.to_thread(queue.list_jobs)
        assert len(jobs) == 1
        await asyncio.to_thread(
            queue.update_job_state,
            jobs[0].job_id,
            JobState.SUCCEEDED,
        )
        return {"approved": True}

    async def scenario() -> None:
        async with Client(
            _task_server(settings, queue),
            mode="auto",
            elicitation_handler=approve,
        ) as client:
            task = await call_tool_task(
                client,
                "relay_submit_agent",
                _submit_arguments(tmp_path, "input-required"),
            )
            _put_input_round(queue, task.task_id, keys=("approval",))
            assert (await task.status()).status == "input_required"

            result = await task.result()
            assert result.is_error is False
            assert elicitation_messages == ["Approve approval?"]
            persisted = queue.get_mcp_task(task.task_id).projection.input_round
            assert persisted is not None
            assert persisted.outstanding == {}
            answer = persisted.answered["approval"]
            assert answer["action"] == "accept"
            assert answer["content"] == {"approved": True}

    asyncio.run(scenario())


def test_task_input_updates_are_partial_replay_safe_and_cas_retrying(
    tmp_path: Path,
) -> None:
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
    )
    queue = ClioCoreQueue(settings.core_dir)
    runtime = RelayMcpRuntime(settings=settings, profile="user", queue=queue)

    async def scenario() -> None:
        async with Client(_task_server(settings, queue), mode="auto") as client:
            task = await call_tool_task(
                client,
                "relay_submit_agent",
                _submit_arguments(tmp_path, "input-cas"),
            )
        _put_input_round(queue, task.task_id, keys=("first", "second"))
        original = queue.get_mcp_task(task.task_id)

        await runtime.update_task(
            original,
            {
                "unknown": {"ignored": True},
                "first": {"action": "accept", "content": {"approved": True}},
            },
        )
        after_first = queue.get_mcp_task(task.task_id)
        first_round = after_first.projection.input_round
        assert first_round is not None
        assert set(first_round.outstanding) == {"second"}
        first_updated_at = after_first.updated_at

        await runtime.update_task(
            after_first,
            {"first": {"action": "accept", "content": {"approved": False}}},
        )
        assert queue.get_mcp_task(task.task_id).updated_at == first_updated_at

        await runtime.update_task(
            original,
            {"second": {"action": "decline"}},
        )
        final_round = queue.get_mcp_task(task.task_id).projection.input_round
        assert final_round is not None
        assert final_round.outstanding == {}
        assert set(final_round.answered) == {"first", "second"}
        assert final_round.answered["first"]["content"] == {"approved": True}

    asyncio.run(scenario())


def test_task_projection_is_idempotent_bounded_and_conflict_checked(
    tmp_path: Path,
) -> None:
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
    )
    queue = ClioCoreQueue(settings.core_dir)
    runtime = RelayMcpRuntime(settings=settings, profile="user", queue=queue)
    definitions, _catalog = mcp_tool_definitions_and_remote_catalog(profile="user")
    definition = next(item for item in definitions if item["name"] == "relay_submit_agent")
    tool = RelayTool(
        definition,
        runtime=runtime,
        catalog_revision=None,
        task_capable=True,
    )
    result = ToolResult(
        content=[mcp_types.TextContent(type="text", text="accepted")],
        structured_content={
            "job_id": "job-projection-idempotency",
            "state": JobState.QUEUED.value,
            "terminal": False,
        },
    )

    async def scenario() -> None:
        first = await runtime.create_task(
            tool=tool,
            arguments={"value": "same"},
            result=result,
        )
        replay = await runtime.create_task(
            tool=tool,
            arguments={"value": "same"},
            result=result,
        )
        assert first is not None
        assert replay == first

        progressed_replay = await runtime.create_task(
            tool=tool,
            arguments={"value": "same"},
            result=ToolResult(
                content=result.content,
                structured_content={
                    "job_id": "job-projection-idempotency",
                    "state": JobState.RUNNING.value,
                    "terminal": False,
                },
            ),
        )
        assert progressed_replay is not None
        assert progressed_replay == first
        assert progressed_replay.projection.initial_result["state"] == JobState.QUEUED.value

        with pytest.raises(McpTaskIdentityConflictError, match="different semantics"):
            await runtime.create_task(
                tool=tool,
                arguments={"value": "different"},
                result=result,
            )

        oversized = "x" * (MAX_TASK_ARGUMENT_BYTES + 1)
        assert len(json.dumps({"value": oversized}).encode("utf-8")) > (MAX_TASK_ARGUMENT_BYTES)
        with pytest.raises(ValueError, match="projection limit"):
            await runtime.create_task(
                tool=tool,
                arguments={"value": oversized},
                result=ToolResult(
                    content=result.content,
                    structured_content={
                        "job_id": "job-projection-oversized",
                        "state": JobState.QUEUED.value,
                        "terminal": False,
                    },
                ),
            )

        with pytest.raises(ValueError, match="finite JSON"):
            await runtime.create_task(
                tool=tool,
                arguments={"value": float("nan")},
                result=ToolResult(
                    content=result.content,
                    structured_content={
                        "job_id": "job-projection-nonfinite",
                        "state": JobState.QUEUED.value,
                        "terminal": False,
                    },
                ),
            )

        deep_arguments: JSON = {}
        nested = deep_arguments
        for _depth in range(66):
            child: JSON = {}
            nested["child"] = child
            nested = child
        with pytest.raises(ValueError, match="nesting levels"):
            await runtime.create_task(
                tool=tool,
                arguments=deep_arguments,
                result=ToolResult(
                    content=result.content,
                    structured_content={
                        "job_id": "job-projection-deep",
                        "state": JobState.QUEUED.value,
                        "terminal": False,
                    },
                ),
            )

        errored_projection = RelayMcpTaskProjection.model_validate(
            {
                **first.projection.model_dump(mode="python"),
                "protocol_error": {
                    "code": mcp_types.INTERNAL_ERROR,
                    "message": "relay protocol projection failed",
                },
            }
        )
        errored = await asyncio.to_thread(
            lambda: queue.update_mcp_task_projection(
                first.task_id,
                errored_projection,
                expected_updated_at=first.updated_at,
            )
        )
        errored_status = await runtime.task_status(errored)
        assert errored_status.status == "failed"
        assert errored_status.error == {
            "code": mcp_types.INTERNAL_ERROR,
            "message": "relay protocol projection failed",
        }
        errored_replay = await runtime.create_task(
            tool=tool,
            arguments={"value": "same"},
            result=result,
        )
        assert errored_replay == errored

    asyncio.run(scenario())

    projection_base: JSON = {
        "tool_name": "relay_submit_agent",
        "profile": "user",
        "arguments": {},
        "initial_result": {
            "job_id": "job-projection-invariants",
            "state": JobState.QUEUED.value,
            "terminal": False,
        },
    }
    with pytest.raises(ValueError, match="unissued request key"):
        RelayMcpTaskProjection.model_validate(
            {
                **projection_base,
                "input_round": {
                    "leg": 1,
                    "outstanding": {"unissued": _elicitation_document("Approve?")},
                },
            }
        )
    with pytest.raises(ValueError, match="outstanding and answered"):
        RelayMcpTaskProjection.model_validate(
            {
                **projection_base,
                "issued_input_keys": ["approval"],
                "input_round": {
                    "leg": 1,
                    "outstanding": {"approval": _elicitation_document("Approve?")},
                    "answered": {"approval": {"action": "accept", "content": {}}},
                },
            }
        )
    with pytest.raises(ValueError, match="conflicting result states"):
        RelayMcpTaskProjection.model_validate(
            {
                **projection_base,
                "issued_input_keys": ["approval"],
                "input_round": {
                    "leg": 1,
                    "outstanding": {"approval": _elicitation_document("Approve?")},
                },
                "completed_result": {"isError": False},
            }
        )


def test_task_projection_conflict_check_ignores_relay_control_only_arguments(
    tmp_path: Path,
) -> None:
    """#218: control-only transport keys must not trip the conflict check.

    ``wait_for_terminal``/``wait_timeout_seconds``/``poll_seconds`` are consumed
    by clio-relay's own transport layer and never forwarded to the remote MCP
    server (``VIRTUAL_REMOTE_MCP_RELAY_CONTROL_FIELDS``); they cannot change the
    executed work. A later dispatch of the identical work that varies only in
    those keys must replay the existing task instead of raising
    ``QueueConflictError`` -- the door-side bug that produced an unhandled
    -32603 when a probe omitted ``wait_for_terminal``/``wait_timeout_seconds``
    relative to an earlier cached dispatch.
    """
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
    )
    queue = ClioCoreQueue(settings.core_dir)
    runtime = RelayMcpRuntime(settings=settings, profile="user", queue=queue)
    definitions, _catalog = mcp_tool_definitions_and_remote_catalog(profile="user")
    definition = next(item for item in definitions if item["name"] == "relay_submit_agent")
    tool = RelayTool(
        definition,
        runtime=runtime,
        catalog_revision=None,
        task_capable=True,
    )
    result = ToolResult(
        content=[mcp_types.TextContent(type="text", text="accepted")],
        structured_content={
            "job_id": "job-control-only-replay",
            "state": JobState.QUEUED.value,
            "terminal": False,
        },
    )

    async def scenario() -> None:
        first = await runtime.create_task(
            tool=tool,
            arguments={
                "value": "same",
                "wait_for_terminal": False,
                "wait_timeout_seconds": 600,
                "poll_seconds": 2,
            },
            result=result,
        )
        assert first is not None
        # Same substantive work, but wait_for_terminal/wait_timeout_seconds are
        # omitted and poll_seconds differs -- exactly the shape that collided
        # with a cached record in the live repro (clio-relay#218).
        replay = await runtime.create_task(
            tool=tool,
            arguments={"value": "same", "poll_seconds": 5},
            result=result,
        )
        assert replay == first

    asyncio.run(scenario())


def test_task_projection_conflict_surfaces_as_typed_mcp_error(tmp_path: Path) -> None:
    """#218: a genuine task-identity conflict must surface as a typed MCPError.

    ``intercept_tool_call`` calls ``create_task`` with no surrounding
    try/except, so an unhandled task identity conflict from ``put_mcp_task``
    previously escaped through FastMCP's generic handler as a bare, typeless
    -32603 internal error (the exact live symptom). A real conflict (as
    opposed to the control-only-argument false positive covered above) must
    still be refused, but as a typed, queryable MCPError.

    #231 R3: re-pointed at ``door_errors.classify(..., reason="mcp_task_conflict")``
    -- the wire ``reason`` string now comes from the frozen ``REASONS`` table
    (``door_errors.REASONS["mcp_task_conflict"]``) instead of the ad hoc
    ``"mcp_task_identity_conflict"`` this site invented locally.
    """
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
    )
    queue = ClioCoreQueue(settings.core_dir)
    server = _task_server(settings, queue)
    real_put_mcp_task = queue.put_mcp_task
    conflicting_task_ids: list[str] = []

    def forced_conflict(task: RelayMcpTaskRecord) -> RelayMcpTaskRecord:
        conflicting_task_ids.append(task.task_id)
        raise McpTaskIdentityConflictError(
            f"MCP task identity was reused with different semantics: {task.task_id}"
        )

    async def scenario() -> None:
        async with Client(server, mode="auto") as client:
            arguments = _submit_arguments(tmp_path, "typed-conflict")
            await call_tool_task(client, "relay_submit_agent", arguments)
            queue.put_mcp_task = forced_conflict  # type: ignore[method-assign]
            with pytest.raises(MCPError) as failure:
                await call_tool_task(client, "relay_submit_agent", arguments)
            assert failure.value.code == mcp_types.INVALID_PARAMS
            assert "different semantics" in str(failure.value)
            # door_errors.as_mcp_error always carries data={"reason": ..., ...}
            # so the caller can query the failure by its frozen reason.
            assert conflicting_task_ids
            assert failure.value.data == {
                "reason": "mcp_task_conflict",
                "task_id": conflicting_task_ids[-1],
            }

    try:
        asyncio.run(scenario())
    finally:
        queue.put_mcp_task = real_put_mcp_task  # type: ignore[method-assign]


def test_legacy_queue_state_during_task_creation_keeps_foreign_text_private(
    tmp_path: Path,
) -> None:
    """Only a task-identity conflict may publish its queue-conflict message."""
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
    )
    queue = ClioCoreQueue(settings.core_dir)
    server = _task_server(settings, queue)
    real_put_mcp_task = queue.put_mcp_task
    foreign_text = "FOREIGN pydantic validator disclosed secret diagnostic"

    def legacy_state(_task: RelayMcpTaskRecord) -> RelayMcpTaskRecord:
        raise LegacyQueueStateError(
            family="events",
            path=settings.core_dir / "events" / "foreign.json",
            reason=f"event record is invalid: ValueError: {foreign_text}",
        )

    async def scenario() -> None:
        async with Client(server, mode="auto") as client:
            queue.put_mcp_task = legacy_state  # type: ignore[method-assign]
            with pytest.raises(MCPError) as failure:
                await call_tool_task(
                    client,
                    "relay_submit_agent",
                    _submit_arguments(tmp_path, "legacy-task-creation"),
                )
            spec = door_errors.REASONS["internal_error"]
            assert failure.value.code == spec.mcp_code
            assert failure.value.data == {"reason": "internal_error"}
            assert door_errors.public_message(reason=spec.reason, title=spec.title) in str(
                failure.value
            )
            assert foreign_text not in str(failure.value)
            assert foreign_text not in json.dumps(failure.value.data, default=str)

    try:
        asyncio.run(scenario())
    finally:
        queue.put_mcp_task = real_put_mcp_task  # type: ignore[method-assign]


def test_task_persistence_untyped_failure_surfaces_as_typed_error_v1(
    tmp_path: Path,
) -> None:
    """relay#234 adversarial review finding 1: ``put_mcp_task`` can raise a
    non-conflict error too (disk-full, permission) -- ``intercept_tool_call``
    previously caught only ``TaskInputParkConflictError``/``QueueConflictError``,
    so anything else (an ``OSError`` here, standing in for a real storage
    failure) escaped through FastMCP's own generic handler with no relay
    ``reason`` at all, violating the error.v1/no-silent-fallback doctrine.

    Asserts the SERVED wire shape (typed ``reason``/``code``/bounded
    ``message``), not merely that some exception was raised -- and that the
    raw ``OSError`` text never reaches the client (dispatch rule 4: an
    unmatched exception type classifies as ``internal_error`` with a fixed,
    handler-internals-free message).
    """
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
    )
    queue = ClioCoreQueue(settings.core_dir)
    server = _task_server(settings, queue)
    real_put_mcp_task = queue.put_mcp_task

    def disk_full(_task: RelayMcpTaskRecord) -> RelayMcpTaskRecord:
        raise OSError("No space left on device")

    async def scenario() -> None:
        async with Client(server, mode="auto") as client:
            arguments = _submit_arguments(tmp_path, "untyped-persistence-failure")
            with pytest.raises(MCPError) as failure:
                await call_tool_task(client, "relay_submit_agent", arguments)
            spec = door_errors.REASONS["internal_error"]
            assert failure.value.code == spec.mcp_code
            assert failure.value.data == {"reason": "internal_error"}
            assert "No space left on device" not in str(failure.value)
            assert "No space left on device" not in json.dumps(
                failure.value.data,
                default=str,
            )

    queue.put_mcp_task = disk_full  # type: ignore[method-assign]
    try:
        asyncio.run(scenario())
    finally:
        queue.put_mcp_task = real_put_mcp_task  # type: ignore[method-assign]


def test_park_agent_input_cas_exhaustion_is_never_mistyped_as_invalid_params(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """clio-relay#218 rework: _park_agent_input's CAS-exhaustion conflict is
    a transient concurrency conflict, never a client parameter problem -- it
    must not be mistyped as MCPError(INVALID_PARAMS) the way put_mcp_task's
    genuine task-identity-reuse conflict correctly is (test above). Forcing
    every update_mcp_task_projection call to conflict exhausts
    _park_agent_input's 8 retry attempts and raises
    TaskInputParkConflictError.

    #231 R3: this closes the live hole the #218 comment named but left open
    -- ``intercept_tool_call`` no longer leaves ``TaskInputParkConflictError``
    unwrapped. ``door_errors.classify`` now types it into the dedicated,
    retryable ``mcp_task_input_park_conflict`` reason (its own MCP code,
    never ``INTERNAL_ERROR`` and never ``INVALID_PARAMS``).
    """

    def create_test_directory(path: Path) -> None:
        path.mkdir(parents=True, exist_ok=True)

    def accept_test_path(path: Path, *, directory: bool) -> None:
        if directory:
            create_test_directory(path)

    for module_name in (
        "clio_relay.cluster_config",
        "clio_relay.core_queue",
        "clio_relay.worker_lifetime_lock",
    ):
        monkeypatch.setattr(
            f"{module_name}.ensure_private_configuration_directory",
            create_test_directory,
        )
        monkeypatch.setattr(
            f"{module_name}.ensure_private_configuration_path",
            accept_test_path,
        )

    def _open_atomic(path: Path) -> BinaryIO:
        return path.open("xb")

    monkeypatch.setattr(
        "clio_relay.queue_store_write.cluster_config.open_private_atomic_file",
        _open_atomic,
    )

    def _no_cluster(_cluster: str) -> None:
        return None

    monkeypatch.setattr("clio_relay.mcp_server._optional_cluster_definition", _no_cluster)
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
    )
    queue = ClioCoreQueue(settings.core_dir)
    server = _task_server(settings, queue)

    def always_conflict(*args: Any, **kwargs: Any) -> RelayMcpTaskRecord:
        raise QueueConflictError("forced permanent parking CAS conflict")

    monkeypatch.setattr(queue, "update_mcp_task_projection", always_conflict)

    async def scenario() -> None:
        async with Client(server, mode="auto") as client:
            arguments = {
                **_submit_arguments(tmp_path, "park-cas-exhaustion"),
                "request_followup_message": True,
            }
            with pytest.raises(MCPError) as failure:
                await call_tool_task(client, "relay_submit_agent", arguments)
            # #231 R3: typed via door_errors into its own dedicated,
            # retryable reason -- never the generic INTERNAL_ERROR bucket
            # (the old bare-re-raise behavior) and never INVALID_PARAMS (the
            # code that tells a client "your parameters are wrong" when the
            # real cause is transient server-side CAS contention).
            spec = door_errors.REASONS["mcp_task_input_park_conflict"]
            assert failure.value.code == spec.mcp_code
            assert failure.value.code != mcp_types.INTERNAL_ERROR
            assert failure.value.code != mcp_types.INVALID_PARAMS
            assert failure.value.data == {"reason": "mcp_task_input_park_conflict"}

    asyncio.run(scenario())


# --------------------------------------------------------------------------- #
# C1 fixtures: sanitized, structurally-verbatim captures from a live p5run2
# relay deployment's durable MCP task records
# (D:\relay-p5local\core\mcp_tasks\job_995b61bf....json and
# job_3544b072....json). Usernames and spool host paths are redacted;
# hashes, job/artifact ids, and every field name are kept verbatim. C1's
# whole point is that the eager and lazy documents must match a REAL
# captured wire shape, not merely each other, so these are the acceptance
# fixtures for the equivalence proof below -- never a hand-built shape that
# happens to satisfy the code under test.
# --------------------------------------------------------------------------- #

_LIVE_CREATE_PIPELINE_JOB_ID = "job_995b61bfee794b90b4221c4b33ac26eb"

# The create-time receipt ``create_task`` receives as ``structured`` (live
# ``initial_result``, verbatim): flat, ``job_id``/``state`` at the top level.
_LIVE_CREATE_PIPELINE_RECEIPT: JSON = {
    "cluster": "ares-p5run2",
    "job_id": _LIVE_CREATE_PIPELINE_JOB_ID,
    "state": "succeeded",
    "kind": "mcp_call",
    "terminal": True,
    "remote": True,
    "route_revision": "e701bebd118f9bbdec5f76f13c742ecd412f29aec39c66b93757fe65ae14a141",
    "observation": {
        "outcome": "terminal",
        "timeout_seconds": 600.0,
        "scheduler_action": "none",
        "relay_action": "none",
    },
    "last_error": None,
    "mcp_result_artifact": {
        "artifact_id": "artifact_ee911b95320f4237898a3041c93ad419",
        "job_id": _LIVE_CREATE_PIPELINE_JOB_ID,
        "kind": "mcp_result",
        "size_bytes": 15178,
        "sha256": "cbb62884dc180c3dd7aec91934397cfafaea03a0139eb9dd97ef01618fd28a3d",
        "created_at": "2026-08-11T21:45:19.166672Z",
    },
    "mcp_result": {
        "operation": "tools/call",
        "tool": "jarvis_create_pipeline",
        "returncode": 0,
        "timed_out": False,
        "protocol_error": None,
        "structured_result": {
            "pipeline_id": "phase-d-stage-check-1786484613",
            "status": "created",
        },
        "protocol_version": "2024-11-05",
        "server_info": {"name": "jarvis", "version": "4.0.0b1"},
        "result_validation": None,
    },
    "catalog_revision": "c0d254025e861b1dc41e661f7fedc98cf1a8765e7efea7db2566fc93f5b1117a",
}

# The document ``task_status``'s lazy re-derivation independently produces
# for the SAME job once settled: the ``job``/``relay_queue``/``scheduler``/
# ``transform``/``artifacts`` envelope shape is a genuine live capture
# (job_3544b072....json's own ``completed_result.structuredContent`` shape,
# spool paths redacted); ``job``/``mcp_result`` are this job's own succeeded
# evidence so both fixtures describe the identical dispatch.
_LIVE_CREATE_PIPELINE_WAIT_DOCUMENT: JSON = {
    "job": {
        "job_id": _LIVE_CREATE_PIPELINE_JOB_ID,
        "cluster": "ares-p5run2",
        "kind": "mcp_call",
        "state": "succeeded",
        "last_error": None,
    },
    "transform": None,
    "relay_queue": {"state": "succeeded", "jobs_ahead": None, "position": None},
    "scheduler": [],
    "terminal": True,
    "cluster": "ares-p5run2",
    "route_revision": "e701bebd118f9bbdec5f76f13c742ecd412f29aec39c66b93757fe65ae14a141",
    "observation": {
        "outcome": "terminal",
        "timeout_seconds": 0.01,
        "scheduler_action": "none",
        "relay_action": "none",
    },
    "last_error": None,
    "mcp_result_artifact": {
        "artifact_id": "artifact_ee911b95320f4237898a3041c93ad419",
        "job_id": _LIVE_CREATE_PIPELINE_JOB_ID,
        "kind": "mcp_result",
        "size_bytes": 15178,
        "sha256": "cbb62884dc180c3dd7aec91934397cfafaea03a0139eb9dd97ef01618fd28a3d",
        "created_at": "2026-08-11T21:45:19.166672Z",
    },
    "mcp_result": {
        "operation": "tools/call",
        "tool": "jarvis_create_pipeline",
        "returncode": 0,
        "timed_out": False,
        "protocol_error": None,
        "structured_result": {
            "pipeline_id": "phase-d-stage-check-1786484613",
            "status": "created",
        },
        "protocol_version": "2024-11-05",
        "server_info": {"name": "jarvis", "version": "4.0.0b1"},
        "result_validation": None,
    },
    "artifacts": [
        {
            "artifact_id": "artifact_ee911b95320f4237898a3041c93ad419",
            "job_id": _LIVE_CREATE_PIPELINE_JOB_ID,
            "sequence": 1,
            "uri": "file:///redacted/relay-spool/job_995b61bf/mcp-result.json",
            "kind": "mcp_result",
            "size_bytes": 15178,
            "sha256": "cbb62884dc180c3dd7aec91934397cfafaea03a0139eb9dd97ef01618fd28a3d",
            "created_at": "2026-08-11T21:45:19.166672Z",
        },
    ],
}

_LIVE_FAILED_DESCRIBE_JOB_ID = "job_3544b0721ffe45099371957593130a9d"
_LIVE_FAILED_DESCRIBE_REMOTE_MESSAGE = (
    "1 validation error for call[jarvis_describe_tool]\n"
    "target\n  Missing required argument [type=missing_argument, "
    "input_value={'query': 'script'}, input_type=dict]"
)

# Live create-time receipt for a job that reached JARVIS but the remote call
# itself failed (job_3544b072....json's own ``initial_result``, verbatim).
_LIVE_FAILED_DESCRIBE_RECEIPT: JSON = {
    "cluster": "ares-p5run2",
    "job_id": _LIVE_FAILED_DESCRIBE_JOB_ID,
    "state": "failed",
    "kind": "mcp_call",
    "terminal": True,
    "remote": True,
    "route_revision": "d16c7e8ddf3c66a3f2d986695b97606ea2d2cf704348728ff7cc40d26563421b",
    "observation": {
        "outcome": "terminal",
        "timeout_seconds": 600.0,
        "scheduler_action": "none",
        "relay_action": "none",
    },
    "last_error": "exit code 1",
    "mcp_result_artifact": {
        "artifact_id": "artifact_e35348ca11bd4c3389bd2329e5e09b54",
        "job_id": _LIVE_FAILED_DESCRIBE_JOB_ID,
        "kind": "mcp_result",
        "size_bytes": 15244,
        "sha256": "d8bc0bfbe7563066d2f0702fe2683384aa50f80f15077ddb0a39834a435c7a78",
        "created_at": "2026-08-11T19:13:30.339982Z",
    },
    "mcp_result": {
        "operation": "tools/call",
        "tool": "jarvis_describe",
        "returncode": 1,
        "timed_out": False,
        "protocol_error": "tools/call returned isError=true",
        "structured_result": None,
        "protocol_result": {
            "content": [{"text": _LIVE_FAILED_DESCRIBE_REMOTE_MESSAGE, "type": "text"}],
            "isError": True,
        },
        "protocol_version": "2024-11-05",
        "server_info": {"name": "jarvis", "version": "4.0.0b1"},
        "result_validation": None,
    },
    "catalog_revision": "21c338ab4da4ab00adee9e8b1afc6523586b9b184737b5dc3b94fe703c6ee37f",
}

# Live lazy-resolved document for the SAME failed job
# (job_3544b072....json's own ``completed_result.structuredContent``,
# verbatim except a trimmed ``artifacts`` list and redacted spool paths).
_LIVE_FAILED_DESCRIBE_WAIT_DOCUMENT: JSON = {
    "job": {
        "job_id": _LIVE_FAILED_DESCRIBE_JOB_ID,
        "cluster": "ares-p5run2",
        "kind": "mcp_call",
        "state": "failed",
        "last_error": "exit code 1",
    },
    "transform": None,
    "relay_queue": {"state": "failed", "jobs_ahead": None, "position": None},
    "scheduler": [],
    "terminal": True,
    "cluster": "ares-p5run2",
    "route_revision": "d16c7e8ddf3c66a3f2d986695b97606ea2d2cf704348728ff7cc40d26563421b",
    "observation": {
        "outcome": "terminal",
        "timeout_seconds": 0.01,
        "scheduler_action": "none",
        "relay_action": "none",
    },
    "last_error": "exit code 1",
    "mcp_result_artifact": {
        "artifact_id": "artifact_e35348ca11bd4c3389bd2329e5e09b54",
        "job_id": _LIVE_FAILED_DESCRIBE_JOB_ID,
        "kind": "mcp_result",
        "size_bytes": 15244,
        "sha256": "d8bc0bfbe7563066d2f0702fe2683384aa50f80f15077ddb0a39834a435c7a78",
        "created_at": "2026-08-11T19:13:30.339982Z",
    },
    "mcp_result": {
        "operation": "tools/call",
        "tool": "jarvis_describe",
        "returncode": 1,
        "timed_out": False,
        "protocol_error": "tools/call returned isError=true",
        "structured_result": None,
        "protocol_result": {
            "content": [{"text": _LIVE_FAILED_DESCRIBE_REMOTE_MESSAGE, "type": "text"}],
            "isError": True,
        },
        "protocol_version": "2024-11-05",
        "server_info": {"name": "jarvis", "version": "4.0.0b1"},
        "result_validation": None,
    },
    "artifacts": [
        {
            "artifact_id": "artifact_e35348ca11bd4c3389bd2329e5e09b54",
            "job_id": _LIVE_FAILED_DESCRIBE_JOB_ID,
            "sequence": 1,
            "uri": "file:///redacted/relay-spool/job_3544b072/mcp-result.json",
            "kind": "mcp_result",
            "size_bytes": 15244,
            "sha256": "d8bc0bfbe7563066d2f0702fe2683384aa50f80f15077ddb0a39834a435c7a78",
            "created_at": "2026-08-11T19:13:30.339982Z",
        },
    ],
}


def _live_relay_job(job_id: str, *, state: JobState, tool: str) -> JSON:
    """A minimal, fully valid ``RelayJob`` document for the given live job id."""

    return RelayJob(
        job_id=job_id,
        cluster="ares-p5run2",
        kind=JobKind.MCP_CALL,
        state=state,
        spec=McpCallSpec(
            server="clio-kit",
            server_args=["mcp-server", "jarvis"],
            expected_server_artifact_digest="a" * 64,
            tool=tool,
            arguments={},
        ),
        idempotency_key=f"test-{job_id}",
    ).model_dump(mode="json")


def _make_tool(runtime: RelayMcpRuntime) -> RelayTool:
    definitions, _catalog = mcp_tool_definitions_and_remote_catalog(profile="user")
    definition = next(item for item in definitions if item["name"] == "relay_submit_agent")
    return RelayTool(definition, runtime=runtime, catalog_revision=None, task_capable=True)


def test_create_task_eager_promotion_matches_the_lazy_first_poll_document(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """C1: the eager ``completed_result`` D22 promotes at create time must be
    byte-equivalent to what the lazy ``tasks/get`` path serves for the SAME
    job -- the WAIT DOCUMENT (with ``job``/``relay_queue`` siblings), not the
    flat create-time receipt. Before this fix, ``create_task`` wrapped
    ``structured`` (the receipt) directly; the receipt and the wait document
    are structurally different (flat ``job_id``/``state`` at the top level
    vs. a nested ``job``), and the client's envelope detector -- keyed on
    exactly that shape difference -- silently handed the receipt back to the
    agent as if it were the tool's own result (every successful dispatch,
    100% of the time).

    Proven with a REAL live-captured fixture pair (see the module constants
    above): the create-time receipt goes in as ``structured`` for the eager
    path; the SAME live wait-document shape comes back out of a mocked
    ``wait_mcp_job`` for a SEPARATE, freshly created record driven through
    the real lazy ``task_status`` path. They must be byte-for-byte equal --
    not by construction (the eager path does not just echo its input; the
    lazy path independently reaches the identical builder), but because
    both now run through the SAME shared function
    (``RelayMcpRuntime._terminal_completed_result``)."""

    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    runtime = RelayMcpRuntime(settings=settings, profile="user", queue=queue)
    tool = _make_tool(runtime)

    wait_calls: list[JSON] = []

    def fake_wait_mcp_job(arguments: JSON, **_kwargs: object) -> JSON:
        wait_calls.append(dict(arguments))
        assert arguments["job_id"] == _LIVE_CREATE_PIPELINE_JOB_ID
        assert arguments["cluster"] == "ares-p5run2"
        return json.loads(json.dumps(_LIVE_CREATE_PIPELINE_WAIT_DOCUMENT))

    def fake_status_mcp_job(arguments: JSON, **_kwargs: object) -> JSON:
        assert arguments["job_id"] == _LIVE_CREATE_PIPELINE_JOB_ID
        return {
            "job": _live_relay_job(
                _LIVE_CREATE_PIPELINE_JOB_ID,
                state=JobState.SUCCEEDED,
                tool="jarvis_create_pipeline",
            )
        }

    monkeypatch.setattr(fastmcp_server_module, "wait_mcp_job", fake_wait_mcp_job)
    monkeypatch.setattr(fastmcp_server_module, "status_mcp_job", fake_status_mcp_job)

    async def scenario() -> None:
        # Eager: create_task's own promotion.
        saved = await runtime.create_task(
            tool=tool,
            arguments={"pipeline_id": "phase-d-stage-check-1786484613"},
            result=ToolResult(
                content=[mcp_types.TextContent(type="text", text="done")],
                structured_content=_LIVE_CREATE_PIPELINE_RECEIPT,
            ),
        )
        assert saved is not None
        assert len(wait_calls) == 1
        eager = saved.projection.completed_result
        assert eager is not None
        assert eager["isError"] is False
        # The flat create-time receipt's own top-level shape must NOT leak
        # through -- this is the exact C1 regression.
        assert "job_id" not in eager["structuredContent"]
        assert eager["structuredContent"]["job"]["state"] == "succeeded"
        assert "relay_queue" in eager["structuredContent"]

        # Lazy: a SEPARATE, freshly created non-terminal record for the SAME
        # job, driven through the real task_status() first-poll path.
        wait_calls.clear()
        lazy_projection = RelayMcpTaskProjection(
            tool_name=tool.name,
            profile="user",
            arguments={"pipeline_id": "phase-d-stage-check-1786484613"},
            catalog_revision=None,
            initial_result=_LIVE_CREATE_PIPELINE_RECEIPT,
        )
        lazy_record = RelayMcpTaskRecord(
            task_id=_LIVE_CREATE_PIPELINE_JOB_ID,
            job_id=_LIVE_CREATE_PIPELINE_JOB_ID,
            state=JobState.RUNNING,
            projection=lazy_projection,
        )
        await asyncio.to_thread(queue.put_mcp_task, lazy_record)
        lazy = await runtime.task_status(lazy_record)
        assert len(wait_calls) == 1
        assert lazy.status == "completed"

        # The A/B proof: identical underlying job, two different resolution
        # paths (eager at create time, lazy at first poll) -- byte-for-byte
        # equal.
        assert eager == lazy.result

        # Zero extra round trips on the NEXT poll of the EAGER task: once
        # promoted, task_status must serve completed_result straight off
        # the projection, never touching wait_mcp_job/status_mcp_job again.
        def must_not_be_called(*_args: object, **_kwargs: object) -> object:
            raise AssertionError("status_mcp_job/wait_mcp_job must not be called")

        monkeypatch.setattr(fastmcp_server_module, "status_mcp_job", must_not_be_called)
        monkeypatch.setattr(fastmcp_server_module, "wait_mcp_job", must_not_be_called)
        observed = await runtime.task_status(saved)
        assert observed.status == "completed"
        assert observed.result == eager

        # Sibling: a non-terminal receipt gets no completed_result at all.
        queued_task = await runtime.create_task(
            tool=tool,
            arguments={"pipeline_id": "q"},
            result=ToolResult(
                content=[mcp_types.TextContent(type="text", text="queued")],
                structured_content={
                    "job_id": "job-not-yet-terminal",
                    "state": JobState.QUEUED.value,
                    "terminal": False,
                },
            ),
        )
        assert queued_task is not None
        assert queued_task.projection.completed_result is None

    asyncio.run(scenario())


def test_create_task_eager_promotion_preserves_a_failed_dispatchs_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """C1's failure half: a FAILED job's eager promotion must preserve the
    SAME failure evidence (``protocol_error``/``returncode``/
    ``protocol_result.isError``) the lazy path would carry, so the client's
    ``raise_remote_call_failure`` still finds it and raises the typed
    ``jarvis_remote_call_failed`` -- never a masked success. Fixture is the
    live failed-describe job's own captured receipt and wait document
    (job_3544b072....json), which genuinely reached ``completed_result`` in
    production."""

    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    runtime = RelayMcpRuntime(settings=settings, profile="user", queue=queue)
    tool = _make_tool(runtime)

    def fake_wait_mcp_job(arguments: JSON, **_kwargs: object) -> JSON:
        assert arguments["job_id"] == _LIVE_FAILED_DESCRIBE_JOB_ID
        return json.loads(json.dumps(_LIVE_FAILED_DESCRIBE_WAIT_DOCUMENT))

    monkeypatch.setattr(fastmcp_server_module, "wait_mcp_job", fake_wait_mcp_job)

    async def scenario() -> None:
        saved = await runtime.create_task(
            tool=tool,
            arguments={"target": "package", "package_name": "clio_relay.bounded_command"},
            result=ToolResult(
                content=[mcp_types.TextContent(type="text", text="failed")],
                structured_content=_LIVE_FAILED_DESCRIBE_RECEIPT,
            ),
        )
        assert saved is not None
        eager = saved.projection.completed_result
        assert eager is not None
        # The promotion itself must classify this as a failure...
        assert eager["isError"] is True
        # ...and the evidence clio-agent's raise_remote_call_failure keys on
        # must survive intact: protocol_error, non-zero returncode, and the
        # remote tool's own rejection message under mcp_result.
        mcp_result = eager["structuredContent"]["mcp_result"]
        assert mcp_result["protocol_error"] == "tools/call returned isError=true"
        assert mcp_result["returncode"] == 1
        assert mcp_result["protocol_result"]["isError"] is True
        assert (
            _LIVE_FAILED_DESCRIBE_REMOTE_MESSAGE
            in (mcp_result["protocol_result"]["content"][0]["text"])
        )
        # C1's shape claim, not just the outcome: the failed eager document
        # must ALSO be the nested job/relay_queue wait shape, never the flat
        # create-time receipt (whose top-level carries job_id directly).
        assert "job_id" not in eager["structuredContent"]
        assert eager["structuredContent"]["job"]["state"] == "failed"

        # A/B: the SAME live wait document, resolved through the real lazy
        # task_status() path for a separate freshly created record, must be
        # byte-for-byte identical to the eager one above.
        lazy_projection = RelayMcpTaskProjection(
            tool_name=tool.name,
            profile="user",
            arguments={"target": "package", "package_name": "clio_relay.bounded_command"},
            catalog_revision=None,
            initial_result=_LIVE_FAILED_DESCRIBE_RECEIPT,
        )
        lazy_record = RelayMcpTaskRecord(
            task_id=_LIVE_FAILED_DESCRIBE_JOB_ID,
            job_id=_LIVE_FAILED_DESCRIBE_JOB_ID,
            state=JobState.RUNNING,
            projection=lazy_projection,
        )
        await asyncio.to_thread(queue.put_mcp_task, lazy_record)

        def fake_status_mcp_job(arguments: JSON, **_kwargs: object) -> JSON:
            assert arguments["job_id"] == _LIVE_FAILED_DESCRIBE_JOB_ID
            return {
                "job": _live_relay_job(
                    _LIVE_FAILED_DESCRIBE_JOB_ID,
                    state=JobState.FAILED,
                    tool="jarvis_describe",
                )
            }

        monkeypatch.setattr(fastmcp_server_module, "status_mcp_job", fake_status_mcp_job)
        lazy = await runtime.task_status(lazy_record)
        assert lazy.status == "completed"
        assert eager == lazy.result

        # task_status must report the SAME "completed, isError=True" shape
        # -- SEP-2663 "completed" means the TASK finished; the tool-level
        # failure rides inside the result's own isError, exactly as the
        # lazy path already reports it -- straight off the promoted
        # projection, with zero extra round trips. (RELAY_STATE_MAP maps
        # "tool_failure" -> status "completed"; only a relay-level
        # ``protocol_error`` maps to status "failed".)
        def must_not_be_called(*_args: object, **_kwargs: object) -> object:
            raise AssertionError("status_mcp_job/wait_mcp_job must not be called")

        monkeypatch.setattr(fastmcp_server_module, "status_mcp_job", must_not_be_called)
        monkeypatch.setattr(fastmcp_server_module, "wait_mcp_job", must_not_be_called)
        observed = await runtime.task_status(saved)
        assert observed.status == "completed"
        assert observed.result == eager
        assert observed.result is not None
        assert observed.result["isError"] is True

    asyncio.run(scenario())


def test_create_task_does_not_promote_a_cancelled_at_birth_job(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """C3: a job CANCELED before its claim was minted must never be reported
    as ``completed`` -- ``create_task`` must not promote a
    ``completed_result`` for it at all, so ``task_status`` keeps reaching
    its honest ``cancelled`` branch instead of the shadowed
    ``completed_result is not None`` branch."""

    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    runtime = RelayMcpRuntime(settings=settings, profile="user", queue=queue)
    tool = _make_tool(runtime)

    def must_not_be_called(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("a cancelled-at-birth job must never re-derive a result")

    monkeypatch.setattr(fastmcp_server_module, "wait_mcp_job", must_not_be_called)

    def fake_status_mcp_job(arguments: JSON, **_kwargs: object) -> JSON:
        return {
            "job": _live_relay_job(
                cast(str, arguments["job_id"]),
                state=JobState.CANCELED,
                tool="jarvis_describe",
            )
        }

    monkeypatch.setattr(fastmcp_server_module, "status_mcp_job", fake_status_mcp_job)

    cancelled_receipt: JSON = {
        "cluster": "ares-p5run2",
        "job_id": "job-cancelled-at-birth",
        "state": JobState.CANCELED.value,
        "kind": JobKind.MCP_CALL.value,
        "terminal": True,
        "remote": True,
        "route_revision": "e701bebd118f9bbdec5f76f13c742ecd412f29aec39c66b93757fe65ae14a141",
        "last_error": "cancelled before claim",
    }

    async def scenario() -> None:
        saved = await runtime.create_task(
            tool=tool,
            arguments={"target": "package"},
            result=ToolResult(
                content=[mcp_types.TextContent(type="text", text="cancelled")],
                structured_content=cancelled_receipt,
            ),
        )
        assert saved is not None
        assert saved.state is JobState.CANCELED
        # C3: no completed_result was promoted -- wait_mcp_job asserted it
        # was never called, above.
        assert saved.projection.completed_result is None

        observed = await runtime.task_status(saved)
        assert observed.status == "cancelled"
        assert observed.result is None

    asyncio.run(scenario())


def test_create_task_degrades_to_lazy_resolution_when_eager_transport_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """N1: the eager ``completed_result`` promotion C1 introduced performs a
    network round trip (``wait_mcp_job``) INSIDE ``create_task``, reached
    from ``intercept_tool_call`` with no surrounding try/except. Before this
    fix, a transport failure there killed the whole ``tools/call`` AND left
    no durable task record at all -- relay#215's own defect class,
    relocated out of ``_handle_get`` (already guarded by D7) and into
    ``create_task`` (not).

    The fix wraps ONLY the eager resolution: on failure it logs a typed
    reason and leaves ``completed_result`` as ``None`` -- exactly what a
    non-terminal job's projection looks like -- so the lazy ``tasks/get``
    path resolves it on first poll to the identical document C1 proved the
    eager path would have produced. Degradation is safe, not silent, and
    never a lost dispatch."""

    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    runtime = RelayMcpRuntime(settings=settings, profile="user", queue=queue)
    tool = _make_tool(runtime)

    def failing_wait_mcp_job(arguments: JSON, **_kwargs: object) -> JSON:
        raise RuntimeError("ssh channel closed during the post-dispatch re-derivation")

    monkeypatch.setattr(fastmcp_server_module, "wait_mcp_job", failing_wait_mcp_job)

    async def scenario() -> None:
        with caplog.at_level("ERROR", logger="clio_relay.fastmcp_server"):
            saved = await runtime.create_task(
                tool=tool,
                arguments={"pipeline_id": "phase-d-stage-check-1786484613"},
                result=ToolResult(
                    content=[mcp_types.TextContent(type="text", text="done")],
                    structured_content=_LIVE_CREATE_PIPELINE_RECEIPT,
                ),
            )

        # tools/call succeeds: create_task returns a record instead of the
        # transport failure escaping out through intercept_tool_call.
        assert saved is not None
        assert saved.state is JobState.SUCCEEDED
        # The eager resolution failed -- typed and logged, not silent -- and
        # promoted no completed_result rather than a lost dispatch.
        assert saved.projection.completed_result is None
        # The durable task record was still written: N1's regression was a
        # failure escaping BEFORE ``put_mcp_task`` was ever reached.
        assert queue.get_mcp_task(saved.task_id).task_id == saved.task_id

        # A typed, queryable reason reached the log (no-silent-fallback),
        # with the real underlying transport failure attached as a
        # traceback rather than swallowed.
        deferred_records = [
            record
            for record in caplog.records
            if record.name == "clio_relay.fastmcp_server" and record.exc_info is not None
        ]
        assert deferred_records, "expected the eager-resolution failure to log a traceback"
        assert "mcp_task_eager_result_deferred" in deferred_records[-1].message
        exc_info = deferred_records[-1].exc_info
        assert exc_info is not None
        logged_exc_type = exc_info[0]
        assert logged_exc_type is not None and issubclass(logged_exc_type, RuntimeError)

        # The lazy path is fully intact: once transport recovers, the FIRST
        # ``tasks/get`` resolves the SAME job to the identical document C1
        # proved the eager path would have produced for this exact fixture.
        def recovered_wait_mcp_job(arguments: JSON, **_kwargs: object) -> JSON:
            assert arguments["job_id"] == _LIVE_CREATE_PIPELINE_JOB_ID
            return json.loads(json.dumps(_LIVE_CREATE_PIPELINE_WAIT_DOCUMENT))

        def fake_status_mcp_job(arguments: JSON, **_kwargs: object) -> JSON:
            assert arguments["job_id"] == _LIVE_CREATE_PIPELINE_JOB_ID
            return {
                "job": _live_relay_job(
                    _LIVE_CREATE_PIPELINE_JOB_ID,
                    state=JobState.SUCCEEDED,
                    tool="jarvis_create_pipeline",
                )
            }

        monkeypatch.setattr(fastmcp_server_module, "wait_mcp_job", recovered_wait_mcp_job)
        monkeypatch.setattr(fastmcp_server_module, "status_mcp_job", fake_status_mcp_job)
        lazy = await runtime.task_status(saved)
        assert lazy.status == "completed"
        assert lazy.result is not None
        assert "job_id" not in lazy.result["structuredContent"]
        assert lazy.result["structuredContent"]["job"]["state"] == "succeeded"
        assert "relay_queue" in lazy.result["structuredContent"]

    asyncio.run(scenario())


def test_task_get_wraps_a_status_reconciliation_failure_as_a_typed_error(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """D7 (relay#215): ``task_status``'s re-derivation path had no
    try/except in ``_handle_get``, so any exception raised inside it -- a
    real, observed intermittent relay#215 failure -- escaped as a bare,
    typeless "Internal server error" via the SDK's generic handler
    catch-all (``mcp/server/runner.py``, only ``MCPError``/``ValidationError``
    are mapped; everything else returns ``None`` and gets replaced by that
    literal string). Reproduced here without reaching relay's SSH-tunneled
    remote path at all: a task record whose backing job was never durably
    written drives ``task_status``'s local re-derivation branch straight
    into ``queue.get_job``'s natural ``NotFoundError``, which must now
    surface as a typed, queryable ``MCPError`` instead of an opaque crash."""

    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
    )
    queue = ClioCoreQueue(settings.core_dir)
    runtime = RelayMcpRuntime(settings=settings, profile="user", queue=queue)
    definitions, _catalog = mcp_tool_definitions_and_remote_catalog(profile="user")
    definition = next(item for item in definitions if item["name"] == "relay_submit_agent")
    tool = RelayTool(
        definition,
        runtime=runtime,
        catalog_revision=None,
        task_capable=True,
    )
    server: FastMCP[dict[str, Any]] = FastMCP(
        "relay-task-reconciliation-test",
        tools=[tool],
        lifespan=runtime.lifespan,
        tasks=False,
        strict_input_validation=True,
    )
    server.add_extension(RelayTasksExtension(runtime))

    async def scenario() -> None:
        saved = await runtime.create_task(
            tool=tool,
            arguments={"value": "orphan"},
            result=ToolResult(
                content=[mcp_types.TextContent(type="text", text="queued")],
                structured_content={
                    "job_id": "job-never-durably-written",
                    "state": JobState.QUEUED.value,
                    "terminal": False,
                },
            ),
        )
        assert saved is not None

        with caplog.at_level("ERROR", logger="clio_relay.fastmcp_server"):
            async with Client(server, mode="auto") as client:
                with pytest.raises(MCPError) as failure:
                    await client.session.send_request(
                        GetTaskRequest(params=GetTaskRequestParams(task_id=saved.task_id)),
                        ClientGetTaskResult,
                    )
        assert failure.value.code == mcp_types.INTERNAL_ERROR
        assert failure.value.data == {
            "reason": "mcp_task_status_reconciliation_failed",
            "task_id": saved.task_id,
        }
        # D7 polish: the wire message is generic -- the typed reason and
        # task_id in ``data`` are the queryable signal, never handler
        # internals (``str(exc)``) or even the task_id folded into prose.
        assert failure.value.message == "relay could not reconcile this task's status."
        assert saved.task_id not in failure.value.message
        # D7 polish: raising ``MCPError`` short-circuits the SDK's own
        # ``logger.exception(...)`` (it only fires for handler exceptions
        # left unmapped), so the traceback must be logged here or it is
        # lost server-side entirely -- prove it actually is.
        reconciliation_records = [
            record
            for record in caplog.records
            if record.name == "clio_relay.fastmcp_server" and record.exc_info is not None
        ]
        assert reconciliation_records, "expected the reconciliation failure to log a traceback"
        exc_info = reconciliation_records[-1].exc_info
        assert exc_info is not None
        logged_exc_type = exc_info[0]
        assert logged_exc_type is not None and issubclass(logged_exc_type, NotFoundError)

    asyncio.run(scenario())


def test_mcp_task_family_is_additive_and_collected_with_its_job(tmp_path: Path) -> None:
    core_dir = tmp_path / "core"
    queue = ClioCoreQueue(core_dir)
    queue.initialize()
    task_dir = core_dir / "mcp_tasks"
    assert task_dir.is_dir()

    task_dir.rmdir()
    queue = ClioCoreQueue(core_dir)
    queue.initialize()
    assert task_dir.is_dir()

    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.REMOTE_AGENT,
            spec=RemoteAgentTaskSpec(prompt_path="/remote/prompt.md"),
            idempotency_key="mcp-task-gc",
        )
    )
    queue.put_mcp_task(
        RelayMcpTaskRecord(
            task_id=job.job_id,
            job_id=job.job_id,
            state=job.state,
            projection=RelayMcpTaskProjection(
                tool_name="relay_submit_agent",
                profile="user",
                arguments={"cluster": "test-cluster"},
                initial_result={
                    "job_id": job.job_id,
                    "state": job.state.value,
                    "terminal": False,
                },
            ),
        )
    )
    queue.update_job_state(job.job_id, JobState.SUCCEEDED)

    for _attempt in range(100):
        collected = queue.collect_terminal_job(
            job.job_id,
            execute=True,
            batch_size=3,
            external_quarantine_id=f"test-quarantine:{job.job_id}",
        )
        if collected.complete:
            break
    else:
        raise AssertionError("terminal MCP task GC did not complete")

    with pytest.raises(NotFoundError):
        queue.get_mcp_task(job.job_id)


def test_guarded_input_round_completes_before_relay_task_creation(
    tmp_path: Path,
) -> None:
    settings = RelaySettings(
        core_dir=tmp_path / "core",
        spool_dir=tmp_path / "spool",
    )
    queue = ClioCoreQueue(settings.core_dir)
    elicitation_count = 0

    async def approve(
        _message: str,
        _response_type: type[Any] | None,
        _params: mcp_types.ElicitRequestParams,
        _context: Any,
    ) -> dict[str, bool]:
        nonlocal elicitation_count
        elicitation_count += 1
        return {"approved": True}

    async def scenario() -> None:
        async with Client(
            _guarded_task_server(settings, queue),
            mode="auto",
            elicitation_handler=approve,
        ) as client:
            pending = asyncio.create_task(
                client.call_tool(
                    "relay_submit_agent",
                    _submit_arguments(tmp_path, "guarded"),
                )
            )
            for _attempt in range(1_000):
                jobs = await asyncio.to_thread(queue.list_jobs)
                if jobs:
                    break
                await asyncio.sleep(0.01)
            else:
                raise AssertionError("guarded relay job was not admitted")
            assert elicitation_count == 1
            queue.update_job_state(jobs[0].job_id, JobState.SUCCEEDED)
            result = await asyncio.wait_for(pending, timeout=5)
            assert result.is_error is False
            assert queue.get_mcp_task(jobs[0].job_id).task_id == jobs[0].job_id

    asyncio.run(scenario())

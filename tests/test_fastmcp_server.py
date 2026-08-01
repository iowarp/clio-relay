from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any, BinaryIO

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
from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.errors import NotFoundError, QueueConflictError
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
        "clio_relay.core_queue.open_private_atomic_file",
        _open_atomic,
    )
    monkeypatch.setattr(
        "clio_relay.mcp_server._optional_cluster_definition",
        lambda _cluster: None,
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

            ordinary = await client.call_tool(
                "relay_submit_agent",
                _submit_arguments(tmp_path, "ordinary-agent"),
            )
            assert ordinary.is_error is False
            assert ordinary.structured_content is not None
            ordinary_job_id = ordinary.structured_content["job_id"]
            assert queue.get_job(ordinary_job_id).job_id == ordinary_job_id
            with pytest.raises(NotFoundError):
                queue.get_mcp_task(ordinary_job_id)

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

        with pytest.raises(QueueConflictError, match="different semantics"):
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

from __future__ import annotations

import base64
import hashlib
import json
from datetime import timedelta
from io import StringIO
from pathlib import Path

import pytest

from clio_relay.cluster_config import ClusterDefinition, ClusterRegistry
from clio_relay.config import RelaySettings
from clio_relay.core_queue import ClioCoreQueue
from clio_relay.mcp_server import (
    handle_request,
    render_agent_mcp_profile,
    render_codex_mcp_profile,
    serve_stdio,
)
from clio_relay.models import (
    ArtifactRef,
    Cursor,
    EndpointRegistration,
    EndpointRole,
    JarvisRunSpec,
    JobKind,
    JobState,
    McpCallSpec,
    RelayJob,
    RelayTask,
    RemoteAgentTaskSpec,
    utc_now,
)
from clio_relay.spool import JobSpool


@pytest.fixture(autouse=True)
def _admin_profile_for_operational_dispatch_tests(  # pyright: ignore[reportUnusedFunction]
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Exercise legacy low-level dispatch tests through the admin profile."""
    monkeypatch.setenv("CLIO_RELAY_MCP_PROFILE", "admin")


def _configure_local_cluster(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    name: str,
) -> None:
    registry_path = tmp_path / "clusters.json"
    ClusterRegistry(clusters={name: ClusterDefinition(name=name, ssh_host="localhost")}).save(
        registry_path
    )
    monkeypatch.setenv("CLIO_RELAY_CLUSTER_REGISTRY", str(registry_path))
    monkeypatch.setenv("CLIO_RELAY_CLI_MODE", "local")

    def artifact_binding(_cluster: str) -> str:
        return "a" * 64

    monkeypatch.setattr(
        "clio_relay.mcp_server.jarvis_mcp_artifact_binding",
        artifact_binding,
    )


def test_mcp_lists_relay_tools(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    queue = ClioCoreQueue(tmp_path / "core")

    response = handle_request(
        {"jsonrpc": "2.0", "id": 1, "method": "tools/list"},
        queue=queue,
        profile="user",
    )

    assert response is not None
    tool_names = {tool["name"] for tool in response["result"]["tools"]}
    assert tool_names == {
        "relay_remote_mcp_context",
        "relay_submit_agent",
        "relay_status",
        "relay_cancel",
        "relay_observe",
        "relay_wait",
        "relay_queue_list",
        "relay_queue_diagnose",
        "relay_queue_stale",
        "relay_storage_status",
        "jarvis_create_pipeline",
        "jarvis_describe",
        "jarvis_add_step",
        "jarvis_edit_step",
        "jarvis_run",
    }
    assert "jarvis_create_pipeline" in tool_names
    create_pipeline_tool = next(
        tool for tool in response["result"]["tools"] if tool["name"] == "jarvis_create_pipeline"
    )
    assert create_pipeline_tool["inputSchema"]["required"] == ["cluster", "pipeline_id"]
    assert "pipeline_id" in create_pipeline_tool["inputSchema"]["properties"]
    edit_step_tool = next(
        tool for tool in response["result"]["tools"] if tool["name"] == "jarvis_edit_step"
    )
    assert edit_step_tool["inputSchema"]["properties"]["operation"]["enum"] == [
        "edit",
        "remove",
    ]
    assert "config" not in edit_step_tool["inputSchema"]["required"]
    assert edit_step_tool["outputSchema"]["properties"]["kind"] == {
        "type": "string",
        "const": "mcp_call",
    }
    run_tool = next(tool for tool in response["result"]["tools"] if tool["name"] == "jarvis_run")
    spack_specs = run_tool["inputSchema"]["properties"]["spack_specs"]
    assert spack_specs["default"] is None
    assert spack_specs["anyOf"][0] == {
        "type": "array",
        "items": {"type": "string"},
    }
    assert "$defs" not in run_tool["inputSchema"]
    execution = run_tool["inputSchema"]["properties"]["execution"]["anyOf"][0]
    assert execution["properties"]["mode"]["enum"] == [
        "auto",
        "local",
        "direct",
        "cluster",
        "scheduler",
        "hostfile",
    ]
    diagnose_tool = next(
        tool for tool in response["result"]["tools"] if tool["name"] == "relay_queue_diagnose"
    )
    assert diagnose_tool["inputSchema"]["required"] == ["job_id"]
    stale_tool = next(
        tool for tool in response["result"]["tools"] if tool["name"] == "relay_queue_stale"
    )
    assert stale_tool["inputSchema"]["required"] == ["cluster", "older_than_seconds"]
    for name in ("relay_observe", "relay_wait"):
        log_tool = next(tool for tool in response["result"]["tools"] if tool["name"] == name)
        assert log_tool["inputSchema"]["properties"]["log_limit"]["maximum"] == 1_048_576


def test_mcp_admin_profile_lists_operational_tools(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path / "core")

    response = handle_request(
        {"jsonrpc": "2.0", "id": 1, "method": "tools/list"},
        queue=queue,
        profile="admin",
    )

    assert response is not None
    tool_names = {tool["name"] for tool in response["result"]["tools"]}
    assert "relay_submit_jarvis_pipeline" in tool_names
    assert "relay_submit_remote_agent" in tool_names
    assert "relay_submit_mcp_call" in tool_names
    assert "relay_get_job" in tool_names
    assert "relay_get_job_status" in tool_names
    assert "relay_monitor_job" in tool_names
    assert "relay_queue_list" in tool_names
    assert "relay_create_gateway_session" in tool_names
    create_gateway_tool = next(
        tool
        for tool in response["result"]["tools"]
        if tool["name"] == "relay_create_gateway_session"
    )
    state_enum = create_gateway_tool["inputSchema"]["properties"]["state"]["enum"]
    assert "allocated" in state_enum


def test_mcp_event_monitor_and_log_schemas_publish_strict_maximums(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path / "core")

    response = handle_request(
        {"jsonrpc": "2.0", "id": 1, "method": "tools/list"},
        queue=queue,
        profile="all",
    )

    assert response is not None
    tools = {tool["name"]: tool for tool in response["result"]["tools"]}
    for name in (
        "relay_observe",
        "relay_monitor_job",
        "relay_watch_job_events",
        "relay_watch_task_events",
        "relay_evaluate_monitor_rules",
    ):
        assert tools[name]["inputSchema"]["properties"]["limit"]["maximum"] == 500
    assert tools["relay_read_job_log"]["inputSchema"]["properties"]["limit"]["maximum"] == 1_048_576


def test_mcp_event_and_monitor_handlers_reject_huge_limits_when_schema_is_bypassed(
    tmp_path: Path,
) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml="name: generic\npkgs: []\n"),
            idempotency_key="mcp-huge-page",
        )
    )
    task = queue.append_task(RelayTask(job_id=job.job_id, name="bounded.events"))
    calls = [
        ("relay_observe", {"job_id": job.job_id, "limit": 10**12}),
        ("relay_monitor_job", {"job_id": job.job_id, "limit": 10**12}),
        ("relay_watch_job_events", {"job_id": job.job_id, "limit": 10**12}),
        ("relay_watch_task_events", {"task_id": task.task_id, "limit": 10**12}),
        ("relay_evaluate_monitor_rules", {"limit": 10**12}),
    ]

    for request_id, (name, arguments) in enumerate(calls, start=1):
        response = handle_request(
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "method": "tools/call",
                "params": {"name": name, "arguments": arguments},
            },
            queue=queue,
            settings=settings,
            profile="all",
        )
        assert response is not None
        assert response["error"]["message"] == "limit must be between 1 and 500"

    log_response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 99,
            "method": "tools/call",
            "params": {
                "name": "relay_read_job_log",
                "arguments": {
                    "job_id": job.job_id,
                    "stream": "stdout",
                    "limit": 1_048_577,
                },
            },
        },
        queue=queue,
        settings=settings,
        profile="all",
    )
    assert log_response is not None
    assert log_response["error"]["message"] == "limit must be between 1 and 1048576"


def test_mcp_user_profile_rejects_direct_call_to_hidden_tool(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path / "core")

    response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {
                "name": "relay_submit_mcp_call",
                "arguments": {
                    "cluster": "test-cluster",
                    "server": "unregistered-server",
                    "tool": "run",
                    "arguments": {},
                },
            },
        },
        queue=queue,
        profile="user",
    )

    assert response is not None
    assert response["error"]["code"] == -32000
    assert response["error"]["message"] == (
        "tool is not available in MCP profile 'user': relay_submit_mcp_call"
    )
    assert queue.list_jobs() == []


def test_mcp_remote_mcp_context_describes_virtual_tools(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path / "core")

    response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 11,
            "method": "tools/call",
            "params": {"name": "relay_remote_mcp_context", "arguments": {}},
        },
        queue=queue,
    )

    assert response is not None
    context = response["result"]["structuredContent"]["context"]
    assert "jarvis_create_pipeline" in context
    assert "cluster argument" in context


def test_mcp_submit_jarvis_pipeline_creates_real_job(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    pipeline_yaml = "name: generic\npkgs: []\n"

    response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {
                "name": "relay_submit_jarvis_pipeline",
                "arguments": {
                    "cluster": "test-cluster",
                    "pipeline_yaml": pipeline_yaml,
                },
            },
        },
        queue=queue,
    )

    assert response is not None
    result = response["result"]["structuredContent"]
    job = queue.get_job(result["job_id"])
    assert job.cluster == "test-cluster"
    assert job.kind == JobKind.JARVIS
    assert isinstance(job.spec, JarvisRunSpec)
    assert job.spec.pipeline_yaml == pipeline_yaml


def test_mcp_submit_jarvis_job_creates_named_pipeline_job(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path / "core")

    response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 12,
            "method": "tools/call",
            "params": {
                "name": "relay_submit_jarvis_job",
                "arguments": {
                    "cluster": "test-cluster",
                    "pipeline_name": "site_simulation_4node",
                    "idempotency_key": "mcp-named-pipeline",
                },
            },
        },
        queue=queue,
    )

    assert response is not None
    result = response["result"]["structuredContent"]
    job = queue.get_job(result["job_id"])
    assert isinstance(job.spec, JarvisRunSpec)
    assert job.spec.pipeline_name == "site_simulation_4node"


def test_mcp_submit_remote_agent_creates_real_job(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    prompt_path = tmp_path / "prompt.md"
    mcp_config_path = tmp_path / "mcp.toml"

    response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 21,
            "method": "tools/call",
            "params": {
                "name": "relay_submit_remote_agent",
                "arguments": {
                    "cluster": "test-cluster",
                    "prompt_path": str(prompt_path),
                    "mcp_config_path": str(mcp_config_path),
                    "model": "configured-model",
                    "workdir": str(tmp_path),
                    "timeout_seconds": 30,
                    "idempotency_key": "remote-agent-tool",
                },
            },
        },
        queue=queue,
    )

    assert response is not None
    result = response["result"]["structuredContent"]
    job = queue.get_job(result["job_id"])
    assert job.kind == JobKind.REMOTE_AGENT
    assert isinstance(job.spec, RemoteAgentTaskSpec)
    assert job.spec.prompt_path == str(prompt_path)
    assert job.spec.mcp_config_path == str(mcp_config_path)
    assert job.spec.model == "configured-model"
    assert job.spec.timeout_seconds == 30


def test_mcp_compact_submit_agent_creates_real_job(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    prompt_path = tmp_path / "prompt.md"

    response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 28,
            "method": "tools/call",
            "params": {
                "name": "relay_submit_agent",
                "arguments": {
                    "cluster": "test-cluster",
                    "prompt_path": str(prompt_path),
                    "timeout_seconds": 45,
                    "idempotency_key": "compact-agent-tool",
                },
            },
        },
        queue=queue,
    )

    assert response is not None
    result = response["result"]["structuredContent"]
    job = queue.get_job(result["job_id"])
    assert job.kind == JobKind.REMOTE_AGENT
    assert isinstance(job.spec, RemoteAgentTaskSpec)
    assert job.spec.prompt_path == str(prompt_path)
    assert job.spec.timeout_seconds == 45


def test_mcp_compact_status_observe_wait_and_cancel(tmp_path: Path) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml="name: generic\npkgs: []\n"),
            idempotency_key="compact-observe",
        )
    )
    spool = JobSpool(settings.spool_dir, job)
    spool.initialize()
    spool.append_stdout("step 25\nfinished\n")
    spool.append_stderr("warning: none\n")
    queue.append_event(job.job_id, "stdout.delta", "progress event", payload={"text": "progress\n"})

    status_response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 29,
            "method": "tools/call",
            "params": {"name": "relay_status", "arguments": {"job_id": job.job_id}},
        },
        queue=queue,
        settings=settings,
    )
    observe_response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 30,
            "method": "tools/call",
            "params": {
                "name": "relay_observe",
                "arguments": {"job_id": job.job_id, "pattern": r"step\s+(?P<step>\d+)"},
            },
        },
        queue=queue,
        settings=settings,
    )
    queue.update_job_state(job.job_id, JobState.SUCCEEDED, message="done")
    wait_response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 31,
            "method": "tools/call",
            "params": {
                "name": "relay_wait",
                "arguments": {"job_id": job.job_id, "timeout_seconds": 1},
            },
        },
        queue=queue,
        settings=settings,
    )
    cancel_response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 32,
            "method": "tools/call",
            "params": {"name": "relay_cancel", "arguments": {"job_id": job.job_id}},
        },
        queue=queue,
        settings=settings,
    )

    assert status_response is not None
    assert status_response["result"]["structuredContent"]["job"]["job_id"] == job.job_id
    assert observe_response is not None
    observed = observe_response["result"]["structuredContent"]
    assert observed["matched"] is True
    assert observed["matches"][0]["source"] == "stdout"
    assert observed["matches"][0]["groupdict"] == {"step": "25"}
    assert "step 25" in observed["logs"]["stdout"]["text"]
    assert wait_response is not None
    waited = wait_response["result"]["structuredContent"]
    assert waited["terminal"] is True
    assert "finished" in waited["logs"]["stdout"]["text"]
    assert cancel_response is not None
    assert cancel_response["result"]["structuredContent"]["job_id"] == job.job_id


def test_mcp_compact_log_limit_is_enforced_before_log_access(tmp_path: Path) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(command=["echo", "hello"]),
            idempotency_key="compact-log-bound",
        )
    )

    response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 33,
            "method": "tools/call",
            "params": {
                "name": "relay_observe",
                "arguments": {
                    "job_id": job.job_id,
                    "log_limit": 1_048_577,
                },
            },
        },
        queue=queue,
        settings=settings,
    )

    assert response is not None
    assert response["error"]["message"] == "log_limit must be between 1 and 1048576"


def test_mcp_compact_job_handle_routes_remote_lifecycle_and_verifies_result(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry_path = tmp_path / "clusters.json"
    ClusterRegistry(clusters={"ares": ClusterDefinition(name="ares", ssh_host="ares-login")}).save(
        registry_path
    )
    monkeypatch.setenv("CLIO_RELAY_CLUSTER_REGISTRY", str(registry_path))
    queue = ClioCoreQueue(tmp_path / "desktop-core")
    settings = RelaySettings(core_dir=tmp_path / "desktop-core", spool_dir=tmp_path / "spool")
    job_id = "remote-job-1"
    payload = json.dumps(
        {
            "operation": "tools/call",
            "tool": "inspect",
            "returncode": 0,
            "timed_out": False,
            "protocol_error": None,
            "structured_result": {"count": 4},
            "protocol_result": {"structuredContent": {"count": 4}},
            "protocol_version": "2024-11-05",
            "server_info": {"name": "science"},
            "server_artifact": {"private": "must not cross the agent boundary"},
        },
        sort_keys=True,
    ).encode()
    artifact = {
        "artifact_id": "artifact-mcp-result",
        "job_id": job_id,
        "kind": "mcp_result",
        "sha256": hashlib.sha256(payload).hexdigest(),
    }
    envelope_data = {"value": base64.b64encode(payload).decode("ascii")}
    commands: list[list[str]] = []

    def run_remote(_definition: ClusterDefinition, args: list[str]) -> str:
        commands.append(args)
        if args[:2] == ["job", "status"]:
            return json.dumps(
                {
                    "job": {
                        "job_id": job_id,
                        "cluster": "ares",
                        "kind": "mcp_call",
                        "state": "succeeded",
                    },
                    "terminal": True,
                }
            )
        if args[:2] == ["job", "monitor"]:
            return json.dumps({"job": {"job_id": job_id}, "events": [], "terminal": True})
        if args[:2] == ["job", "read-log"]:
            stream = args[args.index("--stream") + 1]
            return json.dumps({"stream": stream, "text": f"{stream} text", "eof": True})
        if args[:2] == ["job", "list-artifacts"]:
            return json.dumps(
                {
                    "artifacts": [artifact],
                    "cursor": 1,
                    "limit": 500,
                    "next_cursor": None,
                    "total": 1,
                }
            )
        if args[:2] == ["job", "read-artifact"]:
            return json.dumps(
                {
                    "artifact": artifact,
                    "encoding": "base64",
                    "data": envelope_data["value"],
                }
            )
        if args[:2] in (["job", "wait"], ["job", "cancel"]):
            return ""
        raise AssertionError(f"unexpected remote command: {args}")

    monkeypatch.setattr("clio_relay.mcp_server.run_remote_clio", run_remote)

    status = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 40,
            "method": "tools/call",
            "params": {
                "name": "relay_status",
                "arguments": {"cluster": "ares", "job_id": job_id},
            },
        },
        queue=queue,
        settings=settings,
    )
    assert status is not None
    handle = status["result"]["structuredContent"]
    route_revision = handle["route_revision"]
    assert handle["cluster"] == "ares"

    observe = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 41,
            "method": "tools/call",
            "params": {
                "name": "relay_observe",
                "arguments": {
                    "cluster": "ares",
                    "route_revision": route_revision,
                    "job_id": job_id,
                },
            },
        },
        queue=queue,
        settings=settings,
    )
    wait = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 42,
            "method": "tools/call",
            "params": {
                "name": "relay_wait",
                "arguments": {
                    "cluster": "ares",
                    "route_revision": route_revision,
                    "job_id": job_id,
                    "timeout_seconds": 1,
                },
            },
        },
        queue=queue,
        settings=settings,
    )
    cancel = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 43,
            "method": "tools/call",
            "params": {
                "name": "relay_cancel",
                "arguments": {
                    "cluster": "ares",
                    "route_revision": route_revision,
                    "job_id": job_id,
                },
            },
        },
        queue=queue,
        settings=settings,
    )

    assert observe is not None and "error" not in observe
    assert wait is not None and "error" not in wait
    waited = wait["result"]["structuredContent"]
    assert waited["mcp_result"]["structured_result"] == {"count": 4}
    assert "server_artifact" not in waited["mcp_result"]
    assert waited["artifacts"] == [artifact]
    assert cancel is not None and "error" not in cancel
    canceled = cancel["result"]["structuredContent"]
    assert canceled["scheduler_policy"] == "relay-only"
    cancel_command = next(command for command in commands if command[:2] == ["job", "cancel"])
    assert cancel_command == ["job", "cancel", job_id]
    assert queue.list_jobs() == []

    envelope_data["value"] = base64.b64encode(b"{}").decode("ascii")
    tampered = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 44,
            "method": "tools/call",
            "params": {
                "name": "relay_wait",
                "arguments": {
                    "cluster": "ares",
                    "route_revision": route_revision,
                    "job_id": job_id,
                    "include_logs": False,
                },
            },
        },
        queue=queue,
        settings=settings,
    )
    assert tampered is not None
    assert "SHA-256 does not match" in tampered["error"]["message"]

    ClusterRegistry(
        clusters={"ares": ClusterDefinition(name="ares", ssh_host="new-ares-login")}
    ).save(registry_path)
    command_count = len(commands)
    stale_route = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 45,
            "method": "tools/call",
            "params": {
                "name": "relay_status",
                "arguments": {
                    "cluster": "ares",
                    "route_revision": route_revision,
                    "job_id": job_id,
                },
            },
        },
        queue=queue,
        settings=settings,
    )
    assert stale_route is not None
    assert "cluster route changed" in stale_route["error"]["message"]
    assert len(commands) == command_count


def test_virtual_jarvis_route_fails_closed_for_unknown_cluster(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry_path = tmp_path / "clusters.json"
    ClusterRegistry(clusters={"ares": ClusterDefinition(name="ares", ssh_host="localhost")}).save(
        registry_path
    )
    monkeypatch.setenv("CLIO_RELAY_CLUSTER_REGISTRY", str(registry_path))
    queue = ClioCoreQueue(tmp_path / "core")

    response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 46,
            "method": "tools/call",
            "params": {
                "name": "jarvis_describe",
                "arguments": {"cluster": "typo", "target": "packages"},
            },
        },
        queue=queue,
        profile="user",
    )

    assert response is not None
    assert response["error"]["message"] == "cluster is not configured: typo"
    assert queue.list_jobs() == []


def test_virtual_jarvis_route_fails_closed_without_discovered_artifact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry_path = tmp_path / "clusters.json"
    cache_path = tmp_path / "remote-mcp-cache.json"
    ClusterRegistry(clusters={"ares": ClusterDefinition(name="ares", ssh_host="localhost")}).save(
        registry_path
    )
    monkeypatch.setenv("CLIO_RELAY_CLUSTER_REGISTRY", str(registry_path))
    monkeypatch.setenv("CLIO_RELAY_REMOTE_MCP_CACHE", str(cache_path))
    queue = ClioCoreQueue(tmp_path / "core")

    response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 461,
            "method": "tools/call",
            "params": {
                "name": "jarvis_describe",
                "arguments": {"cluster": "ares", "target": "packages"},
            },
        },
        queue=queue,
        profile="user",
    )

    assert response is not None
    assert "run jarvis-mcp-refresh" in response["error"]["message"]
    assert queue.list_jobs() == []


def test_remote_virtual_jarvis_staged_arguments_are_removed_after_write_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    definition = ClusterDefinition(name="ares", ssh_host="ares-login")
    writes: list[str] = []
    removals: list[tuple[str, bool]] = []

    def remote_definition(_cluster: str) -> ClusterDefinition:
        return definition

    def artifact_binding(_cluster: str) -> str:
        return "a" * 64

    def write_remote(_definition: ClusterDefinition, path: str, _data: bytes) -> None:
        writes.append(path)
        raise RuntimeError("staging write failed")

    def remove_remote(
        _definition: ClusterDefinition,
        path: str,
        *,
        remove_empty_parent: bool = False,
    ) -> None:
        removals.append((path, remove_empty_parent))

    monkeypatch.setattr(
        "clio_relay.mcp_server._remote_cluster_definition",
        remote_definition,
    )
    monkeypatch.setattr(
        "clio_relay.mcp_server.jarvis_mcp_artifact_binding",
        artifact_binding,
    )
    monkeypatch.setattr(
        "clio_relay.mcp_server.write_remote_file",
        write_remote,
    )
    monkeypatch.setattr(
        "clio_relay.mcp_server.remove_remote_file",
        remove_remote,
    )

    def fail_remote(_definition: ClusterDefinition, _args: list[str]) -> str:
        raise AssertionError("remote launch must not follow a failed staged write")

    monkeypatch.setattr("clio_relay.mcp_server.run_remote_clio", fail_remote)

    response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 47,
            "method": "tools/call",
            "params": {
                "name": "jarvis_describe",
                "arguments": {"cluster": "ares", "target": "packages"},
            },
        },
        queue=queue,
        profile="user",
    )

    assert response is not None
    assert response["error"]["message"] == "staging write failed"
    assert len(writes) == 1
    assert removals == [(writes[0], True)]
    assert queue.list_jobs() == []


def test_mcp_records_and_watches_task_events(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.REMOTE_AGENT,
            spec=RemoteAgentTaskSpec(prompt_path="/tmp/prompt.md"),
            idempotency_key="mcp-task-events",
        )
    )
    task = queue.append_task(RelayTask(job_id=job.job_id, name="remote-agent.discovery"))

    record_response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 22,
            "method": "tools/call",
            "params": {
                "name": "relay_record_task_event",
                "arguments": {
                    "task_id": task.task_id,
                    "event_type": "dataset_found",
                    "label": "dataset",
                    "status": "succeeded",
                    "summary": "Found staged dataset",
                    "path_refs": ["/mnt/common/datasets/example_001"],
                },
            },
        },
        queue=queue,
    )
    watch_response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 23,
            "method": "tools/call",
            "params": {
                "name": "relay_watch_task_events",
                "arguments": {"task_id": task.task_id, "cursor": 1},
            },
        },
        queue=queue,
    )

    assert record_response is not None
    assert watch_response is not None
    recorded = record_response["result"]["structuredContent"]
    watched = watch_response["result"]["structuredContent"]
    assert recorded["seq"] == 1
    assert watched["events"][0]["event_type"] == "dataset_found"
    assert watched["next_cursor"] == 2


def test_mcp_gateway_session_lifecycle(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path / "core")

    create_response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 24,
            "method": "tools/call",
            "params": {
                "name": "relay_create_gateway_session",
                "arguments": {
                    "cluster": "test-cluster",
                    "name": "live-service-example",
                    "gateway": {"strategy": "ssh_forward", "remote_port": 11111},
                },
            },
        },
        queue=queue,
    )
    assert create_response is not None
    session_id = create_response["result"]["structuredContent"]["session_id"]

    update_response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 25,
            "method": "tools/call",
            "params": {
                "name": "relay_update_gateway_session",
                "arguments": {
                    "session_id": session_id,
                    "state": "ready",
                    "scheduler_job_id": "12345",
                    "node": "ares-comp-01",
                    "gateway": {"strategy": "ssh_forward", "local_port": 5900},
                },
            },
        },
        queue=queue,
    )
    close_response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 26,
            "method": "tools/call",
            "params": {
                "name": "relay_close_gateway_session",
                "arguments": {"session_id": session_id},
            },
        },
        queue=queue,
    )
    reopen_response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 27,
            "method": "tools/call",
            "params": {
                "name": "relay_update_gateway_session",
                "arguments": {"session_id": session_id, "state": "ready"},
            },
        },
        queue=queue,
    )

    assert update_response is not None
    assert close_response is not None
    assert reopen_response is not None
    assert update_response["result"]["structuredContent"]["state"] == "ready"
    assert update_response["result"]["structuredContent"]["gateway"]["local_port"] == 5900
    assert close_response["result"]["structuredContent"]["state"] == "closed"
    assert "cannot reopen closed gateway session" in reopen_response["error"]["message"]


def test_mcp_submit_mcp_call_creates_real_job_with_arguments(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path / "core")

    response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 22,
            "method": "tools/call",
            "params": {
                "name": "relay_submit_mcp_call",
                "arguments": {
                    "cluster": "test-cluster",
                    "server": "remote-tool-server",
                    "server_args": ["--stdio"],
                    "tool": "run",
                    "arguments": {"case": "site-simulation", "steps": 100},
                    "timeout_seconds": 60,
                    "idempotency_key": "mcp-call-tool",
                },
            },
        },
        queue=queue,
    )

    assert response is not None
    result = response["result"]["structuredContent"]
    job = queue.get_job(result["job_id"])
    assert job.kind == JobKind.MCP_CALL
    assert isinstance(job.spec, McpCallSpec)
    assert job.spec.server == "remote-tool-server"
    assert job.spec.server_args == ["--stdio"]
    assert job.spec.tool == "run"
    assert job.spec.arguments == {"case": "site-simulation", "steps": 100}
    assert job.spec.timeout_seconds == 60


def test_mcp_call_jarvis_mcp_uses_builtin_cluster_command(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path / "core")

    response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 23,
            "method": "tools/call",
            "params": {
                "name": "relay_call_jarvis_mcp",
                "arguments": {
                    "cluster": "test-cluster",
                    "tool": "jarvis_describe",
                    "arguments": {"target": "packages"},
                    "idempotency_key": "jarvis-mcp-tool",
                },
            },
        },
        queue=queue,
    )

    assert response is not None
    result = response["result"]["structuredContent"]
    job = queue.get_job(result["job_id"])
    assert isinstance(job.spec, McpCallSpec)
    assert job.spec.server == "uvx"
    assert job.spec.server_args == [
        "--from",
        "clio-kit==3.0.0",
        "clio-kit",
        "mcp-server",
        "jarvis",
    ]
    assert job.spec.tool == "jarvis_describe"
    assert job.spec.arguments == {"target": "packages"}


def test_mcp_virtual_jarvis_tool_routes_to_cluster_mcp(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_local_cluster(tmp_path, monkeypatch, "ares")
    queue = ClioCoreQueue(tmp_path / "core")

    response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 24,
            "method": "tools/call",
            "params": {
                "name": "jarvis_create_pipeline",
                "arguments": {
                    "cluster": "ares",
                    "pipeline_id": "site_simulation_4node",
                    "idempotency_key": "virtual-jarvis-create",
                },
            },
        },
        queue=queue,
    )

    assert response is not None
    result = response["result"]["structuredContent"]
    job = queue.get_job(result["job_id"])
    assert isinstance(job.spec, McpCallSpec)
    assert job.cluster == "ares"
    assert job.spec.server == "uvx"
    assert job.spec.server_args == [
        "--from",
        "clio-kit==3.0.0",
        "clio-kit",
        "mcp-server",
        "jarvis",
    ]
    assert job.spec.tool == "jarvis_create_pipeline"
    assert job.spec.arguments == {"pipeline_id": "site_simulation_4node"}


def test_remote_virtual_jarvis_call_defers_artifact_selection_to_cluster(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    definition = ClusterDefinition(name="ares", ssh_host="ares-login")
    writes: list[tuple[str, bytes]] = []
    removals: list[str] = []
    commands: list[list[str]] = []

    def remote_definition(_cluster: str) -> ClusterDefinition:
        return definition

    def artifact_binding(_cluster: str) -> str:
        return "a" * 64

    def write_remote(_definition: ClusterDefinition, path: str, data: bytes) -> None:
        writes.append((path, data))

    def fail_local_resolution() -> str:
        raise AssertionError("desktop resolved JARVIS artifact")

    def remove_remote(
        _definition: ClusterDefinition,
        path: str,
        *,
        remove_empty_parent: bool = False,
    ) -> None:
        del remove_empty_parent
        removals.append(path)

    monkeypatch.setattr(
        "clio_relay.mcp_server._remote_cluster_definition",
        remote_definition,
    )
    monkeypatch.setattr(
        "clio_relay.mcp_server.jarvis_mcp_artifact_binding",
        artifact_binding,
    )
    monkeypatch.setattr(
        "clio_relay.mcp_server.write_remote_file",
        write_remote,
    )
    monkeypatch.setattr(
        "clio_relay.mcp_server.remove_remote_file",
        remove_remote,
    )

    def run_remote(_definition: ClusterDefinition, args: list[str]) -> str:
        commands.append(args)
        return "job_remote_jarvis\n"

    monkeypatch.setattr("clio_relay.mcp_server.run_remote_clio", run_remote)
    monkeypatch.setattr(
        "clio_relay.mcp_server.jarvis_mcp_server",
        fail_local_resolution,
    )

    response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 240,
            "method": "tools/call",
            "params": {
                "name": "jarvis_describe",
                "arguments": {
                    "cluster": "ares",
                    "target": "packages",
                    "idempotency_key": "remote-receipt-bound-jarvis",
                },
            },
        },
        queue=queue,
    )

    assert response is not None
    assert response["result"]["structuredContent"]["job_id"] == "job_remote_jarvis"
    assert writes and json.loads(writes[0][1]) == {"target": "packages"}
    assert removals == [writes[0][0]]
    assert commands[0][0] == "jarvis-mcp-call"
    assert "--server" not in commands[0]
    assert commands[0][commands[0].index("--tool") + 1] == "jarvis_describe"


def test_mcp_virtual_jarvis_edit_routes_remove_operation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_local_cluster(tmp_path, monkeypatch, "test-cluster")
    queue = ClioCoreQueue(tmp_path / "core")

    response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 25,
            "method": "tools/call",
            "params": {
                "name": "jarvis_edit_step",
                "arguments": {
                    "cluster": "test-cluster",
                    "pipeline_id": "example",
                    "step_id": "simulation",
                    "operation": "remove",
                },
            },
        },
        queue=queue,
    )

    assert response is not None
    job = queue.get_job(response["result"]["structuredContent"]["job_id"])
    assert isinstance(job.spec, McpCallSpec)
    assert job.spec.tool == "jarvis_edit_step"
    assert job.spec.arguments == {
        "pipeline_id": "example",
        "step_id": "simulation",
        "operation": "remove",
    }


def test_mcp_virtual_jarvis_run_forwards_spack_specs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_local_cluster(tmp_path, monkeypatch, "test-cluster")
    queue = ClioCoreQueue(tmp_path / "core")

    response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 26,
            "method": "tools/call",
            "params": {
                "name": "jarvis_run",
                "arguments": {
                    "cluster": "test-cluster",
                    "pipeline_id": "example",
                    "spack_specs": ["lammps@2024.08.29"],
                },
            },
        },
        queue=queue,
    )

    assert response is not None
    job = queue.get_job(response["result"]["structuredContent"]["job_id"])
    assert isinstance(job.spec, McpCallSpec)
    assert job.spec.tool == "jarvis_run"
    assert job.spec.arguments == {
        "pipeline_id": "example",
        "spack_specs": ["lammps@2024.08.29"],
    }


def test_mcp_virtual_jarvis_run_is_fresh_unless_idempotency_is_explicit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _configure_local_cluster(tmp_path, monkeypatch, "test-cluster")
    queue = ClioCoreQueue(tmp_path / "core")
    request = {
        "jsonrpc": "2.0",
        "id": 27,
        "method": "tools/call",
        "params": {
            "name": "jarvis_run",
            "arguments": {
                "cluster": "test-cluster",
                "pipeline_id": "example",
            },
        },
    }

    first = handle_request(request, queue=queue)
    second = handle_request(request, queue=queue)

    assert first is not None
    assert second is not None
    assert (
        first["result"]["structuredContent"]["job_id"]
        != (second["result"]["structuredContent"]["job_id"])
    )


def test_mcp_remote_agent_default_idempotency_includes_timeout(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    base_arguments = {
        "cluster": "test-cluster",
        "prompt_path": "/remote/prompt.md",
    }

    first = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 31,
            "method": "tools/call",
            "params": {"name": "relay_submit_remote_agent", "arguments": base_arguments},
        },
        queue=queue,
    )
    second = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 32,
            "method": "tools/call",
            "params": {
                "name": "relay_submit_remote_agent",
                "arguments": {**base_arguments, "timeout_seconds": 30},
            },
        },
        queue=queue,
    )

    assert first is not None
    assert second is not None
    first_result = first["result"]["structuredContent"]
    second_result = second["result"]["structuredContent"]
    assert first_result["job_id"] != second_result["job_id"]
    assert queue.get_job(second_result["job_id"]).spec.timeout_seconds == 30


def test_mcp_call_default_idempotency_includes_timeout(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    base_arguments = {
        "cluster": "test-cluster",
        "server": "remote-tool-server",
        "tool": "run",
        "arguments": {"case": "site-simulation"},
    }

    first = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 33,
            "method": "tools/call",
            "params": {"name": "relay_submit_mcp_call", "arguments": base_arguments},
        },
        queue=queue,
    )
    second = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 34,
            "method": "tools/call",
            "params": {
                "name": "relay_submit_mcp_call",
                "arguments": {**base_arguments, "timeout_seconds": 60},
            },
        },
        queue=queue,
    )

    assert first is not None
    assert second is not None
    first_result = first["result"]["structuredContent"]
    second_result = second["result"]["structuredContent"]
    assert first_result["job_id"] != second_result["job_id"]
    assert queue.get_job(second_result["job_id"]).spec.timeout_seconds == 60


def test_mcp_submit_is_idempotent(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    request = {
        "jsonrpc": "2.0",
        "id": 3,
        "method": "tools/call",
        "params": {
            "name": "relay_submit_jarvis_pipeline",
            "arguments": {
                "cluster": "test-cluster",
                "pipeline_yaml": "name: generic\npkgs: []\n",
                "idempotency_key": "same",
            },
        },
    }

    first = handle_request(request, queue=queue)
    second = handle_request(request, queue=queue)

    assert first is not None
    assert second is not None
    assert (
        first["result"]["structuredContent"]["job_id"]
        == second["result"]["structuredContent"]["job_id"]
    )


def test_mcp_watch_events_returns_cursor(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    submit_response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 4,
            "method": "tools/call",
            "params": {
                "name": "relay_submit_jarvis_pipeline",
                "arguments": {
                    "cluster": "test-cluster",
                    "pipeline_yaml": "name: generic\npkgs: []\n",
                },
            },
        },
        queue=queue,
    )
    assert submit_response is not None
    job_id = submit_response["result"]["structuredContent"]["job_id"]

    watch_response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 5,
            "method": "tools/call",
            "params": {
                "name": "relay_watch_job_events",
                "arguments": {"job_id": job_id, "cursor": 1},
            },
        },
        queue=queue,
    )

    assert watch_response is not None
    structured = watch_response["result"]["structuredContent"]
    assert structured["events"][0]["event_type"] == "job.queued"
    assert structured["next_cursor"] == 2


def test_mcp_monitor_returns_job_and_events(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    submit_response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 7,
            "method": "tools/call",
            "params": {
                "name": "relay_submit_jarvis_pipeline",
                "arguments": {
                    "cluster": "test-cluster",
                    "pipeline_yaml": "name: generic\npkgs: []\n",
                },
            },
        },
        queue=queue,
    )
    assert submit_response is not None
    job_id = submit_response["result"]["structuredContent"]["job_id"]

    monitor_response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 8,
            "method": "tools/call",
            "params": {
                "name": "relay_monitor_job",
                "arguments": {"job_id": job_id, "cursor": 1},
            },
        },
        queue=queue,
    )

    assert monitor_response is not None
    structured = monitor_response["result"]["structuredContent"]
    assert structured["job"]["job_id"] == job_id
    assert structured["events"][0]["event_type"] == "job.queued"
    assert structured["terminal"] is False


def test_mcp_get_job_status_returns_relay_queue(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    submit_response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 27,
            "method": "tools/call",
            "params": {
                "name": "relay_submit_jarvis_pipeline",
                "arguments": {
                    "cluster": "test-cluster",
                    "pipeline_yaml": "name: generic\npkgs: []\n",
                },
            },
        },
        queue=queue,
    )
    assert submit_response is not None
    job_id = submit_response["result"]["structuredContent"]["job_id"]

    status_response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 28,
            "method": "tools/call",
            "params": {
                "name": "relay_get_job_status",
                "arguments": {"job_id": job_id},
            },
        },
        queue=queue,
    )

    assert status_response is not None
    status = status_response["result"]["structuredContent"]
    assert status["relay_queue"] == {"state": "queued", "jobs_ahead": 0, "position": 1}


def test_mcp_records_and_lists_progress(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    submit_response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 23,
            "method": "tools/call",
            "params": {
                "name": "relay_submit_jarvis_pipeline",
                "arguments": {
                    "cluster": "test-cluster",
                    "pipeline_yaml": "name: generic\npkgs: []\n",
                },
            },
        },
        queue=queue,
    )
    assert submit_response is not None
    job_id = submit_response["result"]["structuredContent"]["job_id"]

    record_response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 24,
            "method": "tools/call",
            "params": {
                "name": "relay_record_progress",
                "arguments": {
                    "job_id": job_id,
                    "label": "iteration",
                    "current": 1,
                    "total": 2,
                    "unit": "step",
                    "message": "running",
                    "metadata": {
                        "source": "jarvis_package",
                        "adapter": "site-progress",
                        "package_name": "site.simulation",
                        "package_version": "2.1",
                        "run_id": "spoofed",
                    },
                },
            },
        },
        queue=queue,
    )
    list_response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 25,
            "method": "tools/call",
            "params": {
                "name": "relay_list_progress",
                "arguments": {"job_id": job_id},
            },
        },
        queue=queue,
    )

    assert record_response is not None
    assert list_response is not None
    recorded = record_response["result"]["structuredContent"]
    listed = list_response["result"]["structuredContent"]["progress"]
    assert recorded["label"] == "iteration"
    assert recorded["current"] == 1
    assert recorded["metadata"]["source"] == "external_mcp"
    assert "package_name" not in recorded["metadata"]
    assert "run_id" not in recorded["metadata"]
    assert listed[0]["progress_id"] == recorded["progress_id"]


def test_mcp_lists_job_tasks(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml="name: generic\npkgs: []\n"),
            idempotency_key="mcp-tasks",
        )
    )
    task = queue.append_task(RelayTask(job_id=job.job_id, name="jarvis.execution"))

    response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 26,
            "method": "tools/call",
            "params": {
                "name": "relay_list_tasks",
                "arguments": {"job_id": job.job_id},
            },
        },
        queue=queue,
    )

    assert response is not None
    tasks = response["result"]["structuredContent"]["tasks"]
    assert tasks[0]["task_id"] == task.task_id
    assert tasks[0]["name"] == "jarvis.execution"


def test_agent_mcp_profile_points_to_clio_relay_server() -> None:
    rendered = render_agent_mcp_profile(
        settings=RelaySettings(core_dir=Path("/tmp/core"), spool_dir=Path("/tmp/spool"))
    )

    assert "[mcp_servers.clio-relay]" in rendered
    assert 'command = "clio-relay"' in rendered
    assert 'args = ["mcp-server"]' in rendered
    assert "[mcp_servers.clio-relay.env]" in rendered
    assert "CLIO_RELAY_CORE_DIR =" in rendered
    assert "tmp" in rendered
    assert "core" in rendered
    assert "CLIO_RELAY_SPOOL_DIR =" in rendered
    assert "spool" in rendered


def test_codex_mcp_profile_alias_matches_generic_agent_profile() -> None:
    settings = RelaySettings(core_dir=Path("/tmp/core"), spool_dir=Path("/tmp/spool"))

    assert render_codex_mcp_profile(settings=settings) == render_agent_mcp_profile(
        settings=settings
    )


def test_mcp_response_content_is_json(tmp_path: Path) -> None:
    response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 6,
            "method": "tools/call",
            "params": {
                "name": "relay_submit_jarvis_pipeline",
                "arguments": {
                    "cluster": "test-cluster",
                    "pipeline_yaml": "name: generic\npkgs: []\n",
                },
            },
        },
        queue=ClioCoreQueue(tmp_path / "core"),
    )

    assert response is not None
    text = response["result"]["content"][0]["text"]
    assert json.loads(text)["state"] == "queued"


def test_mcp_reads_logs_and_artifacts(tmp_path: Path) -> None:
    settings = RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool")
    queue = ClioCoreQueue(settings.core_dir)
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml="name: generic\npkgs: []\n"),
            idempotency_key="log-artifact",
        )
    )
    spool = settings.spool_dir / job.job_id
    spool.mkdir(parents=True)
    stdout_path = spool / "stdout.log"
    stdout_path.write_text("hello world\n", encoding="utf-8")
    artifact = queue.append_artifact(
        ArtifactRef(job_id=job.job_id, uri=stdout_path.as_uri(), kind="stdout")
    )

    log_response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 9,
            "method": "tools/call",
            "params": {
                "name": "relay_read_job_log",
                "arguments": {"job_id": job.job_id, "stream": "stdout", "offset": 0, "limit": 5},
            },
        },
        queue=queue,
        settings=settings,
    )
    list_response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 10,
            "method": "tools/call",
            "params": {
                "name": "relay_list_artifacts",
                "arguments": {"job_id": job.job_id},
            },
        },
        queue=queue,
        settings=settings,
    )
    content_response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 11,
            "method": "tools/call",
            "params": {
                "name": "relay_read_artifact",
                "arguments": {"artifact_id": artifact.artifact_id},
            },
        },
        queue=queue,
        settings=settings,
    )

    assert log_response is not None
    assert log_response["result"]["structuredContent"]["text"] == "hello"
    assert log_response["result"]["structuredContent"]["next_offset"] == 5
    assert list_response is not None
    assert (
        list_response["result"]["structuredContent"]["artifacts"][0]["artifact_id"]
        == artifact.artifact_id
    )
    assert content_response is not None
    assert content_response["result"]["structuredContent"]["encoding"] == "base64"


def test_mcp_cancels_job(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml="name: generic\npkgs: []\n"),
            idempotency_key="mcp-cancel",
        )
    )

    response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 12,
            "method": "tools/call",
            "params": {
                "name": "relay_cancel_job",
                "arguments": {"job_id": job.job_id},
            },
        },
        queue=queue,
    )

    assert response is not None
    assert response["result"]["structuredContent"]["job"]["state"] == "canceled"
    assert response["result"]["structuredContent"]["scheduler_policy"] == "relay-only"
    events, _ = queue.drain_events(Cursor(job_id=job.job_id), limit=20)
    assert [event.event_type for event in events][-2:] == [
        "job.cancel_requested",
        "job.canceled",
    ]


def test_mcp_queue_management_tools(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    queue.register_endpoint(
        EndpointRegistration(
            role=EndpointRole.WORKER,
            cluster="test-cluster",
            hostname="node",
            pid=123,
            metadata={"concurrency": 5},
        )
    )
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml="name: generic\npkgs: []\n"),
            idempotency_key="mcp-queue-management",
        )
    )
    queue.acquire_next_job("endpoint-1", cluster="test-cluster", ttl_seconds=-1)

    listed = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 41,
            "method": "tools/call",
            "params": {
                "name": "relay_queue_list",
                "arguments": {"cluster": "test-cluster"},
            },
        },
        queue=queue,
    )
    diagnosed = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 42,
            "method": "tools/call",
            "params": {
                "name": "relay_queue_diagnose",
                "arguments": {
                    "job_id": job.job_id,
                    "cluster": "test-cluster",
                },
            },
        },
        queue=queue,
    )
    stale = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 45,
            "method": "tools/call",
            "params": {
                "name": "relay_queue_stale",
                "arguments": {
                    "cluster": "test-cluster",
                    "older_than_seconds": 3600,
                    "kind": "jarvis",
                },
            },
        },
        queue=queue,
    )
    workers = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 43,
            "method": "tools/call",
            "params": {
                "name": "relay_worker_status",
                "arguments": {"cluster": "test-cluster"},
            },
        },
        queue=queue,
    )
    cleanup = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 44,
            "method": "tools/call",
            "params": {
                "name": "relay_queue_cleanup_stale",
                "arguments": {"cluster": "test-cluster", "dry_run": False},
            },
        },
        queue=queue,
    )

    assert listed is not None
    assert diagnosed is not None
    assert stale is not None
    assert workers is not None
    assert cleanup is not None
    assert listed["result"]["structuredContent"]["jobs"][0]["job"]["job_id"] == job.job_id
    assert diagnosed["result"]["structuredContent"]["reason"] == "stale_lease"
    assert stale["result"]["structuredContent"]["jobs"][0]["job"]["job_id"] == job.job_id
    assert workers["result"]["structuredContent"]["configured_concurrency"] == 5
    assert cleanup["result"]["structuredContent"]["recovered_count"] == 1


def test_mcp_stale_exact_job_target_preserves_neighbor(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    jobs = [
        queue.submit_job(
            RelayJob(
                cluster="test-cluster",
                kind=JobKind.JARVIS,
                spec=JarvisRunSpec(command=["true"]),
                idempotency_key=f"mcp-exact-stale-{index}",
            )
        )
        for index in range(2)
    ]
    old = utc_now() - timedelta(hours=3)
    for job in jobs:
        queue._write_job_unlocked(  # pyright: ignore[reportPrivateUsage]  # noqa: SLF001
            job.model_copy(update={"created_at": old, "updated_at": old})
        )

    discovered = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 81,
            "method": "tools/call",
            "params": {
                "name": "relay_queue_stale",
                "arguments": {
                    "cluster": "test-cluster",
                    "job_id": jobs[0].job_id,
                    "older_than_seconds": 60,
                },
            },
        },
        queue=queue,
        profile="admin",
    )
    cleaned = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 82,
            "method": "tools/call",
            "params": {
                "name": "relay_queue_cleanup_stale",
                "arguments": {
                    "cluster": "test-cluster",
                    "job_id": jobs[0].job_id,
                    "older_than_seconds": 60,
                    "cancel_queued": True,
                    "dry_run": False,
                },
            },
        },
        queue=queue,
        profile="admin",
    )

    assert discovered is not None
    assert cleaned is not None
    discovered_payload = discovered["result"]["structuredContent"]
    cleaned_payload = cleaned["result"]["structuredContent"]
    assert [item["job"]["job_id"] for item in discovered_payload["jobs"]] == [jobs[0].job_id]
    assert [item["job_id"] for item in cleaned_payload["planned"]] == [jobs[0].job_id]
    assert queue.get_job(jobs[0].job_id).state is JobState.CANCELED
    assert queue.get_job(jobs[1].job_id).state is JobState.QUEUED


def test_mcp_creates_and_evaluates_monitor_rule(tmp_path: Path) -> None:
    queue = ClioCoreQueue(tmp_path / "core")
    job = queue.submit_job(
        RelayJob(
            cluster="test-cluster",
            kind=JobKind.JARVIS,
            spec=JarvisRunSpec(pipeline_yaml="name: generic\npkgs: []\n"),
            idempotency_key="mcp-monitor",
        )
    )
    queue.append_event(job.job_id, "stdout.delta", "step 75", payload={"text": "step 75\n"})

    create_response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 12,
            "method": "tools/call",
            "params": {
                "name": "relay_create_monitor_rule",
                "arguments": {
                    "job_id": job.job_id,
                    "pattern": "step 75",
                    "event_types": ["stdout.delta"],
                },
            },
        },
        queue=queue,
    )
    list_response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 13,
            "method": "tools/call",
            "params": {
                "name": "relay_list_monitor_rules",
                "arguments": {"job_id": job.job_id},
            },
        },
        queue=queue,
    )
    run_response = handle_request(
        {
            "jsonrpc": "2.0",
            "id": 14,
            "method": "tools/call",
            "params": {"name": "relay_evaluate_monitor_rules", "arguments": {}},
        },
        queue=queue,
    )

    assert create_response is not None
    assert create_response["result"]["structuredContent"]["job_id"] == job.job_id
    assert list_response is not None
    assert list_response["result"]["structuredContent"]["rules"][0]["job_id"] == job.job_id
    assert run_response is not None
    assert run_response["result"]["structuredContent"]["actions"][0]["action"] == "emit_event"


def test_stdio_server_reports_parse_errors(tmp_path: Path) -> None:
    stdout = StringIO()

    serve_stdio(
        stdin=StringIO("not-json\n"),
        stdout=stdout,
        settings=RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool"),
    )

    response = json.loads(stdout.getvalue())
    assert response["error"]["code"] == -32700


def test_stdio_server_accepts_utf8_bom(tmp_path: Path) -> None:
    stdout = StringIO()

    serve_stdio(
        stdin=StringIO('\ufeff{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}\n'),
        stdout=stdout,
        settings=RelaySettings(core_dir=tmp_path / "core", spool_dir=tmp_path / "spool"),
        profile="user",
    )

    response = json.loads(stdout.getvalue())
    tool_names = {tool["name"] for tool in response["result"]["tools"]}
    assert "relay_submit_agent" in tool_names
    assert "relay_submit_remote_agent" not in tool_names
